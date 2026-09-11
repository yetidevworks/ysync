#!/usr/bin/env python3
"""Measure isolated watcher work and daemon CPU; no peers or real user folders."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--files', type=int, default=10000)
    parser.add_argument('--ignored-dirs', type=int, default=1000)
    parser.add_argument('--base-dir', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.files < 2 or args.ignored_dirs < 0:
        parser.error('need at least two files and a nonnegative ignored directory count')
    binary = str(Path(args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix='ysync-watch-bench-', dir=args.base_dir) as temp:
        base = Path(temp)
        state, root = base/'state', base/'files'
        root.mkdir()
        def cli(*command):
            return subprocess.check_output([binary, '--home', str(state), *command], text=True)
        cli('init', '--listen', '127.0.0.1:0', '--rescan-secs', '3600')
        cli('folder', 'add', 'bench', str(root), '--dev')
        for i in range(args.files):
            path = root/f'src/d{i//100:05}/f{i:08}'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'initial')
        for i in range(args.ignored_dirs):
            (root/f'node_modules/p{i}').mkdir(parents=True)
        def status():
            return json.loads((state/'status.json').read_text())['folders']['bench']
        def entry(path):
            with sqlite3.connect(f'file:{state}/index.sqlite?mode=ro', uri=True) as c:
                row = c.execute('SELECT data FROM entries WHERE folder=? AND path=?', ('bench', path)).fetchone()
                return json.loads(row[0]) if row else None
        def cpu_seconds(pid):
            proc = Path(f'/proc/{pid}/stat')
            if proc.exists():
                fields = proc.read_text().rsplit(')', 1)[1].split()
                return (int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK')
            value = subprocess.check_output(['ps', '-p', str(pid), '-o', 'time='], text=True).strip()
            total = 0.0
            for component in value.split(':'):
                total = total*60+float(component)
            return total
        def wait(condition, timeout=120):
            until = time.monotonic()+timeout
            while time.monotonic() < until:
                if daemon.poll() is not None:
                    raise RuntimeError('daemon exited')
                try:
                    if condition(): return
                except (FileNotFoundError, KeyError, sqlite3.OperationalError):
                    pass
                time.sleep(.1)
            raise TimeoutError(f'watch benchmark condition timed out: {status()}')
        with (base/'daemon.log').open('w') as log:
            daemon = subprocess.Popen([binary, '--home', str(state), 'serve'], stdout=log, stderr=log)
            try:
                wait(lambda: status()['phase']=='watching' and status()['hashed_files']==args.files)
                time.sleep(1.2)
                before = status()
                cpu_before = cpu_seconds(daemon.pid)
                started = time.monotonic()
                time.sleep(4)
                idle_cpu = cpu_seconds(daemon.pid)-cpu_before
                idle_wall = time.monotonic()-started
                idle = status()
                assert idle['full_scans']==before['full_scans']
                assert idle['checked_entries']==before['checked_entries']
                assert idle['hashed_files']==before['hashed_files']
                first = 'src/d00000/f00000000'
                old_seq = entry(first)['seq']
                for _ in range(200): (root/first).write_bytes(b'after burst')
                wait(lambda: entry(first)['seq']>old_seq and status()['hashed_files']>idle['hashed_files'])
                time.sleep(1.2)
                edited = status()
                second = 'src/d00000/f00000001'
                (root/second).unlink()
                wait(lambda: entry(second)['kind']=='Deleted' and status()['scoped_scans']>edited['scoped_scans'])
                time.sleep(1.2)
                deleted = status()
                if args.ignored_dirs:
                    for i in range(200): (root/f'node_modules/p0/f{i}').write_bytes(b'ignored')
                time.sleep(1.2)
                ignored = status()
                assert ignored['full_scans']==before['full_scans'], 'ordinary activity triggered full scan'
                assert ignored['hashed_files']==deleted['hashed_files'], 'ignored activity hashed files'
                assert ignored['checked_entries']==deleted['checked_entries'], 'ignored activity checked files'
                result = dict(files=args.files, ignored_directories=args.ignored_dirs,
                    native_registrations=before['native_watches'], watcher=before['watcher'],
                    idle_seconds=round(idle_wall,3), idle_cpu_seconds=round(idle_cpu,4),
                    idle_cpu_percent_of_one_core=round(100*idle_cpu/idle_wall,2),
                    idle_checked_entries=idle['checked_entries']-before['checked_entries'],
                    burst_writes=200, burst_checked_entries=edited['checked_entries']-idle['checked_entries'],
                    burst_hashed_files=edited['hashed_files']-idle['hashed_files'],
                    delete_checked_entries=deleted['checked_entries']-edited['checked_entries'],
                    delete_hashed_files=deleted['hashed_files']-edited['hashed_files'],
                    extra_full_scans=ignored['full_scans']-before['full_scans'],
                    ignored_checked_entries=ignored['checked_entries']-deleted['checked_entries'],
                    scope='One daemon, synthetic disk-backed tree when --base-dir is disk-backed; CPU sample includes the complete daemon, not just watcher callbacks. Single trial; OS CPU accounting resolution applies.')
                print(json.dumps(result, indent=2))
                if args.output:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(json.dumps(result, indent=2)+'\n')
            except Exception:
                print((base/'daemon.log').read_text()[-6000:])
                raise
            finally:
                daemon.terminate()
                try: daemon.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait()


if __name__ == '__main__':
    main()
