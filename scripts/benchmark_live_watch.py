#!/usr/bin/env python3
"""Evaluate real roots with unpaired ysync; change only uniquely named probe files."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
import uuid


def cpu_seconds(pid):
    proc = Path(f'/proc/{pid}/stat')
    if proc.exists():
        fields = proc.read_text().rsplit(')', 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')
    value = subprocess.check_output(['ps', '-p', str(pid), '-o', 'time='], text=True).strip()
    total = 0.0
    for component in value.split(':'):
        total = total * 60 + float(component)
    return total


def temperatures():
    values = {}
    for hw in Path('/sys/class/hwmon').glob('hwmon*'):
        try:
            if (hw/'name').read_text().strip() not in ('k10temp', 'coretemp'):
                continue
            for sensor in hw.glob('temp*_input'):
                values[str(sensor)] = int(sensor.read_text()) / 1000
        except (OSError, ValueError):
            pass
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--state', required=True, type=Path)
    parser.add_argument('--root', action='append', required=True, type=Path)
    parser.add_argument('--resume', action='store_true', help='Reuse this evaluation index')
    parser.add_argument('--cpu-quota', type=int, help='Linux transient service CPU cap, percent of one core')
    args = parser.parse_args()
    binary, state = args.binary.resolve(), args.state.resolve()
    state.mkdir(parents=True, exist_ok=args.resume)
    roots = {p.name.lower(): p.resolve() for p in args.root}
    if len(roots) != len(args.root):
        parser.error('root names must be unique')
    def cli(*command):
        return subprocess.check_output([str(binary), '--home', str(state), *command], text=True)
    cli('init', '--name', 'unpaired-scan-evaluation', '--listen', '127.0.0.1:0',
        '--scan-workers', '2', '--rescan-secs', '3600')
    if not args.resume:
        for name, root in roots.items():
            cli('folder', 'add', name, str(root))
    config = json.loads((state/'config.json').read_text())
    assert config['peers'] == []
    assert config['listen'] == '127.0.0.1:0'
    configured = {f['id']: Path(f['path']) for f in config['folders']}
    assert all(configured.get(name) == root for name, root in roots.items())
    for name in configured:
        cli('folder', 'resume' if name in roots else 'pause', name)
    if args.cpu_quota is not None and (args.cpu_quota < 1 or not Path('/proc').exists()):
        parser.error('--cpu-quota requires Linux and a positive value')
    result = {'state': str(state), 'roots': {k: str(v) for k, v in roots.items()},
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'workers': 2, 'resumed_index': args.resume, 'cpu_quota_percent': args.cpu_quota,
              'initial': {}, 'probes': {}, 'cpu_temperature_peak_c': None,
              'scope': 'Unpaired daemons on live trees; existing files only read. Temporary probes created and removed. Concurrent unrelated filesystem activity may affect counters. No peer transfers.'}
    probes = []
    daemon = None
    unit = None
    def snapshot():
        for attempt in range(10):
            try:
                return json.loads((state/'status.json').read_text())
            except json.JSONDecodeError:
                time.sleep(.01)
        raise RuntimeError('cannot read a complete status snapshot')
    def daemon_cpu():
        return cpu_seconds(snapshot()['pid'])
    def row(name, path):
        with sqlite3.connect(f'file:{state}/index.sqlite?mode=ro', uri=True, timeout=1) as db:
            found = db.execute('SELECT data FROM entries WHERE folder=? AND path=?', (name, path)).fetchone()
            return json.loads(found[0]) if found else None
    last_report = 0
    started = time.monotonic()
    def check():
        nonlocal last_report
        if daemon.poll() is not None:
            raise RuntimeError(f'daemon exited: {daemon.returncode}')
        temps = temperatures()
        if temps:
            peak = max(temps.values())
            result['cpu_temperature_peak_c'] = max(result['cpu_temperature_peak_c'] or 0, peak)
            if peak >= 85:
                raise RuntimeError(f'evaluation stopped at CPU temperature {peak} C')
        now = time.monotonic()
        if now - last_report >= 15:
            try:
                s = snapshot()
                print(json.dumps({'elapsed_s': round(now-started, 1), 'cpu_s': daemon_cpu(),
                    'temperature_c': temps, 'folders': {k: {field: v[field] for field in ('phase', 'scanned', 'hashed_files', 'full_scans', 'scoped_scans', 'native_watches', 'error')} for k, v in s['folders'].items()}}), flush=True)
            except (FileNotFoundError, KeyError):
                pass
            last_report = now
    def wait(predicate, timeout=120):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            check()
            try:
                if predicate():
                    return
            except (FileNotFoundError, KeyError, sqlite3.OperationalError):
                pass
            time.sleep(.1)
        raise TimeoutError('evaluation condition timed out')
    def settle(seconds):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            check()
            time.sleep(.2)
    def delta(a, b):
        return {k: b[k]-a[k] for k in ('checked_entries', 'hashed_files', 'hashed_bytes', 'full_scans', 'scoped_scans', 'watch_events', 'coalesced_events', 'watch_overflows')}
    try:
        (state/'status.json').unlink(missing_ok=True)
        with (state/'daemon.log').open('w') as log:
            command = ['nice', '-n', '10', str(binary), '--home', str(state), 'serve']
            if args.cpu_quota:
                unit = 'ysync-evaluation-' + uuid.uuid4().hex
                command = ['systemd-run', '--user', '--unit', unit, '--collect', '--wait', '--pipe',
                           '--property', f'CPUQuota={args.cpu_quota}%', *command]
            daemon = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True)
        (state/'evaluation.json').write_text(json.dumps({'pid': daemon.pid, 'runner_pid': os.getpid(), 'started': time.time()}))
        def indexed():
            s = snapshot()
            for name, f in s['folders'].items():
                if name in roots and name not in result['initial'] and f['last_scan'] is not None and f['phase'] in ('watching', 'polling', 'error', 'incomplete'):
                    result['initial'][name] = {'wall_seconds': round(time.monotonic()-started, 3), **f}
            return len(result['initial']) == len(roots)
        wait(indexed, timeout=3600)
        result['initial_cpu_seconds'] = daemon_cpu()
        settle(3)
        before = snapshot()
        cpu_start, idle_start = daemon_cpu(), time.monotonic()
        settle(15)
        after = snapshot()
        duration = time.monotonic()-idle_start
        cpu = daemon_cpu()-cpu_start
        result['idle'] = {'wall_seconds': round(duration, 3), 'cpu_seconds': cpu,
            'cpu_percent_of_one_core': round(100*cpu/duration, 3),
            'folders': {name: delta(before['folders'][name], after['folders'][name]) for name in roots}}
        for name, root in roots.items():
            path = root / f'ysync-evaluation-{uuid.uuid4().hex}.txt'
            probes.append(path)
            before = snapshot()['folders'][name]
            t = time.monotonic()
            with path.open('xb') as f:
                f.write(b'a'*1048576)
            wait(lambda: (row(name, path.name) or {}).get('size') == 1048576)
            created_latency = time.monotonic()-t
            settle(1.5)
            created = snapshot()['folders'][name]
            old = row(name, path.name)
            t = time.monotonic()
            for i in range(200):
                with path.open('r+b') as f:
                    f.seek(100)
                    f.write(i.to_bytes(4, 'little'))
            wait(lambda: (row(name, path.name) or {}).get('hash') != old['hash'])
            edit_latency = time.monotonic()-t
            settle(1.5)
            edited = snapshot()['folders'][name]
            t = time.monotonic()
            path.unlink()
            wait(lambda: (row(name, path.name) or {}).get('kind') == 'Deleted')
            delete_latency = time.monotonic()-t
            settle(1.5)
            deleted = snapshot()['folders'][name]
            result['probes'][name] = {'create_seconds': round(created_latency, 3), 'create_work': delta(before, created),
                'burst_writes': 200, 'burst_seconds': round(edit_latency, 3), 'burst_work': delta(created, edited),
                'delete_seconds': round(delete_latency, 3), 'delete_work': delta(edited, deleted)}
        result['final_status'] = snapshot()
        result['completed'] = True
    except Exception as exc:
        result['error'] = str(exc)
        try:
            result['final_status'] = snapshot()
        except (FileNotFoundError, ValueError):
            pass
    finally:
        if unit:
            subprocess.run(['systemctl', '--user', 'stop', unit], capture_output=True, timeout=20)
        if daemon is not None and daemon.poll() is None:
            daemon.send_signal(signal.SIGTERM)
            try:
                daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait()
        for path in probes:
            path.unlink(missing_ok=True)
        result['total_seconds'] = round(time.monotonic()-started, 3)
        (state/'results.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({'result': str(state/'results.json'), 'completed': result.get('completed', False), 'error': result.get('error')}), flush=True)
    if not result.get('completed'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
