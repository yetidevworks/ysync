#!/usr/bin/env python3
"""Compare first indexing and warm reconciliation on a nested synthetic tree."""
import argparse
import json
from pathlib import Path
import subprocess
import time
from benchmark_live_watch import cpu_seconds

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', required=True, type=Path)
p.add_argument('--base', required=True, type=Path)
p.add_argument('--label', required=True)
a = p.parse_args()
base = a.base.resolve()
base.mkdir(parents=True, exist_ok=True)
root = base/'files'
if not root.exists():
    root.mkdir()
    for group in range(60):
        parent = root/f'g{group:03}'
        for depth in range(8):
            parent /= f'd{depth}'
        parent.mkdir(parents=True)
        for f in range(100):
            (parent/f'f{f:03}.txt').write_bytes(b'x'*4096)
state = base/a.label
if state.exists():
    raise SystemExit('label state already exists')
binary = str(a.binary.resolve())
def cli(*args):
    subprocess.run([binary, '--home', str(state), *args], check=True, stdout=subprocess.DEVNULL)
cli('init', '--listen', '127.0.0.1:0', '--scan-workers', '2', '--rescan-secs', '3600')
cli('folder', 'add', 'bench', str(root))
result = {'label': a.label, 'files': 6000, 'file_bytes': 4096, 'directory_depth': 9,
          'scope': 'Synthetic nested tree, warm filesystem cache, two hash workers; fresh index then daemon restart. Single trial, startup/status polling included.'}
for stage in ('initial_index', 'warm_reconciliation'):
    (state/'status.json').unlink(missing_ok=True)
    start = time.monotonic()
    with (state/f'{stage}.log').open('w') as log:
        child = subprocess.Popen([binary, '--home', str(state), 'serve'], stdout=log, stderr=log)
    try:
        while True:
            if child.poll() is not None:
                raise RuntimeError('daemon exited')
            try:
                status = json.loads((state/'status.json').read_text())['folders']['bench']
                if status['phase'] in ('error', 'incomplete'):
                    raise RuntimeError(status['error'])
                if status['phase']=='watching' and status['files']==6000:
                    break
            except (FileNotFoundError, KeyError, json.JSONDecodeError):
                pass
            if time.monotonic()-start > 180:
                raise TimeoutError('scan benchmark timed out')
            time.sleep(.05)
        result[stage] = {'wall_seconds': round(time.monotonic()-start, 3),
                         'cpu_seconds': cpu_seconds(child.pid),
                         'checked_entries': status['checked_entries'],
                         'hashed_files': status['hashed_files']}
        if stage=='warm_reconciliation':
            assert status['hashed_files']==0
    finally:
        child.terminate()
        try: child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
(base/f'{a.label}.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result, indent=2))
