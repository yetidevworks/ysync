#!/usr/bin/env python3
"""Isolated two-daemon benchmark. Never touches a user's existing sync configuration."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--binary", required=True)
    p.add_argument("--scan-workers", type=int, default=8)
    p.add_argument("--files", type=int, default=10000)
    p.add_argument("--size", type=int, default=4096)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    if args.files < 1 or args.size < 1:
        p.error("--files and --size must be positive")
    binary = str(Path(shutil.which(args.binary) or args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix="ysync-benchmark-") as tmp:
        base = Path(tmp)
        devices = []
        procs = []
        logs = []
        def cli(state, *cmd):
            return subprocess.check_output([binary, "--home", str(state), *cmd], text=True)
        try:
            for name in ["a", "b"]:
                state, files = base / (name + "-state"), base / (name + "-files")
                files.mkdir()
                addr = f"127.0.0.1:{port()}"
                cli(state, "init", "--name", name, "--listen", addr, "--scan-workers", str(args.scan_workers))
                cli(state, "folder", "add", "bench", str(files))
                devices.append((state, files, addr, cli(state, "id").strip()))
            a, b = devices
            payload = os.urandom(args.size)
            for i in range(args.files):
                directory = a[1] / f"d{i // 1000:05}"
                directory.mkdir(exist_ok=True)
                (directory / f"f{i:08}.bin").write_bytes(payload)
            cli(a[0], "peer", "add", b[3], "--address", b[2], "--folder", "bench")
            # Pre-approve the reverse fingerprint. Only A dials, as in the documented setup.
            path = b[0] / "config.json"
            config = json.loads(path.read_text())
            config["peers"].append(dict(id=a[3], name="a", address=None, approved=True, folders=["bench"]))
            path.write_text(json.dumps(config))
            started = time.monotonic()
            for state, *_ in devices:
                log = (state / "benchmark.log").open("w")
                logs.append(log)
                procs.append(subprocess.Popen([binary, "--home", str(state), "serve"], stdout=log, stderr=log))
            deadline = started + args.timeout
            last_report = 0
            while True:
                if any(p.poll() is not None for p in procs):
                    raise RuntimeError("a daemon exited")
                with sqlite3.connect(b[0] / "index.sqlite") as db:
                    count = db.execute("SELECT count(*) FROM entries WHERE json_extract(data,'$.kind')='File'").fetchone()[0]
                if count >= args.files:
                    break
                elapsed = time.monotonic() - started
                if elapsed - last_report >= 5:
                    print(f"{elapsed:.1f}s: {count:,}/{args.files:,} files durably indexed", flush=True)
                    last_report = elapsed
                if time.monotonic() > deadline:
                    raise TimeoutError(f"only {count} of {args.files} files synced")
                time.sleep(.1)
            duration = time.monotonic() - started
            # Verify every payload after the receiver's committed index reports completion.
            expected_hash = hashlib.sha256(payload).digest()
            for i in range(args.files):
                dest = b[1] / f"d{i // 1000:05}" / f"f{i:08}.bin"
                assert hashlib.sha256(dest.read_bytes()).digest() == expected_hash, dest
            edit = b"ysync latency probe"
            relative = "d00000/f00000000.bin"
            start_edit = time.monotonic()
            (a[1] / relative).write_bytes(edit)
            while (b[1] / relative).read_bytes() != edit:
                if time.monotonic() - start_edit > 30:
                    raise TimeoutError("watcher update exceeded 30 seconds")
                time.sleep(.01)
            result = dict(scan_workers=args.scan_workers,files=args.files, bytes=args.files * args.size,
                          bootstrap_seconds=round(duration, 3),
                          files_per_second=round(args.files / duration, 1),
                          payload_MB_per_second=round(args.files * args.size / duration / 1e6, 2),
                          edit_latency_ms=round((time.monotonic() - start_edit) * 1000, 1),
                          environment="two local daemons, loopback, same filesystem", verified_all_payloads=True)
            print(json.dumps(result, indent=2))
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(result, indent=2) + "\n")
        except Exception:
            for state, *_ in devices:
                log = state / "benchmark.log"
                if log.exists():
                    print(log.read_text()[-6000:])
                status = state / "status.json"
                if status.exists():
                    print(status.read_text()[-6000:])
            raise
        finally:
            for proc in procs:
                proc.terminate()
            for proc in procs:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
