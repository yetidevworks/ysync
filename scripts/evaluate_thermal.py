#!/usr/bin/env python3
"""Exercise real Linux CPU sensing with two tiny files and no peers.

Requires a supported sensor currently above 40 C and below 70 C. Changes only
settings in a disposable state directory; does not heat or cool the machine.
"""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    records = {}
    with tempfile.TemporaryDirectory(prefix="ysync-thermal-") as temp:
        base = Path(temp)
        state, files = base / "state", base / "files"
        files.mkdir()
        (files / "first").write_bytes(b"initial tiny file")

        def cli(*args):
            subprocess.run([str(binary), "--home", str(state), *args],
                           check=True, stdout=subprocess.DEVNULL)

        def wait(label, condition):
            until = time.monotonic() + 15
            latest = None
            while time.monotonic() < until:
                if process.poll() is not None:
                    raise RuntimeError(f"daemon exited: {process.returncode}")
                try:
                    latest = json.loads((state / "status.json").read_text())
                    if condition(latest):
                        records[label] = {"thermal": latest["thermal"], "folder": latest["folders"].get("tiny")}
                        return latest
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                time.sleep(.05)
            raise RuntimeError(f"{label}: timed out; snapshot {latest}")

        cli("init", "--listen", "127.0.0.1:0", "--scan-workers", "1", "--scan-max-temp-c", "40")
        cli("folder", "add", "tiny", str(files))
        with (base / "daemon.log").open("w+") as log:
            process = subprocess.Popen([str(binary), "--home", str(state), "serve"], stdout=log, stderr=log)
            try:
                paused = wait("initial_cooling", lambda s: s["thermal"]["cooling"]
                              and s["folders"].get("tiny", {}).get("scan_waiting_for_cooling"))
                assert paused["thermal"]["error"] is None
                assert paused["folders"]["tiny"]["hashed_files"] == 0
                cli("init", "--scan-max-temp-c", "75")
                resumed = wait("initial_resumed", lambda s: not s["thermal"]["cooling"]
                               and s["folders"].get("tiny", {}).get("phase") == "watching")
                assert resumed["folders"]["tiny"]["full_scans"] == 1
                assert resumed["folders"]["tiny"]["hashed_files"] == 1
                cli("init", "--scan-max-temp-c", "40")
                wait("idle_cooling", lambda s: s["thermal"]["cooling"])
                (files / "second").write_bytes(b"queued watcher change")
                scoped = wait("scoped_cooling", lambda s: s["folders"].get("tiny", {}).get("scan_waiting_for_cooling"))
                assert scoped["folders"]["tiny"]["hashed_files"] == 1
                started = time.monotonic()
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=3)
                records["stop_seconds"] = time.monotonic() - started
                assert process.returncode == 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    records["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    records["scope"] = "two tiny files, no peers; thresholds moved around actual CPU temperature"
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
