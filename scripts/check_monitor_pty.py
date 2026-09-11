#!/usr/bin/env python3
"""Exercise the real monitor in a disposable PTY; never start a sync daemon."""
import argparse
import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import tempfile
import termios
import time
from pathlib import Path


def check(binary):
    with tempfile.TemporaryDirectory(prefix="ysync-monitor-") as home:
        result = subprocess.run([binary, "--home", home, "monitor"], capture_output=True)
        assert result.returncode != 0 and b"interactive terminal" in result.stderr, result.stderr
        for exit_mode in ("q", "ctrl-c", "sigterm"):
            for name in ("config.json", "status.json"):
                Path(home, name).unlink(missing_ok=True)
            master, slave = pty.openpty()
            before = termios.tcgetattr(slave)
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 42, 140, 0, 0))
            process = subprocess.Popen([binary, "--home", home, "monitor"], stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
            output = bytearray()

            def drain(seconds=0.3):
                until = time.monotonic() + seconds
                while time.monotonic() < until:
                    if select.select([master], [], [], max(0, until-time.monotonic()))[0]:
                        output.extend(os.read(master, 65536))

            try:
                drain(0.7)
                assert process.poll() is None, output.decode(errors="replace")
                assert b"Snapshot unavailable" in output, output.decode(errors="replace")
                assert b"\x1b[?1049h" in output, "alternate screen not entered"
                config = {"name": "pty-test", "listen": "localhost:0", "rescan_secs": 3600, "folders": [], "peers": []}
                status = {key: 0 for key in ("started", "sent_bytes", "received_bytes", "sent_entries", "received_entries", "send_bytes_per_sec", "receive_bytes_per_sec", "conflicts")}
                status.update(pid=123, updated=int(time.time()), device="test-device", listen="localhost:0", folders={}, connected_peers=[], events=[], daemon_version="test-version")
                Path(home, "config.json").write_text(json.dumps(config))
                Path(home, "status.json").write_text(json.dumps(status))
                expected = {p.name: p.read_bytes() for p in Path(home).iterdir()}
                drain(1.2)
                assert b"LIVE" in output and b"test-version" in output, "missing snapshot did not recover"

                for key in (b"?", b"?", b"\t", b"f", b"/test", b"\r", b"x", b"d", b"\x1b[B", b"d", b" ", b" "):
                    os.write(master, key)
                    drain(0.05)
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 10, 40, 0, 0))
                os.kill(process.pid, signal.SIGWINCH)
                drain()
                assert b"Enlarge" in output, "resize did not render small-terminal fallback"
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
                os.kill(process.pid, signal.SIGWINCH)
                drain()
                if exit_mode == "sigterm":
                    process.send_signal(signal.SIGTERM)
                else:
                    os.write(master, b"q" if exit_mode == "q" else b"\x03")
                drain(0.4)
                assert process.wait(timeout=3) == 0, output.decode(errors="replace")
                assert termios.tcgetattr(slave) == before, "terminal attributes not restored"
                assert b"\x1b[?1049l" in output, "alternate screen not restored"
                assert {p.name: p.read_bytes() for p in Path(home).iterdir()} == expected, "monitor wrote to its state directory"
                print(f"PASS: resize, input, read-only missing-state recovery, {exit_mode} cleanup")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                os.close(master)
                os.close(slave)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    check(str(Path(parser.parse_args().binary).resolve()))
