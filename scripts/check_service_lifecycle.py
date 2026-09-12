#!/usr/bin/env python3
"""Exercise generated user-service definitions with disposable identities and roots.

Installs only a unique ysync-check-* job; never controls the normal ysync service.
Requires the current user's launchd GUI domain or systemd user manager.
This tests login enablement, not an actual machine reboot or logout.
"""
import argparse
import json
import os
from pathlib import Path
import plistlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid


def run(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True)


def wait(label, predicate, timeout=75):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(0.2)
    raise AssertionError(f"Timed out: {label}")


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def replace(path, data):
    # Only complete versions are exposed to either scanner.
    with tempfile.NamedTemporaryFile(dir=path.parent / ".ysync/tmp", delete=False) as f:
        f.write(data)
        temporary = f.name
    os.replace(temporary, path)


def check(binary, cycles, output):
    platform = "macos" if sys.platform == "darwin" else "linux"
    name = "ysync-check-" + uuid.uuid4().hex
    label = "dev.yetidevworks." + name
    domain = f"gui/{os.getuid()}"
    if platform == "macos":
        run("launchctl", "print", domain)
        unit = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
    else:
        run("systemctl", "--user", "show-environment")
        unit = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "systemd/user" / f"{name}.service"
    started = time.monotonic()
    results = []
    created = False
    other = None
    success = False
    # Spaces, XML metacharacters and systemd substitutions exercise real escaping.
    with tempfile.TemporaryDirectory(prefix="ysync service %$ & ") as temporary:
        base = Path(temporary)
        states = [base / "a-state", base / "b-state"]
        roots = [base / "a-files", base / "b-files"]
        for root in roots:
            root.mkdir()

        def cli(index, *args):
            return run(binary, "--home", str(states[index]), *args).stdout.strip()

        def status(index):
            return json.loads((states[index] / "status.json").read_text())

        def start():
            if platform == "macos":
                run("launchctl", "bootstrap", domain, str(unit))
            else:
                run("systemctl", "--user", "start", unit.name)

        def stop():
            if platform == "macos":
                run("launchctl", "bootout", f"{domain}/{label}", check=False)
            else:
                run("systemctl", "--user", "stop", unit.name, check=False)

        def pid(previous=0):
            s = status(0)
            value = s["pid"]
            if value == 0 or value == previous or time.time() - s["updated"] > 5:
                return False
            # Do not signal an unverified PID from a stale/reused snapshot.
            command = run("ps", "-p", str(value), "-o", "command=", check=False).stdout
            return value if str(states[0]) in command and "serve" in command else False

        def converged():
            for index in (0, 1):
                s = status(index)
                folder = s["folders"].get("check", {})
                if time.time() - s["updated"] > 5 or folder.get("phase") != "watching" or folder.get("pending_conflicts", 0):
                    return False
                delivery = s.get("delivery", {}).get(ids[1-index], {}).get("check", {})
                pending = delivery.get("pending") or {}
                acknowledged = delivery.get("acknowledged", [])
                if not acknowledged or delivery.get("active_lanes") != len(acknowledged):
                    return False
                if not pending.get("complete") or pending.get("entries") != 0:
                    return False
                if not all(n is not None and n >= delivery["local_head"] for n in acknowledged):
                    return False
            return True

        try:
            for index in (0, 1):
                cli(index, "init", "--listen", f"127.0.0.1:{port()}", "--scan-workers", "2")
                cli(index, "folder", "add", "check", str(roots[index]))
            ids = [cli(index, "id") for index in (0, 1)]
            for index in (0, 1):
                path = states[index] / "config.json"
                cfg = json.loads(path.read_text())
                remote = json.loads((states[1-index] / "config.json").read_text())
                cfg["peers"] = [dict(id=ids[1-index], name="isolated-peer", approved=True, folders=["check"], address=remote["listen"] if index == 0 else None)]
                cfg["rescan_secs"] = 3600
                path.write_text(json.dumps(cfg))
            initial = os.urandom(2 * 1024 * 1024)
            (roots[0] / "payload.bin").write_bytes(initial)
            (roots[0] / "link").symlink_to("payload.bin")
            body = cli(0, "service", "print", "--platform", platform)
            if platform == "macos":
                spec = plistlib.loads(body.encode())
                assert spec["RunAtLoad"] and spec["KeepAlive"]
                assert spec["ProgramArguments"] == [binary, "--home", str(states[0]), "serve"]
                spec["Label"] = label
                body = plistlib.dumps(spec).decode()
            else:
                assert "WantedBy=default.target" in body and "Restart=on-failure" in body
            unit.parent.mkdir(parents=True, exist_ok=True)
            with unit.open("x") as file:
                file.write(body)
            created = True
            if platform == "linux":
                run("systemctl", "--user", "daemon-reload")
                run("systemctl", "--user", "enable", "--now", unit.name)
                assert run("systemctl", "--user", "is-enabled", unit.name).stdout.strip() == "enabled"
            else:
                run("plutil", "-lint", str(unit))
                start()
            with (states[1] / "service.log").open("w") as log:
                other = subprocess.Popen([binary, "--home", str(states[1]), "serve"], stdout=log, stderr=log)
            current = wait("service launch", pid)
            wait("initial payload and symlink", lambda: (roots[1] / "payload.bin").read_bytes() == initial and os.readlink(roots[1] / "link") == "payload.bin")
            wait("initial durable delivery", converged)
            duplicate = run(binary, "--home", str(states[0]), "serve", check=False)
            assert duplicate.returncode != 0 and "already running" in duplicate.stderr
            assert pid() == current
            results.append("installed, login-enabled, initial replication, duplicate daemon refused")

            for cycle in range(cycles):
                cycle_start = time.monotonic()
                previous = wait("owned service PID", pid)
                if cycle % 3 == 2:
                    os.kill(previous, signal.SIGKILL)
                    mode = "crash/restart"
                else:
                    stop()
                    wait("clean stop", lambda: status(0)["pid"] == 0)
                    mode = "stop/start"
                payload = initial[:100_003] + f"cycle-{cycle:04}".encode() + initial[100_013:]
                replace(roots[1] / "payload.bin", payload)
                if mode == "stop/start":
                    start()
                wait("new supervised PID", lambda: pid(previous))
                wait("edit made while service was down", lambda: (roots[0] / "payload.bin").read_bytes() == payload)
                wait("delivery after restart", converged)
                note = f"reverse edit {cycle}".encode()
                replace(roots[0] / "note.txt", note)
                wait("reverse replication", lambda: (roots[1] / "note.txt").read_bytes() == note)
                os.chmod(roots[0] / "note.txt", 0o640)
                wait("permission preservation", lambda: (roots[1] / "note.txt").stat().st_mode & 0o777 == 0o640)
                wait("metadata delivery", converged)
                (roots[0] / "note.txt").unlink()
                wait("deletion", lambda: not (roots[1] / "note.txt").exists())
                wait("final cycle delivery", converged)
                assert [cli(index, "id") for index in (0, 1)] == ids
                results.append(dict(cycle=cycle + 1, mode=mode, seconds=round(time.monotonic() - cycle_start, 2)))
                print(f"PASS cycle {cycle+1}/{cycles}: {mode}, offline edit, reverse edit, permissions, deletion, delivery", flush=True)
            assert any(p.is_file() and p.suffix != ".json" and p.read_bytes() == initial for p in (roots[0] / ".ysync/versions").iterdir())
            results.append("original payload retained in archive; identities preserved")
            success = True
        finally:
            cleanup_error = None
            try:
                if created:
                    stop()
                    if platform == "linux":
                        run("systemctl", "--user", "disable", unit.name, check=False)
                    unit.unlink(missing_ok=True)
                    if platform == "linux":
                        run("systemctl", "--user", "daemon-reload")
                        assert not (unit.parent / "default.target.wants" / unit.name).is_symlink()
                    else:
                        # bootout requests asynchronous removal from launchd.
                        wait("launchd job removed", lambda: run("launchctl", "print", f"{domain}/{label}", check=False).returncode != 0, timeout=15)
            except Exception as error:
                cleanup_error = error
            if other is not None and other.poll() is None:
                other.terminate()
                try:
                    other.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    other.kill()
                    other.wait()
            report = dict(success=success and cleanup_error is None, platform=platform, cycles=cycles, seconds=round(time.monotonic()-started, 2), checks=results, unit_removed=not unit.exists(), actual_reboot_tested=False)
            if output:
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps(report, indent=2) + "\n")
                for index in (0, 1):
                    log = states[index] / "service.log"
                    if log.exists():
                        output.with_suffix(f".device-{index}.log").write_bytes(log.read_bytes())
            print(json.dumps(report), flush=True)
            if cleanup_error:
                raise cleanup_error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 100:
        parser.error("cycles must be between 1 and 100")
    check(str(Path(args.binary).resolve()), args.cycles, args.output)
