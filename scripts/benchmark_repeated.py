#!/usr/bin/env python3
"""Repeat isolated ysync trials; preserve raw samples and avoid cold-cache claims."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time


def distribution(values):
    ordered = sorted(values)
    return {"samples": values, "min": min(values), "median": statistics.median(values),
            "p95_nearest_rank": ordered[math.ceil(.95 * len(values)) - 1], "max": max(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--files", type=int, default=2000)
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--scan-workers", type=int, default=2)
    parser.add_argument("--edit-samples", type=int, default=20)
    parser.add_argument("--idle-seconds", type=float, default=10)
    parser.add_argument("--one-way", action="store_true")
    args = parser.parse_args()
    if args.trials < 2:
        parser.error("at least two trials are required")
    runner = Path(__file__).with_name("benchmark.py")
    report = {
        "schema": 1, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "runner_sha256": hashlib.sha256(runner.read_bytes()).hexdigest(),
        "method": "Sequential isolated loopback trials; distinct deterministic payloads; same seed each trial; source generation warms OS cache; no cache eviction; existing workloads remain active.",
        "limitations": ["Not a competitor comparison or a LAN benchmark", "No physical disk-I/O measurement",
                        "CPU snapshots use ps precision; short idle windows do not cover hourly safety scans",
                        "Latency samples include 10 ms polling and tiny replacements of one existing file",
                        "Small sample p95 estimates do not establish tail-latency guarantees"],
        "trials": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ysync-repeated-report-") as tmp:
        for trial in range(args.trials):
            output = Path(tmp) / f"{trial}.json"
            command = [sys.executable, str(runner), "--binary", args.binary, "--output", str(output)]
            for option in ("files", "size", "scan_workers", "edit_samples", "idle_seconds"):
                command += ["--" + option.replace("_", "-"), str(getattr(args, option))]
            if args.one_way:
                command.append("--one-way")
            subprocess.run(command, check=True)
            report["trials"].append(json.loads(output.read_text()))
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    report["summary"] = {key: distribution([trial[key] for trial in report["trials"]]) for key in (
        "bootstrap_seconds", "files_per_second", "payload_MB_per_second")}
    report["summary"]["edit_latency_ms"] = distribution([sample for trial in report["trials"] for sample in trial["edit_latency_samples_ms"]])
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
