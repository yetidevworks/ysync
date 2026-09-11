#!/usr/bin/env python3
"""Package an installed native binary with license, documentation, and version checks."""
import argparse
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
}
ROOT = Path(__file__).resolve().parent.parent


def version():
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"], cwd=ROOT))
    return next(p["version"] for p in metadata["packages"] if p["name"] == "ysync")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-version", action="store_true")
    parser.add_argument("--target", choices=sorted(TARGETS))
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    release_version = version()
    ref = os.environ.get("GITHUB_REF", "")
    if ref.startswith("refs/tags/") and ref != f"refs/tags/v{release_version}":
        raise SystemExit(f"Tag {ref} does not match Cargo.toml version {release_version}")
    if args.check_version:
        print(release_version)
        return
    if not args.target or not args.binary:
        parser.error("--target and --binary are required when packaging")
    binary = args.binary.resolve()
    actual = subprocess.check_output([str(binary), "--version"], text=True).strip()
    if actual != f"ysync {release_version}":
        raise SystemExit(f"Unexpected binary version: {actual}")
    name = f"ysync-v{release_version}-{args.target}"
    args.output.mkdir(parents=True, exist_ok=True)
    archive_path = args.output / f"{name}.tar.gz"
    files = {"ysync": binary, "LICENSE": ROOT / "LICENSE", "README.md": ROOT / "README.md", "CHANGELOG.md": ROOT / "CHANGELOG.md"}
    # Stable archive metadata; this does not promise bit-identical compiler output.
    with archive_path.open("wb") as output:
        with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for filename, source in files.items():
                    data = source.read_bytes()
                    info = tarfile.TarInfo(f"{name}/{filename}")
                    info.size = len(data)
                    info.mode = 0o755 if filename == "ysync" else 0o644
                    archive.addfile(info, io.BytesIO(data))
    print(archive_path)


if __name__ == "__main__":
    main()
