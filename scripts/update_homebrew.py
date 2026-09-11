#!/usr/bin/env python3
"""Render the Homebrew formula from a published, checksummed release."""
import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent
TARGETS = ("aarch64-apple-darwin", "x86_64-apple-darwin", "aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu")
REPO = "yetidevworks/ysync"


def render(tag, release, checksums):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?", tag):
        raise ValueError("expected a semantic version tag")
    if release["tagName"] != tag or release["isDraft"]:
        raise ValueError("release is unpublished or tag does not match")
    assets = {asset["name"]: asset for asset in release["assets"]}
    manifest = {}
    for line in checksums.splitlines():
        digest, name = line.split()
        name = name.removeprefix("./")
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or name in manifest:
            raise ValueError("invalid or duplicate checksum")
        manifest[name] = digest
    text = (ROOT / "packaging/homebrew/ysync.rb.in").read_text().replace("@VERSION@", tag[1:])
    for target in TARGETS:
        name = f"ysync-{tag}-{target}.tar.gz"
        asset = assets[name]
        digest = manifest[name]
        expected_url = f"https://github.com/{REPO}/releases/download/{tag}/{name}"
        if asset["url"] != expected_url or asset["state"] != "uploaded":
            raise ValueError(f"invalid release asset: {name}")
        if asset.get("digest") and asset["digest"] != f"sha256:{digest}":
            raise ValueError(f"GitHub digest disagrees with SHA256SUMS: {name}")
        text = text.replace(f"@{target}@", digest)
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    release = json.loads(subprocess.check_output([
        "gh", "release", "view", args.tag, "--repo", REPO,
        "--json", "tagName,isDraft,assets"], text=True))
    checksums = subprocess.check_output([
        "gh", "release", "download", args.tag, "--repo", REPO,
        "--pattern", "SHA256SUMS", "--output", "-"], text=True)
    formula = render(args.tag, release, checksums)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(formula)
    print(f"Wrote {args.output} for {args.tag}")


if __name__ == "__main__":
    main()
