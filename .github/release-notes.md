Fixes unnecessary filesystem writes during initial reconciliation: entries whose content and permissions already match now merge synchronization history without chmod or file fsync. This removes a source of watcher events, queue overflows, and repeated full scans.

Regression tests cover unchanged file/directory metadata and real permission updates. Wire protocol and state formats are unchanged.

Upgrade with:

```sh
brew update
brew upgrade ysync
```

Or install with Rust 1.88 or newer:

```sh
cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.1.1 --locked ysync
```

Stop the service before upgrading and restart it afterward when ready. Folder pause settings are preserved. This remains experimental: initial backlog, existing-file conflicts, and unreadable paths are not resolved by this patch. Test with copies before enabling important folders.

Release archives cover ARM64/x86-64 macOS and Linux, with SHA256SUMS. Linux archives require glibc 2.35 or newer; macOS binaries are unsigned.
