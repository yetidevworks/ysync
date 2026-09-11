# Changelog

## 0.1.0

First experimental release for macOS and Linux.

- Encrypted direct connections with explicit peer approval and per-folder grants.
- Bidirectional synchronization, conflict preservation, durable cursors, interrupted-transfer resume, and content-defined delta transfers.
- Native filesystem watching, scoped updates, cached hashes, configurable reconciliation, and bounded parallel hashing.
- Linux partial watch coverage and capacity diagnostics; optional temperature-aware scanner pause/resume.
- Terminal monitoring, JSON status, and launchd/systemd user services.
- Installation from the private Git repository with Cargo, plus native archives for both CPU architectures on macOS and Linux.

The multi-million-file live-folder trials remain incomplete. See VALIDATION.md and ROADMAP.md for measured behavior and remaining work. This release is not yet a proven replacement for Syncthing on production trees.
