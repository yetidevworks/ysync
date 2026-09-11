# Changelog

## 0.2.1

- Keep encrypted progress messages flowing during receiver index/content checks, durability flushes, scanner-lock waits, and publication, so slow local work does not trip the peer's network read timeout.
- Cancel cooperative receiver reads and lock waits when the connection fails, the folder is paused/revoked, or the daemon stops. Acknowledgements still follow durable publication and index commit.
- Report slow receiver work in activity events and include batch/stage context in request and acknowledgement timeout errors.
- Add isolated regressions for delayed scanner access, database contention, pause, and disconnect. Wire protocol 4 and the 0.2.0 conflict protections are unchanged.

## 0.2.0

- Replace automatic hash-based conflict winners with durable pending conflicts. Independent content, permission/type differences, and edit/delete conflicts never automatically replace the working version.
- Add `conflict list`, explicit `conflict resolve --keep-local`, and pending counts in the monitor. Conflict clocks remain separate until a deliberate resolution.
- Recheck live content during negotiation/publication and publish newly created files without overwriting a destination created in the meantime.
- Require protocol 4 on both peers; reject 0.1.x peers before file batches. Existing identities and indexes are retained, with an added pending-conflicts table. Legacy archives require separate review.

## 0.1.1

- Avoid chmod and file fsync when incoming content and permissions already match. Synchronization history still merges, while unchanged files no longer generate metadata events that can overflow watcher queues and trigger repeated full scans during bootstrap.
- Add regressions for unchanged file/directory metadata and real permission updates.

This fixes one source of scan feedback. Initial-reconciliation backlog, conflict review, and inaccessible paths still require attention on large existing trees.

## 0.1.0

First experimental release for macOS and Linux.

- Encrypted direct connections with explicit peer approval and per-folder grants.
- Bidirectional synchronization, conflict preservation, durable cursors, interrupted-transfer resume, and content-defined delta transfers.
- Native filesystem watching, scoped updates, cached hashes, configurable reconciliation, and bounded parallel hashing.
- Linux partial watch coverage and capacity diagnostics; optional temperature-aware scanner pause/resume.
- Terminal monitoring, JSON status, and launchd/systemd user services.
- Installation from the Git repository with Cargo, plus native archives for both CPU architectures on macOS and Linux.

The multi-million-file live-folder trials remain incomplete. See VALIDATION.md and ROADMAP.md for measured behavior and remaining work. This release is not yet a proven replacement for Syncthing on production trees.
