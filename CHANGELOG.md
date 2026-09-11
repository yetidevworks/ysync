# Changelog

## 0.2.5

- Replace `ysync monitor` with a responsive Ratatui dashboard: payload sparklines, selectable folders, expanded watcher/device diagnostics, keyboard navigation, and searchable/filterable activity. Keep `monitor --plain`, `status`, and `status --json` available.
- Label incoming reconciliation records as index activity rather than file transfers. Bound index events to 16 of the 64 recent events so they do not flood out other activity; cumulative counters remain unchanged.
- Show daemon and monitor versions separately, suppress stale rates, reset chart history on daemon restart, and restore the terminal on exit, error, panic, and handled termination signals. Monitoring remains read-only and does not scan sync trees.

## 0.2.4

- Do not create missing parent directories when applying historical deletions at a fresh receiver. The old behavior could invent empty directories and create false directory/deletion conflicts.
- Compare symbolic links by target rather than platform-specific permission bits. File and directory permissions still participate in conflict detection; differing symlink targets remain conflicts.
- Add regressions for fresh bootstrap across multiple tombstone batches, absence of phantom parents, unchanged symlink inodes, stable rescans, and conflicting link targets. Existing recorded conflicts are retained for deliberate resolution.

## 0.2.3

- Flush sibling directories concurrently with a reusable pool capped at eight disk workers. Finish each deeper level before its parents, and finish all file/directory flushes before acknowledging or committing the cursor. Failed flushes join in-flight work and abort acknowledgement.
- Reuse the same bounded pool for file-content flushes instead of creating new threads every batch. Scan worker settings remain independent.
- Add SSH benchmark options for the destination filesystem and directory shape; use hourly reconciliation to measure native watcher behavior without five-second rescan interference.

## 0.2.2

- Match canonically equivalent Unicode filenames and parent directories to their existing local spelling before receive negotiation, publication, and durability checks. This fixes macOS/Linux composed-versus-decomposed filename stalls without renaming working files.
- Keep case-only collisions, duplicate incoming path keys, and distinct physical Unicode aliases rejected; retain normal conflict preservation. Bound the prefix lookup metadata and preserve request positions for ignored entries.
- Add bidirectional Unicode filename/parent tests, conflict-preservation tests, and a Linux regression for distinct canonical aliases. Wire protocol remains 4; upgrade both peers for matching behavior in both directions.

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
