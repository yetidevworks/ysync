# ysync roadmap

## Completed foundation

- [x] macOS/Linux daemon, encrypted direct transport, pinned device identities, approval and per-folder grants.
- [x] Bidirectional indexing and native watchers, periodic reconciliation, conflict preservation, durable batch cursors.
- [x] Parallel hashing and durability flushes, pipelined index batches, terminal monitoring and native service commands.
- [x] Interrupted-file resume: persist receive buffers, independently verify the prefix on both endpoints, reuse only matching bytes, and report reused bytes in the monitor. Restart, corruption, and payload accounting are covered by automated tests.
- [x] Content-defined delta transfers: reuse verified chunks from the existing destination, stream missing chunks, retain atomic publication and prefix resume, and report reused bytes. Compare overwrites, insertions, and deletions against fixed-size blocks; keep manifests and buffers bounded.
- [x] Efficient watcher scheduling: bounded path coalescing/debounce, scoped deletion and directory metadata handling, cached hash reuse, included-directory watches on Linux, automatic watch recovery, configurable safety scans, and scan/hash diagnostics.
- [x] Live-folder scan evaluation: million-entry trees on both machines, measured scoped changes and idle CPU, quota-aware retry delay, and persistent reporting of incomplete full scans. Thermal, combined Linux watch-capacity, and unreadable-file limitations remain open.
- [x] First scan-cost reduction: checked parent-handle traversal, reused SQL statements, live batch-level scan/hash/watch counters, retained partial-work accounting, and cause-first diagnostics. The old plugin-tree error was traced to unreadable mode-000 files; permissions remain unchanged.

- [x] Cooperative scanner cooling: Linux CPU temperature sampling, hysteresis, retained walk/hash progress, visible sensor failures, and prompt stop/manual-pause checkpoints. Opt-in; large-root thermal validation remains.

- [x] Linux watch-capacity handling: read-only index estimates, live shared-limit diagnostics, retained partial coverage on quota exhaustion, and scoped recovery of uncovered subtrees. Raising the shared limit triggers early recovery; real combined-root validation remains.

## Next performance work

Temperature-aware scanner pause/resume is implemented and covered by controlled tests. A scanner CPU/duty budget independent of worker count and a controlled large-root cooling evaluation remain. The first path-opening optimization is measured in VALIDATION.md; larger controlled follow-up remains. Track initial indexing separately from idle watching and scoped edits. Two hash workers and a later 50% CPU quota did not prevent every Linux trial from reaching its 85°C stop threshold. The server deliberately uses a shallow fan curve: keep temperature control optional, and assess efficiency through CPU seconds, disk reads, repeated work, and idle activity.

1. **Multiple transfer lanes with bounded resource use.** Separate large-file traffic from small edits; tune disk and network concurrency independently of hashing. Keep ordering, memory limits, and backpressure explicit.
2. **Single-read initial transfer.** Reduce duplicate source reads while retaining source-change detection and reliable indexing/reconnect behavior.
3. **Persistent chunk indexes and adaptive delta selection.** Avoid repeated basis scans where safe, and measure when delta negotiation costs more than streaming the file.

## Operational work

- Configurable retention and garbage collection for versions, conflicts, and abandoned partial downloads.
- Long-running large-tree evaluation, Linux watch-capacity planning, unreadable-file handling, persistent event-history recovery where supported, and hash reuse across renames.
- Deeper monitoring of per-peer backlog and progress within a single large-file hash.
- Test service install/uninstall and reboot/login behavior on both platforms.
- Metadata fidelity, file/directory conflict handling, discovery, and a web interface.

## Evaluation gate

Live unpaired scanner/watcher trials are recorded in VALIDATION.md. Resolve the observed scan-error, thermal, and watch-capacity issues before enabling both real roots together. Then benchmark isolated copies of representative folders for initial transfer, large-file updates, interrupted transfers, bidirectional conflicts, and long-running stability. Keep exclusions and durability settings comparable. These measurements do not establish a speedup over Syncthing.
