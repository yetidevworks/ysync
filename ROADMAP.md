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

- [x] Conflict safety: preserve working content for independent/concurrent versions, retain durable pending conflicts, require explicit CLI resolution, and refuse peers using the older automatic conflict policy.

- [x] Ratatui monitoring and guarded conflict review with verified previews and explicit keep-local confirmation (0.2.5–0.2.6).
- [x] Per-peer outbound delivery in the TUI: lane acknowledgements, bounded queue counts, and explicit offline/scanning/conflict states. Counts are logical indexed versions; remote scan/conflict state is not certified.
- [x] Initial pairing workflow: receiver metadata export, merge/seed-local preview, paginated plan inspection, and atomic explicit source baselines (0.3.2). This one-time baseline workflow is separate from the permanent folder policies below.
- [x] Retention controls: dry runs, age/space limits, manual cleanup, and separately opt-in automatic maintenance. Unresolved/uncommitted conflicts and locked partials are protected (0.3.2). Very large archive directories and unsupported legacy artifacts still require manual maintenance.

## Completed in 0.4.0

- [x] Explicit `send-receive`, `send-only` and conservative `receive-only` folder policies; protocol 6 negotiates each folder and lane, with local enforcement before receiving/publication. Existing configurations remain bidirectional.
- [x] CLI policy changes hold the stopped-daemon lock; mode is visible in JSON/TUI and disabled sending is not reported as successful delivery.
- [x] Isolated direction/restart/receiver-divergence tests and deterministic mixed offline edits across repeated process crashes.
- [x] Distinct reproducible benchmark contents, repeated raw trials and latency/CPU observations. See BENCHMARKING.md for limits; no comparative performance win is established.
- [x] Correct README/validation drift about released lanes, caches, retention and historical tests.
- [ ] Receiver local-difference review/revert workflow; forced mirroring remains a separate explicit policy decision.

## Next performance work

Temperature-aware scanner pause/resume is implemented and covered by controlled tests. A scanner CPU/duty budget independent of worker count and a controlled large-root cooling evaluation remain. The first path-opening optimization is measured in VALIDATION.md; larger controlled follow-up remains. Track initial indexing separately from idle watching and scoped edits. Two hash workers and a later 50% CPU quota did not prevent every Linux trial from reaching its 85°C stop threshold. The server deliberately uses a shallow fan curve: keep temperature control optional, and assess efficiency through CPU seconds, disk reads, repeated work, and idle activity.

- [x] Negotiated transfer lanes with bounded resource use (0.3.2). One metadata/small-edit lane and stable bulk lanes; independent durable cursors and parent ordering. Configurable separately from hashing. Controlled blocked-bulk and layout migration tests pass.
- [x] Bounded single-read initial fast path (0.3.2). Reuse scanned payloads up to 8 MiB from a configurable RAM cache. Larger/evicted files still need another read; universal streaming handoff remains future work.
- [x] Persistent chunk indexes and adaptive delta selection (0.3.2). Reuse verified fingerprint-bound signatures and received layouts across restarts; fall back when savings are poor and temporarily skip subsequent probes.
- [ ] Representative LAN/disk benchmarks and long-running evaluation of the new lanes and caches, including independent Linux execution. Separate disk scheduling/rate budgets and wider single-read coverage remain possible follow-ups.
- [ ] Reduce idle protocol/index polling while preserving edit responsiveness. September 12 repeated local fixtures measured about 2–3% of one core per daemon over ten-second idle windows; compare CPU seconds and edit latency before/after, including a full safety-scan interval.

## Operational work

- Improve retention of unsupported/legacy artifacts and add bounded pagination for archive stores exceeding 100,000 directory entries.
- Monitoring polish: automatic conflict-list refresh, grouping by path/version, and exchange of remote scan/conflict state for a combined bidirectional convergence indicator.
- Application-consistent snapshots for live databases.
- Long-running large-tree evaluation, Linux watch-capacity planning, unreadable-file handling, persistent event-history recovery where supported, and hash reuse across renames.
- Progress within a single large-file hash and active lane/cache details.
- Native user-service lifecycle and repeated restart/replication tests now run with isolated jobs in CI. Actual reboot/logout trials, longer soak tests, and Homebrew-specific lifecycle automation remain.
- Metadata fidelity, file/directory conflict handling, discovery, and a web interface.

## Evaluation gate

Scanner/watcher and transfer trials are recorded in VALIDATION.md. The selected Grav, Trilby Media and Yeti Dev Works folders now run on both machines; inherited index-history conflicts were explicitly reconciled to the Mac baseline with Linux originals retained. This establishes the reviewed live baseline, not long-term reliability or a general speedup over Syncthing. Continue isolated representative benchmarks and extended stability testing, including reboot/login behavior, interrupted transfers, concurrent edits, and independent Linux validation of new features. Keep exclusions and durability settings comparable.
