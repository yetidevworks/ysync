# Evaluation results

This is a chronological evidence log. Counts, versions, deployment state and unresolved issues in older sections describe those trials, not the current release. Later sections supersede earlier observations; short successful trials do not establish long-term reliability. Reproducible comparison requirements are in [BENCHMARKING.md](BENCHMARKING.md).

## September 10, 2026 baseline

This is an experimental implementation. Earlier transfer tests used disposable folders; the live-folder scanner evaluation below used the actual Projects/workspace roots with no peers or transfers. Existing content was only read; ysync markers/ignore files and temporary probe files were created. Syncthing was stopped before the live evaluation. No persistent ysync service was installed; capped Linux trials used temporary systemd jobs that were stopped afterward.

## Correctness checks

| Environment | Result |
| --- | --- |
| macOS ARM64, Rust 1.98 | Latest source: 33 unit tests and 11 integration tests passed; native FSEvents required |
| `home-omarchy`, Linux x86_64 | Latest source: 36 unit tests and 11 integration tests passed; native inotify required. Tests ran sequentially on small isolated fixtures |
| Isolated Linux container, x86_64 | Prior delta build: 8 integration tests passed; the latest watcher changes were checked on the actual Linux server |
| Formatting and lint | `cargo fmt --check` and `cargo clippy --all-targets --locked -- -D warnings` passed |
| Service definitions | macOS `plutil -lint` and Linux `systemd-analyze --user verify` accepted generated definitions |

Integration tests use separate daemon processes and real encrypted TCP sessions. They cover pending approval, fingerprint checks, folder grants, pause/resume, revocation, transfers in both directions, native watcher changes, empty directories, relative and absolute symlinks, ignored paths, deletion across batches, offline concurrent edits, durable restart cursors, simultaneous dialing, and a receiver killed during a file transfer. The interrupted payload is never exposed as a completed destination file.

Unit regressions include lost root markers, traversal and symlink-parent boundaries, unsupported paths during scanning, and directory disappearance between enumeration and hashing. That last race must publish child deletions before their parent directory deletion.

Resume regressions verify that a killed receiver retains a partial download and receives exactly the missing byte count after restart. A deliberately corrupted prefix triggers a full retransmission with correct final content. A sender-disconnect regression also exercises the live receiver's error cleanup path, checking that the partial survives and no payload bytes are duplicated after reconnect. Unit tests cover complete and empty prefixes, invalid offsets, content-version isolation, exclusive buffer ownership, readonly staging recovery, and rejection of symlink/hard-link cache entries.

Delta integration tests edit a 16 MiB file in both directions, including an overwrite, an unaligned insertion, and a deletion. Each edit must converge with less than 1 MiB of new payload, with payload plus reused bytes exactly accounting for the destination length. Another test kills a receiver during a 32 MiB delta assembly: the previous destination remains intact, and restart combines prefix resume with delta reuse. Unit tests reject changed/truncated copy sources, malformed signatures/plans, out-of-bounds copies, gaps, invalid hashes, and excessive block counts; they also check empty files, low-entropy content, and read-fragmentation independence.

Watcher tests coalesce 100,000 duplicate notifications into one path, bound unique-path overflow, preserve recovery signals, and check that directory metadata notifications and unchanged files do not rehash content. Scoped subtree deletion must leave unrelated paths untouched and journal children before parents. Integration tests exercise write bursts, deletion, populated-directory rename, subsequent edits under the new name, and ignored-tree churn with no extra full scans. Linux-specific coverage verifies selective watch registration and automatically recovers from an injected directory-permission failure without restarting the daemon. That recovery test ran as the unprivileged user on the actual server.

At this September 10 baseline, service installation, login/reboot behavior, power-loss recovery, and long-duration operation had not been validated. Later service tests cover installation and process restart recovery; actual reboot/power-loss and long-duration evaluation remain separate gaps. The live multi-million-entry evaluation below identified unresolved operational limits. Passing these tests is not a security audit or proof of production readiness.

## Live Projects/workspace scanner evaluation

The actual roots on the Mac and `home-omarchy` used the 34 converted Syncthing exclusions, two hash workers, nice level 10, an hourly safety scan, isolated state under `~/.cache/ysync-live-eval-20260910`, loopback listeners, and no approved peers. Only uniquely named 1 MiB probe files were created/edited/deleted. The runner sampled process CPU time and Linux CPU sensors, stopping trials at 85°C. These are single trials on active trees, not a controlled comparison with Syncthing.

| Folder/host | Indexed files | Scan observation | 200-write burst detection | Deletion detection |
| --- | ---: | --- | ---: | ---: |
| Mac Projects | 1,575,610 | First pass ended after about 18.6 min with a deep-path error; accessible entries indexed | 0.235 s | 0.214 s |
| Mac workspace | 1,826,161 | First pass reached watching after about 21.4 min; both roots scanned concurrently | 0.233 s | 0.120 s |
| Linux workspace, alone | 1,825,395 | Warm metadata reconciliation: 241.875 s at 50% of one core; zero files rehashed | 0.204 s | 0.202 s |
| Linux Projects, alone | — | Warm scan stopped at 85.25°C despite a 50% CPU cap | Not completed | Not completed |

Each completed burst test checked and hashed exactly one file. Each deletion checked one path, hashed zero files, and caused no additional full scan. The 15-second Mac observation used 0.796% of one core while four unrelated workspace files changed (88.8 MB hashed); Projects did no scanning during that window. Linux workspace used 0.199% of one core with zero checked entries and zero hashing. These CPU figures cover the entire daemon and are limited by OS accounting resolution.

Operational findings:

- The first uncapped Linux two-folder run stopped after 376.2 s at 85.25°C, having checked roughly 3.6 million entries. Two hash workers did not bound sustained CPU temperature. A 50% CPU quota reduced typical temperatures, but the later Projects-only run still hit the cutoff; CPU quota alone is not a guaranteed thermal limit. The successful warm workspace trial peaked at 80.75°C and cooled after scanning.
- Linux workspace required 418,248 native watch registrations. Both roots together exhausted the shared 524,288 quota even with Syncthing stopped. No kernel limits or ignore rules were changed to conceal that limit; Linux roots were then evaluated separately.
- Quota exhaustion previously led to another full coverage-building scan after a short retry. The new source waits at least the safety-scan interval for this specific error while preserving ordinary transient-error recovery. Explicit pause/resume can request an earlier retry.
- Mac Projects reported an incomplete full scan in the deeply nested old `qubwa-dev.old` plugin tree. Later scoped successes cleared the warning in the measured build, so the initial `files: 0` status and `watching` phase were misleading. File totals above were queried from the stopped index. The new source retains the full-scan warning and shows `incomplete` until a full reconciliation succeeds. The deep-path issue itself remains unresolved.
- Mac sampling showed substantial time in filesystem path opening and reads. Its first pass overlapped normal activity, a brief CPU profile, and a small local build. Linux warm timings reuse an index from the interrupted runs and cannot be compared directly to Mac first-index timings.

The measured binaries preceded the quota-retry and health-reporting fixes. Those fixes have regression coverage and refreshed release artifacts; the full live-folder experiment was not repeated afterward. All evaluation daemons and temporary Linux services were stopped, and probe files removed. State/indexes were retained outside the synced roots for future evaluation. No bidirectional synchronization was enabled.

Runner: `scripts/benchmark_live_watch.py`. Raw measurements, aborted runs, binary fingerprints, and caveats: [live-folders-20260910.json](benchmarks/live-folders-20260910.json). `--resume` reuses the evaluation index and pauses registered folders not selected by `--root`; `--cpu-quota 50` uses a temporary Linux service. Never point it at a paired operational state directory.

## Scanner path and database optimization

A subsequent change replaced repeated parent-prefix resolution with a checked directory-handle traversal, reused that handle for file observation, and cached the hot SQLite statements. Scan/hash/watch counters now refresh after indexing batches and preserve work done before an error. A regression verifies that cached files still reject symlink parents and recognize directory replacement; the incomplete-scan integration test also verifies retained checked-entry counts.

Before/after runs used the same synthetic tree: 6,000 files of 4 KiB, 60 branches each nine directory levels deep, and two hash workers. Each version first built a fresh index, then restarted for a warm reconciliation. Filesystem caches were warm. Results are single trials and include daemon startup and roughly one-second status refreshes, which dominate the Linux wall times.

| Host and operation | Before wall | After wall | Before CPU time | After CPU time |
| --- | ---: | ---: | ---: | ---: |
| Mac initial indexing | 3.098 s | 2.076 s | 4.02 s | 1.75 s |
| Mac warm reconciliation | 2.067 s | 1.053 s | 2.91 s | 1.05 s |
| Linux initial indexing | 1.054 s | 1.055 s | 0.47 s | 0.45 s |
| Linux warm reconciliation | 1.055 s | 1.054 s | 0.32 s | 0.34 s |

Mac CPU time fell approximately 56% for initial indexing and 64% for warm reconciliation in this fixture. Linux performance was essentially unchanged at this measurement resolution. Both warm reconciliations hashed zero files. This does not establish the improvement on the complete live roots or solve their thermal/watch-capacity constraints. Measured candidates preceded final display-only changes and removal of unnecessary handle duplication for root-level files; raw binary fingerprints are retained with the results.

A targeted read-only diagnostic of the failed Mac plugin subtree checked 389 entries in 0.569 s and reported `Permission denied (os error 13)`. Three sampled failing regular files had mode `000`, owner UID 501, and failed `os.access(R_OK)`. The failure is unreadable content, rather than an established path-length bug. No source permissions or exclusions were changed. `cargo run --release --example scan_path -- STATE_DIR FOLDER_ID RELATIVE_SUBTREE` scans a specified non-root subtree into a disposable index without connecting peers or modifying source content.

Runner: `scripts/benchmark_scan_paths.py`. Raw data: [path-scan-optimization.json](benchmarks/path-scan-optimization.json). The real roots were not rescanned for this follow-up; only the small synthetic trees and the specific failing subtree were read.

## Linux watch-capacity follow-up — September 11

Read-only `watch-capacity --json` on the Linux evaluation index reports:

| Folder | Cached directory-watch estimate |
| --- | ---: |
| Projects | 235,211 |
| workspace | 418,248 |
| Both | 653,459 |
| Shared user limit | 524,288 |

The combined estimate already exceeds the limit by 129,171. Projects' incomplete scan can undercount demand. Workspace's cached estimate matches the earlier measured 418,248 registrations. The report's 25% headroom calculation suggests at least 851,968; 1,048,576 would provide additional room for incomplete estimates and other applications. No kernel limits, fan settings, or real-folder enablement were changed. The SSH account requires a sudo password, so the limit change needs an administrator. The diagnostic queried existing SQLite directory records and current exclusions; it did not walk or hash source trees. Raw report: [watch-capacity-linux-20260911.json](benchmarks/watch-capacity-linux-20260911.json).

Quota exhaustion now preserves successfully registered watches, records uncovered subtrees, and exposes partial coverage separately from scan health. Recovery targets the uncovered scopes; a higher watch limit triggers an early retry, while unchanged limits use the normal reconciliation interval. Overlapping recovery/event scopes are coalesced. Generic watcher failures retain the previous polling fallback.

Linux native-watcher regressions inject a three-watch budget inside the test watcher without consuming the real user quota. They verify live events from covered paths during exhaustion, recovery visiting only the missing subtree, events after recovery, and pruning a removed gap without dropping unrelated gaps. These are small controlled fixtures, not validation of both complete real roots under load.

## Cooperative scanner cooling

Optional Linux temperature control now pauses enumeration and hashing with 5°C hysteresis and retains the current walk and partial hash in memory. Config changes and manual pause are read about once per second; stop wakes waiting workers immediately. Deterministic tests verify mid-hash continuation and cancellation, a 300-file walk resuming without repeated directory visits or hashes, database writer availability during cooling, deletion-inference suppression after cancellation, hottest-CPU sensor selection, missing/invalid sensor behavior, and hysteresis. A macOS process-level test verifies the explicit unsupported-sensor pause and live configuration recovery.

A release-binary probe on `home-omarchy` used two tiny files with no peers. Moving the configured pause threshold from 40°C to 75°C around the real 54.6–55.4°C readings paused and resumed the initial scan with `full_scans = 1`; it then queued a scoped edit during cooling and terminated cleanly in 0.031 s. No heat load was generated to cross a threshold. This checks integration with actual sensors, not sustained thermal behavior under a large workload. Runner: `scripts/evaluate_thermal.py`. Raw results: [thermal-control-20260910.json](benchmarks/thermal-control-20260910.json).

The Linux machine intentionally uses a shallow fan curve. Temperature is diagnostic context, not the efficiency score. Cooling is an optional operational guardrail and remains disabled in existing evaluation configurations. It does not establish a performance improvement or a hard temperature ceiling. Subsequent performance comparisons should report CPU seconds, disk reads, scan counts, hash reuse, change latency, and idle CPU with comparable workloads. No fan or machine-wide thermal settings were changed.

## Network measurements

The bootstrap measurements in this section used the initial protocol 1 build, before interrupted-file resume and delta transfer were added. Transfers used the Mac and `home-omarchy`, with server test roots under disk-backed `~/.cache` rather than `/tmp` (which is tmpfs on this server). Every received payload was independently SHA-256 verified. Existing workloads remained active. These are single trials; bootstrap timing includes daemon startup and polling the receiver's committed index over SSH.

| Dataset | Scan workers | Bootstrap | Payload throughput |
| --- | ---: | ---: | ---: |
| 5,000 × 4 KiB files | 1 | 7.100 s | 2.88 MB/s; 704 files/s |
| 5,000 × 4 KiB files | 8 | 7.118 s | 2.88 MB/s; 702 files/s |
| 64 × 4 MiB files | 8 | 5.005 s | 53.63 MB/s |

Small-file results show no measurable benefit from extra hashing workers in this workload. Metadata operations, durable publication, and other serial work can dominate when each file takes very little CPU time to hash. This is not a benchmark against Syncthing.

Raw results: [one worker](benchmarks/lan-1-worker.json), [eight workers](benchmarks/lan-8-workers.json), [larger files](benchmarks/lan-large-files.json). Reproduce with `scripts/benchmark_ssh.py --help`. The two small-file trials preceded the final directory-deletion and absolute-symlink regression fixes; the larger-file trial used the final protocol 1 build.

## Where parallelism helps

The scanner benchmark hashes and indexes 64 × 16 MiB files (1 GiB total), pre-read into the Mac's filesystem cache. Each worker count uses a fresh index. It excludes the network and receiver writes, and uses one trial per setting.

| Workers | Scan/hash/index duration | Throughput |
| ---: | ---: | ---: |
| 1 | 0.558 s | 1.92 GB/s |
| 2 | 0.272 s | 3.95 GB/s |
| 4 | 0.138 s | 7.78 GB/s |
| 8 | 0.075 s | 14.30 GB/s |
| 16 | 0.067 s | 16.05 GB/s |

Eight workers improved this CPU-heavy cached scan by about 7.4×. Sixteen gave a smaller additional gain. These figures do not predict cold-disk or end-to-end synchronization speed. The default pool uses at most eight workers, and durability flushes also run concurrently in bounded batches. Enumeration, index publication, and transfer overlap.

Reproduce with `cargo bench --bench scanner --locked`. Raw data: [scanner-warm-cache.json](benchmarks/scanner-warm-cache.json).

## Delta algorithm comparison

This synthetic comparison uses a deterministic 32 MiB random-content fixture held in memory. It hashes both versions and plans the transfer; it excludes network, encryption, and receiver writes/fsync. Values below count literal file bytes separately from metadata.

| Change | Content-defined literal bytes | Fixed 64 KiB block literal bytes |
| --- | ---: | ---: |
| 4 KiB overwrite | 134,250 | 65,536 |
| 19-byte insertion near the start | 21,645 | 33,554,451 |
| 4 KiB deletion | 213,589 | 25,554,944 |

Basis signatures plus the copy/literal plan add approximately 87 KB of JSON per edit, before framing and encryption overhead. Scanning both versions and planning took about 0.145 seconds in each single trial. Content-defined boundaries preserve matches after offsets shift; fixed blocks were cheaper for this overwrite. This is a payload/algorithm comparison, not a general throughput win. Delta mode adds local reading and hashing, which may outweigh its network savings on some workloads.

Reproduce with `cargo bench --bench delta --locked`. Raw data: [delta-synthetic.json](benchmarks/delta-synthetic.json). The SSH harness supports `--delta --files 1 --size 33554432` for an isolated end-to-end check using a 32 MiB test file.

## Delta transfers between the Mac and server

The packaged protocol 3 release binaries also completed three sequential changes to an isolated 32 MiB random-content file, from this Mac to `home-omarchy`. The receiver stored test data under disk-backed `~/.cache`, and each published result passed an independent SHA-256 check. Existing workloads remained active.

| Change | New payload | Reused destination bytes | Observed completion |
| --- | ---: | ---: | ---: |
| 4 KiB overwrite | 262,144 B | 33,292,288 B | 0.827 s |
| 19-byte insertion | 36,016 B | 33,518,435 B | 1.403 s |
| 4 KiB deletion | 107,813 B | 33,442,542 B | 0.872 s |

Payload counts exclude signatures, plans, framing, and encryption overhead. Completion timings include SSH probes, independent hashing, and waiting for the monitoring snapshot to refresh; they are not precise watcher-latency measurements. These are single trials on generated data, not the pending real-folder benchmark or a comparison against Syncthing. Initial transfer of this one file took 2.092 seconds including startup and probes. Raw data: [delta-lan.json](benchmarks/delta-lan.json).

## Server watcher limit

During evaluation, the server's per-user `fs.inotify.max_user_watches` limit was 524,288. The Syncthing worker held 524,082 watches in one inotify instance, over 99.9% of that budget. Additional native watchers failed with an exhausted-limit error. This explains the ysync watcher fallback; it does not establish the cause of Syncthing's initial-transfer speed.

The original protocol 1 SSH benchmark explicitly used a five-second reconciliation interval, so its reverse-edit latencies of roughly 2.9–4.9 seconds reflect polling plus connection/probe overhead. They are not native watcher latency measurements. New ysync configurations now default to hourly safety scans; existing configurations retain their saved interval. The monitor labels fallback as `polling`, and native watch registration is retried with backoff. No kernel limits were changed.

After the user stopped Syncthing, the server had no Syncthing processes, 190 visible watch entries, and 0.3% aggregate CPU activity in a one-second sample. The latest watcher tests could therefore require native inotify coverage instead of relying on polling.

## Watcher work and idle CPU

The packaged release binaries were measured with one isolated daemon, 10,000 small files in 100 data directories, and 1,000 ignored directories under `node_modules`. Test roots were disk-backed on both hosts. A one-hour safety-scan interval kept periodic scans outside this short experiment. There were no peers or network transfers.

| Measurement | Mac | Linux server |
| --- | ---: | ---: |
| Native registrations | 1 FSEvents root stream | 103 inotify directory watches |
| Entries checked during four seconds idle | 0 | 0 |
| CPU time recorded during idle | 0.01 s | 0.00 s |
| Approximate share of one core while idle | 0.25% | Below measurement resolution |
| Paths checked / files hashed after 200 writes to one file | 1 / 1 | 1 / 1 |
| Paths checked / files hashed after one deletion | 1 / 0 | 1 / 0 |
| Extra full scans during the experiment | 0 | 0 |
| Entries checked after ignored-tree churn | 0 | 0 |

Linux registrations cover the root, the marker-control directory, `src`, and its 100 data directories. Ignored subdirectories consume no Linux watches. macOS filters ignored events from its root stream. CPU accounting includes the entire daemon, uses OS process-time resolution, and is only a four-second sample. This does not demonstrate zero CPU use, thermal behavior, or million-file scalability. Initial indexing still reads and hashes the dataset; periodic reconciliation still checks metadata across the tree.

Reproduce with `python3 scripts/benchmark_watch.py --binary /absolute/path/to/ysync --base-dir /disk-backed/test-parent`. Raw results: [watcher-macos.json](benchmarks/watcher-macos.json), [watcher-linux.json](benchmarks/watcher-linux.json). The real-folder evaluation remains outstanding.

## Packaged artifacts

`dist/ysync-macos-arm64` and `dist/ysync-linux-x86_64` are release builds, with checksums in `dist/SHA256SUMS`. The Linux build was cross-compiled with `cargo zigbuild` and exercised on the actual server. The tested Linux executable also remains at `/home/rhuk/.cache/ysync-eval.caGcvu/ysync`; it is not registered as a service.

Those historical packaged binaries used wire protocol 3; both peers needed that update. Identities, configuration, indexes, and partial-buffer formats remain compatible. Delta reuse is scoped to the existing destination file, with signatures generated on demand. See the README for setup and limitations, including no cross-file deduplication or persisted chunk index, unbounded retained versions/obsolete partials, and unsupported metadata. See [ROADMAP.md](ROADMAP.md) for the next work.

That watcher update kept wire protocol 3 and could be deployed independently on an existing protocol 3 peer. The delta timing measurements above predate these watcher scheduling changes.


## 0.2.0 conflict safety

The macOS suite passes 43 unit tests and 11 integration tests with native watchers required. Regression coverage includes independently indexed content, stale versions, concurrent edits and permissions, deletion versus an unscanned edit, edits during transfer and payload negotiation, and atomic creation when another application creates the destination first. The end-to-end test leaves both offline edits in place, verifies durable pending conflicts and archived incoming bytes, rejects resolution while the daemon runs, and uses the CLI to explicitly choose a version that then converges on both peers.

A separate process probe connected the installed 0.1.1 binary to the new 0.2.0 binary using isolated roots. Wire protocol 3 was refused by protocol 4 before any entries transferred, and both pre-existing files remained unchanged. Both real Projects roots remained paused, with their services stopped, throughout these checks. This does not retroactively resolve earlier conflicts or demonstrate completion of the real-folder sync.

Automatic replacement now requires causal version history; independent differences require an explicit choice. Destination checks catch changes before publication, and new regular files use atomic creation without replacement. These checks do not provide application-consistent snapshots or eliminate every race with applications writing through existing open file handles. Earlier conflict archives remain available for separate review.


## 0.2.1 receiver timeout reproduction

A monitored 0.2.0 Projects trial moved 76,072,127 payload bytes from Mac to Linux and processed 47,744 received index entries on each side before transfers stalled. Scanning continued, and connection errors included macOS `Resource temporarily unavailable`, resets, and Linux `device already connected`. No watcher overflows were recorded. Both services were stopped and Projects paused after approximately three minutes. The highest sampled Linux CPU temperature was 77.125 C; this was not continuous temperature sampling. Pending conflicts were retained (24 Mac, 8 Linux), and the original `test.text` had not reached Linux.

An isolated encrypted-transfer regression holds the receiver's folder gate for four seconds with a two-second peer read timeout. The old receiver fails with the same macOS error before receiving its durable acknowledgement. The patched receiver succeeds without publishing a file or advancing its cursor while the gate is held. Further regressions hold an SQLite write transaction during preflight and verify that folder pause and peer disconnect cancel a receiver still waiting for the gate, leaving its destination and cursor untouched.

The local macOS suite passes 47 unit and 11 integration tests with native watchers required. This reproduces and fixes a timeout mechanism consistent with the live failure; a further monitored large-tree trial is needed to assess remaining bottlenecks. The fix does not reduce the underlying scan/hash workload or automatically resolve platform-specific symlink conflicts.


## 0.2.2 canonical Unicode paths

The resumed 0.2.1 real-folder trial passed its earlier timeout position. In `yetidevworks`, test files arrived in both directions, the user's moved `test.text` matched its original SHA256 on Linux, and follow-up edits were observed in both directions after approximately 1.2 seconds including SSH verification. Initial scans finished with native watchers and no overflows. This is a single live observation, not a controlled latency benchmark.

`trilbymedia` subsequently stopped on two representations of an accented filename: composed versus decomposed Unicode. Both spellings were confirmed to resolve to the same inode on the Mac. Its receiver treated the spelling difference as a path-key collision. The fix resolves canonical aliases, including parent directories, to their indexed local spelling before payload negotiation and publication. Case-only differences and physically distinct aliases remain errors.

The Mac suite passes 49 unit and 12 integration tests. The new end-to-end test uses different canonical encodings for each device's directory, verifies bidirectional edits and a new child, and confirms that local directory names remain unchanged. An additional Linux-only test rejects two physically distinct files with canonical aliases. Large-folder reconciliation remains in progress; these tests do not establish complete live-folder convergence.

## Parallel directory flushes — September 11, 2026 (0.2.3)

Live Linux thread samples repeatedly showed Btrfs commit waits during small-file transfer while CPU utilization was low. Directory durability work was sequential even though file contents already flushed in parallel. Version 0.2.3 uses one reusable, eight-thread disk pool for both, with a barrier between directory depths. Errors still prevent cursor commit and acknowledgement. A regression injects a flush failure and verifies that other in-flight flushes finish before the error returns.

Isolated SSH trials used 2,048 × 4 KiB random-content files, 16 files per leaf, and three directory levels on the same Linux Projects SSD. Native watchers used two scan workers and hourly reconciliation. Trial order was baseline / changed / changed / baseline, with the live Grav copy left running. Baseline Homebrew 0.2.1 measured 179.8 and 176.7 files/sec; the change (on top of 0.2.2) measured 270.4 and 252.1 files/sec. Mean throughput increased about 46%; elapsed times were 11.390/11.593 seconds versus 7.575/8.124 seconds. Every payload hash passed, and reverse watcher latency was 300–325 ms including SSH. This is a small-tree comparison under background load, not a network ceiling or Syncthing comparison. Raw results: [parallel-directory-flush.json](benchmarks/parallel-directory-flush.json).

macOS validation: 50 unit tests and 12 integration tests passed with native watchers required; formatting and Clippy passed. Extra on-server test compilation was stopped at 86°C; Linux validation is delegated to CI.

## Fresh receiver follow-up — September 11, 2026 (0.2.4)

Grav's live transfer cursors caught up after the 0.2.3 restart, but 82 empty Git object directories on Linux conflicted with Mac tombstones. Applying a tombstone for an absent file called parent creation before performing its no-op deletion. The new regression fails on the old code and passes when deletion skips parent creation. An encrypted two-daemon integration test now bootstraps 260 deleted files and their directory tombstones into a fresh receiver across multiple batches, requires equal indexes and zero conflicts, checks no retired directories exist, and then checks a reverse watcher edit.

A separate set of Trilbymedia symlink conflicts had identical target strings and hashes, with modes 0755 on Mac versus 0777 on Linux. The engine does not transfer symlink modes, so comparisons now ignore them for symlinks. A regression requires clock merging without modifying the link inode, a stable subsequent scan sequence, and preservation of a genuinely different incoming target as a conflict. File/directory permission protections remain in the existing suite. Recorded live conflicts are preserved for review.


## Ratatui monitor (0.2.5)

The monitor renders payload graphs, folders, watcher/device diagnostics, and filtered activity from the bounded daemon snapshot. It does not open SQLite, enumerate watched folders, or change configuration. Local rendering tests cover live/stale rates, old snapshots without a version field, missing-state behavior, bounded history/restart reset, filtering, keyboard selection, and terminal sizes down to 1×1. An actual PTY check exercises input, resize, missing-snapshot recovery, alternate-screen restoration, terminal attributes after q/Ctrl-C/SIGTERM, non-terminal rejection, and unchanged state files. The PTY check is included in macOS/Linux CI.

Local macOS validation: 57 unit tests and 13 integration tests passed with native watchers required; formatting, Clippy, and Rust 1.88 all-target checks passed. A live snapshot was rendered through Ratatui's TestBackend and visually inspected. Existing transfer, deletion, conflict-preservation, Unicode, and durability regressions remain covered.


## Conflict review panel (0.2.6)

The panel queries read-only SQLite connections only on request, returns at most 50 records per page, and reads verified text previews capped at 64 KiB. Selected changed working files may be hashed up to 16 MiB to support manual merges without a daemon restart; larger changed files need scanner indexing first. One background worker keeps these operations off the terminal event loop.

Resolver tests reject running-daemon access, stale content/metadata and clock reviews, and a deleted path recreated after review. A manually merged file resolves without changing its bytes, incoming archives remain, unrelated conflict records remain, and a failed decision rolls back index changes. Preview tests cover binary/oversized content, corrupted archives, symlink archive rejection, scoped search and bounded pages. Render tests exercise narrow and wide layouts and keep confirmation controls visible. The real terminal test cancels a resolution, then explicitly confirms one disposable record and checks the database and preserved local absence. Existing encrypted transfer and conflict-propagation tests still apply.

## Pairing and retention (shipped in 0.3.2)

Local macOS validation on September 12, 2026 passes 74 unit tests and 15 integration tests with native watchers required. Clippy with warnings denied, Rust 1.88 all-target checks, terminal restoration/conflict-panel PTY checks, and a disposable-folder CLI walkthrough also pass. These features have not yet been released or deployed to the live Homebrew services; new Linux execution/CI validation remains outstanding.

Pairing coverage includes normal merge versus explicit source seeding, stale-plan transaction rollback, changed folder exclusions, daemon-lock refusal, receiver edits after snapshot export, retained original receiver bytes, and source working-file preservation. A two-daemon seed trial removes 281 receiver-only file/directory entries across multiple protocol batches, transfers source-only files, reaches identical indexes without conflicts, then verifies subsequent edits in both directions. Restored directory permissions are published before source-only children to avoid creating receiver-local metadata divergence.

Retention coverage includes disabled defaults, non-deleting previews, age and space policies, working-file hard-link preservation, locked resumable buffers, symlink/unknown-artifact protection, and protection of both pending and uncommitted conflict archives. An integration test enables automatic maintenance on a disposable daemon, verifies that its first scheduled cleanup removes an old archive, and verifies that the working file and running daemon remain intact. No real conflict/version archives were cleaned and no live retention policy was enabled during development.

## Transfer lanes and disk-read reduction (shipped in 0.3.2)

September 12, 2026 macOS validation covered 80 unit tests and 18 integration tests. The full suite passed with native watchers required, followed by targeted rechecks for the final lane-startup timing adjustment and signature-cache eviction test. Clippy with warnings denied, Rust 1.88 all-target checks, formatting, and the real-terminal monitor/conflict-panel checks pass. New independent Linux execution remains outstanding. The live Homebrew services were not upgraded, and no real sync files or archive policies were changed by these trials.

A deterministic two-daemon test locks a large file's receive buffer, observes the blocked bulk lane, and requires a later small edit to arrive while that lock remains held. It then releases the lock, verifies the large payload and parent mode 0700, changes negotiated lane counts through 1/2/3, moves a file between small/bulk sizes, and verifies recursive deletion and equal indexes. Existing simultaneous dialing, restart/resume, interrupted delta, Unicode, independent-edit conflicts, pause and revocation tests pass with the new default lanes. Cursor tests drain multiple query pages without skips and retain the minimum durable watermark across layout changes.

A cache integration test makes two delta edits separated by daemon restarts: the second receiver update uses a persistent signature and reports zero new basis-signature input bytes. A complete-file rewrite test verifies that an ineffective delta falls back, then the next version streams with no additional signature reads and exactly one source payload read. Cache unit tests reject changed fingerprints/parameters, corrupted records and corrupt SQLite files, compare cached manifests with originals, enforce eviction/byte budgets, and verify bounded per-path feedback. Existing payload and copied-chunk verification remains active. The 64 MiB scan cache only captures files up to 8 MiB; these changes do not eliminate the second source read for all initial files.

Final sequential loopback samples are saved in [benchmarks/transfer-lanes-local.json](benchmarks/transfer-lanes-local.json). Every received payload was checked. These measurements include startup and durable publication, with two scan workers on each disposable daemon:

| Workload | Build/settings | Bootstrap | Payload rate | Transfer-side source reads |
| --- | --- | ---: | ---: | ---: |
| 2,000 × 4 KiB | Released 0.2.6 | 8.028 s | 1.02 MB/s | Not instrumented |
| 2,000 × 4 KiB | New, 3 lanes, 64 MiB scan cache | 6.507 s | 1.26 MB/s | 0 |
| 2,000 × 4 KiB | New, 3 lanes, cache disabled | 7.979 s | 1.03 MB/s | 8,192,019 bytes |
| 64 × 16 MiB | New, 1 lane, cache disabled | 5.653 s | 189.94 MB/s | 1,073,741,843 bytes |
| 64 × 16 MiB | New, 3 lanes, cache disabled | 4.391 s | 244.52 MB/s | 1,073,741,843 bytes |

The source-read counters include the 19-byte post-bootstrap edit probe and exclude scanner reads; they are not physical device I/O. The cached small-file run reused all 8,192,019 bytes for sending. Post-bootstrap watcher probes took 181–312 ms in these final trials. Their timing does not measure edits during bulk traffic; the locked-buffer integration test establishes that independence.

These are single final samples with no randomized order or cold-cache control. Earlier exploratory runs varied substantially, including a 1 GiB comparison of 214 MB/s with one lane versus 212 MB/s with three. The final results support fewer duplicate reads and functioning concurrency, not a universal throughput gain. Repeated representative LAN/disk tests and CPU/temperature measurements remain necessary before claiming production performance improvements. Wire protocol 5 requires upgrading both peers together; first startup builds the new partial change indexes over existing indexed entries.

## 0.3.2 Homebrew deployment and LAN verification

On September 12, 2026, commit `d8869f1` passed CI on macOS/Linux, Rust 1.88 checks, and native release tests/builds on Apple Silicon, Intel Mac, Linux x86-64 and Linux ARM64. The 0.3.0/0.3.1 tags were not published: release testing identified an early status-accounting sample in a test, then a transient inherited-descriptor lock lifetime. 0.3.2 waits for full transfer accounting and explicitly releases offline-operation locks at scope exit, with a duplicate-descriptor regression. The macOS suite now contains 81 unit and 18 integration tests.

Both live machines were stopped, their SQLite indexes and configuration backed up, upgraded through Homebrew to 0.3.2, and restarted together. The three active shares retained their identities, exclusions and two scan workers. Pairing was not reseeded and automatic retention was not enabled. Historical records belonging to inactive shares were preserved. All three active shares finished in native watching mode, with fresh snapshots, connected peers, three durable lane cursors per folder and zero pending conflicts.

Disposable files inside each active share verified Mac-to-Linux and Linux-to-Mac edits. A 4 MiB file and a 4 KiB modification verified bulk and delta publication; rename and deletion also propagated. Only the exact test-created contents were removed, and all three disposable directories disappeared on Linux. Initial Mac-to-Linux probes overlapped startup reconciliation (2.55 s Grav, 9.14 s Trilbymedia); the other probes took 0.26–0.74 s including SSH checks.

The installed Homebrew binaries then transferred eight 32 MiB files into an isolated root on the Linux **project disk**, rather than the separate system/cache disk. All 256 MiB verified in 4.755 s, averaging 56.46 MB/s including startup, indexing and durable writes. The reverse edit took 340 ms including its SSH write. Subsequent 32 MiB-file deltas sent 196,574 bytes for a 4 KiB overwrite, 104,674 bytes for a 19-byte insertion, and 19,669 bytes for a 4 KiB deletion; every resulting file was SHA-256 verified. These are single samples with existing workloads running, not a sustained-link or comparative Syncthing benchmark.

Five-second temperature samples across deployment/testing peaked at 77.125°C on Linux. No thermal limit or CPU quota was added. During an approximately 83-second observation after the isolated benchmark, daemon CPU time averaged 2.41% of one core on Linux and 8.12% on the Mac; normal application edits continued, so this was not a controlled idle-power trial. Final Linux sensors read 49.9–59.0°C. Details are preserved in [benchmarks/upgrade-0.3.2-lan.json](benchmarks/upgrade-0.3.2-lan.json). Both services remained running after verification; existing monitors can be reopened to use the installed version.

## September 12: explicit directions and stronger evidence (0.4.0-dev)

The unreleased development build uses protocol 6. Mac validation covers 89 unit and 25 integration tests; Linux covers 93 unit and 25 integration tests with native inotify required and two test threads. The full suites passed before two final tests were added; those protocol-5 rejection and opposite-per-folder-flow tests then passed independently on both platforms. Formatting, all-target Clippy with warnings denied, four Python packaging tests, and the monitor PTY checks passed. Raw version/source/binary fingerprints and execution boundaries: [directionality-validation-20260912.json](benchmarks/directionality-validation-20260912.json).

Direction tests cover a sender or receiver initiating connections, bulk and metadata lanes, source deletions, receiver restart, preservation of accidental receiver edits as conflicts, and zero reverse entry transfer. Existing configuration defaults to send-receive; unknown mode strings fail parsing. The policy command holds the daemon lock and refuses an active daemon. A publication-gate regression changes the local receiver to send-only while it waits and verifies no publication or acknowledgement. Opposite directions on separate folders share a connection successfully; incompatible same-mode folders report a mismatch. Protocol 5 is refused before folder exchange.

The deterministic eight-round recovery test kills both isolated processes at verified baselines, alternates startup order and applies disjoint edits, renames and deletions offline. Each round checks both complete indexed file maps against an independent expected-content map and requires no conflicts. This supplements existing interrupted-transfer coverage; it does not simulate power loss or establish a long-running soak.

Repeated isolated local trials use **distinct seeded contents**, two scan workers, default three lanes/64 MiB caches, and send-only to receive-only. Source generation warms the OS cache; receiver files and indexes start fresh. The retained `transfer_lanes` and cache fields in these trial records are null because no override was requested; the pinned development binary uses the defaults above. Every destination file is SHA-256 verified before subsequent edits. Each dataset has three trials and 60 total edit samples, with 10 ms polling; throughput is logical payload MB/s.

| Local fixture | Bootstrap median (range) | Throughput median | Edit median / empirical p95 |
| --- | --- | --- | --- |
| 2,000 × 4 KiB | 7.062 s (6.902–7.445) | 283.2 files/s; 1.16 MB/s | 211.35 / 277.3 ms |
| 8 × 32 MiB | 2.391 s (2.222–2.909) | 112.25 MB/s | 214.7 / 275.9 ms |

Raw repeated results: [small files](benchmarks/directionality-local-small-20260912.json), [bulk files](benchmarks/directionality-local-bulk-20260912.json). The ten-second idle observations measured roughly 0.24–0.26 CPU seconds per sender and 0.21–0.23 per receiver (about 2–3% of one core). This exposes remaining idle polling work; it is not a zero-CPU result or a measurement across a full safety-scan interval. Cumulative CPU before idle includes bootstrap, edits and the elapsed verification period. Earlier fixtures, file counts/sizes and caching conditions differ, so these numbers do not establish a speedup or regression against prior runs.

An isolated disk-backed Mac→Linux trial of eight distinct 32 MiB files completed in **5.183 s / 51.80 MB/s**, with every file SHA-256 verified. A reverse edit arrived in 422.2 ms including the probe overhead. A 4 KiB overwrite, 19-byte insertion and 4 KiB deletion within a 32 MiB file sent 76,815 / 164,799 / 170,075 new payload bytes respectively; all resulting files verified. Signature/plan traffic is excluded. This LAN trial used the normal bidirectional mode and active background workloads, not the one-way local configuration. It is one trial, not a throughput ranking. Raw record: [LAN/delta](benchmarks/directionality-lan-20260912.json).

The installed Mac and Linux 0.3.3 services remained running with their original PIDs throughout. Final read-only health checks showed all three live folders natively watching, connected and without active conflicts. The development policies have **not** been installed or applied to those live folders.
