# Evaluation results — September 10, 2026

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

Service installation, login/reboot behavior, power-loss recovery, and long-duration operation have not been validated. The live multi-million-entry evaluation below identified unresolved operational limits. Passing these tests is not a security audit or proof of production readiness.

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
