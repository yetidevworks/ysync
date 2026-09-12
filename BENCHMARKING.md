# Evidence and correctness gates

Version 0.4.0 uses protocol 6; releases 0.3.2–0.3.3 use protocol 5. The initial evidence records identify the tested 0.4.0-dev candidate and its executable fingerprints. Tests and binaries must identify which they use; a passing development run is not evidence that an installed older daemon enforces the new policies.

## Repeatable evidence now available

`scripts/benchmark_repeated.py` runs separate disposable pairs of foreground daemons. It never controls an installed service. Each trial has fresh identities, indexes and receiver files. File contents are distinct deterministic SHAKE-256 streams keyed by the dataset seed and file number. The SHA-256 of every destination is verified before editing. Previous fixtures reused one payload across files; their recorded numbers remain historical and must not be silently compared with this new corpus.

Reports retain each trial, executable SHA-256/version, runner SHA-256, platform, corpus seed/shape, bootstrap timing, files/s, payload MB/s, all edit latencies, CPU snapshots and idle observations. `--one-way` uses send-only/receive-only. Timed bootstrap includes startup/indexing/publication and index polling; verification follows outside that time. Edit samples replace one existing file with distinct small contents and include 10 ms polling. Source generation warms OS caches; no cold-cache claim is made. Fresh receiver/index is distinct from cold filesystem cache.

Per-daemon `ps` CPU time has platform-dependent precision (typically coarser on Linux). Zero measured change means below that precision, not zero work. CPU before the idle window includes bootstrap, edits and the elapsed verification period. Short idle windows do not cover hourly reconciliation. Source-read counters are logical transfer reads, not physical storage I/O. These trials do not measure whole-system energy, physical disk traffic or peak memory.

```sh
python3 scripts/benchmark_repeated.py --binary /absolute/path/to/ysync \
  --trials 3 --files 2000 --size 4096 --scan-workers 2 \
  --one-way --edit-samples 20 --idle-seconds 10 --output /tmp/small-files.json
python3 scripts/benchmark_repeated.py --binary /absolute/path/to/ysync \
  --trials 3 --files 8 --size 33554432 --scan-workers 2 \
  --one-way --edit-samples 20 --idle-seconds 10 --output /tmp/bulk-files.json
```

The SSH runner also uses the distinct seeded corpus and verifies every received file. Its source/destination roots are disposable. Use a disk-backed remote base; `/tmp` may be tmpfs. It remains a single-trial runner; archive repeated outputs separately. Background workload and SSH/status polling are part of its stated limitations.

## Required controlled comparison

Compare current pinned builds of Syncthing, Mutagen, Unison and rsync on the same Mac/Linux hosts. Use disposable copies outside all live shares. Match exclusions, file contents, network route, verification and durability semantics; record any setting that cannot be matched. Do not substitute macOS's bundled openrsync for current upstream rsync without naming that difference. Use the same direction semantics for a one-way trial; test bidirectional correctness separately.

Run at least five trials per condition in randomized tool order. Separate:

1. Cold initial indexing/transfer (only label cold when caches are actually controlled), warm restart/reconciliation, and already synchronized idle operation.
2. Many tiny files, mixed development trees, large incompressible files, and a separately labeled duplicate/compressible corpus.
3. Overwrites, unaligned insertions/deletions, renames, permission changes and edit latency while bulk lanes are occupied.
4. CPU seconds on each endpoint, logical and physical read/write bytes, memory, wire bytes and convergence latency distributions. Measure the direct network ceiling separately.
5. At least one complete safety-scan interval for idle observations, with scanner activity distinguished from ordinary watcher updates.

Record all runs and failures, version/configuration hashes, tool output, hardware/filesystem/network details and background activity. Report medians/ranges and adequate latency sample counts; do not infer a population p95 from a handful of probes. An rsync command returning and a watcher-driven daemon acknowledging a change are different completion semantics; verify bytes and explain timing boundaries for each adapter.

No automated multi-tool comparison adapter or controlled head-to-head result is claimed by the current runner.

## Correctness work

Direction tests exercise source and receiver initiated connections, metadata and bulk lanes, local receiver edits, source deletion, restart, conflict preservation, mode mismatch and stopped-daemon policy changes. A publication-gate test changes a receiver to send-only while it waits on the scanner and verifies no file or acknowledgement is published. Existing conflict tests cover unscanned local edits and stale causal versions.

A deterministic eight-round sequence kills both disposable daemons at verified baselines, alternates startup order and edits/renames/deletes disjoint paths offline. Every round checks both complete indexed file maps against an independent expected-content map and requires no unexplained conflicts. This supplements existing interrupted-payload tests. It is a bounded process-crash test, not randomized exhaustive state-machine testing or power-loss evidence.

Still required before broader reliability claims: sustained large-tree operation, actual reboot/logout and power-loss/disk-full trials, more filesystem fidelity, fuzzed protocol/state-machine testing and independent security review. Live database consistency requires application-aware snapshots; folder direction does not make a database/WAL pair transactional.
