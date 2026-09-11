Improve small-file transfers by flushing sibling directories concurrently, using a reusable pool capped at eight disk workers. Directory levels still commit from children to parents, and acknowledgements still wait for durable files, directories, and the index cursor. Conflict preservation and overwrite checks are retained.

Two pairs of isolated Mac-to-Linux tests on the Projects Btrfs SSD transferred 2,048 files of 4 KiB across a three-level tree: 177–180 files/sec on 0.2.1 versus 252–270 files/sec with this change (about 46% higher mean throughput). All payload hashes and reverse watcher edits passed. The real Grav transfer remained active, so these results are workload-specific, not a general network throughput claim.

Includes the 0.2.2 Unicode filename fix. Upgrade both peers with `brew update && brew upgrade ysync`, or `cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.2.3 --locked ysync`. Stop services before upgrading. Wire protocol remains 4.

Experimental release; larger live-folder reconciliation remains under evaluation, and content conflicts require review.
