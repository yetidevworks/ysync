Fix two issues found during the live macOS-to-Linux bootstrap trial. Historical deletions no longer create missing parent directories on a fresh receiver, preventing phantom empty directories and resulting deletion conflicts. Symbolic links with identical targets no longer conflict solely because macOS and Linux report different permission bits; file/directory permissions and differing link targets remain protected.

Tests cover multiple batches of historical deletions into an empty destination, no invented directories, stable symlink inodes and rescan sequences, conflicting link targets, and the existing interrupted-transfer and overwrite protections. The parallel durability improvement from 0.2.3 is retained.

Upgrade both peers with `brew update && brew upgrade ysync`, or `cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.2.4 --locked ysync`. Stop services before upgrading. Wire protocol remains 4. Existing recorded conflicts are retained for deliberate review; this release does not automatically clean them up.

Experimental release; live reconciliation remains under evaluation.
