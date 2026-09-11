Fix a macOS/Linux synchronization stall caused by canonically equivalent Unicode filenames (for example, an accented letter encoded as one character versus a base letter plus a combining accent).

Receivers now use the existing local filename and parent-directory spelling for equivalent names. Working files are not renamed. Content conflicts remain pending for explicit review. Case-only collisions, duplicate batch path keys, and distinct physical files with equivalent Unicode names are still rejected.

Regression tests cover equivalent existing files, later edits in both directions, new children under differently encoded directory names, conflict preservation, ignored entries, and distinct Linux aliases. The 0.2.1 progress/cancellation fix is retained.

Upgrade both peers with `brew update && brew upgrade ysync`, or install with `cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.2.2 --locked ysync`. Stop services before upgrading. Wire protocol remains 4; identities, configuration, index history and pending conflicts are retained.

Experimental release. The larger live-folder reconciliation is still being evaluated; existing content conflicts require review.
