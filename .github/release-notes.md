Fix receiver timeouts during slow initial scans. Destination checks, filesystem flushes, scanner-lock waits, and publication now send encrypted progress messages while the peer waits. Previously, these silent waits could exceed the 30-second transport timeout and leave reconnects encountering an unfinished session.

Pause, revocation, daemon stop, and connection failure cancel cooperative receiver reads and lock waits. The peer is acknowledged only after files and the sync cursor are committed. The 0.2.0 conflict protections remain intact; independent versions still require explicit resolution.

Four new isolated regressions cover scanner-lock delay, database-writer contention, pause, and disconnect. Slow receiver activity is visible in events, and timeout errors identify the batch and transfer stage.

Upgrade with `brew update && brew upgrade ysync`, or `cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.2.1 --locked ysync`. Stop services before upgrading. Both peers should be upgraded to receive the fix in both directions; wire protocol remains 4. Configuration, identity, indexes, and pending conflicts are retained.

This remains experimental. The interrupted real Projects trial requires another monitored run; this release does not resolve existing conflicts, exclusions, inaccessible files, or initial backlog.
