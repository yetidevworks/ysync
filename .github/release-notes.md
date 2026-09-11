A new Ratatui dashboard replaces the plain live monitor. It includes payload throughput graphs, selectable folders, expanded watcher/device diagnostics, and a searchable, filterable activity feed. Press `?` for controls, `d` for full diagnostics, and `q` to leave while synchronization continues. `ysync monitor --plain`, `ysync status`, and JSON status remain available.

Incoming index reconciliation is now labeled clearly and limited within the recent activity window, keeping other events visible. The monitor distinguishes its own version from the daemon version, handles stale/missing snapshots, and restores the terminal on exit. It reads snapshots once per second without scanning folders or opening the sync index.

Upgrade with `brew update && brew upgrade ysync`, or `cargo install --git https://github.com/yetidevworks/ysync.git --tag v0.2.5 --locked ysync`. Restart the service for daemon version reporting and improved activity retention; launch a new `ysync monitor` for the dashboard. Wire protocol remains 4 and conflict decisions are unchanged.

Experimental release; live reconciliation remains under evaluation.
