Press `c` in the monitor to open conflict review. Browse by folder, search, page through records, and inspect current local versus preserved incoming versions with metadata and bounded text previews.

Press `l` then `y` to explicitly keep one reviewed local version. The daemon must be stopped; changed files or version clocks reject stale confirmation. Incoming archives and unrelated records stay preserved. Manually merge or copy chosen incoming content outside the TUI, refresh the review, then keep that local result. This release does not choose resolutions automatically or add bulk incoming replacement.

Conflict queries and preview work are on demand and off the terminal event loop. Normal monitoring keeps its snapshot-only behavior. Mac/Linux terminal tests exercise review, cancellation, one-record confirmation, resize, and clean exit; resolver regressions cover stale edits, manual merges, archive safety, and bounded pages.

Upgrade with `brew update && brew upgrade ysync` or install Cargo tag `v0.2.6`. Restart the monitor to use the panel. Wire protocol remains4; existing conflict decisions are unchanged. Experimental release.
