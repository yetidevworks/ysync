Safety change: conflicting incoming content no longer automatically replaces a working file. Independently changed or initially different files remain in place on each device; incoming versions are preserved as pending conflicts. Hash ordering and timestamps do not select a winner.

Known older causal versions are ignored. Ordinary later edits to a shared version still synchronize. Local content is checked before publication, and creation of a new file cannot overwrite a path created during the final publication window.

New commands:

```sh
ysync conflict list
ysync conflict list --json
# Stop the daemon first; review/merge the working file before choosing it.
ysync conflict resolve projects FULL_CONFLICT_ID --keep-local
```

**Upgrade both devices.** Protocol 4 refuses protocol 3 peers (0.1.x) before exchanging file batches. Stop services before `brew update && brew upgrade ysync`. Pause settings, identities and indexes are retained; the pending-conflicts table is added automatically. Legacy conflict archives remain on disk for separate review; this release does not undo earlier conflict choices.

This remains experimental. Existing-file conflict review, initial backlog, inaccessible paths, and application-consistent snapshots are still separate concerns. Native archives cover ARM64/x86-64 macOS and Linux; Linux requires glibc 2.35 or newer. Checksums are included.
