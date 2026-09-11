First experimental ysync release: encrypted bidirectional sync, native watchers, resume/delta transfers, monitoring, and native user services.

Install from this private repository with Rust 1.88 or newer and an SSH identity that has repository access:

```sh
CARGO_NET_GIT_FETCH_WITH_CLI=true cargo install \
  --git ssh://git@github.com/yetidevworks/ysync.git \
  --tag v0.1.0 --locked ysync
```

Alternatively, download the archive for your CPU/OS, verify it against SHA256SUMS, extract it, and put `ysync` on your PATH. Linux archives use glibc 2.35 or newer. macOS binaries are unsigned; install through Cargo if your local security policy blocks downloaded executables.

Each archive is built on its matching architecture after native-watcher tests and a Cargo installation check. The release remains experimental; see README.md, VALIDATION.md, and ROADMAP.md before enabling synchronization of important folders.
