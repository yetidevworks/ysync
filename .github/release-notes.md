First experimental ysync release: encrypted bidirectional sync, native watchers, resume/delta transfers, monitoring, and native user services.

Install with Rust 1.88 or newer:

```sh
cargo install \
  --git https://github.com/yetidevworks/ysync.git \
  --tag v0.1.0 --locked ysync
```

Or install a prebuilt binary with Homebrew:

```sh
# If your Homebrew requires tap trust, run this first:
# brew trust --formula yetidevworks/ysync/ysync
brew tap yetidevworks/ysync
brew install yetidevworks/ysync/ysync
```

Alternatively, download the archive for your CPU/OS, verify it against SHA256SUMS, extract it, and put `ysync` on your PATH. Linux archives use glibc 2.35 or newer. macOS binaries are unsigned; install through Cargo if your local security policy blocks downloaded executables.

Each archive is built on its matching architecture after native-watcher tests and a Cargo installation check. The release remains experimental; see README.md, VALIDATION.md, and ROADMAP.md before enabling synchronization of important folders.
