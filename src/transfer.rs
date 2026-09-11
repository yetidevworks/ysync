//! Restartable, private receive buffers. Prefixes are hints until the sender verifies them.
use crate::{engine::Root, model::Entry};
use anyhow::{Context, Result, bail};
use cap_std::fs::{MetadataExt, OpenOptions, OpenOptionsExt};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::PermissionsExt,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resume {
    pub offset: u64,
    pub prefix_hash: String,
    #[serde(default)]
    pub basis_size: Option<u64>,
}

pub struct Partial {
    pub name: String,
    pub file: File,
    pub hash: blake3::Hasher,
    pub request: Resume,
}

/// Hash only bounded buffers, sending liveness notices during a long disk read.
pub fn hash_prefix(
    file: &mut impl Read,
    mut size: u64,
    mut progress: impl FnMut() -> Result<()>,
) -> Result<blake3::Hasher> {
    if size > 0 {
        progress()?;
    }
    let mut hash = blake3::Hasher::new();
    let mut buf = [0; 256 * 1024];
    let mut tick = Instant::now();
    while size > 0 {
        let count = size.min(buf.len() as u64) as usize;
        let n = file.read(&mut buf[..count])?;
        if n == 0 {
            bail!("file shortened while verifying resume prefix");
        }
        hash.update(&buf[..n]);
        size -= n as u64;
        if tick.elapsed() >= Duration::from_secs(1) {
            progress()?;
            tick = Instant::now();
        }
    }
    Ok(hash)
}

impl Partial {
    pub fn open(
        root: &Root,
        peer: &str,
        entry: &Entry,
        progress: impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        entry.validate()?;
        root.check()?;
        // Scope to peer, path, and exact content version. Never use peer-supplied text as a filename.
        let key = blake3::hash(&serde_json::to_vec(&(
            peer,
            &entry.path,
            &entry.hash,
            entry.size,
        ))?);
        let name = format!(".ysync/tmp/resume-{}.part", key.to_hex());
        root.dir.create_dir_all(".ysync/tmp")?;
        match root.dir.symlink_metadata(&name) {
            Ok(m) => {
                if !m.is_file() || m.is_symlink() || m.nlink() != 1 {
                    bail!("resume buffer must be a regular file with no hard-link aliases");
                }
                // A crash during publication may leave a complete buffer with its destination mode.
                // Restore private staging permissions before opening it for append.
                if m.mode() & 0o777 != 0o600 {
                    root.dir.set_permissions(
                        &name,
                        cap_std::fs::Permissions::from_std(std::fs::Permissions::from_mode(0o600)),
                    )?;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let mut file = root
            .dir
            .open_with(
                &name,
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .mode(0o600),
            )?
            .into_std();
        // Keep ownership through publication/cleanup. Other sessions must not append to this inode.
        fs2::FileExt::try_lock_exclusive(&file).context("resume buffer already in use")?;
        let mut offset = file.metadata()?.len();
        if offset > entry.size {
            file.set_len(0)?;
            offset = 0;
        }
        let hash = hash_prefix(&mut file, offset, progress)?;
        let request = Resume {
            offset,
            prefix_hash: hash.finalize().to_hex().to_string(),
            basis_size: None,
        };
        Ok(Self {
            name,
            file,
            hash,
            request,
        })
    }

    pub fn start(&mut self, accepted: u64) -> Result<()> {
        if accepted != 0 && accepted != self.request.offset {
            bail!("invalid accepted resume offset");
        }
        if accepted == 0 {
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.hash = blake3::Hasher::new();
        }
        Ok(())
    }
}

/// The source must independently verify the receiver's prefix before omitting any bytes.
pub fn accept_resume(
    file: &mut (impl Read + Seek),
    size: u64,
    request: &Resume,
    progress: impl FnMut() -> Result<()>,
) -> Result<(u64, blake3::Hasher)> {
    if request.offset > size
        || request.prefix_hash.len() != 64
        || hex::decode(&request.prefix_hash).is_err()
    {
        bail!("invalid resume request");
    }
    let hash = hash_prefix(file, request.offset, progress)?;
    if hash.finalize().to_hex().as_str() == request.prefix_hash {
        Ok((request.offset, hash))
    } else {
        file.seek(SeekFrom::Start(0))?;
        Ok((0, blake3::Hasher::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config, engine, model::Kind};
    use std::{
        collections::BTreeMap,
        io::Write,
        sync::{Arc, Mutex},
    };

    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, Root, Entry) {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let peer = config::initialize(state.path(), None, None).unwrap();
        engine::add_folder(state.path(), "code", files.path(), false).unwrap();
        let root = Root::open(
            config::load(state.path()).unwrap().folders.remove(0),
            Arc::new(Mutex::new(())),
        )
        .unwrap();
        let entry = Entry {
            path: "large.bin".into(),
            kind: Kind::File,
            size: 10,
            hash: blake3::hash(b"0123456789").to_hex().to_string(),
            target: None,
            mode: 0o444,
            clock: BTreeMap::from([(peer, 1)]),
            seq: 1,
            stamp: String::new(),
        };
        (state, files, root, entry)
    }

    #[test]
    fn prefixes_survive_reopen_and_are_verified_including_empty_and_complete() {
        let (_state, _files, root, e) = fixture();
        let mut partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        assert_eq!(partial.request.offset, 0);
        partial.file.write_all(b"01234").unwrap();
        drop(partial);
        let mut partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        assert_eq!(partial.request.offset, 5);
        let mut source = std::io::Cursor::new(b"0123456789");
        let (offset, mut hash) =
            accept_resume(&mut source, e.size, &partial.request, || Ok(())).unwrap();
        assert_eq!(offset, 5);
        assert_eq!(source.position(), 5);
        partial.start(offset).unwrap();
        let mut suffix = Vec::new();
        source.read_to_end(&mut suffix).unwrap();
        assert_eq!(suffix, b"56789");
        hash.update(&suffix);
        assert_eq!(hash.finalize().to_hex().as_str(), e.hash);
        partial.file.write_all(&suffix).unwrap();
        partial
            .file
            .set_permissions(std::fs::Permissions::from_mode(0o444))
            .unwrap();
        drop(partial);
        let partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        assert_eq!(partial.request.offset, e.size);
        source.set_position(0);
        assert_eq!(
            accept_resume(&mut source, e.size, &partial.request, || Ok(()))
                .unwrap()
                .0,
            e.size
        );
        assert_eq!(source.position(), e.size);
        let empty = Resume {
            offset: 0,
            prefix_hash: blake3::hash(b"").to_hex().to_string(),
            basis_size: None,
        };
        source.set_position(0);
        assert_eq!(
            accept_resume(&mut source, 0, &empty, || Ok(())).unwrap().0,
            0
        );
    }

    #[test]
    fn corruption_restarts_and_invalid_offsets_are_rejected() {
        let (_state, _files, root, e) = fixture();
        let mut partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        partial.file.write_all(b"wrong").unwrap();
        drop(partial);
        let mut partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        let mut source = std::io::Cursor::new(b"0123456789");
        assert_eq!(
            accept_resume(&mut source, e.size, &partial.request, || Ok(()))
                .unwrap()
                .0,
            0
        );
        assert_eq!(source.position(), 0);
        assert!(partial.start(4).is_err());
        partial.start(0).unwrap();
        assert_eq!(partial.file.metadata().unwrap().len(), 0);
        partial.request.offset = e.size + 1;
        assert!(accept_resume(&mut source, e.size, &partial.request, || Ok(())).is_err());
        partial.request.offset = 0;
        partial.request.prefix_hash = "bad".into();
        assert!(accept_resume(&mut source, e.size, &partial.request, || Ok(())).is_err());
    }

    #[test]
    fn cache_is_version_scoped_exclusive_and_rejects_links() {
        let (_state, files, root, mut e) = fixture();
        let mut partial = Partial::open(&root, "peer", &e, || Ok(())).unwrap();
        assert!(Partial::open(&root, "peer", &e, || Ok(())).is_err());
        partial.file.write_all(b"01234").unwrap();
        let name = partial.name.clone();
        assert_eq!(
            Partial::open(&root, "other-peer", &e, || Ok(()))
                .unwrap()
                .request
                .offset,
            0
        );
        e.hash = blake3::hash(b"different!").to_hex().to_string();
        assert_eq!(
            Partial::open(&root, "peer", &e, || Ok(()))
                .unwrap()
                .request
                .offset,
            0
        );
        e.hash = blake3::hash(b"0123456789").to_hex().to_string();
        drop(partial);
        // A stale/truncated source must not make an overlong buffer a valid resume offset.
        std::fs::write(files.path().join(&name), b"too long for this entry").unwrap();
        assert_eq!(
            Partial::open(&root, "peer", &e, || Ok(()))
                .unwrap()
                .request
                .offset,
            0
        );
        std::fs::remove_file(files.path().join(&name)).unwrap();
        std::fs::write(files.path().join("innocent"), b"leave alone").unwrap();
        std::os::unix::fs::symlink("../../innocent", files.path().join(&name)).unwrap();
        assert!(Partial::open(&root, "peer", &e, || Ok(())).is_err());
        std::fs::remove_file(files.path().join(&name)).unwrap();
        std::fs::hard_link(files.path().join("innocent"), files.path().join(&name)).unwrap();
        assert!(Partial::open(&root, "peer", &e, || Ok(())).is_err());
        assert_eq!(
            std::fs::read(files.path().join("innocent")).unwrap(),
            b"leave alone"
        );
    }
}
