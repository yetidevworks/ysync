use crate::{
    config,
    daemon::Shared,
    delta::{self, Block, Operation, Parameters},
    engine::{self, Root},
    model::{Entry, Kind},
    store,
    transfer::{Partial, Resume, accept_resume},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

const CHUNK: usize = 60 * 1024;
const MAX_JSON: usize = 4 * 1024 * 1024;
#[derive(Serialize, Deserialize, Debug)]
enum Message {
    Hello {
        version: u32,
        name: String,
        folders: Vec<String>,
        cursors: BTreeMap<String, u64>,
    },
    Denied {
        reason: String,
    },
    Batch {
        folder: String,
        entries: Vec<Entry>,
        upto: u64,
    },
    Want(Vec<Option<Resume>>),
    Preparing,
    FileStart {
        offset: u64,
        delta: bool,
    },
    Basis {
        blocks: Option<Vec<Block>>,
    },
    DeltaPlan {
        operations: Option<Vec<Operation>>,
    },
    FileEnd {
        valid: bool,
    },
    Ack {
        upto: u64,
    },
    Turn,
}
pub struct Wire {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    noise: snow::TransportState,
    pub peer: String,
}
fn raw_write(w: &mut impl Write, data: &[u8]) -> Result<()> {
    if data.len() > 65535 {
        bail!("frame too large");
    }
    w.write_all(&(data.len() as u32).to_be_bytes())?;
    w.write_all(data)?;
    Ok(())
}
fn raw_read(r: &mut impl Read) -> Result<Vec<u8>> {
    let mut len = [0; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    if n > 65535 || n == 0 {
        bail!("invalid frame length");
    }
    let mut buf = vec![0; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
impl Wire {
    pub fn handshake(
        stream: TcpStream,
        home: &Path,
        expected: Option<&str>,
        initiator: bool,
    ) -> Result<Self> {
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))?;
        let (_, key) = config::identity(home)?;
        let builder = snow::Builder::new(config::NOISE.parse()?).local_private_key(&key)?;
        let mut noise = if initiator {
            builder.build_initiator()?
        } else {
            builder.build_responder()?
        };
        let mut reader = BufReader::with_capacity(256 * 1024, stream.try_clone()?);
        let mut writer = BufWriter::with_capacity(256 * 1024, stream);
        let mut out = [0; 65535];
        let mut plain = [0; 65535];
        for round in 0..3 {
            if (round % 2 == 0) == initiator {
                let n = noise.write_message(&[], &mut out)?;
                raw_write(&mut writer, &out[..n])?;
                writer.flush()?;
            } else {
                noise.read_message(&raw_read(&mut reader)?, &mut plain)?;
            }
        }
        let peer = blake3::hash(
            noise
                .get_remote_static()
                .context("missing authenticated static key")?,
        )
        .to_hex()
        .to_string();
        if expected.is_some_and(|id| id != peer) {
            bail!("device fingerprint mismatch; connection refused");
        }
        Ok(Self {
            reader,
            writer,
            noise: noise.into_transport_mode()?,
            peer,
        })
    }
    fn frame(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > CHUNK {
            bail!("plaintext frame too large");
        }
        let mut out = vec![0; data.len() + 16];
        let n = self.noise.write_message(data, &mut out)?;
        raw_write(&mut self.writer, &out[..n])
    }
    fn read_frame(&mut self) -> Result<Vec<u8>> {
        let cipher = raw_read(&mut self.reader)?;
        let mut plain = vec![0; cipher.len()];
        let n = self.noise.read_message(&cipher, &mut plain)?;
        plain.truncate(n);
        Ok(plain)
    }
    fn send(&mut self, msg: &Message) -> Result<()> {
        self.send_buffered(msg)?;
        self.writer.flush()?;
        Ok(())
    }
    fn send_buffered(&mut self, msg: &Message) -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        if data.len() > MAX_JSON {
            bail!("metadata batch too large");
        }
        self.frame(&(data.len() as u32).to_be_bytes())?;
        for chunk in data.chunks(CHUNK) {
            self.frame(chunk)?;
        }
        Ok(())
    }
    fn recv(&mut self) -> Result<Message> {
        let header = self.read_frame()?;
        if header.len() != 4 {
            bail!("invalid message header");
        }
        let len = u32::from_be_bytes(header.try_into().unwrap()) as usize;
        if len > MAX_JSON {
            bail!("metadata message too large");
        }
        let mut data = Vec::with_capacity(len);
        while data.len() < len {
            let b = self.read_frame()?;
            if b.is_empty() || data.len() + b.len() > len {
                bail!("invalid message framing");
            }
            data.extend(b);
        }
        Ok(serde_json::from_slice(&data)?)
    }
    // Keep the peer alive while local disk work or a scanner-held gate is slow.
    // Only this thread touches the encrypted wire. At most one scoped worker is
    // active; dropping a connection cancels cooperative reads and gate waits.
    fn preparing<T: Send>(
        &mut self,
        control: &crate::scanning::ScanControl,
        mut check: impl FnMut() -> Result<()>,
        work: impl FnOnce() -> Result<T> + Send,
    ) -> Result<T> {
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let worker = scope.spawn(move || {
                let _ = tx.send(work());
            });
            let result = (|| {
                loop {
                    match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(result) => return result,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            check()?;
                            self.send(&Message::Preparing)?;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            bail!("receiver worker stopped unexpectedly");
                        }
                    }
                }
            })();
            if result.is_err() {
                control.stop();
            }
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("receiver worker panicked"))?;
            result
        })
    }
    fn recv_prepared(&mut self) -> Result<Message> {
        loop {
            match self.recv()? {
                Message::Preparing => continue,
                msg => return Ok(msg),
            }
        }
    }
}

fn hello(shared: &Shared, peer: &str) -> Result<Message> {
    let cfg = config::load(&shared.home)?;
    let p = cfg
        .peers
        .iter()
        .find(|p| p.id == peer && p.approved)
        .context("device awaiting approval")?;
    let c = store::open(&shared.home)?;
    let folders: Vec<String> = cfg
        .folders
        .iter()
        .filter(|f| !f.paused && p.folders.contains(&f.id) && shared.ready(&f.id))
        .map(|f| f.id.clone())
        .collect();
    let mut cursors = BTreeMap::new();
    for f in &folders {
        cursors.insert(f.clone(), store::cursor(&c, peer, f)?);
    }
    Ok(Message::Hello {
        version: 4,
        name: cfg.name,
        folders,
        cursors,
    })
}
fn admitted(shared: &Shared, id: &str) -> Result<bool> {
    let cfg = config::load(&shared.home)?;
    if cfg.peers.iter().any(|p| p.id == id && p.approved) {
        return Ok(true);
    }
    if !cfg.peers.iter().any(|p| p.id == id) && cfg.peers.len() < 128 {
        config::edit(&shared.home, |c| {
            if !c.peers.iter().any(|p| p.id == id) {
                c.peers.push(config::Peer {
                    id: id.into(),
                    name: "Pending device".into(),
                    address: None,
                    approved: false,
                    folders: vec![],
                });
            }
            Ok(())
        })?;
        shared.event(
            "approval",
            None,
            &format!("Device {id} requested a connection"),
        );
    }
    Ok(false)
}
fn root(shared: &Shared, peer: &str, folder: &str) -> Result<Root> {
    let cfg = config::load(&shared.home)?;
    let p = cfg
        .peers
        .iter()
        .find(|p| p.id == peer && p.approved)
        .context("device revoked")?;
    if !p.folders.iter().any(|f| f == folder) {
        bail!("folder not authorized for device");
    }
    let f = cfg
        .folders
        .into_iter()
        .find(|f| f.id == folder && !f.paused)
        .context("folder unavailable or paused")?;
    if !shared.ready(folder) {
        bail!("folder reconciliation not ready");
    }
    Root::open(f, shared.gate(folder))
}
struct BasisFile {
    file: cap_std::fs::File,
    size: u64,
    stamp: String,
}
impl BasisFile {
    fn size(root: &Root, path: &str) -> Option<u64> {
        root.parents(path, false).ok()?;
        let meta = root.dir.symlink_metadata(path).ok()?;
        (meta.is_file() && !meta.is_symlink() && meta.len() >= delta::THRESHOLD)
            .then_some(meta.len())
    }
    fn open(root: &Root, path: &str) -> Option<Self> {
        (|| -> Result<Self> {
            root.parents(path, false)?;
            let meta = root.dir.symlink_metadata(path)?;
            if !meta.is_file() || meta.is_symlink() || meta.len() < delta::THRESHOLD {
                bail!("no eligible basis");
            }
            let file = root.dir.open(path)?;
            let stamp = engine::stamp(&meta);
            if engine::stamp(&file.metadata()?) != stamp {
                bail!("basis changed while opening");
            }
            Ok(Self {
                file,
                size: meta.len(),
                stamp,
            })
        })()
        .ok()
    }
    fn signature(
        &mut self,
        params: Parameters,
        progress: impl FnMut() -> Result<()>,
    ) -> Result<Vec<Block>> {
        if engine::stamp(&self.file.metadata()?) != self.stamp {
            bail!("basis changed before indexing");
        }
        self.file.seek(SeekFrom::Start(0))?;
        let blocks = delta::signature(
            &mut self.file,
            0,
            self.size,
            params,
            &mut blake3::Hasher::new(),
            progress,
        )?;
        if engine::stamp(&self.file.metadata()?) != self.stamp {
            bail!("basis changed during indexing");
        }
        Ok(blocks)
    }
}
fn send_payload(
    w: &mut Wire,
    shared: &Shared,
    file: &mut impl Read,
    mut size: u64,
    hash: &mut blake3::Hasher,
) -> Result<()> {
    let mut buf = [0; CHUNK];
    while size > 0 {
        let count = size.min(CHUNK as u64) as usize;
        let n = file.read(&mut buf[..count])?;
        if n == 0 {
            bail!("source shortened during transfer");
        }
        hash.update(&buf[..n]);
        w.frame(&buf[..n])?;
        size -= n as u64;
        shared.bytes_sent(n as u64);
    }
    Ok(())
}
fn receive_payload(
    w: &mut Wire,
    shared: &Shared,
    partial: &mut Partial,
    mut size: u64,
    mut hash: Option<&mut blake3::Hasher>,
) -> Result<()> {
    while size > 0 {
        let bytes = w.read_frame()?;
        if bytes.is_empty() || bytes.len() as u64 > size {
            bail!("invalid file payload");
        }
        partial.file.write_all(&bytes)?;
        partial.hash.update(&bytes);
        if let Some(hash) = &mut hash {
            hash.update(&bytes);
        }
        size -= bytes.len() as u64;
        shared.bytes_received(bytes.len() as u64);
    }
    Ok(())
}
fn receive_delta(
    w: &mut Wire,
    shared: &Shared,
    partial: &mut Partial,
    basis: &mut BasisFile,
    operations: &[Operation],
) -> Result<u64> {
    let mut reused = 0;
    let mut tick = Instant::now();
    for op in operations {
        let start = partial.file.stream_position()?;
        let mut hash = blake3::Hasher::new();
        let result = (|| -> Result<()> {
            if op.basis_offset.is_some() {
                delta::copy_verified(
                    &mut basis.file,
                    &mut partial.file,
                    op,
                    &mut partial.hash,
                    || {
                        if tick.elapsed() >= Duration::from_secs(1) {
                            w.send(&Message::Preparing)?;
                            tick = Instant::now();
                        }
                        Ok(())
                    },
                )?;
            } else {
                receive_payload(w, shared, partial, op.len, Some(&mut hash))?;
                if hash.finalize().to_hex().as_str() != op.hash {
                    bail!("delta literal checksum mismatch");
                }
            }
            Ok(())
        })();
        if result.is_err() && op.basis_offset.is_some() {
            // Never retain an unverified local copy in a resumable prefix.
            partial.file.set_len(start)?;
        }
        result?;
        if op.basis_offset.is_some() {
            reused += op.len;
        }
    }
    Ok(reused)
}

fn send_batch(w: &mut Wire, shared: &Shared, folder: &str, after: u64) -> Result<u64> {
    let root = root(shared, &w.peer, folder)?;
    root.check()?;
    let c = store::open(&shared.home)?;
    let entries = store::changes(&c, folder, after, 128)?;
    let upto = entries.last().map(|e| e.seq).unwrap_or(after);
    let entries: Vec<_> = entries
        .into_iter()
        .filter(|e| !root.excluded(&e.path))
        .collect();
    w.send(&Message::Batch {
        folder: folder.into(),
        entries: entries.clone(),
        upto,
    })?;
    let want = match w
        .recv_prepared()
        .with_context(|| format!("waiting for {folder} batch {upto} payload requests"))?
    {
        Message::Want(v) if v.len() == entries.len() => v,
        _ => bail!("expected file resume requests"),
    };
    for (e, wanted) in entries.iter().zip(want) {
        let Some(request) = wanted else {
            continue;
        };
        if e.kind != Kind::File {
            bail!("peer requested data for non-file");
        }
        root.parents(&e.path, false)?;
        let mut file = root.dir.open(&e.path)?;
        let initial = engine::stamp(&file.metadata()?);
        let (offset, mut hash) =
            accept_resume(&mut file, e.size, &request, || w.send(&Message::Preparing))?;
        let use_delta = request
            .basis_size
            .is_some_and(|size| size >= delta::THRESHOLD && size <= i64::MAX as u64)
            && e.size - offset >= delta::THRESHOLD;
        w.send_buffered(&Message::FileStart {
            offset,
            delta: use_delta,
        })?;
        let mut used_delta = false;
        if use_delta {
            w.writer.flush()?;
            let basis_size = request.basis_size.unwrap();
            let params = Parameters::for_sizes(e.size, basis_size);
            match w.recv_prepared()? {
                Message::Basis {
                    blocks: Some(blocks),
                } => {
                    delta::validate_basis(&blocks, basis_size, params)?;
                    let source = delta::signature(
                        &mut file,
                        offset,
                        e.size - offset,
                        params,
                        &mut hash,
                        || w.send(&Message::Preparing),
                    )?;
                    if hash.finalize().to_hex().as_str() != e.hash
                        || engine::stamp(&file.metadata()?) != initial
                    {
                        bail!("source changed during delta indexing: {}", e.path);
                    }
                    let operations = delta::plan(&source, &blocks);
                    w.send_buffered(&Message::DeltaPlan {
                        operations: Some(operations.clone()),
                    })?;
                    for (block, op) in source.iter().zip(&operations) {
                        if op.basis_offset.is_none() {
                            file.seek(SeekFrom::Start(block.offset))?;
                            let mut block_hash = blake3::Hasher::new();
                            send_payload(w, shared, &mut file, block.len, &mut block_hash)?;
                            if block_hash.finalize().to_hex().as_str() != block.hash {
                                bail!("source changed while sending delta block");
                            }
                        }
                    }
                    used_delta = true;
                }
                Message::Basis { blocks: None } => {
                    w.send_buffered(&Message::DeltaPlan { operations: None })?
                }
                _ => bail!("expected delta basis"),
            }
        }
        if !used_delta {
            send_payload(w, shared, &mut file, e.size - offset, &mut hash)?;
        }
        let valid = hash.finalize().to_hex().as_str() == e.hash
            && engine::stamp(&file.metadata()?) == initial;
        w.send_buffered(&Message::FileEnd { valid })?;
        if !valid {
            bail!("source changed during transfer: {}", e.path);
        }
        shared.file_sent(folder, &e.path);
    }
    w.writer.flush()?;
    match w
        .recv_prepared()
        .with_context(|| format!("waiting for {folder} batch {upto} durable acknowledgement"))?
    {
        Message::Ack { upto: n } if n == upto => Ok(n),
        _ => bail!("batch not acknowledged"),
    }
}
fn receive_batch(w: &mut Wire, shared: &Shared, expected_folder: &str) -> Result<()> {
    let (folder, entries, upto) = match w.recv()? {
        Message::Batch {
            folder,
            entries,
            upto,
        } if folder == expected_folder && entries.len() <= 128 => (folder, entries, upto),
        _ => bail!("invalid batch"),
    };
    let mut root = root(shared, &w.peer, &folder)?;
    let control = Arc::new(crate::scanning::ScanControl::default());
    root.scan_control = Some(control.clone());
    let peer = w.peer.clone();
    let check = |detail: &'static str| {
        let peer = &peer;
        let folder = &folder;
        let mut reported = false;
        move || -> Result<()> {
            if shared.stopping() {
                bail!("daemon stopping");
            }
            self::root(shared, peer, folder)?.check()?;
            if !reported {
                shared.event("preparing", Some(folder), detail);
                reported = true;
            }
            Ok(())
        }
    };
    let (mut c, prior, wants) = w.preparing(
        &control,
        check("Checking destination files and index"),
        || {
            let c = store::open(&shared.home)?;
            let prior = store::cursor(&c, &peer, &folder)?;
            if upto < prior
                || entries.windows(2).any(|v| v[0].seq >= v[1].seq)
                || entries.iter().any(|e| e.seq <= prior || e.seq > upto)
            {
                bail!("invalid change sequence");
            }
            let wants = entries
                .iter()
                .map(|e| engine::wants(&c, &root, e))
                .collect::<Result<Vec<_>>>()?;
            Ok((c, prior, wants))
        },
    )?;
    let mut partials = BTreeMap::new();
    let mut requests = Vec::with_capacity(entries.len());
    for (entry, wanted) in entries.iter().zip(wants) {
        if wanted {
            let mut partial = Partial::open(&root, &peer, entry, || w.send(&Message::Preparing))?;
            if entry.size >= delta::THRESHOLD
                && let Some(size) = BasisFile::size(&root, &entry.path)
            {
                partial.request.basis_size = Some(size);
            }
            requests.push(Some(partial.request.clone()));
            partials.insert(entry.path.clone(), partial);
        } else {
            requests.push(None);
        }
    }
    w.send(&Message::Want(requests))?;
    if entries.is_empty() && upto == prior {
        w.send(&Message::Ack { upto })?;
        return Ok(());
    }
    let result = (|| -> Result<()> {
        for e in &entries {
            let Some(partial) = partials.get_mut(&e.path) else {
                continue;
            };
            let (offset, use_delta) = match w.recv_prepared()? {
                Message::FileStart { offset, delta } => (offset, delta),
                _ => bail!("expected file start"),
            };
            partial.start(offset)?;
            if offset > 0 {
                shared.resumed(&folder, &e.path, offset);
            }
            let mut used_delta = false;
            if use_delta {
                let basis_size = partial
                    .request
                    .basis_size
                    .context("unrequested delta transfer")?;
                let mut basis = BasisFile::open(&root, &e.path).filter(|b| b.size == basis_size);
                let params = Parameters::for_sizes(e.size, basis_size);
                let blocks = basis
                    .as_mut()
                    .and_then(|b| b.signature(params, || w.send(&Message::Preparing)).ok());
                let offered = blocks.is_some();
                w.send(&Message::Basis { blocks })?;
                match w.recv_prepared()? {
                    Message::DeltaPlan {
                        operations: Some(operations),
                    } if offered => {
                        delta::validate_plan(&operations, e.size - offset, basis_size, params)?;
                        let reused = receive_delta(
                            w,
                            shared,
                            partial,
                            basis.as_mut().unwrap(),
                            &operations,
                        )?;
                        if reused > 0 {
                            shared.delta_reused(&folder, &e.path, reused);
                        }
                        used_delta = true;
                    }
                    Message::DeltaPlan { operations: None } => {}
                    _ => bail!("invalid delta plan response"),
                }
            }
            if !used_delta {
                receive_payload(w, shared, partial, e.size - offset, None)?;
            }
            match w.recv()? {
                Message::FileEnd { valid: true } => {}
                _ => bail!("source changed during transfer"),
            }
            if partial.hash.finalize().to_hex().as_str() != e.hash {
                partial.file.set_len(0)?;
                bail!("content hash mismatch for {}", e.path);
            }
            use std::os::unix::fs::PermissionsExt;
            partial
                .file
                .set_permissions(std::fs::Permissions::from_mode(e.mode))?;
        }
        {
            let c = &mut c;
            let root = &root;
            let partials = &partials;
            let entries = &entries;
            let folder = &folder;
            let peer = &peer;
            let control = &control;
            w.preparing(
                control,
                check("Waiting for scanner access or committing received files"),
                move || {
                    let flushes: Vec<_> = partials.values().map(|p| &p.file).collect();
                    // Overlap durable file flushes with a bounded worker count. All must finish before publication.
                    std::thread::scope(|scope| -> Result<()> {
                        let workers = 8.min(flushes.len());
                        if workers == 0 {
                            return Ok(());
                        }
                        let mut handles = Vec::new();
                        for group in flushes.chunks(flushes.len().div_ceil(workers)) {
                            handles.push(scope.spawn(move || -> std::io::Result<()> {
                                for file in group {
                                    file.sync_all()?;
                                }
                                Ok(())
                            }));
                        }
                        for handle in handles {
                            handle
                                .join()
                                .map_err(|_| anyhow::anyhow!("flush worker panicked"))??;
                        }
                        Ok(())
                    })?;
                    let _guard = loop {
                        control.checkpoint()?;
                        match root.gate.try_lock() {
                            Ok(guard) => break guard,
                            Err(std::sync::TryLockError::WouldBlock) => {
                                std::thread::sleep(Duration::from_millis(50))
                            }
                            Err(std::sync::TryLockError::Poisoned(_)) => {
                                bail!("folder gate poisoned")
                            }
                        }
                    };
                    // Recheck permissions and pause/revocation after the data transfer, before any publication.
                    let _ = self::root(shared, peer, folder)?;
                    let tx =
                        c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    // Children must be removed before their deleted parents, irrespective of journal order.
                    let mut ordered: Vec<&Entry> = entries.iter().collect();
                    ordered.sort_by(|a, b| match (&a.kind, &b.kind) {
                        (Kind::Deleted, Kind::Deleted) => b.path.len().cmp(&a.path.len()),
                        (Kind::Deleted, _) => std::cmp::Ordering::Greater,
                        (_, Kind::Deleted) => std::cmp::Ordering::Less,
                        _ => a.path.len().cmp(&b.path.len()),
                    });
                    for e in ordered {
                        control.checkpoint()?;
                        let conflict = engine::apply(
                            &tx,
                            root,
                            &shared.id,
                            e,
                            partials.get(&e.path).map(|p| p.name.as_str()),
                        )?;
                        if conflict {
                            shared.event("conflict", Some(folder), &e.path);
                        }
                    }
                    control.checkpoint()?;
                    engine::sync_directories(root, entries)?;
                    store::set_cursor(&tx, peer, folder, upto)?;
                    tx.commit()?;
                    for e in entries {
                        if !root.excluded(&e.path) {
                            shared.file_received(folder, &e.path);
                        }
                    }
                    Ok(())
                },
            )?;
        }
        w.send(&Message::Ack { upto })?;
        Ok(())
    })();
    if result.is_ok() {
        for partial in partials.values() {
            let _ = root.dir.remove_file(&partial.name);
        }
    }
    result
}

pub fn session(
    stream: TcpStream,
    shared: Arc<Shared>,
    expected: Option<String>,
    initiator: bool,
) -> Result<()> {
    let mut w = Wire::handshake(stream, &shared.home, expected.as_deref(), initiator)?;
    if w.peer == shared.id {
        bail!("cannot synchronize with self");
    }
    if !admitted(&shared, &w.peer)? {
        w.send(&Message::Denied {
            reason: "Approve the device fingerprint and folder grants on the receiving machine"
                .into(),
        })?;
        bail!("device awaiting approval");
    }
    // Bidirectional peers can dial simultaneously. Only one session acquires the per-device gate.
    let gate = shared.peer_gate(&w.peer);
    let Ok(_guard) = gate.try_lock() else {
        bail!("device already connected");
    };
    let peer = w.peer.clone();
    shared.connected(&peer, true);
    let result = (|| -> Result<()> {
        w.send(&hello(&shared, &peer)?)?;
        let (remote_folders, mut remote_cursors) = match w.recv()? {
            Message::Hello {
                version: 4,
                folders,
                cursors,
                ..
            } if folders.len() <= 128 => (folders, cursors),
            Message::Denied { reason } => bail!("peer denied connection: {reason}"),
            _ => bail!("incompatible protocol"),
        };
        let local = match hello(&shared, &peer)? {
            Message::Hello { folders, .. } => folders,
            _ => unreachable!(),
        };
        let mut folders: Vec<_> = local
            .into_iter()
            .filter(|f| remote_folders.contains(f))
            .collect();
        folders.sort();
        if folders.is_empty() {
            bail!("no approved, scanned folders in common");
        }
        let mut rounds = 0;
        while !shared.stopping() {
            for f in &folders {
                if initiator {
                    let n = send_batch(&mut w, &shared, f, *remote_cursors.get(f).unwrap_or(&0))?;
                    remote_cursors.insert(f.clone(), n);
                    receive_batch(&mut w, &shared, f)?;
                } else {
                    receive_batch(&mut w, &shared, f)?;
                    let n = send_batch(&mut w, &shared, f, *remote_cursors.get(f).unwrap_or(&0))?;
                    remote_cursors.insert(f.clone(), n);
                }
            }
            // Brief fairness yield; large bootstrap batches continue without a per-file round trip.
            if initiator {
                w.send(&Message::Turn)?;
                match w.recv()? {
                    Message::Turn => {}
                    _ => bail!("expected turn boundary"),
                }
            } else {
                match w.recv()? {
                    Message::Turn => {}
                    _ => bail!("expected turn boundary"),
                };
                w.send(&Message::Turn)?;
            }
            rounds += 1;
            if rounds % 100 == 0 {
                break;
            } // Refresh folder grants and shared-folder list without a daemon restart.
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    })();
    shared.connected(&peer, false);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_encrypted_round_trip_and_pin_rejection() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        config::initialize(a.path(), None, None).unwrap();
        let bid = config::initialize(b.path(), None, None).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let home = b.path().to_owned();
        let t = std::thread::spawn(move || {
            let mut w = Wire::handshake(listener.accept().unwrap().0, &home, None, false).unwrap();
            assert!(matches!(w.recv().unwrap(), Message::Turn));
            w.send(&Message::Ack { upto: 42 }).unwrap();
        });
        let mut w = Wire::handshake(
            TcpStream::connect(addr).unwrap(),
            a.path(),
            Some(&bid),
            true,
        )
        .unwrap();
        w.send(&Message::Turn).unwrap();
        assert!(matches!(w.recv().unwrap(), Message::Ack { upto: 42 }));
        t.join().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let home = b.path().to_owned();
        let t = std::thread::spawn(move || {
            let _ = Wire::handshake(listener.accept().unwrap().0, &home, None, false);
        });
        assert!(
            Wire::handshake(
                TcpStream::connect(addr).unwrap(),
                a.path(),
                Some(&"0".repeat(64)),
                true
            )
            .is_err()
        );
        t.join().unwrap();
    }
    #[derive(Clone, Copy, PartialEq)]
    enum GateAction {
        Release,
        Pause,
        Disconnect,
        StoreBusy,
    }

    #[test]
    fn receiver_ack_survives_scanner_gate_delay() {
        delayed_receiver(GateAction::Release);
    }
    #[test]
    fn paused_receiver_cancels_while_scanner_gate_is_held() {
        delayed_receiver(GateAction::Pause);
    }
    #[test]
    fn disconnected_receiver_cancels_while_scanner_gate_is_held() {
        delayed_receiver(GateAction::Disconnect);
    }

    #[test]
    fn receiver_preflight_survives_database_writer_delay() {
        delayed_receiver(GateAction::StoreBusy);
    }

    fn delayed_receiver(action: GateAction) {
        let sender_home = tempfile::tempdir().unwrap();
        let receiver_home = tempfile::tempdir().unwrap();
        let sender_files = tempfile::tempdir().unwrap();
        let receiver_files = tempfile::tempdir().unwrap();
        let sender_id = config::initialize(sender_home.path(), None, None).unwrap();
        let receiver_id = config::initialize(receiver_home.path(), None, None).unwrap();
        for (home, files, peer) in [
            (sender_home.path(), sender_files.path(), &receiver_id),
            (receiver_home.path(), receiver_files.path(), &sender_id),
        ] {
            engine::add_folder(home, "code", files, true).unwrap();
            config::edit(home, |c| {
                c.peers.push(config::Peer {
                    id: peer.clone(),
                    name: "peer".into(),
                    address: None,
                    approved: true,
                    folders: vec!["code".into()],
                });
                Ok(())
            })
            .unwrap();
        }
        let sender = Shared::new(sender_home.path(), sender_id.clone(), String::new());
        let receiver = Shared::new(receiver_home.path(), receiver_id.clone(), String::new());
        sender.mark_ready_for_test("code");
        receiver.mark_ready_for_test("code");
        std::fs::write(sender_files.path().join("probe"), b"new file").unwrap();
        let source = root(&sender, &receiver_id, "code").unwrap();
        engine::scan(&source, sender_home.path(), &sender_id, None, |_| {}).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let gate = receiver.gate("code");
        // Emulate the scanner holding its folder gate during expensive hashing.
        let guard = gate.lock().unwrap();
        let receiver_db = store::open(receiver_home.path()).unwrap();
        if action == GateAction::StoreBusy {
            receiver_db.execute_batch("BEGIN IMMEDIATE").unwrap();
        }
        let (socket_tx, socket_rx) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let result = std::thread::scope(|scope| {
            let receiver_task = scope.spawn(|| {
                let mut wire = Wire::handshake(
                    listener.accept().unwrap().0,
                    receiver_home.path(),
                    None,
                    false,
                )
                .unwrap();
                let result = receive_batch(&mut wire, &receiver, "code");
                done_tx.send(()).unwrap();
                result
            });
            let sender_task = scope.spawn(|| {
                let mut wire = Wire::handshake(
                    TcpStream::connect(addr).unwrap(),
                    sender_home.path(),
                    Some(&receiver_id),
                    true,
                )
                .unwrap();
                wire.reader
                    .get_ref()
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket_tx
                    .send(wire.reader.get_ref().try_clone().unwrap())
                    .unwrap();
                send_batch(&mut wire, &sender, "code", 0)
            });
            let socket = socket_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            std::thread::sleep(Duration::from_secs(4));
            assert!(!receiver_files.path().join("probe").exists());
            assert_eq!(store::cursor(&receiver_db, &sender_id, "code").unwrap(), 0);
            match action {
                GateAction::Release => {}
                GateAction::StoreBusy => receiver_db.execute_batch("COMMIT").unwrap(),
                GateAction::Pause => config::edit(receiver_home.path(), |c| {
                    c.folders[0].paused = true;
                    Ok(())
                })
                .unwrap(),
                GateAction::Disconnect => socket.shutdown(std::net::Shutdown::Both).unwrap(),
            }
            let cancelled = matches!(action, GateAction::Release | GateAction::StoreBusy)
                || done_rx.recv_timeout(Duration::from_secs(6)).is_ok();
            drop(guard);
            let sent = sender_task.join().unwrap();
            let received = receiver_task.join().unwrap();
            (sent, received, cancelled)
        });
        if matches!(action, GateAction::Pause | GateAction::Disconnect) {
            assert!(
                result.2,
                "cancelled receiver remained blocked on scanner gate"
            );
            assert!(result.0.is_err());
            assert!(result.1.is_err());
            assert!(!receiver_files.path().join("probe").exists());
            assert_eq!(
                store::cursor(
                    &store::open(receiver_home.path()).unwrap(),
                    &sender_id,
                    "code"
                )
                .unwrap(),
                0
            );
            return;
        }
        assert!(
            result.0.is_ok(),
            "sender timed out before durable acknowledgement: {:?}",
            result.0
        );
        result.1.unwrap();
        assert_eq!(
            std::fs::read(receiver_files.path().join("probe")).unwrap(),
            b"new file"
        );
        assert_eq!(
            store::cursor(
                &store::open(receiver_home.path()).unwrap(),
                &sender_id,
                "code"
            )
            .unwrap(),
            result.0.unwrap()
        );
    }

    #[test]
    fn rejects_oversized_wire_frames() {
        let mut bytes = &[0u8, 1, 0, 0][..];
        assert!(raw_read(&mut bytes).is_err());
    }
}
