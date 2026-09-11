use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use ysync::{config, engine};

struct Device {
    state: TempDir,
    files: TempDir,
    id: String,
    address: String,
    child: Option<Child>,
}
impl Device {
    fn new(name: &str) -> Self {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let id =
            config::initialize(state.path(), Some(name.into()), Some(address.clone())).unwrap();
        engine::add_folder(state.path(), "code", files.path(), true).unwrap();
        config::edit(state.path(), |c| {
            c.rescan_secs = 5;
            Ok(())
        })
        .unwrap();
        Self {
            state,
            files,
            id,
            address,
            child: None,
        }
    }
    fn start(&mut self) {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.state.path().join("test.log"))
            .unwrap();
        self.child = Some(
            Command::new(
                std::env::var("YSYNC_TEST_BINARY")
                    .unwrap_or_else(|_| env!("CARGO_BIN_EXE_ysync").into()),
            )
            .arg("--home")
            .arg(self.state.path())
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap(),
        );
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
    fn path(&self, p: &str) -> PathBuf {
        self.files.path().join(p)
    }
    fn write(&self, p: &str, data: &[u8]) {
        let path = self.path(p);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, data).unwrap();
    }
    fn dial(&self, other: &Self) {
        config::edit(self.state.path(), |c| {
            c.peers.push(config::Peer {
                id: other.id.clone(),
                name: "test-peer".into(),
                address: Some(other.address.clone()),
                approved: true,
                folders: vec!["code".into()],
            });
            Ok(())
        })
        .unwrap();
    }
    fn approve(&self, other: &Self) {
        config::edit(self.state.path(), |c| {
            let p = c.peers.iter_mut().find(|p| p.id == other.id).unwrap();
            p.approved = true;
            p.folders = vec!["code".into()];
            Ok(())
        })
        .unwrap();
    }
}
impl Drop for Device {
    fn drop(&mut self) {
        self.stop();
    }
}
fn wait(label: &str, a: &Device, b: &Device, condition: impl Fn() -> bool) {
    let until = Instant::now() + Duration::from_secs(60);
    while Instant::now() < until {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{label} timed out\nA: {}\nB: {}",
        format_args!(
            "{}\n{}",
            fs::read_to_string(a.state.path().join("test.log")).unwrap_or_default(),
            fs::read_to_string(a.state.path().join("status.json")).unwrap_or_default()
        ),
        format_args!(
            "{}\n{}",
            fs::read_to_string(b.state.path().join("test.log")).unwrap_or_default(),
            fs::read_to_string(b.state.path().join("status.json")).unwrap_or_default()
        )
    );
}
fn equals(d: &Device, p: &str, bytes: &[u8]) -> bool {
    fs::read(d.path(p)).is_ok_and(|b| b == bytes)
}

fn settled(a: &Device, b: &Device) {
    wait("durable common baseline", a, b, || {
        if std::env::var("YSYNC_REQUIRE_NATIVE_WATCHERS").is_ok() {
            for d in [a, b] {
                let Ok(status) = ysync::daemon::read_status(d.state.path()) else {
                    return false;
                };
                if status
                    .folders
                    .get("code")
                    .is_none_or(|f| f.watcher != "native")
                {
                    return false;
                }
            }
        }
        let snapshot = |d: &Device| {
            let c = ysync::store::open(d.state.path()).unwrap();
            let mut entries = ysync::store::changes(&c, "code", 0, 10000).unwrap();
            entries.sort_by(|a, b| a.path.cmp(&b.path));
            entries
                .into_iter()
                .map(|e| (e.path, format!("{:?}", e.kind), e.hash, e.clock))
                .collect::<Vec<_>>()
        };
        snapshot(a) == snapshot(b)
    });
}

#[test]
fn scoped_success_does_not_clear_incomplete_full_scan() {
    let mut device = Device::new("incomplete-scan");
    config::edit(device.state.path(), |c| {
        c.rescan_secs = 3600;
        Ok(())
    })
    .unwrap();
    let socket = std::os::unix::net::UnixListener::bind(device.path("unsupported.sock")).unwrap();
    device.start();
    wait("incomplete initial scan", &device, &device, || {
        ysync::daemon::read_status(device.state.path())
            .is_ok_and(|s| s.folders.get("code").is_some_and(|f| f.error.is_some()))
    });
    assert!(
        ysync::daemon::read_status(device.state.path())
            .unwrap()
            .folders["code"]
            .checked_entries
            >= 1
    );
    device.write("ordinary.txt", b"watcher still works");
    wait("ordinary file indexed", &device, &device, || {
        ysync::daemon::read_status(device.state.path()).is_ok_and(|s| {
            s.folders.get("code").is_some_and(|f| {
                f.scoped_scans > 0 && f.hashed_files > 0 && f.phase == "incomplete"
            })
        })
    });
    let s = ysync::daemon::read_status(device.state.path()).unwrap();
    assert!(
        s.folders["code"]
            .error
            .as_ref()
            .unwrap()
            .contains("unsupported.sock")
    );
    assert_eq!(s.folders["code"].full_scans, 1);
    assert!(s.folders["code"].checked_entries >= 2);
    drop(socket);
    fs::remove_file(device.path("unsupported.sock")).unwrap();
    // An ignore-file notification explicitly requests a complete reconciliation.
    device.write(".ysyncignore", b"# retry full reconciliation\n");
    wait("full scan repairs health", &device, &device, || {
        ysync::daemon::read_status(device.state.path()).is_ok_and(|s| {
            s.folders
                .get("code")
                .is_some_and(|f| f.full_scans > 1 && f.error.is_none() && f.phase == "watching")
        })
    });
}

#[test]
fn two_daemons_pair_replicate_watch_conflict_and_revoke() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.write("hello.txt", b"initial");
    a.write("node_modules/ignored.js", b"do not send");
    fs::create_dir_all(a.path("empty")).unwrap();
    for n in 0..260 {
        a.write(&format!("many/f{n:04}.txt"), format!("file {n}").as_bytes());
    }
    std::os::unix::fs::symlink("hello.txt", a.path("link")).unwrap();
    std::os::unix::fs::symlink("/ysync-test-external-target", a.path("absolute-link")).unwrap();
    a.dial(&b);
    b.start();
    a.start();
    wait("pending approval", &a, &b, || {
        config::load(b.state.path())
            .unwrap()
            .peers
            .iter()
            .any(|p| p.id == a.id && !p.approved)
    });
    assert!(
        !b.path("hello.txt").exists(),
        "unapproved peer received file content"
    );
    b.approve(&a);
    wait("initial replication", &a, &b, || {
        equals(&b, "hello.txt", b"initial")
            && b.path("many/f0259.txt").exists()
            && b.path("many/f0000.txt").exists()
            && b.path("link").is_symlink()
            && b.path("empty").is_dir()
    });
    for n in 0..260 {
        wait("all bootstrap files", &a, &b, || {
            equals(
                &b,
                &format!("many/f{n:04}.txt"),
                format!("file {n}").as_bytes(),
            )
        });
    }
    wait("absolute symlink", &a, &b, || {
        std::fs::read_link(b.path("absolute-link"))
            .is_ok_and(|p| p == std::path::Path::new("/ysync-test-external-target"))
    });
    assert!(!b.path("node_modules").exists());
    settled(&a, &b);
    std::fs::remove_file(a.path("absolute-link")).unwrap();
    std::os::unix::fs::symlink("/ysync-test-second-target", a.path("absolute-link")).unwrap();
    wait("symlink replacement", &a, &b, || {
        std::fs::read_link(b.path("absolute-link"))
            .is_ok_and(|p| p == std::path::Path::new("/ysync-test-second-target"))
    });
    b.write("from-b.txt", b"reverse direction");
    wait("reverse direction", &a, &b, || {
        equals(&a, "from-b.txt", b"reverse direction")
    });
    settled(&a, &b);
    a.write("hello.txt", b"changed");
    wait("watcher edit", &a, &b, || {
        equals(&b, "hello.txt", b"changed")
    });
    settled(&a, &b);
    fs::rename(a.path("from-b.txt"), a.path("renamed.txt")).unwrap();
    wait("rename and deletion", &a, &b, || {
        equals(&b, "renamed.txt", b"reverse direction") && !b.path("from-b.txt").exists()
    });
    settled(&a, &b);
    fs::remove_dir_all(a.path("many")).unwrap();
    wait("directory deletion across batches", &a, &b, || {
        !b.path("many").exists()
    });
    // Let both devices acknowledge the common base before taking them offline.
    wait("common base", &a, &b, || {
        let x = ysync::store::open(a.state.path()).unwrap();
        let y = ysync::store::open(b.state.path()).unwrap();
        let l = ysync::store::get(&x, "code", "hello.txt").unwrap().unwrap();
        let r = ysync::store::get(&y, "code", "hello.txt").unwrap().unwrap();
        l.clock == r.clock
    });
    a.stop();
    b.stop();
    a.write("hello.txt", b"offline edit A");
    b.write("hello.txt", b"offline edit B");
    a.start();
    b.start();
    wait("offline conflict convergence", &a, &b, || {
        let x = fs::read(a.path("hello.txt")).unwrap();
        let y = fs::read(b.path("hello.txt")).unwrap();
        x == y && (a.path(".ysync/conflicts").is_dir() || b.path(".ysync/conflicts").is_dir())
    });
    let mut preserved = Vec::new();
    for d in [&a, &b] {
        if let Ok(entries) = fs::read_dir(d.path(".ysync/conflicts")) {
            for e in entries.flatten() {
                if let Ok(data) = fs::read(e.path()) {
                    preserved.push(data);
                }
            }
        }
    }
    let winner = fs::read(a.path("hello.txt")).unwrap();
    let loser = if winner == b"offline edit A" {
        b"offline edit B"
    } else {
        b"offline edit A"
    };
    assert!(
        preserved.iter().any(|v| v == loser),
        "losing edit was not retained"
    );
    config::edit(b.state.path(), |c| {
        c.peers[0].approved = false;
        Ok(())
    })
    .unwrap();
    thread::sleep(Duration::from_secs(2));
    a.write("after-revoke.txt", b"not authorized");
    thread::sleep(Duration::from_secs(3));
    assert!(!b.path("after-revoke.txt").exists());
}

#[test]
fn restart_resumes_and_preserves_large_binary() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    config::edit(b.state.path(), |c| {
        c.peers.push(config::Peer {
            id: a.id.clone(),
            name: "a".into(),
            address: None,
            approved: true,
            folders: vec!["code".into()],
        });
        Ok(())
    })
    .unwrap();
    let data: Vec<u8> = (0..2_000_000).map(|n| (n % 251) as u8).collect();
    a.write("large.bin", &data);
    a.write("zero", b"");
    a.start();
    b.start();
    wait("large binary", &a, &b, || {
        equals(&b, "large.bin", &data) && equals(&b, "zero", b"")
    });
    a.stop();
    b.stop();
    a.write("after-restart.txt", b"resume");
    a.start();
    b.start();
    wait("restart", &a, &b, || {
        equals(&b, "after-restart.txt", b"resume")
    });
    assert!(equals(&b, "large.bin", &data));
    settled(&a, &b);
    let c = ysync::store::open(b.state.path()).unwrap();
    assert!(ysync::store::cursor(&c, &a.id, "code").unwrap() > 0);
}

#[test]
fn interrupted_payload_is_never_published_and_reconnect_retries() {
    interrupted_transfer(false);
}

#[test]
fn corrupted_partial_is_retransmitted_in_full() {
    interrupted_transfer(true);
}

#[test]
fn sender_disconnect_preserves_partial_without_restarting_receiver() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let data = vec![0x91; 64 * 1024 * 1024];
    a.write("disconnected.bin", &data);
    a.start();
    b.start();
    wait("streaming payload", &a, &b, || {
        fs::read_dir(b.path(".ysync/tmp")).is_ok_and(|es| {
            es.flatten()
                .any(|e| e.metadata().is_ok_and(|m| m.len() > 0))
        })
    });
    a.stop();
    wait("receiver observes connection loss", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.received_bytes > 0 && !s.connected_peers.contains(&a.id))
    });
    assert!(!b.path("disconnected.bin").exists());
    let partial = fs::read_dir(b.path(".ysync/tmp"))
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().ends_with(".part"))
        .expect("connection error must preserve the partial")
        .path();
    let saved = fs::metadata(&partial).unwrap().len();
    assert!(saved > 0 && saved < data.len() as u64);
    a.start();
    wait("resume after sender restart", &a, &b, || {
        equals(&b, "disconnected.bin", &data)
    });
    settled(&a, &b);
    wait("no duplicate payload after reconnect", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.resumed_bytes == saved && s.received_bytes == data.len() as u64)
    });
    assert!(!partial.exists());
}

fn interrupted_transfer(corrupt: bool) {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a); // Exercise simultaneous dialing as well as reconnect.
    let data = vec![0x6d; 16 * 1024 * 1024];
    a.write("payload.bin", &data);
    a.start();
    b.start();
    wait("partial staging file", &a, &b, || {
        fs::read_dir(b.path(".ysync/tmp")).is_ok_and(|es| {
            es.flatten().any(|e| {
                e.metadata()
                    .is_ok_and(|m| m.len() > 0 && m.len() < data.len() as u64)
            })
        })
    });
    b.stop();
    assert!(
        !b.path("payload.bin").exists(),
        "incomplete payload was exposed"
    );
    let partial = fs::read_dir(b.path(".ysync/tmp"))
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().ends_with(".part"))
        .expect("restartable partial must survive receiver crash")
        .path();
    let saved = fs::metadata(&partial).unwrap().len();
    assert!(saved > 0 && saved < data.len() as u64);
    if corrupt {
        use std::io::Write;
        let mut file = fs::OpenOptions::new().write(true).open(&partial).unwrap();
        file.write_all(b"corrupted!").unwrap();
        file.sync_all().unwrap();
    }
    b.start();
    wait("retry after receiver crash", &a, &b, || {
        equals(&b, "payload.bin", &data)
    });
    settled(&a, &b);
    let expected_reused = if corrupt { 0 } else { saved };
    wait("resume payload accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path()).is_ok_and(|s| {
            s.resumed_bytes == expected_reused
                && s.received_bytes == data.len() as u64 - expected_reused
        })
    });
    assert!(!partial.exists(), "published partial was not cleaned up");
}

#[test]
fn pause_and_folder_grants_are_enforced() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let secret_a = tempfile::tempdir().unwrap();
    let secret_b = tempfile::tempdir().unwrap();
    engine::add_folder(a.state.path(), "private", secret_a.path(), false).unwrap();
    engine::add_folder(b.state.path(), "private", secret_b.path(), false).unwrap();
    fs::write(secret_a.path().join("secret.txt"), b"never shared").unwrap();
    a.write("base", b"base");
    a.start();
    b.start();
    wait("base", &a, &b, || equals(&b, "base", b"base"));
    settled(&a, &b);
    config::edit(b.state.path(), |c| {
        c.folders
            .iter_mut()
            .find(|f| f.id == "code")
            .unwrap()
            .paused = true;
        Ok(())
    })
    .unwrap();
    thread::sleep(Duration::from_secs(1));
    a.write("paused", b"later");
    thread::sleep(Duration::from_secs(2));
    assert!(!b.path("paused").exists());
    assert!(!secret_b.path().join("secret.txt").exists());
    config::edit(b.state.path(), |c| {
        c.folders
            .iter_mut()
            .find(|f| f.id == "code")
            .unwrap()
            .paused = false;
        Ok(())
    })
    .unwrap();
    wait("resume", &a, &b, || equals(&b, "paused", b"later"));
    assert!(!secret_b.path().join("secret.txt").exists());
}

fn delta_fixture(size: usize) -> Vec<u8> {
    let mut data = vec![0; size];
    blake3::Hasher::new()
        .update(b"ysync integration delta fixture")
        .finalize_xof()
        .fill(&mut data);
    data
}

fn delta_change(from: &Device, to: &Device, data: &[u8]) {
    let before = ysync::daemon::read_status(to.state.path()).unwrap();
    from.write("delta.bin", data);
    wait("delta content", from, to, || equals(to, "delta.bin", data));
    settled(from, to);
    wait("delta byte accounting", from, to, || {
        ysync::daemon::read_status(to.state.path()).is_ok_and(|s| {
            s.received_bytes - before.received_bytes + s.delta_reused_bytes
                - before.delta_reused_bytes
                == data.len() as u64
        })
    });
    let after = ysync::daemon::read_status(to.state.path()).unwrap();
    let sent = after.received_bytes - before.received_bytes;
    assert!(
        sent < 1024 * 1024,
        "delta sent {sent} bytes for a small edit"
    );
    assert!(after.delta_reused_bytes - before.delta_reused_bytes > data.len() as u64 - 1024 * 1024);
}

#[test]
fn delta_edits_insertions_and_deletions_work_in_both_directions() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let mut data = delta_fixture(16 * 1024 * 1024);
    a.write("delta.bin", &data);
    a.start();
    b.start();
    wait("delta baseline", &a, &b, || equals(&b, "delta.bin", &data));
    settled(&a, &b);
    wait("baseline accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.received_bytes == data.len() as u64)
    });
    data[8_000_000..8_004_096].fill(0xf3);
    delta_change(&a, &b, &data);
    data.splice(
        1234..1234,
        b"an insertion that shifts all subsequent bytes"
            .iter()
            .copied(),
    );
    delta_change(&b, &a, &data);
    data.drain(2_000_003..2_006_007);
    delta_change(&a, &b, &data);
}

#[test]
fn interrupted_delta_preserves_destination_and_resumes() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let original = delta_fixture(32 * 1024 * 1024);
    a.write("delta.bin", &original);
    a.start();
    b.start();
    wait("delta crash baseline", &a, &b, || {
        equals(&b, "delta.bin", &original)
    });
    settled(&a, &b);
    let mut changed = original.clone();
    changed[8 * 1024 * 1024..16 * 1024 * 1024].fill(0x33);
    a.write("delta.bin", &changed);
    wait("partial delta", &a, &b, || {
        fs::read_dir(b.path(".ysync/tmp")).is_ok_and(|es| {
            es.flatten().any(|e| {
                e.metadata()
                    .is_ok_and(|m| m.len() > 0 && m.len() < changed.len() as u64)
            })
        })
    });
    b.stop();
    assert!(
        equals(&b, "delta.bin", &original),
        "delta exposed incomplete destination"
    );
    let saved = fs::read_dir(b.path(".ysync/tmp"))
        .unwrap()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .max()
        .unwrap();
    assert!(saved > 0 && saved < changed.len() as u64);
    b.start();
    wait("resumed delta", &a, &b, || {
        equals(&b, "delta.bin", &changed)
    });
    settled(&a, &b);
    wait("combined resume/delta accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path()).is_ok_and(|s| {
            s.resumed_bytes == saved
                && s.delta_reused_bytes > 0
                && s.received_bytes + s.resumed_bytes + s.delta_reused_bytes == changed.len() as u64
        })
    });
}

fn folder_status(d: &Device) -> ysync::daemon::FolderStatus {
    ysync::daemon::read_status(d.state.path()).unwrap().folders["code"].clone()
}
fn indexed(d: &Device, path: &str) -> Option<ysync::model::Entry> {
    ysync::store::get(&ysync::store::open(d.state.path()).unwrap(), "code", path).unwrap()
}
#[test]
fn watcher_bursts_deletions_and_ignored_trees_stay_scoped() {
    let mut a = Device::new("watcher");
    config::edit(a.state.path(), |c| {
        c.rescan_secs = 3600;
        Ok(())
    })
    .unwrap();
    for i in 0..1000 {
        a.write(&format!("src/d{:02}/f{i}", i / 100), b"baseline");
    }
    for i in 0..50 {
        a.write(&format!("node_modules/p{i}/file"), b"ignored");
    }
    a.start();
    wait("watcher baseline", &a, &a, || {
        ysync::daemon::read_status(a.state.path()).is_ok_and(|s| {
            s.folders
                .get("code")
                .is_some_and(|f| f.phase == "watching" && f.hashed_files == 1000)
        })
    });
    thread::sleep(Duration::from_secs(1));
    let before = folder_status(&a);
    #[cfg(target_os = "linux")]
    assert_eq!(before.native_watches, 13); // root, .ysync control, src, ten data directories
    for _ in 0..200 {
        a.write("src/d00/f0", b"after burst");
    }
    wait("burst indexed", &a, &a, || {
        indexed(&a, "src/d00/f0")
            .is_some_and(|e| e.hash == blake3::hash(b"after burst").to_hex().as_str())
    });
    fs::remove_file(a.path("src/d00/f1")).unwrap();
    wait("single delete", &a, &a, || {
        indexed(&a, "src/d00/f1").is_some_and(|e| e.kind == ysync::model::Kind::Deleted)
    });
    fs::rename(a.path("src/d01"), a.path("src/renamed")).unwrap();
    wait("subtree rename", &a, &a, || {
        indexed(&a, "src/renamed/f100").is_some()
            && indexed(&a, "src/d01/f100").is_some_and(|e| e.kind == ysync::model::Kind::Deleted)
    });
    a.write("src/renamed/f100", b"watch still attached");
    wait("edit in renamed directory", &a, &a, || {
        indexed(&a, "src/renamed/f100")
            .is_some_and(|e| e.hash == blake3::hash(b"watch still attached").to_hex().as_str())
    });
    for i in 0..200 {
        a.write(&format!("node_modules/p0/ignored{i}"), b"ignore this churn");
    }
    thread::sleep(Duration::from_secs(2));
    let after = folder_status(&a);
    assert_eq!(
        after.full_scans, before.full_scans,
        "ordinary events triggered a whole-folder scan"
    );
    assert!(
        after.checked_entries - before.checked_entries < 400,
        "unrelated tree was scanned"
    );
    // The renamed paths are newly indexed and hashed once; duplicate events reuse their metadata cache.
    assert!(
        after.hashed_files - before.hashed_files <= 110,
        "duplicate events repeatedly hashed content"
    );
    assert!(indexed(&a, "node_modules/p0/ignored0").is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn watcher_registration_failure_recovers_without_daemon_restart() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if fs::metadata("/proc/self").unwrap().uid() == 0 {
        return;
    } // Root bypasses this injected permission failure.
    let mut a = Device::new("watch recovery");
    config::edit(a.state.path(), |c| {
        c.rescan_secs = 3600;
        Ok(())
    })
    .unwrap();
    fs::create_dir(a.path("blocked")).unwrap();
    fs::set_permissions(a.path("blocked"), fs::Permissions::from_mode(0o000)).unwrap();
    a.start();
    wait("native registration failure", &a, &a, || {
        ysync::daemon::read_status(a.state.path()).is_ok_and(|s| {
            s.folders
                .get("code")
                .is_some_and(|f| f.watcher == "polling fallback")
        })
    });
    fs::set_permissions(a.path("blocked"), fs::Permissions::from_mode(0o755)).unwrap();
    wait("automatic native watcher recovery", &a, &a, || {
        folder_status(&a).phase == "watching"
    });
    a.write("blocked/after-recovery", b"native again");
    wait("new event after recovery", &a, &a, || {
        indexed(&a, "blocked/after-recovery").is_some()
    });
    assert_eq!(folder_status(&a).watcher, "native");
}

#[cfg(target_os = "macos")]
#[test]
fn unsupported_thermal_sensor_pauses_and_live_disable_resumes() {
    let mut device = Device::new("thermal-unavailable");
    device.write("file", b"retained until scanning is enabled");
    config::edit(device.state.path(), |c| {
        c.scan_max_temp_c = Some(75);
        c.rescan_secs = 3600;
        Ok(())
    })
    .unwrap();
    device.start();
    wait("thermal sensor unavailable", &device, &device, || {
        ysync::daemon::read_status(device.state.path()).is_ok_and(|s| {
            s.thermal.cooling
                && s.thermal.error.is_some()
                && s.folders
                    .get("code")
                    .is_some_and(|f| f.scan_waiting_for_cooling && f.hashed_files == 0)
        })
    });
    config::edit(device.state.path(), |c| {
        c.folders[0].paused = true;
        Ok(())
    })
    .unwrap();
    wait("manual pause interrupts cooling", &device, &device, || {
        ysync::daemon::read_status(device.state.path()).is_ok_and(|s| {
            s.folders
                .get("code")
                .is_some_and(|f| f.phase == "paused" && !f.scan_waiting_for_cooling)
        })
    });
    config::edit(device.state.path(), |c| {
        c.folders[0].paused = false;
        c.scan_max_temp_c = None;
        Ok(())
    })
    .unwrap();
    wait("live disable resumes scanning", &device, &device, || {
        ysync::daemon::read_status(device.state.path()).is_ok_and(|s| {
            !s.thermal.cooling
                && s.thermal.error.is_none()
                && s.folders
                    .get("code")
                    .is_some_and(|f| f.phase == "watching" && f.hashed_files == 1)
        })
    });
}
