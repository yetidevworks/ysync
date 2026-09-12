use std::{
    fs,
    io::Write,
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
fn fresh_receiver_accepts_old_deletions_without_inventing_directories() {
    let mut a = Device::new("old-deletions");
    let mut b = Device::new("fresh-receiver");
    // Keep enough tombstones to cross multiple protocol batches. The receiver
    // never saw either the former files or their parent directory versions.
    for n in 0..260 {
        a.write(&format!("retired/objects/{n:03}/old.bin"), b"obsolete");
    }
    let root = engine::Root::open(
        config::load(a.state.path()).unwrap().folders.remove(0),
        std::sync::Arc::new(std::sync::Mutex::new(())),
    )
    .unwrap();
    engine::scan(&root, a.state.path(), &a.id, None, |_| {}).unwrap();
    fs::remove_dir_all(a.path("retired")).unwrap();
    engine::scan(&root, a.state.path(), &a.id, None, |_| {}).unwrap();
    a.write("live.txt", b"still live");
    a.dial(&b);
    b.dial(&a);
    a.start();
    b.start();
    wait("live file after historical deletions", &a, &b, || {
        equals(&b, "live.txt", b"still live")
    });
    settled(&a, &b);
    for d in [&a, &b] {
        assert!(!d.path("retired").exists());
        let c = ysync::store::open(d.state.path()).unwrap();
        assert!(ysync::conflicts::list(&c).unwrap().is_empty());
    }
    b.write("reverse.txt", b"new on receiver");
    wait("reverse edit after deletion bootstrap", &a, &b, || {
        equals(&a, "reverse.txt", b"new on receiver")
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
    let pending =
        |d: &Device| ysync::conflicts::list(&ysync::store::open(d.state.path()).unwrap()).unwrap();
    wait(
        "offline edits preserved for explicit resolution",
        &a,
        &b,
        || !pending(&a).is_empty() && !pending(&b).is_empty(),
    );
    assert!(equals(&a, "hello.txt", b"offline edit A"));
    assert!(equals(&b, "hello.txt", b"offline edit B"));
    let conflict = pending(&a)
        .into_iter()
        .find(|c| c.incoming.path == "hello.txt")
        .unwrap();
    assert_eq!(
        fs::read(a.path(conflict.payload.as_ref().unwrap())).unwrap(),
        b"offline edit B"
    );
    assert!(
        ysync::conflicts::keep_local(a.state.path(), "code", &conflict.id).is_err(),
        "resolution must reject a running daemon"
    );
    a.stop();
    b.stop();
    assert_eq!(
        pending(&a).len(),
        1,
        "pending conflicts survive daemon shutdown"
    );
    let binary =
        std::env::var("YSYNC_TEST_BINARY").unwrap_or_else(|_| env!("CARGO_BIN_EXE_ysync").into());
    let output = Command::new(&binary)
        .arg("--home")
        .arg(a.state.path())
        .args(["conflict", "list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let listed: Vec<ysync::conflicts::Conflict> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(listed.len(), 1);
    let resolve = || {
        let mut command = Command::new(&binary);
        command.arg("--home").arg(a.state.path()).args([
            "conflict",
            "resolve",
            "code",
            &conflict.id,
        ]);
        command
    };
    assert!(
        !resolve().output().unwrap().status.success(),
        "explicit choice must be required"
    );
    assert!(
        resolve()
            .arg("--keep-local")
            .output()
            .unwrap()
            .status
            .success()
    );
    a.start();
    b.start();
    wait("explicit resolution propagates", &a, &b, || {
        equals(&b, "hello.txt", b"offline edit A")
            && pending(&a).is_empty()
            && pending(&b).is_empty()
    });
    settled(&a, &b);
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
    // Publish one complete version. An in-place truncate/write can let a scan
    // replicate the empty intermediate version, removing the receiver's delta
    // basis. That is valid synchronization, but invalidates this savings test.
    let staging = from.path(".ysync/tmp");
    fs::create_dir_all(&staging).unwrap();
    let mut file = tempfile::NamedTempFile::new_in(staging).unwrap();
    file.write_all(data).unwrap();
    file.as_file()
        .set_permissions(fs::metadata(from.path("delta.bin")).unwrap().permissions())
        .unwrap();
    file.persist(from.path("delta.bin")).unwrap();
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
        "delta sent {sent} bytes for a small edit\nsource: {}\nreceiver: {}",
        fs::read_to_string(from.state.path().join("status.json")).unwrap_or_default(),
        fs::read_to_string(to.state.path().join("status.json")).unwrap_or_default()
    );
    assert!(after.delta_reused_bytes - before.delta_reused_bytes > data.len() as u64 - 1024 * 1024);
}

#[test]
fn visible_truncation_streams_full_replacement_then_delta_recovers() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let mut data = delta_fixture(4 * 1024 * 1024);
    a.write("delta.bin", &data);
    a.start();
    b.start();
    wait("initial content", &a, &b, || equals(&b, "delta.bin", &data));
    settled(&a, &b);
    wait("initial accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.received_bytes == data.len() as u64)
    });

    // Model a writer paused after truncation long enough for a watcher or
    // safety scan to publish that intermediate version on the other device.
    a.write("delta.bin", b"");
    wait("visible truncation", &a, &b, || {
        equals(&b, "delta.bin", b"")
    });
    settled(&a, &b);
    let before = ysync::daemon::read_status(b.state.path()).unwrap();
    data[2_000_000..2_004_096].fill(0xf3);
    a.write("delta.bin", &data);
    wait("replacement content", &a, &b, || {
        equals(&b, "delta.bin", &data)
    });
    settled(&a, &b);
    wait("full replacement accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path()).is_ok_and(|s| {
            s.received_bytes - before.received_bytes == data.len() as u64
                && s.delta_reused_bytes == before.delta_reused_bytes
        })
    });

    // Once a complete basis exists again, small edits must regain delta reuse.
    data[3_000_000..3_004_096].fill(0x97);
    delta_change(&b, &a, &data);
    for device in [&a, &b] {
        assert!(
            ysync::conflicts::list(&ysync::store::open(device.state.path()).unwrap())
                .unwrap()
                .is_empty()
        );
    }
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

#[test]
fn unicode_equivalent_paths_replicate_without_renaming_local_files() {
    let mut a = Device::new("unicode-a");
    let mut b = Device::new("unicode-b");
    let decomposed = "cafe\u{301}";
    let composed = "caf\u{e9}";
    let ap = format!("{decomposed}/file.txt");
    let bp = format!("{composed}/file.txt");
    a.write(&ap, b"base");
    b.write(&bp, b"base");
    a.start();
    b.start();
    wait("independent Unicode scans", &a, &b, || {
        [&a, &b].iter().all(|d| {
            ysync::daemon::read_status(d.state.path())
                .is_ok_and(|s| s.folders.get("code").is_some_and(|f| f.phase == "watching"))
        })
    });
    a.dial(&b);
    b.dial(&a);
    let entry = |d: &Device| -> Option<ysync::model::Entry> {
        let c = ysync::store::open(d.state.path()).ok()?;
        let data: String = c
            .query_row(
                "select data from entries where folder='code' and path_key=?",
                [ysync::model::path_key(&ap)],
                |r| r.get(0),
            )
            .ok()?;
        serde_json::from_str(&data).ok()
    };
    wait("canonical Unicode baseline", &a, &b, || {
        match (entry(&a), entry(&b)) {
            (Some(x), Some(y)) => x.clock == y.clock,
            _ => false,
        }
    });
    a.write(&ap, b"edit from a");
    wait("Unicode edit a to b", &a, &b, || {
        equals(&b, &bp, b"edit from a")
    });
    b.write(&bp, b"edit from b");
    wait("Unicode edit b to a", &a, &b, || {
        equals(&a, &ap, b"edit from b")
    });
    b.write(&format!("{composed}/new.txt"), b"new child");
    wait("new child in Unicode parent", &a, &b, || {
        equals(&a, &format!("{decomposed}/new.txt"), b"new child")
    });
    for (d, spelling) in [(&a, decomposed), (&b, composed)] {
        let names: Vec<_> = fs::read_dir(d.files.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != ".ysync")
            .collect();
        assert_eq!(names, vec![spelling.to_owned()]);
    }
}

#[test]
fn reviewed_seed_bootstraps_existing_receiver_without_conflicts() {
    let mut a = Device::new("seed-source");
    let mut b = Device::new("seed-receiver");
    a.write("shared", b"source version");
    a.write("source-only", b"new file");
    b.write("shared", b"old receiver");
    for n in 0..140 {
        b.write(&format!("retired/{n:03}/old"), b"retained receiver data");
    }
    let artifacts = tempfile::tempdir().unwrap();
    let snapshot = artifacts.path().join("receiver.sqlite");
    let plan = artifacts.path().join("seed.sqlite");
    ysync::pairing::export(b.state.path(), "code", &snapshot).unwrap();
    let summary =
        ysync::pairing::preview(a.state.path(), "code", &snapshot, "seed-local", &plan).unwrap();
    assert_eq!(summary.delete, 281);
    ysync::pairing::apply(a.state.path(), &plan).unwrap();
    a.dial(&b);
    b.dial(&a);
    b.start();
    a.start();
    wait(
        "reviewed seed transfers and deletes across batches",
        &a,
        &b,
        || {
            equals(&b, "shared", b"source version")
                && equals(&b, "source-only", b"new file")
                && !b.path("retired").exists()
        },
    );
    settled(&a, &b);
    for d in [&a, &b] {
        assert!(
            ysync::conflicts::list(&ysync::store::open(d.state.path()).unwrap())
                .unwrap()
                .is_empty()
        );
    }
    let archived: Vec<_> = fs::read_dir(b.path(".ysync/versions"))
        .unwrap()
        .map(|p| fs::read(p.unwrap().path()).unwrap())
        .collect();
    assert!(archived.iter().any(|v| v == b"old receiver"));
    assert_eq!(
        archived
            .iter()
            .filter(|v| *v == b"retained receiver data")
            .count(),
        140
    );
    a.write("shared", b"next source version");
    wait("source edits after seeding", &a, &b, || {
        equals(&b, "shared", b"next source version")
    });
    settled(&a, &b);
    b.write("shared", b"deliberate later receiver edit");
    wait("bidirectional edits still work after seed", &a, &b, || {
        equals(&a, "shared", b"deliberate later receiver edit")
    });
    settled(&a, &b);
}

#[test]
fn opt_in_automatic_retention_cleans_archives_while_daemon_runs() {
    let mut a = Device::new("retention");
    a.write("working", b"keep working file");
    let dir = a.path(".ysync/versions");
    fs::create_dir_all(&dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    fs::write(dir.join(&id), b"old archive").unwrap();
    fs::write(
        dir.join(format!("{id}.json")),
        serde_json::to_vec(&serde_json::json!({"path":"working","saved_at":1})).unwrap(),
    )
    .unwrap();
    config::edit(a.state.path(), |c| {
        c.retention.automatic = true;
        c.retention.versions_days = Some(1);
        Ok(())
    })
    .unwrap();
    a.start();
    let until = Instant::now() + Duration::from_secs(85);
    while dir.join(&id).exists() && Instant::now() < until {
        thread::sleep(Duration::from_millis(250));
    }
    assert!(!dir.join(&id).exists(), "automatic cleanup did not run");
    assert!(equals(&a, "working", b"keep working file"));
    assert!(ysync::daemon::read_status(a.state.path()).unwrap().pid > 0);
}

#[test]
fn small_lane_progresses_while_bulk_is_blocked_then_layouts_can_change() {
    use std::os::unix::fs::PermissionsExt;
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let data = delta_fixture(2 * 1024 * 1024);
    a.write("private/bulk.bin", &data);
    fs::set_permissions(a.path("private"), fs::Permissions::from_mode(0o700)).unwrap();
    let hash = blake3::hash(&data).to_hex().to_string();
    let key = blake3::hash(
        &serde_json::to_vec(&(&a.id, "private/bulk.bin", &hash, data.len() as u64)).unwrap(),
    );
    fs::create_dir_all(b.path(".ysync/tmp")).unwrap();
    let lock =
        fs::File::create(b.path(&format!(".ysync/tmp/resume-{}.part", key.to_hex()))).unwrap();
    fs2::FileExt::lock_exclusive(&lock).unwrap();
    a.start();
    b.start();
    wait("bulk waiting on its private buffer", &a, &b, || {
        ysync::daemon::read_status(b.state.path()).is_ok_and(|s| {
            s.events
                .iter()
                .any(|e| e.detail.contains("resume buffer already in use"))
        })
    });
    a.write("private/edit.txt", b"small edit bypasses blocked bulk");
    wait("small lane independent progress", &a, &b, || {
        equals(&b, "private/edit.txt", b"small edit bypasses blocked bulk")
    });
    assert!(!b.path("private/bulk.bin").exists());
    wait(
        "blocked bulk remains visible in delivery queue",
        &a,
        &b,
        || {
            ysync::daemon::read_status(a.state.path()).is_ok_and(|s| {
                s.delivery
                    .get(&b.id)
                    .and_then(|f| f.get("code"))
                    .and_then(|d| d.pending.as_ref())
                    .is_some_and(|p| p.files >= 1 && p.bytes >= data.len() as u64)
            })
        },
    );
    assert_eq!(
        fs::metadata(b.path("private"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    drop(lock);
    wait("bulk recovery", &a, &b, || {
        equals(&b, "private/bulk.bin", &data)
    });
    settled(&a, &b);
    wait("scan bytes reused by sender", &a, &b, || {
        ysync::daemon::read_status(a.state.path())
            .is_ok_and(|s| s.scan_cache_reused_bytes >= data.len() as u64)
    });
    wait("all lanes acknowledge delivery", &a, &b, || {
        ysync::daemon::read_status(a.state.path()).is_ok_and(|s| {
            s.delivery
                .get(&b.id)
                .and_then(|f| f.get("code"))
                .is_some_and(|d| {
                    d.active_lanes == 3
                        && d.pending
                            .as_ref()
                            .is_some_and(|p| p.complete && p.entries == 0)
                        && d.acknowledged
                            .iter()
                            .all(|n| n.is_some_and(|n| n >= d.local_head))
                })
        })
    });
    for count in [1, 2, 3] {
        a.stop();
        b.stop();
        config::edit(a.state.path(), |c| {
            c.transfer_lanes = count;
            Ok(())
        })
        .unwrap();
        // Receiver offers 3: exercise minimum negotiation as well as cursor migration.
        a.write(
            "private/bulk.bin",
            if count == 2 { &data } else { b"now small" },
        );
        a.start();
        b.start();
        let expected: &[u8] = if count == 2 { &data } else { b"now small" };
        wait("layout and size transition", &a, &b, || {
            equals(&b, "private/bulk.bin", expected)
        });
        settled(&a, &b);
    }
    fs::remove_dir_all(a.path("private")).unwrap();
    wait("delete after lane migration", &a, &b, || {
        !b.path("private").exists()
    });
    settled(&a, &b);
}

#[test]
fn receiver_chunk_index_survives_restart_and_avoids_next_basis_scan() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    a.dial(&b);
    b.dial(&a);
    let mut data = delta_fixture(4 * 1024 * 1024);
    a.write("delta.bin", &data);
    a.start();
    b.start();
    wait("cache baseline", &a, &b, || equals(&b, "delta.bin", &data));
    settled(&a, &b);
    wait("cache baseline accounting", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.received_bytes == data.len() as u64)
    });
    data[100_000..104_096].fill(0x33);
    delta_change(&a, &b, &data);
    a.stop();
    b.stop();
    a.start();
    b.start();
    settled(&a, &b);
    wait("fresh cache test status", &a, &b, || {
        ysync::daemon::read_status(b.state.path())
            .is_ok_and(|s| s.pid == b.child.as_ref().unwrap().id() && s.received_bytes == 0)
    });
    data[3_000_000..3_004_096].fill(0x44);
    delta_change(&a, &b, &data);
    wait("persistent receiver signature hit", &a, &b, || {
        ysync::daemon::read_status(b.state.path()).is_ok_and(|s| s.signature_cache_hits > 0)
    });
    assert_eq!(
        ysync::daemon::read_status(b.state.path())
            .unwrap()
            .signature_indexed_bytes,
        0
    );
}

#[test]
fn ineffective_delta_streams_next_version_without_signature_reads() {
    let mut a = Device::new("a");
    let mut b = Device::new("b");
    for d in [&a, &b] {
        config::edit(d.state.path(), |c| {
            c.send_cache_mib = 0;
            Ok(())
        })
        .unwrap();
    }
    a.dial(&b);
    b.dial(&a);
    let mut data = delta_fixture(2 * 1024 * 1024);
    a.write("rewrite.bin", &data);
    a.start();
    b.start();
    wait("rewrite baseline", &a, &b, || {
        equals(&b, "rewrite.bin", &data)
    });
    settled(&a, &b);
    for byte in &mut data {
        *byte ^= 0xff;
    }
    a.write("rewrite.bin", &data);
    wait("ineffective delta fallback", &a, &b, || {
        equals(&b, "rewrite.bin", &data)
    });
    settled(&a, &b);
    wait("fallback counters", &a, &b, || {
        ysync::daemon::read_status(a.state.path())
            .is_ok_and(|s| s.delta_fallbacks == 1 && s.sent_bytes == 2 * data.len() as u64)
    });
    let before = ysync::daemon::read_status(a.state.path()).unwrap();
    for byte in &mut data {
        *byte ^= 0x17;
    }
    a.write("rewrite.bin", &data);
    wait("direct stream after ineffective delta", &a, &b, || {
        equals(&b, "rewrite.bin", &data)
    });
    settled(&a, &b);
    wait("adaptive accounting", &a, &b, || {
        ysync::daemon::read_status(a.state.path())
            .is_ok_and(|s| s.delta_fallbacks == 2 && s.sent_bytes == 3 * data.len() as u64)
    });
    let after = ysync::daemon::read_status(a.state.path()).unwrap();
    assert_eq!(
        after.signature_indexed_bytes,
        before.signature_indexed_bytes
    );
    assert_eq!(
        after.source_read_bytes - before.source_read_bytes,
        data.len() as u64
    );
}
