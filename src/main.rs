use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use ysync::{config, daemon, engine, service, store};

#[derive(Parser)]
#[command(version, about = "Direct, encrypted file sync for macOS and Linux")]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Daemon state directory (keep outside synchronized folders)"
    )]
    home: Option<PathBuf>,
    #[command(subcommand)]
    command: Cmd,
}
#[derive(Subcommand)]
enum Cmd {
    /// Create a private device identity and configuration.
    Init {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        listen: Option<String>,
        #[arg(long, value_parser=clap::value_parser!(u8).range(1..=64),help="Bounded scanner/hash worker pool; defaults to min(cores, 8)")]
        scan_workers: Option<u8>,
        /// Total transfer lanes: one small-file lane and the remainder bulk lanes. Restart both peers after changes.
        #[arg(long, value_parser=clap::value_parser!(u8).range(1..=8))]
        transfer_lanes: Option<u8>,
        /// RAM scan-to-send cache in MiB (0 disables; files above 8 MiB stream normally).
        #[arg(long, value_parser=clap::value_parser!(u16).range(0..=1024))]
        send_cache_mib: Option<u16>,
        /// Persistent delta signature budget in MiB (0 disables caching and adaptive history).
        #[arg(long, value_parser=clap::value_parser!(u16).range(0..=1024))]
        chunk_cache_mib: Option<u16>,
        #[arg(long, value_parser=clap::value_parser!(u64).range(5..), help="Periodic full reconciliation interval; default 3600 seconds, applies live")]
        rescan_secs: Option<u64>,
        #[arg(long, value_parser=clap::value_parser!(u8).range(40..=95), conflicts_with="disable_scan_thermal_limit", help="Linux CPU temperature to pause scanning; resumes 5 C cooler; applies live")]
        scan_max_temp_c: Option<u8>,
        #[arg(long, help = "Disable scanner temperature control; applies live")]
        disable_scan_thermal_limit: bool,
    },
    /// Print the device fingerprint to approve on another machine.
    Id,
    /// Run the daemon in the foreground.
    Serve,
    /// Display a live terminal activity monitor.
    Monitor {
        /// Use the legacy text display.
        #[arg(long)]
        plain: bool,
    },
    /// Show the latest daemon activity snapshot.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Report Linux watcher capacity using the existing index; does not walk source trees.
    WatchCapacity {
        #[arg(long)]
        json: bool,
    },
    /// Review preserved incoming versions and explicitly resolve conflicts.
    Conflict {
        #[command(subcommand)]
        command: ConflictCmd,
    },
    /// Preview normal pairing or explicitly seed a receiver from this device.
    Pairing {
        #[command(subcommand)]
        command: PairingCmd,
    },
    /// Preview or configure archive cleanup. Unresolved conflicts are always protected.
    Retention {
        #[command(subcommand)]
        command: RetentionCmd,
    },
    /// Manage synchronized folders. IDs must match on both devices.
    Folder {
        #[command(subcommand)]
        command: FolderCmd,
    },
    /// Add devices, review requests, and grant folder access.
    Peer {
        #[command(subcommand)]
        command: PeerCmd,
    },
    /// Manage the launchd or systemd user service.
    Service {
        #[command(subcommand)]
        command: ServiceCmd,
    },
}
#[derive(Subcommand)]
enum PairingCmd {
    /// Scan and export receiver metadata before granting access. Stop the daemon first.
    Export {
        folder: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Scan the source and write a reviewable plan; working files are unchanged.
    Preview {
        folder: String,
        #[arg(long)]
        receiver: PathBuf,
        #[arg(long, default_value="merge", value_parser=["merge","seed-local"])]
        mode: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Show paths, actions and both versions from a saved plan.
    Show {
        plan: PathBuf,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t=50, value_parser=clap::value_parser!(u16).range(1..=1000))]
        limit: u16,
    },
    /// Publish the reviewed source baseline. Stop the source daemon first.
    Apply {
        plan: PathBuf,
        #[arg(long, required = true)]
        seed_local: bool,
    },
}
#[derive(Subcommand)]
enum RetentionCmd {
    /// List eligible archives and estimated reclaimable payload bytes. Does not delete files.
    Preview {
        #[arg(long)]
        folder: Option<String>,
    },
    /// Remove eligible archives under the current policy. Stop the daemon first.
    Clean {
        #[arg(long)]
        folder: Option<String>,
        #[arg(long, required = true)]
        apply: bool,
    },
    /// Set retention limits (per folder). Automatic cleanup is separately opt-in.
    Configure {
        #[arg(long, value_parser=clap::value_parser!(u32).range(1..))]
        versions_days: Option<u32>,
        #[arg(long)]
        versions_max_mib: Option<u64>,
        #[arg(long, value_parser=clap::value_parser!(u32).range(1..))]
        partial_days: Option<u32>,
        #[arg(long, value_parser=clap::value_parser!(u32).range(1..))]
        resolved_conflicts_days: Option<u32>,
        #[arg(long, conflicts_with = "disable")]
        automatic: bool,
        /// Clear all limits and disable automatic cleanup.
        #[arg(long)]
        disable: bool,
    },
}
#[derive(Subcommand)]
enum FolderCmd {
    Add {
        id: String,
        path: PathBuf,
        #[arg(long, help = "Ignore common regenerable development output")]
        dev: bool,
    },
    List,
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
}
#[derive(Subcommand)]
enum ConflictCmd {
    List {
        #[arg(long)]
        json: bool,
    },
    /// Keep the current local version (after any manual merge). Stop the daemon first.
    Resolve {
        folder: String,
        id: String,
        #[arg(long, required = true)]
        keep_local: bool,
    },
}
#[derive(Subcommand)]
enum PeerCmd {
    Add {
        id: String,
        #[arg(long)]
        address: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long = "folder", required = true)]
        folders: Vec<String>,
    },
    List,
    Pending,
    Approve {
        id: String,
        #[arg(long = "folder", required = true)]
        folders: Vec<String>,
        #[arg(long)]
        name: Option<String>,
    },
    Revoke {
        id: String,
    },
}
#[derive(Subcommand)]
enum ServiceCmd {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
    Print {
        #[arg(long)]
        platform: Option<String>,
    },
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = cli.home.unwrap_or_else(config::default_home);
    let home = if home.is_absolute() {
        home
    } else {
        std::env::current_dir()?.join(home)
    };
    match cli.command {
        Cmd::Init {
            name,
            listen,
            scan_workers,
            transfer_lanes,
            send_cache_mib,
            chunk_cache_mib,
            rescan_secs,
            scan_max_temp_c,
            disable_scan_thermal_limit,
        } => {
            let id = config::initialize(&home, name, listen)?;
            if let Some(workers) = scan_workers {
                config::edit(&home, |c| {
                    c.scan_workers = workers as usize;
                    Ok(())
                })?;
            }
            if transfer_lanes.is_some() || send_cache_mib.is_some() || chunk_cache_mib.is_some() {
                config::edit(&home, |c| {
                    if let Some(v) = transfer_lanes {
                        c.transfer_lanes = v;
                    }
                    if let Some(v) = send_cache_mib {
                        c.send_cache_mib = v;
                    }
                    if let Some(v) = chunk_cache_mib {
                        c.chunk_cache_mib = v;
                    }
                    Ok(())
                })?;
            }
            if let Some(seconds) = rescan_secs {
                config::edit(&home, |c| {
                    c.rescan_secs = seconds;
                    Ok(())
                })?;
            }
            if scan_max_temp_c.is_some() || disable_scan_thermal_limit {
                config::edit(&home, |c| {
                    c.scan_max_temp_c = scan_max_temp_c;
                    Ok(())
                })?;
            }
            store::open(&home)?;
            println!("Device: {id}\nState: {}", home.display());
        }
        Cmd::WatchCapacity { json } => ysync::capacity::print_report(&home, json)?,
        Cmd::Id => println!("{}", config::identity(&home)?.0),
        Cmd::Conflict { command } => match command {
            ConflictCmd::List { json } => {
                let conflicts = ysync::conflicts::list(&store::open(&home)?)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&conflicts)?);
                } else {
                    for conflict in conflicts {
                        println!(
                            "{}  {}  {:?}\n  incoming: {:?}",
                            conflict.id, conflict.folder, conflict.incoming.path, conflict.payload
                        );
                    }
                }
            }
            ConflictCmd::Resolve {
                folder,
                id,
                keep_local: true,
            } => {
                ysync::conflicts::keep_local(&home, &folder, &id)?;
                println!(
                    "Kept the current local version. The explicit resolution will propagate when synchronization resumes."
                );
            }
            ConflictCmd::Resolve { .. } => bail!("explicit --keep-local is required"),
        },
        Cmd::Pairing { command } => match command {
            PairingCmd::Export { folder, output } => {
                ysync::pairing::export(&home, &folder, &output)?;
                println!(
                    "Receiver metadata exported to {}. Copy it to the source to preview pairing.",
                    output.display()
                );
            }
            PairingCmd::Preview {
                folder,
                receiver,
                mode,
                output,
            } => {
                let summary = ysync::pairing::preview(&home, &folder, &receiver, &mode, &output)?;
                println!("{}", serde_json::to_string_pretty(&summary)?);
                println!(
                    "Plan: {}. Merge uses normal approval; seed-local requires an explicit apply. Receiver replacements/deletions retain file versions.",
                    output.display()
                );
            }
            PairingCmd::Show {
                plan,
                offset,
                limit,
            } => println!(
                "{}",
                serde_json::to_string_pretty(&ysync::pairing::show(&plan, offset, limit)?)?
            ),
            PairingCmd::Apply {
                plan,
                seed_local: true,
            } => {
                let n = ysync::pairing::apply(&home, &plan)?;
                println!(
                    "Published {n} reviewed baselines. Approve/connect the devices to transfer. Source working files were not changed."
                );
            }
            PairingCmd::Apply { .. } => bail!("explicit --seed-local is required"),
        },
        Cmd::Retention { command } => match command {
            RetentionCmd::Preview { folder } => println!(
                "{}",
                serde_json::to_string_pretty(&ysync::retention::command(
                    &home,
                    folder.as_deref(),
                    false
                )?)?
            ),
            RetentionCmd::Clean {
                folder,
                apply: true,
            } => println!(
                "{}",
                serde_json::to_string_pretty(&ysync::retention::command(
                    &home,
                    folder.as_deref(),
                    true
                )?)?
            ),
            RetentionCmd::Clean { .. } => bail!("explicit --apply is required"),
            RetentionCmd::Configure {
                versions_days,
                versions_max_mib,
                partial_days,
                resolved_conflicts_days,
                automatic,
                disable,
            } => {
                config::edit(&home, |c| {
                    if disable {
                        c.retention = Default::default();
                    } else {
                        if let Some(v) = versions_days {
                            c.retention.versions_days = Some(v);
                        }
                        if let Some(v) = versions_max_mib {
                            c.retention.versions_max_bytes = Some(
                                v.checked_mul(1024 * 1024)
                                    .ok_or_else(|| anyhow::anyhow!("size limit too large"))?,
                            );
                        }
                        if let Some(v) = partial_days {
                            c.retention.partial_days = Some(v);
                        }
                        if let Some(v) = resolved_conflicts_days {
                            c.retention.resolved_conflicts_days = Some(v);
                        }
                        if automatic {
                            c.retention.automatic = true;
                        }
                    }
                    Ok(())
                })?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&config::load(&home)?.retention)?
                );
            }
        },
        Cmd::Serve => daemon::serve(&home)?,
        Cmd::Monitor { plain } => {
            if plain {
                daemon::monitor(&home, false)?;
            } else {
                ysync::monitor::run(&home)?;
            }
        }
        Cmd::Status { json } => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&daemon::read_status(&home)?)?
                );
            } else {
                daemon::monitor(&home, true)?;
            }
        }
        Cmd::Folder { command } => match command {
            FolderCmd::Add { id, path, dev } => {
                engine::add_folder(&home, &id, &path, dev)?;
                println!("Added {id}. Share this folder ID with approved devices.");
            }
            FolderCmd::List => {
                for f in config::load(&home)?.folders {
                    println!(
                        "{}\t{}\t{}",
                        f.id,
                        if f.paused { "paused" } else { "active" },
                        f.path.display()
                    );
                }
            }
            FolderCmd::Pause { id } => config::edit(&home, |c| {
                let f = c
                    .folders
                    .iter_mut()
                    .find(|f| f.id == id)
                    .ok_or_else(|| anyhow::anyhow!("unknown folder"))?;
                f.paused = true;
                Ok(())
            })?,
            FolderCmd::Resume { id } => config::edit(&home, |c| {
                let f = c
                    .folders
                    .iter_mut()
                    .find(|f| f.id == id)
                    .ok_or_else(|| anyhow::anyhow!("unknown folder"))?;
                f.paused = false;
                Ok(())
            })?,
        },
        Cmd::Peer { command } => match command {
            PeerCmd::List | PeerCmd::Pending => {
                let pending = matches!(command, PeerCmd::Pending);
                for p in config::load(&home)?.peers {
                    if pending && p.approved {
                        continue;
                    }
                    println!(
                        "{}  {}  {}  {}",
                        p.id,
                        if p.approved { "approved" } else { "pending" },
                        p.name,
                        p.folders.join(",")
                    );
                }
            }
            PeerCmd::Add {
                id,
                address,
                name,
                folders,
            } => {
                config::valid_id(&id)?;
                if id == config::identity(&home)?.0 {
                    bail!("cannot add self");
                }
                config::edit(&home, |c| {
                    validate_grants(c, &folders)?;
                    if let Some(p) = c.peers.iter_mut().find(|p| p.id == id) {
                        p.address = Some(address);
                        p.approved = true;
                        p.folders = folders;
                        if let Some(n) = name {
                            p.name = n;
                        }
                    } else {
                        c.peers.push(config::Peer {
                            id: id.clone(),
                            name: name.unwrap_or_else(|| id[..12].into()),
                            address: Some(address),
                            approved: true,
                            folders,
                        });
                    }
                    Ok(())
                })?;
                println!(
                    "Device approved locally. Start both daemons, then approve this device's fingerprint on the other machine."
                );
            }
            PeerCmd::Approve { id, folders, name } => {
                config::valid_id(&id)?;
                config::edit(&home, |c| {
                    validate_grants(c, &folders)?;
                    let p = c.peers.iter_mut().find(|p| p.id == id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "unknown device; wait for a connection request or use peer add"
                        )
                    })?;
                    p.approved = true;
                    p.folders = folders;
                    if let Some(n) = name {
                        p.name = n;
                    }
                    Ok(())
                })?;
                println!("Approved {id}");
            }
            PeerCmd::Revoke { id } => config::edit(&home, |c| {
                let p = c
                    .peers
                    .iter_mut()
                    .find(|p| p.id == id)
                    .ok_or_else(|| anyhow::anyhow!("unknown device"))?;
                p.approved = false;
                p.folders.clear();
                Ok(())
            })?,
        },
        Cmd::Service { command } => match command {
            ServiceCmd::Print { platform } => println!(
                "{}",
                service::render(
                    platform.as_deref().unwrap_or(std::env::consts::OS),
                    &std::env::current_exe()?,
                    &home
                )?
            ),
            other => service::action(
                &home,
                match other {
                    ServiceCmd::Install => "install",
                    ServiceCmd::Uninstall => "uninstall",
                    ServiceCmd::Start => "start",
                    ServiceCmd::Stop => "stop",
                    ServiceCmd::Status => "status",
                    _ => unreachable!(),
                },
            )?,
        },
    }
    Ok(())
}
fn validate_grants(c: &config::Config, folders: &[String]) -> Result<()> {
    for id in folders {
        if !c.folders.iter().any(|f| &f.id == id) {
            bail!("unknown local folder {id}");
        }
    }
    Ok(())
}
