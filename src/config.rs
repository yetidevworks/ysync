use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub const NOISE: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum FolderMode {
    #[default]
    SendReceive,
    SendOnly,
    ReceiveOnly,
}
impl FolderMode {
    pub fn can_send(self) -> bool {
        self != Self::ReceiveOnly
    }
    pub fn can_receive(self) -> bool {
        self != Self::SendOnly
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SendReceive => "send-receive",
            Self::SendOnly => "send-only",
            Self::ReceiveOnly => "receive-only",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Folder {
    pub id: String,
    pub path: PathBuf,
    pub marker: String,
    pub paused: bool,
    #[serde(default)]
    pub mode: FolderMode,
    pub ignores: Vec<String>,
}
/// Hold the daemon lock throughout a policy change so no in-flight publication
/// can cross the user's newly selected boundary.
pub fn set_folder_mode(home: &Path, id: &str, mode: FolderMode) -> Result<()> {
    let _stopped = crate::pairing::stopped(home)?;
    edit(home, |c| {
        c.folders
            .iter_mut()
            .find(|f| f.id == id)
            .context("unknown folder")?
            .mode = mode;
        Ok(())
    })
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub approved: bool,
    pub folders: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub retention: crate::retention::Policy,
    #[serde(default = "default_transfer_lanes")]
    pub transfer_lanes: u8,
    #[serde(default = "default_cache_mib")]
    pub send_cache_mib: u16,
    #[serde(default = "default_cache_mib")]
    pub chunk_cache_mib: u16,
    pub name: String,
    pub listen: String,
    pub rescan_secs: u64,
    #[serde(default = "default_workers")]
    pub scan_workers: usize,
    #[serde(default)]
    pub scan_max_temp_c: Option<u8>,
    pub folders: Vec<Folder>,
    pub peers: Vec<Peer>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            retention: Default::default(),
            transfer_lanes: default_transfer_lanes(),
            send_cache_mib: default_cache_mib(),
            chunk_cache_mib: default_cache_mib(),
            name: std::env::var("HOSTNAME").unwrap_or_else(|_| "ysync-device".into()),
            listen: "0.0.0.0:39280".into(),
            rescan_secs: 3600,
            scan_workers: default_workers(),
            scan_max_temp_c: None,
            folders: vec![],
            peers: vec![],
        }
    }
}
pub fn default_transfer_lanes() -> u8 {
    3
}
pub fn default_cache_mib() -> u16 {
    64
}
pub fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .min(8)
}

pub fn default_home() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ysync")
}
pub fn initialize(home: &Path, name: Option<String>, listen: Option<String>) -> Result<String> {
    fs::create_dir_all(home)?;
    fs::set_permissions(home, fs::Permissions::from_mode(0o700))?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.join("config.lock"))?;
    fs2::FileExt::lock_exclusive(&lock)?;
    if !home.join("identity.json").exists() {
        let keys = snow::Builder::new(NOISE.parse()?).generate_keypair()?;
        atomic_write(
            &home.join("identity.json"),
            &serde_json::to_vec(
                &serde_json::json!({"private":hex::encode(keys.private),"public":hex::encode(keys.public)}),
            )?,
        )?;
    }
    if !home.join("config.json").exists() {
        let mut c = Config::default();
        if let Some(n) = name {
            c.name = n;
        }
        if let Some(l) = listen {
            c.listen = l;
        }
        atomic_write(&home.join("config.json"), &serde_json::to_vec_pretty(&c)?)?;
    }
    identity(home).map(|(id, _)| id)
}
pub fn identity(home: &Path) -> Result<(String, Vec<u8>)> {
    let data: serde_json::Value = serde_json::from_slice(
        &fs::read(home.join("identity.json")).context("run ysync init first")?,
    )?;
    let key = hex::decode(data["private"].as_str().context("missing private key")?)?;
    let public = hex::decode(data["public"].as_str().context("missing public key")?)?;
    if key.len() != 32 || public.len() != 32 {
        bail!("invalid identity key");
    }
    Ok((blake3::hash(&public).to_hex().to_string(), key))
}
pub fn load(home: &Path) -> Result<Config> {
    let c: Config = serde_json::from_slice(
        &fs::read(home.join("config.json")).context("run ysync init first")?,
    )?;
    if !(1..=8).contains(&c.transfer_lanes) || c.send_cache_mib > 1024 || c.chunk_cache_mib > 1024 {
        bail!("transfer_lanes must be 1–8 and cache sizes at most 1024 MiB");
    }
    if c.scan_max_temp_c.is_some_and(|t| !(40..=95).contains(&t)) {
        bail!("scan_max_temp_c must be between 40 and 95");
    }
    Ok(c)
}
pub fn edit<T>(home: &Path, f: impl FnOnce(&mut Config) -> Result<T>) -> Result<T> {
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.join("config.lock"))?;
    fs2::FileExt::lock_exclusive(&lock)?;
    let mut c = load(home)?;
    let out = f(&mut c)?;
    atomic_write(&home.join("config.json"), &serde_json::to_vec_pretty(&c)?)?;
    Ok(out)
}
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    fs::File::open(path.parent().context("missing parent")?)?.sync_all()?;
    Ok(())
}
pub fn dev_ignores() -> Vec<String> {
    [
        "node_modules",
        ".venv",
        "venv",
        "__pycache__",
        "target",
        "dist",
        "build",
        ".next",
        ".nuxt",
        ".turbo",
        ".cache",
        ".DS_Store",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}
pub fn valid_id(id: &str) -> Result<()> {
    if id.len() != 64 || hex::decode(id).is_err() {
        bail!("device ID must be the full 64-character fingerprint");
    }
    Ok(())
}
pub fn valid_folder_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("folder ID must contain 1–64 letters, digits, hyphens or underscores");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_folder_defaults_to_bidirectional_and_unknown_policy_is_rejected() {
        let json = serde_json::json!({"id":"code", "path":"/tmp/code", "marker":"marker", "paused":false, "ignores":[]});
        let folder: Folder = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(folder.mode, FolderMode::SendReceive);
        let mut invalid = json;
        invalid["mode"] = "recieve-only".into();
        assert!(serde_json::from_value::<Folder>(invalid).is_err());
    }
}
