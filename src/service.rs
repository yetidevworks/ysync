use crate::config;
use anyhow::{Context, Result, bail};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn systemd_quote(p: &str) -> String {
    format!(
        "\"{}\"",
        p.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
            .replace('\n', "\\n")
    )
}
pub fn render(platform: &str, exe: &Path, home: &Path) -> Result<String> {
    let exe = exe.to_str().context("non-UTF-8 executable path")?;
    let h = home.to_str().context("non-UTF-8 state path")?;
    match platform {
        "macos" => Ok(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>dev.yetidevworks.ysync</string>\n<key>ProgramArguments</key><array><string>{}</string><string>--home</string><string>{}</string><string>serve</string></array>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n<key>ThrottleInterval</key><integer>10</integer>\n<key>StandardOutPath</key><string>{}/service.log</string>\n<key>StandardErrorPath</key><string>{}/service.log</string>\n</dict></plist>\n",
            xml(exe),
            xml(h),
            xml(h),
            xml(h)
        )),
        "linux" => Ok(format!(
            "[Unit]\nDescription=ysync encrypted file synchronization\nAfter=network-online.target\n\n[Service]\nType=simple\nExecStart={} --home {} serve\nRestart=on-failure\nRestartSec=5\nUMask=0077\nNoNewPrivileges=true\n\n[Install]\nWantedBy=default.target\n",
            systemd_quote(exe),
            systemd_quote(h)
        )),
        _ => bail!("services are supported on macOS and Linux"),
    }
}
fn path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("cannot locate home")?;
    match std::env::consts::OS {
        "macos" => Ok(home.join("Library/LaunchAgents/dev.yetidevworks.ysync.plist")),
        "linux" => Ok(dirs::config_dir()
            .context("cannot locate config directory")?
            .join("systemd/user/ysync.service")),
        _ => bail!("unsupported platform"),
    }
}
/// The definition `brew services start ysync` writes, under its current or legacy name.
fn homebrew(platform: &str, home: &Path) -> Option<PathBuf> {
    let (dir, names) = match platform {
        "macos" => (
            "Library/LaunchAgents",
            ["sh.brew.ysync.plist", "homebrew.mxcl.ysync.plist"],
        ),
        "linux" => (
            ".config/systemd/user",
            ["sh.brew.ysync.service", "homebrew.ysync.service"],
        ),
        _ => return None,
    };
    names
        .iter()
        .map(|name| home.join(dir).join(name))
        .find(|p| p.exists())
}
fn uid() -> Result<String> {
    let o = Command::new("id").arg("-u").output()?;
    if !o.status.success() {
        bail!("cannot determine UID");
    }
    Ok(String::from_utf8(o.stdout)?.trim().into())
}
fn run(cmd: &str, args: &[&str]) -> Result<()> {
    if !Command::new(cmd).args(args).status()?.success() {
        bail!("{cmd} {} failed", args.join(" "));
    }
    Ok(())
}
pub fn action(home: &Path, action: &str) -> Result<()> {
    let mut p = path()?;
    let platform = std::env::consts::OS;
    // ysync's own definition wins; without one, manage the daemon Homebrew installed.
    let brewed = homebrew(platform, &dirs::home_dir().context("cannot locate home")?)
        .filter(|_| !p.exists());
    if action == "install" {
        config::load(home)?;
        if p.exists() {
            bail!(
                "service already exists at {}; uninstall it first",
                p.display()
            );
        }
        if let Some(b) = &brewed {
            bail!(
                "Homebrew already runs ysync from {}; run `brew services stop ysync` first",
                b.display()
            );
        }
        std::fs::create_dir_all(p.parent().unwrap())?;
        let body = render(
            platform,
            &std::env::current_exe()?,
            &std::fs::canonicalize(home)?,
        )?;
        config::atomic_write(&p, body.as_bytes())?;
        if platform == "linux" {
            run("systemctl", &["--user", "daemon-reload"])?;
            run("systemctl", &["--user", "enable", "--now", "ysync.service"])?;
        } else {
            run(
                "launchctl",
                &["bootstrap", &format!("gui/{}", uid()?), p.to_str().unwrap()],
            )?;
        }
        println!("Installed and started {}", p.display());
        return Ok(());
    }
    if let Some(b) = brewed {
        if action == "uninstall" {
            bail!(
                "{} belongs to Homebrew; remove it with `brew services stop ysync`",
                b.display()
            );
        }
        println!("Managing the Homebrew service at {}", b.display());
        p = b;
    }
    if platform == "linux" {
        let unit = p.file_name().unwrap().to_str().unwrap();
        match action {
            "uninstall" => {
                run(
                    "systemctl",
                    &["--user", "disable", "--now", "ysync.service"],
                )?;
                std::fs::remove_file(&p)?;
                run("systemctl", &["--user", "daemon-reload"])?;
            }
            "start" | "stop" | "status" => run("systemctl", &["--user", action, unit])?,
            _ => bail!("unknown service action"),
        }
    } else {
        let domain = format!("gui/{}", uid()?);
        let label = format!("{domain}/{}", p.file_stem().unwrap().to_str().unwrap());
        match action {
            "start" => run("launchctl", &["bootstrap", &domain, p.to_str().unwrap()])?,
            "stop" => run("launchctl", &["bootout", &label])?,
            "status" => run("launchctl", &["print", &label])?,
            "uninstall" => {
                let _ = run("launchctl", &["bootout", &label]);
                std::fs::remove_file(&p)?;
            }
            _ => bail!("unknown service action"),
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escapes_service_paths() {
        let exe = Path::new("/Users/A & B/tool");
        let h = Path::new("/tmp/a%\"$b");
        assert!(render("macos", exe, h).unwrap().contains("A &amp; B"));
        let s = render("linux", exe, h).unwrap();
        assert!(s.contains("%%\\\"$$b"));
        assert!(s.contains("UMask=0077"));
    }
    #[test]
    fn finds_homebrew_definitions_current_name_first() {
        let home = tempfile::tempdir().unwrap();
        let found = |platform| homebrew(platform, home.path());
        assert!(found("macos").is_none() && found("linux").is_none());
        let agents = home.path().join("Library/LaunchAgents");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(agents.join("homebrew.mxcl.ysync.plist"), "").unwrap();
        assert_eq!(
            found("macos").unwrap(),
            agents.join("homebrew.mxcl.ysync.plist")
        );
        std::fs::write(agents.join("sh.brew.ysync.plist"), "").unwrap();
        assert_eq!(found("macos").unwrap(), agents.join("sh.brew.ysync.plist"));
        let units = home.path().join(".config/systemd/user");
        std::fs::create_dir_all(&units).unwrap();
        std::fs::write(units.join("homebrew.ysync.service"), "").unwrap();
        assert_eq!(
            found("linux").unwrap(),
            units.join("homebrew.ysync.service")
        );
        assert!(found("windows").is_none());
    }
}
