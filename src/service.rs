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
    let p = path()?;
    let platform = std::env::consts::OS;
    if action == "install" {
        config::load(home)?;
        if p.exists() {
            bail!(
                "service already exists at {}; uninstall it first",
                p.display()
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
    if platform == "linux" {
        match action {
            "uninstall" => {
                run(
                    "systemctl",
                    &["--user", "disable", "--now", "ysync.service"],
                )?;
                std::fs::remove_file(&p)?;
                run("systemctl", &["--user", "daemon-reload"])?;
            }
            "start" | "stop" | "status" => run("systemctl", &["--user", action, "ysync.service"])?,
            _ => bail!("unknown service action"),
        }
    } else {
        let domain = format!("gui/{}", uid()?);
        let label = format!("{domain}/dev.yetidevworks.ysync");
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
}
