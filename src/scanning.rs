//! Cooperative scanner control. The normal checkpoint is a single atomic read.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

const RUNNING: u8 = 0;
const COOLING: u8 = 1;
const CANCELLED: u8 = 2;
const STOPPED: u8 = 3;

#[derive(Debug)]
pub struct ScanCancelled;
impl std::fmt::Display for ScanCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("scan cancelled by folder pause/removal or daemon stop")
    }
}
impl std::error::Error for ScanCancelled {}

#[derive(Default)]
pub struct ScanControl {
    state: AtomicU8,
    waiters: AtomicUsize,
    lock: Mutex<()>,
    wake: Condvar,
}
impl ScanControl {
    pub fn update(&self, cancelled: bool, cooling: bool) {
        let _lock = self.lock.lock().unwrap();
        if self.state.load(Ordering::Relaxed) != STOPPED {
            self.state.store(
                if cancelled {
                    CANCELLED
                } else if cooling {
                    COOLING
                } else {
                    RUNNING
                },
                Ordering::Release,
            );
        }
        self.wake.notify_all();
    }
    pub fn stop(&self) {
        let _lock = self.lock.lock().unwrap();
        self.state.store(STOPPED, Ordering::Release);
        self.wake.notify_all();
    }
    pub fn waiting(&self) -> bool {
        self.waiters.load(Ordering::Relaxed) > 0
    }
    pub fn checkpoint(&self) -> Result<()> {
        if self.state.load(Ordering::Acquire) == RUNNING {
            return Ok(());
        }
        let mut lock = self.lock.lock().unwrap();
        while self.state.load(Ordering::Relaxed) == COOLING {
            self.waiters.fetch_add(1, Ordering::Relaxed);
            lock = self.wake.wait(lock).unwrap();
            self.waiters.fetch_sub(1, Ordering::Relaxed);
        }
        if self.state.load(Ordering::Relaxed) >= CANCELLED {
            return Err(ScanCancelled.into());
        }
        Ok(())
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ThermalStatus {
    pub max_temp_c: Option<u8>,
    pub resume_temp_c: Option<u8>,
    pub temperature_c: Option<f64>,
    pub cooling: bool,
    pub error: Option<String>,
}
impl ThermalStatus {
    pub fn sample(&mut self, max: Option<u8>, reading: Result<f64>) {
        let was_cooling = self.cooling;
        *self = Self {
            max_temp_c: max,
            resume_temp_c: max.map(|n| n.saturating_sub(5)),
            ..Self::default()
        };
        if let Some(max) = max {
            match reading {
                Ok(t) if t.is_finite() && (0.0..=150.0).contains(&t) => {
                    self.temperature_c = Some(t);
                    self.cooling = t >= f64::from(max)
                        || (was_cooling && t > f64::from(max.saturating_sub(5)));
                }
                other => {
                    self.cooling = true;
                    self.error = Some(match other {
                        Err(e) => format!("{e:#}"),
                        _ => "invalid CPU temperature reading".into(),
                    });
                }
            }
        }
    }
}

pub fn cpu_temperature() -> Result<f64> {
    #[cfg(target_os = "linux")]
    {
        read_hwmon(std::path::Path::new("/sys/class/hwmon"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        bail!("CPU temperature control is currently supported only on Linux")
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_hwmon(base: &std::path::Path) -> Result<f64> {
    use anyhow::Context;
    let mut highest: Option<f64> = None;
    for sensor in std::fs::read_dir(base)? {
        let sensor = sensor?.path();
        let name = std::fs::read_to_string(sensor.join("name")).unwrap_or_default();
        if !matches!(name.trim(), "k10temp" | "coretemp" | "zenpower") {
            continue;
        }
        let mut found = false;
        for input in std::fs::read_dir(&sensor)? {
            let input = input?.path();
            let name = input.file_name().unwrap().to_string_lossy();
            if !name.starts_with("temp") || !name.ends_with("_input") {
                continue;
            }
            found = true;
            let t = std::fs::read_to_string(&input)?.trim().parse::<f64>()? / 1000.0;
            if !t.is_finite() || !(0.0..=150.0).contains(&t) {
                bail!("invalid CPU sensor reading: {}", input.display());
            }
            highest = Some(highest.map_or(t, |old| old.max(t)));
        }
        if !found {
            bail!("CPU sensor has no temperature inputs: {}", sensor.display());
        }
    }
    highest.context("no supported CPU temperature sensor found (k10temp/coretemp/zenpower)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::{Duration, Instant},
    };
    #[test]
    fn hysteresis_and_sensor_failure() {
        let mut s = ThermalStatus::default();
        for (reading, cooling) in [(74., false), (75., true), (74., true), (70., false)] {
            s.sample(Some(75), Ok(reading));
            assert_eq!(s.cooling, cooling);
        }
        s.sample(Some(75), Err(anyhow::anyhow!("offline")));
        assert!(s.cooling && s.temperature_c.is_none() && s.error.is_some());
        s.sample(Some(75), Ok(73.));
        assert!(s.cooling);
        s.sample(Some(75), Ok(70.));
        assert!(!s.cooling);
        s.sample(Some(75), Ok(f64::NAN));
        assert!(s.cooling);
        s.sample(None, Err(anyhow::anyhow!("unsupported")));
        assert!(!s.cooling && s.error.is_none());
    }
    #[test]
    fn cooling_wakes_on_resume_pause_and_stop() {
        for action in 0..3 {
            let control = Arc::new(ScanControl::default());
            control.update(false, true);
            let c = control.clone();
            let (tx, rx) = mpsc::channel();
            let worker = thread::spawn(move || tx.send(c.checkpoint()).unwrap());
            let deadline = Instant::now() + Duration::from_secs(3);
            while !control.waiting() {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            assert!(rx.try_recv().is_err());
            match action {
                0 => control.update(false, false),
                1 => control.update(true, true),
                _ => {
                    control.stop();
                    control.update(false, false);
                }
            }
            let result = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(result.is_ok(), action == 0);
            if let Err(e) = result {
                assert!(e.is::<ScanCancelled>());
            }
            worker.join().unwrap();
        }
    }
    #[test]
    fn sensor_discovery_uses_hottest_cpu_and_rejects_missing_readings() {
        let tmp = tempfile::tempdir().unwrap();
        for (dir, name, values) in [
            ("hwmon2", "k10temp", vec!["71000", "77000"]),
            ("hwmon8", "coretemp", vec!["76000"]),
            ("hwmon9", "amdgpu", vec!["99000"]),
        ] {
            let dir = tmp.path().join(dir);
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(dir.join("name"), name).unwrap();
            for (i, value) in values.iter().enumerate() {
                std::fs::write(dir.join(format!("temp{}_input", i + 1)), value).unwrap();
            }
        }
        assert_eq!(read_hwmon(tmp.path()).unwrap(), 77.);
        std::fs::write(tmp.path().join("hwmon2/temp1_input"), "invalid").unwrap();
        assert!(read_hwmon(tmp.path()).is_err());
        assert!(read_hwmon(tempfile::tempdir().unwrap().path()).is_err());
    }
}
