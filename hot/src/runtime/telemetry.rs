//! Event persistence and bounded stdout mirroring at the runtime edge.

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub(crate) fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

pub(crate) struct Emitter {
    log: std::sync::Mutex<copybot_hot::capped::CappedLog>,
    stdout: Option<mpsc::Sender<String>>,
    throttle: std::sync::Mutex<std::collections::HashMap<&'static str, (i64, u64)>>,
}
pub(crate) const RATE_LIMITED: &[(&str, i64)] = &[
    ("warm", 60),
    ("feed_stats", 60),
    ("recon_gap", 60),
    ("latency", 30),
];
impl Emitter {
    pub(crate) fn new(path: String, mirror_stdout: bool) -> Self {
        let stdout = if mirror_stdout {
            let (tx, mut rx) = mpsc::channel::<String>(4096);
            tokio::spawn(async move {
                while let Some(line) = rx.recv().await {
                    println!("{line}");
                }
            });
            Some(tx)
        } else {
            None
        };
        let p = std::path::Path::new(&path);
        let dir = p
            .parent()
            .map(|d| d.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let name = p
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("events")
            .to_string();
        let log = copybot_hot::capped::CappedLog::open(
                &dir,
                &name,
                copybot_hot::capped::DEFAULT_CAP_BYTES,
            )
            .unwrap_or_else(|e| panic!("cannot open ledger {}: {e}", dir.display()));
        Self {
            log: std::sync::Mutex::new(log),
            stdout,
            throttle: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
    pub(crate) fn ledger_status(&self) -> serde_json::Value {
        self.log.lock().unwrap().status()
    }
    pub(crate) fn emit(&self, mut v: serde_json::Value) {
        let mut suppressed = 0u64;
        if let Some(kind) = v["ev"].as_str() {
            if let Some((k, gap)) = RATE_LIMITED.iter().find(|(k, _)| *k == kind) {
                let now = copybot_hot::ledger::now_secs();
                let mut t = self.throttle.lock().unwrap();
                let e = t.entry(k).or_insert((0, 0));
                if now - e.0 < *gap {
                    e.1 += 1;
                    return;
                }
                suppressed = std::mem::take(&mut e.1);
                e.0 = now;
            }
        }
        if suppressed > 0 {
            v["suppressed"] = serde_json::json!(suppressed);
        }
        let line = v.to_string();
        let mut log = self.log.lock().unwrap();
        if let Err(e) = log.write_line(&line) {
            let n = log.write_errors;
            if n == 1 || n.is_power_of_two() {
                eprintln!("[CRITICAL] event ledger write failed ({n} errors): {e}");
            }
        }
        drop(log);
        if let Some(stdout) = &self.stdout {
            let _ = stdout.try_send(line);
        }
    }
}
