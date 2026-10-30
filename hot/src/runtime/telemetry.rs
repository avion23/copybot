//! Event persistence and bounded stdout mirroring at the runtime edge.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

// Probes reuse the event-ledger emitter; they never alter trading state.

pub(crate) fn spawn_latency_probes(
    latency: &std::sync::Arc<copybot_hot::latency::Latency>,
    http: &reqwest::Client,
    clob: &String,
    emitter: &std::sync::Arc<Emitter>,
) {
    tokio::spawn(
        copybot_hot::latency::spawn_baseline_probe(
            latency.clone(),
            http.clone(),
            clob.clone(),
            Duration::from_secs(30),
        ),
    );
    {
        let (l2, e3) = (latency.clone(), emitter.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let snap = l2.snapshot();
                if snap["series"].as_object().map(|m| m.is_empty()).unwrap_or(true) {
                    continue;
                }
                e3.emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "latency", "lat" : snap, "ledger" : e3
                        .ledger_status() }
                    ),
                );
            }
        });
    }
}

pub(crate) fn spawn_route_warm_probe(
    order_paths: &std::sync::Arc<Vec<(String, reqwest::Client)>>,
    clob: &String,
    emitter: &std::sync::Arc<Emitter>,
) {
        let (h, c, e) = (order_paths.clone(), clob.clone(), emitter.clone());
        tokio::spawn(async move {
            loop {
                let t = std::time::Instant::now();
                let mut ok = true;
                let mut rtts: Vec<f64> = Vec::new();
                for (_name, cl) in h.iter() {
                    for _ in 0..2 {
                        let t1 = std::time::Instant::now();
                        ok &= cl.get(format!("{c}/time")).send().await.is_ok();
                        rtts.push(t1.elapsed().as_micros() as f64 / 1000.0);
                    }
                }
                rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
                e.emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "warm", "ok" : ok, "rtt_ms" : t
                        .elapsed().as_micros() as f64 / 1000.0, "best_ms" : rtts.first(),
                        "worst_ms" : rtts.last(), }
                    ),
                );
                tokio::time::sleep(Duration::from_secs(25)).await;
            }
        });
}

/// Owns feed-first-delivery classification, lag aggregation and event cadence.
pub(crate) struct FeedRace {
    seen: copybot_hot::race::SeenTx,
    behind: copybot_hot::race::BehindTally,
    since_report: u64,
}

impl FeedRace {
    pub(crate) fn new() -> Self {
        Self {
            seen: copybot_hot::race::SeenTx::new(8192),
            behind: copybot_hot::race::BehindTally::default(),
            since_report: 0,
        }
    }

    /// Returns true for a later delivery of a transaction already processed.
    pub(crate) fn is_duplicate(
        &mut self,
        raw: &copybot_hot::feeds::RawTx,
        emitter: &std::sync::Arc<Emitter>,
    ) -> bool {
        const FEED_RACE_EVERY: u64 = 250_000;
        if let Some(behind_ns) = self.seen.first_delivery_at(&raw.hash, raw.seen_ns) {
            self.behind.record(&raw.source, behind_ns);
            self.since_report += 1;
            if self.since_report >= FEED_RACE_EVERY {
                self.since_report = 0;
                if !self.behind.is_empty() {
                    emitter.emit(serde_json::json!({
                        "t": now_ms(), "ev": "feed_race",
                        "note": "ms each feed arrived BEHIND the first feed to deliver the same transaction. If every feed sits inside a few ms, a faster source wins nothing measurable; a wide spread is real headroom.",
                        "behind": self.behind.drain_json(),
                    }));
                }
            }
            return true;
        }
        false
    }
}

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
