//! Pending-order journal replay, durable appends, resolution and reservation reconstruction.

use super::*;
use std::io::Write;

impl PendingLog {
    pub fn open(path: &str) -> Self {
        Self::open_with_wal(path, None).0
    }
    pub fn open_with_wal(path: &str, wal: Option<&str>) -> (Self, usize) {
        let mut open: HashMap<String, Pending> = HashMap::new();
        let mut from_wal = 0usize;
        let mut unreadable = 0usize;
        let mut resolved: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (src, is_wal) in [(Some(path), false), (wal, true)] {
            let Some(src) = src else { continue };
            if src.is_empty() {
                continue;
            }
            let raw = match std::fs::read_to_string(src) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    eprintln!(
                        "[pending] ⛔ CANNOT READ {src}: {e} — this is NOT an empty \
                               journal. Outstanding orders are UNKNOWN."
                    );
                    unreadable += 1;
                    continue;
                }
            };
            for line in raw.lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
                    continue
                };
                let Some(hash) = v["order_hash"].as_str() else { continue };
                match v["ev"].as_str().unwrap_or("") {
                    "pending" => {
                        if is_wal {
                            from_wal += 1;
                        }
                        if resolved.contains(hash) {
                            continue;
                        }
                        open.insert(
                            hash.to_string(),
                            Pending {
                                lane: v["lane"].as_str().unwrap_or("").to_string(),
                                token: v["token"].as_str().unwrap_or("").to_string(),
                                side: v["side"].as_u64().unwrap_or(0) as u8,
                                order_hash: hash.to_string(),
                                shares: v["shares"].as_f64().unwrap_or(0.0),
                                limit: v["limit"].as_f64().unwrap_or(0.0),
                                ts: v["t"].as_i64().unwrap_or(0),
                                why: v["why"].as_str().unwrap_or("").to_string(),
                                resting: v["resting"].as_bool().unwrap_or(false),
                            },
                        );
                    }
                    "resolved" => {
                        resolved.insert(hash.to_string());
                        open.remove(hash);
                    }
                    _ => {}
                }
            }
        }
        let me = Self {
            path: path.to_string(),
            open,
            last_write_error: None,
            write_failures: 0,
            unreadable_sources: unreadable,
        };
        (me, from_wal)
    }
    pub fn absorb_wal(&self, rows: usize) -> Result<usize, String> {
        if rows == 0 {
            return Ok(0);
        }
        self.compact().map(|_| rows)
    }
    pub fn compact(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Ok(());
        }
        let tmp = format!("{}.compact.tmp", self.path);
        let mut out = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| format!("open {tmp}: {e}"))?;
        for p in self.open.values() {
            let row = serde_json::json!(
                { "ev" : "pending", "lane" : p.lane, "token" : p.token, "side" : p.side,
                "order_hash" : p.order_hash, "shares" : p.shares, "limit" : p.limit,
                "why" : p.why, "t" : p.ts, "resting" : p.resting }
            );
            writeln!(out, "{row}").map_err(|e| format!("write {tmp}: {e}"))?;
        }
        out.flush()
            .and_then(|_| out.sync_data())
            .map_err(|e| format!("sync {tmp}: {e}"))?;
        std::fs::rename(&tmp, &self.path)
            .map_err(|e| format!("replace {}: {e}", self.path))?;
        if let Some(dir) = std::path::Path::new(&self.path).parent() {
            let _ = std::fs::File::open(dir).and_then(|d| d.sync_all());
        }
        Ok(())
    }
    pub fn row_bytes(p: &Pending) -> Vec<u8> {
        let row = serde_json::json!(
            { "ev" : "pending", "lane" : p.lane, "token" : p.token, "side" : p.side,
            "order_hash" : p.order_hash, "shares" : p.shares, "limit" : p.limit, "why" :
            p.why, "t" : p.ts, "resting" : p.resting }
        );
        let mut b = row.to_string().into_bytes();
        b.push(b'\n');
        b
    }
    pub fn apply_record(&mut self, p: Pending) {
        self.open.insert(p.order_hash.clone(), p);
    }
    fn append(&self, row: &serde_json::Value) -> bool {
        if self.path.is_empty() {
            return true;
        }
        let done = (|| -> std::io::Result<()> {
            if let Some(dir) = std::path::Path::new(&self.path).parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            writeln!(f, "{row}")?;
            f.flush()?;
            f.sync_data()
        })();
        if let Err(e) = done {
            eprintln!("[pending] CRITICAL: cannot persist {}: {e}", self.path);
            return false;
        }
        true
    }
    pub fn record(&mut self, p: Pending) -> Result<(), String> {
        let row = serde_json::json!(
            { "ev" : "pending", "lane" : p.lane, "token" : p.token, "side" : p.side,
            "order_hash" : p.order_hash, "shares" : p.shares, "limit" : p.limit, "why" :
            p.why, "t" : p.ts, "resting" : p.resting }
        );
        if !self.append(&row) {
            let e = format!("cannot persist pending order {}", p.order_hash);
            self.write_failures += 1;
            self.last_write_error = Some(e.clone());
            return Err(e);
        }
        self.open.insert(p.order_hash.clone(), p);
        Ok(())
    }
    pub fn resolve(
        &mut self,
        order_hash: &str,
        verdict: &str,
        booked: Option<(f64, f64)>,
    ) {
        let row = serde_json::json!(
            { "ev" : "resolved", "order_hash" : order_hash, "verdict" : verdict,
            "booked_shares" : booked.map(| b | b.0), "booked_price" : booked.map(| b | b
            .1), "t" : crate ::ledger::now_secs() }
        );
        if self.append(&row) {
            self.open.remove(order_hash);
        }
    }
    pub fn resolve_booked(
        &mut self,
        order_hash: &str,
        verdict: &str,
        receipt: crate::ledger::Booked,
    ) -> Result<(), String> {
        let row = serde_json::json!(
            { "ev" : "resolved", "order_hash" : order_hash, "verdict" : verdict,
            "booked_shares" : receipt.shares, "booked_price" : receipt.price, "t" : crate
            ::ledger::now_secs() }
        );
        if !self.append(&row) {
            let e = format!("cannot persist resolution of {order_hash}");
            self.write_failures += 1;
            self.last_write_error = Some(e.clone());
            return Err(e);
        }
        self.open.remove(order_hash);
        Ok(())
    }
    pub fn due(&self, now: i64) -> Vec<Pending> {
        let mut out: Vec<Pending> = self
            .open
            .values()
            .filter(|p| now - p.ts >= SETTLE_SECS)
            .cloned()
            .collect();
        out.sort_by_key(|p| p.ts);
        out
    }
    pub fn stale(&self, now: i64) -> Vec<Pending> {
        self.stale_excluding(now, &std::collections::HashSet::new())
    }
    pub fn stale_excluding(
        &self,
        now: i64,
        live_rests: &std::collections::HashSet<String>,
    ) -> Vec<Pending> {
        self.open
            .values()
            .filter(|p| {
                let bare = p.order_hash.trim_start_matches("0x");
                let corroborated = live_rests.contains(bare)
                    || live_rests.contains(&p.order_hash);
                now - p.ts > stale_after_kind(p, corroborated)
            })
            .cloned()
            .collect()
    }
    pub fn persistence_ok(&self) -> bool {
        self.last_write_error.is_none()
    }
    pub fn len(&self) -> usize {
        self.open.len()
    }
    pub fn is_open(&self, order_hash: &str) -> bool {
        self.open.contains_key(order_hash)
    }
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }
    pub fn in_flight(&self) -> InFlight {
        let mut by_lane: HashMap<String, f64> = HashMap::new();
        let mut by_token: HashMap<(String, String), f64> = HashMap::new();
        let mut sell_shares_by_token: HashMap<(String, String), f64> = HashMap::new();
        let mut buy_shares_by_token: HashMap<(String, String), f64> = HashMap::new();
        let mut tokens = Vec::new();
        for p in self.open.values() {
            tokens.push((p.lane.clone(), p.token.clone()));
            if p.side != 0 {
                if p.shares.is_finite() && p.shares > 0.0 {
                    *sell_shares_by_token
                        .entry((p.lane.clone(), p.token.clone()))
                        .or_default() += p.shares;
                }
                continue;
            }
            if p.shares.is_finite() && p.shares > 0.0 {
                *buy_shares_by_token
                    .entry((p.lane.clone(), p.token.clone()))
                    .or_default() += p.shares;
            }
            // Rebuild the same gross reservation as Router::decide after a
            // control tick or restart; pending taker-capable buys can incur fees.
            let Some(usd) = crate::budget::buy_budget_cost(p.shares, p.limit) else {
                continue;
            };
            if usd <= 0.0 {
                continue;
            }
            *by_lane.entry(p.lane.clone()).or_default() += usd;
            *by_token.entry((p.lane.clone(), p.token.clone())).or_default() += usd;
        }
        InFlight {
            by_lane,
            by_token,
            tokens,
            sell_shares_by_token,
            buy_shares_by_token,
        }
    }
    pub fn open_tokens(&self) -> Vec<(String, String)> {
        self.open.values().map(|p| (p.lane.clone(), p.token.clone())).collect()
    }
}
