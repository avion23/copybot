//! Pending-order journal replay, durable appends, resolution and reservation reconstruction.

use super::*;
use std::io::Write;

fn parsed_pending(v: &serde_json::Value) -> Option<Pending> {
    let lane = v["lane"].as_str().filter(|value| !value.is_empty())?;
    let token = v["token"].as_str().filter(|value| !value.is_empty())?;
    let order_hash = v["order_hash"].as_str().filter(|value| !value.is_empty())?;
    let side = u8::try_from(v["side"].as_u64()?).ok().filter(|s| *s <= 1)?;
    let shares = v["shares"].as_f64().filter(|v| v.is_finite() && *v > 0.0)?;
    let limit = v["limit"].as_f64().filter(|v| v.is_finite() && *v > 0.0 && *v <= 1.0)?;
    let ts = v["t"].as_i64().filter(|t| *t >= 0)?;
    let why = v["why"].as_str()?;
    let resting = v["resting"].as_bool()?;
    Some(Pending {
        lane: lane.to_string(), token: token.to_string(), side,
        order_hash: order_hash.to_string(), shares, limit, ts,
        why: why.to_string(), resting,
    })
}

impl PendingLog {
    pub fn open(path: &str) -> Self {
        Self::open_with_wal(path, None).0
    }
    pub fn open_with_wal(path: &str, wal: Option<&str>) -> (Self, usize) {
        let mut open: HashMap<String, Pending> = HashMap::new();
        let mut stages: HashMap<String, OrderStage> = HashMap::new();
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
            for line in raw.lines().filter(|line| !line.trim().is_empty()) {
                let v = match serde_json::from_str::<serde_json::Value>(line.trim()) {
                    Ok(v) => v,
                    Err(_) => { unreadable += 1; continue; }
                };
                // The shared WAL also carries SignalGuard entries, which do
                // not own order state. All actual order rows must validate.
                if is_wal && v.get("salt").is_some() && v.get("condition").is_some()
                    && v.get("order_hash").is_none()
                {
                    continue;
                }
                let Some(hash) = v["order_hash"].as_str().filter(|h| !h.is_empty()) else {
                    unreadable += 1;
                    continue;
                };
                match v["ev"].as_str() {
                    Some("pending") => {
                        let Some(p) = parsed_pending(&v) else {
                            unreadable += 1;
                            continue;
                        };
                        if resolved.contains(hash) {
                            continue;
                        }
                        if let Some(previous) = open.get(hash) {
                            if previous.lane != p.lane || previous.token != p.token
                                || previous.side != p.side || previous.shares != p.shares
                                || previous.limit != p.limit
                            {
                                unreadable += 1;
                                continue;
                            }
                        }
                        let stage = match v.get("stage") {
                            None => OrderStage::Reserved,
                            Some(value) => match value.as_str().and_then(OrderStage::parse) {
                                Some(stage) => stage,
                                None => { unreadable += 1; continue; }
                            },
                        };
                        if is_wal { from_wal += 1; }
                        open.insert(hash.to_string(), p);
                        stages.entry(hash.to_string()).or_insert(stage);
                    }
                    Some("order_stage") => {
                        let next = v["stage"].as_str().and_then(OrderStage::parse);
                        match (stages.get(hash).copied(), next) {
                            (Some(previous), Some(next)) if previous.can_advance(next) => {
                                stages.insert(hash.to_string(), next);
                            }
                            _ => unreadable += 1,
                        }
                    }
                    Some("resolved") => {
                        resolved.insert(hash.to_string());
                        open.remove(hash);
                        stages.remove(hash);
                    }
                    _ => unreadable += 1,
                }
            }
        }
        let me = Self {
            path: path.to_string(),
            open,
            stages,
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
            let stage = self.stages.get(&p.order_hash)
                .ok_or_else(|| "order is open without a lifecycle stage".to_string())?;
            let row = serde_json::json!(
                { "ev" : "pending", "lane" : p.lane, "token" : p.token, "side" : p.side,
                "order_hash" : p.order_hash, "shares" : p.shares, "limit" : p.limit,
                "why" : p.why, "t" : p.ts, "resting" : p.resting,
                "stage" : stage.as_str() }
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
        self.stages.entry(p.order_hash.clone()).or_insert(OrderStage::Reserved);
        self.open.insert(p.order_hash.clone(), p);
    }
    pub fn stage_of(&self, order_hash: &str) -> Option<OrderStage> {
        self.stages.get(order_hash).copied()
    }
    /// Persist a venue-observed lifecycle transition before exposing it.
    /// Unknown outcomes retain their original reservation for reconciliation.
    pub fn advance(&mut self, order_hash: &str, next: OrderStage) -> Result<(), String> {
        let previous = self.stage_of(order_hash)
            .ok_or_else(|| "cannot advance an unreserved order".to_string())?;
        if previous == next { return Ok(()); }
        if !previous.can_advance(next) {
            return Err(format!("invalid order transition {} -> {}", previous.as_str(), next.as_str()));
        }
        let row = serde_json::json!({
            "ev": "order_stage", "order_hash": order_hash,
            "stage": next.as_str(), "t": crate::ledger::now_secs(),
        });
        if !self.append(&row) {
            let e = "cannot persist order stage transition".to_string();
            self.write_failures += 1;
            self.last_write_error = Some(e.clone());
            return Err(e);
        }
        self.stages.insert(order_hash.to_string(), next);
        Ok(())
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
        if p.lane.is_empty() || p.token.is_empty() || p.order_hash.is_empty()
            || p.side > 1 || !p.shares.is_finite() || p.shares <= 0.0
            || !p.limit.is_finite() || p.limit <= 0.0 || p.limit > 1.0 || p.ts < 0
        {
            return Err("invalid pending order identity or amount".to_string());
        }
        if let Some(old) = self.open.get(&p.order_hash) {
            if old.lane != p.lane || old.token != p.token
                || old.side != p.side || old.shares != p.shares || old.limit != p.limit
            {
                return Err("order digest conflicts with a different reservation".to_string());
            }
        }
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
        self.stages.entry(p.order_hash.clone()).or_insert(OrderStage::Reserved);
        self.open.insert(p.order_hash.clone(), p);
        Ok(())
    }
    pub fn resolve(
        &mut self,
        order_hash: &str,
        verdict: &str,
        booked: Option<(f64, f64)>,
    ) -> Result<(), String> {
        let row = serde_json::json!(
            { "ev" : "resolved", "order_hash" : order_hash, "verdict" : verdict,
            "booked_shares" : booked.map(| b | b.0), "booked_price" : booked.map(| b | b
            .1), "t" : crate ::ledger::now_secs() }
        );
        if !self.append(&row) {
            let error = "cannot persist order resolution".to_string();
            self.write_failures += 1;
            self.last_write_error = Some(error.clone());
            return Err(error);
        }
        self.open.remove(order_hash);
        self.stages.remove(order_hash);
        Ok(())
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
        self.stages.remove(order_hash);
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

#[cfg(test)]
mod owner_lifecycle_tests {
    use super::*;
    fn fixture_path(label: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "copybot-order-owner-{label}-{}-{}",
            std::process::id(), crate::ledger::now_secs(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("pending.jsonl");
        (dir, file)
    }
    fn pending(hash: &str) -> Pending {
        Pending {
            lane: "synthetic_lane".into(), token: "synthetic_token".into(),
            side: 0, order_hash: hash.into(), shares: 10.0,
            limit: 0.5, ts: 100, why: "pre_submit".into(), resting: true,
        }
    }
    #[test]
    fn reservation_unknown_resting_acceptance_and_release_survive_restart() {
        let (dir, path) = fixture_path("lifecycle");
        let name = path.to_str().unwrap();
        {
            let mut owner = PendingLog::open(name);
            owner.record(pending("synthetic-order")).unwrap();
            assert_eq!(owner.stage_of("synthetic-order"), Some(OrderStage::Reserved));
            assert!(owner.in_flight().by_lane.contains_key("synthetic_lane"));
            owner.advance("synthetic-order", OrderStage::Unknown).unwrap();
        }
        {
            let mut owner = PendingLog::open(name);
            assert_eq!(owner.unreadable_sources, 0);
            assert_eq!(owner.stage_of("synthetic-order"), Some(OrderStage::Unknown));
            assert!(owner.in_flight().by_lane.contains_key("synthetic_lane"));
            owner.advance("synthetic-order", OrderStage::Resting).unwrap();
            owner.compact().unwrap();
        }
        {
            let mut owner = PendingLog::open(name);
            assert_eq!(owner.stage_of("synthetic-order"), Some(OrderStage::Resting));
            owner.advance("synthetic-order", OrderStage::Accepted).unwrap();
            assert!(owner.advance("synthetic-order", OrderStage::Unknown).is_err());
            assert!(owner.is_open("synthetic-order"));
            owner.resolve_booked("synthetic-order", "matched",
                crate::ledger::Booked { shares: 10.0, price: 0.5 }).unwrap();
            assert_eq!(owner.stage_of("synthetic-order"), None);
        }
        let owner = PendingLog::open(name);
        assert_eq!(owner.unreadable_sources, 0);
        assert_eq!(owner.stage_of("synthetic-order"), None);
        assert!(!owner.is_open("synthetic-order"));
        assert!(owner.in_flight().by_lane.is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn malformed_pending_and_phase_never_invent_a_reservation_or_recover_it() {
        let (dir, path) = fixture_path("malformed");
        let valid = serde_json::json!({
            "ev":"pending", "lane":"synthetic_lane", "token":"synthetic_token",
            "side":0, "order_hash":"valid", "shares":10.0, "limit":0.5,
            "t":100, "why":"reserved", "resting":false
        });
        let invalid = serde_json::json!({
            "ev":"pending", "lane":"synthetic_lane", "token":"synthetic_token",
            "side":99, "order_hash":"invalid", "shares":10.0, "limit":0.5,
            "t":100, "why":"reserved", "resting":false
        });
        let unknown_stage = serde_json::json!({
            "ev":"order_stage", "order_hash":"valid", "stage":"invented"
        });
        std::fs::write(&path, format!("{valid}\n{invalid}\n{unknown_stage}\n")).unwrap();
        let owner = PendingLog::open(path.to_str().unwrap());
        assert_eq!(owner.unreadable_sources, 2);
        assert_eq!(owner.stage_of("valid"), Some(OrderStage::Reserved));
        assert_eq!(owner.stage_of("invalid"), None);
        assert!(owner.is_open("valid"));
        assert!(!owner.is_open("invalid"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn reused_digest_with_different_identity_is_refused_before_append() {
        let (dir, path) = fixture_path("conflict");
        let mut owner = PendingLog::open(path.to_str().unwrap());
        owner.record(pending("same")).unwrap();
        let mut different = pending("same");
        different.token = "other_token".into();
        assert!(owner.record(different).is_err());
        assert_eq!(owner.len(), 1);
        assert_eq!(owner.stage_of("same"), Some(OrderStage::Reserved));
        drop(owner);
        let reloaded = PendingLog::open(path.to_str().unwrap());
        assert_eq!(reloaded.unreadable_sources, 0);
        assert_eq!(reloaded.stage_of("same"), Some(OrderStage::Reserved));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
