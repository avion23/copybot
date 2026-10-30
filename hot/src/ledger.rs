use std::collections::HashMap;
use crate::risk::{LaneRisk, RiskConfig};
#[path = "ledger_receipts.rs"]
mod receipts;
use receipts::StoredBooked;
#[derive(Debug, Default, Clone)]
pub struct Position {
    pub shares: f64,
    pub cost: f64,
    pub proceeds: f64,
    pub opened_t: i64,
    pub last_fill_t: i64,
    pub his_cost: f64,
    pub his_shares_known: f64,
}
impl Position {
    pub fn avg_his_price(&self) -> Option<f64> {
        if self.his_shares_known > 1e-9 {
            Some(self.his_cost / self.his_shares_known)
        } else {
            None
        }
    }
    pub fn avg_cost(&self) -> f64 {
        if self.shares > 1e-9 { self.cost / self.shares } else { 0.0 }
    }
}
pub fn utc_day(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}
pub fn year_start_secs(now: i64) -> i64 {
    let z = utc_day(now) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    days_from_civil(year, 1, 1) * 86_400
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RealisedWindows {
    pub total: f64,
    pub d1: f64,
    pub d7: f64,
    pub d30: f64,
    pub ytd: f64,
    pub y1: f64,
}
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
#[derive(Debug)]
pub struct LaneBook {
    pub name: String,
    pub risk: LaneRisk,
    pub positions: HashMap<String, Position>,
    pub spent_today: f64,
    pub spent_day: i64,
    pub fired: u64,
    pub filled: u64,
    pub rejected: u64,
}
impl LaneBook {
    pub fn fresh(name: &str, cfg: RiskConfig) -> LaneBook {
        LaneBook {
            name: name.to_string(),
            risk: LaneRisk::new(name, cfg),
            positions: HashMap::new(),
            spent_today: 0.0,
            spent_day: utc_day(now_secs()),
            fired: 0,
            filled: 0,
            rejected: 0,
        }
    }
    pub fn roll_day(&mut self, now: i64) {
        let d = utc_day(now);
        if self.spent_day != d {
            self.spent_day = d;
            self.spent_today = 0.0;
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq)]
#[must_use = "a Booked receipt is the proof a fill reached disk — dropping it discards \
              the only thing that may close a pending row"]
pub struct Booked {
    pub shares: f64,
    pub price: f64,
}
/// A read-only, durable fill attestation recovered from the ledger on boot.
/// Its fee is the *booked* fee, not necessarily an independently verified venue fee.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordedOrder {
    pub lane: String,
    pub token: String,
    pub side: u8,
    pub receipt: Booked,
    pub fee: f64,
    pub resting: bool,
}
pub struct Ledger {
    pub path: String,
    pub lane_ids: HashMap<String, String>,
    pub lanes: HashMap<String, LaneBook>,
    pub corrupt_lines: u64,
    pub events: u64,
    pub write_failures: u64,
    pub last_write_error: Option<String>,
    /// The sole canonical on-disk fill receipt for each submitted order digest.
    booked_receipts: HashMap<String, StoredBooked>,
    settlement_keys: std::collections::HashSet<(String, String)>,
}
impl Ledger {
    pub fn new(path: &str, cfgs: &[(String, RiskConfig)]) -> Self {
        let mut lanes = HashMap::new();
        for (name, cfg) in cfgs {
            lanes.insert(name.clone(), LaneBook::fresh(name, *cfg));
        }
        let mut l = Self {
            lane_ids: HashMap::new(),
            path: path.into(),
            lanes,
            corrupt_lines: 0,
            events: 0,
            write_failures: 0,
            last_write_error: None,
            booked_receipts: HashMap::new(),
            settlement_keys: std::collections::HashSet::new(),
        };
        l.replay();
        l
    }
    pub fn ensure_lane(&mut self, name: &str, cfg: RiskConfig) {
        if self.lanes.contains_key(name) {
            return;
        }
        self.lanes.insert(name.to_string(), LaneBook::fresh(name, cfg));
        self.replay_lane(name);
    }
    fn replay_lane(&mut self, name: &str) {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(r) => r,
            Err(_) => return,
        };
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if v["lane"].as_str() == Some(name) {
                    self.apply(&v, false);
                }
            }
        }
    }
    fn replay(&mut self) {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                self.last_write_error = Some(format!("ledger read {}: {e}", self.path));
                return;
            }
        };
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(v) => {
                    self.apply(&v, false);
                    self.events += 1;
                }
                Err(_) => self.corrupt_lines += 1,
            }
        }
    }
    fn append(&mut self, row: &serde_json::Value) -> bool {
        use std::io::Write;
        if self.path.is_empty() {
            return true;
        }
        let result = (|| -> std::io::Result<()> {
            let path = std::path::Path::new(&self.path);
            let existed = path.exists();
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            writeln!(f, "{row}")?;
            f.flush()?;
            f.sync_data()?;
            if !existed {
                let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        if let Err(e) = result {
            self.write_failures += 1;
            self.last_write_error = Some(format!("ledger append {}: {e}", self.path));
            eprintln!(
                "[CRITICAL] {}", self.last_write_error.as_deref()
                .unwrap_or("ledger append failed")
            );
            return false;
        }
        true
    }
    pub fn record_fill(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
    ) -> bool {
        self.record_fill_with_his_price(lane, token, side, shares, price, fee, None)
    }
    #[must_use = "a dropped fill leaves inventory NO LANE TRACKS — handle the false case"]
    pub fn record_fill_with_his_price(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
        his_price: Option<f64>,
    ) -> bool {
        let row = serde_json::json!(
            { "ev" : "fill", "lane" : lane, "token" : token, "side" : side, "shares" :
            shares, "price" : price, "fee" : fee, "his_price" : his_price, "t" :
            now_secs() }
        );
        let booked = self.apply(&row, true);
        if !booked {
            eprintln!(
                "[CRITICAL] {lane}: FILL NOT BOOKED — token {token} side {side} \
                       {shares} @ {price}. The venue gave us these shares and the ledger \
                       refused the row (missing lane book, or the durable append failed). \
                       This is unattributed inventory: reconcile it before trading."
            );
        }
        booked
    }
    pub fn record_recon_fill(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
    ) {
        let row = serde_json::json!(
            { "ev" : "fill", "lane" : lane, "token" : token, "side" : side, "shares" :
            shares, "price" : price, "fee" : fee, "recon" : true, "t" : now_secs() }
        );
        self.apply(&row, true);
    }
    pub fn realised_reset(&mut self, lane: &str, why: &str) {
        let row = serde_json::json!(
            { "ev" : "realised_reset", "lane" : lane, "why" : why, "t" : now_secs() }
        );
        self.apply(&row, true);
    }
    pub fn newest_realised_reset_t(&self) -> Option<i64> {
        let raw = std::fs::read_to_string(&self.path).ok()?;
        raw.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
            .filter(|v| v["ev"].as_str() == Some("realised_reset"))
            .filter_map(|v| v["t"].as_i64())
            .max()
    }
    pub fn clear_risk(&mut self, lane: &str, why: &str) -> bool {
        let was = self
            .lanes
            .get(lane)
            .map(|b| b.risk.tripped.is_some())
            .unwrap_or(false);
        let row = serde_json::json!(
            { "ev" : "risk_cleared", "lane" : lane, "why" : why, "t" : now_secs() }
        );
        self.apply(&row, true);
        was
    }
    pub fn record_settlement(&mut self, lane: &str, token: &str, payout: f64) -> bool {
        if !payout.is_finite() || !(0.0..=1.0).contains(&payout)
            || self.holdings(lane).get(token).copied().unwrap_or(0.0) <= 1e-9
        {
            return false;
        }
        let row = serde_json::json!(
            { "ev" : "settle", "lane" : lane, "token" : token, "payout" : payout, "t" :
            now_secs(), "key": crate::settlement::cross_writer_key(lane, token) }
        );
        self.apply(&row, true)
    }

    /// Close a still-open position only when the wallet's redemption history and
    /// a complete, subsequent portfolio snapshot agree. Missing shares alone
    /// cannot establish a payout. One durable event closes cost and books P&L.
    pub fn settle_redeemed_open(
        &mut self,
        lane: &str,
        token: &str,
        redemption: &crate::settlement::Redemption,
        snapshot: &crate::positions::Positions,
        pending: bool,
    ) -> Result<bool, String> {
        let key = redemption.lane_key(lane);
        let alt_key = crate::settlement::cross_writer_key(lane, token);
        if self.settlement_keys.contains(&(lane.into(), key.clone()))
            || self.settlement_keys.contains(&(lane.into(), alt_key.clone()))
        {
            return Ok(false);
        }
        let Some(pos) = self.lanes.get(lane).and_then(|b| b.positions.get(token)) else {
            return Ok(false);
        };
        if pos.shares <= 1e-9 { return Ok(false); }
        if !redemption.is_bookable() || pending
            || redemption.asset != token
            || !snapshot.confirms_zero(token, redemption.ts)
        {
            return Err("redemption requires an explicit matching token, a complete post-redemption zero balance, and no pending order".into());
        }
        // Do not assign a partial redemption, old redemption, or pooled claim
        // to a whole position. These cases need further attribution evidence.
        if pos.last_fill_t <= 0 || pos.last_fill_t > redemption.ts
            || redemption.size + 1e-6 < pos.shares
            || self.lanes.values().filter(|b| b.positions.get(token)
                .map(|p| p.shares > 1e-9).unwrap_or(false)).count() != 1
        {
            return Err("redemption amount, time, or lane ownership is ambiguous".into());
        }
        // A prior neutral release has unbooked basis of its own. Closing only
        // the residual would consume the token's dedupe key and strand that
        // basis. Defer mixed histories rather than silently lose its P&L.
        let history = std::fs::read_to_string(&self.path)
            .map_err(|_| "cannot verify settlement reconciliation history".to_string())?;
        for line in history.lines().filter(|line| !line.trim().is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line)
                .map_err(|_| "cannot settle with malformed ledger history".to_string())?;
            if row["ev"].as_str() == Some("fill") && row["token"].as_str() == Some(token)
                && row["side"].as_u64() == Some(1) && row["recon"].as_bool() == Some(true)
                && row["shares"].as_f64().unwrap_or(0.0) > 1e-9
            {
                return Err("open position also has a neutral reconciliation release; combined basis requires review".into());
            }
        }
        let row = serde_json::json!({
            "ev": "settle", "lane": lane, "token": token,
            "payout": redemption.usdc / redemption.size,
            "shares": pos.shares, "proceeds": pos.shares * redemption.usdc / redemption.size,
            "key": key, "alt_key": alt_key, "redemption_tx": redemption.tx,
            "redemption_t": redemption.ts, "t": now_secs(),
            "why": "auto-redemption confirmed by activity and complete zero-balance snapshot"
        });
        if !self.apply(&row, true) {
            return Err("settlement ledger write failed or settlement already recorded".into());
        }
        Ok(true)
    }
    pub fn record_merge(
        &mut self,
        lane: &str,
        yes_token: &str,
        no_token: &str,
        shares: f64,
    ) {
        let row = serde_json::json!(
            { "ev" : "merge", "lane" : lane, "yes" : yes_token, "no" : no_token, "shares"
            : shares, "t" : now_secs() }
        );
        self.apply(&row, true);
    }
    pub fn mergeable(&self, lane: &str, yes_token: &str, no_token: &str) -> f64 {
        let held = |t: &str| {
            self
                .lanes
                .get(lane)
                .and_then(|b| b.positions.get(t))
                .map(|p| p.shares)
                .unwrap_or(0.0)
        };
        held(yes_token).min(held(no_token))
    }
    pub fn record_realised_adjustment(
        &mut self,
        lane: &str,
        token: &str,
        shares: f64,
        proceeds: f64,
        avg_cost: f64,
        why: &str,
    ) {
        let pnl = proceeds - shares * avg_cost;
        let row = serde_json::json!(
            { "ev" : "realised_adjust", "lane" : lane, "token" : token, "shares" :
            shares, "proceeds" : proceeds, "avg_cost" : avg_cost, "pnl" : pnl, "why" :
            why, "t" : now_secs() }
        );
        self.apply(&row, true);
    }
    pub fn record_realised_adjustment_tx(
        &mut self,
        lane: &str,
        token: &str,
        shares: f64,
        proceeds: f64,
        avg_cost: f64,
        why: &str,
        key: &str,
    ) -> bool {
        let pnl = proceeds - shares * avg_cost;
        let row = serde_json::json!(
            { "ev" : "realised_adjust", "lane" : lane, "token" : token, "shares" :
            shares, "proceeds" : proceeds, "avg_cost" : avg_cost, "pnl" : pnl, "why" :
            why, "key" : key, "alt_key" : crate ::settlement::cross_writer_key(lane,
            token), "t" : now_secs() }
        );
        self.apply(&row, true)
    }
    pub fn corrected_keys(&self) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        let Ok(raw) = std::fs::read_to_string(&self.path) else { return out };
        for line in raw.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) {
                for field in ["key", "alt_key"] {
                    if let Some(key) = v[field].as_str() {
                        out.insert(key.to_string());
                    }
                }
            }
        }
        out
    }
    pub fn last_recon_release(
        &self,
        lane: &str,
        token: &str,
        not_before: i64,
    ) -> Option<(f64, f64, i64)> {
        let raw = std::fs::read_to_string(&self.path).ok()?;
        let mut found = None;
        for line in raw.lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
                continue
            };
            if v["ev"].as_str() == Some("fill") && v["lane"].as_str() == Some(lane)
                && v["token"].as_str() == Some(token) && v["side"].as_u64() == Some(1)
                && v["recon"].as_bool() == Some(true)
            {
                let t = v["t"].as_i64().unwrap_or(0);
                if t < not_before {
                    continue;
                }
                let shares = v["shares"].as_f64().unwrap_or(0.0);
                let price = v["price"].as_f64().unwrap_or(0.0);
                found = Some((shares, price, t));
            }
        }
        found
    }
    fn apply(&mut self, row: &serde_json::Value, durable: bool) -> bool {
        let lane_name = row["lane"].as_str().unwrap_or("");
        let ev = row["ev"].as_str().unwrap_or("");
        let token = row["token"].as_str().unwrap_or("").to_string();
        if !self.lanes.contains_key(lane_name) {
            return false;
        }
        // The ledger is inventory truth. A crash after booking but before
        // pending resolution may repeat a submitted order hash; replaying the
        // same receipt must never add inventory twice.
        let receipt = if ev == "fill" && row["order_hash"].as_str().is_some_and(|h| !h.is_empty()) {
            match StoredBooked::from_fill_row(row) {
                Some((hash, stored)) => {
                    if let Some(previous) = self.booked_receipts.get(&hash) {
                        if previous != &stored {
                            self.last_write_error =
                                Some("conflicting durable fill for existing order hash".into());
                            self.corrupt_lines += 1;
                        }
                        return false;
                    }
                    Some((hash, stored))
                }
                None => {
                    self.last_write_error =
                        Some("hashed ledger fill has missing or invalid money evidence".into());
                    self.corrupt_lines += 1;
                    return false;
                }
            }
        } else {
            None
        };
        let mut settlement_keys = Vec::new();
        if matches!(ev, "settle" | "realised_adjust") {
            for field in ["key", "alt_key"] {
                if let Some(key) = row[field].as_str() {
                    settlement_keys.push((lane_name.to_string(), key.to_string()));
                }
            }
            // Legacy settle rows did not carry a key. Replaying one still
            // protects against later recovery crediting the same token again.
            if ev == "settle" {
                settlement_keys.push((lane_name.into(), crate::settlement::cross_writer_key(lane_name, &token)));
            }
            if settlement_keys.iter().any(|key| self.settlement_keys.contains(key)) {
                return false;
            }
        }
        if durable && !self.append(row) {
            return false;
        }
        self.settlement_keys.extend(settlement_keys);
        if let Some((hash, stored)) = receipt {
            self.booked_receipts.insert(hash, stored);
        }
        let Some(lane) = self.lanes.get_mut(lane_name) else {
            return false;
        };
        match ev {
            "fill" => {
                let shares = row["shares"].as_f64().unwrap_or(0.0);
                let price = row["price"].as_f64().unwrap_or(0.0);
                let fee = row["fee"].as_f64().unwrap_or(0.0);
                let side = row["side"].as_u64().unwrap_or(0) as u8;
                let tok_tail = token[token.len().saturating_sub(8)..].to_string();
                let pos = lane.positions.entry(token).or_default();
                let was_flat = pos.shares <= 1e-9;
                pos.last_fill_t = pos.last_fill_t.max(row["t"].as_i64().unwrap_or(0));
                if side == 0 {
                    pos.shares += shares;
                    pos.cost += shares * price + fee;
                    if let Some(hp) = row["his_price"].as_f64() {
                        pos.his_cost += shares * hp;
                        pos.his_shares_known += shares;
                    }
                    if was_flat {
                        pos.opened_t = row["t"].as_i64().unwrap_or(0);
                    }
                    let now = now_secs();
                    lane.roll_day(now);
                    let row_t = row["t"].as_i64().unwrap_or(0);
                    let is_recon = row["recon"].as_bool().unwrap_or(false);
                    if !is_recon && row_t > 0 && utc_day(row_t) == utc_day(now) {
                        lane.spent_today += shares * price + fee;
                    }
                    if was_flat {
                        lane.risk.on_open();
                    }
                } else {
                    let sold = shares.min(pos.shares);
                    if shares > pos.shares + 1e-6 {
                        eprintln!(
                            "[ledger] {lane_name}: sell row claims {shares:.4} sh \
of …{} but only {:.4} tracked — excess IGNORED (replay-safe), investigate the row",
                            tok_tail, pos.shares
                        );
                    }
                    let avg = pos.avg_cost();
                    let is_recon = row["recon"].as_bool().unwrap_or(false);
                    let realised = if is_recon {
                        0.0
                    } else {
                        sold * price - sold * avg - fee
                    };
                    pos.cost -= sold * avg;
                    if let Some(his_avg) = pos.avg_his_price() {
                        let his_sold = sold.min(pos.his_shares_known);
                        pos.his_cost -= his_sold * his_avg;
                        pos.his_shares_known -= his_sold;
                    }
                    pos.shares -= sold;
                    pos.proceeds += sold * price - fee;
                    if !is_recon {
                        lane.risk.on_close(realised);
                    }
                    if pos.shares <= 1e-9 {
                        lane.risk.on_flat();
                    }
                }
                lane.filled += 1;
            }
            "realised_reset" => {
                lane.risk.realised_pnl = 0.0;
            }
            "risk_cleared" => {
                lane.risk.clear();
            }
            "realised_adjust" => {
                lane.risk.on_adjust(row["pnl"].as_f64().unwrap_or(0.0));
            }
            "settle" => {
                let payout = row["payout"].as_f64().unwrap_or(0.0);
                if let Some(pos) = lane.positions.get_mut(&token) {
                    if pos.shares > 1e-9 {
                        let realised = pos.shares * payout - pos.cost;
                        lane.risk.on_close(realised);
                        lane.risk.on_flat();
                        pos.proceeds += pos.shares * payout;
                        pos.shares = 0.0;
                        pos.cost = 0.0;
                        pos.his_cost = 0.0;
                        pos.his_shares_known = 0.0;
                    }
                }
            }
            "merge" => {
                let yes_tok = row["yes"].as_str().unwrap_or("").to_string();
                let no_tok = row["no"].as_str().unwrap_or("").to_string();
                let want = row["shares"].as_f64().unwrap_or(0.0);
                let (ys, avg_yes) = lane
                    .positions
                    .get(&yes_tok)
                    .map(|p| (p.shares, p.avg_cost()))
                    .unwrap_or((0.0, 0.0));
                let (ns, avg_no) = lane
                    .positions
                    .get(&no_tok)
                    .map(|p| (p.shares, p.avg_cost()))
                    .unwrap_or((0.0, 0.0));
                let m = want.min(ys).min(ns);
                if m > 1e-9 {
                    lane.risk.on_close(m - m * avg_yes - m * avg_no);
                    let mut flats = 0;
                    if let Some(p) = lane.positions.get_mut(&yes_tok) {
                        p.cost -= m * avg_yes;
                        p.shares -= m;
                        p.proceeds += m * avg_yes;
                        if p.shares <= 1e-9 {
                            flats += 1;
                        }
                    }
                    if let Some(p) = lane.positions.get_mut(&no_tok) {
                        p.cost -= m * avg_no;
                        p.shares -= m;
                        p.proceeds += m * avg_no;
                        if p.shares <= 1e-9 {
                            flats += 1;
                        }
                    }
                    for _ in 0..flats {
                        lane.risk.on_flat();
                    }
                    lane.filled += 1;
                }
            }
            _ => {}
        }
        true
    }
    pub fn book_fill(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
        his_price: Option<f64>,
        order_hash: &str,
    ) -> Option<Booked> {
        self.book_fill_ex(
            lane,
            token,
            side,
            shares,
            price,
            fee,
            his_price,
            order_hash,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn book_fill_ex(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
        his_price: Option<f64>,
        order_hash: &str,
        resting: bool,
    ) -> Option<Booked> {
        self.book_fill_ex_px(
            lane,
            token,
            side,
            shares,
            price,
            fee,
            his_price,
            order_hash,
            resting,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn book_fill_ex_px(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
        his_price: Option<f64>,
        order_hash: &str,
        resting: bool,
        px_provisional: bool,
    ) -> Option<Booked> {
        if !StoredBooked::valid_amounts(side, shares, price, fee) {
            self.last_write_error = Some("refusing invalid fill economics".into());
            return None;
        }
        if let Some(stored) = self.booked_receipts.get(order_hash) {
            if stored.agrees(lane, token, side, shares, price, fee, resting) {
                return Some(stored.receipt());
            }
            // A digest already booked with different money, market, side or
            // fee cannot be "re-booked" by trusting a new caller's numbers.
            self.last_write_error =
                Some("conflicting evidence for an already booked order".into());
            return None;
        }
        let mut row = serde_json::json!(
            { "ev" : "fill", "lane" : lane, "token" : token, "side" : side, "shares" :
            shares, "price" : price, "fee" : fee, "his_price" : his_price, "exec" : if
            resting { "maker" } else { "taker" }, "order_hash" : order_hash, "t" :
            now_secs() }
        );
        if let Some(id) = self.lane_ids.get(lane) {
            row["lane_id"] = serde_json::json!(id);
        }
        if px_provisional {
            row["px_provisional"] = serde_json::json!(true);
        }
        if self.apply(&row, true) { Some(Booked { shares, price }) } else { None }
    }
    pub fn was_order_booked(&self, order_hash: &str) -> bool {
        self.booked_receipts.contains_key(order_hash)
    }
    pub fn recorded_order(&self, order_hash: &str) -> Option<RecordedOrder> {
        self.booked_receipts.get(order_hash).map(|saved| RecordedOrder {
            lane: saved.lane.clone(),
            token: saved.token.clone(),
            side: saved.side,
            receipt: saved.receipt(),
            fee: saved.fee,
            resting: saved.resting,
        })
    }
    pub fn open_usd(&self, lane: &str) -> f64 {
        self.lanes
            .get(lane)
            .map(|b| {
                b.positions.values().filter(|p| p.shares > 1e-9).map(|p| p.cost).sum()
            })
            .unwrap_or(0.0)
    }
    pub fn avg_cost(&self, lane: &str, token: &str) -> f64 {
        self.lanes
            .get(lane)
            .and_then(|l| l.positions.get(token))
            .map(|p| p.avg_cost())
            .unwrap_or(0.0)
    }
    pub fn holdings(&self, lane: &str) -> HashMap<String, f64> {
        self.lanes
            .get(lane)
            .map(|b| {
                b.positions
                    .iter()
                    .filter(|(_, p)| p.shares > 1e-9)
                    .map(|(t, p)| (t.clone(), p.shares))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn lane_epoch(&self, lane: &str) -> Option<i64> {
        let raw = std::fs::read_to_string(&self.path).ok()?;
        raw.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
            .filter(|v| v["lane"].as_str() == Some(lane))
            .filter_map(|v| v["t"].as_i64())
            .filter(|t| *t > 0)
            .min()
    }
    pub fn equity_series(&self, lane: Option<&str>) -> Vec<(i64, f64)> {
        equity_series_at(&self.path, lane)
    }
    pub fn book_merge(
        &mut self,
        lane: &str,
        token_a: &str,
        token_b: &str,
        pairs: f64,
        mark_a: f64,
        mark_b: f64,
        tx_hash: &str,
    ) -> Option<(Booked, Booked)> {
        if !(pairs.is_finite() && pairs > 0.0) {
            return None;
        }
        let total = mark_a + mark_b;
        let price_a = if total.is_finite() && total > 0.0 && mark_a.is_finite()
            && mark_a >= 0.0
        {
            (mark_a / total).clamp(0.0, 1.0)
        } else {
            0.5
        };
        let price_b = 1.0 - price_a;
        let a = self
            .book_fill_ex_px(
                lane,
                token_a,
                1,
                pairs,
                price_a,
                0.0,
                None,
                &format!("{tx_hash}:a"),
                false,
                false,
            )?;
        let b = self
            .book_fill_ex_px(
                lane,
                token_b,
                1,
                pairs,
                price_b,
                0.0,
                None,
                &format!("{tx_hash}:b"),
                false,
                false,
            )?;
        Some((a, b))
    }
    pub fn realised_windows_at(
        path: &str,
        lane: Option<&str>,
        now: i64,
    ) -> RealisedWindows {
        RealisedWindows::from_series(&equity_series_at(path, lane), now)
    }
}
#[path = "ledger_queries.rs"]
mod queries;
pub use queries::{equity_series_at, scan_for_settlement};
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Continuity {
    pub lines: u64,
    pub bytes: u64,
    pub prefix_sha256: String,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ContinuityVerdict {
    FirstRun,
    Intact,
    Broken(String),
}
fn prefix_digest(raw: &str, lines: u64) -> (String, u64) {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    let mut n = 0u64;
    for line in raw.lines() {
        if n >= lines {
            break;
        }
        h.update(line.as_bytes());
        h.update(b"\n");
        n += 1;
    }
    (hex::encode(h.finalize()), n)
}
pub const LEDGER_GROWTH_ALARM_LINES: u64 = 250_000;
impl Ledger {
    pub fn continuity(&self) -> Option<Continuity> {
        let raw = std::fs::read_to_string(&self.path).ok()?;
        let total = raw.lines().count() as u64;
        let (prefix_sha256, lines) = prefix_digest(&raw, total);
        Some(Continuity {
            lines,
            bytes: raw.len() as u64,
            prefix_sha256,
        })
    }
    pub fn verify_continuity(&self, sidecar: &str) -> ContinuityVerdict {
        let Ok(prev_raw) = std::fs::read_to_string(sidecar) else {
            return ContinuityVerdict::FirstRun;
        };
        let Ok(prev) = serde_json::from_str::<Continuity>(&prev_raw) else {
            return ContinuityVerdict::Broken(
                "the continuity record itself is unreadable".into(),
            );
        };
        let Ok(raw) = std::fs::read_to_string(&self.path) else {
            return ContinuityVerdict::Broken(format!("cannot read {}", self.path));
        };
        let total = raw.lines().count() as u64;
        if total < prev.lines {
            return ContinuityVerdict::Broken(
                format!(
                    "the ledger LOST history: {total} lines now, {} at the last boot",
                    prev.lines
                ),
            );
        }
        let (digest, seen) = prefix_digest(&raw, prev.lines);
        if seen < prev.lines {
            return ContinuityVerdict::Broken(
                format!(
                    "only {seen} of the {} previously replayed lines remain", prev.lines
                ),
            );
        }
        if digest != prev.prefix_sha256 {
            return ContinuityVerdict::Broken(
                format!(
                    "the first {} lines no longer hash to what they did at the last boot — \
history was REWRITTEN, not appended to",
                    prev.lines
                ),
            );
        }
        ContinuityVerdict::Intact
    }
    pub fn record_continuity(&self, sidecar: &str) -> Result<(), String> {
        let Some(c) = self.continuity() else {
            return Err(format!("cannot read {} to fingerprint it", self.path));
        };
        let body = serde_json::to_string(&c).map_err(|e| e.to_string())?;
        let tmp = format!("{sidecar}.tmp");
        std::fs::write(&tmp, body).map_err(|e| format!("write {tmp}: {e}"))?;
        std::fs::rename(&tmp, sidecar).map_err(|e| format!("replace {sidecar}: {e}"))
    }
}
