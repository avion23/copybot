use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::ledger::Ledger;
use crate::lanes::{Router, MICRO};
pub struct Control {
    pub ledger: Ledger,
    pub operator_path: String,
    pub his_pos: HashMap<String, HashMap<String, f64>>,
    pub legacy: HashMap<String, HashMap<String, f64>>,
    pub fires: u64,
    pub skips: HashMap<String, u64>,
    pub ambiguous: Vec<serde_json::Value>,
    pub trips: Vec<serde_json::Value>,
    pub reconciliations: Vec<serde_json::Value>,
    pub boot_faults: HashMap<String, String>,
    pub guardian_stale: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
fn halt_causes(
    reason: &Option<String>,
    persistence_fault: bool,
    boot_fault: Option<&String>,
) -> Vec<String> {
    let mut causes = Vec::new();
    if let Some(r) = reason {
        causes.push(format!("breaker: {r}"));
    }
    if persistence_fault {
        causes.push("ledger persistence fault".to_string());
    }
    if let Some(f) = boot_fault {
        causes.push(format!("boot fault: {f}"));
    }
    causes
}
impl Control {
    pub fn new(ledger: Ledger, operator_path: &str) -> Self {
        let his_pos = ledger.lanes.keys().map(|k| (k.clone(), HashMap::new())).collect();
        let legacy = ledger.lanes.keys().map(|k| (k.clone(), HashMap::new())).collect();
        Self {
            ledger,
            operator_path: operator_path.into(),
            his_pos,
            legacy,
            fires: 0,
            skips: HashMap::new(),
            ambiguous: Vec::new(),
            trips: Vec::new(),
            reconciliations: Vec::new(),
            boot_faults: HashMap::new(),
            guardian_stale: std::sync::Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
        }
    }
    pub fn set_boot_fault(&mut self, lane: &str, reason: impl Into<String>) {
        self.boot_faults.entry(lane.to_string()).or_insert_with(|| reason.into());
    }
    pub fn operator_intent(&self) -> HashMap<String, bool> {
        let raw = match std::fs::read_to_string(&self.operator_path) {
            Ok(r) => r,
            Err(_) => return HashMap::new(),
        };
        let v: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => return HashMap::new(),
        };
        v["lanes"]
            .as_object()
            .map(|o| {
                o
                    .iter()
                    .map(|(k, x)| (k.clone(), x["armed"].as_bool().unwrap_or(false)))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn operator_halts(&self) -> HashMap<String, bool> {
        let raw = match std::fs::read_to_string(&self.operator_path) {
            Ok(r) => r,
            Err(_) => return HashMap::new(),
        };
        let v: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => return HashMap::new(),
        };
        v["lanes"]
            .as_object()
            .map(|o| {
                o
                    .iter()
                    .map(|(k, x)| (k.clone(), x["halt_buys"].as_bool().unwrap_or(false)))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn seed_his_book(
        &mut self,
        lane: &str,
        positions: &HashMap<String, f64>,
    ) -> usize {
        let mut marked = 0;
        let held = self.ledger.holdings(lane);
        for (token, shares) in positions {
            self.his_pos.entry(lane.into()).or_default().insert(token.clone(), *shares);
            if held.get(token).copied().unwrap_or(0.0) > 1e-9 {
                continue;
            }
            self.legacy.entry(lane.into()).or_default().insert(token.clone(), *shares);
            marked += 1;
        }
        marked
    }
    pub fn observe_his_fill(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        size: f64,
    ) -> f64 {
        let hp = self.his_pos.entry(lane.into()).or_default();
        let cur = hp.get(token).copied().unwrap_or(0.0);
        let next = if side == 0 { cur + size } else { (cur - size).max(0.0) };
        hp.insert(token.into(), next);
        if side == 1 {
            if let Some(l) = self.legacy.get_mut(lane) {
                if let Some(v) = l.get_mut(token) {
                    *v = (*v - size).max(0.0);
                }
            }
        }
        next
    }
    /// Obsolete pre-A4 fallback retained ONLY for historical unit tests.
    /// This symbol must not exist in a production build: it can default
    /// unknown fees to zero and prices to the limit.
    #[cfg(test)]
    pub fn book_response(
        &mut self,
        lane: &str,
        token: &str,
        side: u8,
        limit: f64,
        raw: &str,
        his_price: Option<f64>,
        order_hash: &str,
        resting: bool,
    ) -> Option<crate::ledger::Booked> {
        let resp: serde_json::Value = serde_json::from_str(raw)
            .unwrap_or(serde_json::Value::Null);
        let Some(book) = self.ledger.lanes.get_mut(lane) else { return None };
        if !resp.is_object() {
            book.rejected += 1;
            return None;
        }
        let err = resp["errorMsg"]
            .as_str()
            .filter(|s| !s.is_empty())
            .or_else(|| resp["error"].as_str());
        if resp["success"] == serde_json::json!(false) || err.is_some() {
            book.rejected += 1;
            return None;
        }
        let share_key = if side == 0 { "takingAmount" } else { "makingAmount" };
        let read = |k: &str| {
            resp[k]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| resp[k].as_f64())
        };
        let mut size = [
            "size_matched",
            "sizeMatched",
            share_key,
            "filled_size",
            "filledSize",
        ]
            .iter()
            .find_map(|k| read(k))
            .unwrap_or(0.0);
        let matched = resp["status"]
            .as_str()
            .map(|s| {
                let s = s.to_ascii_lowercase();
                s == "matched" || s == "filled" || s == "success"
            })
            .unwrap_or(false);
        let has_tx = resp["transactionsHashes"].is_array()
            || resp["transactionHashes"].is_array();
        if size <= 0.0 {
            if matched || has_tx || resp["success"] == serde_json::json!(true) {
                self.ambiguous
                    .push(
                        serde_json::json!(
                            { "lane" : lane, "token" : token, "raw" : raw.chars()
                            .take(200).collect::< String > () }
                        ),
                    );
            } else {
                book.rejected += 1;
            }
            return None;
        }
        let other_keys: &[&str] = if side == 0 {
            &[
                "makingAmount",
                "making_amount",
                "makerAmount",
                "maker_amount",
                "matched_amount",
                "matchedAmount",
                "value",
                "usd",
            ]
        } else {
            &[
                "takingAmount",
                "taking_amount",
                "takerAmount",
                "taker_amount",
                "matched_amount",
                "matchedAmount",
                "value",
                "usd",
            ]
        };
        let mut price = limit;
        let mut px_provisional = true;
        if let Some(usd) = other_keys.iter().find_map(|k| read(k)) {
            if usd > 0.0 && size > 0.0 {
                price = usd / size;
                px_provisional = false;
            }
        }
        if side == 1 {
            if let Some(p) = book.positions.get(token) {
                size = size.min(p.shares);
            }
        }
        if size > 0.0 {
            let fee = ["fee", "feeAmount", "fee_amount", "makerFee", "takerFee"]
                .iter()
                .find_map(|k| read(k))
                .or_else(|| {
                    ["fee_rate_bps", "feeRateBps"]
                        .iter()
                        .find_map(|k| read(k))
                        .map(|bps| bps / 10_000.0 * price * size)
                })
                .filter(|f| f.is_finite() && *f >= 0.0)
                .unwrap_or(0.0);
            if fee > 0.0 {
                eprintln!(
                    "[fee] ⚠️ VENUE REPORTED A FEE: {fee:.6} on {lane} {token} \
                           — fee-free market assumption no longer holds"
                );
            }
            if px_provisional {
                self.ambiguous
                    .push(
                        serde_json::json!(
                            { "lane" : lane, "token" : token, "why" :
                            "price PROVISIONAL: the venue reply carried no amount leg, so \
this fill is booked at our LIMIT — dearer than truth on a buy, cheaper on a sell"
                            }
                        ),
                    );
            }
            self.ledger
                .book_fill_ex_px(
                    lane,
                    token,
                    side,
                    size,
                    price,
                    fee,
                    his_price,
                    order_hash,
                    resting,
                    px_provisional,
                )
        } else {
            None
        }
    }
    pub fn tick(&mut self, router: &Arc<Router>) {
        self.tick_with(router, &crate::pending::InFlight::default())
    }
    pub fn tick_with(
        &mut self,
        router: &Arc<Router>,
        in_flight: &crate::pending::InFlight,
    ) {
        // This cache refresh must not replace reservations between admission
        // checks and their accounting mutations. A poisoned owner refuses
        // new admission and visibly disarms every lane instead of guessing.
        let _admission = match router.admission_owner().enter() {
            Ok(guard) => guard,
            Err(error) => {
                eprintln!("order admission owner unavailable: {error:?}");
                for lane in router.snapshot() {
                    lane.state.armed.store(false, Ordering::Relaxed);
                    lane.state.halted.store(true, Ordering::Relaxed);
                }
                return;
            }
        };
        let intent = self.operator_intent();
        let halts = self.operator_halts();
        let persistence_fault = !self.ledger.persistence_ok();
        for lane in router.snapshot().iter() {
            let name = &lane.cfg.name;
            let Some(book) = self.ledger.lanes.get_mut(name) else { continue };
            book.roll_day(crate::ledger::now_secs());
            let reason = book.risk.evaluate();
            if let Some(r) = &reason {
                if !self.trips.iter().any(|t| t["lane"] == serde_json::json!(name)) {
                    self.trips.push(serde_json::json!({ "lane" : name, "reason" : r }));
                }
            }
            let wants = intent.get(name).copied().unwrap_or(false);
            let boot_fault = self.boot_faults.get(name);
            let breaker = reason.is_some() || persistence_fault || boot_fault.is_some();
            if breaker {
                if !lane.state.halt_latch.load(Ordering::Relaxed) {
                    eprintln!(
                        "[{name}] HALT_LATCH TRIP: {}", halt_causes(& reason,
                        persistence_fault, boot_fault).join("; ")
                    );
                }
                lane.state.halt_latch.store(true, Ordering::Relaxed);
            }
            let latched = lane.state.halt_latch.load(Ordering::Relaxed);
            let op_halt = halts.get(name).copied().unwrap_or(false);
            let unwatched = self.guardian_stale.load(Ordering::Relaxed);
            let halted = latched || op_halt || unwatched;
            let prev = lane.state.halted_since.load(Ordering::Relaxed);
            if halted && prev == 0 {
                lane.state
                    .halted_since
                    .store(crate::ledger::now_secs(), Ordering::Relaxed);
            } else if !halted && prev != 0 {
                lane.state.halted_since.store(0, Ordering::Relaxed);
            }
            lane.state.armed.store(wants && boot_fault.is_none(), Ordering::Relaxed);
            lane.state.halted.store(halted, Ordering::Relaxed);
            let flight = in_flight.by_lane.get(name).copied().unwrap_or(0.0);
            lane.state
                .spent_today
                .store(((book.spent_today + flight) * MICRO) as i64, Ordering::Relaxed);
            lane.state
                .open_usd
                .store(
                    ((self.ledger.open_usd(name) + flight) * MICRO) as i64,
                    Ordering::Relaxed,
                );
            let per_token = self
                .ledger
                .lanes
                .get(name)
                .map(|b| {
                    b.positions
                        .iter()
                        .filter(|(_, p)| p.shares > 1e-9 && p.cost > 1e-9)
                        .map(|(token, p)| (token.clone(), (p.cost * MICRO) as i64))
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default();
            let mut per_token = per_token;
            for ((l, tok), usd) in in_flight.by_token.iter() {
                if l != name {
                    continue;
                }
                *per_token.entry(tok.clone()).or_insert(0) += (*usd * MICRO) as i64;
            }
            *lane.per_token.lock().unwrap() = per_token;
            let held = self.ledger.holdings(name);
            let stale: Vec<String> = lane
                .holdings
                .lock()
                .unwrap()
                .keys()
                .filter(|k| !held.contains_key(*k))
                .cloned()
                .collect();
            for t in stale {
                lane.set_holding(&t, 0.0);
                router.release_by_name(&t, name);
            }
            let claimed = router.claims_of(name);
            for t in claimed {
                if held.contains_key(&t) {
                    continue;
                }
                if in_flight.tokens.iter().any(|(l, tok)| l == name && tok == &t) {
                    continue;
                }
                router.release_by_name(&t, name);
            }
            for (t, sh) in &held {
                lane.set_holding(t, *sh);
            }
            let mut reserved: HashMap<String, f64> = HashMap::new();
            for ((l, tok), sh) in in_flight.sell_shares_by_token.iter() {
                if l != name {
                    continue;
                }
                reserved.insert(tok.clone(), *sh);
            }
            lane.set_sell_reservations(reserved);
            if let Some(hp) = self.his_pos.get(name) {
                for (t, v) in hp {
                    lane.set_his(t, *v);
                }
            }
            if let Some(lg) = self.legacy.get(name) {
                for (t, v) in lg {
                    lane.set_legacy(t, *v);
                }
            }
        }
    }
    pub fn status(&self, router: &Arc<Router>) -> serde_json::Value {
        let lanes: serde_json::Value = router
            .snapshot()
            .iter()
            .map(|l| {
                let name = &l.cfg.name;
                let b = self.ledger.lanes.get(name);
                (
                    name.clone(),
                    serde_json::json!(
                        { "armed" : l.state.armed.load(Ordering::Relaxed), "halted" : l
                        .state.halted.load(Ordering::Relaxed), "halted_since" : l.state
                        .halted_since.load(Ordering::Relaxed), "ready" : l.state.ready
                        .load(Ordering::Relaxed), "retired" : l.state.retired
                        .load(Ordering::Relaxed), "risk" : b.map(| x | x.risk
                        .snapshot()), "spent_today" : b.map(| x | x.spent_today),
                        "open_usd" : self.ledger.open_usd(name), "cap_scale" : l.state
                        .cap_scale.load(Ordering::Relaxed) as f64 / MICRO, "holdings" :
                        self.ledger.holdings(name), "fired" : b.map(| x | x.fired),
                        "filled" : b.map(| x | x.filled), "rejected" : b.map(| x | x
                        .rejected), "boot_fault" : self.boot_faults.get(name), }
                    ),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into();
        serde_json::json!(
            { "fires" : self.fires, "skips" : self.skips, "ambiguous" : self.ambiguous
            .len(), "trips" : self.trips, "reconciliations" : self.reconciliations.len(),
            "corrupt_ledger_lines" : self.ledger.corrupt_lines, "ledger_persistence" : {
            "ok" : self.ledger.persistence_ok(), "write_failures" : self.ledger
            .write_failures, "last_error" : self.ledger.last_write_error, }, "lanes" :
            lanes, }
        )
    }
    pub fn record_fire(&mut self, lane: &str) {
        self.fires += 1;
        if let Some(book) = self.ledger.lanes.get_mut(lane) {
            book.fired += 1;
        }
    }
}
#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
