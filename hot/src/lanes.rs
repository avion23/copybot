use crate::signal_guard::Progress;
use std::collections::HashMap;
pub const CLAIM_GRACE_SECS: i64 = 60;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;
use crate::calldata::Decoded;
use crate::venue::{ceil_shares, sell_terms, whole};
pub type Micro = i64;
pub const MICRO: f64 = 1e6;
fn checked_micro(value: f64) -> Result<Micro, Skip> {
    if !value.is_finite() || value < 0.0 || value >= Micro::MAX as f64 {
        return Err(Skip::InvalidMoneyAmount);
    }
    Ok(value as Micro)
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sizing {
    Shares(f64),
    Usd(f64),
    Pct(f64),
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Execution {
    Taker,
    Hybrid,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Skip {
    Disarmed,
    Halted,
    TokenOwnedByOtherLane,
    DailyBudget,
    PerMarketCap,
    MaxOpen,
    PriceOutOfBand,
    BelowVenueMinimum,
    DustAfterSizing,
    SellNoPosition,
    SellAlreadyReserved,
    NotReady,
    InvalidFeeEstimate,
    InvalidSignalEvidence,
    InvalidMoneyAmount,
    OrderStateUnavailable,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Route {
    Take,
    RestInFront { limit: f64, room_ticks: i64 },
}
#[derive(Debug, Clone, PartialEq)]
pub struct Intent {
    pub lane: usize,
    pub token_id: String,
    pub side: u8,
    pub shares: f64,
    pub limit: f64,
    /// Order notional before fee, in micro-dollars.
    pub usd: Micro,
    /// Fee-inclusive economic reservation, not the physical USDC debit.
    pub budget_usd: Micro,
    pub execution: Execution,
    pub his_remaining: f64,
    pub he_was_maker: bool,
    pub route: Route,
}
#[derive(Debug, Clone)]
pub struct LaneConfig {
    pub name: String,
    pub wallet20: [u8; 20],
    pub sizing: Sizing,
    pub execution: Execution,
    pub buy_slippage_c: f64,
    pub sell_slippage_c: f64,
    pub copy_maker_sells: bool,
    pub sell_floor_frac: f64,
    pub min_order_usd: f64,
    pub max_usd_per_fill: f64,
    pub daily_budget_usd: f64,
    pub per_market_usd: f64,
    pub max_open_usd: f64,
    pub max_buy_price: f64,
    pub min_buy_price: f64,
    pub min_fill_floor: bool,
    pub sell_all_frac: f64,
    pub max_effective_pct: f64,
    pub compound: bool,
    pub copy_makers: bool,
    pub exclude_political: bool,
}
impl LaneConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_usd_per_fill > self.per_market_usd {
            return Err(
                format!(
                    "lane {}: max_usd_per_fill ${} exceeds per_market_usd ${} — the per-fill \
                 clamp can never bind, tuning it will appear to do nothing",
                    self.name, self.max_usd_per_fill, self.per_market_usd
                ),
            );
        }
        if self.per_market_usd > self.max_open_usd {
            return Err(
                format!("lane {}: per_market_usd exceeds max_open_usd", self.name),
            );
        }
        if !self.sell_all_frac.is_finite() || !(0.0..=1.0).contains(&self.sell_all_frac) {
            return Err(format!("lane {}: sell_all_frac must be in [0,1]", self.name));
        }
        if self.min_order_usd < 1.0 {
            return Err(
                format!(
                    "lane {}: min_order_usd below the $1 VENUE floor — orders will be \
                 rejected outright",
                    self.name
                ),
            );
        }
        if self.min_buy_price <= 0.0 || self.max_buy_price >= 1.0
            || self.min_buy_price >= self.max_buy_price
        {
            return Err(
                format!("lane {}: price band is not 0 < min < max < 1", self.name),
            );
        }
        match self.sizing {
            Sizing::Shares(n) if n <= 0.0 => {
                Err(format!("lane {}: shares <= 0", self.name))
            }
            Sizing::Usd(u) if u <= 0.0 => Err(format!("lane {}: usd <= 0", self.name)),
            Sizing::Pct(p) if !(p > 0.0 && p <= 1.0) => {
                Err(format!("lane {}: pct must be in (0,1]", self.name))
            }
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Clone)]
pub struct SizingPolicy {
    pub generation: u64,
    pub seed_usd: f64,
    pub sizing: Sizing,
    pub max_effective_pct: f64,
    pub compound: bool,
    pub caps: crate::budget::Caps,
}
impl SizingPolicy {
    pub fn build(
        generation: u64,
        seed_usd: f64,
        sizing: Sizing,
        max_effective_pct: f64,
        compound: bool,
        fracs: &crate::budget::Fracs,
    ) -> Result<Self, String> {
        if !(seed_usd.is_finite() && seed_usd > 0.0) {
            return Err(format!("seed_usd must be a positive number, got {seed_usd}"));
        }
        if !(max_effective_pct.is_finite() && max_effective_pct > 0.0
            && max_effective_pct <= 1.0)
        {
            return Err(
                format!("max_effective_pct must be in (0,1], got {max_effective_pct}"),
            );
        }
        if let Sizing::Pct(p) = sizing {
            if !(p.is_finite() && p > 0.0 && p <= 1.0) {
                return Err(format!("pct must be in (0,1], got {p}"));
            }
        }
        crate::budget::validate_fracs(fracs)?;
        let caps = crate::budget::derive(seed_usd, fracs);
        if !(caps.max_usd_per_fill <= caps.per_market_usd
            && caps.per_market_usd <= caps.max_open_usd)
        {
            return Err("derived caps violate fill <= market <= open".into());
        }
        Ok(Self {
            generation,
            seed_usd,
            sizing,
            max_effective_pct,
            compound,
            caps,
        })
    }
}
#[derive(Debug)]
pub struct LaneState {
    pub armed: AtomicBool,
    pub rest_buys: AtomicBool,
    pub rest_sells: AtomicBool,
    pub halted: AtomicBool,
    pub halt_latch: AtomicBool,
    pub spent_today: AtomicI64,
    pub open_usd: AtomicI64,
    pub cap_scale: AtomicI64,
    pub retired: AtomicBool,
    pub ready: AtomicBool,
    pub halted_since: AtomicI64,
    pub policy: std::sync::RwLock<std::sync::Arc<SizingPolicy>>,
}
impl Default for LaneState {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            rest_buys: AtomicBool::new(false),
            rest_sells: AtomicBool::new(false),
            halted: AtomicBool::new(false),
            halt_latch: AtomicBool::new(false),
            spent_today: AtomicI64::new(0),
            open_usd: AtomicI64::new(0),
            cap_scale: AtomicI64::new(MICRO as i64),
            retired: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            halted_since: AtomicI64::new(0),
            policy: std::sync::RwLock::new(
                std::sync::Arc::new(SizingPolicy {
                    generation: 0,
                    seed_usd: 1.0,
                    sizing: Sizing::Pct(0.05),
                    max_effective_pct: 0.05,
                    compound: false,
                    caps: crate::budget::Caps {
                        max_open_usd: 0.0,
                        per_market_usd: 0.0,
                        max_usd_per_fill: 0.0,
                        daily_usd: 0.0,
                    },
                }),
            ),
        }
    }
}
pub struct Lane {
    pub cfg: LaneConfig,
    /// The cap ratios that belong to this lane. Runtime seed changes must reuse these
    /// ratios; `Fracs::default()` is only the default for a newly-created runtime lane.
    pub budget_fracs: crate::budget::Fracs,
    pub state: LaneState,
    pub per_token: Mutex<HashMap<String, Micro>>,
    pub holdings: Mutex<HashMap<String, f64>>,
    pub his_pos: Mutex<HashMap<String, f64>>,
    pub legacy: Mutex<HashMap<String, f64>>,
    pub sell_reservations: Mutex<HashMap<String, f64>>,
}
impl Lane {
    pub fn new(cfg: LaneConfig) -> Self {
        let state = LaneState::default();
        state
            .rest_buys
            .store(
                cfg.copy_makers || cfg.execution == Execution::Hybrid,
                Ordering::Relaxed,
            );
        state
            .rest_sells
            .store(
                cfg.copy_maker_sells || cfg.execution == Execution::Hybrid,
                Ordering::Relaxed,
            );
        let seeded = SizingPolicy {
            generation: 0,
            seed_usd: 0.0,
            sizing: cfg.sizing,
            max_effective_pct: cfg.max_effective_pct,
            compound: cfg.compound,
            caps: crate::budget::Caps {
                max_open_usd: cfg.max_open_usd,
                per_market_usd: cfg.per_market_usd,
                max_usd_per_fill: cfg.max_usd_per_fill,
                daily_usd: cfg.daily_budget_usd,
            },
        };
        if let Ok(mut g) = state.policy.write() {
            *g = std::sync::Arc::new(seeded);
        }
        Self {
            cfg,
            budget_fracs: crate::budget::Fracs::default(),
            state,
            per_token: Mutex::new(HashMap::new()),
            holdings: Mutex::new(HashMap::new()),
            his_pos: Mutex::new(HashMap::new()),
            legacy: Mutex::new(HashMap::new()),
            sell_reservations: Mutex::new(HashMap::new()),
        }
    }
    /// Construct a bankroll-backed lane with its initial policy already carrying the
    /// authoritative seed and fractions. This makes the first registry reconciliation a
    /// no-op and gives later seed edits the same cap ratios as boot.
    pub fn new_seeded(
        cfg: LaneConfig,
        seed_usd: f64,
        budget_fracs: crate::budget::Fracs,
    ) -> Result<Self, String> {
        let policy = SizingPolicy::build(
            0,
            seed_usd,
            cfg.sizing,
            cfg.max_effective_pct,
            cfg.compound,
            &budget_fracs,
        )?;
        let mut lane = Self::new(cfg);
        lane.budget_fracs = budget_fracs;
        let mut slot = lane
            .state
            .policy
            .write()
            .map_err(|_| "policy lock poisoned while seeding lane".to_string())?;
        *slot = std::sync::Arc::new(policy);
        drop(slot);
        Ok(lane)
    }
    pub fn set_holding(&self, token: &str, shares: f64) {
        let mut h = self.holdings.lock().unwrap();
        if shares <= 1e-9 {
            h.remove(token);
        } else {
            h.insert(token.into(), shares);
        }
    }
    pub fn mark_ready(&self) {
        self.state.ready.store(true, Ordering::Relaxed);
    }
    pub fn mark_activating(&self) {
        self.state.ready.store(false, Ordering::Relaxed);
    }
    pub fn holding(&self, token: &str) -> f64 {
        *self.holdings.lock().unwrap().get(token).unwrap_or(&0.0)
    }
    pub fn set_his(&self, token: &str, shares: f64) {
        self.his_pos.lock().unwrap().insert(token.into(), shares.max(0.0));
    }
    pub fn set_legacy(&self, token: &str, shares: f64) {
        let mut l = self.legacy.lock().unwrap();
        if shares <= 1e-9 {
            l.remove(token);
        } else {
            l.insert(token.into(), shares);
        }
    }
    pub fn sell_reserved(&self, token: &str) -> f64 {
        *self.sell_reservations.lock().unwrap().get(token).unwrap_or(&0.0)
    }
    pub fn set_sell_reservations(&self, m: HashMap<String, f64>) {
        *self.sell_reservations.lock().unwrap() = m;
    }
}
/// One admission gate for the decision/reservation caches and the
/// ledger-derived control refresh. No second writer can reset spent/open or
/// sell reservations while another thread is checking and reserving an order.
/// Durable pending/WAL ownership remains a separate migration seam.
#[derive(Default)]
pub struct OrderAdmissionOwner {
    serial: Mutex<()>,
}
impl OrderAdmissionOwner {
    pub fn enter(&self) -> Result<std::sync::MutexGuard<'_, ()>, Skip> {
        self.serial.lock().map_err(|_| Skip::OrderStateUnavailable)
    }
}
pub struct Router {
    lanes: std::sync::RwLock<Vec<std::sync::Arc<Lane>>>,
    owner: Mutex<HashMap<String, (usize, i64)>>,
    admission: OrderAdmissionOwner,
}
impl Router {
    pub fn admission_owner(&self) -> &OrderAdmissionOwner {
        &self.admission
    }
    pub fn new(lanes: Vec<Lane>) -> Self {
        let lanes = lanes.into_iter().map(std::sync::Arc::new).collect();
        Router {
            lanes: std::sync::RwLock::new(lanes),
            owner: Mutex::new(HashMap::new()),
            admission: OrderAdmissionOwner::default(),
        }
    }
    pub fn snapshot(&self) -> Vec<std::sync::Arc<Lane>> {
        self.lanes.read().unwrap().clone()
    }
    pub fn len(&self) -> usize {
        self.lanes.read().unwrap().len()
    }
    pub fn is_empty(&self) -> bool {
        self.lanes.read().unwrap().is_empty()
    }
    pub fn lane(&self, ix: usize) -> Option<std::sync::Arc<Lane>> {
        self.lanes.read().unwrap().get(ix).cloned()
    }
    pub fn push(&self, lane: Lane) -> usize {
        let mut g = self.lanes.write().unwrap();
        g.push(std::sync::Arc::new(lane));
        g.len() - 1
    }
    #[cfg(test)]
    pub fn lane_for(&self, wallet20: &[u8; 20]) -> Option<usize> {
        self.lanes.read().unwrap().iter().position(|l| &l.cfg.wallet20 == wallet20)
    }
    pub fn claim(&self, token: &str, lane_ix: usize) {
        self.owner
            .lock()
            .unwrap()
            .entry(token.to_string())
            .or_insert((lane_ix, crate::ledger::now_secs()));
    }
    pub fn release(&self, token: &str, lane: usize) {
        let mut o = self.owner.lock().unwrap();
        if o.get(token).map(|(ix, _)| *ix) == Some(lane) {
            o.remove(token);
        }
    }
    pub fn release_by_name(&self, token: &str, lane_name: &str) {
        let ix = self.lanes.read().unwrap().iter().position(|l| l.cfg.name == lane_name);
        if let Some(ix) = ix {
            self.release(token, ix);
        }
    }
    pub fn claims_of(&self, lane_name: &str) -> Vec<String> {
        let ix = match self
            .lanes
            .read()
            .unwrap()
            .iter()
            .position(|l| l.cfg.name == lane_name)
        {
            Some(i) => i,
            None => return Vec::new(),
        };
        let cutoff = crate::ledger::now_secs() - CLAIM_GRACE_SECS;
        self.owner
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, &(o, at))| o == ix && at <= cutoff)
            .map(|(t, _)| t.clone())
            .collect()
    }
    pub fn backdate_claims_for_test(&self, secs: i64) {
        for (_, v) in self.owner.lock().unwrap().iter_mut() {
            v.1 -= secs;
        }
    }
    pub fn owner_of(&self, token: &str) -> Option<usize> {
        self.owner.lock().unwrap().get(token).map(|(ix, _)| *ix)
    }
    pub fn claim_existing(&self, token: &str, lane_ix: usize) -> bool {
        let mut o = self.owner.lock().unwrap();
        match o.get(token) {
            Some(_) => false,
            None => {
                o.insert(token.to_string(), (lane_ix, crate::ledger::now_secs()));
                true
            }
        }
    }
    pub fn restore_ownership(
        &self,
        held_by_lane: &[(usize, String, Vec<String>)],
    ) -> (usize, Vec<(String, String, String)>) {
        let mut restored = 0usize;
        let mut owner_of: HashMap<String, String> = HashMap::new();
        let mut conflicts = Vec::new();
        for (ix, name, tokens) in held_by_lane {
            for tok in tokens {
                if self.claim_existing(tok, *ix) {
                    restored += 1;
                    owner_of.insert(tok.clone(), name.clone());
                } else {
                    conflicts
                        .push((
                            tok.clone(),
                            owner_of.get(tok).cloned().unwrap_or_default(),
                            name.clone(),
                        ));
                }
            }
        }
        (restored, conflicts)
    }
    pub fn decide(
        &self,
        lane_ix: usize,
        d: &Decoded,
        progress: Progress,
    ) -> Result<Intent, Skip> {
        let _admission = self.admission.enter()?;
        if d.side != 0 && d.side != 1
            || !d.price.is_finite() || !(0.0..=1.0).contains(&d.price)
            || !d.fill_size.is_finite() || d.fill_size <= 0.0
            || !d.order_size.is_finite() || d.order_size < d.fill_size
        {
            return Err(Skip::InvalidSignalEvidence);
        }
        let lane = match self.lane(lane_ix) {
            Some(l) => l,
            None => return Err(Skip::Disarmed),
        };
        let c = &lane.cfg;
        if !lane.state.ready.load(Ordering::Relaxed) {
            return Err(Skip::NotReady);
        }
        if !lane.state.armed.load(Ordering::Relaxed) {
            return Err(Skip::Disarmed);
        }
        if d.side == 0 && lane.state.halted.load(Ordering::Relaxed) {
            return Err(Skip::Halted);
        }
        if d.side == 0 && lane.state.retired.load(Ordering::Relaxed) {
            return Err(Skip::Halted);
        }
        if d.side == 0 {
            if let Some((owner, _)) = self.owner.lock().unwrap().get(&d.token_id) {
                if *owner != lane_ix {
                    return Err(Skip::TokenOwnedByOtherLane);
                }
            }
        }
        if d.side == 1 {
            let mut resv = lane.sell_reservations.lock().unwrap();
            let gross = lane.holding(&d.token_id);
            let committed = *resv.get(&d.token_id).unwrap_or(&0.0);
            let held = (gross - committed).max(0.0);
            if held <= 1e-9 {
                return Err(
                    if gross > 1e-9 {
                        Skip::SellAlreadyReserved
                    } else {
                        Skip::SellNoPosition
                    },
                );
            }
            let his_before = {
                let hp = lane.his_pos.lock().unwrap();
                *hp.get(&d.token_id).unwrap_or(&0.0)
            };
            let leg = *lane.legacy.lock().unwrap().get(&d.token_id).unwrap_or(&0.0);
            let frac = if his_before <= 1e-9 {
                1.0
            } else if leg > 1e-9 {
                let matched_before = (his_before - leg).max(1e-9);
                let sold_matched = (d.fill_size - leg.min(d.fill_size)).max(0.0);
                (sold_matched / matched_before).min(1.0)
            } else {
                (d.fill_size / his_before).min(1.0)
            };
            if frac <= 1e-9 {
                return Err(Skip::SellNoPosition);
            }
            let want = if frac >= c.sell_all_frac { held } else { held * frac };
            let desired = (d.price - c.sell_slippage_c)
                .max(d.price * c.sell_floor_frac)
                .max(0.01);
            let (shares, limit) = sell_terms(want.min(held), desired, 0.06);
            if !shares.is_finite() || !limit.is_finite() {
                return Err(Skip::InvalidMoneyAmount);
            }
            if shares <= 0.0 {
                return Err(Skip::DustAfterSizing);
            }
            *resv.entry(d.token_id.clone()).or_insert(0.0) += shares;
            drop(resv);
            self.claim(&d.token_id, lane_ix);
            return Ok(Intent {
                lane: lane_ix,
                token_id: d.token_id.clone(),
                side: 1,
                shares,
                limit,
                usd: 0,
                budget_usd: 0,
                execution: c.execution,
                his_remaining: (d.order_size - d.fill_size).max(0.0),
                he_was_maker: d.role == "maker",
                route: Route::Take,
            });
        }
        if d.price > c.max_buy_price || d.price < c.min_buy_price {
            return Err(Skip::PriceOutOfBand);
        }
        let limit = crate::venue::buy_limit((d.price + c.buy_slippage_c).min(0.99));
        let scale = lane.state.cap_scale.load(Ordering::Relaxed) as f64 / MICRO;
        let pol: std::sync::Arc<SizingPolicy> = match lane.state.policy.read() {
            Ok(g) => g.clone(),
            Err(_) => return Err(Skip::NotReady),
        };
        let mut shares = match pol.sizing {
            Sizing::Shares(n) => ceil_shares(n),
            Sizing::Usd(u) => whole(u / limit),
            Sizing::Pct(p) => {
                let target = progress.his_filled.max(d.fill_size)
                    * crate::budget::effective_pct(
                        p,
                        scale,
                        pol.max_effective_pct,
                        pol.compound,
                    ) * (d.price / limit);
                whole(target - progress.our_copied)
            }
        };
        let first_clip = progress.our_copied <= 0.0;
        if shares <= 0.0 {
            if !c.min_fill_floor || !first_clip {
                return Err(Skip::DustAfterSizing);
            }
            shares = 0.0;
        }
        let mut usd = shares * limit;
        if !usd.is_finite() || !shares.is_finite() {
            return Err(Skip::InvalidMoneyAmount);
        }
        if usd < c.min_order_usd {
            if !c.min_fill_floor || !first_clip {
                return Err(Skip::BelowVenueMinimum);
            }
            shares = ceil_shares(c.min_order_usd / limit);
            usd = shares * limit;
        }
        if shares < crate::venue::VENUE_MIN_SHARES {
            return Err(Skip::BelowVenueMinimum);
        }
        let Some(cost_per_share) = crate::budget::buy_budget_cost(1.0, limit) else {
            return Err(Skip::InvalidFeeEstimate);
        };
        let cap_fill = pol.caps.max_usd_per_fill * scale;
        if shares * cost_per_share > cap_fill {
            shares = whole(cap_fill / cost_per_share);
            if shares <= 0.0 {
                return Err(Skip::DustAfterSizing);
            }
            usd = shares * limit;
        }
        if shares < crate::venue::VENUE_MIN_SHARES || usd < c.min_order_usd {
            return Err(Skip::BelowVenueMinimum);
        }
        let usd_micro = checked_micro(usd * MICRO)?;
        let Some(cost) = crate::budget::buy_budget_cost(shares, limit) else {
            return Err(Skip::InvalidFeeEstimate);
        };
        let budget_micro = checked_micro((cost * MICRO).ceil())?;
        let day_cap = checked_micro(pol.caps.daily_usd * scale * MICRO)?;
        let new_day = lane.state.spent_today.load(Ordering::Relaxed)
            .checked_add(budget_micro).ok_or(Skip::InvalidMoneyAmount)?;
        if new_day > day_cap {
            return Err(Skip::DailyBudget);
        }
        let open_cap = checked_micro(pol.caps.max_open_usd * scale * MICRO)?;
        let new_open = lane.state.open_usd.load(Ordering::Relaxed)
            .checked_add(budget_micro).ok_or(Skip::InvalidMoneyAmount)?;
        if new_open > open_cap {
            return Err(Skip::MaxOpen);
        }
        {
            let mut pt = lane.per_token.lock().unwrap();
            let spent = *pt.get(&d.token_id).unwrap_or(&0);
            let new_market = spent.checked_add(budget_micro)
                .ok_or(Skip::InvalidMoneyAmount)?;
            if new_market > checked_micro(pol.caps.per_market_usd * scale * MICRO)? {
                return Err(Skip::PerMarketCap);
            }
            pt.insert(d.token_id.clone(), new_market);
        }
        lane.state.spent_today.fetch_add(budget_micro, Ordering::Relaxed);
        lane.state.open_usd.fetch_add(budget_micro, Ordering::Relaxed);
        self.claim(&d.token_id, lane_ix);
        Ok(Intent {
            lane: lane_ix,
            token_id: d.token_id.clone(),
            side: 0,
            shares,
            limit,
            usd: usd_micro,
            budget_usd: budget_micro,
            execution: c.execution,
            his_remaining: (d.order_size - d.fill_size).max(0.0),
            he_was_maker: d.role == "maker",
            route: Route::Take,
        })
    }
}
pub struct TrancheLedger {
    inner: std::sync::Mutex<
        (std::collections::VecDeque<[u8; 32]>, std::collections::HashMap<[u8; 32], f64>),
    >,
    cap: usize,
}
impl TrancheLedger {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new((
                std::collections::VecDeque::new(),
                std::collections::HashMap::new(),
            )),
            cap: cap.max(1),
        }
    }
    pub fn observe(&self, salt: &[u8; 32], fill: f64) -> f64 {
        let mut g = self.inner.lock().unwrap();
        let (order, map) = &mut *g;
        let e = map
            .entry(*salt)
            .or_insert_with(|| {
                order.push_back(*salt);
                0.0
            });
        *e += fill.max(0.0);
        let cum = *e;
        while map.len() > self.cap {
            let Some(old) = order.pop_front() else { break };
            map.remove(&old);
        }
        cum
    }
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().1.len()
    }
}
pub fn sell_tranche_flags(cum: f64, fill: f64, order_size: f64) -> (bool, bool) {
    let first = cum <= fill + 1e-9;
    let done = order_size <= 1e-9 || cum >= order_size - 1e-9;
    (first, done)
}
#[cfg(test)]
fn route_for(
    intent: &Intent,
    cfg: &LaneConfig,
    top: Option<crate::book::Top>,
    his_price: f64,
    first_clip: bool,
    his_first_tranche: bool,
    his_order_done: bool,
) -> Route {
    resolve_route(
        intent,
        cfg,
        top,
        his_price,
        first_clip,
        his_first_tranche,
        his_order_done,
        cfg.copy_makers || cfg.execution == Execution::Hybrid,
        cfg.copy_maker_sells || cfg.execution == Execution::Hybrid,
    )
}
pub fn needs_book(lane: &std::sync::Arc<Lane>) -> bool {
    lane.cfg.execution == Execution::Hybrid
        || lane.state.rest_buys.load(Ordering::Relaxed)
        || lane.state.rest_sells.load(Ordering::Relaxed)
}
pub fn resolve_route(
    intent: &Intent,
    cfg: &LaneConfig,
    top: Option<crate::book::Top>,
    his_price: f64,
    first_clip: bool,
    his_first_tranche: bool,
    his_order_done: bool,
    rest_buys: bool,
    rest_sells: bool,
) -> Route {
    let side_enabled = if intent.side == 0 { rest_buys } else { rest_sells };
    let want_rest = side_enabled
        && (cfg.execution == Execution::Hybrid || intent.he_was_maker);
    if !want_rest {
        return Route::Take;
    }
    let opening_clip = if intent.side == 1 { his_first_tranche } else { first_clip };
    if opening_clip && cfg.execution != Execution::Hybrid {
        return Route::Take;
    }
    if his_order_done && intent.side == 1 {
        return Route::Take;
    }
    let top = match top {
        Some(t) if t.fresh() => t,
        _ => return Route::Take,
    };
    let tick = crate::book::tick_size(his_price);
    let room = crate::book::room_ticks(&top, his_price, intent.side, tick);
    if room < 1 {
        return Route::Take;
    }
    let limit = if intent.side == 0 {
        crate::venue::buy_limit(his_price + tick)
    } else {
        crate::venue::sell_limit(his_price - tick)
    };
    if limit <= 0.0 || limit >= 1.0 {
        return Route::Take;
    }
    Route::RestInFront {
        limit,
        room_ticks: room,
    }
}
#[cfg(test)]
#[path = "lanes_tests/mod.rs"]
mod tests;
#[cfg(test)]
#[path = "lanes_route_tests.rs"]
mod route_tests;
impl Lane {
    pub fn apply_policy(&self, next: SizingPolicy) -> Result<u64, String> {
        let mut slot = self
            .state
            .policy
            .write()
            .map_err(|_| "policy lock poisoned".to_string())?;
        let gen = slot.generation.saturating_add(1);
        let published = SizingPolicy {
            generation: gen,
            ..next
        };
        *slot = std::sync::Arc::new(published);
        Ok(gen)
    }
    pub fn policy(&self) -> std::sync::Arc<SizingPolicy> {
        self.state
            .policy
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|e| e.into_inner().clone())
    }
}
