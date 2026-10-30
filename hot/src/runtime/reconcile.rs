//! Existing restart-recovery sizing and lane reactivation boundary.

use copybot_hot::control::Control;
use copybot_hot::position_evidence::{complete_balances, PositionError};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum ReactivationError {
    Position(PositionError),
    EmptyLeaderWithHoldings,
}

impl std::fmt::Display for ReactivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Position(error) => write!(f, "leader book refused: {error}"),
            Self::EmptyLeaderWithHoldings => write!(f, "leader snapshot is empty while our ledger has holdings"),
        }
    }
}
impl std::error::Error for ReactivationError {}

pub(crate) const RECON_SECS: u64 = 5;
pub(crate) const RECON_QUIET_SECS: i64 = 120;
pub(crate) const RECON_RELEASE_QUIET_SECS: i64 = 1_800;

pub(crate) fn recovery_target_shares(
    leader_size: f64,
    leader_avg_price: f64,
    our_avg_price: f64,
    pct: f64,
    scale: f64,
    min_order_usd: f64,
    max_effective_pct: f64,
    compound: bool,
) -> Option<f64> {
    copybot_hot::budget::target_shares(
        leader_size,
        leader_avg_price,
        our_avg_price,
        pct,
        scale,
        min_order_usd,
        max_effective_pct,
        compound,
    )
}

pub(crate) async fn reactivate_lane(
    lane: &std::sync::Arc<copybot_hot::lanes::Lane>,
    control: &Arc<std::sync::Mutex<Control>>,
    http: &reqwest::Client,
) -> Result<usize, ReactivationError> {
    let name = lane.cfg.name.clone();
    let leader = format!("0x{}", hex::encode(lane.cfg.wallet20));
    let snap = copybot_hot::positions::fetch(
            http,
            "https://data-api.polymarket.com",
            &leader,
            "0.0001",
            "",
            copybot_hot::ledger::now_secs(),
        )
        .await;
    let mut book = complete_balances(&snap).map_err(ReactivationError::Position)?;
    book.retain(|_, size| *size > 0.0);
    let held = control.lock().unwrap().ledger.holdings(&name);
    if book.is_empty() && !held.is_empty() {
        return Err(ReactivationError::EmptyLeaderWithHoldings);
    }
    control.lock().unwrap().seed_his_book(&name, &book);
    Ok(book.len())
}
