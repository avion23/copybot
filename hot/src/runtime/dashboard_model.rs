//! Shared read-model values and pure dashboard accounting calculations.

use std::sync::Arc;

pub(crate) type DashboardLane = (
    String,
    String,
    Option<f64>,
    f64,
    Arc<copybot_hot::matchup::Matchup>,
);
#[derive(Clone, Copy, Default)]
pub(crate) struct PhysicalWallet {
    pub(crate) cash: Option<f64>,
    pub(crate) equity: Option<f64>,
    pub(crate) fetched_at: Option<i64>,
    pub(crate) stale_reason: Option<&'static str>,
}

pub(crate) fn wallet_equity_pnl(portfolio: Option<f64>, funding_basis: f64) -> Option<f64> {
    portfolio.map(|p| ((p - funding_basis) * 100.0).round() / 100.0)
}

pub(crate) fn marked_position_value(rows: &serde_json::Value) -> Option<f64> {
    let rows = rows.as_array()?;
    let mut total = 0.0;
    for p in rows {
        let v = p["currentValue"]
            .as_f64()
            .or_else(|| p["currentValue"].as_str().and_then(|x| x.parse().ok()))?;
        if !v.is_finite() {
            return None;
        }
        total += v;
    }
    Some(total)
}
