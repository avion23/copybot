//! Existing restart-recovery sizing and lane reactivation boundary.

use copybot_hot::control::Control;
use std::sync::Arc;

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
) -> Result<usize, String> {
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
    if !snap.may_act_destructively() {
        return Err(
            format!(
                "leader book is not a complete list ({}) — refusing to seed \
from a partial book, because an omitted token reads as fully exited",
                snap.completeness.reason()
            ),
        );
    }
    let rows: Vec<serde_json::Value> = snap.rows;
    let mut book: std::collections::HashMap<String, f64> = Default::default();
    for p in &rows {
        let t = p["asset"].as_str().unwrap_or("").to_string();
        let sz = p["size"]
            .as_f64()
            .or_else(|| p["size"].as_str().and_then(|x| x.parse().ok()))
            .unwrap_or(0.0);
        if !t.is_empty() && sz > 0.0 {
            book.insert(t, sz);
        }
    }
    let held = control.lock().unwrap().ledger.holdings(&name);
    if book.is_empty() && !held.is_empty() {
        return Err("leader snapshot is empty while our ledger has holdings".into());
    }
    control.lock().unwrap().seed_his_book(&name, &book);
    Ok(book.len())
}
