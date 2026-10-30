//! Periodic custody reconciliation; incomplete snapshots never authorize correction.

use super::reconcile::{RECON_QUIET_SECS, RECON_RELEASE_QUIET_SECS, RECON_SECS};
use super::telemetry::{now_ms, Emitter};
use copybot_hot::{control::Control, lanes::Router, pending::PendingLog};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) fn spawn(
    control: &Arc<Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &str,
    pending: &Arc<Mutex<PendingLog>>,
    errors_path: &str,
    drift_by_lane: &Arc<Mutex<HashMap<String, f64>>>,
) {
    let (ctl, rt, em, fund, pend) = (
        control.clone(), router.clone(), emitter.clone(), funder.to_owned(), pending.clone(),
    );
    let errpath = errors_path.to_owned();
    let drift_w = drift_by_lane.clone();
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        let mut tick = tokio::time::interval(Duration::from_secs(RECON_SECS));
        loop {
            tick.tick().await;
            let snap = copybot_hot::positions::fetch(
                &http,
                "https://data-api.polymarket.com",
                &fund,
                "0.0001",
                "",
                (now_ms() / 1000) as i64,
            ).await;
            if !snap.may_act_destructively() {
                em.emit(serde_json::json!({
                    "t": now_ms(), "ev": "recon_skipped_incomplete",
                    "why": snap.completeness.reason(), "rows": snap.len(),
                    "note": "a partial portfolio cannot prove a token is gone",
                }));
                continue;
            }
            if snap.is_empty() {
                continue;
            }
            let chain = snap.by_asset();
            let inflight: HashSet<String> = pend.lock().unwrap().open_tokens()
                .into_iter().map(|(_, t)| t).collect();
            let now = (now_ms() / 1000) as i64;
            for lane in rt.snapshot().iter() {
                let name = lane.cfg.name.clone();
                let held = ctl.lock().unwrap().ledger.holdings(&name);
                let mut total_drift = 0.0f64;
                for (tok, sh) in held.iter() {
                    let total = chain.get(tok).copied().unwrap_or(0.0);
                    let pool = ctl.lock().unwrap().ledger.pool_claim(tok);
                    let on_chain = copybot_hot::custody::lane_share_of(total, pool, *sh);
                    let d = sh - on_chain;
                    if d.abs() <= 0.001 {
                        continue;
                    }
                    total_drift += d.abs();
                    em.emit(serde_json::json!({
                        "t": now_ms(), "ev": "recon_gap", "lane": name,
                        "tok": &tok[..tok.len().min(14)], "ledger": sh,
                        "chain": on_chain, "pooled": total, "pool_claim": pool, "delta": d,
                        "note": if d > 0.0 {
                            "ledger over chain — an exit could oversell"
                        } else {
                            "chain over ledger — a position we may not be tracking"
                        },
                    }));
                }
                drift_w.lock().unwrap().insert(name.clone(), total_drift);
                let phantoms: Vec<(String, f64, f64)> = ctl.lock().unwrap().ledger
                    .releasable(&name, &chain, RECON_RELEASE_QUIET_SECS, now)
                    .into_iter().filter(|(t, _, _)| !inflight.contains(t)).collect();
                for (tok, gone, px) in phantoms {
                    ctl.lock().unwrap().ledger.record_recon_fill(&name, &tok, 1, gone, px, 0.0);
                    eprintln!(
                        "[recon] {name}: released phantom …{} ({gone:.4} sh) — \
the chain has not shown it for {RECON_RELEASE_QUIET_SECS}s",
                        &tok[tok.len().saturating_sub(8)..]
                    );
                    em.emit(serde_json::json!({
                        "t": now_ms(), "ev": "recon_released", "lane": name,
                        "tok": &tok[..tok.len().min(14)], "shares": gone, "price": px,
                        "note": "phantom share count corrected; P&L unchanged",
                    }));
                    copybot_hot::errors::record(&errpath, &copybot_hot::errors::ErrorRow {
                        t: now, lane: name.clone(), kind: "recon".into(),
                        detail: format!(
                            "released {gone:.4} sh of …{} at cost {px:.4}",
                            &tok[tok.len().saturating_sub(10)..]
                        ),
                        human: format!(
                            "Corrected our share count on one market by {gone:.4} of a \
share — the wallet no longer holds it. Profit and loss are unchanged, and nothing \
needed selling."
                        ),
                        severity: "notice".into(),
                    });
                }
                let surplus: Vec<(String, f64, f64)> = ctl.lock().unwrap().ledger
                    .adoptable(&name, &chain, RECON_QUIET_SECS, now, 0.05)
                    .into_iter().filter(|(t, _, _)| !inflight.contains(t)).collect();
                for (tok, extra, px) in surplus {
                    ctl.lock().unwrap().ledger.record_recon_fill(&name, &tok, 0, extra, px, 0.0);
                    eprintln!(
                        "[recon] {name}: adopted …{} ({extra:.4} sh) — the chain \
has shown it for {RECON_QUIET_SECS}s and we hold the market",
                        &tok[tok.len().saturating_sub(8)..]
                    );
                    em.emit(serde_json::json!({
                        "t": now_ms(), "ev": "recon_adopted", "lane": name,
                        "tok": &tok[..tok.len().min(14)], "shares": extra, "price": px,
                        "note": "untracked remainder booked at our own cost; P&L unchanged",
                    }));
                    copybot_hot::errors::record(&errpath, &copybot_hot::errors::ErrorRow {
                        t: now, lane: name.clone(), kind: "recon".into(),
                        detail: format!(
                            "adopted {extra:.4} sh of …{} at cost {px:.4}",
                            &tok[tok.len().saturating_sub(10)..]
                        ),
                        human: format!(
                            "Started tracking {extra:.4} of a share we already owned on \
one market — left over from an earlier sale. It can now be sold with the rest; profit and \
loss are unchanged."
                        ),
                        severity: "notice".into(),
                    });
                }
            }
        }
    });
}
