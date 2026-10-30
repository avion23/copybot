//! Durable pending-order registration around the existing venue submission race.

use super::telemetry::{now_ms, Emitter};
use std::sync::Arc;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn submit_tracked(
    pend: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    em: &Emitter,
    prov: &copybot_hot::provenance::Provenance,
    order_hash: String,
    resting: bool,
    paths: &[(String, reqwest::Client)],
    url: &str,
    body: &str,
    headers: &[(&'static str, String)],
    timeout: Duration,
    already_durable: bool,
) -> Result<
    (copybot_hot::race_send::Outcome, Vec<copybot_hot::race_send::PathResult>),
    String,
> {
    use copybot_hot::race_send::Outcome;
    let hash = order_hash.clone();
    let p = copybot_hot::pending::Pending {
        lane: prov.lane.clone(),
        token: prov.token.clone(),
        side: prov.side,
        order_hash,
        shares: prov.shares,
        limit: prov.limit,
        ts: copybot_hot::ledger::now_secs(),
        why: prov.why(),
        resting,
    };
    if prov.is_oversell() {
        eprintln!(
            "[{}] ⚠️ {} would sell {:.4} sh of …{} while the ledger holds {:.4}",
            prov.lane, prov.origin.label(), prov.shares, & prov.token[prov.token.len()
            .saturating_sub(8)..], prov.held_before
        );
    }
    {
        let mut owner = pend.lock().map_err(|_| "pending owner mutex poisoned")?;
        owner.reserve_for_submit(p, already_durable)?;
    }
    // Record order_submit only after token admission was durably confirmed.
    {
        let mut row = prov.to_json();
        if let Some(m) = row.as_object_mut() {
            m.insert("t".into(), (now_ms() as i64).into());
            m.insert("ev".into(), "order_submit".into());
            m.insert("order_hash".into(), hash.clone().into());
            m.insert("resting".into(), resting.into());
        }
        em.emit(row);
    }
    let (outcome, results) = copybot_hot::race_send::race_submit(
            paths,
            url,
            body,
            headers,
            timeout,
        )
        .await;
    use copybot_hot::pending::OrderStage;
    let stage = match &outcome {
        Outcome::Rejected { .. } => None,
        Outcome::Matched { .. } => Some(OrderStage::Accepted),
        Outcome::Resting { .. } => Some(OrderStage::Resting),
        Outcome::DuplicateOnly { .. } | Outcome::Ambiguous { .. }
        | Outcome::NoResponse => Some(OrderStage::Unknown),
    };
    let mut owner = pend.lock().unwrap();
    if let Some(stage) = stage {
        if let Err(why) = owner.advance(&hash, stage) {
            // The HTTP request already ran: never call this a refused
            // submission. Preserve the reservation and expose the fault.
            em.emit(serde_json::json!({
                "t": now_ms(), "ev": "order_stage_unavailable",
                "order_hash": hash, "reason": why,
            }));
        }
    } else if let Err(why) = owner.resolve(&hash, "rejected", None) {
        // A rejection is not a durable release until the journal confirms it.
        em.emit(serde_json::json!({
            "t": now_ms(), "ev": "order_release_unavailable",
            "order_hash": hash, "reason": why,
        }));
    }
    drop(owner);
    em.emit(
        serde_json::json!(
            { "t" : now_ms(), "ev" : "order_result", "origin" : prov.origin.kind(),
            "lane" : prov.lane, "tok" : prov.token, "side" : if prov.side == 1 { "SELL" }
            else { "BUY" }, "order_hash" : hash, "outcome" : format!("{outcome:?}"),
            "paths" : results.len(), }
        ),
    );
    Ok((outcome, results))
}
