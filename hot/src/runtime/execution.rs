//! Durable pending-order registration around the existing venue submission race.

use super::telemetry::{now_ms, Emitter};
use std::sync::Arc;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
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
    if prov.is_oversell() {
        eprintln!(
            "[{}] ⚠️ {} would sell {:.4} sh of …{} while the ledger holds {:.4}",
            prov.lane, prov.origin.label(), prov.shares, & prov.token[prov.token.len()
            .saturating_sub(8)..], prov.held_before
        );
    }
    if !already_durable {
        pend.lock().unwrap().record(p)?;
    }
    let (outcome, results) = copybot_hot::race_send::race_submit(
            paths,
            url,
            body,
            headers,
            timeout,
        )
        .await;
    match &outcome {
        Outcome::Rejected { .. } => {
            pend.lock().unwrap().resolve(&hash, "rejected", None);
        }
        _ => {}
    }
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
