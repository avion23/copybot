//! Read-only /api/positions projection of the single ledger and venue snapshot.
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use super::dashboard_trades::TitleCache;

pub(crate) type PositionCache =
    Arc<tokio::sync::Mutex<Option<copybot_hot::positions::Positions>>>;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_positions(
    sock: &mut tokio::net::TcpStream,
    http2: &reqwest::Client,
    funder2: &str,
    pos_cache2: &PositionCache,
    title_cache2: &TitleCache,
    r: &Arc<copybot_hot::lanes::Router>,
    c: &Arc<Mutex<copybot_hot::control::Control>>,
) {
    const POS_FRESH_SECS: i64 = 15;
    const POS_FALLBACK_SECS: i64 = 300;
    let now_s = copybot_hot::ledger::now_secs();
    let cached: Option<copybot_hot::positions::Positions> = pos_cache2
        .lock()
        .await
        .clone();
    let snapshot = if let Some(c) = cached
        .as_ref()
        .filter(|c| now_s - c.as_of <= POS_FRESH_SECS)
    {
        c.clone()
    } else {
        let fetched = tokio::time::timeout(
                Duration::from_secs(15),
                copybot_hot::positions::fetch(
                    &http2,
                    "https://data-api.polymarket.com",
                    &funder2,
                    "0.0001",
                    "",
                    now_s,
                ),
            )
            .await;
        let fetched = match fetched {
            Ok(s) => s,
            Err(_) => {
                copybot_hot::positions::Positions {
                    rows: Vec::new(),
                    completeness: copybot_hot::positions::Completeness::Failed {
                        after_pages: 0,
                        why: "positions request timed out".into(),
                    },
                    as_of: now_s,
                }
            }
        };
        match &fetched.completeness {
            copybot_hot::positions::Completeness::Complete => {
                *pos_cache2.lock().await = Some(fetched.clone());
                fetched
            }
            copybot_hot::positions::Completeness::Failed { .. }
            | copybot_hot::positions::Completeness::RateLimited { .. } => {
                match cached
                    .filter(|c| now_s - c.as_of <= POS_FALLBACK_SECS)
                {
                    Some(c) => c,
                    None => fetched,
                }
            }
            _ => fetched,
        }
    };
    let stale_reason: Option<String> = match &snapshot
        .completeness
    {
        copybot_hot::positions::Completeness::Failed { .. }
        | copybot_hot::positions::Completeness::RateLimited { .. } => {
            Some(snapshot.completeness.reason())
        }
        _ => None,
    };
    let ours: Vec<serde_json::Value> = snapshot.rows.clone();
    {
        let mut w = title_cache2.write().unwrap();
        for row in &ours {
            if let (Some(tok), Some(title)) = (
                row["asset"].as_str(),
                row["title"].as_str(),
            ) {
                w.entry(tok.to_string())
                    .or_insert_with(|| title.to_string());
            }
        }
    }
    if let Some(why) = &stale_reason {
        let out = serde_json::json!(
            { "status" : "stale", "stale_reason" : why, "as_of" :
            copybot_hot::ledger::now_secs(), "positions" :
            serde_json::Value::Null, }
        )
            .to_string();
        let resp = format!(
            "HTTP/1.1 503 Service Unavailable\r\n\
Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            out.len(), out
        );
        let _ = sock.write_all(resp.as_bytes()).await;
        return;
    }
    let pos_of = |tok: &str| -> Option<&serde_json::Value> {
        ours.iter().find(|p| p["asset"].as_str() == Some(tok))
    };
    let snap = r.snapshot();
    let mut out_rows = Vec::new();
    {
        let g = c.lock().unwrap();
        for lane in snap.iter() {
            let leader = format!(
                "0x{}", hex::encode(lane.cfg.wallet20)
            );
            for (tok, shares, avg, opened_t, his, his_known) in g
                .ledger
                .open_positions(&lane.cfg.name)
            {
                let p = pos_of(&tok);
                let mark = p.and_then(|p| p["curPrice"].as_f64());
                let title = p.and_then(|p| p["title"].as_str());
                let outcome = p.and_then(|p| p["outcome"].as_str());
                let end_date = p.and_then(|p| p["endDate"].as_str());
                let redeemable = p
                    .and_then(|p| p["redeemable"].as_bool())
                    .unwrap_or(false);
                let unreal = mark
                    .map(|m| ((m - avg) * shares * 100.0).round() / 100.0);
                out_rows
                    .push(
                        serde_json::json!(
                            { "lane" : lane.cfg.name, "leader" : leader, "token" : tok,
                            "title" : title, "outcome" : outcome, "shares" : (shares *
                            100.0).round() / 100.0, "our_price" : (avg * 10000.0)
                            .round() / 10000.0, "leader_price" : his, "his_known_shares"
                            : (his_known * 100.0).round() / 100.0, "drag_known" :
                            his_known > 1e-9 && (shares - his_known) <= 1e-4 * shares
                            .max(1.0), "mark" : mark, "entry_ts" : opened_t,
                            "unrealised" : unreal, "end_date" : end_date, "redeemable" :
                            redeemable, }
                        ),
                    );
            }
        }
    }
    let complete = snapshot.completeness.is_complete();
    let out = serde_json::to_string(
            &serde_json::json!(
                { "status" : if complete { "ok" } else { "partial" },
                "complete" : complete, "completeness" : snapshot
                .completeness.reason(), "as_of" :
                copybot_hot::ledger::now_secs(), "positions" : out_rows }
            ),
        )
        .unwrap_or_default();
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
        out.len(), out
    );
    let _ = sock.write_all(resp.as_bytes()).await;
    return;
}
