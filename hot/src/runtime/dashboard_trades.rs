//! Trade-history title cache and read-only /api/trades endpoint.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub(crate) type TitleCache = Arc<RwLock<HashMap<String, String>>>;

/// Existing 5-minute bounded venue-trade title refresh for dashboard labels.
pub(crate) fn spawn_title_cache_refresh(
    http_mu: &reqwest::Client,
    funder_mu: &str,
    title_cache: &TitleCache,
) {
    let (http_t, funder_t, cache_t) = (
        http_mu.clone(),
        funder_mu.to_owned(),
        title_cache.clone(),
    );
    tokio::spawn(async move {
        const PAGE: usize = 500;
        const MAX_PAGES: usize = 20;
        let mut tick = tokio::time::interval(Duration::from_secs(300));
        loop {
            tick.tick().await;
            let mut scanned = 0usize;
            for page in 0..MAX_PAGES {
                let url = format!(
                    "https://data-api.polymarket.com/trades?user={funder_t}\
                     &limit={PAGE}&offset={}",
                    page * PAGE
                );
                let rows: Vec<serde_json::Value> = match http_t
                    .get(&url)
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        r.json().await.unwrap_or_default()
                    }
                    _ => break,
                };
                let n = rows.len();
                if n > 0 {
                    let mut w = cache_t.write().unwrap();
                    for row in &rows {
                        if let (Some(tok), Some(title)) = (
                            row["asset"].as_str(),
                            row["title"].as_str(),
                        ) {
                            w.entry(tok.to_string())
                                .or_insert_with(|| title.to_string());
                        }
                    }
                }
                scanned += n;
                if n < PAGE {
                    break;
                }
            }
            eprintln!(
                "[titles] sweep: {scanned} trades scanned, {} known", cache_t
                .read().unwrap().len()
            );
        }
    });
}

/// Existing ledger-backed /api/trades read handler; no mutation authority.
pub(crate) async fn serve_trades(
    sock: &mut tokio::net::TcpStream,
    req: &str,
    c: &Arc<Mutex<copybot_hot::control::Control>>,
    title_cache2: &TitleCache,
) {
    let path = req.split_whitespace().nth(1).unwrap_or("");
    let q = path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let want_lane = q
        .split('&')
        .find_map(|p| p.strip_prefix("lane="))
        .filter(|v| !v.is_empty() && *v != "all");
    let limit: usize = q
        .split('&')
        .find_map(|p| p.strip_prefix("limit="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .min(500);
    let ledger_path = c.lock().unwrap().ledger.path.clone();
    let mut rows: Vec<serde_json::Value> = Vec::new();
    match std::fs::metadata(&ledger_path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            let out = serde_json::json!(
                { "status" : "stale", "stale_reason" :
                format!("ledger unreadable: {e}"), "trades" :
                serde_json::Value::Null }
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
        _ => {}
    }
    if let Ok(raw) = std::fs::read_to_string(&ledger_path) {
        for line in raw.lines().rev() {
            if rows.len() >= limit {
                break;
            }
            let Ok(v) = serde_json::from_str::<
                serde_json::Value,
            >(line.trim()) else { continue };
            if v["ev"].as_str() != Some("fill") {
                continue;
            }
            if v["recon"].as_bool() == Some(true) {
                continue;
            }
            let lane = v["lane"].as_str().unwrap_or("");
            if let Some(w) = want_lane {
                if lane != w {
                    continue;
                }
            }
            let drag_c = match (
                v["price"].as_f64(),
                v["his_price"].as_f64(),
            ) {
                (Some(px), Some(his)) if his > 0.0 => {
                    serde_json::json!(((px - his) * 10000.0).round() / 100.0)
                }
                _ => serde_json::Value::Null,
            };
            let title = v["token"]
                .as_str()
                .and_then(|t| title_cache2.read().unwrap().get(t).cloned());
            rows.push(
                serde_json::json!(
                    { "lane" : lane, "token" : v["token"], "title" : title,
                    "side" : v["side"], "shares" : v["shares"], "price" :
                    v["price"], "his_price" : v["his_price"], "drag_c" : drag_c,
                    "fee" : v["fee"], "t" : v["t"], "exec" : v["exec"].as_str()
                    .unwrap_or("taker"), }
                ),
            );
        }
    }
    let out = serde_json::to_string(
            &serde_json::json!({ "trades" : rows }),
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
