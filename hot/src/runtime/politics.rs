//! Guardian-independent read-only political-exclusion evidence refresh.
//! The single shared readiness book is None until a complete validated read.

use super::signal::is_political_title;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::political_evidence::{self, Conditions};
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub(crate) type PoliticsBook = Arc<RwLock<Option<Conditions>>>;

pub(crate) fn spawn(
    politics: &PoliticsBook,
    registry: &copybot_hot::wallets::WalletRegistry,
    emitter: &Arc<Emitter>,
    hc: reqwest::Client,
) {
    let (pol, reg, em) = (politics.clone(), registry.clone(), emitter.clone());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(600));
        loop {
            tick.tick().await;

            // Any in-flight or failed refresh is unknown, never an empty
            // political book. This blocks policy-excluded lanes before I/O.
            if let Ok(mut guard) = pol.write() {
                *guard = None;
            } else {
                em.emit(serde_json::json!({
                    "t": now_ms(), "ev": "politics_unavailable",
                    "why": "political_exclusion_lock_poisoned",
                }));
                continue;
            }
            let leaders: Vec<String> = reg.specs().into_iter()
                .filter(|spec| spec.exclude_political)
                .map(|spec| spec.leader)
                .collect();
            if leaders.is_empty() {
                continue;
            }

            let mut all = Conditions::new();
            let mut fault: Option<String> = None;
            for leader in leaders {
                for page in 0..political_evidence::MAX_PAGES {
                    let url = format!(
                        "https://data-api.polymarket.com/activity?user={leader}&limit={}&offset={}",
                        political_evidence::PAGE, page * political_evidence::PAGE
                    );
                    let rows: Vec<serde_json::Value> = match hc.get(&url).send().await {
                        Ok(response) if response.status().is_success() => {
                            match response.json::<Vec<serde_json::Value>>().await {
                                Ok(rows) => rows,
                                Err(_) => {
                                    fault = Some("activity_json_decode_failed".into());
                                    break;
                                }
                            }
                        }
                        Ok(response) => {
                            fault = Some(format!("activity_http_{}", response.status().as_u16()));
                            break;
                        }
                        Err(_) => {
                            fault = Some("activity_transport_failed".into());
                            break;
                        }
                    };
                    match political_evidence::check_page(&rows, page, is_political_title) {
                        Ok((conditions, complete)) => {
                            all.extend(conditions);
                            if complete {
                                break;
                            }
                        }
                        Err(error) => {
                            fault = Some(error.to_string());
                            break;
                        }
                    }
                }
                if fault.is_some() {
                    break;
                }
            }
            if let Some(why) = fault {
                eprintln!("[politics] REFUSED: {why} — excluded lanes remain gated");
                em.emit(serde_json::json!({
                    "t": now_ms(), "ev": "politics_unavailable", "why": why,
                }));
                continue;
            }

            let n = all.len();
            if let Ok(mut guard) = pol.write() {
                *guard = Some(all);
                eprintln!("[politics] complete exclusion history: {n} conditions");
            } else {
                em.emit(serde_json::json!({
                    "t": now_ms(), "ev": "politics_unavailable",
                    "why": "political_exclusion_lock_poisoned",
                }));
            }
        }
    });
}
