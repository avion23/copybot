//! Background exclusion list from leader activity, using existing signal classifier.
//! The entrypoint retains the single politics set and the live-mode gate.

use super::signal::is_political_title;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    politics: &Arc<std::sync::RwLock<std::collections::HashSet<[u8; 32]>>>,
    registry: &copybot_hot::wallets::WalletRegistry,
    hc: reqwest::Client,
) {
        let (pol, reg) = (politics.clone(), registry.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(600));
            loop {
                tick.tick().await;
                let leaders: Vec<String> = reg
                    .specs()
                    .into_iter()
                    .filter(|w| w.exclude_political)
                    .map(|w| w.leader)
                    .collect();
                if leaders.is_empty() {
                    continue;
                }
                let mut set = std::collections::HashSet::new();
                for leader in leaders {
                    let url = format!(
                        "https://data-api.polymarket.com/activity?\
user={leader}&limit=500"
                    );
                    let rows: Vec<serde_json::Value> = match hc.get(&url).send().await {
                        Ok(r) if r.status().is_success() => {
                            r.json().await.unwrap_or_default()
                        }
                        _ => continue,
                    };
                    for r in &rows {
                        let ty = r["type"].as_str().unwrap_or("");
                        let political = ty == "SPLIT" || ty == "MERGE"
                            || is_political_title(r["title"].as_str().unwrap_or(""));
                        if political {
                            if let Some(cid) = r["conditionId"]
                                .as_str()
                                .and_then(copybot_hot::merge::condition_id_bytes)
                            {
                                set.insert(cid);
                            }
                        }
                    }
                }
                let n = set.len();
                if let Ok(mut g) = pol.write() {
                    *g = set;
                }
                eprintln!("[politics] exclusion set refreshed: {n} conditions");
            }
        });
}
