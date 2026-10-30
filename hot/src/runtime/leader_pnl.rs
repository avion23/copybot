//! Leader profit-window readings for dashboard; no operator mutation authority.
use copybot_hot::lanes::Router;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    router: &Arc<Router>,
    registry: &copybot_hot::wallets::WalletRegistry,
    leader_pnl: &Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>,
) {
        let (rt_lp, reg_lp, lp) = (router.clone(), registry.clone(), leader_pnl.clone());
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_default();
            let mut tick = tokio::time::interval(Duration::from_secs(900));
            loop {
                tick.tick().await;
                for lane in rt_lp.snapshot().iter() {
                    let name = lane.cfg.name.clone();
                    let Some(spec) = reg_lp.specs().into_iter().find(|w| w.name == name)
                    else { continue };
                    let mut out = serde_json::Map::new();
                    for (win, key) in [("1d", "d1"), ("7d", "d7"), ("30d", "d30")] {
                        let url = format!(
                            "https://lb-api.polymarket.com/profit?window={win}&limit=1&address={}",
                            spec.leader
                        );
                        if let Ok(r) = http.get(&url).send().await {
                            if let Ok(v) = r.json::<serde_json::Value>().await {
                                if let Some(a) = v.as_array().and_then(|a| a.first()) {
                                    if let Some(x) = a["amount"].as_f64() {
                                        out.insert(
                                            key.into(),
                                            serde_json::json!((x * 100.0).round() / 100.0),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    if !out.is_empty() {
                        lp.lock().unwrap().insert(name, serde_json::Value::Object(out));
                    }
                }
            }
        });
}
