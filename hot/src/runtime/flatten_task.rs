//! Operator-requested flatten execution. The caller owns the live-mode gate.

use super::accounting::book_and_resolve;
use super::execution::submit_tracked;
use super::exits::rescue_exit;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use copybot_hot::order::order_signer;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    control_path: &str,
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
    pk: Option<[u8; 32]>,
    signer_addr: &String,
    sig_type: u8,
    clob: &String,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    order_paths: &Arc<Vec<(String, reqwest::Client)>>,
    sweep_tx: &Arc<tokio::sync::mpsc::UnboundedSender<String>>,
    registry: &copybot_hot::wallets::WalletRegistry,
    errors_path: &String,
) {
        let flatten_path = format!("{control_path}.flatten");
        let pend4 = pending_log.clone();
        let inc4 = incidents.clone();
        let (c4, r4, e4, f4, pk4, sa4, st4, clob4, creds4, paths4, wake4, reg4) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
            pk,
            signer_addr.clone(),
            sig_type,
            clob.clone(),
            creds.clone(),
            order_paths.clone(),
            sweep_tx.clone(),
            registry.clone(),
        );
        let err4 = errors_path.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();
            let mut done: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let mut boxes: Vec<String> = r4
                    .snapshot()
                    .iter()
                    .map(|l| format!("{flatten_path}.{}", l.cfg.name))
                    .collect();
                boxes.push(flatten_path.clone());
                let found = boxes
                    .iter()
                    .find_map(|m| {
                        let raw = std::fs::read_to_string(m).ok()?;
                        let intent: copybot_hot::flatten::Intent = serde_json::from_str(
                                &raw,
                            )
                            .ok()?;
                        if done.get(&intent.lane).copied().unwrap_or(0) >= intent.ts {
                            return None;
                        }
                        Some(intent)
                    });
                let Some(intent) = found else { continue };
                let Some(lane) = r4
                    .snapshot()
                    .iter()
                    .find(|l| l.cfg.name == intent.lane)
                    .cloned() else {
                    let why = "unknown lane";
                    e4.emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "flatten_gate_refused", "reason" :
                            why, "lane" : intent.lane }
                        ),
                    );
                    copybot_hot::errors::record(
                        &err4,
                        &copybot_hot::errors::ErrorRow {
                            t: copybot_hot::ledger::now_secs(),
                            lane: intent.lane.clone(),
                            kind: "flatten".into(),
                            detail: format!("flatten intent refused: {why}"),
                            human: format!(
                                "A flatten was requested for {:?}, which is not a wallet this \
bot runs. NOTHING HAS BEEN SOLD. Check the wallet name and try again.",
                                intent.lane
                            ),
                            severity: "warn".into(),
                        },
                    );
                    done.insert(intent.lane.clone(), intent.ts);
                    continue;
                };
                let armed = lane.state.armed.load(Ordering::Relaxed);
                let mode = match copybot_hot::flatten::gate(
                    &intent,
                    &lane.cfg.name,
                    armed,
                    copybot_hot::ledger::now_secs(),
                ) {
                    Ok(m) => m,
                    Err(reason) => {
                        e4.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "flatten_gate_refused", "lane" :
                                intent.lane, "mode" : intent.mode, "reason" : reason }
                            ),
                        );
                        copybot_hot::errors::record(
                            &err4,
                            &copybot_hot::errors::ErrorRow {
                                t: copybot_hot::ledger::now_secs(),
                                lane: intent.lane.clone(),
                                kind: "flatten".into(),
                                detail: format!("flatten intent refused: {reason}"),
                                human: format!(
                                    "A {} flatten of {} was refused: {reason}. NOTHING HAS BEEN \
SOLD — the wallet still holds everything it held before.",
                                    intent.mode, intent.lane
                                ),
                                severity: "warn".into(),
                            },
                        );
                        done.insert(intent.lane.clone(), intent.ts);
                        continue;
                    }
                };
                done.insert(intent.lane.clone(), intent.ts);
                e4.emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "flatten_start", "lane" : lane.cfg.name,
                        "mode" : mode.as_str() }
                    ),
                );
                lane.state.retired.store(true, Ordering::Relaxed);
                let modestr = mode.as_str().to_string();
                let laname = lane.cfg.name.clone();
                let _ = reg4
                    .mutate(|specs| {
                        if let Some(w) = specs.iter_mut().find(|w| w.name == laname) {
                            w.stop_mode = modestr.clone();
                        }
                    });
                if mode == copybot_hot::flatten::Mode::Graceful {
                    e4.emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "flatten_done", "lane" : lane.cfg
                            .name, "mode" : mode.as_str(), "positions_sold" : 0 }
                        ),
                    );
                    continue;
                }
                let (Some(cr), Some(key)) = (creds4.as_ref(), pk4.as_ref()) else {
                    continue
                };
                let url = format!(
                    "https://data-api.polymarket.com/positions?user={f4}\
&sizeThreshold=0.0001&limit=500"
                );
                let ours: Vec<serde_json::Value> = match http.get(&url).send().await {
                    Ok(r) if r.status().is_success() => {
                        r.json().await.unwrap_or_default()
                    }
                    _ => Vec::new(),
                };
                let mark_of = |tok: &str| -> f64 {
                    ours.iter()
                        .find(|p| p["asset"].as_str() == Some(tok))
                        .and_then(|p| {
                            p["curPrice"]
                                .as_f64()
                                .or_else(|| {
                                    p["curPrice"].as_str().and_then(|x| x.parse().ok())
                                })
                        })
                        .unwrap_or(0.0)
                };
                let held = c4.lock().unwrap().ledger.holdings(&lane.cfg.name);
                let mut sold = 0usize;
                for (tok, sz) in held.iter() {
                    if *sz <= 1e-9 {
                        continue;
                    }
                    let mark = mark_of(tok);
                    let avg = c4.lock().unwrap().ledger.avg_cost(&lane.cfg.name, tok);
                    if !copybot_hot::flatten::should_sell(mode, mark, avg) {
                        continue;
                    }
                    let px = if mark > 0.0 { mark } else { 0.02 };
                    let (shares, limit) = if mode == copybot_hot::flatten::Mode::Panic {
                        copybot_hot::venue::dust_fak_terms(*sz, px)
                    } else {
                        copybot_hot::venue::cleanup_fak_terms(*sz, px)
                    };
                    if shares <= 0.0 {
                        continue;
                    }
                    let neg = ours
                        .iter()
                        .find(|p| p["asset"].as_str() == Some(tok.as_str()))
                        .and_then(|p| p["negativeRisk"].as_bool())
                        .unwrap_or(false);
                    let (ma, ta) = copybot_hot::order::amounts(limit, shares, 1);
                    let ord = copybot_hot::order::Order {
                        salt: copybot_hot::order::safe_salt(now_ms()),
                        maker: f4.clone(),
                        signer: order_signer(st4, &f4, &sa4).to_owned(),
                        token_id: tok.clone(),
                        maker_amount: ma,
                        taker_amount: ta,
                        side: 1,
                        signature_type: st4,
                        timestamp: now_ms(),
                        neg_risk: neg,
                    };
                    let sig = ord.sign_for_type(key);
                    let body = copybot_hot::order::json_body(&ord, &sig, &cr.key, "FAK");
                    let Ok(body_s) = serde_json::to_string(&body) else { continue };
                    let hdrs = match copybot_hot::auth::l2_headers(
                        &sa4,
                        cr,
                        copybot_hot::auth::now_secs(),
                        "POST",
                        "/order",
                        Some(&body_s),
                    ) {
                        Ok(h) => h,
                        Err(_) => continue,
                    };
                    let flat_hash = hex::encode(ord.digest());
                    let (outcome, _) = match submit_tracked(
                            &pend4,
                            &e4,
                            &copybot_hot::provenance::Provenance::own(
                                copybot_hot::provenance::Origin::Flatten {
                                    mode: mode.as_str().to_string(),
                                },
                                &lane.cfg.name,
                                &tok,
                                1,
                                shares,
                                limit,
                                *sz,
                            ),
                            flat_hash.clone(),
                            false,
                            &paths4,
                            &format!("{clob4}/order"),
                            &body_s,
                            &hdrs,
                            Duration::from_secs(10),
                            false,
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!(
                                "[{}] flatten REFUSING TO SUBMIT: {e}", lane.cfg.name
                            );
                            continue;
                        }
                    };
                    match &outcome {
                        copybot_hot::race_send::Outcome::Matched { body, .. } => {
                            book_and_resolve(
                                &c4,
                                &pend4,
                                &r4,
                                &inc4,
                                &e4,
                                &lane.cfg.name,
                                tok,
                                1,
                                limit,
                                body,
                                None,
                                &flat_hash,
                                false,
                            );
                            let _ = wake4.send(tok.clone());
                            sold += 1;
                        }
                        copybot_hot::race_send::Outcome::Rejected { body } => {
                            rescue_exit(
                                    body,
                                    ord,
                                    pk4,
                                    &cr.key,
                                    "FAK",
                                    shares,
                                    *sz,
                                    limit,
                                    &paths4,
                                    &clob4,
                                    creds4.as_ref().as_ref(),
                                    &sa4,
                                    &lane.cfg.name,
                                    tok,
                                    &c4,
                                    &r4,
                                    &e4,
                                    &wake4,
                                    &pend4,
                                    &inc4,
                                )
                                .await;
                            sold += 1;
                        }
                        _ => {}
                    }
                }
                e4.emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "flatten_done", "lane" : lane.cfg.name,
                        "mode" : mode.as_str(), "positions_sold" : sold }
                    ),
                );
            }
        });
}
