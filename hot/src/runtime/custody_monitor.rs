//! Periodic read-only custody comparison and alarm telemetry.
//! The live-only task gate stays at the binary composition boundary.

use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use copybot_hot::pending::PendingLog;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
    pending_log: &Arc<std::sync::Mutex<PendingLog>>,
    custody_report: &Arc<std::sync::Mutex<Option<copybot_hot::custody::Report>>>,
) {
        let (ctl_ca, rt_ca, em_ca, fund_ca, pend_ca, last_ca) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
            pending_log.clone(),
            custody_report.clone(),
        );
        tokio::spawn(async move {
            let http = match reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(client) => client,
                Err(_) => {
                    eprintln!("[custody] REFUSED: cannot initialize timed position client");
                    em_ca.emit(serde_json::json!({
                        "t": now_ms(), "ev": "custody_unavailable",
                        "reason": "http_client_initialization",
                    }));
                    return;
                }
            };
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            let mut alarm_since: Option<i64> = None;
            loop {
                tick.tick().await;
                let snap = copybot_hot::positions::fetch(
                        &http,
                        "https://data-api.polymarket.com",
                        &fund_ca,
                        "0.0001",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                let (venue, complete) =
                    match copybot_hot::position_evidence::complete_balances(&snap) {
                        Ok(balances) => (balances, true),
                        Err(error) => {
                            eprintln!("[custody] REFUSED: invalid venue positions: {error}");
                            em_ca.emit(serde_json::json!({
                                "t": now_ms(), "ev": "custody_unavailable",
                                "reason": error.to_string(),
                            }));
                            // The audit below marks every holding Unknown. An invalid read
                            // cannot certify custody agreement or clear an existing alarm.
                            (std::collections::HashMap::new(), false)
                        }
                    };
                let flight = pend_ca.lock().unwrap().in_flight();
                let (in_flight, in_flight_buys) = (
                    flight.sell_shares_by_token,
                    flight.buy_shares_by_token,
                );
                let mut rows: Vec<copybot_hot::custody::Holding> = Vec::new();
                for lane in rt_ca.snapshot().iter() {
                    let name = lane.cfg.name.clone();
                    let held = ctl_ca.lock().unwrap().ledger.holdings(&name);
                    for (tok, ours) in held.iter() {
                        if *ours <= 1e-9 {
                            continue;
                        }
                        let pool = ctl_ca.lock().unwrap().ledger.pool_claim(tok);
                        let total = venue.get(tok).copied().unwrap_or(0.0);
                        let mine = copybot_hot::custody::lane_share_of(
                            total,
                            pool,
                            *ours,
                        );
                        rows.push(copybot_hot::custody::Holding {
                            lane: name.clone(),
                            token: tok.clone(),
                            ledger: *ours,
                            venue: mine,
                            in_flight: in_flight
                                .get(&(name.clone(), tok.clone()))
                                .copied()
                                .unwrap_or(0.0),
                            in_flight_buy: in_flight_buys
                                .get(&(name.clone(), tok.clone()))
                                .copied()
                                .unwrap_or(0.0),
                        });
                    }
                }
                let report = copybot_hot::custody::audit(&rows, complete);
                *last_ca.lock().unwrap() = Some(report.clone());
                if report.is_alarm() {
                    let now = copybot_hot::ledger::now_secs();
                    let first = alarm_since.is_none();
                    if alarm_since.is_none() {
                        alarm_since = Some(now);
                    }
                    if first {
                        eprintln!(
                            "[custody] {}", copybot_hot::custody::describe(& report)
                        );
                        em_ca
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms() as i64, "ev" : "custody_delta", "msg" :
                                    copybot_hot::custody::describe(& report), "unattributed" :
                                    report.unattributed, "unattributed_shares" : (report
                                    .unattributed_shares * 10_000.0).round() / 10_000.0,
                                    "checked" : report.checked, "rows" : report.rows, }
                                ),
                            );
                    }
                } else if complete && alarm_since.take().is_some() {
                    em_ca
                        .emit(
                            serde_json::json!(
                                { "t" : now_ms() as i64, "ev" : "custody_clear", "msg" :
                                copybot_hot::custody::describe(& report) }
                            ),
                        );
                }
            }
        });
}
