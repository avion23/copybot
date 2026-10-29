//! Rest-cancellation settlement and the existing bounded exit-rescue loop.

use super::accounting::book_and_resolve;
use super::execution::submit_tracked;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::Arc;

pub(crate) fn release_cancelled(
    guard: &Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
    emitter: &Arc<Emitter>,
    order: &copybot_hot::resting::Resting,
    reason: &'static str,
) {
    if order.side != 0 {
        return;
    }
    if order.salt.iter().all(|&b| b == 0) {
        return;
    }
    let Some(g) = guard.as_ref() else { return };
    if let Err(e) = g
        .release(
            &order.salt,
            &order.condition,
            &order.lane,
            order.shares,
            copybot_hot::ledger::now_secs(),
        )
    {
        eprintln!(
            "[{}] cancel-release FAILED ({reason}): {e:?} — {} shares stay \
                   committed against his order",
            order.lane, order.shares
        );
        emitter
            .emit(
                serde_json::json!(
                    { "t" : now_ms(), "ev" : "cancel_release_failed", "lane" : order
                    .lane, "tok" : order.token, "shares" : order.shares, "reason" :
                    reason, "why" : format!("{e:?}"), }
                ),
            );
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn settle_cancelled_rest(
    http: &reqwest::Client,
    clob: &str,
    addr: &str,
    cr: &copybot_hot::auth::ApiCreds,
    rb: &Arc<copybot_hot::resting::RestingBook>,
    guard: &Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
    emitter: &Arc<Emitter>,
    order_id: &str,
    reason: &'static str,
) {
    use copybot_hot::pending::Verdict;
    if !rb.try_claim_settle(order_id) {
        return;
    }
    struct Claim<'a>(&'a Arc<copybot_hot::resting::RestingBook>, String);
    impl Drop for Claim<'_> {
        fn drop(&mut self) {
            self.0.finish_settle(&self.1);
        }
    }
    let _claim = Claim(rb, order_id.to_string());
    let Some(o) = rb
        .all()
        .into_iter()
        .find(|r| {
            let bare = r.order_id.trim_start_matches("0x");
            bare == order_id.trim_start_matches("0x")
        }) else { return };
    let bare = order_id.trim_start_matches("0x");
    let path = format!("/data/order/0x{bare}");
    let body = match copybot_hot::auth::l2_headers(
        addr,
        cr,
        copybot_hot::auth::now_secs(),
        "GET",
        &path,
        None,
    ) {
        Ok(h) => {
            let mut rq = http.get(format!("{clob}{path}"));
            for (k, v) in &h {
                rq = rq.header(*k, v);
            }
            match rq.send().await {
                Ok(r) if r.status().is_success() => r.text().await.ok(),
                _ => None,
            }
        }
        Err(_) => None,
    };
    let verdict = match body {
        Some(b) => copybot_hot::pending::parse_order_status_kind(&b, o.side, true),
        None => {
            let tpath = format!("/data/trades?asset_id={}", o.token);
            let tbody = match copybot_hot::auth::l2_headers(
                addr,
                cr,
                copybot_hot::auth::now_secs(),
                "GET",
                &tpath,
                None,
            ) {
                Ok(h) => {
                    let mut rq = http.get(format!("{clob}{tpath}"));
                    for (k, v) in &h {
                        rq = rq.header(*k, v);
                    }
                    match rq.send().await {
                        Ok(r) if r.status().is_success() => r.text().await.ok(),
                        _ => None,
                    }
                }
                Err(_) => None,
            };
            match tbody {
                Some(t) => copybot_hot::pending::parse_trades(&t, bare, o.side, true),
                None => copybot_hot::pending::Verdict::Unknown,
            }
        }
    };
    match verdict {
        Verdict::Filled { shares: filled, .. } => {
            rb.remove(&o.order_id);
            let unfilled = (o.shares - filled).max(0.0);
            if unfilled > 1e-9 {
                let mut give = o.clone();
                give.shares = unfilled;
                release_cancelled(guard, emitter, &give, reason);
            }
            emitter
                .emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "rest_cancelled", "lane" : o.lane, "tok"
                        : & o.token[..o.token.len().min(14)], "reason" : reason, "filled"
                        : filled, "released" : unfilled }
                    ),
                );
        }
        Verdict::NotFilled => {
            rb.remove(&o.order_id);
            release_cancelled(guard, emitter, &o, reason);
            emitter
                .emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "rest_cancelled", "lane" : o.lane, "tok"
                        : & o.token[..o.token.len().min(14)], "reason" : reason, "filled"
                        : 0.0, "released" : o.shares }
                    ),
                );
        }
        Verdict::Unknown => {}
    }
}

pub(crate) fn rest_verdict(
    levels: &Arc<copybot_hot::book::Levels>,
    prints: &Arc<copybot_hot::book::TradePrints>,
    o: &copybot_hot::resting::Resting,
) -> copybot_hot::restwatch::RestVerdict {
    use copybot_hot::restwatch::{classify, Evidence, FEED_LIVENESS_WINDOW};
    classify(
        &Evidence {
            anchor_remaining: o.his_remaining,
            anchor_printed: o.printed_at_place,
            anchor_contested: o.contested_at_place,
            level_now: levels.size_at(&o.token, o.his_price),
            printed_now: prints.cumulative_at(&o.token, o.his_price),
            feed_alive: levels.feed_alive(FEED_LIVENESS_WINDOW),
        },
    )
}

pub(crate) async fn rescue_exit(
    first_body: &str,
    ord: copybot_hot::order::Order,
    pk: Option<[u8; 32]>,
    owner: &str,
    order_type: &str,
    intended_shares: f64,
    full_holding: f64,
    limit: f64,
    paths: &[(String, reqwest::Client)],
    clob: &str,
    creds: Option<&copybot_hot::auth::ApiCreds>,
    addr: &str,
    lane: &str,
    token: &str,
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    wake: &tokio::sync::mpsc::UnboundedSender<String>,
    pend: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
) -> bool {
    use copybot_hot::exit_rescue as er;
    use copybot_hot::order::{amounts, json_body};
    use copybot_hot::race_send::Outcome;
    let started = std::time::Instant::now();
    let mut fail = er::classify(first_body);
    let mut chain_size = er::reported_balance(first_body);
    let mut attempt = 0usize;
    emitter
        .emit(
            serde_json::json!(
                { "ev" : "exit_rescue_start", "lane" : lane, "token" : token, "why" :
                format!("{fail:?}"), "chain_size" : chain_size, "t" : now_ms() }
            ),
        );
    while let Some(step) = er::next_step(fail, attempt, started.elapsed()) {
        tokio::time::sleep(step.delay).await;
        let shares = er::retry_size(
            step.sell_all,
            chain_size,
            intended_shares,
            full_holding,
        );
        if shares <= 0.0 {
            break;
        }
        let mut o = ord.clone();
        o.salt = copybot_hot::order::safe_salt(er::retry_salt_seed(now_ms(), attempt));
        o.timestamp = now_ms();
        let (ma, ta) = amounts(limit, shares, 1);
        o.maker_amount = ma;
        o.taker_amount = ta;
        let sig = match pk.as_ref().map(|k| o.sign_for_type(k)) {
            Some(s) => s,
            None => break,
        };
        let body = match serde_json::to_string(&json_body(&o, &sig, owner, order_type)) {
            Ok(b) => b,
            Err(_) => break,
        };
        let hdrs = match creds {
            Some(cr) => {
                match copybot_hot::auth::l2_headers(
                    addr,
                    cr,
                    copybot_hot::auth::now_secs(),
                    "POST",
                    "/order",
                    Some(&body),
                ) {
                    Ok(headers) => headers,
                    Err(e) => {
                        emitter
                            .emit(
                                serde_json::json!(
                                    { "ev" : "exit_rescue_FAILED", "lane" : lane, "token" :
                                    token, "why" : format!("L2 header build failed: {e}"), "msg"
                                    : "[CRITICAL] exit rescue could not authenticate", "t" :
                                    now_ms(), }
                                ),
                            );
                        break;
                    }
                }
            }
            None => break,
        };
        let retry_hash = hex::encode(o.digest());
        let Ok((outcome, _)) = submit_tracked(
                pend,
                emitter,
                &copybot_hot::provenance::Provenance::own(
                    copybot_hot::provenance::Origin::ExitRescue {
                        attempt: attempt as u32,
                    },
                    lane,
                    token,
                    1,
                    shares,
                    limit,
                    full_holding,
                ),
                retry_hash.clone(),
                false,
                paths,
                &format!("{clob}/order"),
                &body,
                &hdrs,
                std::time::Duration::from_secs(10),
                false,
            )
            .await else {
            emitter
                .emit(
                    serde_json::json!(
                        { "ev" : "exit_rescue_FAILED", "lane" : lane, "token" : token,
                        "why" : "cannot persist pending order", "t" : now_ms() }
                    ),
                );
            break;
        };
        attempt += 1;
        match &outcome {
            Outcome::Matched { body, .. } => {
                book_and_resolve(
                    control,
                    pend,
                    router,
                    incidents,
                    emitter,
                    lane,
                    token,
                    1,
                    limit,
                    body,
                    None,
                    &retry_hash,
                    false,
                );
                let _ = wake.send(token.to_string());
                emitter
                    .emit(
                        serde_json::json!(
                            { "ev" : "exit_rescue_ok", "lane" : lane, "token" : token,
                            "attempts" : attempt, "shares" : shares, "sell_all" : step
                            .sell_all, "secs" : started.elapsed().as_secs_f64(), "t" :
                            now_ms() }
                        ),
                    );
                return true;
            }
            Outcome::Rejected { body } => {
                fail = er::classify(body);
                if let Some(b) = er::reported_balance(body) {
                    chain_size = Some(b);
                }
            }
            _ => break,
        }
    }
    emitter
        .emit(
            serde_json::json!(
                { "ev" : "exit_rescue_FAILED", "lane" : lane, "token" : token, "attempts"
                : attempt, "why" : format!("{fail:?}"), "secs" : started.elapsed()
                .as_secs_f64(), "msg" :
                "[CRITICAL] exit could not be completed — position still held", "t" :
                now_ms() }
            ),
        );
    eprintln!(
        "[{lane}] [CRITICAL] EXIT RESCUE FAILED on {token} after {attempt} attempts \
({fail:?}) — position is STILL HELD"
    );
    false
}
