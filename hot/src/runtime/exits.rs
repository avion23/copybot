//! Rest-cancellation settlement and the existing bounded exit-rescue loop.

use super::accounting::{book_and_resolve, book_matched_fak_race, fak_owner_submit_ready};
use super::control::latch_buys;
use super::execution::submit_tracked;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
pub(crate) fn cancel_rests_on_leader_exit(
    d: &copybot_hot::calldata::Decoded,
    resting_book: &Arc<copybot_hot::resting::RestingBook>,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    http: &reqwest::Client,
    clob: &String,
    auth_addr: &String,
    emitter: &Arc<Emitter>,
    signal_guard: &Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
) {
                if d.side == 1 && !resting_book.is_empty() {
                    let his_taker_exit = d.role != "maker";
                    let live: Vec<_> = resting_book
                        .by_token(&d.token_id)
                        .into_iter()
                        .filter(|o| o.side == 0 || his_taker_exit)
                        .collect();
                    if !live.is_empty() {
                        if let Some(cr) = creds.as_ref().as_ref() {
                            let ids: Vec<String> = live
                                .iter()
                                .map(|o| o.order_id.clone())
                                .collect();
                            let (rb2, h2, cl2, ad2, cr2, e3) = (
                                resting_book.clone(),
                                http.clone(),
                                clob.clone(),
                                auth_addr.clone(),
                                cr.clone(),
                                emitter.clone(),
                            );
                            let tok2 = d.token_id.clone();
                            let sg2 = signal_guard.clone();
                            tokio::spawn(async move {
                                let res = copybot_hot::resting::cancel_batch(
                                        &h2,
                                        &cl2,
                                        &ad2,
                                        &cr2,
                                        &ids,
                                    )
                                    .await;
                                futures_util::future::join_all(
                                        res
                                            .iter()
                                            .filter(|(_, ok)| *ok)
                                            .map(|(id, _)| {
                                                settle_cancelled_rest(
                                                    &h2,
                                                    &cl2,
                                                    &ad2,
                                                    &cr2,
                                                    &rb2,
                                                    &sg2,
                                                    &e3,
                                                    id,
                                                    "he_exited",
                                                )
                                            }),
                                    )
                                    .await;
                                e3.emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "cancel_sweep", "reason" :
                                        copybot_hot::resting::CancelReason::HeExited.as_str(), "tok"
                                        : tok2, "attempted" : ids.len(), "cancelled" : res.iter()
                                        .filter(| (_, ok) | * ok).count(), "still_live" : rb2.len(),
                                        }
                                    ),
                                );
                            });
                        }
                    }
                }
}

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
    fak_journal: Option<&Arc<copybot_hot::order_commit::AtomicFakJournal>>,
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
        if order_type == "FAK" {
            // An exit rescue must not become another unsigned/unsourced
            // money writer. Refuse before POST if the journal is unavailable,
            // revoked or poisoned, leaving the original exposure for review.
            if let Err(reason) = fak_owner_submit_ready(fak_journal) {
                for item in router.snapshot().iter() {
                    latch_buys(item, incidents, "fak_owner_unavailable",
                        "exit rescue FAK journal owner invalid; pending exposure remains");
                }
                emitter.emit(serde_json::json!({
                    "t": now_ms(), "ev":"exit_rescue_FAILED",
                    "lane":lane, "token":token, "why":"fak_owner_unavailable",
                    "detail":format!("{reason:?}"), "attempts":attempt,
                }));
                return false;
            }
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
        let Ok((outcome, results)) = submit_tracked(
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
                let (booked, _) = if order_type == "FAK" {
                    book_matched_fak_race(
                        fak_journal, control, pend, router, incidents,
                        emitter, &retry_hash, lane, &outcome, &results,
                        paths.len(),
                    )
                } else {
                    // Preserve the existing legacy GTC rescue semantics.
                    // The retry reservation is non-resting; GTC exits remain
                    // outside this FAK-only journal migration.
                    book_and_resolve(
                        control, pend, router, incidents, emitter, lane, token,
                        1, limit, body, None, &retry_hash, false,
                    )
                };
                if !booked {
                    emitter.emit(serde_json::json!({
                        "ev": "exit_rescue_FAILED", "lane": lane, "token": token,
                        "attempts": attempt, "why": "matched_but_unbooked",
                        "msg": "[CRITICAL] venue matched but durable ledger booking failed; pending retained",
                        "t": now_ms(),
                    }));
                    return false;
                }
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

pub(crate) fn spawn_cancellation_sweep(
    resting_book: &Arc<copybot_hot::resting::RestingBook>,
    http: &reqwest::Client,
    clob: &String,
    auth_addr: &String,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    signal_guard: &Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
    levels: &Arc<copybot_hot::book::Levels>,
    prints: &Arc<copybot_hot::book::TradePrints>,
) {
        const TTL_SECS: i64 = 1_800;
        const MAX_LIVE_PER_LANE: usize = 6;
        const MAX_LIVE: usize = 36;
        let (rb, http_c, clob_c, addr_c, creds_c, r_c, e_c) = (
            resting_book.clone(),
            http.clone(),
            clob.clone(),
            auth_addr.clone(),
            creds.clone(),
            router.clone(),
            emitter.clone(),
        );
        let sg_sweep = signal_guard.clone();
        let (levels_sweep, prints_sweep) = (levels.clone(), prints.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if rb.is_empty() {
                    continue;
                }
                let Some(cr) = creds_c.as_ref().as_ref() else { continue };
                let now = copybot_hot::ledger::now_secs();
                let armed_anywhere = r_c
                    .snapshot()
                    .iter()
                    .any(|l| l.state.armed.load(Ordering::Relaxed));
                use copybot_hot::resting::CancelReason;
                let mut doomed: Vec<(String, CancelReason)> = Vec::new();
                if !armed_anywhere {
                    for o in rb.all() {
                        doomed.push((o.order_id, CancelReason::Disarmed));
                    }
                } else {
                    let by_name: std::collections::HashMap<String, std::sync::Arc<_>> = r_c
                        .snapshot()
                        .into_iter()
                        .map(|l| (l.cfg.name.clone(), l))
                        .collect();
                    for o in rb.all() {
                        let pull = match by_name.get(&o.lane) {
                            Some(l) => {
                                copybot_hot::resting::halted_lane_must_pull(
                                    o.side,
                                    l.state.halted.load(Ordering::Relaxed),
                                    l.state.armed.load(Ordering::Relaxed),
                                    l.state.retired.load(Ordering::Relaxed),
                                )
                            }
                            None => o.side == 0,
                        };
                        if pull {
                            doomed.push((o.order_id, CancelReason::LaneHalted));
                        }
                    }
                    for o in rb.expired(now, TTL_SECS) {
                        if doomed.iter().any(|(i, _)| i == &o.order_id) {
                            continue;
                        }
                        let v = rest_verdict(&levels_sweep, &prints_sweep, &o);
                        if copybot_hot::restwatch::renewable(v, now - o.placed, o.side) {
                            continue;
                        }
                        doomed.push((o.order_id, CancelReason::Expired));
                    }
                    for o in rb.all() {
                        if doomed.iter().any(|(i, _)| i == &o.order_id) {
                            continue;
                        }
                        if rest_verdict(&levels_sweep, &prints_sweep, &o)
                            == copybot_hot::restwatch::RestVerdict::HeCancelled
                        {
                            doomed.push((o.order_id, CancelReason::HeCancelled));
                        }
                    }
                    for o in rb.all() {
                        let permitted = r_c
                            .snapshot()
                            .iter()
                            .find(|l| l.cfg.name == o.lane)
                            .map(|l| {
                                if o.side == 0 {
                                    l.state.rest_buys.load(Ordering::Relaxed)
                                } else {
                                    l.state.rest_sells.load(Ordering::Relaxed)
                                }
                            })
                            .unwrap_or(false);
                        if !permitted && !doomed.iter().any(|(i, _)| i == &o.order_id) {
                            doomed.push((o.order_id, CancelReason::RestingDisabled));
                        }
                    }
                    for o in rb
                        .over_cap_per_lane(MAX_LIVE_PER_LANE)
                        .into_iter()
                        .chain(rb.over_cap(MAX_LIVE))
                    {
                        if !doomed.iter().any(|(i, _)| i == &o.order_id) {
                            doomed.push((o.order_id, CancelReason::OverCap));
                        }
                    }
                }
                if doomed.is_empty() {
                    continue;
                }
                let mut ids: Vec<String> = doomed
                    .iter()
                    .map(|(i, _)| i.clone())
                    .collect();
                ids.sort();
                ids.dedup();
                let res = copybot_hot::resting::cancel_batch(
                        &http_c,
                        &clob_c,
                        &addr_c,
                        cr,
                        &ids,
                    )
                    .await;
                let settles = res
                    .iter()
                    .filter(|(_, ok)| *ok)
                    .map(|(id, _)| {
                        let why = doomed
                            .iter()
                            .find(|(i, _)| i == id)
                            .map(|(_, r)| r.as_str())
                            .unwrap_or("cancelled");
                        settle_cancelled_rest(
                            &http_c,
                            &clob_c,
                            &addr_c,
                            cr,
                            &rb,
                            &sg_sweep,
                            &e_c,
                            id,
                            why,
                        )
                    });
                futures_util::future::join_all(settles).await;
                let why = doomed.first().map(|(_, r)| r.as_str()).unwrap_or("?");
                e_c.emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "cancel_sweep", "reason" : why,
                        "attempted" : ids.len(), "cancelled" : res.iter().filter(| (_,
                        ok) | * ok).count(), "still_live" : rb.len(), }
                    ),
                );
            }
        });
}

pub(crate) fn spawn_open_order_reconcile(
    resting_book: &Arc<copybot_hot::resting::RestingBook>,
    http: &reqwest::Client,
    clob: &String,
    auth_addr: &String,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    emitter: &Arc<Emitter>,
    signal_guard: &Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
    router: &Arc<Router>,
    control: &Arc<std::sync::Mutex<Control>>,
) {
        let (rb_r, http_r, clob_r, addr_r, creds_r, e_r, sg_r, rt_r, ctl_r) = (
            resting_book.clone(),
            http.clone(),
            clob.clone(),
            auth_addr.clone(),
            creds.clone(),
            emitter.clone(),
            signal_guard.clone(),
            router.clone(),
            control.clone(),
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            let mut last_unattributable = 0usize;
            loop {
                tick.tick().await;
                let Some(cr) = creds_r.as_ref().as_ref() else { continue };
                let path = "/data/orders";
                let Ok(h) = copybot_hot::auth::l2_headers(
                    &addr_r,
                    cr,
                    copybot_hot::auth::now_secs(),
                    "GET",
                    path,
                    None,
                ) else { continue };
                let mut rq = http_r.get(format!("{clob_r}{path}"));
                for (k, v) in &h {
                    rq = rq.header(*k, v);
                }
                let Ok(resp) = rq.send().await else { continue };
                if !resp.status().is_success() {
                    continue;
                }
                let Ok(v) = resp.json::<serde_json::Value>().await else { continue };
                let Some(rows) = v.as_array() else { continue };
                if rows.is_empty() && !rb_r.is_empty() {
                    e_r.emit(
                        serde_json::json!(
                            { "t" : now_ms() as i64, "ev" : "open_orders_reconcile",
                            "skipped" :
                            "empty venue list \
while we hold rests — refusing to prune on an ambiguous read",
                            "ours" : rb_r.len() }
                        ),
                    );
                    continue;
                }
                let venue_ids: std::collections::HashSet<String> = rows
                    .iter()
                    .filter_map(|r| r["id"].as_str().or_else(|| r["orderID"].as_str()))
                    .map(|i| i.trim_start_matches("0x").to_string())
                    .collect();
                let missing: Vec<String> = rb_r
                    .all()
                    .into_iter()
                    .filter(|o| !venue_ids.contains(o.order_id.trim_start_matches("0x")))
                    .map(|o| o.order_id)
                    .collect();
                let mut pruned = 0usize;
                if !missing.is_empty() && !rb_r.prune_is_plausible(missing.len()) {
                    e_r.emit(
                        serde_json::json!(
                            { "t" : now_ms() as i64, "ev" : "open_orders_reconcile",
                            "skipped" :
                            "too many of our rests are absent at once — treating \
this as a bad read, not a mass cancellation",
                            "missing" : missing.len(), "live" : rb_r.len() }
                        ),
                    );
                } else {
                    for id in &missing {
                        settle_cancelled_rest(
                                &http_r,
                                &clob_r,
                                &addr_r,
                                cr,
                                &rb_r,
                                &sg_r,
                                &e_r,
                                id,
                                "venue_gone",
                            )
                            .await;
                        pruned += 1;
                    }
                }
                let mut adopted = 0usize;
                let mut unattributable = 0usize;
                for r in rows {
                    let Some(id) = r["id"].as_str().or_else(|| r["orderID"].as_str())
                    else { continue };
                    if rb_r.contains(id) {
                        continue;
                    }
                    let Some(tok) = r["asset_id"]
                        .as_str()
                        .or_else(|| r["market"].as_str()) else { continue };
                    let snap = rt_r.snapshot();
                    let owner = rt_r
                        .owner_of(tok)
                        .and_then(|ix| snap.get(ix).map(|l| l.cfg.name.clone()))
                        .or_else(|| {
                            let g = ctl_r.lock().unwrap();
                            let by_held: Vec<String> = snap
                                .iter()
                                .filter(|l| {
                                    g
                                        .ledger
                                        .holdings(&l.cfg.name)
                                        .get(tok)
                                        .copied()
                                        .unwrap_or(0.0) > 1e-9
                                })
                                .map(|l| l.cfg.name.clone())
                                .collect();
                            if by_held.len() == 1 {
                                return Some(by_held[0].clone());
                            }
                            let by_his: Vec<String> = snap
                                .iter()
                                .filter(|l| {
                                    g.his_pos
                                        .get(&l.cfg.name)
                                        .map(|h| h.contains_key(tok))
                                        .unwrap_or(false)
                                })
                                .map(|l| l.cfg.name.clone())
                                .collect();
                            if by_his.len() == 1 {
                                Some(by_his[0].clone())
                            } else {
                                None
                            }
                        });
                    match owner {
                        Some(lane) => {
                            adopted += rb_r.rehydrate(&serde_json::json!([r]), &lane);
                        }
                        None => unattributable += 1,
                    }
                }
                if pruned > 0 || adopted > 0 || unattributable != last_unattributable {
                    e_r.emit(
                        serde_json::json!(
                            { "t" : now_ms() as i64, "ev" : "open_orders_reconcile",
                            "pruned" : pruned, "adopted" : adopted, "unattributable" :
                            unattributable, "live" : rb_r.len() }
                        ),
                    );
                }
                last_unattributable = unattributable;
            }
        });
}
