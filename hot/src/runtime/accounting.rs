//! Runtime fill recovery, ledger booking and persistence-fault dispatch.

use super::control::latch_buys;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::Arc;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_pending(
    log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    clob: &str,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    signer_addr: &str,
    _sig_type: u8,
    resting: Option<&Arc<copybot_hot::resting::RestingBook>>,
) -> Result<(), reqwest::Error> {
    use copybot_hot::pending::Verdict;
    let Some(cr) = creds.as_ref().as_ref() else { return Ok(()) };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let now = copybot_hot::ledger::now_secs();
    let due = log.lock().unwrap().due(now);
    if due.is_empty() {
        return Ok(());
    }
    let get = |path: String| {
        let (http, clob, cr, addr) = (
            http.clone(),
            clob.to_string(),
            cr.clone(),
            signer_addr.to_string(),
        );
        async move {
            let hdrs = copybot_hot::auth::l2_headers(
                    &addr,
                    &cr,
                    copybot_hot::auth::now_secs(),
                    "GET",
                    &path,
                    None,
                )
                .ok()?;
            let mut rq = http.get(format!("{clob}{path}"));
            for (k, v) in hdrs {
                rq = rq.header(k, v);
            }
            let resp = rq.send().await.ok()?;
            if !resp.status().is_success() {
                return None;
            }
            resp.text().await.ok()
        }
    };
    for p in due {
        let hash = p.order_hash.trim_start_matches("0x").to_string();
        let mut verdict = match get(format!("/data/order/0x{hash}")).await {
            Some(body) => {
                copybot_hot::pending::parse_order_status_kind(&body, p.side, p.resting)
            }
            None => Verdict::Unknown,
        };
        if copybot_hot::pending::knows_shares_but_not_price(&verdict) {
            if let Some(body) = get(format!("/data/trades?asset_id={}", p.token)).await {
                verdict = copybot_hot::pending::merge_price_from_trades(
                    verdict,
                    copybot_hot::pending::parse_trades(&body, &hash, p.side, p.resting),
                );
            }
        }
        if verdict == Verdict::Unknown {
            if let Some(body) = get(format!("/data/trades?asset_id={}", p.token)).await {
                verdict = copybot_hot::pending::parse_trades(
                    &body,
                    &hash,
                    p.side,
                    p.resting,
                );
            }
        }
        match verdict {
            Verdict::Filled { shares, price } => {
                let (px, px_provisional) = copybot_hot::pending::recovered_price(
                    p.resting,
                    price,
                    p.limit,
                );
                let mut sz = shares;
                if p.side == 1 {
                    let held = control
                        .lock()
                        .unwrap()
                        .ledger
                        .holdings(&p.lane)
                        .get(&p.token)
                        .copied()
                        .unwrap_or(0.0);
                    sz = sz.min(held);
                }
                let receipt = if sz > 0.0 {
                    let r = control
                        .lock()
                        .unwrap()
                        .ledger
                        .book_fill_ex_px(
                            &p.lane,
                            &p.token,
                            p.side,
                            sz,
                            px,
                            0.0,
                            None,
                            &p.order_hash,
                            p.resting,
                            px_provisional,
                        );
                    if r.is_some() && p.side == 0 {
                        if let Some(ix) = router
                            .snapshot()
                            .iter()
                            .position(|l| l.cfg.name == p.lane)
                        {
                            router.claim_existing(&p.token, ix);
                        }
                    }
                    r
                } else {
                    log.lock()
                        .unwrap()
                        .resolve(&p.order_hash, "filled", Some((0.0, px)));
                    continue;
                };
                let Some(r) = receipt else {
                    eprintln!(
                        "[pending] {} could NOT book a recovered fill — row kept open",
                        p.lane
                    );
                    emitter
                        .emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "pending_recover_failed", "lane" :
                                p.lane, "tok" : & p.token[..p.token.len().min(14)], "shares"
                                : sz, "price" : px }
                            ),
                        );
                    continue;
                };
                if let Err(e) = log
                    .lock()
                    .unwrap()
                    .resolve_booked(&p.order_hash, "filled", r)
                {
                    eprintln!(
                        "[pending] {} fill IS booked but its row stayed open: {e}", p
                        .lane
                    );
                }
                eprintln!(
                    "[pending] {} RECOVERED a lost fill: {sz:.4} sh @ {px:.4} on …{}",
                    p.lane, & p.token[p.token.len().saturating_sub(8)..]
                );
                emitter
                    .emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "pending_recovered", "lane" : p
                            .lane, "tok" : & p.token[..p.token.len().min(14)], "side" : p
                            .side, "shares" : sz, "price" : px, "why" : p.why }
                        ),
                    );
            }
            Verdict::NotFilled => {
                log.lock().unwrap().resolve(&p.order_hash, "not_filled", None);
                emitter
                    .emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "pending_not_filled", "lane" : p
                            .lane, "tok" : & p.token[..p.token.len().min(14)] }
                        ),
                    );
            }
            Verdict::Unknown => {}
        }
    }
    let live_rests: std::collections::HashSet<String> = resting
        .map(|rb| {
            rb
                .all()
                .into_iter()
                .map(|r| r.order_id.trim_start_matches("0x").to_string())
                .collect()
        })
        .unwrap_or_default();
    for p in log.lock().unwrap().stale_excluding(now, &live_rests) {
        eprintln!(
            "[pending] ⚠️  UNRESOLVED for {}s: {} {} …{} ({:.4} sh) — verify by hand",
            now - p.ts, p.lane, if p.side == 0 { "BUY" } else { "SELL" }, & p.token[p
            .token.len().saturating_sub(8)..], p.shares
        );
        emitter
            .emit(
                serde_json::json!(
                    { "t" : now_ms(), "ev" : "pending_stale", "lane" : p.lane, "tok" : &
                    p.token[..p.token.len().min(14)], "age_secs" : now - p.ts, "shares" :
                    p.shares, "why" : p.why }
                ),
            );
    }
    Ok(())
}

pub(crate) fn halt_buys_on_persistence_fault_with(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    pend: Option<&Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
) {
    let ledger_fault = control
        .lock()
        .map(|c| !c.ledger.persistence_ok())
        .unwrap_or(true);
    let pend_fault = pend
        .map(|p| p.lock().map(|l| !l.persistence_ok()).unwrap_or(true))
        .unwrap_or(false);
    let fault = ledger_fault || pend_fault;
    if fault {
        for lane in router.snapshot().iter() {
            latch_buys(
                lane,
                incidents,
                "persistence",
                "ledger or pending log cannot persist",
            );
        }
        eprintln!(
            "[CRITICAL] durable-memory fault (ledger_ok={}, pending_ok={}) — all \
BUY lanes halted; exits remain enabled",
            ! ledger_fault, ! pend_fault
        );
    }
}

pub(crate) fn book_and_resolve(
    control: &Arc<std::sync::Mutex<Control>>,
    pend: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    emitter: &Arc<Emitter>,
    lane: &str,
    token: &str,
    side: u8,
    limit: f64,
    body: &str,
    his_price: Option<f64>,
    order_hash: &str,
    resting: bool,
) -> (bool, f64) {
    let receipt = control
        .lock()
        .unwrap()
        .book_response(lane, token, side, limit, body, his_price, order_hash, resting);
    let mut resolve_err: Option<String> = None;
    let had_receipt = receipt.is_some();
    let still_open_after: usize;
    let filled_shares = receipt.as_ref().map(|r| r.shares).unwrap_or(0.0);
    let booked = match receipt {
        Some(r) => {
            if let Err(e) = pend.lock().unwrap().resolve_booked(order_hash, "matched", r)
            {
                resolve_err = Some(e.to_string());
                eprintln!(
                    "[{lane}] fill IS booked but its pending row stayed open: {e}"
                );
            }
            true
        }
        None => {
            eprintln!(
                "[{lane}] NOT BOOKED: {} {} @ {limit} — refusal inside a 200, an \
                       AMBIGUOUS reply, or a failed ledger append. The pending row stays \
                       OPEN for the resolver; if it is still unbooked later, this is \
                       unattributed inventory.",
                if side == 0 { "BUY" } else { "SELL" }, & token[..token.len().min(14)]
            );
            false
        }
    };
    still_open_after = pend.lock().unwrap().len();
    let orphaned = had_receipt && resolve_err.is_none()
        && pend.lock().unwrap().is_open(order_hash);
    if orphaned {
        eprintln!(
            "[{lane}] ⛔ ORPHANED PENDING ROW: fill booked, resolve reported success, \
                   and the row {order_hash} is STILL OPEN. Its shares remain reserved."
        );
    }
    emitter
        .emit(
            serde_json::json!(
                { "t" : now_ms(), "ev" : "book_resolve", "lane" : lane, "tok" : &
                token[..token.len().min(14)], "side" : if side == 0 { "BUY" } else {
                "SELL" }, "booked" : booked, "had_receipt" : had_receipt, "resolve_err" :
                resolve_err, "orphaned" : orphaned, "pending_open_after" :
                still_open_after, "order_hash" : order_hash, }
            ),
        );
    halt_buys_on_persistence_fault_with(control, router, Some(pend), incidents);
    (booked, filled_shares)
}

pub(crate) fn spawn_settlement_booking(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
) {
        let (ctl, rt, em, fund) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
        );
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let url = format!(
                    "https://data-api.polymarket.com/positions?user={fund}\
&sizeThreshold=0.0001&limit=500&redeemable=true"
                );
                let rows: Vec<serde_json::Value> = match http.get(&url).send().await {
                    Ok(r) if r.status().is_success() => {
                        r.json().await.unwrap_or_default()
                    }
                    _ => continue,
                };
                let mut payout: std::collections::HashMap<String, f64> = Default::default();
                for p in &rows {
                    let view = copybot_hot::settlement::PositionView {
                        redeemable: p["redeemable"].as_bool().unwrap_or(false),
                        cur_price: p["curPrice"]
                            .as_f64()
                            .or_else(|| {
                                p["curPrice"].as_str().and_then(|x| x.parse().ok())
                            })
                            .unwrap_or(f64::NAN),
                    };
                    if let (Some(tok), Some(pay)) = (
                        p["asset"].as_str(),
                        copybot_hot::settlement::resolved_payout(view),
                    ) {
                        if !tok.is_empty() {
                            payout.insert(tok.to_string(), pay);
                        }
                    }
                }
                if payout.is_empty() {
                    continue;
                }
                for lane in rt.snapshot().iter() {
                    let name = lane.cfg.name.clone();
                    let held = ctl.lock().unwrap().ledger.holdings(&name);
                    for tok in held.keys() {
                        let Some(&pay) = payout.get(tok) else { continue };
                        let mut g = ctl.lock().unwrap();
                        let before = g
                            .ledger
                            .lanes
                            .get(&name)
                            .map(|b| b.risk.realised_pnl)
                            .unwrap_or(0.0);
                        if !g.ledger.record_settlement(&name, tok, pay) {
                            continue;
                        }
                        let after = g
                            .ledger
                            .lanes
                            .get(&name)
                            .map(|b| b.risk.realised_pnl)
                            .unwrap_or(0.0);
                        drop(g);
                        eprintln!(
                            "[settle] {name}: token …{} resolved at {pay:.3} \
                                   -> realised {:+.2}",
                            & tok[tok.len().saturating_sub(8)..], after - before
                        );
                        em.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "settle", "lane" : name, "tok" : &
                                tok[..tok.len().min(14)], "payout" : pay, "realised_delta" :
                                ((after - before) * 100.0).round() / 100.0 }
                            ),
                        );
                    }
                }
            }
        });
}

pub(crate) fn spawn_redemption_correction(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
) {
    const SETTLE_CLOCK_SLOP_SECS: i64 = 120;
        let (ctl, rt, em, fund) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
        );
        let settlement_pending = pending_log.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();
            let mut market_tokens: std::collections::HashMap<String, Vec<String>> = Default::default();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let ledger_path = ctl.lock().unwrap().ledger.path.clone();
                let (mut corrected, releases) = copybot_hot::ledger::scan_for_settlement(
                    &ledger_path,
                );
                const PAGE: usize = 200;
                const MAX_PAGES: usize = 25;
                let mut redemptions = Vec::new();
                for page in 0..MAX_PAGES {
                    let url = format!(
                        "https://data-api.polymarket.com/activity?user={fund}\
&limit={PAGE}&offset={}&type=REDEEM",
                        page * PAGE
                    );
                    let body: serde_json::Value = match http.get(&url).send().await {
                        Ok(r) if r.status().is_success() => {
                            r.json().await.unwrap_or_default()
                        }
                        _ => break,
                    };
                    let rows = copybot_hot::settlement::parse_redemptions(&body);
                    let short = body.as_array().map(|a| a.len() < PAGE).unwrap_or(true);
                    redemptions.extend(rows);
                    if short {
                        break;
                    }
                    if page + 1 == MAX_PAGES {
                        eprintln!(
                            "[settle] redemption scan reached the {MAX_PAGES}-page cap; \
older history is not covered and may require offline reconciliation"
                        );
                    }
                }
                if redemptions.is_empty() {
                    continue;
                }
                let todo = copybot_hot::settlement::unbooked(&redemptions, &corrected);
                if todo.is_empty() {
                    continue;
                }
                let settlement_snapshot = copybot_hot::positions::fetch(
                    &http, "https://data-api.polymarket.com", &fund, "0", "&includeArchived=true",
                    copybot_hot::ledger::now_secs(),
                ).await;
                let lane_names: Vec<String> = rt
                    .snapshot()
                    .iter()
                    .map(|l| l.cfg.name.clone())
                    .collect();
                for r in todo {
                    let tokens = if let Some(t) = market_tokens.get(&r.condition_id) {
                        t.clone()
                    } else {
                        let murl = format!(
                            "https://clob.polymarket.com/markets/{}", r.condition_id
                        );
                        let toks: Vec<String> = match http.get(&murl).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                resp.json::<serde_json::Value>()
                                    .await
                                    .ok()
                                    .and_then(|m| {
                                        m["tokens"]
                                            .as_array()
                                            .map(|a| {
                                                a
                                                    .iter()
                                                    .map(|t| t["token_id"].as_str().unwrap_or("").to_string())
                                                    .collect()
                                            })
                                    })
                                    .unwrap_or_default()
                            }
                            _ => Vec::new(),
                        };
                        if !toks.is_empty() {
                            market_tokens.insert(r.condition_id.clone(), toks.clone());
                        }
                        toks
                    };
                    let Some(tok) = r.token(&tokens) else {
                        em.emit(serde_json::json!({"t": now_ms(), "ev": "settle_deferred",
                            "reason": "redemption token mapping missing or inconsistent"}));
                        continue;
                    };
                    let payout_per_share = r.usdc / r.size;
                    let mut booked: Option<(String, f64)> = None;
                    for name in &lane_names {
                        let key = r.lane_key(name);
                        if corrected.contains(&key) {
                            continue;
                        }
                        let xkey = copybot_hot::settlement::cross_writer_key(name, tok);
                        if corrected.contains(&xkey) {
                            eprintln!(
                                "[settle] {name}: skipping token …{} — already \
                                       booked by settlement accounting ({xkey})",
                                & tok[tok.len().saturating_sub(8)..]
                            );
                            em.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "settle_dedupe_crosswriter", "lane"
                                    : name, "tok" : & tok[..tok.len().min(14)], "key" : key,
                                    "alt_key" : xkey, "tx" : & r.tx, "note" :
                                    "a settlement writer already credited this token; \
                                         if this token really redeemed twice, this is the \
                                         payout that went unbooked"
                                    }
                                ),
                            );
                            continue;
                        }
                        let pending = settlement_pending.lock().unwrap().open_tokens()
                            .iter().any(|(_, token)| token == tok);
                        let mut guard = ctl.lock().unwrap();
                        if guard.ledger.holdings(name).get(tok).copied().unwrap_or(0.0) > 1e-9 {
                            let stale = copybot_hot::ledger::now_secs() - settlement_snapshot.as_of > 120;
                            let result = guard.ledger.settle_redeemed_open(
                                name, tok, r, &settlement_snapshot, pending || stale,
                            );
                            drop(guard);
                            match result {
                                Ok(true) => {
                                    corrected.insert(key.clone());
                                    corrected.insert(xkey);
                                    em.emit(serde_json::json!({"t": now_ms(), "ev": "settle",
                                        "lane": name, "token": tok, "payout": payout_per_share,
                                        "key": key, "source": "redemption_activity"}));
                                }
                                Ok(false) => {},
                                Err(reason) => em.emit(serde_json::json!({"t": now_ms(),
                                    "ev": "settle_deferred", "lane": name, "token": tok,
                                    "reason": reason})),
                            }
                            continue;
                        }
                        drop(guard);
                        let Some(&(released, avg_cost, rel_t)) = releases
                            .get(&(name.clone(), tok.to_string())) else { continue };
                        if rel_t < r.ts - SETTLE_CLOCK_SLOP_SECS {
                            continue;
                        }
                        let shares = released.min(r.size);
                        if shares <= 1e-9 {
                            continue;
                        }
                        let proceeds = shares * payout_per_share;
                        let mut g = ctl.lock().unwrap();
                        let written = g.ledger
                            .record_realised_adjustment_tx(
                                name,
                                tok,
                                shares,
                                proceeds,
                                avg_cost,
                                "redemption observed after an earlier P&L-neutral \
                             reconciliation release; corrected to the real on-chain payout",
                                &key,
                            );
                        drop(g);
                        if !written {
                            em.emit(serde_json::json!({"t": now_ms(), "ev": "settle_deferred",
                                "lane": name, "token": tok, "reason": "adjustment not written or already booked"}));
                            continue;
                        }
                        corrected.insert(key.clone());
                        corrected.insert(xkey);
                        booked = Some((name.clone(), shares));
                        eprintln!(
                            "[settle] {name}: CORRECTED a prior neutral release for \
                                   token …{} — real payout ${payout_per_share:.4}/sh \
                                   (${proceeds:.2} total)",
                            & tok[tok.len().saturating_sub(8)..]
                        );
                        em.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "settle_correction", "lane" : name,
                                "tok" : & tok[..tok.len().min(14)], "payout" :
                                payout_per_share, "shares" : shares, "key" : key, "proceeds"
                                : (proceeds * 100.0).round() / 100.0 }
                            ),
                        );
                        break;
                    }
                    if let Some((lane, shares)) = booked {
                        if shares + 1e-9 < r.size {
                            eprintln!(
                                "[settle] {lane} claimed {shares:.4} of a \
{:.4}-share redemption on …{} — the remainder belongs to no lane",
                                r.size, & tok[tok.len().saturating_sub(8)..]
                            );
                            em.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "settle_unclaimed_remainder",
                                    "lane" : lane, "tok" : & tok[..tok.len().min(14)], "claimed"
                                    : shares, "redeemed" : r.size }
                                ),
                            );
                        }
                    }
                }
            }
        });
}

pub(crate) fn spawn_pending_recovery(
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    clob: &String,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    signer_addr: &String,
    sig_type: u8,
    resting_book: &Arc<copybot_hot::resting::RestingBook>,
) {
        let (lg, ctl, rt, em, clob_r, creds_r, addr_r) = (
            pending_log.clone(),
            control.clone(),
            router.clone(),
            emitter.clone(),
            clob.clone(),
            creds.clone(),
            signer_addr.clone(),
        );
        let rb_res = resting_book.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                if lg.lock().unwrap().is_empty() {
                    continue;
                }
                if let Err(_client_error) = resolve_pending(
                    &lg, &ctl, &rt, &em, &clob_r, &creds_r, &addr_r,
                    sig_type, Some(&rb_res),
                ).await {
                    em.emit(serde_json::json!({
                        "t": now_ms(), "ev": "pending_recovery_skipped",
                        "reason": "http_client_initialization", "pending_retained": true,
                    }));
                    eprintln!("[pending] timed HTTP client unavailable; recovery skipped, rows retained");
                }
            }
        });
}
