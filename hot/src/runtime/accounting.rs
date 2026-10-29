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
) {
    use copybot_hot::pending::Verdict;
    let Some(cr) = creds.as_ref().as_ref() else { return };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap_or_default();
    let now = copybot_hot::ledger::now_secs();
    let due = log.lock().unwrap().due(now);
    if due.is_empty() {
        return;
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
