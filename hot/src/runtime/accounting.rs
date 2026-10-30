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
                // The order-status/trades recovery API here has NO verified
                // per-fill fee. An unfilled FAK journal row therefore cannot
                // be booked at fee=0 (nor at a provisional limit price).
                // Keep it reserved for independently proven money evidence.
                if copybot_hot::pending::unproven_fak_money(
                    p.resting, price.is_finite() && price > 0.0, false,
                ) {
                    emitter.emit(serde_json::json!({
                        "t": now_ms(), "ev": "pending_fak_money_unverified",
                        "lane": p.lane, "order_hash": p.order_hash,
                        "reason": "venue recovery lacks proven fee; booking deferred",
                    }));
                    continue;
                }
                // A verified venue match advances the durable reservation
                // before any ledger booking. Unknown outcomes stay reserved.
                if let Err(error) = log.lock().unwrap().advance(
                    &p.order_hash, copybot_hot::pending::OrderStage::Accepted,
                ) {
                    emitter.emit(serde_json::json!({
                        "t": now_ms(), "ev": "pending_state_unavailable",
                        "lane": p.lane, "reason": error,
                    }));
                    continue;
                }
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
                    // A zero bookable balance does not prove that an observed
                    // fill did not take place; retain the identity for review.
                    emitter.emit(serde_json::json!({
                        "t": now_ms(), "ev": "pending_unattributed",
                        "lane": p.lane, "tok": p.token,
                        "reason": "venue match has no bookable shares",
                    }));
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
                if let Err(error) = log.lock().unwrap().resolve(
                    &p.order_hash, "not_filled", None,
                ) {
                    emitter.emit(serde_json::json!({
                        "t": now_ms(), "ev": "pending_release_unavailable",
                        "lane": p.lane, "reason": error,
                    }));
                    continue;
                }
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

/// Repair only already-booked FAK reservations after a crash between ledger
/// fsync and pending-log resolution. This is one coordinated recovery edge;
/// both files remain independently durable until the full owner migration.
pub(crate) fn recover_booked_pending_at_boot(
    control: &Arc<std::sync::Mutex<Control>>,
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    emitter: &Arc<Emitter>,
) {
    let result = {
        let ctl = control.lock().unwrap();
        let mut journal = pending.lock().unwrap();
        copybot_hot::order_state_owner::OrderStateOwner::new(
            &ctl.ledger, &mut journal,
        ).reconcile_restart()
    };
    match result {
        Ok(report) => {
            if report.recovered > 0 || report.retained_resting > 0 {
                eprintln!(
                    "[order_owner] restart: {} booked FAK reservation(s) durably closed; {} resting order(s) still reserved for venue proof",
                    report.recovered, report.retained_resting
                );
                emitter.emit(serde_json::json!({
                    "t": now_ms(), "ev":"order_owner_restart",
                    "booked_fak_closed": report.recovered,
                    "resting_kept": report.retained_resting,
                    "unverified_kept": report.missing_ledger_receipts,
                }));
            }
        }
        Err(refusal) => {
            // Never free outstanding budget if either durable source cannot
            // prove a coherent full order identity and booked amount.
            eprintln!(
                "[order_owner] REFUSING restart reconciliation: {refusal:?}; unresolved reservations remain, new buys halted"
            );
            for lane in router.snapshot().iter() {
                latch_buys(
                    lane, incidents, "order_owner_restart",
                    "ledger/pending booking evidence conflicts or cannot persist",
                );
            }
            emitter.emit(serde_json::json!({
                "t": now_ms(), "ev":"order_owner_restart_refusal",
                "why": format!("{refusal:?}"),
            }));
        }
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

/// Bootstrap the canonical FAK owner only for actual non-shadow live mode.
/// Dry observation has zero journal side effects. Failed ownership/recovery
/// visibly latches buys and refuses subsequent FAK submission before POST.
pub(crate) fn open_fak_journal_for_live(
    enabled: bool,
    control_path: &str,
    control: &Arc<std::sync::Mutex<Control>>,
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
) -> Option<Arc<copybot_hot::order_commit::AtomicFakJournal>> {
    if !enabled { return None; }
    let path = format!("{control_path}.fak_commit");
    let result = copybot_hot::order_commit::AtomicFakJournal::open(&path)
        .and_then(|journal| {
            let repaired = {
                let mut ctl = control.lock().map_err(|_| {
                    copybot_hot::order_commit::JournalRefusal::Unsafe(
                        "ledger owner mutex poisoned".into(),
                    )
                })?;
                let mut pend = pending.lock().map_err(|_| {
                    copybot_hot::order_commit::JournalRefusal::Unsafe(
                        "pending owner mutex poisoned".into(),
                    )
                })?;
                journal.repair_legacy(&mut ctl.ledger, &mut pend)
            }?;
            Ok((journal, repaired))
        });
    match result {
        Ok((journal, repaired)) => {
            eprintln!("[fak_owner] boot replay: {} fills, {} closed, {} already closed",
                repaired.fills_booked, repaired.pending_closed, repaired.already_closed);
            Some(Arc::new(journal))
        }
        Err(error) => {
            eprintln!("[fak_owner] boot ownership/replay REFUSED: {error:?}");
            for lane in router.snapshot().iter() {
                latch_buys(lane, incidents, "fak_owner_unavailable",
                    "FAK journal lock/replay cannot be verified; new buys halted");
            }
            None
        }
    }
}

/// The one migrated runtime matched-FAK booking edge. A venue HTTP race
/// has ALREADY happened: failure here must keep the durable reservation and
/// halt new buys instead of silently trying legacy limit/zero-fee booking.
#[allow(clippy::too_many_arguments)]
pub(crate) fn book_matched_fak_race(
    journal: Option<&Arc<copybot_hot::order_commit::AtomicFakJournal>>,
    control: &Arc<std::sync::Mutex<Control>>,
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    emitter: &Arc<Emitter>,
    hash: &str,
    lane: &str,
    outcome: &copybot_hot::race_send::Outcome,
    results: &[copybot_hot::race_send::PathResult],
    attempted_paths: usize,
) -> (bool, f64) {
    let attempt = journal
        .ok_or(copybot_hot::venue_fak::StrictFakRefusal::DurableStateUnavailable)
        .and_then(|owner| {
            // Same lock order as D614 boot replay. The only FAK journal owner
            // is process-exclusive; no concurrent Ledger/PendingLog release.
            let mut ctl = control.lock()
                .map_err(|_| copybot_hot::venue_fak::StrictFakRefusal::DurableStateUnavailable)?;
            let mut pend = pending.lock()
                .map_err(|_| copybot_hot::venue_fak::StrictFakRefusal::DurableStateUnavailable)?;
            copybot_hot::venue_fak::commit_matched_fak_from_race(
                owner, &mut ctl.ledger, &mut pend, hash, outcome, results,
                attempted_paths,
            )
        });
    let (booked, shares, reason) = match attempt {
        Ok(done) => (true, done.receipt.shares, None),
        Err(refusal) => {
            let why = format!("{refusal:?}");
            eprintln!("[{lane}] FAK money proof REFUSED: {why}; pending remains reserved");
            for item in router.snapshot().iter() {
                latch_buys(item, incidents, "fak_money_unverified",
                    "matched FAK monetary evidence/journal unavailable; retained reservation");
            }
            (false, 0.0, Some(why))
        }
    };
    emitter.emit(serde_json::json!({
        "t": now_ms(), "ev": "strict_fak_book",
        "lane": lane, "order_hash": hash, "booked": booked,
        "shares": shares, "reason": reason,
    }));
    halt_buys_on_persistence_fault_with(control, router, Some(pending), incidents);
    (booked, shares)
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


#[cfg(test)]
mod d619_tests {
    use super::*;
    use copybot_hot::pending::{Pending, PendingLog};
    use copybot_hot::ledger::Ledger;
    use copybot_hot::risk::RiskConfig;
    use copybot_hot::race_send::{Outcome, PathResult};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(1);
    type Fixture = (
        PathBuf, Arc<std::sync::Mutex<Control>>,
        Arc<std::sync::Mutex<PendingLog>>, Arc<Router>,
        Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>, Arc<Emitter>,
        String,
    );
    fn fixture(label: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!(
            "cb-d619-{label}-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let control_path = dir.join("ctl").to_string_lossy().into_owned();
        let ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("alpha".into(), RiskConfig::default())],
        );
        let control = Arc::new(std::sync::Mutex::new(Control::new(
            ledger, dir.join("operator.json").to_str().unwrap(),
        )));
        let pending = Arc::new(std::sync::Mutex::new(PendingLog::open(
            dir.join("pending.jsonl").to_str().unwrap(),
        )));
        pending.lock().unwrap().record(Pending {
            lane: "alpha".into(), token: "tok".into(), side: 0,
            order_hash: "h1".into(), shares: 10.0, limit: 0.6,
            ts: 12, why: "offline_fixture".into(), resting: false,
        }).unwrap();
        let router = Arc::new(Router::new(vec![]));
        let incidents = Arc::new(std::sync::Mutex::new(
            copybot_hot::incidents::Incidents::open(dir.join("incidents.jsonl")),
        ));
        let emitter = Arc::new(Emitter::new(
            dir.join("events.jsonl").to_string_lossy().into_owned(), false,
        ));
        (dir, control, pending, router, incidents, emitter, control_path)
    }
    fn matched(fee: bool) -> String {
        format!(
            r#"{{"status":"matched","success":true,"orderID":"0xh1","takingAmount":"9","makingAmount":"4.05"{}}}"#,
            if fee { r#","feeAmount":"0.015""# } else { "" },
        )
    }
    #[test]
    fn dry_and_shadow_never_open_a_durable_fak_owner_file() {
        let (dir, c, p, r, i, _e, path) = fixture("dry");
        assert!(open_fak_journal_for_live(false, &path, &c, &p, &r, &i).is_none());
        assert!(!dir.join("ctl.fak_commit").exists());
        assert!(!dir.join("ctl.fak_commit.owner.lock").exists());
        assert!(p.lock().unwrap().is_open("h1"));
        assert!(!c.lock().unwrap().ledger.was_order_booked("h1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn prepared_owner_routes_matched_fak_through_one_journal_then_ledger() {
        let (dir, c, p, r, i, e, path) = fixture("match");
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i)
            .expect("valid empty owner must open");
        let body = matched(true);
        let outcome = Outcome::Matched {
            body: body.clone(), winner: "p".into(), ms: 1.0,
        };
        let results = [PathResult {
            path: "p".into(), http: 200, body, ms: 1.0,
        }];
        let (booked, shares) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e, "h1", "alpha",
            &outcome, &results, 1,
        );
        assert!(booked);
        assert_eq!(shares, 9.0);
        assert!(!p.lock().unwrap().is_open("h1"));
        assert_eq!(journal.commits().unwrap().len(), 1);
        let guard = c.lock().unwrap();
        assert_eq!(guard.ledger.recorded_order("h1").unwrap().fee, 0.015);
        drop(guard);
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn matched_without_fee_latches_result_and_keeps_pending_unchanged() {
        let (dir, c, p, r, i, e, path) = fixture("unknown");
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
        let body = matched(false);
        let outcome = Outcome::Matched {
            body: body.clone(), winner: "p".into(), ms: 1.0,
        };
        let results = [PathResult {
            path: "p".into(), http: 200, body, ms: 1.0,
        }];
        let (booked, shares) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e, "h1", "alpha",
            &outcome, &results, 1,
        );
        assert!(!booked);
        assert_eq!(shares, 0.0);
        assert!(p.lock().unwrap().is_open("h1"));
        assert!(!c.lock().unwrap().ledger.was_order_booked("h1"));
        assert!(journal.commits().unwrap().is_empty());
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn journal_only_crash_is_recovered_at_boot_without_network_or_post() {
        let (dir, c, p, r, i, _e, path) = fixture("restart");
        let journal = copybot_hot::order_commit::AtomicFakJournal::open(
            dir.join("ctl.fak_commit"),
        ).unwrap();
        let fact = copybot_hot::venue_fak::parse_matched_fak(
            &p.lock().unwrap().open_rows()[0], &matched(true),
        ).unwrap();
        journal.append(fact).unwrap();
        drop(journal);
        let recovered = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
        assert_eq!(recovered.commits().unwrap().len(), 1);
        assert!(!p.lock().unwrap().is_open("h1"));
        assert_eq!(c.lock().unwrap().ledger.recorded_order("h1").unwrap().fee, 0.015);
        drop(recovered);
        std::fs::remove_dir_all(dir).unwrap();
    }
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
