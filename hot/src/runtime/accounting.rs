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
    _control: &Arc<std::sync::Mutex<Control>>,
    _router: &Arc<Router>,
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
                // Neither status nor /data/trades supplies an independently
                // proved per-order charged fee. Resting GTC may also fill in
                // multiple pieces. Never invent fee=0, fall back to the
                // submitted limit, clamp a SELL, or close PendingLog here.
                emitter.emit(serde_json::json!({
                    "t": now_ms(), "ev": "pending_money_unverified",
                    "lane": p.lane, "order_hash": p.order_hash,
                    "resting": p.resting,
                    "observed_shares": shares,
                    "observed_price": if price.is_finite() && price > 0.0 {
                        Some(price)
                    } else { None },
                    "reason": "venue recovery lacks authenticated cumulative filled amount and charged fee",
                    "reservation_retained": true,
                }));
                continue;
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
/// A previously booked order WITHOUT a canonical journal commitment is a
/// legacy split-brain residue, not an authority for freeing a reservation.
/// D615–D623 proven fills are repaired through the journal before this audit.
pub(crate) fn audit_unowned_boot_receipts(
    journal: Option<&Arc<copybot_hot::order_commit::AtomicFakJournal>>,
    control: &Arc<std::sync::Mutex<Control>>,
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    emitter: &Arc<Emitter>,
) {
    let Ok(commits) = journal.ok_or("canonical journal not bootstrapped")
        .and_then(|j| j.commits().map_err(|_| "journal replay unavailable"))
    else { return; /* no live owner => no live orders may be posted */ };
    let certified: std::collections::HashSet<String> = commits.into_iter()
        .map(|c| c.order_hash).collect();
    let legacy = {
        let control = control.lock().unwrap();
        let pending = pending.lock().unwrap();
        pending.open_rows().into_iter()
            .filter(|p| control.ledger.was_order_booked(&p.order_hash)
                && !certified.contains(&p.order_hash))
            .map(|p| p.order_hash).collect::<Vec<_>>()
    };
    if !legacy.is_empty() {
        for lane in router.snapshot().iter() {
            latch_buys(lane, incidents, "legacy_unjournaled_fill",
                "booked order is not part of canonical money journal; reservations retained");
        }
        emitter.emit(serde_json::json!({
            "t":now_ms(), "ev":"owner_boot_legacy_receipt_refusal",
            "uncertified_booked_pending_count":legacy.len(),
            "reservation_retained":true,
        }));
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

/// A single process-exclusive ledger/PendingLog critical section for every
/// payout, custody correction and pair merge. The journal lease guards the
/// authority; each Ledger mutation is one fsynced event, with replay keys
/// for redemptions and pair merges. No second PendingLog write follows a
/// payout or merge, so there is no cross-file terminal transition to race.
pub(crate) fn with_canonical_money<R>(
    journal: Option<&Arc<copybot_hot::order_commit::AtomicFakJournal>>,
    control: &Arc<std::sync::Mutex<Control>>,
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    action: impl FnOnce(
        &mut copybot_hot::order_state_owner::CanonicalMoneyOwner<'_>
    ) -> Result<R, String>,
) -> Result<R, String> {
    let lease = journal.ok_or_else(|| "canonical money journal not bootstrapped".to_string())?;
    let mut ctl = control.lock()
        .map_err(|_| "money Ledger mutex poisoned".to_string())?;
    let pending_guard = pending.lock()
        .map_err(|_| "money PendingLog mutex poisoned".to_string())?;
    let mut owner = copybot_hot::order_state_owner::CanonicalMoneyOwner::new(
        lease, &mut ctl.ledger, &pending_guard,
    )?;
    action(&mut owner)
}

/// Cheap canonical-FAK-owner admission check. No journal file is created,
/// and this never submits an order. Replaced or unlinked inodes refuse before
/// the caller can sign/POST or reserve a new FAK trade.
pub(crate) fn fak_owner_submit_ready(
    owner: Option<&Arc<copybot_hot::order_commit::AtomicFakJournal>>,
) -> Result<(), copybot_hot::order_commit::JournalRefusal> {
    owner.ok_or_else(|| copybot_hot::order_commit::JournalRefusal::Unsafe(
        "FAK journal not bootstrapped/replayed".into(),
    ))?.verify_owner()
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

/// A GTC partial/matched status without independently verified charged fee
/// is *not* a durable fill commitment. Keep every signed reservation until
/// cumulative order evidence can safely be recorded by the canonical owner.
#[allow(clippy::too_many_arguments)]
pub(crate) fn refuse_unproved_resting_money(
    pending: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    router: &Arc<Router>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    emitter: &Arc<Emitter>,
    lane: &str,
    hash: &str,
    why: &str,
) -> (bool, f64) {
    let retained = pending.lock().map(|p| p.is_open(hash)).unwrap_or(false);
    for l in router.snapshot().iter() {
        latch_buys(l, incidents, "gtc_money_unverified",
            "GTC fill lacks independently verified cumulative shares, price and fee");
    }
    emitter.emit(serde_json::json!({
        "t": now_ms(), "ev": "gtc_money_unverified", "lane": lane,
        "order_hash": hash, "reservation_retained": retained,
        "reason": why, "booked": false,
    }));
    (false, 0.0)
}

#[cfg(test)]
#[path = "accounting_tests.rs"]
mod d619_tests;


// The old periodic redeemable-price settlement writer is intentionally gone:
// a read-only position marked redeemable is NOT an on-chain redemption or
// proof of funds. Only the explicit redemption+complete-balance path below
// may book a terminal payout through the canonical owner.

pub(crate) fn spawn_redemption_correction(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    money_journal: &Option<Arc<copybot_hot::order_commit::AtomicFakJournal>>,
) {
    const SETTLE_CLOCK_SLOP_SECS: i64 = 120;
        let (ctl, rt, em, fund) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
        );
        let settlement_pending = pending_log.clone();
        let money_lease = money_journal.clone();
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
                        let still_held = ctl.lock().unwrap().ledger.holdings(name)
                            .get(tok).copied().unwrap_or(0.0) > 1e-9;
                        if still_held {
                            let stale = copybot_hot::ledger::now_secs()
                                - settlement_snapshot.as_of > 120;
                            let result = with_canonical_money(
                                money_lease.as_ref(), &ctl, &settlement_pending,
                                |owner| owner.redeem(name, tok, r, &settlement_snapshot, stale),
                            );
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
                        let written = with_canonical_money(
                            money_lease.as_ref(), &ctl, &settlement_pending,
                            |owner| owner.correct_redeemed_release(
                                name, tok, shares, proceeds, avg_cost, &key, r.ts,
                            ),
                        );
                        if !matches!(written, Ok(true)) {
                            em.emit(serde_json::json!({"t": now_ms(), "ev": "settle_deferred",
                                "lane": name, "token": tok,
                                "reason": format!("canonical adjustment refused: {written:?}")}));
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
