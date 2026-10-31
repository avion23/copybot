//! Native merge execution, reconciliation task and historical pair-ledger seeding.

use super::accounting::{book_matched_fak_race, fak_owner_submit_ready};
use super::control::latch_buys;
use super::execution::submit_tracked;
use super::exits::rescue_exit;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use copybot_hot::order::order_signer;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Account for leader split/merge evidence and classify merge withdrawals.
/// Observe-only: no follower execution or additional pair-ledger owner.
pub(crate) fn observe_leader_pair_tx(
    raw: &copybot_hot::feeds::RawTx,
    router: &Arc<Router>,
    pair_ledger: &Arc<std::sync::Mutex<copybot_hot::pairledger::PairLedger>>,
    emitter: &Arc<Emitter>,
    errors_path: &str,
) {
        if !raw.to.is_empty() {
            let owned: Vec<(usize, [u8; 20])> = (0..router.len())
                .filter_map(|ix| router.lane(ix).map(|l| (ix, l.cfg.wallet20)))
                .collect();
            let acts = copybot_hot::mergewatch::route(
                &raw.to,
                &raw.input,
                owned.iter().map(|(ix, w)| (*ix, w)),
            );
            for (lane_ix, act) in acts {
                let Some(lane) = router.lane(lane_ix) else { continue };
                let name = lane.cfg.name.clone();
                let leader = format!("0x{}", hex::encode(lane.cfg.wallet20));
                match act.kind {
                    copybot_hot::mergewatch::PairKind::Split => {
                        pair_ledger
                            .lock()
                            .unwrap()
                            .credit_split(
                                &leader,
                                &act.condition_id,
                                act.shares,
                                act.sized,
                            );
                        emitter
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "leader_split", "lane" : name,
                                    "cond" : & act.condition_id[..act.condition_id.len()
                                    .min(14)], "shares" : act.shares, "sized" : act.sized,
                                    "note" : "credited to his split balance; NOT copied" }
                                ),
                            );
                    }
                    copybot_hot::mergewatch::PairKind::Merge => {
                        let signal = pair_ledger
                            .lock()
                            .unwrap()
                            .merge_signal(
                                &leader,
                                &act.condition_id,
                                act.shares,
                                act.sized,
                            );
                        let (kind, exit_shares) = match &signal {
                            copybot_hot::pairledger::MergeSignal::Exit { shares } => {
                                ("exit", *shares)
                            }
                            copybot_hot::pairledger::MergeSignal::NonSignal => {
                                ("income_withdrawal", 0.0)
                            }
                            copybot_hot::pairledger::MergeSignal::Unknown => {
                                ("UNKNOWN_needs_human", 0.0)
                            }
                        };
                        emitter
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "leader_merge", "lane" : name,
                                    "cond" : & act.condition_id[..act.condition_id.len()
                                    .min(14)], "merged" : act.shares, "sized" : act.sized,
                                    "signal" : kind, "exit_shares" : exit_shares, "acted" :
                                    false, "note" :
                                    "OBSERVE-ONLY: decoded and netted, no order placed" }
                                ),
                            );
                        if matches!(
                            signal, copybot_hot::pairledger::MergeSignal::Unknown
                        ) {
                            copybot_hot::errors::record(
                                &errors_path,
                                &copybot_hot::errors::ErrorRow {
                                    t: copybot_hot::ledger::now_secs(),
                                    lane: name.clone(),
                                    kind: "merge_unknown".into(),
                                    detail: format!(
                                        "…{} merged {:.4} — split balance unknowable", & act
                                        .condition_id[act.condition_id.len().saturating_sub(10)..],
                                        act.shares
                                    ),
                                    human: format!(
                                        "{name}'s leader merged a position we cannot classify \
because an earlier split on that market had no readable size. It may be a real exit or a \
savings withdrawal — nothing was done automatically. Check the market by hand."
                                    ),
                                    severity: "warn".into(),
                                },
                            );
                        }
                    }
                }
            }
        }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MergeOutcome {
    Merged,
    DidNotHappen,
    Unresolved,
}
pub(crate) async fn run_merge_native(
    http: &reqwest::Client,
    rpc_url: &str,
    key: &[u8; 32],
    safe: &[u8; 20],
    adapter: &[u8; 20],
    condition: &str,
    pairs: f64,
    chain_id: u64,
) -> Result<String, String> {
    if !(pairs.is_finite() && pairs > 0.0) {
        return Err(format!("refusing to merge {pairs} pairs"));
    }
    let cid = copybot_hot::merge::condition_id_bytes(condition)
        .ok_or_else(|| format!("unreadable conditionId {condition}"))?;
    let amount = (pairs * 1e6).floor() as u128;
    if amount == 0 {
        return Err("merge amount rounds to zero".into());
    }
    let inner = copybot_hot::merge::merge_calldata(
        &copybot_hot::merge::COLLATERAL_PUSD,
        &cid,
        &copybot_hot::merge::BINARY_PARTITION,
        amount,
    );
    let tx = copybot_hot::txsend::send_safe_call(
            http,
            rpc_url,
            key,
            safe,
            adapter,
            &inner,
            chain_id,
        )
        .await?;
    match copybot_hot::txsend::wait_receipt(
            http,
            rpc_url,
            &tx,
            copybot_hot::mergeclaim::EXECUTOR_TIMEOUT_SECS,
        )
        .await
    {
        Ok(true) => Ok(tx),
        Ok(false) => Err(format!("merge reverted on chain ({tx})")),
        Err(e) => Err(format!("{e} — outcome UNKNOWN, hold the pair")),
    }
}
pub(crate) async fn seed_pair_ledger(
    http: &reqwest::Client,
    leader: &str,
    ledger: &std::sync::Mutex<copybot_hot::pairledger::PairLedger>,
) -> Result<(usize, usize), String> {
    let mut all: Vec<serde_json::Value> = Vec::new();
    for offset in (0..4000).step_by(500) {
        let url = format!(
            "https://data-api.polymarket.com/activity?\
user={leader}&limit=500&offset={offset}"
        );
        let rows: Vec<serde_json::Value> = match http.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                r.json().await.map_err(|e| e.to_string())?
            }
            Ok(r) => return Err(format!("activity HTTP {}", r.status())),
            Err(e) => return Err(e.to_string()),
        };
        let n = rows.len();
        all.extend(rows);
        if n < 500 {
            break;
        }
    }
    let seeds = copybot_hot::pairledger::seed_rows_from_activity(&all);
    let n = seeds.len();
    let mut g = ledger.lock().map_err(|_| "pair ledger lock poisoned".to_string())?;
    g.seed(leader, &seeds);
    let with_balance = seeds
        .iter()
        .filter(|(_, c, _, _)| g.balance(leader, c).map(|b| b > 1e-6).unwrap_or(true))
        .map(|(_, c, _, _)| c.clone())
        .collect::<std::collections::HashSet<_>>()
        .len();
    Ok((n, with_balance))
}

pub(crate) fn spawn_reconcile(
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    merge_claims: &Arc<std::sync::Mutex<copybot_hot::mergeclaim::MergeClaims>>,
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
    fak_journal: &Option<Arc<copybot_hot::order_commit::AtomicFakJournal>>,
) {
    const MERGE_MATERIAL_USD: f64 = 1.0;
        let execute = std::env::var("MERGE_RECONCILE").ok().as_deref() == Some("1");
        eprintln!(
            "[merge] reconciliation task up — execute={execute} (set MERGE_RECONCILE=1 to act)"
        );
        let pend5 = pending_log.clone();
        let fak5 = fak_journal.clone();
        let inc5 = incidents.clone();
        let mc5 = merge_claims.clone();
        let rpc_url5 = std::env::var("FILLWATCH_RPC")
            .unwrap_or_else(|_| copybot_hot::txsend::DEFAULT_RPC.to_string());
        let chain_id5: u64 = std::env::var("CHAIN_ID")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(137);
        let safe20_5 = {
            let v = hex::decode(funder.trim_start_matches("0x")).unwrap_or_default();
            let mut a = [0u8; 20];
            if v.len() == 20 {
                a.copy_from_slice(&v);
            }
            a
        };
        let mut merge_enabled5 = std::env::var("COPYBOT_MERGE_ENABLE").ok().as_deref()
            == Some("1");
        if merge_enabled5 && safe20_5 == [0u8; 20] {
            eprintln!(
                "[merge] ⛔ CRITICAL: COPYBOT_MERGE_ENABLE=1 but FUNDER_ADDRESS is not a \
readable 20-byte address — merging is DISABLED and pairs will be flattened by selling BOTH \
legs, paying the book twice."
            );
            merge_enabled5 = false;
        }
        let merge_enabled5 = merge_enabled5;
        eprintln!(
            "[merge] on-chain merging enabled={merge_enabled5} \
(set COPYBOT_MERGE_ENABLE=1 to merge instead of selling both legs)"
        );
        let (c5, r5, e5, f5, pk5, sa5, st5, clob5, creds5, paths5, wake5) = (
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
        );
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            let mut cond_outcomes: std::collections::HashMap<String, usize> = Default::default();
            loop {
                tick.tick().await;
                let ours_snap = copybot_hot::positions::fetch(
                        &http,
                        "https://data-api.polymarket.com",
                        &f5,
                        "0.0001",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                if !ours_snap.may_act_destructively() {
                    continue;
                }
                let ours: Vec<serde_json::Value> = ours_snap.rows;
                let mut tok_cond = std::collections::HashMap::new();
                for p in &ours {
                    if let (Some(a), Some(c)) = (
                        p["asset"].as_str(),
                        p["conditionId"].as_str(),
                    ) {
                        tok_cond.insert(a.to_string(), c.to_string());
                    }
                }
                let phys_of = |rows: &[serde_json::Value], tok: &str| {
                    rows
                        .iter()
                        .find(|p| p["asset"].as_str() == Some(tok))
                        .and_then(|p| {
                            p["size"]
                                .as_f64()
                                .or_else(|| p["size"].as_str().and_then(|x| x.parse().ok()))
                        })
                        .unwrap_or(0.0)
                };
                let mark_of = |tok: &str| {
                    ours
                        .iter()
                        .find(|p| p["asset"].as_str() == Some(tok))
                        .and_then(|p| p["curPrice"].as_f64())
                        .unwrap_or(0.0)
                };
                let neg_of = |tok: &str| {
                    ours
                        .iter()
                        .find(|p| p["asset"].as_str() == Some(tok))
                        .and_then(|p| p["negativeRisk"].as_bool())
                        .unwrap_or(false)
                };
                for lane in r5.snapshot().iter() {
                    if !lane.state.armed.load(Ordering::Relaxed)
                        || lane.state.retired.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let leader = format!("0x{}", hex::encode(lane.cfg.wallet20));
                    let his_snap = copybot_hot::positions::fetch(
                            &http,
                            "https://data-api.polymarket.com",
                            &leader,
                            "0.0001",
                            "",
                            copybot_hot::ledger::now_secs(),
                        )
                        .await;
                    if !his_snap.may_act_destructively() {
                        continue;
                    }
                    let his: Vec<serde_json::Value> = his_snap.rows;
                    if his.is_empty() {
                        let s2 = copybot_hot::positions::fetch(
                                &http,
                                "https://data-api.polymarket.com",
                                &leader,
                                "0.0001",
                                "",
                                copybot_hot::ledger::now_secs(),
                            )
                            .await;
                        let second = if s2.completeness.is_complete() {
                            copybot_hot::snapshot::Snapshot::Data(s2.len())
                        } else {
                            copybot_hot::snapshot::Snapshot::Failed(
                                s2.completeness.reason(),
                            )
                        };
                        let snaps = [copybot_hot::snapshot::Snapshot::Data(0), second];
                        if copybot_hot::snapshot::judge_empty(&snaps, 2)
                            != copybot_hot::snapshot::Verdict::Believable
                        {
                            eprintln!(
                                "[merge] {}: leader looked flat but a second read \
disagreed — skipping rather than liquidating",
                                lane.cfg.name
                            );
                            continue;
                        }
                    }
                    let his_conds: std::collections::HashSet<String> = his
                        .iter()
                        .filter_map(|p| p["conditionId"].as_str().map(|s| s.to_string()))
                        .collect();
                    let held: Vec<(String, f64)> = c5
                        .lock()
                        .unwrap()
                        .ledger
                        .holdings(&lane.cfg.name)
                        .into_iter()
                        .collect();
                    {
                        let mut candidates: std::collections::HashMap<&str, usize> = Default::default();
                        for (tok, sh) in &held {
                            if *sh <= 1e-6 {
                                continue;
                            }
                            if let Some(cid) = tok_cond.get(tok) {
                                *candidates.entry(cid.as_str()).or_default() += 1;
                            }
                        }
                        for (cid, n) in candidates {
                            if n != 2 || his_conds.contains(cid) {
                                continue;
                            }
                            if cond_outcomes.contains_key(cid) {
                                continue;
                            }
                            let murl = format!(
                                "https://clob.polymarket.com/markets/{cid}"
                            );
                            let count = match http.get(&murl).send().await {
                                Ok(r) if r.status().is_success() => {
                                    r.json::<serde_json::Value>()
                                        .await
                                        .ok()
                                        .and_then(|m| m["tokens"].as_array().map(|a| a.len()))
                                }
                                _ => None,
                            };
                            if let Some(c) = count {
                                cond_outcomes.insert(cid.to_string(), c);
                            }
                        }
                    }
                    let orphans = copybot_hot::merge::find_orphans(
                        &held,
                        &tok_cond,
                        |c| !his_conds.contains(c),
                        |c| cond_outcomes.get(c).copied(),
                    );
                    for o in &orphans {
                        let cid_ok = copybot_hot::merge::condition_id_bytes(
                                &o.condition_id,
                            )
                            .map(|cid| {
                                copybot_hot::merge::merge_calldata(
                                        &copybot_hot::merge::USDC,
                                        &cid,
                                        &copybot_hot::merge::BINARY_PARTITION,
                                        (o.mergeable() * 1e6) as u128,
                                    )
                                    .len()
                            })
                            .unwrap_or(0);
                        e5.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "merge_reconcile", "lane" : lane
                                .cfg.name, "condition" : o.condition_id, "mergeable" : o
                                .mergeable(), "residual" : o.residual().map(| (t, s) |
                                serde_json::json!({ "token" : t, "shares" : s })),
                                "merge_calldata_bytes" : cid_ok, "execute" : execute }
                            ),
                        );
                        if !execute {
                            continue;
                        }
                        let (Some(cr), Some(key)) = (creds5.as_ref(), pk5.as_ref()) else {
                            continue
                        };
                        let legs = [o.token_a.as_str(), o.token_b.as_str()];
                        let now_s = now_ms() as u64 / 1000;
                        let claimed = mc5
                            .lock()
                            .map(|mut g| g.claim(&lane.cfg.name, &legs, now_s))
                            .unwrap_or(false);
                        let pairs = o.mergeable();
                        if !claimed {
                            e5.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "merge_skip", "lane" : lane.cfg
                                    .name, "condition" : o.condition_id, "reason" :
                                    "legs_already_claimed" }
                                ),
                            );
                            continue;
                        }
                        if !merge_enabled5 {
                            if let Ok(mut g) = mc5.lock() {
                                g.release(&lane.cfg.name, &legs);
                            }
                            e5.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "merge_skip", "lane" : lane.cfg
                                    .name, "condition" : o.condition_id, "reason" :
                                    "COPYBOT_MERGE_ENABLE not set — selling both legs" }
                                ),
                            );
                        }
                        let before_a = phys_of(&ours, &o.token_a);
                        let before_b = phys_of(&ours, &o.token_b);
                        let outcome = if !merge_enabled5 {
                            MergeOutcome::DidNotHappen
                        } else {
                            let neg = neg_of(&o.token_a);
                            let r = run_merge_native(
                                    &http,
                                    &rpc_url5,
                                    key,
                                    &safe20_5,
                                    &copybot_hot::merge::merge_adapter_for(neg),
                                    &o.condition_id,
                                    pairs,
                                    chain_id5,
                                )
                                .await;
                            match r {
                                Ok(tx) => {
                                    let (ma, mb) = (mark_of(&o.token_a), mark_of(&o.token_b));
                                    let booked = c5
                                        .lock()
                                        .ok()
                                        .and_then(|mut g| {
                                            g
                                                .ledger
                                                .book_merge(
                                                    &lane.cfg.name,
                                                    &o.token_a,
                                                    &o.token_b,
                                                    pairs,
                                                    ma,
                                                    mb,
                                                    &tx,
                                                )
                                        })
                                        .is_some();
                                    e5.emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "merge_executed", "lane" : lane.cfg
                                            .name, "condition" : o.condition_id, "pairs" : pairs, "tx" :
                                            tx, "booked" : booked, "neg_risk" : neg }
                                        ),
                                    );
                                    if booked {
                                        MergeOutcome::Merged
                                    } else {
                                        e5.emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "merge_unbooked", "lane" : lane.cfg
                                                .name, "condition" : o.condition_id, "tx" : tx, "pairs" :
                                                pairs, "severity" : "critical", "why" :
                                                "merge confirmed on chain but the ledger \
refused the booking — holding the pair; do NOT sell, the shares are already gone"
                                                }
                                            ),
                                        );
                                        MergeOutcome::Unresolved
                                    }
                                }
                                Err(why) => {
                                    let unknown = why.contains("UNKNOWN");
                                    e5.emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "merge_failed", "lane" : lane.cfg
                                            .name, "condition" : o.condition_id, "pairs" : pairs, "why"
                                            : why, "outcome_unknown" : unknown, "severity" : "warn" }
                                        ),
                                    );
                                    if !unknown {
                                        MergeOutcome::DidNotHappen
                                    } else {
                                        let after = copybot_hot::positions::fetch(
                                                &http,
                                                "https://data-api.polymarket.com",
                                                &f5,
                                                "0.0001",
                                                "",
                                                copybot_hot::ledger::now_secs(),
                                            )
                                            .await;
                                        if !after.may_act_destructively() {
                                            e5.emit(
                                                serde_json::json!(
                                                    { "t" : now_ms(), "ev" : "merge_unresolved", "lane" : lane
                                                    .cfg.name, "condition" : o.condition_id, "pairs" : pairs,
                                                    "severity" : "critical", "why" :
                                                    "merge outcome UNKNOWN and the position \
read was incomplete — cannot tell whether it landed; holding the matched pair"
                                                    }
                                                ),
                                            );
                                            MergeOutcome::Unresolved
                                        } else {
                                            let aa = phys_of(&after.rows, &o.token_a);
                                            let ab = phys_of(&after.rows, &o.token_b);
                                            let v = copybot_hot::mergeverify::confirm(
                                                before_a,
                                                before_b,
                                                aa,
                                                ab,
                                                pairs,
                                            );
                                            e5.emit(
                                                serde_json::json!(
                                                    { "t" : now_ms(), "ev" : "merge_resolved", "lane" : lane.cfg
                                                    .name, "condition" : o.condition_id, "pairs" : pairs,
                                                    "verdict" : format!("{v:?}"), "before" : [before_a,
                                                    before_b], "after" : [aa, ab] }
                                                ),
                                            );
                                            match v {
                                                copybot_hot::mergeverify::Confirmation::Confirmed {
                                                    ..
                                                } => {
                                                    let (ma, mb) = (mark_of(&o.token_a), mark_of(&o.token_b));
                                                    let key = format!(
                                                        "mergeresolve:{}:{}", o.condition_id, pairs
                                                    );
                                                    let booked = c5
                                                        .lock()
                                                        .ok()
                                                        .and_then(|mut g| {
                                                            g
                                                                .ledger
                                                                .book_merge(
                                                                    &lane.cfg.name,
                                                                    &o.token_a,
                                                                    &o.token_b,
                                                                    pairs,
                                                                    ma,
                                                                    mb,
                                                                    &key,
                                                                )
                                                        })
                                                        .is_some();
                                                    if booked {
                                                        MergeOutcome::Merged
                                                    } else {
                                                        MergeOutcome::Unresolved
                                                    }
                                                }
                                                copybot_hot::mergeverify::Confirmation::NotApplied => {
                                                    MergeOutcome::DidNotHappen
                                                }
                                                copybot_hot::mergeverify::Confirmation::Inconsistent {
                                                    why,
                                                } => {
                                                    e5.emit(
                                                        serde_json::json!(
                                                            { "t" : now_ms(), "ev" : "merge_inconsistent", "lane" : lane
                                                            .cfg.name, "condition" : o.condition_id, "severity" :
                                                            "critical", "why" : why }
                                                        ),
                                                    );
                                                    MergeOutcome::Unresolved
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        };
                        let mut to_sell: Vec<String> = o
                            .residual()
                            .map(|(t, _)| t.to_string())
                            .into_iter()
                            .collect();
                        if outcome == MergeOutcome::DidNotHappen {
                            to_sell = legs.iter().map(|t| t.to_string()).collect();
                        }
                        if outcome == MergeOutcome::Unresolved && !to_sell.is_empty() {
                            e5.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "merge_hold_pair", "lane" : lane
                                    .cfg.name, "condition" : o.condition_id, "holding_pairs" :
                                    pairs, "flattening_residual" : to_sell, "why" :
                                    "the matched pair is inert and is held; the residual is \
naked exposure and is flattened regardless of the merge outcome"
                                    }
                                ),
                            );
                        }
                        for tok in to_sell.iter().map(|t| t.as_str()) {
                            let sz = c5
                                .lock()
                                .unwrap()
                                .ledger
                                .holdings(&lane.cfg.name)
                                .get(tok)
                                .copied()
                                .unwrap_or(0.0);
                            if sz <= 1e-9 {
                                continue;
                            }
                            let m = mark_of(tok);
                            let px = if m > 0.0 { m } else { 0.02 };
                            let (shares, limit) = if sz * px >= MERGE_MATERIAL_USD {
                                copybot_hot::venue::cleanup_fak_terms(sz, px)
                            } else {
                                copybot_hot::venue::dust_fak_terms(sz, px)
                            };
                            if shares <= 0.0 {
                                continue;
                            }
                            // Merge-residual FAK must hold the SAME money owner
                            // as the feed and sweep. Refuse before signing/POST
                            // if its lease or journal inode is unavailable.
                            if let Err(error) = fak_owner_submit_ready(fak5.as_ref()) {
                                latch_buys(&lane, &inc5, "fak_owner_unavailable",
                                    "merge FAK has no valid canonical journal");
                                e5.emit(serde_json::json!({
                                    "t": now_ms(), "ev":"merge_skip",
                                    "lane":lane.cfg.name, "token":tok,
                                    "why":"fak_owner_unavailable",
                                    "detail":format!("{error:?}"),
                                }));
                                continue;
                            }
                            let (ma, ta) = copybot_hot::order::amounts(limit, shares, 1);
                            let ord = copybot_hot::order::Order {
                                salt: copybot_hot::order::safe_salt(now_ms()),
                                maker: f5.clone(),
                                signer: order_signer(st5, &f5, &sa5).to_owned(),
                                token_id: tok.to_string(),
                                maker_amount: ma,
                                taker_amount: ta,
                                side: 1,
                                signature_type: st5,
                                timestamp: now_ms(),
                                neg_risk: neg_of(tok),
                            };
                            let sig = ord.sign_for_type(key);
                            let body = copybot_hot::order::json_body(
                                &ord,
                                &sig,
                                &cr.key,
                                "FAK",
                            );
                            let Ok(bs) = serde_json::to_string(&body) else { continue };
                            let hdrs = match copybot_hot::auth::l2_headers(
                                &sa5,
                                cr,
                                copybot_hot::auth::now_secs(),
                                "POST",
                                "/order",
                                Some(&bs),
                            ) {
                                Ok(h) => h,
                                Err(_) => continue,
                            };
                            let m_hash = hex::encode(ord.digest());
                            let (outcome, results) = match submit_tracked(
                                    &pend5,
                                    &e5,
                                    &copybot_hot::provenance::Provenance::own(
                                        copybot_hot::provenance::Origin::MergeReconcile,
                                        &lane.cfg.name,
                                        tok,
                                        1,
                                        shares,
                                        limit,
                                        sz,
                                    ),
                                    m_hash.clone(),
                                    false,
                                    &paths5,
                                    &format!("{clob5}/order"),
                                    &bs,
                                    &hdrs,
                                    Duration::from_secs(10),
                                    false,
                                )
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!(
                                        "[{}] merge REFUSING TO SUBMIT: {e}", lane.cfg.name
                                    );
                                    continue;
                                }
                            };
                            match &outcome {
                                copybot_hot::race_send::Outcome::Matched { .. } => {
                                    let (booked, _) = book_matched_fak_race(
                                        fak5.as_ref(), &c5, &pend5, &r5, &inc5, &e5,
                                        &m_hash, &lane.cfg.name, &outcome, &results,
                                        paths5.len(),
                                    );
                                    if booked {
                                        let _ = wake5.send(tok.to_string());
                                    } else {
                                        e5.emit(serde_json::json!({
                                            "t":now_ms(), "ev":"merge_unconfirmed",
                                            "lane":lane.cfg.name, "tok":tok,
                                            "why":"matched_but_fak_money_unverified",
                                        }));
                                    }
                                }
                                copybot_hot::race_send::Outcome::Rejected { body } => {
                                    rescue_exit(
                                            body,
                                            ord,
                                            pk5,
                                            &cr.key,
                                            "FAK",
                                            shares,
                                            sz,
                                            limit,
                                            &paths5,
                                            &clob5,
                                            creds5.as_ref().as_ref(),
                                            &sa5,
                                            &lane.cfg.name,
                                            tok,
                                            &c5,
                                            &r5,
                                            &e5,
                                            &wake5,
                                            &pend5,
                                            &inc5,
                                            fak5.as_ref(),
                                        )
                                        .await;
                                }
                                _ => {}
                            }
                        }
                        if let Ok(mut g) = mc5.lock() {
                            g.release(&lane.cfg.name, &legs);
                        }
                        e5.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "merge_reconcile_done", "lane" :
                                lane.cfg.name, "condition" : o.condition_id }
                            ),
                        );
                    }
                }
            }
        });
}
