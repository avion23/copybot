//! Existing periodic and post-sale sweep task. The caller owns the live-mode gate.

use super::accounting::book_and_resolve;
use super::execution::submit_tracked;
use super::reconcile::recovery_target_shares;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::{Router, MICRO};
use copybot_hot::order::order_signer;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
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
    mut sweep_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
) {
        let pend3 = pending_log.clone();
        let inc3 = incidents.clone();
        let mc3 = merge_claims.clone();
        let (c3, r3, e3, f3, pk3, sa3, st3, clob3, creds3, paths3) = (
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
        );
        tokio::spawn(async move {
            let http = match reqwest::Client::builder()
                .timeout(Duration::from_secs(30)).build() {
                Ok(client) => client,
                Err(_client_error) => {
                    e3.emit(serde_json::json!({
                        "t": now_ms(), "ev": "sweep_unavailable",
                        "reason": "http_client_initialization",
                    }));
                    return;
                }
            };
            let mut swept: std::collections::HashMap<String, u64> = Default::default();
            let mut pending: std::collections::HashMap<
                (String, String),
                (u64, f64, f64),
            > = Default::default();
            let mut first_backstop = true;
            loop {
                let delay = if first_backstop { 10 } else { 60 };
                let just_sold: Option<String> = tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(delay)) => {
                    first_backstop = false; None }, tok = sweep_rx.recv() => {
                    tokio::time::sleep(Duration::from_secs(3)). await; match tok {
                    Some(t) => Some(t), None => continue } }
                };
                let Some(cr) = creds3.as_ref() else { continue };
                let Some(key) = pk3.as_ref() else { continue };
                // Read and validate all venue pages and every active lane
                // BEFORE any sweep order can be signed in this tick.
                let ours_snapshot = copybot_hot::positions::fetch(
                    &http, "https://data-api.polymarket.com", &f3,
                    "0.0001", "", copybot_hot::ledger::now_secs(),
                ).await;
                let mut staged = Vec::new();
                let mut evidence_failed = false;
                for lane in r3.snapshot() {
                    let name = lane.cfg.name.clone();
                    if !lane.state.armed.load(Ordering::Relaxed) {
                        pending.retain(|(lane_name, _), _| lane_name != &name);
                        e3.emit(serde_json::json!({
                            "t": now_ms(), "ev": "sweep_skip", "lane": name,
                            "reason": "lane_disarmed",
                        }));
                        continue;
                    }
                    let held = c3.lock().unwrap().ledger.holdings(&name);
                    let ours = match copybot_hot::sweep::complete_owned(
                        &ours_snapshot, &held,
                    ) {
                        Ok(rows) => rows,
                        Err(error) => {
                            e3.emit(serde_json::json!({
                                "t": now_ms(), "ev": "sweep_unavailable",
                                "lane": name, "reason": error.to_string(),
                            }));
                            evidence_failed = true;
                            break;
                        }
                    };
                    let leader = format!("0x{}", hex::encode(lane.cfg.wallet20));
                    let his_snapshot = copybot_hot::positions::fetch(
                        &http, "https://data-api.polymarket.com", &leader,
                        "0.0001", "", copybot_hot::ledger::now_secs(),
                    ).await;
                    let his = match copybot_hot::sweep::complete_leader(&his_snapshot) {
                        Ok(rows) => rows,
                        Err(error) => {
                            e3.emit(serde_json::json!({
                                "t": now_ms(), "ev": "sweep_unavailable",
                                "lane": name, "reason": error.to_string(),
                            }));
                            evidence_failed = true;
                            break;
                        }
                    };
                    staged.push((lane, held, ours, his));
                }
                if evidence_failed {
                    // Retain previous observations, reservations and ledger;
                    // refuse a partial snapshot instead of treating rows as zero.
                    continue;
                }
                for (lane, held, ours, his) in staged {
                    let name = lane.cfg.name.clone();
                    for (tok, p) in ours {
                        if p.redeemable {
                            continue;
                        }
                        if mc3
                            .lock()
                            .map(|g| g.is_claimed(&name, &tok, now_ms() as u64 / 1000))
                            .unwrap_or(true)
                        {
                            e3.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "sweep_skip", "lane" : name,
                                    "token" : tok, "reason" : "merge_in_flight" }
                                ),
                            );
                            continue;
                        }
                        // An absent leader token is verified flat only because
                        // complete_leader validated the entire paginated read.
                        let leader_position = his.get(&tok).copied();
                        let his_sz = leader_position.map_or(0.0, |row| row.shares);
                        let his_avg = leader_position.and_then(|row| row.average);
                        let physical_sz = p.physical;
                        let ours_claim = held[&tok];
                        let pool = c3.lock().unwrap().ledger.pool_claim(&tok);
                        let sz = ours_claim
                            .min(
                                copybot_hot::custody::lane_share_of(
                                    physical_sz,
                                    pool,
                                    ours_claim,
                                ),
                            );
                        let px = p.mark;
                        let our_avg = p.average;
                        if sz <= 1e-6 {
                            continue;
                        }
                        let obs_key = (name.clone(), tok.clone());
                        let now_s = now_ms() as u64 / 1000;
                        let prior = pending
                            .get(&obs_key)
                            .map(|(seen, t, e)| (now_s.saturating_sub(*seen), *t, *e));
                        let pol = lane.policy();
                        let proportional_target = match (pol.sizing, his_avg) {
                            (copybot_hot::lanes::Sizing::Pct(pct), Some(leader_avg)) => {
                                let scale = lane.state.cap_scale.load(Ordering::Relaxed)
                                    as f64 / MICRO;
                                recovery_target_shares(
                                    his_sz, leader_avg, our_avg, pct, scale,
                                    lane.cfg.min_order_usd, pol.max_effective_pct,
                                    pol.compound,
                                )
                            }
                            _ => None,
                        };
                        let action = copybot_hot::sweep::decide(
                            &copybot_hot::sweep::SweepInputs {
                                owned: ours_claim,
                                physical: physical_sz,
                                leader: his_sz,
                                mark: px,
                                redeemable: p.redeemable,
                                just_sold: just_sold.as_deref() == Some(tok.as_str()),
                                proportional_target,
                                prior,
                                confirm_secs: 60,
                                min_usd: 0.02,
                            },
                        );
                        let (sell_qty, reason, is_dust) = match action {
                            copybot_hot::sweep::SweepAction::Skip { .. } => {
                                pending.remove(&obs_key);
                                continue;
                            }
                            copybot_hot::sweep::SweepAction::Observe {
                                why,
                                target,
                                excess,
                            } => {
                                let stored = copybot_hot::sweep::observation_to_store(
                                    pending.get(&obs_key),
                                    now_s,
                                    target,
                                    excess,
                                );
                                pending.insert(obs_key.clone(), stored);
                                e3.emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "recovery_pending", "lane" : name,
                                        "tok" : tok, "owned" : sz, "physical" : physical_sz,
                                        "leader" : his_sz, "target" : target, "excess" : excess,
                                        "reason" : why, "his_avg" : his_avg, "our_avg" : our_avg,
                                        "cap_scale" : lane.state.cap_scale.load(Ordering::Relaxed)
                                        as f64 / MICRO, "confirm_after_secs" : 60 }
                                    ),
                                );
                                continue;
                            }
                            copybot_hot::sweep::SweepAction::Sell { qty, why, dust } => {
                                pending.remove(&obs_key);
                                (qty, why, dust)
                            }
                        };
                        let now = now_ms() as u64 / 1000;
                        let sweep_key = format!("{name}:{tok}");
                        if swept
                            .get(&sweep_key)
                            .map(|t| now.saturating_sub(*t) < 60)
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        swept.insert(sweep_key, now);
                        let (shares, limit) = if is_dust {
                            copybot_hot::venue::dust_fak_terms(sell_qty, px)
                        } else {
                            copybot_hot::venue::cleanup_fak_terms(sell_qty, px)
                        };
                        if shares <= 0.0 {
                            e3.emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "sweep_skip", "lane" : name, "tok"
                                    : tok, "shares" : sz, "reason" : "below_venue_share_tick" }
                                ),
                            );
                            continue;
                        }
                        let (ma, ta) = copybot_hot::order::amounts(limit, shares, 1);
                        let neg = p.negative_risk;
                        let ord = copybot_hot::order::Order {
                            salt: copybot_hot::order::safe_salt(now_ms()),
                            maker: f3.clone(),
                            signer: order_signer(st3, &f3, &sa3).to_owned(),
                            token_id: tok.clone(),
                            maker_amount: ma,
                            taker_amount: ta,
                            side: 1,
                            signature_type: st3,
                            timestamp: now_ms(),
                            neg_risk: neg,
                        };
                        let sig = ord.sign_for_type(key);
                        let body = copybot_hot::order::json_body(
                            &ord,
                            &sig,
                            &cr.key,
                            "FAK",
                        );
                        let Ok(body_s) = serde_json::to_string(&body) else { continue };
                        let hdrs = match copybot_hot::auth::l2_headers(
                            &sa3,
                            cr,
                            copybot_hot::auth::now_secs(),
                            "POST",
                            "/order",
                            Some(&body_s),
                        ) {
                            Ok(headers) => headers,
                            Err(err) => {
                                e3.emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "sweep_skip", "lane" : name, "tok"
                                        : tok, "reason" : "auth_header_failed", "error" : err, }
                                    ),
                                );
                                continue;
                            }
                        };
                        let sweep_hash = hex::encode(ord.digest());
                        let (outcome, _res) = match submit_tracked(
                                &pend3,
                                &e3,
                                &copybot_hot::provenance::Provenance::own(
                                    copybot_hot::provenance::Origin::DustSweep {
                                        reason: reason.to_string(),
                                    },
                                    &name,
                                    &tok,
                                    1,
                                    shares,
                                    limit,
                                    sz,
                                ),
                                sweep_hash.clone(),
                                false,
                                &paths3,
                                &format!("{clob3}/order"),
                                &body_s,
                                &hdrs,
                                Duration::from_secs(10),
                                false,
                            )
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("[{name}] sweep REFUSING TO SUBMIT: {e}");
                                e3.emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "submit_refused", "lane" : name,
                                        "tok" : tok, "why" : e }
                                    ),
                                );
                                continue;
                            }
                        };
                        e3.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "sweep", "lane" : name, "tok" :
                                tok, "shares" : shares, "limit" : limit, "reason" : reason,
                                "order_type" : "FAK", "max_slippage" : "100%", "usd" :
                                shares * limit, "outcome" : format!("{:?}", outcome)
                                .chars().take(90).collect::< String > () }
                            ),
                        );
                        if let copybot_hot::race_send::Outcome::Matched { body, .. } = &outcome {
                            book_and_resolve(
                                &c3,
                                &pend3,
                                &r3,
                                &inc3,
                                &e3,
                                &name,
                                &tok,
                                1,
                                limit,
                                body,
                                None,
                                &sweep_hash,
                                false,
                            );
                        }
                    }
                }
            }
        });
}
