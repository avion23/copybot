//! Orphan holding diagnostic and optional exit submission; called only under main live gate.
//! One worker retains its own cadence, warning suppression and tracked submission path.

use super::execution::submit_tracked;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use copybot_hot::order::order_signer;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    funder: &String,
    pk: Option<[u8; 32]>,
    signer_addr: &String,
    sig_type: u8,
    clob: &String,
    creds: &Arc<Option<copybot_hot::auth::ApiCreds>>,
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    order_paths: &Arc<Vec<(String, reqwest::Client)>>,
) {
        let fire = std::env::var("ORPHAN_SWEEP").ok().as_deref() == Some("1");
        eprintln!(
            "[orphan] stranded-position sweep up — fire={fire} \
                   (set ORPHAN_SWEEP=1 to act)"
        );
        let (c6, r6, e6, f6, pk6, sa6, st6, clob6, creds6) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            funder.clone(),
            pk,
            signer_addr.clone(),
            sig_type,
            clob.clone(),
            creds.clone(),
        );
        let user6 = funder.clone();
        let pend6 = pending_log.clone();
        let paths6 = order_paths.clone();
        tokio::spawn(async move {
            let http = match reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(client) => client,
                Err(_) => {
                    eprintln!("[orphan] REFUSED: cannot initialize timed position client");
                    e6.emit(serde_json::json!({
                        "t": now_ms(), "ev": "orphan_unavailable",
                        "why": "http_client_initialization",
                    }));
                    return;
                }
            };
            let mut orphan_said: std::collections::HashMap<
                (String, String),
                (u64, u64),
            > = std::collections::HashMap::new();
            const ORPHAN_RESAY_SECS: u64 = 3600;
            let mut tick = tokio::time::interval(Duration::from_secs(90));
            loop {
                tick.tick().await;
                let mut rows: Vec<copybot_hot::orphan::Holding> = Vec::new();
                for lane in r6.snapshot().iter() {
                    if !lane.state.armed.load(Ordering::Relaxed) {
                        continue;
                    }
                    let holds = c6.lock().unwrap().ledger.holdings(&lane.cfg.name);
                    for (tok, ours) in holds.iter() {
                        if *ours <= 1e-9 {
                            continue;
                        }
                        rows.push(copybot_hot::orphan::Holding {
                            lane: lane.cfg.name.clone(),
                            token: tok.clone(),
                            ours: *ours,
                            his: *lane.his_pos.lock().unwrap().get(tok).unwrap_or(&0.0),
                            reserved: lane.sell_reserved(tok),
                            legacy: *lane.legacy.lock().unwrap().get(tok).unwrap_or(&0.0),
                        });
                    }
                }
                let plan = copybot_hot::orphan::plan(
                    &rows,
                    copybot_hot::orphan::MIN_SHARES,
                );
                let list = match plan {
                    copybot_hot::orphan::Sweep::Idle => continue,
                    copybot_hot::orphan::Sweep::Refused {
                        why,
                        would_have_fired,
                        held,
                    } => {
                        eprintln!("[orphan] REFUSED: {why}");
                        e6.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "orphan_refused", "why" : why,
                                "would_have_fired" : would_have_fired, "held" : held }
                            ),
                        );
                        continue;
                    }
                    copybot_hot::orphan::Sweep::Fire(v) => v,
                };
                let snap = copybot_hot::positions::fetch(
                        &http,
                        "https://data-api.polymarket.com",
                        &user6,
                        "0.0001",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                let held: std::collections::HashMap<String, f64> = list
                    .iter()
                    .map(|orphan| (orphan.token.clone(), orphan.ours))
                    .collect();
                let quotes = match copybot_hot::flatten::market_terms_for_held(&snap, &held) {
                    Ok(quotes) => quotes,
                    Err(error) => {
                        eprintln!("[orphan] REFUSED: unverifiable market evidence: {error}");
                        e6.emit(serde_json::json!({
                            "t": now_ms(), "ev": "orphan_refused",
                            "why": error.to_string(), "would_have_fired": list.len(),
                            "held": list.len(),
                        }));
                        continue;
                    }
                };
                for o in list {
                    let Some(quote) = quotes.get(&o.token) else {
                        e6.emit(serde_json::json!({
                            "t": now_ms(), "ev": "orphan_refused",
                            "why": "validated_market_quote_missing", "held": 1,
                        }));
                        continue;
                    };
                    let sz = o.strandable();
                    let m = quote.mark;
                    let (shares, limit) = copybot_hot::venue::cleanup_fak_terms(sz, m);
                    if shares <= 0.0 {
                        continue;
                    }
                    let said_key = (o.lane.clone(), o.token.clone());
                    let now_s = now_ms() as u64 / 1000;
                    let say = match orphan_said.get_mut(&said_key) {
                        None => {
                            orphan_said.insert(said_key.clone(), (now_s, 0));
                            Some(0)
                        }
                        Some(
                            (last, held_back),
                        ) if now_s.saturating_sub(*last) >= ORPHAN_RESAY_SECS => {
                            let n = *held_back;
                            *last = now_s;
                            *held_back = 0;
                            Some(n)
                        }
                        Some((_, held_back)) => {
                            *held_back += 1;
                            None
                        }
                    };
                    if let Some(suppressed) = say {
                        eprintln!(
                            "[orphan] {} holds {shares:.4} sh of …{} that he has fully \
                                   left — {} at {limit:.4} (mark {m:.4})",
                            o.lane, & o.token[o.token.len().saturating_sub(8)..], if fire
                            { "EXITING" } else { "would exit" }
                        );
                        e6.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "orphan_exit", "lane" : o.lane,
                                "tok" : & o.token[..o.token.len().min(14)], "shares" :
                                shares, "limit" : limit, "mark" : m, "ours" : o.ours,
                                "reserved" : o.reserved, "fired" : fire, "suppressed_passes"
                                : suppressed, "unsellable" : o.ours * m < 1.0 }
                            ),
                        );
                    }
                    if !fire {
                        continue;
                    }
                    let (Some(cr), Some(key)) = (creds6.as_ref().as_ref(), pk6.as_ref())
                    else { continue };
                    let (ma, ta) = copybot_hot::order::amounts(limit, shares, 1);
                    let ord = copybot_hot::order::Order {
                        salt: copybot_hot::order::safe_salt(now_ms()),
                        maker: f6.clone(),
                        signer: order_signer(st6, &f6, &sa6).to_owned(),
                        token_id: o.token.clone(),
                        maker_amount: ma,
                        taker_amount: ta,
                        side: 1,
                        signature_type: st6,
                        timestamp: now_ms(),
                        neg_risk: quote.negative_risk,
                    };
                    let sig = ord.sign_for_type(key);
                    let body = copybot_hot::order::json_body(&ord, &sig, &cr.key, "FAK");
                    let Ok(bs) = serde_json::to_string(&body) else { continue };
                    let Ok(hdrs) = copybot_hot::auth::l2_headers(
                        &sa6,
                        cr,
                        copybot_hot::ledger::now_secs() as u64,
                        "POST",
                        "/order",
                        Some(&bs),
                    ) else { continue };
                    let sweep_hash = hex::encode(ord.digest());
                    let hdr_vec: Vec<(&'static str, String)> = hdrs
                        .into_iter()
                        .collect();
                    let outcome = match submit_tracked(
                            &pend6,
                            &e6,
                            &copybot_hot::provenance::Provenance::mirrored(
                                copybot_hot::provenance::Origin::OrphanSweep,
                                &o.lane,
                                &o.token,
                                1,
                                shares,
                                limit,
                                o.ours,
                                Some(o.his),
                                None,
                                None,
                                None,
                            ),
                            sweep_hash,
                            false,
                            &paths6,
                            &format!("{clob6}/order"),
                            &bs,
                            &hdr_vec,
                            Duration::from_secs(10),
                            false,
                        )
                        .await
                    {
                        Ok((outcome, _)) => outcome,
                        Err(e) => {
                            eprintln!("[orphan] REFUSING TO SUBMIT: {e}");
                            continue;
                        }
                    };
                    e6.emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "orphan_exit_resp", "lane" : o.lane,
                            "outcome" : format!("{outcome:?}") }
                        ),
                    );
                }
            }
        });
}
