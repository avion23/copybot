//! Single receive-loop owner for deduplicated leader fills and tracked venue execution.
//! The entrypoint owns feeds, journals, registries and the only context instance.

use super::*;

pub(crate) struct Context {
    pub(crate) live: bool,
    pub(crate) emitter: Arc<Emitter>,
    pub(crate) feed_registry: Arc<copybot_hot::feeds::FeedRegistry>,
    pub(crate) router: Arc<Router>,
    pub(crate) pair_ledger: Arc<std::sync::Mutex<copybot_hot::pairledger::PairLedger>>,
    pub(crate) errors_path: String,
    pub(crate) politics: runtime::politics::PoliticsBook,
    pub(crate) watch_tokens: Arc<std::sync::Mutex<Vec<String>>>,
    pub(crate) resting_book: Arc<copybot_hot::resting::RestingBook>,
    pub(crate) creds: Arc<Option<copybot_hot::auth::ApiCreds>>,
    pub(crate) http: reqwest::Client,
    pub(crate) clob: String,
    pub(crate) auth_addr: String,
    pub(crate) signal_guard: Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
    pub(crate) book: Arc<RaceBook>,
    pub(crate) control: Arc<std::sync::Mutex<Control>>,
    pub(crate) incidents: Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    pub(crate) physical_wallet: Arc<std::sync::Mutex<PhysicalWallet>>,
    pub(crate) fire_rate: Arc<copybot_hot::firerate::FireRate>,
    pub(crate) order_paths: Arc<Vec<(String, reqwest::Client)>>,
    pub(crate) pk: Option<[u8; 32]>,
    pub(crate) funder: String,
    pub(crate) signer_addr: String,
    pub(crate) sig_type: u8,
    pub(crate) pending_log: Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    pub(crate) fak_journal: Option<Arc<copybot_hot::order_commit::AtomicFakJournal>>,
    pub(crate) wal: Arc<copybot_hot::wal::Wal>,
    pub(crate) latency: Arc<copybot_hot::latency::Latency>,
    pub(crate) levels: Arc<copybot_hot::book::Levels>,
    pub(crate) prints: Arc<copybot_hot::book::TradePrints>,
    pub(crate) sweep_tx: Arc<tokio::sync::mpsc::UnboundedSender<String>>,
    pub(crate) books: Arc<BookCache>,
    pub(crate) response_times: Arc<copybot_hot::response_times::Ring>,
    pub(crate) sell_tranches: Arc<copybot_hot::lanes::TrancheLedger>,
}

pub(crate) async fn run(mut rx: mpsc::UnboundedReceiver<RawTx>, ctx: Context) {
    let Context {
        live, emitter, feed_registry, router, pair_ledger,
        errors_path, politics, watch_tokens, resting_book, creds,
        http, clob, auth_addr, signal_guard, book,
        control, incidents, physical_wallet, fire_rate, order_paths,
        pk, funder, signer_addr, sig_type, pending_log, fak_journal,
        wal, latency, levels, prints, sweep_tx,
        books, response_times, sell_tranches
    } = ctx;
    let mut feed_race = runtime::telemetry::FeedRace::new();
    while let Some(raw) = rx.recv().await {
        let t0 = std::time::Instant::now();
        let queue_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.as_nanos().saturating_sub(raw.seen_ns)) as f64 / 1000.0)
            .unwrap_or(0.0);
        if feed_race.is_duplicate(&raw, &emitter) {
            continue;
        }
        feed_registry.record_win(&raw.source);
        runtime::merge::observe_leader_pair_tx(&raw, &router, &pair_ledger, &emitter, &errors_path);
        let present = participants(&raw.input);
        if present.is_empty() {
            continue;
        }
        for lane_ix in 0..router.len() {
            let lane = match router.lane(lane_ix) {
                Some(l) => l,
                None => continue,
            };
            let w20 = lane.cfg.wallet20;
            if !present.iter().any(|p| p == &w20) {
                continue;
            }
            for d in decode_all(&raw.input, &w20) {
                if lane.cfg.exclude_political
                    && politics
                        .read()
                        .map(|s| copybot_hot::political_evidence::blocks_condition(
                            s.as_ref(), &d.condition_id,
                        ))
                        .unwrap_or(true)
                {
                    continue;
                }
                if copybot_hot::lanes::needs_book(&lane) {
                    let mut w = watch_tokens.lock().unwrap();
                    if !w.iter().any(|t| t == &d.token_id) {
                        w.push(d.token_id.clone());
                        while w.len() > copybot_hot::book::WATCH_HARD_MAX {
                            w.remove(0);
                        }
                    }
                }
                runtime::exits::cancel_rests_on_leader_exit(
                    &d, &resting_book, &creds, &http, &clob,
                    &auth_addr, &emitter, &signal_guard,
                );
                let key = FillKey {
                    tx: raw.hash.to_ascii_lowercase(),
                    token: d.token_id.clone(),
                    side: d.side,
                    size_micro: (d.fill_size * 1e6) as u64,
                };
                if !book.first_seen(&raw.source, key, None) {
                    continue;
                }
                if d.side == 0 {
                    let his_now = control
                        .lock()
                        .unwrap()
                        .observe_his_fill(
                            &lane.cfg.name,
                            &d.token_id,
                            d.side,
                            d.fill_size,
                        );
                    lane.set_his(&d.token_id, his_now);
                }
                let mut progress = copybot_hot::signal_guard::Progress {
                    his_filled: d.fill_size,
                    our_copied: 0.0,
                };
                if d.side == 0 {
                    if let Some(guard) = &signal_guard {
                        match guard
                            .observe(
                                &d.salt,
                                &d.condition_id,
                                &lane.cfg.name,
                                d.fill_size,
                                d.order_size,
                                copybot_hot::ledger::now_secs(),
                            )
                        {
                            Ok(p) => progress = p,
                            Err(reason) => {
                                emitter
                                    .emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "signal_guard_skip", "lane" : lane
                                            .cfg.name, "tok" : d.token_id, "condition" : hex::encode(d
                                            .condition_id), "his_order_id" : hex::encode(d.salt), "why"
                                            : format!("{reason:?}"), }
                                        ),
                                    );
                                continue;
                            }
                        }
                        if let Err(reason) = guard
                            .check(
                                &d.salt,
                                &d.condition_id,
                                &lane.cfg.name,
                                &d.token_id,
                                copybot_hot::ledger::now_secs(),
                            )
                        {
                            emitter
                                .emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "signal_guard_skip", "lane" : lane
                                        .cfg.name, "tok" : d.token_id, "condition" : hex::encode(d
                                        .condition_id), "why" : format!("{reason:?}"), }
                                    ),
                                );
                            if matches!(
                                reason, copybot_hot::signal_guard::Refusal::Persistence(_)
                            ) {
                                latch_buys(
                                    &lane,
                                    &incidents,
                                    "persistence",
                                    "signal guard cannot persist",
                                );
                            }
                            continue;
                        }
                    }
                }
                let decided = router.decide(lane_ix, &d, progress);
                let lane_name = lane.cfg.name.clone();
                if d.side == 1 {
                    let his_now = control
                        .lock()
                        .unwrap()
                        .observe_his_fill(&lane_name, &d.token_id, d.side, d.fill_size);
                    lane.set_his(&d.token_id, his_now);
                }
                match decided {
                    Err(skip) => {
                        *control
                            .lock()
                            .unwrap()
                            .skips
                            .entry(format!("{skip:?}"))
                            .or_insert(0) += 1;
                        emitter
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "skip", "lane" : lane_name, "why" :
                                    format!("{skip:?}"), "src" : raw.source, "tok" : & d
                                    .token_id[..d.token_id.len().min(14)], "side" : if d.side ==
                                    0 { "BUY" } else { "SELL" }, "px" : d.price, "his_fill" : d
                                    .fill_size, "his_order" : d.order_size, "his_order_id" :
                                    hex::encode(d.salt), "progress_committed" : d.side == 0, }
                                ),
                            );
                    }
                    Ok(mut intent) => {
                        if intent.side == 0 {
                            let pol = lane.policy();
                            let deployed = lane.state.open_usd.load(Ordering::Relaxed)
                                as f64 / MICRO;
                            // Router.decide has already included this intent in open_usd.
                            // Subtract its own reservation exactly once before the cash gate.
                            let cost = intent.budget_usd as f64 / MICRO;
                            let cash_cost = intent.usd as f64 / MICRO;
                            let available = copybot_hot::budget::lane_available(
                                pol.seed_usd,
                                (deployed - cost).max(0.0),
                            );
                            let (free_cash, cash_age) = physical_wallet
                                .lock()
                                .ok()
                                .map(|w| (
                                    w.cash,
                                    w.fetched_at.map(|t| copybot_hot::ledger::now_secs() - t),
                                ))
                                .unwrap_or((None, None));
                            // Dry is a *counterfactual* $100-budget
                            // detection/strategy test, not a wallet-cash gate.
                            // Live POST eligibility still requires fresh real
                            // cash and is never inferred from that bankroll.
                            let affordable = if live {
                                copybot_hot::budget::buy_fits_aged_with_cash_cost(
                                    available, free_cash, cost, cash_cost, cash_age,
                                )
                            } else {
                                copybot_hot::budget::buy_fits_virtual(
                                    available, cost, cash_cost,
                                )
                            };
                            if !affordable {
                                *control
                                    .lock()
                                    .unwrap()
                                    .skips
                                    .entry("NoCash".to_string())
                                    .or_insert(0) += 1;
                                emitter
                                    .emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "skip", "lane" : lane_name, "why" :
                                            "NoCash", "src" : raw.source, "tok" : & intent
                                            .token_id[..intent.token_id.len().min(14)], "cost_usd" :
                                            cost, "lane_available_usd" : available, "lane_seed_usd" :
                                            pol.seed_usd, "lane_deployed_usd" : deployed,
                                            "fee_reserve_usd" : cost - cash_cost,
                                            "physical_cash_required_usd" : cash_cost,
                                            "physical_cash" : free_cash,
                                            "cash_basis": if live {"fresh_physical"} else {"virtual_bankroll"},
                                            "physical_cash_verdict": if live {"refused"} else {"not_evaluated"},
                                            }
                                        ),
                                    );
                                continue;
                            }
                        }
                        let intended_frac = match lane.cfg.sizing {
                            copybot_hot::lanes::Sizing::Pct(p) => p,
                            _ => 0.0,
                        };
                        let overcopied = copybot_hot::firerate::is_overcopy(
                            progress.his_filled,
                            progress.our_copied,
                            intended_frac,
                            copybot_hot::firerate::OVERCOPY_TOLERANCE,
                        );
                        if intent.side == 0
                            && fire_rate.record(lane_ix, copybot_hot::ledger::now_secs())
                            && !overcopied
                        {
                            emitter
                                .emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "fire_rate_corroborated", "lane" :
                                        lane_name, "tok" : intent.token_id, "in_window" : fire_rate
                                        .count(lane_ix, copybot_hot::ledger::now_secs()),
                                        "his_filled" : progress.his_filled, "our_copied" : progress
                                        .our_copied, "intended_frac" : intended_frac, "note" :
                                        "burst rate breached but our size tracks his fills — NOT halted",
                                        }
                                    ),
                                );
                        }
                        if intent.side == 0 && overcopied
                            && fire_rate.count(lane_ix, copybot_hot::ledger::now_secs())
                                >= fire_rate.limit()
                        {
                            latch_buys(
                                &lane,
                                &incidents,
                                "fire_rate",
                                "buy rate breached AND we hold more than his fills justify",
                            );
                            eprintln!(
                                "[{lane_name}] FIRE-RATE HALT: >{} buys in {}s — buys stopped, exits still allowed",
                                fire_rate.limit(), copybot_hot::firerate::WINDOW_SECS
                            );
                            emitter
                                .emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "fire_rate_halt", "lane" :
                                        lane_name, "limit" : fire_rate.limit(), "window_s" :
                                        copybot_hot::firerate::WINDOW_SECS, "in_window" : fire_rate
                                        .count(lane_ix, copybot_hot::ledger::now_secs()), "tok" :
                                        intent.token_id, "his_filled" : progress.his_filled,
                                        "our_copied" : progress.our_copied, "intended_frac" :
                                        intended_frac, "note" :
                                        "BUYS halted; SELLS continue so we can still follow him out",
                                        }
                                    ),
                                );
                            continue;
                        }
                        let (his_first_tranche, his_order_done, his_cum_filled) = if intent
                            .side == 1
                        {
                            let cum = sell_tranches.observe(&d.salt, d.fill_size);
                            let (first, done) = copybot_hot::lanes::sell_tranche_flags(
                                cum,
                                d.fill_size,
                                d.order_size,
                            );
                            (first, done, cum)
                        } else {
                            (false, false, progress.his_filled)
                        };
                        intent.route = resolve_route(
                            &intent,
                            &lane.cfg,
                            books.top(&intent.token_id),
                            d.price,
                            progress.our_copied <= 0.0,
                            his_first_tranche,
                            his_order_done,
                            lane.state.rest_buys.load(Ordering::Relaxed),
                            lane.state.rest_sells.load(Ordering::Relaxed),
                        );
                        let (limit, order_type) = match intent.route {
                            Route::Take => (intent.limit, "FAK"),
                            Route::RestInFront { limit, .. } => (limit, "GTC"),
                        };
                        let (ma, ta) = amounts(limit, intent.shares, intent.side);
                        let salt = safe_salt(now_ms() ^ (t0.elapsed().as_nanos() << 20));
                        let ord = Order {
                            salt,
                            maker: funder.clone(),
                            signer: order_signer(sig_type, &funder, &signer_addr).to_owned(),
                            token_id: intent.token_id.clone(),
                            maker_amount: ma,
                            taker_amount: ta,
                            side: intent.side,
                            signature_type: sig_type,
                            timestamp: now_ms(),
                            neg_risk: raw.to_neg_risk,
                        };
                        let sig = pk.as_ref().map(|k| ord.sign_for_type(k));
                        let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                        response_times
                            .push(copybot_hot::response_times::Sample {
                                t: copybot_hot::ledger::now_secs(),
                                lane: lane_name.clone(),
                                token: intent.token_id.clone(),
                                side: intent.side,
                                us: elapsed_us,
                            });
                        let ingest_lag_us = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos())
                            .unwrap_or(0))
                            .saturating_sub(raw.seen_ns) / 1_000;
                        let fire_ev = serde_json::json!(
                            { "t" : now_ms(), "ev" : if live { "fire" } else {
                            "would_fire" }, "ingest_lag_us" : ingest_lag_us, "lane" :
                            lane_name, "src" : raw.source, "tx" : raw.hash, "tok" :
                            intent.token_id, "condition" : hex::encode(d.condition_id),
                            "his_order_id" : hex::encode(d.salt), "side" : if intent.side
                            == 0 { "BUY" } else { "SELL" }, "shares" : intent.shares,
                            "limit" : limit, "route" : match intent.route { Route::Take
                            => serde_json::json!("take"), Route::RestInFront { limit,
                            room_ticks } => serde_json::json!({ "rest" : limit,
                            "room_ticks" : room_ticks }), }, "usd" : intent.usd as f64 /
                            MICRO, "budget_usd" : intent.budget_usd as f64 / MICRO,
                            "his_px" : d.price, "pad_c" : ((limit - d.price) *
                            10000.0).round() / 100.0, "his_fill" : d.fill_size,
                            "his_order" : d.order_size, "his_remaining" : intent
                            .his_remaining, "exec" : format!("{:?}", intent.execution),
                            "signal_to_ready_us" : elapsed_us, "queue_us" : queue_us,
                            "cash_basis": if live {"fresh_physical"} else {"virtual_bankroll"}, }
                        );
                        if !live {
                            emitter.emit(fire_ev.clone());
                        }
                        if live {
                            // FAK submissions may never use legacy limit/zero-fee
                            // accounting. Without a successfully replayed owner,
                            // refuse BEFORE HTTP POST and latch the buy side.
                            if order_type == "FAK" {
                                if let Err(reason) =
                                    runtime::accounting::fak_owner_submit_ready(fak_journal.as_ref())
                                {
                                    latch_buys(&lane, &incidents, "fak_owner_unavailable",
                                        "FAK execution needs an intact durable money journal before POST");
                                    emitter.emit(serde_json::json!({
                                        "t":now_ms(), "ev":"submit_precondition_failed",
                                        "lane":lane_name, "tok":intent.token_id,
                                        "why":format!("FAK owner unavailable: {reason:?}"),
                                        "buys_halted":true,
                                    }));
                                    continue;
                                }
                            }
                            let Some(sig) = sig else {
                                latch_buys(
                                    &lane,
                                    &incidents,
                                    "signature",
                                    "missing order signature",
                                );
                                emitter
                                    .emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "submit_precondition_failed",
                                            "lane" : lane_name, "tok" : intent.token_id, "why" :
                                            "missing order signature", "buys_halted" : true, }
                                        ),
                                    );
                                continue;
                            };
                            let Some(credential) = creds.as_ref().as_ref() else {
                                latch_buys(
                                    &lane,
                                    &incidents,
                                    "credentials",
                                    "API credentials unavailable",
                                );
                                emitter
                                    .emit(
                                        serde_json::json!(
                                            { "t" : now_ms(), "ev" : "submit_precondition_failed",
                                            "lane" : lane_name, "tok" : intent.token_id, "why" :
                                            "missing L2 credentials", "buys_halted" : true, }
                                        ),
                                    );
                                continue;
                            };
                            let owner = credential.key.clone();
                            let body = json_body(&ord, &sig, &owner, order_type);
                            let body = match serde_json::to_string(&body) {
                                Ok(s) => s,
                                Err(e) => {
                                    latch_buys(
                                        &lane,
                                        &incidents,
                                        "precondition",
                                        "submit precondition failed",
                                    );
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "submit_precondition_failed",
                                                "lane" : lane_name, "tok" : intent.token_id, "why" :
                                                format!("order body serialize failed: {e}"), "buys_halted" :
                                                true, }
                                            ),
                                        );
                                    continue;
                                }
                            };
                            let hdrs = match copybot_hot::auth::l2_headers(
                                &auth_addr,
                                credential,
                                copybot_hot::auth::now_secs(),
                                "POST",
                                "/order",
                                Some(&body),
                            ) {
                                Ok(h) => h,
                                Err(e) => {
                                    latch_buys(
                                        &lane,
                                        &incidents,
                                        "precondition",
                                        "submit precondition failed",
                                    );
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "submit_precondition_failed",
                                                "lane" : lane_name, "tok" : intent.token_id, "why" :
                                                format!("L2 header build failed: {e}"), "buys_halted" :
                                                true, }
                                            ),
                                        );
                                    continue;
                                }
                            };
                            let mut buy_wal_row: Option<copybot_hot::pending::Pending> = None;
                            if intent.side == 0 {
                                let Some(guard) = &signal_guard else {
                                    latch_buys(
                                        &lane,
                                        &incidents,
                                        "precondition",
                                        "submit precondition failed",
                                    );
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "submit_precondition_failed",
                                                "lane" : lane_name, "tok" : intent.token_id, "why" :
                                                "live signal guard missing", "buys_halted" : true, }
                                            ),
                                        );
                                    continue;
                                };
                                let p = copybot_hot::pending::Pending {
                                    lane: lane_name.clone(),
                                    token: intent.token_id.clone(),
                                    side: intent.side,
                                    order_hash: hex::encode(ord.digest()),
                                    shares: intent.shares,
                                    limit,
                                    ts: copybot_hot::ledger::now_secs(),
                                    why: "submitting".into(),
                                    resting: order_type == "GTC",
                                };
                                let row = copybot_hot::pending::PendingLog::row_bytes(&p);
                                if let Err(reason) = guard
                                    .commit_with(
                                        &d.salt,
                                        &d.condition_id,
                                        &lane_name,
                                        &intent.token_id,
                                        intent.shares,
                                        copybot_hot::ledger::now_secs(),
                                        &wal,
                                        Some(&row),
                                    )
                                {
                                    if matches!(
                                        reason, copybot_hot::signal_guard::Refusal::Persistence(_)
                                    ) {
                                        latch_buys(
                                            &lane,
                                            &incidents,
                                            "persistence",
                                            "signal guard commit cannot persist",
                                        );
                                    }
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "signal_guard_skip", "lane" :
                                                lane_name, "tok" : intent.token_id, "condition" :
                                                hex::encode(d.condition_id), "why" : format!("{reason:?}"),
                                                }
                                            ),
                                        );
                                    continue;
                                }
                                pending_log.lock().unwrap().apply_record(p.clone());
                                buy_wal_row = Some(p);
                            }
                            emitter.emit(fire_ev);
                            control.lock().unwrap().record_fire(&lane_name);
                            let (paths, c, e2) = (
                                order_paths.clone(),
                                clob.clone(),
                                emitter.clone(),
                            );
                            let creds_c = creds.clone();
                            let addr_c = auth_addr.clone();
                            let lat_c = latency.clone();
                            let c2 = control.clone();
                            let ln = lane_name.clone();
                            let tok_for_resp = intent.token_id.clone();
                            let side_for_resp = intent.side;
                            let limit_for_resp = limit;
                            let shares_for_resp = intent.shares;
                            let his_price_for_resp = d.price;
                            let rb_c = resting_book.clone();
                            let wake_c = sweep_tx.clone();
                            let router_c = router.clone();
                            let ord_c = ord.clone();
                            let pk_c = pk;
                            let owner_c = owner.clone();
                            let otype_c = order_type;
                            let full_holding = lane.holding(&intent.token_id);
                            let prov_his_pos = lane
                                .his_pos
                                .lock()
                                .unwrap()
                                .get(&intent.token_id)
                                .copied();
                            let anchor_level = levels.size_at(&intent.token_id, d.price);
                            let anchor_his = if anchor_level.is_some() {
                                (d.order_size - his_cum_filled).max(0.0)
                            } else {
                                0.0
                            };
                            let anchor_printed = prints
                                .cumulative_at(&intent.token_id, d.price);
                            let anchor_contested = match (anchor_level, anchor_his) {
                                (Some(lvl), his) if his > 0.0 => {
                                    lvl
                                        > his * (1.0 + copybot_hot::restwatch::DEFICIT_TOLERANCE)
                                }
                                _ => true,
                            };
                            let anchor_price = d.price;
                            let prov_his_order = d.order_size;
                            let prov_his_filled = progress.his_filled;
                            let prov_our_copied = progress.our_copied;
                            let pend_c = pending_log.clone();
                            let fak_journal_c = fak_journal.clone();
                            let sg_c = signal_guard.clone();
                            let inc_c = incidents.clone();
                            let salt_c = d.salt;
                            let cond_c = d.condition_id;
                            let hash_c = hex::encode(ord.digest());
                            tokio::spawn(async move {
                                use copybot_hot::race_send::Outcome;
                                let (outcome, results) = match submit_tracked(
                                        &pend_c,
                                        &e2,
                                        &copybot_hot::provenance::Provenance::mirrored(
                                            if side_for_resp == 1 {
                                                copybot_hot::provenance::Origin::MirrorSell
                                            } else {
                                                copybot_hot::provenance::Origin::MirrorBuy
                                            },
                                            &ln,
                                            &tok_for_resp,
                                            side_for_resp,
                                            shares_for_resp,
                                            limit_for_resp,
                                            full_holding,
                                            prov_his_pos,
                                            Some(prov_his_order),
                                            Some(prov_his_filled),
                                            Some(prov_our_copied),
                                        ),
                                        hash_c.clone(),
                                        otype_c == "GTC",
                                        &paths,
                                        &format!("{c}/order"),
                                        &body,
                                        &hdrs,
                                        Duration::from_secs(10),
                                        buy_wal_row.is_some(),
                                    )
                                    .await
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        eprintln!("[{ln}] REFUSING TO SUBMIT: {e}");
                                        e2.emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "submit_refused", "lane" : ln,
                                                "tok" : & tok_for_resp[..tok_for_resp.len().min(14)], "why"
                                                : e }
                                            ),
                                        );
                                        halt_buys_on_persistence_fault_with(
                                            &c2,
                                            &router_c,
                                            Some(&pend_c),
                                            &inc_c,
                                        );
                                        return;
                                    }
                                };
                                let booked = match &outcome {
                                    Outcome::Matched { body, winner, ms } => {
                                        let (durable, filled) = if otype_c == "FAK" {
                                            runtime::accounting::book_matched_fak_race(
                                                fak_journal_c.as_ref(),
                                                &c2, &pend_c, &router_c, &inc_c, &e2,
                                                &hash_c, &ln, &outcome, &results, paths.len(),
                                            )
                                        } else {
                                            // GTC/resting follow-on paths are not yet
                                            // journal-migrated. Do not mislabel them as A4.
                                            book_and_resolve(
                                                &c2, &pend_c, &router_c, &inc_c, &e2,
                                                &ln, &tok_for_resp, side_for_resp,
                                                limit_for_resp, body,
                                                Some(his_price_for_resp), &hash_c, true,
                                            )
                                        };
                                        if side_for_resp == 0 && durable && otype_c != "GTC" {
                                            let unfilled = shares_for_resp - filled;
                                            if unfilled > 1e-9 {
                                                if let Some(g) = sg_c.as_ref() {
                                                    match g
                                                        .release(
                                                            &salt_c,
                                                            &cond_c,
                                                            &ln,
                                                            unfilled,
                                                            copybot_hot::ledger::now_secs(),
                                                        )
                                                    {
                                                        Ok(_) => {
                                                            e2.emit(
                                                                serde_json::json!(
                                                                    { "t" : now_ms(), "ev" : "commit_released", "lane" : ln,
                                                                    "tok" : tok_for_resp, "ordered" : shares_for_resp, "filled"
                                                                    : filled, "released" : unfilled, "why" :
                                                                    "partial fill — the \
venue killed the remainder" }
                                                                ),
                                                            )
                                                        }
                                                        Err(e) => {
                                                            eprintln!(
                                                                "[{ln}] could \
not release {unfilled} unfilled shares: {e:?} — this order stays under-copied"
                                                            )
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if side_for_resp == 1 {
                                            let _ = wake_c.send(tok_for_resp.clone());
                                        }
                                        serde_json::json!(
                                            { "outcome" : "matched", "winner" : winner, "ms" : ms }
                                        )
                                    }
                                    Outcome::Resting { order_id, winner, ms, .. } => {
                                        rb_c.add(copybot_hot::resting::Resting {
                                            order_id: order_id.clone(),
                                            lane: ln.clone(),
                                            token: tok_for_resp.clone(),
                                            side: side_for_resp,
                                            limit: limit_for_resp,
                                            shares: shares_for_resp,
                                            placed: copybot_hot::ledger::now_secs(),
                                            salt: salt_c,
                                            condition: cond_c,
                                            his_price: anchor_price,
                                            his_remaining: anchor_his,
                                            printed_at_place: anchor_printed,
                                            contested_at_place: anchor_contested,
                                        });
                                        serde_json::json!(
                                            { "outcome" : "resting", "order_id" : order_id, "winner" :
                                            winner, "ms" : ms }
                                        )
                                    }
                                    Outcome::DuplicateOnly { winner } => {
                                        c2.lock()
                                            .unwrap()
                                            .ambiguous
                                            .push(
                                                serde_json::json!(
                                                    { "lane":& ln, "token":& tok_for_resp, "why" :
                                                    "all paths returned duplicate" }
                                                ),
                                            );
                                        let _ = pend_c
                                            .lock()
                                            .unwrap()
                                            .record(copybot_hot::pending::Pending {
                                                lane: ln.clone(),
                                                token: tok_for_resp.clone(),
                                                side: side_for_resp,
                                                order_hash: hash_c.clone(),
                                                shares: shares_for_resp,
                                                limit: limit_for_resp,
                                                ts: copybot_hot::ledger::now_secs(),
                                                why: "duplicate_only".into(),
                                                resting: otype_c == "GTC",
                                            });
                                        serde_json::json!(
                                            { "outcome" : "duplicate_only", "winner" : winner }
                                        )
                                    }
                                    Outcome::Rejected { body } => {
                                        // A refused HTTP outcome is never an inventory
                                        // booking source. Old book_response could book a
                                        // 4xx proxy body that echoed matched fields.
                                        if let Some(book) = c2.lock().unwrap().ledger.lanes.get_mut(&ln) {
                                            book.rejected += 1;
                                        }
                                        if side_for_resp == 0 {
                                            if let Some(g) = sg_c.as_ref() {
                                                if let Err(e) = g
                                                    .release(
                                                        &salt_c,
                                                        &cond_c,
                                                        &ln,
                                                        shares_for_resp,
                                                        copybot_hot::ledger::now_secs(),
                                                    )
                                                {
                                                    eprintln!(
                                                        "[{ln}] could not release a \
                                                            rejected commitment: {e:?}"
                                                    );
                                                }
                                            }
                                            let held = router_c
                                                .snapshot()
                                                .iter()
                                                .find(|l| l.cfg.name == ln)
                                                .map(|l| l.holding(&tok_for_resp))
                                                .unwrap_or(0.0);
                                            if held <= 1e-9 {
                                                router_c.release_by_name(&tok_for_resp, &ln);
                                            }
                                        }
                                        if side_for_resp == 1 {
                                            let rescued = rescue_exit(
                                                    body,
                                                    ord_c,
                                                    pk_c,
                                                    &owner_c,
                                                    otype_c,
                                                    shares_for_resp,
                                                    full_holding,
                                                    limit_for_resp,
                                                    &paths,
                                                    &c,
                                                    creds_c.as_ref().as_ref(),
                                                    &addr_c,
                                                    &ln,
                                                    &tok_for_resp,
                                                    &c2,
                                                    &router_c,
                                                    &e2,
                                                    &wake_c,
                                                    &pend_c,
                                                    &inc_c,
                                                    fak_journal_c.as_ref(),
                                                )
                                                .await;
                                            serde_json::json!(
                                                { "outcome" : "rejected", "rescued" : rescued }
                                            )
                                        } else {
                                            serde_json::json!({ "outcome" : "rejected" })
                                        }
                                    }
                                    Outcome::Ambiguous { detail } => {
                                        c2.lock()
                                            .unwrap()
                                            .ambiguous
                                            .push(
                                                serde_json::json!(
                                                    { "lane":& ln, "token":& tok_for_resp, "why" :
                                                    format!("no venue verdict: {detail}") }
                                                ),
                                            );
                                        let _ = pend_c
                                            .lock()
                                            .unwrap()
                                            .record(copybot_hot::pending::Pending {
                                                lane: ln.clone(),
                                                token: tok_for_resp.clone(),
                                                side: side_for_resp,
                                                order_hash: hash_c.clone(),
                                                shares: shares_for_resp,
                                                limit: limit_for_resp,
                                                ts: copybot_hot::ledger::now_secs(),
                                                why: format!("ambiguous:{detail}"),
                                                resting: otype_c == "GTC",
                                            });
                                        serde_json::json!(
                                            { "outcome" : "ambiguous", "detail" : detail, "rescued" :
                                            false }
                                        )
                                    }
                                    Outcome::NoResponse => {
                                        c2.lock()
                                            .unwrap()
                                            .ambiguous
                                            .push(
                                                serde_json::json!(
                                                    { "lane":& ln, "token":& tok_for_resp, "why" :
                                                    "no response from any path" }
                                                ),
                                            );
                                        let _ = pend_c
                                            .lock()
                                            .unwrap()
                                            .record(copybot_hot::pending::Pending {
                                                lane: ln.clone(),
                                                token: tok_for_resp.clone(),
                                                side: side_for_resp,
                                                order_hash: hash_c.clone(),
                                                shares: shares_for_resp,
                                                limit: limit_for_resp,
                                                ts: copybot_hot::ledger::now_secs(),
                                                why: "no_response".into(),
                                                resting: otype_c == "GTC",
                                            });
                                        serde_json::json!({ "outcome" : "no_response" })
                                    }
                                };
                                {
                                    let per: Vec<(String, f64)> = results
                                        .iter()
                                        .map(|r| (r.path.clone(), r.ms))
                                        .collect();
                                    let win = results
                                        .iter()
                                        .map(|r| r.ms)
                                        .fold(f64::INFINITY, f64::min);
                                    if win.is_finite() {
                                        lat_c.record_order(win, &per);
                                    }
                                }
                                let winner_path = match &outcome {
                                    Outcome::Matched { winner, .. }
                                    | Outcome::Resting { winner, .. }
                                    | Outcome::DuplicateOnly { winner } => Some(winner.clone()),
                                    _ => None,
                                };
                                let landed = matches!(
                                    & outcome, Outcome::Matched { .. } | Outcome::Resting { .. }
                                    | Outcome::DuplicateOnly { .. }
                                );
                                e2.emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "clob_resp", "lane" : ln, "tok" :
                                        tok_for_resp, "side" : if side_for_resp == 0 { "BUY" } else
                                        { "SELL" }, "limit" : limit_for_resp, "ok" : landed, "race"
                                        : booked, "paths" : results.iter().map(| r | { let role = if
                                        Some(& r.path) == winner_path.as_ref() { "winner" } else if
                                        landed && copybot_hot::race_send::looks_duplicate(& r.body)
                                        { "dup_loser" } else { "error" }; serde_json::json!({ "path"
                                        : r.path, "ms" : r.ms, "http" : r.http, "role" : role,
                                        "body" : r.body.chars().take(120).collect::< String > () })
                                        }).collect::< Vec < _ >> (), }
                                    ),
                                );
                            });
                        }
                    }
                }
            }
        }
    }
}
