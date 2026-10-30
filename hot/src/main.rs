#![cfg_attr(test, allow(non_snake_case))]
mod runtime;
#[path = "feed_loop.rs"]
mod feed_loop;
use runtime::dashboard_model::{DashboardLane, PhysicalWallet, wallet_equity_pnl};
#[cfg(test)]
use runtime::dashboard_model::marked_position_value;
#[cfg(test)]
use runtime::reconcile::recovery_target_shares;
use runtime::accounting::{book_and_resolve, halt_buys_on_persistence_fault_with};
use runtime::telemetry::{now_ms, Emitter};
use runtime::execution::submit_tracked;
use runtime::control::{control_task, latch_buys};
use runtime::operator::{actor_of, handle_arm_post, handle_flatten_post, handle_wallet_patch, seed_operator_file};
use runtime::exits::{rescue_exit, rest_verdict};
use runtime::http::{mutation_refusal, route_refusal};
#[cfg(test)]
use runtime::http::{header_of, ROUTES};
#[cfg(test)]
use runtime::telemetry::RATE_LIMITED;
use copybot_hot::book::BookCache;
use copybot_hot::control::Control;
use copybot_hot::ledger::Ledger;
use copybot_hot::calldata::{decode_all, participants};
use copybot_hot::config::{addr20, Root};
use copybot_hot::feeds::{FeedConfig, FeedStats, RawTx};
use copybot_hot::lanes::{resolve_route, Route, Router, MICRO};
use copybot_hot::order::{amounts, json_body, order_signer, safe_salt, Order};
use copybot_hot::operator_access::{OperatorAccess, OperatorAction};
use copybot_hot::race::{FillKey, RaceBook};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    eprintln!("Abomination81 Copybot — execution engine");
    let operator_access = match OperatorAccess::from_environment() {
        Ok(access) => Arc::new(access),
        Err(error) => {
            eprintln!("OPERATOR CONFIG ERROR: {error}");
            std::process::exit(2);
        }
    };
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "deploy/copybot2.toml".into());
    let root = match Root::load(&cfg_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("CONFIG ERROR: {e}");
            std::process::exit(2);
        }
    };
    let lanes = match root.build_lanes() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("CONFIG ERROR: {e}");
            std::process::exit(2);
        }
    };
    let shadow = root.bot.mode == "shadow";
    let live = root.bot.mode == "live" || shadow;
    let funder = root.bot.funder.clone();
    let signer = root.bot.signer.clone();
    let sig_type = root.bot.signature_type;
    if addr20(&funder).is_err() || addr20(&signer).is_err() {
        eprintln!("CONFIG ERROR: funder/signer must be 20-byte hex addresses");
        std::process::exit(2);
    }
    let _execution_lease = if live && !shadow {
        match copybot_hot::lease::ExecutionLease::acquire_for_funder(&funder) {
            Ok(lease) => {
                eprintln!("[lease] acquired {}", lease.path.display());
                Some(lease)
            }
            Err(e) => {
                eprintln!("REFUSING TO START: {e}");
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    let pk_hex = std::env::var("PRIVATE_KEY").unwrap_or_default();
    let pk = match hex::decode(pk_hex.trim_start_matches("0x")) {
        Ok(b) if b.len() == 32 => {
            let mut k = [0u8; 32];
            k.copy_from_slice(&b);
            Some(k)
        }
        _ => None,
    };
    let pk = match (shadow, pk) {
        (true, None) => {
            eprintln!(
                "[shadow] no PRIVATE_KEY — signing with a THROWAWAY key so orders \
actually reach the venue and are refused. This key controls no funds."
            );
            let mut k = [0u8; 32];
            for (i, b) in k.iter_mut().enumerate() {
                *b = (i as u8).wrapping_add(1);
            }
            Some(k)
        }
        (_, other) => other,
    };
    let signer_addr: String = match pk.as_ref().map(copybot_hot::auth::address_from_key)
    {
        Some(Ok(a)) => {
            if !a.eq_ignore_ascii_case(&signer) {
                eprintln!(
                    "[auth] ⚠️  config signer {signer} does NOT own PRIVATE_KEY \
({a}) — using the DERIVED owner address for authentication and signing. Fix the config."
                );
            }
            a
        }
        Some(Err(e)) => {
            eprintln!("[auth] cannot derive signer address: {e}");
            signer.clone()
        }
        None => signer.clone(),
    };
    if live && !shadow && pk.is_none() {
        eprintln!("REFUSING TO START: mode=live but PRIVATE_KEY is unset or malformed");
        std::process::exit(2);
    }
    let emitter = Arc::new(Emitter::new(root.bot.events_path.clone(), !live || shadow));
    let prints = Arc::new(copybot_hot::book::TradePrints::new());
    let levels = Arc::new(copybot_hot::book::Levels::new());
    let leader_pnl: Arc<
        std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>,
    > = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let custody_report: Arc<std::sync::Mutex<Option<copybot_hot::custody::Report>>> = Arc::new(
        std::sync::Mutex::new(None),
    );
    let reanchor_pulse: runtime::reconcile::ReanchorPulse =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let rt_path = format!("{}.response_times", root.bot.control_path);
    let response_times = Arc::new(
        copybot_hot::response_times::Ring::with_file(&rt_path),
    );
    if let Err(e) = response_times.compact() {
        eprintln!(
            "[rt] could not compact {rt_path}: {e} — chart still works, file grows"
        );
    }
    let router = Arc::new(Router::new(lanes));
    let book = Arc::new(RaceBook::new());
    let stats = Arc::new(FeedStats::default());
    let feeds: Vec<FeedConfig> = root
        .feed
        .iter()
        .map(|f| FeedConfig {
            name: f.name.clone(),
            url: f.url.clone(),
            sockets: f.sockets,
        })
        .collect();
    let total_sockets: usize = feeds.iter().map(|f| f.sockets).sum();
    emitter
        .emit(
            serde_json::json!(
                { "t" : now_ms(), "ev" : "boot", "mode" : root.bot.mode, "lanes" : router
                .snapshot().iter().map(| l | serde_json::json!({ "name" : l.cfg.name,
                "wallet" : format!("0x{}", hex::encode(l.cfg.wallet20)), "sizing" :
                format!("{:?}", l.policy().sizing), "execution" : format!("{:?}", l.cfg
                .execution), "daily_usd" : l.policy().caps.daily_usd, })).collect::< Vec
                < _ >> (), "feeds" : feeds.iter().map(| f | format!("{}x{}", f.name, f
                .sockets)).collect::< Vec < _ >> (), "sockets" : total_sockets, "note" :
                if shadow {
                "SHADOW — full live path, credentials forced OFF, every \
                             order will be rejected 401"
                } else if live { "LIVE — lanes still need arming in the control file" }
                else { "DRY — nothing will be submitted" }, }
            ),
        );
    let resting_book = Arc::new(copybot_hot::resting::RestingBook::new());
    let sell_tranches = Arc::new(copybot_hot::lanes::TrancheLedger::new(4096));
    let fire_rate = Arc::new(
        copybot_hot::firerate::FireRate::new(
            router.len(),
            copybot_hot::firerate::DEFAULT_LIMIT,
        ),
    );
    let incidents_path = format!("{}.incidents", root.bot.control_path);
    let incidents = Arc::new(
        std::sync::Mutex::new(copybot_hot::incidents::Incidents::open(&incidents_path)),
    );
    {
        let g = incidents.lock().unwrap();
        if let Some(e) = g.unreadable() {
            eprintln!(
                "[incidents] CANNOT READ {incidents_path}: {e} — EVERY lane is \
                       latched for buys until this is fixed (exits still mirror)"
            );
        } else if !g.is_empty() {
            for i in g.open_incidents() {
                eprintln!(
                    "[incidents] OPEN: {} — {} ({}) raised at {}; buys stay halted \
                           until cleared",
                    i.lane, i.why, i.kind, i.t
                );
            }
        }
    }
    let funding_path = format!("{}.funding", root.bot.control_path);
    let funding = Arc::new(
        std::sync::Mutex::new(copybot_hot::funding::Funding::open(&funding_path)),
    );
    match funding.lock().unwrap().basis() {
        Some(b) => eprintln!("[funding] declared external funding: ${b:.2}"),
        None => {
            eprintln!(
                "[funding] NO external funding declared — physical P&L will \
                           read UNKNOWN until you record a deposit (see run/*.funding)"
            )
        }
    }
    let wal_path = format!("{}.wal", root.bot.control_path);
    let wal = match copybot_hot::wal::Wal::open(&wal_path) {
        Ok(w) => Arc::new(w),
        Err(e) => {
            if live && !shadow {
                eprintln!("REFUSING TO START: write-ahead log unavailable: {e}");
                std::process::exit(2);
            }
            eprintln!("[wal] {e} — continuing without it (not live)");
            Arc::new(copybot_hot::wal::Wal::disabled())
        }
    };
    let mut wal_recovery = copybot_hot::wal::WalRecovery::new(
        &wal_path,
        &["signal_guard", "pending"],
    );
    let signal_guard = if live && !shadow {
        match copybot_hot::signal_guard::SignalGuard::open_absorbing(
            &root.bot.signal_guard_path,
            Some(&wal_path),
            copybot_hot::ledger::now_secs(),
        ) {
            Ok((guard, rows)) => {
                wal_recovery.absorbed("signal_guard", rows);
                Some(Arc::new(guard))
            }
            Err(e) => {
                eprintln!("REFUSING TO START: signal guard unavailable: {e}");
                std::process::exit(2);
            }
        }
    } else {
        wal_recovery.absorbed("signal_guard", 0);
        None
    };
    let books = Arc::new(BookCache::new());
    let watch_tokens: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(
        std::sync::Mutex::new(Vec::new()),
    );
    {
        tokio::spawn(
            copybot_hot::book::run_book_feed_multi(
                books.clone(),
                watch_tokens.clone(),
                Some(prints.clone()),
                Some(levels.clone()),
                3,
            ),
        );
    }
    let (tx, rx) = mpsc::unbounded_channel::<RawTx>();
    let watched_addrs = Arc::new(copybot_hot::feeds::WatchedAddresses::new());
    let (_handles, feed_registry) = copybot_hot::feeds::spawn_all(
        &feeds,
        tx.clone(),
        stats.clone(),
        watched_addrs.clone(),
    );
    let feed_registry = Arc::new(feed_registry);
    if root.bot.recycle_secs > 0 {
        let (reg, ev) = (feed_registry.clone(), emitter.clone());
        let every = root.bot.recycle_secs;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                let ranked = reg.wins_snapshot();
                let losers = reg.score_and_recycle(2, 20);
                if !losers.is_empty() {
                    ev.emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "socket_recycle", "recycled" :
                            losers, "ranking" : ranked.iter().take(6).map(| (n, w) |
                            serde_json::json!({ "s" : n, "wins" : w })).collect::< Vec <
                            _ >> (), }
                        ),
                    );
                }
            }
        });
        eprintln!(
            "[feeds] socket scoring on: ranking every {every}s, protecting the top 2"
        );
    }
    let risk_cfgs: Vec<(String, copybot_hot::risk::RiskConfig)> = root
        .lane
        .iter()
        .map(|l| (l.name.clone(), l.risk.unwrap_or_default()))
        .collect();
    let ledger = Ledger::new(&root.bot.ledger_path, &risk_cfgs);
    let operator_path = format!("{}.operator", root.bot.control_path);
    if !std::path::Path::new(&operator_path).exists() {
        let seed = serde_json::json!(
            { "_howto" :
            "set armed=true to allow a lane to fire; the control plane can \
                       only ever take this away",
            "lanes" : risk_cfgs.iter().map(| (n, _) | (n.clone(), serde_json::json!({
            "armed" : false }))).collect::< serde_json::Map < _, _ >> () }
        );
        let body = serde_json::to_vec_pretty(&seed)
            .expect("operator seed is composed only of serializable JSON values");
        if let Err(e) = seed_operator_file(std::path::Path::new(&operator_path), &body) {
            if live && !shadow {
                eprintln!("REFUSING TO START: {e}");
                std::process::exit(2);
            }
            eprintln!("[control] WARN: {e}; all lanes remain fail-closed");
        }
    }
    let control = Arc::new(std::sync::Mutex::new(Control::new(ledger, &operator_path)));
    let merge_claims = Arc::new(
        std::sync::Mutex::new(copybot_hot::mergeclaim::MergeClaims::new()),
    );
    let pair_ledger = Arc::new(
        std::sync::Mutex::new(copybot_hot::pairledger::PairLedger::new()),
    );
    let wallets_path = format!("{}.wallets", root.bot.control_path);
    let registry = copybot_hot::wallets::WalletRegistry::load_or_seed(
        &wallets_path,
        root.wallet_specs(),
    );
    match registry.mint_missing_ids() {
        Ok(0) => {}
        Ok(n) => {
            eprintln!(
                "[laneid] minted and persisted {n} lane id(s) — from now on \
                            the stored value is authoritative, not the leader"
            )
        }
        Err(e) => {
            eprintln!(
                "[laneid] ⚠️ could not persist minted lane ids: {e} — they \
                             remain derived, so a leader edit would move them"
            )
        }
    }
    let dupes = registry.duplicate_ids();
    if !dupes.is_empty() {
        for (id, names) in &dupes {
            eprintln!(
                "[laneid] ⛔ id {id} is claimed by {} lanes: {}", names.len(), names
                .join(", ")
            );
        }
        if live && !shadow {
            eprintln!("[boot] ⛔ REFUSING TO START: duplicate lane identities");
            std::process::exit(2);
        }
    }
    control.lock().unwrap().ledger.lane_ids = registry.id_map();
    watched_addrs.set(registry.specs().iter().map(|w| w.leader.clone()));
    eprintln!(
        "[feeds] watching {} leader address(es) for merges alongside the exchanges",
        watched_addrs.snapshot().len()
    );
    {
        let mut c = control.lock().unwrap();
        let mut restored = 0usize;
        for spec in registry.specs() {
            if c.ledger.lanes.contains_key(&spec.name) {
                continue;
            }
            c.ledger.ensure_lane(&spec.name, spec.risk.unwrap_or_default());
            restored += 1;
        }
        if restored > 0 {
            eprintln!(
                "[ledger] restored {restored} runtime wallet book(s) before \
reconciliation — the pool is now fully known"
            );
        }
    }
    if let Some(why) = registry.load_fault() {
        eprintln!("[boot] ⛔ REFUSING TO START: {why}");
        let errpath = format!(
            "{}/errors.jsonl", std::path::Path::new(& root.bot.control_path).parent()
            .map(| p | p.to_string_lossy().into_owned()).unwrap_or_else(|| "run".into())
        );
        copybot_hot::errors::record(
            &errpath,
            &copybot_hot::errors::ErrorRow {
                t: copybot_hot::ledger::now_secs(),
                lane: String::new(),
                kind: "wallets".into(),
                detail: format!("wallet registry unusable: {why}"),
                human: format!(
                    "The bot REFUSED TO START because it could not read its wallet list. \
Nothing has been sold and no position has changed — but it is also not following anyone \
out right now, so treat this as urgent. {why}"
                ),
                severity: "stop".into(),
            },
        );
        if live && !shadow {
            std::process::exit(2);
        }
        eprintln!(
            "[boot] (continuing anyway: not live — this would be fatal in live)"
        );
    }
    if let Ok(reason) = std::env::var("RESET_REALISED") {
        let r = reason.trim();
        if r.is_empty() || r == "0" || r == "1" || r.eq_ignore_ascii_case("false")
            || r.eq_ignore_ascii_case("true")
        {
            eprintln!(
                "[ledger] RESET_REALISED refused: the value must BE the reason \
(written to the ledger). Got {r:?}. Example: RESET_REALISED=\"rebaselined after X\""
            );
        } else {
            let mut g = control.lock().unwrap();
            let newest_reset = g.ledger.newest_realised_reset_t();
            let now = copybot_hot::ledger::now_secs();
            if newest_reset.is_some_and(|t| now - t < 86_400) {
                eprintln!(
                    "[ledger] RESET_REALISED refused: a reset already happened in \
the last 24h. If this is deliberate, remove the env var, wait, or clear it manually — \
a reset that fires on every restart is how a P&L baseline silently rots."
                );
            } else {
                let names: Vec<String> = g.ledger.lanes.keys().cloned().collect();
                for n in &names {
                    let before = g.ledger.lanes[n].risk.realised_pnl;
                    g.ledger.realised_reset(n, r);
                    eprintln!(
                        "[ledger] {n}: realised RESET {before:+.2} -> 0.00 \
(positions untouched) — {r}"
                    );
                }
            }
        }
    }
    {
        let sidecar = format!("{}.continuity.json", root.bot.ledger_path);
        let verdict = control.lock().unwrap().ledger.verify_continuity(&sidecar);
        match &verdict {
            copybot_hot::ledger::ContinuityVerdict::Intact => {}
            copybot_hot::ledger::ContinuityVerdict::FirstRun => {
                eprintln!("[ledger] continuity: first run — recording the baseline");
            }
            copybot_hot::ledger::ContinuityVerdict::Broken(why) => {
                eprintln!("[ledger] ⛔ CONTINUITY BROKEN: {why}");
                for lane in router.snapshot().iter() {
                    latch_buys(
                        lane,
                        &incidents,
                        "continuity",
                        "ledger continuity broken",
                    );
                }
                emitter
                    .emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "ledger_continuity_broken", "why" :
                            why, "buys_halted" : true }
                        ),
                    );
                copybot_hot::errors::record(
                    &format!(
                        "{}/errors.jsonl", std::path::Path::new(& root.bot.control_path)
                        .parent().map(| p | p.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "run".into())
                    ),
                    &copybot_hot::errors::ErrorRow {
                        t: copybot_hot::ledger::now_secs(),
                        lane: String::new(),
                        kind: "ledger".into(),
                        detail: format!("continuity check failed: {why}"),
                        human: format!(
                            "The accounting file no longer contains the history it had at \
the last start ({why}). Profit, loss and position sizes are all derived from it, so \
buying is stopped until you confirm the change was intended. Selling still works. If you \
ran ledger_adjust or edited the file on purpose, this is expected — restart to accept \
the new baseline."
                        ),
                        severity: "stop".into(),
                    },
                );
            }
        }
        if let Err(e) = control.lock().unwrap().ledger.record_continuity(&sidecar) {
            eprintln!("[ledger] ⚠️  cannot record continuity baseline: {e}");
        }
        if let Some(c) = control.lock().unwrap().ledger.continuity() {
            if c.lines > copybot_hot::ledger::LEDGER_GROWTH_ALARM_LINES {
                eprintln!(
                    "[ledger] ⚠️  {} lines ({} bytes) — past the {} line mark where \
snapshot/checkpointing is worth its risk",
                    c.lines, c.bytes, copybot_hot::ledger::LEDGER_GROWTH_ALARM_LINES
                );
            }
        }
    }
    {
        let lanes = router.snapshot();
        let held_by_lane: Vec<(usize, String, Vec<String>)> = lanes
            .iter()
            .enumerate()
            .map(|(ix, lane)| {
                let held = control.lock().unwrap().ledger.holdings(&lane.cfg.name);
                (ix, lane.cfg.name.clone(), held.keys().cloned().collect())
            })
            .collect();
        let (restored, conflicts) = router.restore_ownership(&held_by_lane);
        if restored > 0 {
            eprintln!("[owner] restored {restored} token claim(s) from the ledger");
        }
        for (tok, owner, loser) in &conflicts {
            for name in [owner, loser] {
                if let Some(l) = lanes.iter().find(|l| &l.cfg.name == name) {
                    latch_buys(
                        &l,
                        &incidents,
                        "ownership_conflict",
                        "two lanes claim one token",
                    );
                }
            }
            let short = &tok[tok.len().saturating_sub(8)..];
            eprintln!(
                "[owner] ⛔ CONFLICT on …{short}: {owner:?} and {loser:?} both hold it in their ledgers — buys halted on both, attribution required"
            );
            emitter
                .emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "ownership_conflict", "tok" : &
                        tok[..tok.len().min(14)], "owner" : owner, "conflicting" : loser,
                        "buys_halted" : true, }
                    ),
                );
            let errpath = format!(
                "{}/errors.jsonl", std::path::Path::new(& root.bot.control_path).parent()
                .map(| p | p.to_string_lossy().into_owned()).unwrap_or_else(|| "run"
                .into())
            );
            copybot_hot::errors::record(
                &errpath,
                &copybot_hot::errors::ErrorRow {
                    t: copybot_hot::ledger::now_secs(),
                    lane: loser.clone(),
                    kind: "ownership".into(),
                    detail: format!(
                        "{owner} and {loser} both hold …{short} in their ledgers"
                    ),
                    human: format!(
                        "Two wallets ({owner} and {loser}) both believe they own the same \
market position. The shares exist only once, so one set of books is wrong and the profit \
split between them cannot be trusted. Buying is stopped on both until you say which one \
owns it. BOTH can still sell: each will follow its own leader out, sized from its own \
books — so if both exit, the second may be rejected for balance and will retry through \
the rescue ladder. Attribute it, or flatten by hand, rather than leaving it."
                    ),
                    severity: "stop".into(),
                },
            );
        }
    }
    let pending_path = format!("{}.pending", root.bot.control_path);
    let drift_by_lane: Arc<std::sync::Mutex<std::collections::HashMap<String, f64>>> = Arc::new(
        std::sync::Mutex::new(Default::default()),
    );
    let unattributed: Arc<
        std::sync::Mutex<std::collections::HashMap<String, (String, f64, f64)>>,
    > = Arc::new(std::sync::Mutex::new(Default::default()));
    let errors_path = format!(
        "{}/errors.jsonl", std::path::Path::new(& root.bot.control_path).parent().map(| p
        | p.to_string_lossy().into_owned()).unwrap_or_else(|| "run".into())
    );
    let pending_log = {
        let (log, wal_rows) = copybot_hot::pending::PendingLog::open_with_wal(
            &pending_path,
            Some(&wal_path),
        );
        match log.absorb_wal(wal_rows) {
            Ok(n) => wal_recovery.absorbed("pending", n),
            Err(e) => wal_recovery.failed("pending", e),
        }
        Arc::new(std::sync::Mutex::new(log))
    };
    let pending_unreadable = pending_log.lock().unwrap().unreadable_sources;
    if pending_unreadable > 0 {
        eprintln!(
            "[pending] ⛔ {pending_unreadable} journal source(s) UNREADABLE — the \
                   set of outstanding orders is UNKNOWN, not empty. Halting buys on every \
                   lane; exits keep mirroring."
        );
        for lane in router.snapshot().iter() {
            latch_buys(
                lane,
                &incidents,
                "pending_unreadable",
                "the pending journal could not be read at boot, so outstanding orders are unknown — buying more would compound an exposure we cannot enumerate",
            );
        }
    }
    let pending_tokens = pending_log.lock().unwrap().open_tokens();
    if !pending_tokens.is_empty() {
        eprintln!(
            "[pending] {} unresolved fill(s) carried over — reconciliation will \
                   leave them to the resolver",
            pending_tokens.len()
        );
    }
    match wal_recovery.finish() {
        copybot_hot::wal::Recycled::Emptied { bytes } => {
            eprintln!("[wal] replayed and recycled {bytes} byte(s)")
        }
        copybot_hot::wal::Recycled::AlreadyEmpty => {}
        copybot_hot::wal::Recycled::Retained { why } => {
            eprintln!(
                "[wal] RETAINED — {why}. It will be replayed again next boot; \
                       investigate before this repeats."
            )
        }
    }
    const UNATTRIBUTED_MIN_USD: f64 = 1.0;
    {
        let rec_http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        let fetch = |w: String| {
            let c = rec_http.clone();
            async move {
                let snap = copybot_hot::positions::fetch(
                        &c,
                        "https://data-api.polymarket.com",
                        &w,
                        "0.01",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                if !snap.may_act_destructively() {
                    return Err(
                        format!(
                            "positions for {w} are not a complete list ({}) — \
refusing to reconcile against a partial portfolio",
                            snap.completeness.reason()
                        ),
                    );
                }
                Ok(snap.rows)
            }
        };
        let ours_result = fetch(funder.clone()).await;
        let ours_error = ours_result.as_ref().err().cloned();
        let ours = ours_result.unwrap_or_default();
        if let Some(e) = &ours_error {
            eprintln!("[recon] ⛔ OUR chain positions unverified: {e}");
        }
        let safe_confirmed_empty = if ours_error.is_none() && ours.is_empty() {
            let second = fetch(funder.clone()).await;
            let snaps = [
                copybot_hot::snapshot::Snapshot::Data(0),
                match &second {
                    Ok(v) => copybot_hot::snapshot::Snapshot::Data(v.len()),
                    Err(e) => copybot_hot::snapshot::Snapshot::Failed(e.clone()),
                },
            ];
            match copybot_hot::snapshot::judge_empty(&snaps, 2) {
                copybot_hot::snapshot::Verdict::Believable => {
                    eprintln!(
                        "[recon] wallet is FLAT (confirmed by two independent \
reads) — phantom ledger holdings will be released"
                    );
                    true
                }
                v => {
                    eprintln!(
                        "[recon] wallet looked flat but that is not confirmed \
({v:?}) — treating as a bad read"
                    );
                    false
                }
            }
        } else {
            false
        };
        for (lane_ix, lane) in router.snapshot().iter().enumerate() {
            let name = lane.cfg.name.clone();
            let w = format!("0x{}", hex::encode(lane.cfg.wallet20));
            let his = match fetch(w).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[seed] ⛔ {name}: {e}");
                    control
                        .lock()
                        .unwrap()
                        .set_boot_fault(
                            &name,
                            format!("leader position snapshot unavailable: {e}"),
                        );
                    continue;
                }
            };
            if let Some(e) = &ours_error {
                control
                    .lock()
                    .unwrap()
                    .set_boot_fault(
                        &name,
                        format!("our position snapshot unavailable: {e}"),
                    );
            }
            let mut book: std::collections::HashMap<String, f64> = Default::default();
            for p in &his {
                let t = p["asset"].as_str().unwrap_or("").to_string();
                let sz = p["size"]
                    .as_f64()
                    .or_else(|| p["size"].as_str().and_then(|x| x.parse().ok()))
                    .unwrap_or(0.0);
                if !t.is_empty() && sz > 0.0 {
                    book.insert(t, sz);
                }
            }
            let held = control.lock().unwrap().ledger.holdings(&name);
            let chain_sz: std::collections::HashMap<String, f64> = ours
                .iter()
                .filter_map(|p| {
                    let t = p["asset"].as_str()?.to_string();
                    let sz = p["size"]
                        .as_f64()
                        .or_else(|| p["size"].as_str().and_then(|x| x.parse().ok()))?;
                    Some((t, sz))
                })
                .collect();
            let redeemable: std::collections::HashSet<String> = ours
                .iter()
                .filter(|p| p["redeemable"].as_bool().unwrap_or(false))
                .filter_map(|p| p["asset"].as_str().map(str::to_string))
                .collect();
            let ours_known = held.values().filter(|v| **v > 0.01).count();
            let still_there = held
                .iter()
                .filter(|(t, v)| **v > 0.01 && chain_sz.contains_key(*t))
                .count();
            let trustworthy = ours_error.is_none()
                && (safe_confirmed_empty || ours_known == 0
                    || still_there * 2 >= ours_known);
            if !trustworthy {
                eprintln!(
                    "[recon] {name}: REFUSING snapshot — only {still_there}/{ours_known} \
of our positions present. Treating as a bad read, not a liquidation."
                );
                control
                    .lock()
                    .unwrap()
                    .set_boot_fault(
                        &name,
                        format!(
                            "untrusted wallet snapshot: {still_there}/{ours_known} ledger positions present"
                        ),
                    );
            }
            let mut adopted = 0usize;
            let mut released = 0usize;
            for (tok, sz) in &chain_sz {
                if !book.contains_key(tok) {
                    continue;
                }
                if redeemable.contains(tok) {
                    continue;
                }
                if pending_tokens.iter().any(|(_, t)| t == tok) {
                    continue;
                }
                let claimed_by_pool = control.lock().unwrap().ledger.pool_claim(tok);
                let unexplained = sz - claimed_by_pool;
                let diff = (sz - held.get(tok).copied().unwrap_or(0.0)).min(unexplained);
                if diff <= 0.01 {
                    continue;
                }
                if let Some(other) = router.owner_of(tok) {
                    if other != lane_ix {
                        eprintln!(
                            "[recon] {name}: NOT adopting …{} — lane {other} owns it",
                            & tok[tok.len().saturating_sub(8)..]
                        );
                        emitter
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "recon_contested", "lane" : name,
                                    "tok" : & tok[..tok.len().min(14)], "owner_lane" : other,
                                    "shares" : diff }
                                ),
                            );
                        continue;
                    }
                }
                let px = ours
                    .iter()
                    .find(|p| p["asset"].as_str() == Some(tok.as_str()))
                    .and_then(|p| {
                        p["avgPrice"]
                            .as_f64()
                            .or_else(|| {
                                p["avgPrice"].as_str().and_then(|x| x.parse().ok())
                            })
                    })
                    .unwrap_or(0.0);
                if px <= 0.0 {
                    continue;
                }
                let had_history = control
                    .lock()
                    .unwrap()
                    .ledger
                    .lanes
                    .get(&name)
                    .map(|b| {
                        b.positions.get(tok).map(|p| p.shares > 1e-9).unwrap_or(false)
                    })
                    .unwrap_or(false);
                let proven = had_history
                    || pending_log
                        .lock()
                        .unwrap()
                        .open_tokens()
                        .iter()
                        .any(|(l, t)| l == &name && t == tok);
                let value_usd = diff * px;
                if !proven && value_usd >= UNATTRIBUTED_MIN_USD {
                    let short = &tok[tok.len().saturating_sub(8)..];
                    eprintln!(
                        "[recon] ⛔ {name}: …{short} ({diff:.4} sh @ {px:.4}, \
${value_usd:.2}) has NO ledger history and NO pending order — left UNATTRIBUTED"
                    );
                    emitter
                        .emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "recon_unattributed", "lane" :
                                name, "tok" : & tok[..tok.len().min(14)], "shares" : diff,
                                "price" : px, "usd" : value_usd }
                            ),
                        );
                    unattributed
                        .lock()
                        .unwrap()
                        .insert(tok.clone(), (name.clone(), diff, value_usd));
                    copybot_hot::errors::record(
                        &errors_path,
                        &copybot_hot::errors::ErrorRow {
                            t: copybot_hot::ledger::now_secs(),
                            lane: name.clone(),
                            kind: "unattributed".into(),
                            detail: format!(
                                "…{short}: {diff:.4} sh @ {px:.4} (${value_usd:.2}), \
no ledger history and no pending order"
                            ),
                            human: format!(
                                "The wallet holds ${value_usd:.2} of a market that {name}'s \
leader also trades, but nothing in our records shows we bought it. It has NOT been added \
to {name}'s books, so we will not sell it by mistake — it may be a manual trade or another \
strategy's. Tell us whose it is, or flatten it by hand."
                            ),
                            severity: "warn".into(),
                        },
                    );
                    continue;
                }
                control
                    .lock()
                    .unwrap()
                    .ledger
                    .record_recon_fill(&name, tok, 0, diff, px, 0.0);
                router.claim_existing(tok, lane_ix);
                adopted += 1;
                if !proven {
                    eprintln!(
                        "[recon] ⚠️  {name}: adopted …{} ({diff:.4} sh @ {px:.4}, \
${value_usd:.2}) with NO provenance — below the ${UNATTRIBUTED_MIN_USD:.2} materiality line",
                        & tok[tok.len().saturating_sub(8)..]
                    );
                    emitter
                        .emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "recon_adopt_no_history", "lane" :
                                name, "tok" : & tok[..tok.len().min(14)], "shares" : diff,
                                "price" : px, "usd" : value_usd }
                            ),
                        );
                }
            }
            if trustworthy {
                for (tok, have) in &held {
                    if *have <= 1e-9 {
                        continue;
                    }
                    let on_chain = chain_sz.get(tok).copied().unwrap_or(0.0);
                    let gone = have - on_chain;
                    if gone <= 1e-9 {
                        continue;
                    }
                    if gone <= 0.01 && on_chain > 0.0 {
                        continue;
                    }
                    let px = control.lock().unwrap().ledger.avg_cost(&name, tok);
                    control
                        .lock()
                        .unwrap()
                        .ledger
                        .record_recon_fill(&name, tok, 1, gone, px, 0.0);
                    released += 1;
                }
            }
            if adopted > 0 || released > 0 {
                eprintln!(
                    "[recon] {name}: adopted {adopted}, RELEASED {released} phantom \
position(s) the chain no longer shows"
                );
                if safe_confirmed_empty && released > 0 {
                    copybot_hot::errors::record(
                        &errors_path,
                        &copybot_hot::errors::ErrorRow {
                            t: copybot_hot::ledger::now_secs(),
                            lane: name.clone(),
                            kind: "recon".into(),
                            detail: format!(
                                "wallet confirmed empty; released all {released} \
ledger position(s) for {name}"
                            ),
                            human: format!(
                                "The wallet is reported as holding nothing, confirmed twice, so \
all {released} of {name}'s recorded positions were cleared. Profit and loss are unchanged \
and nothing was sold. This is normal once every market has settled — but if {name} should \
still be holding something, check that the funder address is right."
                            ),
                            severity: "warn".into(),
                        },
                    );
                }
            } else if !ours.is_empty() {
                eprintln!("[recon] {name}: ledger matches the chain");
            }
            if book.is_empty() && !held.is_empty() {
                let reason = "leader position snapshot is empty while our ledger has holdings";
                eprintln!("[seed] ⛔ {name}: {reason}; refusing to enable execution");
                control.lock().unwrap().set_boot_fault(&name, reason);
                continue;
            }
            let legacy = control.lock().unwrap().seed_his_book(&name, &book);
            let now_held = control.lock().unwrap().ledger.holdings(&name).len();
            lane.mark_ready();
            if incidents.lock().map(|g| g.latched(&name)).unwrap_or(true) {
                lane.state.halt_latch.store(true, Ordering::Relaxed);
                eprintln!(
                    "[{name}] buys remain HALTED: an incident from before the \
                           restart is still open — clear it deliberately to resume"
                );
            }
            eprintln!(
                "[seed] {name}: his book {} positions, we hold {}, {} marked legacy",
                book.len(), now_held, legacy
            );
            emitter
                .emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "recon", "lane" : name, "adopted" :
                        adopted, "his_positions" : book.len(), "we_hold" : now_held,
                        "legacy" : legacy }
                    ),
                );
        }
    }
    if live && !shadow {
        let faults = control.lock().unwrap().boot_faults.clone();
        if !faults.is_empty() {
            for (lane, reason) in faults {
                eprintln!("[boot] REFUSING LIVE EXECUTION for {lane}: {reason}");
            }
            std::process::exit(2);
        }
    }
    if shadow {
        let (c2, r2, e2, f2) = (
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
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                let snap = copybot_hot::positions::fetch(
                        &http,
                        "https://data-api.polymarket.com",
                        &f2,
                        "0.01",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                if !snap.may_act_destructively() {
                    continue;
                }
                let chain = &snap.rows;
                for lane in r2.snapshot().iter() {
                    let name = lane.cfg.name.clone();
                    let held = c2.lock().unwrap().ledger.holdings(&name);
                    let his = c2
                        .lock()
                        .unwrap()
                        .his_pos
                        .get(&name)
                        .cloned()
                        .unwrap_or_default();
                    let mut adopted = 0usize;
                    for p in chain {
                        let tok = p["asset"].as_str().unwrap_or("").to_string();
                        if tok.is_empty() || !his.contains_key(&tok) {
                            continue;
                        }
                        let sz = p["size"]
                            .as_f64()
                            .or_else(|| p["size"].as_str().and_then(|x| x.parse().ok()))
                            .unwrap_or(0.0);
                        let diff = sz - held.get(&tok).copied().unwrap_or(0.0);
                        if diff <= 0.01 {
                            continue;
                        }
                        let px = p["avgPrice"]
                            .as_f64()
                            .or_else(|| {
                                p["avgPrice"].as_str().and_then(|x| x.parse().ok())
                            })
                            .unwrap_or(0.0);
                        if px <= 0.0 {
                            continue;
                        }
                        c2.lock()
                            .unwrap()
                            .ledger
                            .record_fill(&name, &tok, 0, diff, px, 0.0);
                        adopted += 1;
                    }
                    if adopted > 0 {
                        e2.emit(
                            serde_json::json!(
                                { "t" : now_ms(), "ev" : "recon", "lane" : name, "adopted" :
                                adopted, "periodic" : true }
                            ),
                        );
                    }
                }
            }
        });
    }
    {
        let supervised = tokio::spawn(
            control_task(
                control.clone(),
                router.clone(),
                pending_log.clone(),
                emitter.clone(),
                watch_tokens.clone(),
                resting_book.clone(),
                levels.clone(),
                prints.clone(),
            ),
        );
        let em_sup = emitter.clone();
        tokio::spawn(async move {
            let outcome = supervised.await;
            let why = match &outcome {
                Err(e) if e.is_panic() => "control task PANICKED".to_string(),
                Err(e) => format!("control task ended abnormally: {e}"),
                Ok(()) => "control task returned, which it must never do".to_string(),
            };
            eprintln!(
                "[CRITICAL] {why} — the only writer of `armed` is gone, so the \
kill switch is inert. Aborting so systemd restarts a coherent process."
            );
            em_sup
                .emit(
                    serde_json::json!(
                        { "t" : now_ms(), "ev" : "control_task_died", "why" : why,
                        "action" : "abort" }
                    ),
                );
            tokio::time::sleep(Duration::from_millis(250)).await;
            std::process::abort();
        });
    }
    let creds_slot: Arc<std::sync::Mutex<Option<copybot_hot::auth::ApiCreds>>> = Arc::new(
        std::sync::Mutex::new(None),
    );
    let physical_wallet: Arc<std::sync::Mutex<PhysicalWallet>> = Arc::new(
        std::sync::Mutex::new(PhysicalWallet::default()),
    );
    {
        let port: u16 = std::env::var("DASHBOARD_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8806);
        let (c, r, bk2) = (control.clone(), router.clone(), books.clone());
        let operator_access_d = operator_access.clone();
        let operator_events_d = emitter.clone();
        let pend_dash = pending_log.clone();
        let custody_dash = custody_report.clone();
        let leader_pnl_dash = leader_pnl.clone();
        let reanchor_dash = reanchor_pulse.clone();
        let (rb_dash, levels_dash, prints_dash) = (
            resting_book.clone(),
            levels.clone(),
            prints.clone(),
        );
        let fund_pool = funding.clone();
        let inc_dash = incidents.clone();
        let (registry_d, phys_d, ctrl_path_d) = (
            registry.clone(),
            physical_wallet.clone(),
            root.bot.control_path.clone(),
        );
        let pool2_path_d = std::env::var("POOL_PAGE")
            .or_else(|_| std::env::var("POOL2_PATH"))
            .unwrap_or_else(|_| {
                let dir = std::path::Path::new(&root.bot.control_path)
                    .parent()
                    .map(|d| d.to_string_lossy().into_owned())
                    .unwrap_or_else(|| ".".into());
                format!("{dir}/pool.html")
            });
        eprintln!(
            "[dash] /pool served from {pool2_path_d} (edit + refresh, no restart)"
        );
        let errors_path_d = errors_path.clone();
        let drift_dash = drift_by_lane.clone();
        let unattributed_dash = unattributed.clone();
        let response_times_dash = response_times.clone();
        let dash_lanes: Vec<DashboardLane> = router
            .snapshot()
            .iter()
            .map(|lane| {
                let seed = root
                    .lane
                    .iter()
                    .find(|x| x.name == lane.cfg.name)
                    .and_then(|x| x.budget.bankroll_usd);
                (
                    lane.cfg.name.clone(),
                    format!("0x{}", hex::encode(lane.cfg.wallet20)),
                    seed,
                    lane.policy().caps.daily_usd,
                    Arc::new(copybot_hot::matchup::Matchup::new()),
                )
            })
            .collect();
        let http_mu = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_default();
        let funder_mu = funder.clone();
        let pos_cache: Arc<
            tokio::sync::Mutex<Option<copybot_hot::positions::Positions>>,
        > = Arc::new(tokio::sync::Mutex::new(None));
        let title_cache: Arc<
            std::sync::RwLock<std::collections::HashMap<String, String>>,
        > = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        {
            let (http_t, funder_t, cache_t) = (
                http_mu.clone(),
                funder_mu.clone(),
                title_cache.clone(),
            );
            tokio::spawn(async move {
                const PAGE: usize = 500;
                const MAX_PAGES: usize = 20;
                let mut tick = tokio::time::interval(Duration::from_secs(300));
                loop {
                    tick.tick().await;
                    let mut scanned = 0usize;
                    for page in 0..MAX_PAGES {
                        let url = format!(
                            "https://data-api.polymarket.com/trades?user={funder_t}\
                             &limit={PAGE}&offset={}",
                            page * PAGE
                        );
                        let rows: Vec<serde_json::Value> = match http_t
                            .get(&url)
                            .send()
                            .await
                        {
                            Ok(r) if r.status().is_success() => {
                                r.json().await.unwrap_or_default()
                            }
                            _ => break,
                        };
                        let n = rows.len();
                        if n > 0 {
                            let mut w = cache_t.write().unwrap();
                            for row in &rows {
                                if let (Some(tok), Some(title)) = (
                                    row["asset"].as_str(),
                                    row["title"].as_str(),
                                ) {
                                    w.entry(tok.to_string())
                                        .or_insert_with(|| title.to_string());
                                }
                            }
                        }
                        scanned += n;
                        if n < PAGE {
                            break;
                        }
                    }
                    eprintln!(
                        "[titles] sweep: {scanned} trades scanned, {} known", cache_t
                        .read().unwrap().len()
                    );
                }
            });
        }
        let (creds_dash, clob_dash, addr_dash, sig_dash) = (
            creds_slot.clone(),
            root.bot.clob_host.clone(),
            signer_addr.clone(),
            sig_type,
        );
        let signal_guard_dash = signal_guard.clone();
        tokio::spawn(async move {
            let lst = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[dash] bind: {e}");
                    return;
                }
            };
            eprintln!(
                "[dash] http://127.0.0.1:{port} (loopback; mutations {})",
                if operator_access_d.enabled() { "require operator capability" } else { "disabled" },
            );
            const MAX_DASH_CONNS: usize = 32;
            const REQUEST_DEADLINE: Duration = Duration::from_secs(15);
            let conn_limit = std::sync::Arc::new(
                tokio::sync::Semaphore::new(MAX_DASH_CONNS),
            );
            loop {
                let Ok((mut sock, _)) = lst.accept().await else { continue };
                let Ok(permit) = conn_limit.clone().try_acquire_owned() else {
                    use tokio::io::AsyncWriteExt;
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
                        )
                        .await;
                    continue;
                };
                let (c, r, bk2) = (c.clone(), r.clone(), bk2.clone());
                let (http2, funder2, lane_specs) = (
                    http_mu.clone(),
                    funder_mu.clone(),
                    dash_lanes.clone(),
                );
                let pos_cache2 = pos_cache.clone();
                let title_cache2 = title_cache.clone();
                let (creds_d, clob_d, addr_d, sigtype_d) = (
                    creds_dash.clone(),
                    clob_dash.clone(),
                    addr_dash.clone(),
                    sig_dash,
                );
                let signal_guard_d = signal_guard_dash.clone();
                let pend_d = pend_dash.clone();
                let custody_d = custody_dash.clone();
                let leader_pnl_d = leader_pnl_dash.clone();
                let reanchor_d = reanchor_dash.clone();
                let (rb_d, levels_d, prints_d) = (
                    rb_dash.clone(),
                    levels_dash.clone(),
                    prints_dash.clone(),
                );
                let inc_srv = inc_dash.clone();
                let (registry_c, phys_c, ctrl_path_c) = (
                    registry_d.clone(),
                    phys_d.clone(),
                    ctrl_path_d.clone(),
                );
                let ctl_http = c.clone();
                let pool2_path_c = pool2_path_d.clone();
                let errors_path_c = errors_path_d.clone();
                let drift_d = drift_dash.clone();
                let unattributed_d = unattributed_dash.clone();
                let response_times_c = response_times_dash.clone();
                let fund_pool2 = fund_pool.clone();
                let operator_access_c = operator_access_d.clone();
                let operator_events_c = operator_events_d.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let _permit = permit;
                    let served = tokio::time::timeout(
                            REQUEST_DEADLINE,
                            async move {
                                let mut raw: Vec<u8> = Vec::with_capacity(4096);
                                let mut tmp = [0u8; 4096];
                                loop {
                                    match sock.read(&mut tmp).await {
                                        Ok(0) => break,
                                        Ok(n) => raw.extend_from_slice(&tmp[..n]),
                                        Err(_) => break,
                                    }
                                    let s = String::from_utf8_lossy(&raw);
                                    if let Some(idx) = s.find("\r\n\r\n") {
                                        let want = s[..idx]
                                            .lines()
                                            .find_map(|l| {
                                                l.to_ascii_lowercase()
                                                    .strip_prefix("content-length:")
                                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                                            })
                                            .unwrap_or(0);
                                        if raw.len() - (idx + 4) >= want {
                                            break;
                                        }
                                    }
                                    if raw.len() > 65_536 {
                                        break;
                                    }
                                }
                                let req = String::from_utf8_lossy(&raw).into_owned();
                                let body: &str = req
                                    .find("\r\n\r\n")
                                    .map(|i| &req[i + 4..])
                                    .unwrap_or("");
                                let mut parts = req.split_whitespace();
                                let method = parts.next().unwrap_or("");
                                let target = parts.next().unwrap_or("");
                                let path = target.split('?').next().unwrap_or("");
                                let query = target
                                    .split_once('?')
                                    .map(|(_, q)| q)
                                    .unwrap_or("");
                                let _ = query;
                                if method != "GET" {
                                    let action = OperatorAction::from_route(method, path);
                                    let authorization = match action {
                                        Some(action) => operator_access_c.authorize(action, &req),
                                        None => {
                                            let resp = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    };
                                    let authorized = match authorization {
                                        Ok(authorized) => authorized,
                                        Err(error) => {
                                            let (code, message) = error.response();
                                            let out = serde_json::json!({ "error": message }).to_string();
                                            let resp = format!(
                                                "HTTP/1.1 {code}\r\nContent-Type: application/json\r\n\
                                                 WWW-Authenticate: Bearer\r\nCache-Control: no-store\r\n\
                                                 Content-Length: {}\r\n\r\n{}", out.len(), out,
                                            );
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    };
                                    operator_events_c.emit(serde_json::json!({
                                        "t": now_ms(), "ev": "operator_authorized",
                                        "actor": "operator-capability", "action": authorized.action().label(),
                                    }));
                                }
                                if method == "GET"
                                    && (path == "/" || path == "/index"
                                        || path == "/index.html")
                                {
                                    let resp = "HTTP/1.1 302 Found\r\nLocation: /pool\r\n\
                            Content-Length: 0\r\n\r\n";
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && (path == "/pool" || path == "/pool2")
                                {
                                    let (code, body) = match std::fs::read_to_string(
                                        &pool2_path_c,
                                    ) {
                                        Ok(html) => ("200 OK", html),
                                        Err(e) => {
                                            (
                                                "404 Not Found",
                                                format!(
                                                    "<pre style=\"font:14px ui-monospace;padding:24px;\
                                 background:#0b0b0e;color:#f2f2f5\">\
                                 /pool2 is served from a file that is not there yet.\n\n\
                                 expected: {pool2_path_c}\n error:    {e}\n\n\
                                 Drop the page there and refresh — no restart needed.</pre>"
                                                ),
                                            )
                                        }
                                    };
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: text/html; \
                            charset=utf-8\r\nCache-Control: no-store\r\n\
                            Content-Length: {}\r\n\r\n{}",
                                        body.len(), body
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/trades" {
                                    let path = req.split_whitespace().nth(1).unwrap_or("");
                                    let q = path.split_once('?').map(|(_, q)| q).unwrap_or("");
                                    let want_lane = q
                                        .split('&')
                                        .find_map(|p| p.strip_prefix("lane="))
                                        .filter(|v| !v.is_empty() && *v != "all");
                                    let limit: usize = q
                                        .split('&')
                                        .find_map(|p| p.strip_prefix("limit="))
                                        .and_then(|v| v.parse().ok())
                                        .unwrap_or(50)
                                        .min(500);
                                    let ledger_path = c.lock().unwrap().ledger.path.clone();
                                    let mut rows: Vec<serde_json::Value> = Vec::new();
                                    match std::fs::metadata(&ledger_path) {
                                        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                                            let out = serde_json::json!(
                                                { "status" : "stale", "stale_reason" :
                                                format!("ledger unreadable: {e}"), "trades" :
                                                serde_json::Value::Null }
                                            )
                                                .to_string();
                                            let resp = format!(
                                                "HTTP/1.1 503 Service Unavailable\r\n\
                                    Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                                out.len(), out
                                            );
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                        _ => {}
                                    }
                                    if let Ok(raw) = std::fs::read_to_string(&ledger_path) {
                                        for line in raw.lines().rev() {
                                            if rows.len() >= limit {
                                                break;
                                            }
                                            let Ok(v) = serde_json::from_str::<
                                                serde_json::Value,
                                            >(line.trim()) else { continue };
                                            if v["ev"].as_str() != Some("fill") {
                                                continue;
                                            }
                                            if v["recon"].as_bool() == Some(true) {
                                                continue;
                                            }
                                            let lane = v["lane"].as_str().unwrap_or("");
                                            if let Some(w) = want_lane {
                                                if lane != w {
                                                    continue;
                                                }
                                            }
                                            let drag_c = match (
                                                v["price"].as_f64(),
                                                v["his_price"].as_f64(),
                                            ) {
                                                (Some(px), Some(his)) if his > 0.0 => {
                                                    serde_json::json!(((px - his) * 10000.0).round() / 100.0)
                                                }
                                                _ => serde_json::Value::Null,
                                            };
                                            let title = v["token"]
                                                .as_str()
                                                .and_then(|t| title_cache2.read().unwrap().get(t).cloned());
                                            rows.push(
                                                serde_json::json!(
                                                    { "lane" : lane, "token" : v["token"], "title" : title,
                                                    "side" : v["side"], "shares" : v["shares"], "price" :
                                                    v["price"], "his_price" : v["his_price"], "drag_c" : drag_c,
                                                    "fee" : v["fee"], "t" : v["t"], "exec" : v["exec"].as_str()
                                                    .unwrap_or("taker"), }
                                                ),
                                            );
                                        }
                                    }
                                    let out = serde_json::to_string(
                                            &serde_json::json!({ "trades" : rows }),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/pool" {
                                    let specs = registry_c.specs();
                                    let phys = *phys_c.lock().unwrap();
                                    let snap = r.snapshot();
                                    let mut lanes_out = Vec::new();
                                    {
                                        let g = c.lock().unwrap();
                                        let st = g.status(&r);
                                        for lane in snap.iter() {
                                            let name = lane.cfg.name.clone();
                                            let armed = lane.state.armed.load(Ordering::Relaxed);
                                            let retired = lane.state.retired.load(Ordering::Relaxed);
                                            let spec = specs.iter().find(|w| w.name == name);
                                            let seed = spec.map(|s| s.seed_usd);
                                            let pct = spec.map(|s| s.pct);
                                            let enabled = spec.map(|s| s.enabled).unwrap_or(true);
                                            let stop_mode = spec
                                                .map(|s| s.stop_mode.clone())
                                                .unwrap_or_else(|| "none".into());
                                            let realised = st["lanes"][&name]["risk"]["realised_pnl"]
                                                .as_f64()
                                                .unwrap_or(0.0);
                                            let cap_scale = st["lanes"][&name]["cap_scale"]
                                                .as_f64()
                                                .unwrap_or(1.0);
                                            let spent_today = st["lanes"][&name]["spent_today"]
                                                .as_f64()
                                                .unwrap_or(0.0);
                                            let open_usd = g.ledger.open_usd(&name);
                                            let vb = seed
                                                .map(|s| copybot_hot::budget::virtual_bankroll(
                                                    s,
                                                    realised,
                                                ));
                                            let band_lo = spec.map(|s| s.min_buy_price).unwrap_or(0.02);
                                            let band_hi = spec.map(|s| s.max_buy_price).unwrap_or(0.95);
                                            let lpol = lane.policy();
                                            let eff_cap = lpol.max_effective_pct;
                                            let compound = lpol.compound;
                                            let eff_pct = copybot_hot::budget::effective_pct(
                                                pct.unwrap_or(0.0),
                                                cap_scale,
                                                eff_cap,
                                                compound,
                                            );
                                            let reanchor_json = {
                                                let g = reanchor_d.lock().unwrap();
                                                match g.get(&name) {
                                                    Some(
                                                        (
                                                            last,
                                                            last_complete,
                                                            raised,
                                                            lowered,
                                                            agreed,
                                                            refused,
                                                            disarmed,
                                                        ),
                                                    ) => {
                                                        let n = copybot_hot::ledger::now_secs();
                                                        serde_json::json!(
                                                            { "last_secs_ago" : n - last, "last_complete_secs_ago" : if
                                                            * last_complete == 0 { serde_json::Value::Null } else {
                                                            serde_json::json!(n - last_complete) }, "blind" : *
                                                            last_complete == 0 || (n - last_complete) > 1_800, "raised"
                                                            : raised, "lowered" : lowered, "agreed" : agreed, "refused"
                                                            : refused, "disarmed_zeros" : disarmed, }
                                                        )
                                                    }
                                                    None => serde_json::Value::Null,
                                                }
                                            };
                                            lanes_out
                                                .push(
                                                    serde_json::json!(
                                                        { "name" : name, "leader" : format!("0x{}", hex::encode(lane
                                                        .cfg.wallet20)), "armed" : armed, "retired" : retired,
                                                        "enabled" : enabled, "stop_mode" : stop_mode, "seed_usd" :
                                                        seed, "pct" : pct, "lane_id" : specs.iter().find(| w | w
                                                        .name == name).map(| w | w.lane_id()).unwrap_or_default(),
                                                        "reanchor" : reanchor_json, "realised_pnl" : (realised *
                                                        100.0).round() / 100.0, "leader_pnl" : leader_pnl_d.lock()
                                                        .unwrap().get(& name).cloned()
                                                        .unwrap_or(serde_json::Value::Null), "virtual_bankroll" :
                                                        vb, "cap_scale" : cap_scale, "open_usd" : (open_usd * 100.0)
                                                        .round() / 100.0, "spent_today" : (spent_today * 100.0)
                                                        .round() / 100.0, "daily_budget" : lpol.caps.daily_usd,
                                                        "min_buy_price" : band_lo, "max_buy_price" : band_hi,
                                                        "max_effective_pct" : eff_cap, "compound" : compound,
                                                        "effective_pct" : eff_pct, "configured_ceiling" : spec.map(|
                                                        s | s.max_effective_pct), "sizing_fit" : spec.and_then(| s |
                                                        s.leader_stats().ok()).map(| ls | { let cap_fill = lpol.caps
                                                        .max_usd_per_fill * cap_scale; let need = eff_pct * ls
                                                        .max_order_usd; let frac = if need > 0.0 { (cap_fill / need)
                                                        .min(1.0) } else { 1.0 }; serde_json::json!({
                                                        "his_max_order_usd" : ls.max_order_usd, "need_usd" : (need *
                                                        100.0).round() / 100.0, "per_fill_cap_usd" : (cap_fill *
                                                        100.0).round() / 100.0, "delivered_frac_on_his_largest" :
                                                        (frac * 10_000.0).round() / 10_000.0, "clipped" : frac <
                                                        0.999, }) }), "copy_makers" : spec.map(| s | s.copy_makers)
                                                        .unwrap_or(false), "exclude_political" : spec.map(| s | s
                                                        .exclude_political).unwrap_or(false), "risk" : st["lanes"]
                                                        [& name] ["risk"].clone(), "halted" : st["lanes"] [& name]
                                                        ["halted"].clone(), "halt_latch" : lane.state.halt_latch
                                                        .load(Ordering::Relaxed), "ready" : lane.state.ready
                                                        .load(Ordering::Relaxed), "share_drift" : drift_d.lock()
                                                        .unwrap().get(& name).copied(), "boot_fault" : st["lanes"]
                                                        [& name] ["boot_fault"].clone(), }
                                                    ),
                                                );
                                        }
                                    }
                                    let sum_seed: f64 = specs
                                        .iter()
                                        .filter(|w| w.enabled)
                                        .map(|w| w.seed_usd)
                                        .sum();
                                    let funding_basis = fund_pool2.lock().unwrap().basis();
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "physical_cash" : phys.cash, "physical_equity" : phys
                                                .equity, "physical_as_of" : phys.fetched_at,
                                                "physical_age_secs" : phys.fetched_at.map(| t |
                                                copybot_hot::ledger::now_secs() - t),
                                                "physical_stale_reason" : phys.stale_reason, "sum_seed" :
                                                (sum_seed * 100.0).round() / 100.0, "headroom" : phys.equity
                                                .map(| p | ((p - sum_seed) * 100.0).round() / 100.0),
                                                "wallet" : { "portfolio" : phys.equity, "funding_basis" :
                                                funding_basis, "equity_pnl" :
                                                copybot_hot::funding::physical_pnl(phys.equity,
                                                funding_basis), "equity_pnl_source" :
                                                "wallet mark-to-market equity minus \
declared net external funding (deposits - withdrawals); independent of lane allocation",
                                                "sum_seed_allocation" : (sum_seed * 100.0).round() / 100.0,
                                                "as_of" : phys.fetched_at, }, "dedupe" : true,
                                                "unattributed" : unattributed_d.lock().unwrap().iter().map(|
                                                (tok, (lane, sh, usd)) | serde_json::json!({ "token" : tok,
                                                "lane" : lane, "shares" : sh, "usd" : usd, })).collect::<
                                                Vec < _ >> (), "lanes" : lanes_out, }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/equity" {
                                    let path = req.split_whitespace().nth(1).unwrap_or("");
                                    let sel = path
                                        .split_once('?')
                                        .and_then(|(_, q)| {
                                            q.split('&').find_map(|p| p.strip_prefix("lane="))
                                        })
                                        .unwrap_or("all")
                                        .to_string();
                                    let lane_opt = if sel == "all" {
                                        None
                                    } else {
                                        Some(sel.as_str())
                                    };
                                    let lpath = c.lock().unwrap().ledger.path.clone();
                                    if let Err(e) = std::fs::metadata(&lpath) {
                                        if e.kind() != std::io::ErrorKind::NotFound {
                                            let out = serde_json::json!(
                                                { "status" : "stale", "stale_reason" :
                                                format!("ledger unreadable: {e}"), "lane" : sel, "points" :
                                                serde_json::Value::Null }
                                            )
                                                .to_string();
                                            let resp = format!(
                                                "HTTP/1.1 503 Service Unavailable\r\n\
                                    Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                                out.len(), out
                                            );
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    }
                                    let series = copybot_hot::ledger::equity_series_at(
                                        &lpath,
                                        lane_opt,
                                    );
                                    let pts: Vec<serde_json::Value> = series
                                        .iter()
                                        .map(|(t, v)| {
                                            serde_json::json!({ "time" : t, "value" : v })
                                        })
                                        .collect();
                                    let out = serde_json::to_string(
                                            &serde_json::json!({ "lane" : sel, "points" : pts }),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/pnl" {
                                    let now = copybot_hot::ledger::now_secs();
                                    let snap = r.snapshot();
                                    let win = |w: copybot_hot::ledger::RealisedWindows| {
                                        serde_json::json!(
                                            { "total" : w.total, "d1" : w.d1, "d7" : w.d7, "d30" : w
                                            .d30, "ytd" : w.ytd, "y1" : w.y1 }
                                        )
                                    };
                                    let lpath = c.lock().unwrap().ledger.path.clone();
                                    let all = win(
                                        copybot_hot::ledger::Ledger::realised_windows_at(
                                            &lpath,
                                            None,
                                            now,
                                        ),
                                    );
                                    let lanes_out: Vec<serde_json::Value> = snap
                                        .iter()
                                        .map(|lane| {
                                            let mut o = win(
                                                copybot_hot::ledger::Ledger::realised_windows_at(
                                                    &lpath,
                                                    Some(&lane.cfg.name),
                                                    now,
                                                ),
                                            );
                                            o["name"] = serde_json::json!(lane.cfg.name);
                                            o
                                        })
                                        .collect();
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "now" : now, "all" : all, "lanes" : lanes_out }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/positions" {
                                    const POS_FRESH_SECS: i64 = 15;
                                    const POS_FALLBACK_SECS: i64 = 300;
                                    let now_s = copybot_hot::ledger::now_secs();
                                    let cached: Option<copybot_hot::positions::Positions> = pos_cache2
                                        .lock()
                                        .await
                                        .clone();
                                    let snapshot = if let Some(c) = cached
                                        .as_ref()
                                        .filter(|c| now_s - c.as_of <= POS_FRESH_SECS)
                                    {
                                        c.clone()
                                    } else {
                                        let fetched = tokio::time::timeout(
                                                Duration::from_secs(15),
                                                copybot_hot::positions::fetch(
                                                    &http2,
                                                    "https://data-api.polymarket.com",
                                                    &funder2,
                                                    "0.0001",
                                                    "",
                                                    now_s,
                                                ),
                                            )
                                            .await;
                                        let fetched = match fetched {
                                            Ok(s) => s,
                                            Err(_) => {
                                                copybot_hot::positions::Positions {
                                                    rows: Vec::new(),
                                                    completeness: copybot_hot::positions::Completeness::Failed {
                                                        after_pages: 0,
                                                        why: "positions request timed out".into(),
                                                    },
                                                    as_of: now_s,
                                                }
                                            }
                                        };
                                        match &fetched.completeness {
                                            copybot_hot::positions::Completeness::Complete => {
                                                *pos_cache2.lock().await = Some(fetched.clone());
                                                fetched
                                            }
                                            copybot_hot::positions::Completeness::Failed { .. } => {
                                                match cached
                                                    .filter(|c| now_s - c.as_of <= POS_FALLBACK_SECS)
                                                {
                                                    Some(c) => c,
                                                    None => fetched,
                                                }
                                            }
                                            _ => fetched,
                                        }
                                    };
                                    let stale_reason: Option<String> = match &snapshot
                                        .completeness
                                    {
                                        copybot_hot::positions::Completeness::Failed { .. } => {
                                            Some(snapshot.completeness.reason())
                                        }
                                        _ => None,
                                    };
                                    let ours: Vec<serde_json::Value> = snapshot.rows.clone();
                                    {
                                        let mut w = title_cache2.write().unwrap();
                                        for row in &ours {
                                            if let (Some(tok), Some(title)) = (
                                                row["asset"].as_str(),
                                                row["title"].as_str(),
                                            ) {
                                                w.entry(tok.to_string())
                                                    .or_insert_with(|| title.to_string());
                                            }
                                        }
                                    }
                                    if let Some(why) = &stale_reason {
                                        let out = serde_json::json!(
                                            { "status" : "stale", "stale_reason" : why, "as_of" :
                                            copybot_hot::ledger::now_secs(), "positions" :
                                            serde_json::Value::Null, }
                                        )
                                            .to_string();
                                        let resp = format!(
                                            "HTTP/1.1 503 Service Unavailable\r\n\
                                Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let pos_of = |tok: &str| -> Option<&serde_json::Value> {
                                        ours.iter().find(|p| p["asset"].as_str() == Some(tok))
                                    };
                                    let snap = r.snapshot();
                                    let mut out_rows = Vec::new();
                                    {
                                        let g = c.lock().unwrap();
                                        for lane in snap.iter() {
                                            let leader = format!(
                                                "0x{}", hex::encode(lane.cfg.wallet20)
                                            );
                                            for (tok, shares, avg, opened_t, his, his_known) in g
                                                .ledger
                                                .open_positions(&lane.cfg.name)
                                            {
                                                let p = pos_of(&tok);
                                                let mark = p.and_then(|p| p["curPrice"].as_f64());
                                                let title = p.and_then(|p| p["title"].as_str());
                                                let outcome = p.and_then(|p| p["outcome"].as_str());
                                                let end_date = p.and_then(|p| p["endDate"].as_str());
                                                let redeemable = p
                                                    .and_then(|p| p["redeemable"].as_bool())
                                                    .unwrap_or(false);
                                                let unreal = mark
                                                    .map(|m| ((m - avg) * shares * 100.0).round() / 100.0);
                                                out_rows
                                                    .push(
                                                        serde_json::json!(
                                                            { "lane" : lane.cfg.name, "leader" : leader, "token" : tok,
                                                            "title" : title, "outcome" : outcome, "shares" : (shares *
                                                            100.0).round() / 100.0, "our_price" : (avg * 10000.0)
                                                            .round() / 10000.0, "leader_price" : his, "his_known_shares"
                                                            : (his_known * 100.0).round() / 100.0, "drag_known" :
                                                            his_known > 1e-9 && (shares - his_known) <= 1e-4 * shares
                                                            .max(1.0), "mark" : mark, "entry_ts" : opened_t,
                                                            "unrealised" : unreal, "end_date" : end_date, "redeemable" :
                                                            redeemable, }
                                                        ),
                                                    );
                                            }
                                        }
                                    }
                                    let complete = snapshot.completeness.is_complete();
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "status" : if complete { "ok" } else { "partial" },
                                                "complete" : complete, "completeness" : snapshot
                                                .completeness.reason(), "as_of" :
                                                copybot_hot::ledger::now_secs(), "positions" : out_rows }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/response_times" {
                                    let samples = response_times_c.tail(50);
                                    let out = serde_json::to_string(
                                            &serde_json::json!({ "samples" : samples }),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/errors" {
                                    let rows = copybot_hot::errors::tail(&errors_path_c, 200);
                                    let now_s = copybot_hot::ledger::now_secs();
                                    let cutoff = now_s
                                        - copybot_hot::errors::ACTIVE_WINDOW_SECS;
                                    let counts = copybot_hot::errors::counts_by_lane(&rows);
                                    let active = copybot_hot::errors::counts_by_lane_since(
                                        &rows,
                                        cutoff,
                                    );
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "errors" : rows, "active_window_secs" :
                                                copybot_hot::errors::ACTIVE_WINDOW_SECS, "active_by_lane" :
                                                active.iter().map(| (l, s, w, n) | serde_json::json!({
                                                "lane" : l, "stops" : s, "warns" : w, "notices" : n }))
                                                .collect::< Vec < _ >> (), "by_lane" : counts.iter().map(|
                                                (l, s, w, n) | serde_json::json!({ "lane" : l, "stops" : s,
                                                "warns" : w, "notices" : n })).collect::< Vec < _ >> (), }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/wallets" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let live_names: Vec<String> = r
                                        .snapshot()
                                        .iter()
                                        .map(|l| l.cfg.name.clone())
                                        .collect();
                                    let (code, out) = handle_wallet_patch(
                                        &registry_c,
                                        phys_c.lock().unwrap().equity,
                                        &live_names,
                                        body,
                                    );
                                    if code.starts_with("200") {
                                        ctl_http.lock().unwrap().ledger.lane_ids = registry_c
                                            .id_map();
                                    }
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/incident/clear" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let v: serde_json::Value = serde_json::from_str(body)
                                        .unwrap_or(serde_json::Value::Null);
                                    let lane = v["lane"].as_str().unwrap_or("");
                                    let want = format!("clear {lane}");
                                    let (code, out) = if lane.is_empty() {
                                        (
                                            "400 Bad Request",
                                            serde_json::json!({ "error" : "lane required" }).to_string(),
                                        )
                                    } else if v["phrase"].as_str() != Some(want.as_str()) {
                                        (
                                            "400 Bad Request",
                                            serde_json::json!(
                                                { "error" : format!("clearing requires phrase {want:?}") }
                                            )
                                                .to_string(),
                                        )
                                    } else {
                                        let by = v["by"]
                                            .as_str()
                                            .unwrap_or("api:/api/incident/clear");
                                        let res: Result<bool, String> = inc_srv
                                            .lock()
                                            .map_err(|_| "journal lock poisoned".to_string())
                                            .and_then(|mut g| {
                                                g.clear(lane, by, copybot_hot::ledger::now_secs())
                                            });
                                        match res {
                                            Ok(was_open) => {
                                                let risk_cleared = c
                                                    .lock()
                                                    .ok()
                                                    .map(|mut g| {
                                                        g
                                                            .ledger
                                                            .clear_risk(
                                                                lane,
                                                                "operator cleared via /api/incident/clear",
                                                            )
                                                    })
                                                    .unwrap_or(false);
                                                if let Some(l) = r
                                                    .snapshot()
                                                    .iter()
                                                    .find(|l| l.cfg.name == lane)
                                                {
                                                    l.state.halt_latch.store(false, Ordering::Relaxed);
                                                }
                                                (
                                                    "200 OK",
                                                    serde_json::json!(
                                                        { "ok" : true, "lane" : lane, "was_open" : was_open,
                                                        "risk_breaker_cleared" : risk_cleared, "note" :
                                                        "buys resume at the next control tick" }
                                                    )
                                                        .to_string(),
                                                )
                                            }
                                            Err(e) => {
                                                (
                                                    "500 Internal Server Error",
                                                    serde_json::json!({ "error" : e }).to_string(),
                                                )
                                            }
                                        }
                                    };
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/arm" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let (code, out) = handle_arm_post(
                                        &format!("{ctrl_path_c}.operator"),
                                        body,
                                        &actor_of(&req),
                                    );
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/flatten" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let fpath = format!("{ctrl_path_c}.flatten");
                                    let (code, out) = handle_flatten_post(
                                        &fpath,
                                        body,
                                        |name| {
                                            r.snapshot()
                                                .iter()
                                                .find(|l| l.cfg.name == name)
                                                .map(|l| l.state.armed.load(Ordering::Relaxed))
                                        },
                                    );
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/matchup" {
                                    let path = req.split_whitespace().nth(1).unwrap_or("");
                                    let requested = path
                                        .split_once('?')
                                        .and_then(|(_, q)| {
                                            q.split('&').find_map(|part| part.strip_prefix("lane="))
                                        });
                                    let resolved = lane_specs
                                        .iter()
                                        .find(|x| requested.map(|v| v == x.0).unwrap_or(false))
                                        .cloned()
                                        .or_else(|| {
                                            let want = requested?;
                                            let lane = r
                                                .snapshot()
                                                .into_iter()
                                                .find(|l| l.cfg.name == want)?;
                                            Some((
                                                lane.cfg.name.clone(),
                                                format!("0x{}", hex::encode(lane.cfg.wallet20)),
                                                registry_c.seed_of(&lane.cfg.name),
                                                lane.policy().caps.daily_usd,
                                                std::sync::Arc::new(copybot_hot::matchup::Matchup::new()),
                                            ))
                                        })
                                        .or_else(|| {
                                            if requested.is_none() {
                                                lane_specs.first().cloned()
                                            } else {
                                                None
                                            }
                                        });
                                    let Some(
                                        (lane_name, lane_wallet, seed, lane_budget, mu2),
                                    ) = resolved else {
                                        let out = serde_json::json!(
                                            { "error" : "no such lane", "lane" : requested, }
                                        )
                                            .to_string();
                                        let resp = format!(
                                            "HTTP/1.1 404 Not Found\r\nContent-Type: \
                                    application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    };
                                    let ledger_snapshot = c
                                        .lock()
                                        .ok()
                                        .and_then(|g| {
                                            g.ledger
                                                .lanes
                                                .get(&lane_name)
                                                .map(|b| (
                                                    b.risk.realised_pnl,
                                                    g.ledger.open_usd(&lane_name),
                                                ))
                                        });
                                    let led = ledger_snapshot.map(|x| x.0);
                                    let ledger_open = ledger_snapshot.map(|x| x.1);
                                    let lane_epoch = c
                                        .lock()
                                        .ok()
                                        .and_then(|g| g.ledger.lane_epoch(&lane_name));
                                    let mut body = mu2
                                        .get(&http2, &funder2, &lane_wallet, led, lane_epoch)
                                        .await;
                                    if let Some(open) = ledger_open {
                                        let chain_deployed = body["deployed"].as_f64();
                                        body["chain_deployed"] = serde_json::json!(chain_deployed);
                                        body["deployed"] = serde_json::json!(
                                            (open * 100.0).round() / 100.0
                                        );
                                        body["cost_basis_delta"] = serde_json::json!(
                                            chain_deployed.map(| x | ((open - x) * 100.0).round() /
                                            100.0)
                                        );
                                        body["deployed_source"] = serde_json::json!("lane ledger");
                                    }
                                    let have = creds_d.lock().unwrap().clone();
                                    let cash = match have {
                                        Some(cr) => {
                                            copybot_hot::matchup::free_cash(
                                                    &http2,
                                                    &clob_d,
                                                    &addr_d,
                                                    &cr,
                                                    sigtype_d,
                                                )
                                                .await
                                        }
                                        None => None,
                                    };
                                    let st = c.lock().unwrap().status(&r);
                                    let spent = st["lanes"][&lane_name]["spent_today"]
                                        .as_f64()
                                        .unwrap_or(0.0);
                                    let armed = st["lanes"][&lane_name]["armed"]
                                        .as_bool()
                                        .unwrap_or(false);
                                    let cap_scale = st["lanes"][&lane_name]["cap_scale"]
                                        .as_f64()
                                        .unwrap_or(1.0);
                                    let realised = led.unwrap_or(0.0);
                                    let virtual_bankroll = seed
                                        .map(|s| copybot_hot::budget::virtual_bankroll(
                                            s,
                                            realised,
                                        ));
                                    let portfolio = cash
                                        .map(|c| {
                                            let v = body["wallet_value"].as_f64().unwrap_or(0.0);
                                            ((c + v) * 100.0).round() / 100.0
                                        });
                                    let funding_basis: f64 = registry_c
                                        .specs()
                                        .iter()
                                        .filter(|w| w.enabled)
                                        .map(|w| w.seed_usd)
                                        .sum();
                                    let equity_pnl = wallet_equity_pnl(
                                        portfolio,
                                        funding_basis,
                                    );
                                    body["wallet"] = serde_json::json!(
                                        { "portfolio" : portfolio, "funding_basis" : (funding_basis
                                        * 100.0).round() / 100.0, "equity_pnl" : equity_pnl,
                                        "equity_pnl_source" :
                                        "wallet mark-to-market equity minus enabled virtual seeds; not adjusted for later deposits or withdrawals",
                                        "cash" : cash, "deployed" : body["deployed"],
                                        "chain_deployed" : body["chain_deployed"],
                                        "cost_basis_delta" : body["cost_basis_delta"],
                                        "deployed_source" : body["deployed_source"], "value" :
                                        body["value"], "shared_position_value" :
                                        body["wallet_value"], "shared_position_count" :
                                        body["wallet_positions"], "spent_today" : (spent * 100.0)
                                        .round() / 100.0, "daily_budget" : lane_budget,
                                        "seed_bankroll" : seed, "virtual_bankroll" :
                                        virtual_bankroll, "sizing_bankroll" : seed.map(| s | s *
                                        cap_scale), "cap_scale" : cap_scale, "realised_pnl" :
                                        realised, "physical_shared" : true, }
                                    );
                                    body["selected_lane"] = serde_json::json!(lane_name);
                                    body["strategies"] = serde_json::json!(
                                        lane_specs.iter().map(| x | & x.0).collect::< Vec < _ >> ()
                                    );
                                    body["runtime"] = serde_json::json!(
                                        { "port" : port, "pid" : std::process::id(), "authoritative"
                                        : armed, "warning" : if armed { serde_json::Value::Null }
                                        else {
                                        serde_json::json!("DISARMED SLOT: figures are not the live authority")
                                        } }
                                    );
                                    let out = serde_json::to_string(&body).unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if let Some((code, msg)) = route_refusal(method, path) {
                                    let out = serde_json::json!(
                                        { "error" : msg, "path" : path, "method" : method }
                                    )
                                        .to_string();
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                let mut body = c.lock().unwrap().status(&r);
                                for (name, _, seed, _, _) in &lane_specs {
                                    let realised = body["lanes"][name]["risk"]["realised_pnl"]
                                        .as_f64()
                                        .unwrap_or(0.0);
                                    body["lanes"][name]["seed_bankroll"] = serde_json::json!(
                                        seed
                                    );
                                    body["lanes"][name]["virtual_bankroll"] = serde_json::json!(
                                        seed.map(| s | copybot_hot::budget::virtual_bankroll(s,
                                        realised))
                                    );
                                }
                                body["books_cached"] = serde_json::json!(bk2.len());
                                body["resting"] = {
                                    let now = copybot_hot::ledger::now_secs();
                                    let rows: Vec<serde_json::Value> = rb_d
                                        .all()
                                        .into_iter()
                                        .map(|o| {
                                            let v = rest_verdict(&levels_d, &prints_d, &o);
                                            serde_json::json!(
                                                { "lane" : o.lane, "side" : if o.side == 0 { "BUY" } else {
                                                "SELL" }, "tok" : & o.token[..o.token.len().min(14)],
                                                "limit" : o.limit, "shares" : o.shares, "age_secs" : now - o
                                                .placed, "verdict" : format!("{v:?}"), "max_age_secs" :
                                                copybot_hot::restwatch::max_age_for(o.side), "anchored" : o
                                                .his_remaining > 0.0, }
                                            )
                                        })
                                        .collect();
                                    serde_json::json!({ "n" : rows.len(), "orders" : rows })
                                };
                                body["custody"] = match custody_d.lock().unwrap().as_ref() {
                                    Some(r) => {
                                        serde_json::json!(
                                            { "checked" : r.checked, "agreed" : r.agreed, "explained" :
                                            r.explained, "unattributed" : r.unattributed, "unknown" : r
                                            .unknown, "complete" : r.complete, "unattributed_shares" :
                                            (r.unattributed_shares * 10_000.0).round() / 10_000.0,
                                            "alarm" : r.is_alarm(), "rows" : r.rows, "msg" :
                                            copybot_hot::custody::describe(r), }
                                        )
                                    }
                                    None => serde_json::Value::Null,
                                };
                                body["pending_persistence"] = {
                                    let l = pend_d.lock().unwrap();
                                    serde_json::json!(
                                        { "ok" : l.persistence_ok(), "write_failures" : l
                                        .write_failures, "open_orders" : l.len(), "last_error" : l
                                        .last_write_error }
                                    )
                                };
                                body["signal_guard"] = match signal_guard_d {
                                    Some(guard) => {
                                        match guard.active_markets() {
                                            Ok(active) => {
                                                serde_json::json!(
                                                    { "enabled" : true, "healthy" : true, "active_markets" :
                                                    active, "market_ttl_seconds" :
                                                    copybot_hot::signal_guard::MARKET_TTL_SECS, }
                                                )
                                            }
                                            Err(e) => {
                                                serde_json::json!(
                                                    { "enabled" : true, "healthy" : false, "error" :
                                                    format!("{e:?}"), }
                                                )
                                            }
                                        }
                                    }
                                    None => {
                                        serde_json::json!({ "enabled" : false, "healthy" : true })
                                    }
                                };
                                let out = serde_json::to_string_pretty(&body)
                                    .unwrap_or_default();
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                        Content-Length: {}\r\n\r\n{}",
                                    out.len(), out
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            },
                        )
                        .await;
                    if served.is_err() {
                        eprintln!(
                            "[dash] dropped a connection that stalled past the \
{REQUEST_DEADLINE:?} request deadline"
                        );
                    }
                });
            }
        });
    }
    runtime::telemetry::spawn_feed_status(
        &stats, &emitter, &books, &watch_tokens, &feed_registry, &book, &resting_book,
    );
    let mk_client = |h2: bool| {
        let b = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(4)
            .timeout(Duration::from_secs(10));
        if h2 { b.build() } else { b.http1_only().build() }
    };
    let http = mk_client(false).expect("h1 client");
    let mut order_paths: Vec<(String, reqwest::Client)> = vec![
        ("h1".into(), http.clone())
    ];
    if root.bot.race_h2 {
        match mk_client(true) {
            Ok(c) => order_paths.push(("h2".into(), c)),
            Err(e) => eprintln!("[transport] h2 unavailable: {e}"),
        }
        match mk_client(false) {
            Ok(c) => order_paths.push(("h1b".into(), c)),
            Err(e) => eprintln!("[transport] third path unavailable: {e}"),
        }
    }
    eprintln!(
        "[transport] racing {} paths: {}", order_paths.len(), order_paths.iter().map(|
        (n, _) | n.as_str()).collect::< Vec < _ >> ().join(", ")
    );
    let order_paths = Arc::new(order_paths);
    let clob = root.bot.clob_host.clone();
    let creds: Arc<Option<copybot_hot::auth::ApiCreds>> = if shadow {
        eprintln!(
            "[auth] SHADOW — credentials deliberately NOT derived. Every order \
will be rejected 401 by the venue. This process cannot place an order."
        );
        Arc::new(None)
    } else {
        use copybot_hot::auth::derive_or_create_creds;
        match pk {
            Some(pk) => {
                match derive_or_create_creds(&http, &clob, &pk, &signer_addr, 0).await {
                    Ok(c) => {
                        eprintln!("[auth] L2 credentials derived — {}", c.redacted());
                        *creds_slot.lock().unwrap() = Some(c.clone());
                        Arc::new(Some(c))
                    }
                    Err(e) => {
                        eprintln!("[auth] FAILED to obtain L2 credentials: {e}");
                        Arc::new(None)
                    }
                }
            }
            None => {
                eprintln!(
                    "[auth] no PRIVATE_KEY in env — L2 auth UNAVAILABLE (paper mode only)"
                );
                Arc::new(None)
            }
        }
    };
    if let Some(cr) = creds.as_ref() {
        let path = "/data/orders";
        let ts = copybot_hot::auth::now_secs();
        match copybot_hot::auth::l2_headers(&signer_addr, cr, ts, "GET", path, None) {
            Ok(h) => {
                let mut rb = http.get(format!("{clob}{path}"));
                for (k, v) in &h {
                    rb = rb.header(*k, v);
                }
                match rb.send().await {
                    Ok(r) if r.status().is_success() => {
                        eprintln!(
                            "[auth] self-check OK — {path} accepted our L2 headers"
                        );
                        match r.json::<serde_json::Value>().await {
                            Ok(v) => {
                                let n = (v.as_array().map(|a| a.len()))
                                    .or_else(|| v["data"].as_array().map(|a| a.len()))
                                    .unwrap_or(0);
                                if n > 0 {
                                    eprintln!(
                                        "[resting] venue holds {n} live order(s); attribution deferred to the open-orders reconcile (~60s)"
                                    );
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "resting_seen_at_boot", "orders" :
                                                n }
                                            ),
                                        );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "[resting] could not read the open-order list ({e}); a GTC order the venue still holds would be unmanaged"
                                )
                            }
                        }
                    }
                    Ok(r) => {
                        eprintln!(
                            "[auth] ⛔ SELF-CHECK FAILED: {path} -> {}. Credentials \
derived but are NOT accepted; every order would be refused. Refusing to continue.",
                            r.status()
                        );
                        if live && !shadow {
                            std::process::exit(2);
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[auth] ⛔ self-check could not run ({e}); auth is UNVERIFIED"
                        );
                        if live && !shadow {
                            std::process::exit(2);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[auth] ⛔ self-check header build failed: {e}");
                if live && !shadow {
                    std::process::exit(2);
                }
            }
        }
    }
    if live && !shadow && creds.is_none() {
        eprintln!(
            "[auth] ⛔ REFUSING TO ARM: mode=live but there are no L2 credentials.\n\
                   [auth]    Every /order POST would be rejected 401 by the venue.\n\
                   [auth]    Set PRIVATE_KEY, or run mode=paper."
        );
        std::process::exit(2);
    }
    let auth_addr = signer_addr.clone();
    runtime::exits::spawn_cancellation_sweep(
        &resting_book, &http, &clob, &auth_addr, &creds, &router, &emitter,
        &signal_guard, &levels, &prints,
    );
    let (sweep_tx, sweep_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let sweep_tx = std::sync::Arc::new(sweep_tx);
    if live {
        runtime::exit_sweep::spawn(
            &pending_log, &incidents, &merge_claims, &control, &router, &emitter,
            &funder, pk, &signer_addr, sig_type, &clob, &creds, &order_paths, sweep_rx,
        );
    }
    if live {
        runtime::flatten_task::spawn(
            &root.bot.control_path, &pending_log, &incidents, &control, &router,
            &emitter, &funder, pk, &signer_addr, sig_type, &clob, &creds,
            &order_paths, &sweep_tx, &registry, &errors_path,
        );
    }
    if live {
        runtime::merge::spawn_reconcile(
            &pending_log, &incidents, &merge_claims, &control, &router, &emitter,
            &funder, pk, &signer_addr, sig_type, &clob, &creds, &order_paths, &sweep_tx,
        );
    }
    let latency = Arc::new(copybot_hot::latency::Latency::new());
    runtime::telemetry::spawn_latency_probes(&latency, &http, &clob, &emitter);
    if live {
        runtime::orphan_sweep::spawn(
            &control, &router, &emitter, &funder, pk, &signer_addr,
            sig_type, &clob, &creds, &pending_log, &order_paths,
        );
    }
    if live {
        runtime::wallet_sync::spawn(
            &registry, &router, &control, &physical_wallet, &emitter,
            &watched_addrs, &pair_ledger,
        );
    }
    if live {
        runtime::exits::spawn_open_order_reconcile(
            &resting_book, &http, &clob, &auth_addr, &creds, &emitter,
            &signal_guard, &router, &control,
        );
    }
    if live {
        runtime::leader_pnl::spawn(&router, &registry, &leader_pnl);
    }
    if live {
        runtime::control::spawn_guardian_liveness(&control, &root.bot.control_path);
    }
    if live {
        runtime::reconcile::spawn_reanchor(&control, &router, &emitter, &registry, &reanchor_pulse);
    }
    if live {
        runtime::custody_monitor::spawn(
            &control, &router, &emitter, &funder, &pending_log, &custody_report,
        );
    }
    if live {
        runtime::accounting::spawn_settlement_booking(&control, &router, &emitter, &funder);
    }
    if live {
        runtime::accounting::spawn_redemption_correction(
            &control, &router, &emitter, &funder, &pending_log,
        );
    }
    if live {
        runtime::accounting::spawn_pending_recovery(
            &pending_log, &control, &router, &emitter, &clob, &creds,
            &signer_addr, sig_type, &resting_book,
        );
    }
    if live {
        runtime::position_reconciliation::spawn(
            &control, &router, &emitter, &funder, &pending_log, &errors_path, &drift_by_lane,
        );
    }
    if live && !shadow {
        let hc = mk_client(false).expect("seed-check client");
        runtime::equity_monitor::spawn(
            &creds_slot, &clob, &auth_addr, &funder, &physical_wallet,
            &registry, sig_type, hc,
        );
    }
    runtime::bankroll_monitor::spawn(&control, &router, &emitter, &registry);
    runtime::telemetry::spawn_route_warm_probe(&order_paths, &clob, &emitter);
    let politics: runtime::politics::PoliticsBook =
        Arc::new(std::sync::RwLock::new(None));
    if live {
        match mk_client(false) {
            Ok(hc) => runtime::politics::spawn(&politics, &registry, &emitter, hc),
            Err(_) => {
                eprintln!("[politics] REFUSED: cannot construct timed activity client");
                emitter.emit(serde_json::json!({
                    "t": now_ms(), "ev": "politics_unavailable",
                    "why": "http_client_initialization",
                }));
            }
        }
    }
    feed_loop::run(rx, feed_loop::Context {
        live, emitter, feed_registry, router, pair_ledger,
        errors_path, politics, watch_tokens, resting_book, creds,
        http, clob, auth_addr, signal_guard, book,
        control, incidents, physical_wallet, fire_rate, order_paths,
        pk, funder, signer_addr, sig_type, pending_log,
        wal, latency, levels, prints, sweep_tx,
        books, response_times, sell_tranches
    }).await;

}
#[cfg(test)]
#[path = "main_tests.rs"]
mod http_handler_tests;
