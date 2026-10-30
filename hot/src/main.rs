#![cfg_attr(test, allow(non_snake_case))]
mod runtime;
#[path = "feed_loop.rs"]
mod feed_loop;
#[path = "dashboard.rs"]
mod dashboard;
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
    dashboard::spawn(dashboard::Context {
        root: &root,
        control: control.clone(),
        router: router.clone(),
        books: books.clone(),
        operator_access: operator_access.clone(),
        emitter: emitter.clone(),
        pending_log: pending_log.clone(),
        custody_report: custody_report.clone(),
        leader_pnl: leader_pnl.clone(),
        reanchor_pulse: reanchor_pulse.clone(),
        resting_book: resting_book.clone(),
        levels: levels.clone(),
        prints: prints.clone(),
        funding: funding.clone(),
        incidents: incidents.clone(),
        registry: registry.clone(),
        physical_wallet: physical_wallet.clone(),
        errors_path: errors_path.clone(),
        drift_by_lane: drift_by_lane.clone(),
        unattributed: unattributed.clone(),
        response_times: response_times.clone(),
        funder: funder.clone(),
        creds_slot: creds_slot.clone(),
        signer_addr: signer_addr.clone(),
        sig_type: sig_type,
        signal_guard: signal_guard.clone(),
    });
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
