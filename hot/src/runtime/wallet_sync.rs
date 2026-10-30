//! Runtime lane registry reload, repricing, activation and retirement owner.
//! The caller retains the live-mode gate and original task launch ordering.

use super::dashboard_model::PhysicalWallet;
use super::merge::seed_pair_ledger;
use super::reconcile::reactivate_lane;
use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    registry: &copybot_hot::wallets::WalletRegistry,
    router: &Arc<Router>,
    control: &Arc<std::sync::Mutex<Control>>,
    physical_wallet: &Arc<std::sync::Mutex<PhysicalWallet>>,
    emitter: &Arc<Emitter>,
    watched_addrs: &Arc<copybot_hot::feeds::WatchedAddresses>,
    pair_ledger: &Arc<std::sync::Mutex<copybot_hot::pairledger::PairLedger>>,
) {
        let (reg, rt, ctl) = (registry.clone(), router.clone(), control.clone());
        let phys = physical_wallet.clone();
        let ev_rt = emitter.clone();
        let watched_rt = watched_addrs.clone();
        let pl_rt = pair_ledger.clone();
        tokio::spawn(async move {
            let http_rt = match reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
            {
                Ok(client) => client,
                Err(error) => {
                    eprintln!("[wallets] REFUSING registry sync without timed HTTP client: {error}");
                    ev_rt.emit(serde_json::json!({
                        "t": now_ms(), "ev": "wallet_sync_unavailable",
                        "reason": "http_client_initialization",
                    }));
                    return;
                }
            };
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            let mut seed_retry: std::collections::HashMap<
                String,
                (u32, std::time::Instant),
            > = std::collections::HashMap::new();
            loop {
                tick.tick().await;
                if reg.reload() {
                    if let Err(e) = reg.mint_missing_ids() {
                        eprintln!(
                            "[laneid] ⚠️ could not persist a new lane's id: {e}"
                        );
                    }
                    ctl.lock().unwrap().ledger.lane_ids = reg.id_map();
                }
                let pool = reg.specs();
                watched_rt.set(pool.iter().map(|w| w.leader.clone()));
                let snap = rt.snapshot();
                let live_names: Vec<String> = snap
                    .iter()
                    .map(|l| l.cfg.name.clone())
                    .collect();
                let retired_names: Vec<String> = snap
                    .iter()
                    .filter(|l| l.state.retired.load(Ordering::Relaxed))
                    .map(|l| l.cfg.name.clone())
                    .collect();
                let plan = copybot_hot::wallets::reconcile(
                    &pool,
                    &live_names,
                    &retired_names,
                );
                let applied: Vec<(String, f64, f64, f64, bool, bool, bool, f64, f64)> = rt
                    .snapshot()
                    .iter()
                    .map(|l| {
                        let p = l.policy();
                        let pct = match p.sizing {
                            copybot_hot::lanes::Sizing::Pct(v) => v,
                            _ => f64::NAN,
                        };
                        (
                            l.cfg.name.clone(),
                            pct,
                            p.seed_usd,
                            p.max_effective_pct,
                            p.compound,
                            l.state.rest_buys.load(Ordering::Relaxed),
                            l.state.rest_sells.load(Ordering::Relaxed),
                            l.cfg.min_buy_price,
                            l.cfg.max_buy_price,
                        )
                    })
                    .collect();
                for rp in copybot_hot::wallets::repricing(&pool, &applied) {
                    let pool_seed: f64 = pool
                        .iter()
                        .filter(|w| w.enabled)
                        .map(|w| w.seed_usd)
                        .sum();
                    let physical = phys.lock().unwrap().equity;
                    if let Err(e) = copybot_hot::budget::seeds_fit_wallet(
                        pool_seed,
                        physical,
                    ) {
                        eprintln!(
                            "[wallets] ⚠️  OVER-COMMITTED, repricing {} anyway: {e}",
                            rp.name
                        );
                    }
                    let Some(lane) = rt
                        .snapshot()
                        .iter()
                        .find(|l| l.cfg.name == rp.name)
                        .cloned() else { continue };
                    let built = copybot_hot::config::build_reprice_policy(&lane, &rp.spec);
                    let hybrid = lane.cfg.execution
                        == copybot_hot::lanes::Execution::Hybrid;
                    let (want_b, want_s) = (
                        rp.spec.copy_makers || hybrid,
                        rp.spec.copy_maker_sells() || hybrid,
                    );
                    let was_b = lane.state.rest_buys.swap(want_b, Ordering::Relaxed);
                    let was_s = lane.state.rest_sells.swap(want_s, Ordering::Relaxed);
                    if was_b != want_b || was_s != want_s {
                        eprintln!(
                            "[wallets] {} resting: buys {was_b}->{want_b} \
                                   sells {was_s}->{want_s}",
                            rp.name
                        );
                        ev_rt
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms(), "ev" : "rest_switch", "lane" : rp.name,
                                    "rest_buys" : want_b, "rest_sells" : want_s }
                                ),
                            );
                    }
                    match built.and_then(|pol| lane.apply_policy(pol)) {
                        Ok(gen) => {
                            let p = lane.policy();
                            eprintln!(
                                "[wallets] {} REPRICED (gen {gen}): pct {:.3}% ceiling {:.1}% \
                                       seed ${:.0} -> fill ${:.2} / market ${:.0} / open ${:.0} / daily ${:.0}",
                                rp.name, rp.spec.pct * 100.0, rp.spec.max_effective_pct *
                                100.0, p.seed_usd, p.caps.max_usd_per_fill, p.caps
                                .per_market_usd, p.caps.max_open_usd, p.caps.daily_usd
                            );
                            ev_rt
                                .emit(
                                    serde_json::json!(
                                        { "t" : now_ms(), "ev" : "lane_repriced", "lane" : rp.name,
                                        "generation" : gen, "pct" : rp.spec.pct, "max_effective_pct"
                                        : rp.spec.max_effective_pct, "seed_usd" : p.seed_usd, "caps"
                                        : { "fill" : p.caps.max_usd_per_fill, "market" : p.caps
                                        .per_market_usd, "open" : p.caps.max_open_usd, "daily" : p
                                        .caps.daily_usd }, }
                                    ),
                                );
                        }
                        Err(e) => eprintln!("[wallets] {} reprice REFUSED: {e}", rp.name),
                    }
                }
                for spec in &plan.to_add {
                    let pool_seed: f64 = pool
                        .iter()
                        .filter(|w| w.enabled)
                        .map(|w| w.seed_usd)
                        .sum();
                    let physical = phys.lock().unwrap().equity;
                    if let Err(e) = copybot_hot::budget::seeds_fit_wallet(
                        pool_seed,
                        physical,
                    ) {
                        eprintln!(
                            "[wallets] ⚠️  OVER-COMMITTED, loading {} anyway: {e}",
                            spec.name
                        );
                    }
                    match copybot_hot::config::build_runtime_lane(spec) {
                        Ok((lane, risk)) => {
                            ctl.lock().unwrap().ledger.ensure_lane(&spec.name, risk);
                            let ix = rt.push(lane);
                            let Some(placed) = rt.lane(ix) else { continue };
                            match reactivate_lane(&placed, &ctl, &http_rt).await {
                                Ok(seeded) => {
                                    placed.mark_ready();
                                    let brk = if spec.risk.is_none() {
                                        format!(
                                            "risk: DEFAULT drawdown stop ${:.0} \
                                                 (15% of seed; set a `risk` block to override)",
                                            risk.max_drawdown_usd
                                        )
                                    } else {
                                        format!(
                                            "risk: dd ${:.0}/{:.1}% losses {} open {}", risk
                                            .max_drawdown_usd, risk.max_drawdown_pct * 100.0, risk
                                            .max_consecutive_losses, risk.max_open_positions
                                        )
                                    };
                                    eprintln!(
                                        "[wallets] LOADED {} (lane {ix}) seed ${:.0} \
pct {:.3}% {brk} — leader book seeded ({seeded} positions); disarmed, arm it in the operator file",
                                        spec.name, spec.seed_usd, spec.pct * 100.0
                                    );
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[wallets] {} loaded but NOT READY: {e} \
(will retry; it cannot trade until its leader book is seeded)",
                                        spec.name
                                    )
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[wallets] refused to load {}: {e}", spec.name)
                        }
                    }
                }
                for l in rt.snapshot().iter() {
                    if l.state.retired.load(Ordering::Relaxed) {
                        continue;
                    }
                    let leader_hex = format!("0x{}", hex::encode(l.cfg.wallet20));
                    if pl_rt.lock().map(|g| g.is_seeded(&leader_hex)).unwrap_or(true) {
                        continue;
                    }
                    if let Some((_, next)) = seed_retry.get(&leader_hex) {
                        if std::time::Instant::now() < *next {
                            continue;
                        }
                    }
                    match seed_pair_ledger(&http_rt, &leader_hex, &pl_rt).await {
                        Ok((rows, conds)) => {
                            seed_retry.remove(&leader_hex);
                            eprintln!(
                                "[pairledger] {} seeded {rows} split/merge row(s), {conds} \
condition(s) carrying a balance",
                                l.cfg.name
                            );
                        }
                        Err(e) => {
                            let n = seed_retry
                                .get(&leader_hex)
                                .map(|(n, _)| *n)
                                .unwrap_or(0) + 1;
                            let wait = Duration::from_secs(
                                30u64.saturating_mul(1u64 << n.min(4)).min(600),
                            );
                            seed_retry
                                .insert(
                                    leader_hex.clone(),
                                    (n, std::time::Instant::now() + wait),
                                );
                            eprintln!(
                                "[pairledger] ⚠️  {} could NOT seed ({e}) — split balances start \
EMPTY, so an income withdrawal would read as an EXIT. Merge handling is observe-only, so \
nothing acts on it; retry #{n} in {}s.",
                                l.cfg.name, wait.as_secs()
                            );
                        }
                    }
                }
                for l in rt.snapshot().iter() {
                    if l.state.ready.load(Ordering::Relaxed)
                        || l.state.retired.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    if !pool.iter().any(|w| w.enabled && w.name == l.cfg.name) {
                        continue;
                    }
                    if let Ok(seeded) = reactivate_lane(l, &ctl, &http_rt).await {
                        l.mark_ready();
                        eprintln!(
                            "[wallets] {} became READY on retry ({seeded} leader \
positions)",
                            l.cfg.name
                        );
                    }
                }
                for spec in &plan.to_resume {
                    let Some(lane) = rt
                        .snapshot()
                        .iter()
                        .find(|l| l.cfg.name == spec.name)
                        .cloned() else { continue };
                    lane.mark_activating();
                    match reactivate_lane(&lane, &ctl, &http_rt).await {
                        Ok(seeded) => {
                            lane.state.retired.store(false, Ordering::Relaxed);
                            lane.mark_ready();
                            eprintln!(
                                "[wallets] RESUMED {} — leader book re-seeded ({seeded} \
positions); still disarmed, arm it in the operator file",
                                spec.name
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "[wallets] refused to resume {}: {e} \
(stays retired; will retry)",
                                spec.name
                            )
                        }
                    }
                }
                for name in &plan.to_retire {
                    if let Some(lane) = rt
                        .snapshot()
                        .iter()
                        .find(|l| &l.cfg.name == name)
                    {
                        if !lane.state.retired.swap(true, Ordering::Relaxed) {
                            eprintln!(
                                "[wallets] RETIRED {name} — stops buying, drains its \
                                       book, then sits inert"
                            );
                        }
                    }
                }
            }
        });
}
