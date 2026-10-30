//! Existing restart-recovery sizing and lane reactivation boundary.

use copybot_hot::control::Control;
use copybot_hot::position_evidence::{complete_balances, PositionError};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use copybot_hot::lanes::Router;
use super::telemetry::{now_ms, Emitter};

#[derive(Debug)]
pub(crate) enum ReactivationError {
    Position(PositionError),
    EmptyLeaderWithHoldings,
}

impl std::fmt::Display for ReactivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Position(error) => write!(f, "leader book refused: {error}"),
            Self::EmptyLeaderWithHoldings => write!(f, "leader snapshot is empty while our ledger has holdings"),
        }
    }
}
impl std::error::Error for ReactivationError {}

pub(crate) type ReanchorPulse = Arc<
    std::sync::Mutex<
        std::collections::HashMap<String, (i64, i64, usize, usize, usize, usize, usize)>,
    >,
>;

pub(crate) const RECON_SECS: u64 = 5;
pub(crate) const RECON_QUIET_SECS: i64 = 120;
pub(crate) const RECON_RELEASE_QUIET_SECS: i64 = 1_800;

pub(crate) fn recovery_target_shares(
    leader_size: f64,
    leader_avg_price: f64,
    our_avg_price: f64,
    pct: f64,
    scale: f64,
    min_order_usd: f64,
    max_effective_pct: f64,
    compound: bool,
) -> Option<f64> {
    copybot_hot::budget::target_shares(
        leader_size,
        leader_avg_price,
        our_avg_price,
        pct,
        scale,
        min_order_usd,
        max_effective_pct,
        compound,
    )
}

pub(crate) async fn reactivate_lane(
    lane: &std::sync::Arc<copybot_hot::lanes::Lane>,
    control: &Arc<std::sync::Mutex<Control>>,
    http: &reqwest::Client,
) -> Result<usize, ReactivationError> {
    let name = lane.cfg.name.clone();
    let leader = format!("0x{}", hex::encode(lane.cfg.wallet20));
    let snap = copybot_hot::positions::fetch(
            http,
            "https://data-api.polymarket.com",
            &leader,
            "0.0001",
            "",
            copybot_hot::ledger::now_secs(),
        )
        .await;
    let mut book = complete_balances(&snap).map_err(ReactivationError::Position)?;
    book.retain(|_, size| *size > 0.0);
    let held = control.lock().unwrap().ledger.holdings(&name);
    if book.is_empty() && !held.is_empty() {
        return Err(ReactivationError::EmptyLeaderWithHoldings);
    }
    control.lock().unwrap().seed_his_book(&name, &book);
    Ok(book.len())
}

pub(crate) fn spawn_reanchor(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    registry: &copybot_hot::wallets::WalletRegistry,
    reanchor_pulse: &ReanchorPulse,
) {
        let (ctl_ra, rt_ra, em_ra, reg_ra) = (
            control.clone(),
            router.clone(),
            emitter.clone(),
            registry.clone(),
        );
        let pulse_ra = reanchor_pulse.clone();
        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();
            let mut tick = tokio::time::interval(Duration::from_secs(300));
            loop {
                tick.tick().await;
                for lane in rt_ra.snapshot().iter() {
                    if lane.state.retired.load(Ordering::Relaxed) {
                        continue;
                    }
                    let name = lane.cfg.name.clone();
                    let Some(spec) = reg_ra.specs().into_iter().find(|w| w.name == name)
                    else { continue };
                    let snap = copybot_hot::positions::fetch(
                            &http,
                            "https://data-api.polymarket.com",
                            &spec.leader,
                            "0.0001",
                            "",
                            copybot_hot::ledger::now_secs(),
                        )
                        .await;
                    let complete = snap.may_act_destructively();
                    let venue: std::collections::HashMap<String, f64> = snap
                        .rows
                        .iter()
                        .filter_map(|p| {
                            let t = p["asset"].as_str()?.to_string();
                            let sz = p["size"]
                                .as_f64()
                                .or_else(|| {
                                    p["size"].as_str().and_then(|x| x.parse().ok())
                                })?;
                            Some((t, sz))
                        })
                        .collect();
                    let (writes, report) = {
                        let g = ctl_ra.lock().unwrap();
                        let modelled = g.his_pos.get(&name).cloned().unwrap_or_default();
                        let held = g.ledger.holdings(&name);
                        copybot_hot::reanchor::plan(&modelled, &venue, &held, complete)
                    };
                    {
                        let mut g = pulse_ra.lock().unwrap();
                        let e = g.entry(name.clone()).or_insert((0, 0, 0, 0, 0, 0, 0));
                        let last_complete = if complete {
                            now_ms() as i64 / 1000
                        } else {
                            e.1
                        };
                        *e = (
                            now_ms() as i64 / 1000,
                            last_complete,
                            report.raised,
                            report.lowered,
                            report.agreed,
                            report.refused,
                            report.disarmed_zeros,
                        );
                    }
                    if writes.is_empty() {
                        continue;
                    }
                    {
                        let mut g = ctl_ra.lock().unwrap();
                        let hp = g.his_pos.entry(name.clone()).or_default();
                        for (tok, v) in &writes {
                            hp.insert(tok.clone(), *v);
                        }
                    }
                    for (tok, v) in &writes {
                        lane.set_his(tok, *v);
                    }
                    if report.disarmed_zeros > 0 || report.raised > 0
                        || report.lowered > 0
                    {
                        eprintln!(
                            "[reanchor] {name}: raised {} lowered {} agreed {} \
                                   refused {} (disarmed {} modelled-zero position(s))",
                            report.raised, report.lowered, report.agreed, report.refused,
                            report.disarmed_zeros
                        );
                        em_ra
                            .emit(
                                serde_json::json!(
                                    { "t" : now_ms() as i64, "ev" : "his_book_reanchored",
                                    "lane" : name, "raised" : report.raised, "lowered" : report
                                    .lowered, "agreed" : report.agreed, "refused" : report
                                    .refused, "disarmed_zeros" : report.disarmed_zeros,
                                    "shares_raised" : (report.shares_raised * 100.0).round() /
                                    100.0, "complete" : complete }
                                ),
                            );
                    }
                }
            }
        });
}
