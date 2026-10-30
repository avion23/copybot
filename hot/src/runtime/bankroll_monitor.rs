//! Periodic virtual bankroll scaling from the durable realised P&L.
//! No network I/O, additional budget state or independent policy authority.

use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    registry: &copybot_hot::wallets::WalletRegistry,
) {
        let (rt, em, reg) = (router.clone(), emitter.clone(), registry.clone());
        let ctl_b = control.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(120));
            loop {
                tick.tick().await;
                for lane in rt.snapshot().iter() {
                    let Some(seed) = reg.seed_of(&lane.cfg.name) else { continue };
                    let realised = match ctl_b.lock() {
                        Ok(g) => {
                            g.ledger
                                .lanes
                                .get(&lane.cfg.name)
                                .map(|b| b.risk.realised_pnl)
                        }
                        Err(_) => None,
                    };
                    let Some(realised) = realised else {
                        eprintln!(
                            "[bankroll] missing lane ledger for {} — scale unchanged",
                            lane.cfg.name
                        );
                        continue;
                    };
                    let live = copybot_hot::budget::virtual_bankroll(seed, realised);
                    let want = copybot_hot::budget::bankroll_scale(live, seed);
                    let old = lane.state.cap_scale.load(Ordering::Relaxed) as f64
                        / copybot_hot::lanes::MICRO;
                    let next = copybot_hot::budget::slew(old, want);
                    if (next - old).abs() > 1e-4 {
                        lane.state
                            .cap_scale
                            .store(
                                (next * copybot_hot::lanes::MICRO) as i64,
                                Ordering::Relaxed,
                            );
                        em.emit(
                            serde_json::json!(
                                { "ev" : "bankroll", "lane" : & lane.cfg.name,
                                "realised_pnl" : (realised * 100.0).round() / 100.0,
                                "virtual_bankroll" : (live * 100.0).round() / 100.0, "seed"
                                : seed, "scale_from" : (old * 1000.0).round() / 1000.0,
                                "scale_to" : (next * 1000.0).round() / 1000.0, "slewed" :
                                (next - want).abs() > 1e-6, "t" : now_ms() }
                            ),
                        );
                    }
                }
            }
        });
}
