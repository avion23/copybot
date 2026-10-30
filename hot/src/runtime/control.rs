//! Control ticking and persistent buy-halt incidents at the runtime edge.

use super::telemetry::{now_ms, Emitter};
use copybot_hot::control::Control;
use copybot_hot::lanes::Router;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;


pub(crate) fn spawn_guardian_liveness(
    control: &Arc<std::sync::Mutex<Control>>,
    control_path: &str,
) {
        let gate = control.lock().unwrap().guardian_stale.clone();
        let gpath = std::path::Path::new(control_path)
            .parent()
            .map(|d| d.join("guardian_state.json"))
            .unwrap_or_else(|| std::path::PathBuf::from("run/guardian_state.json"));
        eprintln!(
            "[guardwatch] watching {} (buys gate after {}s without a completed                    guardian pass; exits are never gated)",
            gpath.display(), copybot_hot::guardwatch::MAX_AGE_SECS
        );
        tokio::spawn(async move {
            let mut w = copybot_hot::guardwatch::Watch::new(
                copybot_hot::ledger::now_secs(),
            );
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            let mut was_gated = false;
            let mut warned_unarmed = false;
            loop {
                tick.tick().await;
                let mtime = std::fs::metadata(&gpath)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64);
                let v = w.observe(mtime, copybot_hot::ledger::now_secs());
                let gated = v.buys_gated();
                gate.store(gated, Ordering::Relaxed);
                if gated != was_gated {
                    eprintln!("[guardwatch] {}", v.describe());
                    was_gated = gated;
                }
                if matches!(v, copybot_hot::guardwatch::Verdict::Warmup { .. })
                    && !warned_unarmed
                {
                    eprintln!("[guardwatch] {}", v.describe());
                    warned_unarmed = true;
                }
            }
        });
}

pub(crate) fn latch_buys(
    lane: &copybot_hot::lanes::Lane,
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    kind: &str,
    why: &str,
) {
    let mut g = match incidents.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    };
    let persisted = g.raise(&lane.cfg.name, kind, why, copybot_hot::ledger::now_secs());
    lane.state.halt_latch.store(true, Ordering::Relaxed);
    match persisted {
        Ok(true) => {
            eprintln!(
                "[{}] INCIDENT {kind}: {why} — buys halted until cleared \
                               (exits keep mirroring)",
                lane.cfg.name
            )
        }
        Ok(false) => {}
        Err(e) => {
            eprintln!(
                "[{}] CRITICAL: buys latched but the incident could not be \
                             persisted ({e}) — a restart may forget this",
                lane.cfg.name
            )
        }
    }
}

/// Only one control task; exit or panic aborts the process to preserve the kill switch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_supervised(
    control: &Arc<std::sync::Mutex<Control>>,
    router: &Arc<Router>,
    pending_log: &Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    emitter: &Arc<Emitter>,
    watch_tokens: &Arc<std::sync::Mutex<Vec<String>>>,
    resting_book: &Arc<copybot_hot::resting::RestingBook>,
    levels: &Arc<copybot_hot::book::Levels>,
    prints: &Arc<copybot_hot::book::TradePrints>,
) {
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

pub(crate) async fn control_task(
    ctl: Arc<std::sync::Mutex<Control>>,
    router: Arc<Router>,
    pend_ctl: Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    emitter: Arc<Emitter>,
    watch: Arc<std::sync::Mutex<Vec<String>>>,
    resting: Arc<copybot_hot::resting::RestingBook>,
    levels_ctl: Arc<copybot_hot::book::Levels>,
    prints_ctl: Arc<copybot_hot::book::TradePrints>,
) {
    let mut last = String::new();
    loop {
        {
            let mut c = ctl.lock().unwrap();
            let in_flight = pend_ctl.lock().unwrap().in_flight();
            c.tick_with(&router, &in_flight);
            for lane in router.snapshot().iter() {
                if !copybot_hot::lanes::needs_book(lane) {
                    continue;
                }
                let held = c.ledger.holdings(&lane.cfg.name);
                let mut w = watch.lock().unwrap();
                for (t, sh) in held.iter() {
                    if *sh <= 1e-9 {
                        continue;
                    }
                    if !w.iter().any(|x| x == t) {
                        w.push(t.clone());
                    }
                }
                let pinned: std::collections::HashSet<String> = resting
                    .all()
                    .into_iter()
                    .map(|r| r.token)
                    .chain(
                        held.iter().filter(|(_, s)| **s > 1e-9).map(|(t, _)| t.clone()),
                    )
                    .collect();
                copybot_hot::book::evict_watch_over_cap(
                    &mut w,
                    copybot_hot::book::WATCH_CAP,
                    &pinned,
                );
                let keep: std::collections::HashSet<String> = w
                    .iter()
                    .cloned()
                    .collect();
                drop(w);
                levels_ctl.retain_tokens(&keep);
                prints_ctl.retain_tokens(&keep);
            }
            let snap = c.status(&router).to_string();
            if snap != last {
                emitter
                    .emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "control", "lanes" : c.status(&
                            router) ["lanes"] }
                        ),
                    );
                last = snap;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
