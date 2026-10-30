//! Read-only pool allocation and per-lane risk dashboard response.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use tokio::io::AsyncWriteExt;
use super::dashboard_model::PhysicalWallet;
use super::reconcile::ReanchorPulse;

pub(crate) struct PoolView<'a> {
    pub(crate) registry: &'a copybot_hot::wallets::WalletRegistry,
    pub(crate) physical_wallet: &'a Arc<Mutex<PhysicalWallet>>,
    pub(crate) router: &'a Arc<copybot_hot::lanes::Router>,
    pub(crate) control: &'a Arc<Mutex<copybot_hot::control::Control>>,
    pub(crate) reanchor: &'a ReanchorPulse,
    pub(crate) leader_pnl: &'a Arc<Mutex<HashMap<String, serde_json::Value>>>,
    pub(crate) drift: &'a Arc<Mutex<HashMap<String, f64>>>,
    pub(crate) funding: &'a Arc<Mutex<copybot_hot::funding::Funding>>,
    pub(crate) unattributed: &'a Arc<Mutex<HashMap<String, (String, f64, f64)>>>,
}

pub(crate) async fn serve_pool(
    sock: &mut tokio::net::TcpStream,
    view: PoolView<'_>,
) {
    let PoolView {
        registry: registry_c, physical_wallet: phys_c,
        router: r, control: c, reanchor: reanchor_d,
        leader_pnl: leader_pnl_d, drift: drift_d,
        funding: fund_pool2, unattributed: unattributed_d,
    } = view;
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
