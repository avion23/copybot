//! Wallet equity refresh and first-run seed capacity read model.
//! Mutates only the shared dashboard PhysicalWallet snapshot; no trading orders.

use super::dashboard_model::{marked_position_value, PhysicalWallet};
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn spawn(
    creds_slot: &Arc<std::sync::Mutex<Option<copybot_hot::auth::ApiCreds>>>,
    clob: &String,
    auth_addr: &String,
    funder: &String,
    physical_wallet: &Arc<std::sync::Mutex<PhysicalWallet>>,
    registry: &copybot_hot::wallets::WalletRegistry,
    sig_type: u8,
    hc: reqwest::Client,
) {
    let seed_registry = registry.clone();
        let (cs, cl, ad, wallet, cell) = (
            creds_slot.clone(),
            clob.clone(),
            auth_addr.clone(),
            funder.clone(),
            physical_wallet.clone(),
        );
        tokio::spawn(async move {
            for _ in 0..20 {
                if cs.lock().unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let mut first = true;
            loop {
                let creds = cs.lock().unwrap().clone();
                let cash = match creds {
                    Some(cr) => {
                        copybot_hot::matchup::free_cash(&hc, &cl, &ad, &cr, sig_type)
                            .await
                    }
                    None => None,
                };
                let pos_snap = copybot_hot::positions::fetch(
                        &hc,
                        "https://data-api.polymarket.com",
                        &wallet,
                        "0",
                        "",
                        copybot_hot::ledger::now_secs(),
                    )
                    .await;
                let marked = if pos_snap.completeness.is_complete() {
                    marked_position_value(&serde_json::Value::Array(pos_snap.rows))
                } else {
                    None
                };
                let equity = copybot_hot::budget::wallet_equity(cash, marked);
                {
                    let mut snapshot = cell.lock().unwrap();
                    if cash.is_some() {
                        snapshot.cash = cash;
                    }
                    if equity.is_some() {
                        snapshot.equity = equity;
                    }
                    if equity.is_some() {
                        snapshot.fetched_at = Some(copybot_hot::ledger::now_secs());
                        snapshot.stale_reason = None;
                    } else {
                        snapshot.stale_reason = Some(
                            if cash.is_some() {
                                "positions could not be valued — equity is the LAST GOOD one \
and its age is real"
                            } else {
                                "balance refresh returned nothing usable"
                            },
                        );
                    }
                }
                let total_seed: f64 = seed_registry
                    .specs()
                    .iter()
                    .filter(|w| w.enabled)
                    .map(|w| w.seed_usd)
                    .sum();
                if first && total_seed > 0.0 {
                    let fc = cash
                        .map(|c| format!("{c:.2}"))
                        .unwrap_or_else(|| "?".into());
                    let dep = marked
                        .map(|m| format!("{:.2}", m.max(0.0)))
                        .unwrap_or_else(|| "?".into());
                    match copybot_hot::budget::seeds_fit_wallet(total_seed, equity) {
                        Ok(()) => {
                            eprintln!(
                                "[seed] free cash ${fc} + deployed ${dep} = equity ${} ; seeds ${total_seed:.2} ; headroom ${}",
                                equity.map(| e | format!("{e:.2}")).unwrap_or_else(||
                                "unread".into()), equity.map(| e | format!("{:.2}", e -
                                total_seed)).unwrap_or_else(|| "?".into())
                            )
                        }
                        Err(e) => eprintln!("[seed] ⚠️  {e}"),
                    }
                    first = false;
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
}
