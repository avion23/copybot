//! Native merge execution and historical pair-ledger seeding.

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MergeOutcome {
    Merged,
    DidNotHappen,
    Unresolved,
}
pub(crate) async fn run_merge_native(
    http: &reqwest::Client,
    rpc_url: &str,
    key: &[u8; 32],
    safe: &[u8; 20],
    adapter: &[u8; 20],
    condition: &str,
    pairs: f64,
    chain_id: u64,
) -> Result<String, String> {
    if !(pairs.is_finite() && pairs > 0.0) {
        return Err(format!("refusing to merge {pairs} pairs"));
    }
    let cid = copybot_hot::merge::condition_id_bytes(condition)
        .ok_or_else(|| format!("unreadable conditionId {condition}"))?;
    let amount = (pairs * 1e6).floor() as u128;
    if amount == 0 {
        return Err("merge amount rounds to zero".into());
    }
    let inner = copybot_hot::merge::merge_calldata(
        &copybot_hot::merge::COLLATERAL_PUSD,
        &cid,
        &copybot_hot::merge::BINARY_PARTITION,
        amount,
    );
    let tx = copybot_hot::txsend::send_safe_call(
            http,
            rpc_url,
            key,
            safe,
            adapter,
            &inner,
            chain_id,
        )
        .await?;
    match copybot_hot::txsend::wait_receipt(
            http,
            rpc_url,
            &tx,
            copybot_hot::mergeclaim::EXECUTOR_TIMEOUT_SECS,
        )
        .await
    {
        Ok(true) => Ok(tx),
        Ok(false) => Err(format!("merge reverted on chain ({tx})")),
        Err(e) => Err(format!("{e} — outcome UNKNOWN, hold the pair")),
    }
}
pub(crate) async fn seed_pair_ledger(
    http: &reqwest::Client,
    leader: &str,
    ledger: &std::sync::Mutex<copybot_hot::pairledger::PairLedger>,
) -> Result<(usize, usize), String> {
    let mut all: Vec<serde_json::Value> = Vec::new();
    for offset in (0..4000).step_by(500) {
        let url = format!(
            "https://data-api.polymarket.com/activity?\
user={leader}&limit=500&offset={offset}"
        );
        let rows: Vec<serde_json::Value> = match http.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                r.json().await.map_err(|e| e.to_string())?
            }
            Ok(r) => return Err(format!("activity HTTP {}", r.status())),
            Err(e) => return Err(e.to_string()),
        };
        let n = rows.len();
        all.extend(rows);
        if n < 500 {
            break;
        }
    }
    let seeds = copybot_hot::pairledger::seed_rows_from_activity(&all);
    let n = seeds.len();
    let mut g = ledger.lock().map_err(|_| "pair ledger lock poisoned".to_string())?;
    g.seed(leader, &seeds);
    let with_balance = seeds
        .iter()
        .filter(|(_, c, _, _)| g.balance(leader, c).map(|b| b > 1e-6).unwrap_or(true))
        .map(|(_, c, _, _)| c.clone())
        .collect::<std::collections::HashSet<_>>()
        .len();
    Ok((n, with_balance))
}
