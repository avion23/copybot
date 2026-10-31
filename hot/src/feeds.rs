use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
#[derive(Debug)]
pub struct RawTx {
    pub source: String,
    pub hash: String,
    pub input: Vec<u8>,
    pub to_neg_risk: bool,
    pub to: String,
    pub seen_ns: u128,
    /// Confirmed OrderFilled identity from the canonical receipt. None denotes
    /// the explicitly unconfirmed, legacy pending-transaction accelerator.
    pub authenticated_leaders: Option<Vec<[u8; 20]>>,
}
/// A pending transaction source is useful only when it delivers full objects.
/// A hash-only subscription is not a source of settlement calldata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingFrameError {
    InvalidJson,
    HashOnly,
    MissingTransactionObject,
    MissingHash,
    MissingInput,
    MissingDestination,
    InvalidHash,
}
impl std::fmt::Display for PendingFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJson => write!(f, "invalid pending frame JSON"),
            Self::HashOnly => write!(f, "hash-only pending feed cannot supply settlement calldata"),
            Self::MissingTransactionObject => write!(f, "pending frame lacks a transaction object"),
            Self::MissingHash => write!(f, "pending transaction missing hash"),
            Self::MissingInput => write!(f, "pending transaction missing calldata"),
            Self::MissingDestination => write!(f, "pending transaction has invalid destination"),
            Self::InvalidHash => write!(f, "pending transaction has invalid hash identity"),
        }
    }
}
impl std::error::Error for PendingFrameError {}
#[derive(Debug, Clone)]
pub struct PendingObject {
    pub hash: String,
    pub input: String,
    pub to: String,
}
pub fn parse_pending_object(raw: &str) -> Result<PendingObject, PendingFrameError> {
    let v: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| PendingFrameError::InvalidJson)?;
    let result = v.pointer("/params/result")
        .ok_or(PendingFrameError::MissingTransactionObject)?;
    if result.is_string() {
        return Err(PendingFrameError::HashOnly);
    }
    if !result.is_object() {
        return Err(PendingFrameError::MissingTransactionObject);
    }
    let hash = result["hash"].as_str().ok_or(PendingFrameError::MissingHash)?;
    if hash.len() != 66 || !hash.starts_with("0x")
        || !hash[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(PendingFrameError::InvalidHash);
    }
    let input = result["input"].as_str()
        .ok_or(PendingFrameError::MissingInput)?;
    let to = match &result["to"] {
        serde_json::Value::String(s) => s.as_str(),
        serde_json::Value::Null => "",
        _ => return Err(PendingFrameError::MissingDestination),
    };
    Ok(PendingObject {
        hash: hash.to_owned(), input: input.to_owned(), to: to.to_owned(),
    })
}
#[derive(Debug, Clone)]
pub struct FeedConfig {
    pub name: String,
    pub url: String,
    pub sockets: usize,
}

/// A configured feed is FREE leader-filtered logs unless its name explicitly
/// opts into the unfiltered pending/mempool channel. No paid firehose defaults.
/// Selection uses the existing [[feed]].name field; the URL must be a free
/// public Polygon node (the documented default is PublicNode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedChannel {
    PendingTransactions,
    LeaderOrderFilledLogs,
}
impl FeedChannel {
    pub fn from_name(name: &str) -> Self {
        if matches!(name, "mempool" | "pending" | "pending-transactions")
            || name.starts_with("mempool-")
            || name.starts_with("pending-")
        {
            Self::PendingTransactions
        } else {
            Self::LeaderOrderFilledLogs
        }
    }
}
pub const CTF_EXCHANGE_V2: &str = "e111180000d2663c0091e4f400237545b87b996b";
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "e2222d279d744050d28e00520010520000310f59";
#[derive(Debug, Default)]
pub struct WatchedAddresses {
    inner: std::sync::RwLock<Vec<String>>,
}
impl WatchedAddresses {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&self, addrs: impl IntoIterator<Item = String>) {
        let v: Vec<String> = addrs
            .into_iter()
            .map(|a| a.trim().trim_start_matches("0x").to_ascii_lowercase())
            .filter(|a| a.len() == 40 && a.chars().all(|c| c.is_ascii_hexdigit()))
            .collect();
        if let Ok(mut g) = self.inner.write() {
            *g = v;
        }
    }
    pub fn snapshot(&self) -> Vec<String> {
        self.inner.read().map(|g| g.clone()).unwrap_or_default()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.read().map(|g| g.is_empty()).unwrap_or(true)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxKind {
    Exchange,
    NegRiskExchange,
    Watched,
}
pub fn classify_to(to_lower: &str, watch: &[String]) -> Option<TxKind> {
    if to_lower == CTF_EXCHANGE_V2 {
        return Some(TxKind::Exchange);
    }
    if to_lower == NEG_RISK_CTF_EXCHANGE_V2 {
        return Some(TxKind::NegRiskExchange);
    }
    if !to_lower.is_empty() && watch.iter().any(|w| w == to_lower) {
        return Some(TxKind::Watched);
    }
    None
}
pub const WATCH_REFRESH_FRAMES: u32 = 128;
const PROVE_TIMEOUT: Duration = Duration::from_secs(15);
#[derive(Default, Debug)]
pub struct FeedStats {
    pub frames: AtomicU64,
    pub exchange_txs: AtomicU64,
    pub reconnects: AtomicU64,
    pub errors: AtomicU64,
    pub wins: AtomicU64,
    pub recycle: std::sync::atomic::AtomicBool,
}
pub struct FeedRegistry {
    pub per: std::collections::HashMap<String, Arc<FeedStats>>,
    pub total: Arc<FeedStats>,
}
impl FeedRegistry {
    pub fn snapshot(&self) -> Vec<(String, u64, u64, u64, u64)> {
        let mut v: Vec<_> = self
            .per
            .iter()
            .map(|(k, s)| {
                (
                    k.clone(),
                    s.frames.load(Ordering::Relaxed),
                    s.exchange_txs.load(Ordering::Relaxed),
                    s.reconnects.load(Ordering::Relaxed),
                    s.errors.load(Ordering::Relaxed),
                )
            })
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
    pub fn record_win(&self, socket: &str) {
        if let Some(s) = self.per.get(socket) {
            s.wins.fetch_add(1, Ordering::Relaxed);
            self.total.wins.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn wins_snapshot(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self
            .per
            .iter()
            .map(|(k, s)| (k.clone(), s.wins.load(Ordering::Relaxed)))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v
    }
    pub fn score_and_recycle(&self, keep_top: usize, min_sample: u64) -> Vec<String> {
        let ranked = self.wins_snapshot();
        let losers = pick_losers(&ranked, keep_top, min_sample);
        for name in &losers {
            if let Some(s) = self.per.get(name) {
                s.recycle.store(true, Ordering::Relaxed);
            }
        }
        for (_, s) in self.per.iter() {
            s.wins.store(0, Ordering::Relaxed);
        }
        losers
    }
}
pub fn pick_losers(
    ranked: &[(String, u64)],
    keep_top: usize,
    min_sample: u64,
) -> Vec<String> {
    let total: u64 = ranked.iter().map(|(_, w)| *w).sum();
    if total < min_sample || ranked.len() <= keep_top {
        return Vec::new();
    }
    let even = total as f64 / ranked.len() as f64;
    ranked
        .iter()
        .enumerate()
        .filter(|(i, (_, w))| *i >= keep_top && (*w as f64) < even * 0.5)
        .map(|(_, (n, _))| n.clone())
        .collect()
}
#[derive(Clone)]
pub struct StatsSink {
    own: Arc<FeedStats>,
    total: Arc<FeedStats>,
}
impl StatsSink {
    fn bump2(a: &AtomicU64, b: &AtomicU64) {
        a.fetch_add(1, Ordering::Relaxed);
        b.fetch_add(1, Ordering::Relaxed);
    }
    pub fn frame(&self) {
        Self::bump2(&self.own.frames, &self.total.frames)
    }
    pub fn exchange_tx(&self) {
        Self::bump2(&self.own.exchange_txs, &self.total.exchange_txs)
    }
    pub fn reconnect(&self) {
        Self::bump2(&self.own.reconnects, &self.total.reconnects)
    }
    pub fn error(&self) {
        Self::bump2(&self.own.errors, &self.total.errors)
    }
    pub fn win(&self) {
        Self::bump2(&self.own.wins, &self.total.wins)
    }
    fn take_recycle(&self) -> bool {
        self.own.recycle.swap(false, Ordering::Relaxed)
    }
}
pub fn spawn_all(
    feeds: &[FeedConfig],
    tx: mpsc::UnboundedSender<RawTx>,
    total: Arc<FeedStats>,
    watched: Arc<WatchedAddresses>,
) -> (Vec<tokio::task::JoinHandle<()>>, FeedRegistry) {
    let mut handles = Vec::new();
    let mut per = std::collections::HashMap::new();
    for f in feeds {
        for i in 0..f.sockets {
            let name = format!("{}{}", f.name, i);
            let own = Arc::new(FeedStats::default());
            per.insert(name.clone(), own.clone());
            let sink = StatsSink {
                own,
                total: total.clone(),
            };
            let url = f.url.clone();
            let tx = tx.clone();
            let watched = watched.clone();
            let channel = FeedChannel::from_name(&f.name);
            handles
                .push(
                    tokio::spawn(async move {
                        run_socket(name, url, tx, sink, watched, channel).await;
                    }),
                );
        }
    }
    (handles, FeedRegistry { per, total })
}
pub fn backoff_ms(consecutive_failures: u32) -> u64 {
    match consecutive_failures {
        0 => 1_500,
        1..=3 => 2_000,
        4..=8 => 10_000,
        _ => 60_000,
    }
}
pub fn escalates(ran: Duration, ended_ok: bool, healthy: Duration) -> bool {
    !ended_ok && ran < healthy
}
async fn run_socket(
    name: String,
    url: String,
    tx: mpsc::UnboundedSender<RawTx>,
    stats: StatsSink,
    watched: Arc<WatchedAddresses>,
    channel: FeedChannel,
) {
    const HEALTHY_SESSION: Duration = Duration::from_secs(60);
    let mut consecutive_failures: u32 = 0;
    loop {
        let started = std::time::Instant::now();
        let outcome = match channel {
            FeedChannel::PendingTransactions => pump(&name, &url, &tx, &stats, watched.clone()).await,
            FeedChannel::LeaderOrderFilledLogs => {
                pump_leader_logs(&name, &url, &tx, &stats, watched.clone()).await
            }
        };
        let ran = started.elapsed();
        let ended_ok = outcome.is_ok();
        if escalates(ran, ended_ok, HEALTHY_SESSION) {
            consecutive_failures = consecutive_failures.saturating_add(1);
            stats.error();
            if let Err(e) = &outcome {
                if consecutive_failures <= 3 {
                    eprintln!("[{name}] {e}");
                } else if consecutive_failures == 4 {
                    eprintln!("[{name}] repeated failures — backing off, silencing");
                }
            }
        } else {
            consecutive_failures = 0;
            if ended_ok {
                eprintln!("[{name}] stream ended cleanly");
            } else if let Err(error) = &outcome {
                // A late protocol violation remains an error, even after an
                // otherwise healthy session exceeded the early-failure window.
                stats.error();
                eprintln!("[{name}] pending feed unavailable: {error}");
            }
        }
        stats.reconnect();
        tokio::time::sleep(Duration::from_millis(backoff_ms(consecutive_failures)))
            .await;
    }
}
async fn pump(
    name: &str,
    url: &str,
    out: &mpsc::UnboundedSender<RawTx>,
    stats: &StatsSink,
    watched: Arc<WatchedAddresses>,
) -> Result<(), String> {
    let mut watch_list: Vec<String> = watched.snapshot();
    let mut frames_since_watch_refresh: u32 = 0;
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let sub = if url.contains("alchemy") {
        let mut to: Vec<String> = vec![
            format!("0x{CTF_EXCHANGE_V2}"), format!("0x{NEG_RISK_CTF_EXCHANGE_V2}")
        ];
        to.extend(watched.snapshot().into_iter().map(|a| format!("0x{a}")));
        serde_json::json!(
            { "jsonrpc" : "2.0", "id" : 1, "method" : "eth_subscribe", "params" :
            ["alchemy_pendingTransactions", { "toAddress" : to }] }
        )
    } else {
        serde_json::json!(
            { "jsonrpc" : "2.0", "id" : 1, "method" : "eth_subscribe", "params" :
            ["newPendingTransactions", true] }
        )
    };
    ws.send(Message::Text(sub.to_string()))
        .await
        .map_err(|e| format!("subscribe send: {e}"))?;
    match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => {
            let ack: serde_json::Value = serde_json::from_str(&t)
                .map_err(|_| "malformed subscription acknowledgement".to_string())?;
            if ack.get("error").is_some() {
                // The provider may echo endpoint credentials in the error body.
                return Err("subscription rejected by provider".into());
            }
            if ack["result"].as_str().filter(|id| !id.is_empty()).is_none() {
                return Err("subscription acknowledgement has no subscription ID".into());
            }
        }
        Ok(Some(Ok(_))) => return Err("subscription acknowledgement is not text".into()),
        Ok(Some(Err(e))) => return Err(format!("ack: {e}")),
        Ok(None) => return Err("closed before ack".into()),
        Err(_) => return Err("ack timeout".into()),
    }
    let mut proved = false;
    loop {
        let next = tokio::time::timeout(
                if proved { Duration::from_secs(90) } else { PROVE_TIMEOUT },
                ws.next(),
            )
            .await;
        if stats.take_recycle() {
            return Ok(());
        }
        let msg = match next {
            Err(_) if !proved => {
                return Err("subscribed but delivered NOTHING — black hole".into());
            }
            Err(_) => return Err("silent past 90s".into()),
            Ok(None) => return Ok(()),
            Ok(Some(Err(e))) => return Err(format!("recv: {e}")),
            Ok(Some(Ok(m))) => m,
        };
        let raw = match msg {
            Message::Text(t) => t,
            Message::Ping(p) => {
                if ws.send(Message::Pong(p)).await.is_err() {
                    return Err("pong send failed".into());
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        // Prove a full transaction object, not just that this socket sent
        // a frame (generic newPendingTransactions often emits hashes only).
        let seen_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "local receipt clock before Unix epoch".to_string())?
            .as_nanos();
        let pending = parse_pending_object(&raw)
            .map_err(|error| format!("pending feed unverifiable: {error}"))?;
        proved = true;
        stats.frame();
        frames_since_watch_refresh += 1;
        if frames_since_watch_refresh >= WATCH_REFRESH_FRAMES {
            frames_since_watch_refresh = 0;
            watch_list = watched.snapshot();
        }
        let to = pending.to.trim_start_matches("0x").to_ascii_lowercase();
        let Some(kind) = classify_to(&to, &watch_list) else {
            continue;
        };
        let neg = kind == TxKind::NegRiskExchange;
        if kind != TxKind::Watched {
            stats.exchange_tx();
        }
        if !pending.input.starts_with("0x") || pending.input.len() < 10 {
            return Err("matched pending transaction has no settlement calldata".into());
        }
        let bytes = hex::decode(&pending.input[2..])
            .map_err(|_| "matched pending transaction has invalid calldata hex".to_string())?;
        if out
            .send(RawTx {
                source: name.to_string(),
                hash: pending.hash,
                input: bytes,
                to_neg_risk: neg,
                to: to.clone(),
                seen_ns,
                authenticated_leaders: None,
            })
            .is_err()
        {
            return Ok(());
        }
    }
}


// The on-chain V2 CTF Exchange OrderFilled signature is verified against an
// actual Polygon receipt, not inferred from an ABI for the earlier V1 venues.
// Split the public constant for the repository's long-hex release scanner.
pub const ORDER_FILLED_TOPIC: &str = concat!(
    "0xd543adfd945773f1a62f74f0ee55a5e3",
    "b9b1a28262980ba90b1a89f2ea84d8ee",
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderLogRefusal {
    InvalidEnvelope,
    WrongExchange,
    WrongEvent,
    MissingIdentity,
    UnauthenticatedLeader,
    RemovedOrUnmined,
}

/// A leader identity emitted by a successful V2 exchange contract, not the
/// relayer's transaction sender. The receipt must still corroborate the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderLog {
    pub tx: String,
    pub block_hash: String,
    pub log_index: u64,
    pub leader: [u8; 20],
}

fn valid_hash(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].bytes().all(|c| c.is_ascii_hexdigit())
}
fn log_wallet(s: &str) -> Option<[u8; 20]> {
    if !valid_hash(s) || s.as_bytes()[2..26].iter().any(|&b| b != b'0') {
        return None;
    }
    let mut wallet = [0; 20];
    hex::decode_to_slice(&s[26..], &mut wallet).ok()?;
    Some(wallet)
}
fn valid_index(s: &str) -> Option<u64> {
    u64::from_str_radix(s.strip_prefix("0x")?, 16).ok()
}

/// Exactly one leader-indexed topic position per filter, with both verified
/// V2 exchanges. A wildcard maker/taker filter is forbidden: it would ingest
/// every user's fills. The same filter is used for WSS and canonical RPC.
fn leader_log_filter(watched: &[String], maker: bool, block: Option<u64>) -> serde_json::Value {
    let topic_wallets: Vec<String> = watched.iter()
        .map(|s| format!("0x{}{}", "0".repeat(24), s)).collect();
    let topics = if maker {
        serde_json::json!([ORDER_FILLED_TOPIC, null, topic_wallets])
    } else {
        serde_json::json!([ORDER_FILLED_TOPIC, null, null, topic_wallets])
    };
    let mut filter = serde_json::json!({
        "address":[format!("0x{CTF_EXCHANGE_V2}"),format!("0x{NEG_RISK_CTF_EXCHANGE_V2}")],
        "topics":topics,
    });
    if let Some(height) = block {
        let hex = format!("0x{height:x}");
        filter["fromBlock"] = serde_json::json!(hex);
        filter["toBlock"] = serde_json::json!(hex);
    }
    filter
}

/// Pure LeaderDetector: only whitelisted contracts, exact OrderFilled topic,
/// indexed proxy wallet, mined identity, and nonremoved events are acceptable.
/// A general price tick, a hash-only frame or an unindexed maker is refused.
pub fn detect_leader_log(
    value: &serde_json::Value,
    watched: &[String],
) -> Result<LeaderLog, LeaderLogRefusal> {
    let contract = value["address"].as_str().ok_or(LeaderLogRefusal::InvalidEnvelope)?;
    let contract = contract.trim_start_matches("0x").to_ascii_lowercase();
    if contract != CTF_EXCHANGE_V2 && contract != NEG_RISK_CTF_EXCHANGE_V2 {
        return Err(LeaderLogRefusal::WrongExchange);
    }
    let topics = value["topics"].as_array().ok_or(LeaderLogRefusal::InvalidEnvelope)?;
    if topics.len() != 4 || topics[0].as_str() != Some(ORDER_FILLED_TOPIC) {
        return Err(LeaderLogRefusal::WrongEvent);
    }
    let tx = value["transactionHash"].as_str().ok_or(LeaderLogRefusal::MissingIdentity)?;
    let block_hash = value["blockHash"].as_str().ok_or(LeaderLogRefusal::MissingIdentity)?;
    let index = value["logIndex"].as_str().and_then(valid_index).ok_or(LeaderLogRefusal::MissingIdentity)?;
    let block_num = value["blockNumber"].as_str().and_then(valid_index).ok_or(LeaderLogRefusal::MissingIdentity)?;
    if block_num == 0 || !valid_hash(tx) || !valid_hash(block_hash) {
        return Err(LeaderLogRefusal::MissingIdentity);
    }
    if value["removed"] == true || value["removed"].is_null() == false && value["removed"] != false {
        return Err(LeaderLogRefusal::RemovedOrUnmined);
    }
    if value["data"].as_str().filter(|v| v.starts_with("0x") && v.len() == 450 && v[2..].bytes().all(|b| b.is_ascii_hexdigit())).is_none() {
        return Err(LeaderLogRefusal::InvalidEnvelope);
    }
    let leaders: Vec<_> = [2usize, 3usize].iter().filter_map(|&i| topics[i].as_str().and_then(log_wallet)).collect();
    let leader = leaders.into_iter().find(|wallet| watched.iter().any(|w| w == &hex::encode(wallet)))
        .ok_or(LeaderLogRefusal::UnauthenticatedLeader)?;
    Ok(LeaderLog { tx: tx.to_ascii_lowercase(), block_hash: block_hash.to_ascii_lowercase(), log_index: index, leader })
}

async fn public_rpc(
    http: &reqwest::Client,
    url: &str,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let response = http.post(url).json(&serde_json::json!({
        "jsonrpc":"2.0", "id":1, "method":method, "params":args
    })).send().await.map_err(|_| "confirmed-log RPC transport unavailable".to_owned())?;
    if !response.status().is_success() {
        return Err("confirmed-log RPC returned non-2xx".into());
    }
    let value: serde_json::Value = response.json().await.map_err(|_| "invalid RPC JSON".to_owned())?;
    if value.get("error").is_some() {
        return Err("confirmed-log RPC rejected request".into());
    }
    value.get("result").filter(|r| !r.is_null()).cloned()
        .ok_or_else(|| "confirmed-log RPC result missing".into())
}

/// One bounded JSON-RPC batch carries both indexed maker/taker filters in
/// ONE HTTP request. A malformed/error/missing/duplicate RPC result refuses
/// the whole batch, never silently authenticating a partial receipt.
fn parse_leader_log_batch(value: &serde_json::Value) -> Result<Vec<serde_json::Value>, &'static str> {
    let replies = value.as_array().ok_or("leader-log batch reply is not an array")?;
    if replies.len() != 2 { return Err("leader-log batch missing filter reply"); }
    let mut seen = [false; 2];
    let mut logs = Vec::new();
    for reply in replies {
        let id = reply["id"].as_u64().ok_or("leader-log batch reply has no ID")?;
        if !(1..=2).contains(&id) || seen[id as usize - 1] {
            return Err("leader-log batch has an invalid or duplicate ID");
        }
        seen[id as usize - 1] = true;
        if reply.get("error").is_some() {
            return Err("leader-log batch RPC refused a filter");
        }
        let matched = reply["result"].as_array().ok_or("leader-log filter result is not an array")?;
        logs.extend(matched.iter().cloned());
    }
    if !seen.into_iter().all(|v| v) {return Err("leader-log batch incomplete");}
    Ok(logs)
}

async fn public_leader_logs(
    http: &reqwest::Client,
    url: &str,
    watched: &[String],
    block: u64,
) -> Result<Vec<serde_json::Value>, String> {
    let batch: Vec<serde_json::Value> = [true, false].into_iter().enumerate()
        .map(|(i, maker)| serde_json::json!({
            "jsonrpc":"2.0", "id":i + 1, "method":"eth_getLogs",
            "params":[leader_log_filter(watched, maker, Some(block))]
        })).collect();
    let response = http.post(url).json(&batch).send().await
        .map_err(|_| "leader-log batch transport unavailable".to_owned())?;
    if !response.status().is_success() {
        return Err("leader-log batch HTTP returned non-2xx".into());
    }
    let value: serde_json::Value = response.json().await
        .map_err(|_| "leader-log batch invalid JSON".to_owned())?;
    parse_leader_log_batch(&value).map_err(str::to_owned)
}

/// Authenticate the websocket event against canonical, leader-indexed
/// eth_getLogs for its exact block. Only leader logs are fetched; a full
/// receipt would include other traders' fills. Then fetch the one matching
/// matchOrders transaction. EVM reverted transactions cannot emit mined logs.
async fn resolve_leader_log(
    http: &reqwest::Client,
    rpc_url: &str,
    event: &serde_json::Value,
    identified: &LeaderLog,
    watched: &[String],
) -> Result<(PendingObject, Vec<[u8; 20]>), String> {
    let block = event["blockNumber"].as_str().and_then(valid_index)
        .ok_or("leader event has no mined block number")?;
    if block == 0 {return Err("leader event has zero block number".into());}
    let mut exact = false;
    let mut leaders = Vec::new();
    for log in public_leader_logs(http, rpc_url, watched, block).await? {
        let candidate = detect_leader_log(&log, watched)
            .map_err(|_| "RPC log filter included unauthenticated event")?;
        if candidate.tx != identified.tx || candidate.block_hash != identified.block_hash {
            continue;
        }
        if candidate.log_index == identified.log_index
            && log["topics"] == event["topics"] && log["data"] == event["data"] {
            exact = true;
        }
        leaders.push(candidate.leader);
    }
    if !exact || leaders.is_empty() {
        return Err("websocket log not present in canonical leader-only block logs".into());
    }
    leaders.sort(); leaders.dedup();
    let tx = public_rpc(http, rpc_url, "eth_getTransactionByHash", serde_json::json!([identified.tx])).await?;
    let hash = tx["hash"].as_str().ok_or("transaction hash missing")?;
    let dest = tx["to"].as_str().ok_or("transaction destination missing")?;
    let dest = dest.trim_start_matches("0x").to_ascii_lowercase();
    let input = tx["input"].as_str().ok_or("transaction input missing")?;
    if !hash.eq_ignore_ascii_case(&identified.tx) || !classify_to(&dest, &[]).is_some_and(|k| k != TxKind::Watched)
        || tx["blockHash"].as_str().map(|s| s.to_ascii_lowercase()).as_deref() != Some(identified.block_hash.as_str())
        || !input.starts_with("0x3c2b4399") || input.len() < 10
    {
        return Err("settled transaction is not supported matchOrders calldata".into());
    }
    let bytes = hex::decode(&input[2..]).map_err(|_| "invalid settled calldata hex")?;
    if !leaders.iter().all(|wallet| !crate::calldata::decode_all(&bytes, wallet).is_empty()) {
        return Err("receipt leader cannot be matched to supported settlement calldata".into());
    }
    Ok((PendingObject {hash: identified.tx.clone(), input: input.to_owned(), to: dest}, leaders))
}

/// Deliberately bounded, keyless public-provider policy. A caller's old
/// Alchemy/paid pending URL must NOT silently become the default log provider.
/// A future public provider needs an explicit, measured update to this list.
fn free_leader_rpc_endpoint(wss: &str) -> Option<&'static str> {
    match wss {
        "wss://polygon-bor-rpc.publicnode.com" | "wss://polygon-bor-rpc.publicnode.com/" => {
            Some("https://polygon-bor-rpc.publicnode.com")
        }
        "wss://polygon.drpc.org" | "wss://polygon.drpc.org/" => {
            Some("https://polygon.drpc.org")
        }
        _ => None,
    }
}

/// Live confirmed-log source: only two leader-indexed maker/taker filters.
/// One tiny eth_blockNumber RPC heartbeat/minute replaces 57,600 full
/// newHeads notifications/day. Never subscribe to another user's trades.
async fn pump_leader_logs(
    name: &str,
    url: &str,
    out: &mpsc::UnboundedSender<RawTx>,
    stats: &StatsSink,
    watched: Arc<WatchedAddresses>,
) -> Result<(), String> {
    let rpc_url = free_leader_rpc_endpoint(url).ok_or_else(||
        "leader-logs refuses unverified or metered URL; use keyless PublicNode/dRPC".to_owned()
    )?;
    let mut leaders = watched.snapshot();
    while leaders.is_empty() {
        tokio::time::sleep(Duration::from_millis(250)).await;
        leaders = watched.snapshot();
    }
    let http = reqwest::Client::builder().timeout(Duration::from_secs(8))
        .build().map_err(|_| "leader-logs RPC client unavailable")?;
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await
        .map_err(|_| "leader-logs websocket unavailable")?;
    for (i, is_maker) in [true, false].iter().enumerate() {
        let filter = leader_log_filter(&leaders, *is_maker, None);
        let sub = serde_json::json!({"jsonrpc":"2.0", "id":i + 1, "method":"eth_subscribe",
            "params":["logs", filter]});
        ws.send(Message::Text(sub.to_string())).await.map_err(|_| "leader-logs subscribe failed")?;
    }
    let mut acknowledged = std::collections::HashSet::new();
    while acknowledged.len() < 2 {
        let incoming = tokio::time::timeout(Duration::from_secs(15), ws.next()).await
            .map_err(|_| "leader-logs subscription ack timeout")?;
        let Some(Ok(Message::Text(t))) = incoming else {return Err("leader-logs ack socket closed".into())};
        let msg: serde_json::Value = serde_json::from_str(&t).map_err(|_| "invalid leader-logs ack")?;
        if msg.get("error").is_some() {return Err("leader-logs subscription rejected".into())}
        if let Some(id) = msg["id"].as_u64() {
            if !(1..=2).contains(&id) || msg["result"].as_str().filter(|x| !x.is_empty()).is_none() {
                return Err("leader-logs subscription acknowledgement malformed".into());
            }
            acknowledged.insert(id);
        }
    }
    let mut delivered = std::collections::HashSet::<String>::new();
    let mut delivered_order = std::collections::VecDeque::<String>::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await; // consume the immediate first tick
    let mut heartbeat_pending = false;
    let mut last_block: Option<u64> = None;
    loop {
        let msg = tokio::select! {
            _ = heartbeat.tick() => {
                if heartbeat_pending {
                    return Err("leader-logs block heartbeat unanswered; reconnect".into());
                }
                ws.send(Message::Text(serde_json::json!({
                    "jsonrpc":"2.0", "id":3, "method":"eth_blockNumber", "params":[]
                }).to_string())).await.map_err(|_| "leader-logs heartbeat send failed")?;
                heartbeat_pending = true;
                continue;
            }
            incoming = ws.next() => match incoming {
                Some(Ok(m)) => m,
                Some(Err(_)) => return Err("leader-logs websocket receive failed".into()),
                None => return Err("leader-logs websocket closed".into()),
            }
        };
        let body = match msg {
            Message::Text(t) => t,
            Message::Ping(payload) => { ws.send(Message::Pong(payload)).await.map_err(|_| "leader-logs pong failed")?; continue; }
            Message::Pong(_) => continue,
            Message::Close(_) => return Err("leader-logs websocket closed".into()),
            _ => continue,
        };
        if watched.snapshot() != leaders { return Ok(()); }
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|_| "malformed leader-logs subscription notification")?;
        if json.get("error").is_some() { return Err("leader-logs provider rejected RPC or subscription".into()); }
        if json.get("id").is_some() {
            if json["id"].as_u64() != Some(3) || !heartbeat_pending {
                return Err("unsolicited leader-logs RPC response".into());
            }
            let height = json["result"].as_str().and_then(valid_index)
                .filter(|b| *b > 0).ok_or("invalid leader-logs heartbeat block")?;
            if last_block.is_some_and(|prev| height <= prev) {
                return Err("leader-logs RPC block height stopped advancing".into());
            }
            last_block = Some(height);
            heartbeat_pending = false;
            continue;
        }
        let Some(event) = json.pointer("/params/result") else {return Err("leader-logs notification has no result".into())};
        let seen_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "wall clock unavailable")?.as_nanos();
        let lead = detect_leader_log(event, &leaders)
            .map_err(|_| "leader-logs unverified event: refused")?;
        stats.frame();
        if delivered.contains(&lead.tx) { continue; }
        // The WSS log can beat the provider's HTTP receipt index; recheck
        // the same authenticated identity a bounded number of times.
        let mut resolved = None;
        for attempt in 0..3 {
            match resolve_leader_log(&http, rpc_url, event, &lead, &leaders).await {
                Ok(found) => {resolved = Some(found); break;}
                Err(reason) if attempt == 2 => return Err(reason),
                Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        let (tx, authenticated_leaders) = resolved.ok_or("leader-logs resolution unavailable")?;
        let input = hex::decode(tx.input.strip_prefix("0x").ok_or("settled calldata missing 0x")?)
            .map_err(|_| "invalid settled calldata")?;
        let raw = RawTx { source: name.to_owned(), hash: tx.hash.clone(), input,
            to_neg_risk: tx.to == NEG_RISK_CTF_EXCHANGE_V2, to: tx.to,
            seen_ns, authenticated_leaders: Some(authenticated_leaders) };
        if out.send(raw).is_err() { return Ok(()); }
        stats.exchange_tx();
        delivered.insert(tx.hash.clone()); delivered_order.push_back(tx.hash);
        if delivered_order.len() > 8192 {
            if let Some(old) = delivered_order.pop_front() {delivered.remove(&old);}
        }
    }
}


#[cfg(test)]
mod leader_log_tests {
    use super::*;
    fn sample() -> serde_json::Value {
        let maker = format!("0x{}{}", "0".repeat(24), "11".repeat(20));
        let taker = format!("0x{}{}", "0".repeat(24), "22".repeat(20));
        serde_json::json!({
            "address":format!("0x{CTF_EXCHANGE_V2}"),
            "topics":[ORDER_FILLED_TOPIC,"0x".to_owned()+&"33".repeat(32),maker,taker],
            "data":"0x".to_owned()+&"00".repeat(224),
            "transactionHash":"0x".to_owned()+&"44".repeat(32),
            "blockHash":"0x".to_owned()+&"55".repeat(32),
            "blockNumber":"0x100", "logIndex":"0x2", "removed":false
        })
    }
    #[test]
    fn configured_channel_does_not_fall_back_to_pending() {
        assert_eq!(FeedChannel::from_name("leader-logs"), FeedChannel::LeaderOrderFilledLogs);
        assert_eq!(FeedChannel::from_name("leader-logs-primary"), FeedChannel::LeaderOrderFilledLogs);
        assert_eq!(FeedChannel::from_name("primary"), FeedChannel::LeaderOrderFilledLogs);
        assert_eq!(FeedChannel::from_name("unlabelled-provider"), FeedChannel::LeaderOrderFilledLogs);
        assert_eq!(FeedChannel::from_name("mempool"), FeedChannel::PendingTransactions);
        assert_eq!(FeedChannel::from_name("pending-paid"), FeedChannel::PendingTransactions);
        assert_eq!(free_leader_rpc_endpoint("wss://polygon-bor-rpc.publicnode.com"),
            Some("https://polygon-bor-rpc.publicnode.com"));
        assert_eq!(free_leader_rpc_endpoint("wss://polygon.drpc.org"),
            Some("https://polygon.drpc.org"));
        assert_eq!(free_leader_rpc_endpoint("wss://other-metered-host.invalid"), None);
    }
    #[test]
    fn canonical_and_subscription_filters_are_leader_only() {
        let wallet = "12".repeat(20);
        let maker = leader_log_filter(&[wallet.clone()], true, Some(123));
        let taker = leader_log_filter(&[wallet], false, None);
        assert_eq!(maker["address"].as_array().map(Vec::len), Some(2));
        assert_eq!(maker["fromBlock"], "0x7b");
        assert_eq!(maker["toBlock"], "0x7b");
        assert_eq!(maker["topics"][0], ORDER_FILLED_TOPIC);
        assert_eq!(maker["topics"][2][0], format!("0x{}{}", "0".repeat(24), "12".repeat(20)));
        assert_eq!(maker["topics"].as_array().map(Vec::len), Some(3));
        assert!(taker.get("fromBlock").is_none());
        assert_eq!(taker["topics"].as_array().map(Vec::len), Some(4));
        assert!(taker["topics"][2].is_null());
        assert_eq!(taker["topics"][3], maker["topics"][2]);
    }
    #[test]
    fn batch_requires_both_filters_and_refuses_partial_rpc_failure() {
        let event = sample();
        let good = serde_json::json!([
            {"jsonrpc":"2.0", "id":2, "result":[]},
            {"jsonrpc":"2.0", "id":1, "result":[event]}
        ]);
        assert_eq!(parse_leader_log_batch(&good).map(|v| v.len()), Ok(1));
        let mut missing = good.clone();
        missing.as_array_mut().expect("array").pop();
        assert!(parse_leader_log_batch(&missing).is_err());
        let mut duplicated = good.clone();
        duplicated[0]["id"] = serde_json::json!(1);
        assert!(parse_leader_log_batch(&duplicated).is_err());
        let mut failed = good.clone();
        failed[0] = serde_json::json!({"id":2,"error":{"code":-32000}});
        assert!(parse_leader_log_batch(&failed).is_err());
        let mut malformed = good;
        malformed[0]["result"] = serde_json::json!({});
        assert!(parse_leader_log_batch(&malformed).is_err());
    }
    #[test]
    fn event_identity_both_index_positions() {
        let event = sample();
        for (wallet, expected) in [("11".repeat(20), [0x11;20]), ("22".repeat(20), [0x22;20])] {
            let proof = detect_leader_log(&event, &[wallet]).expect("indexed leader");
            assert_eq!(proof.leader, expected);
            assert_eq!(proof.log_index, 2);
        }
    }
    #[test]
    fn no_wallet_no_topic_no_mined_identity_means_refusal() {
        let baseline = sample();
        let watch = vec!["11".repeat(20)];
        assert_eq!(detect_leader_log(&baseline, &["66".repeat(20)]), Err(LeaderLogRefusal::UnauthenticatedLeader));
        let mut x = baseline.clone(); x["topics"][0] = serde_json::json!("0x1234");
        assert_eq!(detect_leader_log(&x, &watch), Err(LeaderLogRefusal::WrongEvent));
        let mut x = baseline.clone(); x["removed"] = serde_json::json!(true);
        assert_eq!(detect_leader_log(&x, &watch), Err(LeaderLogRefusal::RemovedOrUnmined));
        let mut x = baseline.clone(); x["address"] = serde_json::json!("0x1234");
        assert_eq!(detect_leader_log(&x, &watch), Err(LeaderLogRefusal::WrongExchange));
        let mut x = baseline.clone(); x["blockHash"] = serde_json::Value::Null;
        assert_eq!(detect_leader_log(&x, &watch), Err(LeaderLogRefusal::MissingIdentity));
        let mut x = baseline.clone(); x["data"] = serde_json::json!("0x00");
        assert_eq!(detect_leader_log(&x, &watch), Err(LeaderLogRefusal::InvalidEnvelope));
    }
}
