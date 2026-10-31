//! Indexed-leader WebSocket owner: gap recovery, typed receipt admission and Data API corroboration.
use crate::feeds::*;
use crate::feeds::{free_leader_rpc_endpoint,leader_log_filter,public_rpc,public_leader_logs,resolve_leader_log,valid_index};
use futures_util::{SinkExt,StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Shared across PublicNode, dRPC, and historical reads. Transaction identity
/// suppresses duplicate downstream processing; each log retains its distinct
/// (hash, log index) identity for completeness accounting.
#[derive(Default)]
pub(crate) struct SharedLeaderLogs {
    keys: HashSet<(String, u64)>,
    key_order: VecDeque<(String, u64)>,
    txs: HashSet<String>,
    tx_order: VecDeque<String>,
    assets: HashMap<(String, String), HashMap<String, u64>>,
}
pub(crate) type SharedSeen = Arc<Mutex<SharedLeaderLogs>>;
impl SharedLeaderLogs {
    fn has_tx(&self, tx: &str) -> bool {self.txs.contains(tx)}
    fn has_trade(&self, wallet: &str, tx: &str, asset: &str, timestamp: u64) -> bool {
        self.assets.get(&(wallet.to_owned(), tx.to_owned()))
            .and_then(|assets| assets.get(asset))
            .is_some_and(|received| received.abs_diff(timestamp) <= API_GRACE_SECS)
    }
    fn remember(&mut self, raw: &RawTx) -> Result<bool, &'static str> {
        let fills = raw.confirmed_fills.as_ref().filter(|v| !v.is_empty())
            .ok_or("no confirmed maker fills to remember")?;
        let received=u64::try_from(raw.seen_ns/1_000_000_000)
            .map_err(|_|"leader receipt timestamp overflow")?;
        for fill in fills {
            let key = (raw.hash.to_ascii_lowercase(), fill.log_index);
            if self.keys.insert(key.clone()) {self.key_order.push_back(key);}
            self.assets.entry((hex::encode(fill.wallet), raw.hash.to_ascii_lowercase()))
                .or_default().entry(fill.order.token_id.clone()).or_insert(received);
        }
        while self.key_order.len() > 32_768 {
            if let Some(old) = self.key_order.pop_front() {self.keys.remove(&old);}
        }
        let fresh = self.txs.insert(raw.hash.to_ascii_lowercase());
        if fresh {self.tx_order.push_back(raw.hash.to_ascii_lowercase());}
        while self.tx_order.len() > 16_384 {
            if let Some(old) = self.tx_order.pop_front() {
                self.txs.remove(&old);
                self.assets.retain(|(_, tx), _| tx != &old);
            }
        }
        Ok(fresh)
    }
}
fn deliver_unique(shared: &SharedSeen, out: &mpsc::UnboundedSender<RawTx>,
    raw: RawTx) -> Result<bool, String> {
    let mut registry = shared.lock().map_err(|_| "leader identity registry poisoned")?;
    let fresh=registry.remember(&raw).map_err(str::to_owned)?;
    // Sending into this queue is synchronous, not a durable downstream ack.
    // If it fails the caller refuses cursor advancement.
    if fresh {out.send(raw).map_err(|_| "leader fill receiver closed".to_owned())?;}
    Ok(fresh)
}
fn cursor_path(name: &str) -> Result<PathBuf, String> {
    if name.is_empty() || !name.bytes().all(|b|
        b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
    ) {return Err("unsafe leader feed cursor name".into());}
    let base = PathBuf::from(std::env::var("COPYBOT_FEED_STATE_DIR")
        .map_err(|_| "leader feed requires COPYBOT_FEED_STATE_DIR")?);
    if !base.is_absolute() {return Err("feed state dir must be an absolute path".into());}
    let dir=base.join("leader-logs");
    std::fs::create_dir_all(&dir).map_err(|_| "leader feed state dir inaccessible")?;
    Ok(dir.join(format!("{name}.cursor")))
}
fn cursor_wallets(watched: &[String]) -> Vec<String> {
    let mut wallets=watched.to_vec();wallets.sort();wallets.dedup();wallets
}
fn restore_cursor(path: &Path, watched: &[String]) -> Result<Option<u64>, String> {
    let body = match std::fs::read_to_string(path) {
        Ok(b)=>b,
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(None),
        Err(_)=>return Err("leader feed cursor unreadable".into()),
    };
    let checkpoint:serde_json::Value=serde_json::from_str(&body)
        .map_err(|_| "leader cursor malformed")?;
    if checkpoint["version"]!=1 || checkpoint["wallets"]!=serde_json::json!(cursor_wallets(watched)) {
        return Err("leader cursor schema or watched wallets changed: reconcile manually".into());
    }
    checkpoint["block"].as_u64().filter(|v|*v>0).map(Some)
        .ok_or_else(|| "leader cursor block invalid".into())
}
fn persist_cursor(path: &Path, watched: &[String], cursor: u64) -> Result<(), String> {
    use std::io::Write;
    if cursor==0 {return Err("leader feed cursor zero".into());}
    let content=serde_json::to_vec(&serde_json::json!({
        "version":1, "wallets":cursor_wallets(watched),"block":cursor
    })).map_err(|_| "leader cursor serialization failed")?;
    let nanos=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "leader cursor clock unavailable")?.as_nanos();
    let tmp=path.with_extension(format!("cursor.{}.{}.tmp",std::process::id(),nanos));
    let mut f=std::fs::OpenOptions::new().write(true).create_new(true)
        .open(&tmp).map_err(|_|"leader cursor temporary file unavailable")?;
    f.write_all(&content).and_then(|_|f.sync_all())
        .map_err(|_|"leader cursor sync failed")?;
    std::fs::rename(&tmp,path).map_err(|_|"leader cursor replace failed")?;
    std::fs::File::open(path.parent().ok_or("leader cursor no parent")?)
        .and_then(|dir|dir.sync_all()).map_err(|_|"leader cursor directory sync failed".into())
}


#[derive(Default,Debug)]
pub struct RecoveryCursor {pub scanned_through:Option<u64>,pub chunks:u64}
#[derive(Debug,PartialEq,Eq)]
pub enum GapRefusal { Noncontiguous, Reversed, Overflow }
impl RecoveryCursor {
    /// The source refuses to skip a block or move backwards. The new
    /// checkpoint is adopted only after all candidate rows were verified.
    pub fn advance(&mut self,from:u64,to:u64)->Result<(),GapRefusal>{
        if to<from {return Err(GapRefusal::Reversed);}
        if let Some(last)=self.scanned_through {
            let next=last.checked_add(1).ok_or(GapRefusal::Overflow)?;
            if from!=last && from!=next {return Err(GapRefusal::Noncontiguous);}
            if to<last{return Err(GapRefusal::Noncontiguous);}
        }
        self.scanned_through=Some(to);
        self.chunks+=1;
        Ok(())
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    #[test]
    fn checkpoint_refuses_skipped_replayed_or_reversed_blocks() {
        let mut c=RecoveryCursor::default();
        assert_eq!(c.advance(10,19),Ok(()));
        assert_eq!(c.scanned_through,Some(19));
        assert_eq!(c.advance(21,25),Err(GapRefusal::Noncontiguous));
        assert_eq!(c.advance(19,19),Ok(())); // inclusive restart overlap
        assert_eq!(c.advance(20,18),Err(GapRefusal::Reversed));
        assert_eq!(c.scanned_through,Some(19));
        assert_eq!(c.advance(20,25),Ok(()));
        assert_eq!(c.scanned_through,Some(25));
        assert_eq!(c.chunks,3);
    }
}

async fn accept_live_event(
    name:&str,
    http:&reqwest::Client,
    rpc_url:&str,
    out:&mpsc::UnboundedSender<RawTx>,
    stats:&StatsSink,
    leaders:&[String],
    cursor:&mut RecoveryCursor,
    path:&Path,
    shared:&SharedSeen,
    event:&serde_json::Value,
    received_ns:u128,
)->Result<(),String>{
    let lead=detect_leader_log(event,leaders)
        .map_err(|_|"leader-logs unverified event: refused")?;
    stats.frame();
    if shared.lock().map_err(|_|"leader identity registry poisoned")?.has_tx(&lead.tx){
        return Ok(());
    }
    if lead.block_number>cursor.scanned_through.unwrap_or(0).saturating_add(1) {
        backfill(name,http,rpc_url,out,leaders,cursor,lead.block_number-1,path,shared).await?;
    }
    // The websocket often precedes HTTP indexing. Refuse rather than
    // silently ignore proof mismatch after a small bounded retry.
    let mut resolved=None;
    for attempt in 0..3 {
        match resolve_leader_log(http,rpc_url,event,&lead,leaders).await {
            Ok(v)=>{resolved=Some(v);break;},
            Err(reason) if attempt==2=>return Err(reason),
            Err(_)=>tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    let (tx,fills)=resolved.ok_or("leader log resolution unavailable")?;
    let mut owners:Vec<_>=fills.iter().map(|f|f.wallet).collect();
    owners.sort();owners.dedup();
    let input=hex::decode(tx.input.strip_prefix("0x").ok_or("settled calldata missing 0x")?)
        .map_err(|_|"invalid settled calldata")?;
    let raw=RawTx {
        source:name.to_owned(),hash:tx.hash.clone(),input,
        to_neg_risk:tx.to==NEG_RISK_CTF_EXCHANGE_V2,to:tx.to,
        seen_ns:received_ns,authenticated_leaders:Some(owners),
        confirmed_fills:Some(fills.clone()),stage:FillStage::ConfirmedLive,
    };
    if deliver_unique(shared,out,raw)? {
        stats.exchange_tx();
        if let Some(block_time)=fills.first().map(|f|f.block_time) {
            let ts_ms=u128::from(block_time).saturating_mul(1000);
            let receipt_ms=received_ns/1_000_000;
            if receipt_ms>=ts_ms {
                eprintln!("{}",serde_json::json!({
                    "ev":"leader_log_lag","source":name,"tx":tx.hash,
                    "block_timestamp_to_receipt_ms":receipt_ms-ts_ms,
                    "basis":"block_timestamp_proxy_not_exact_inclusion"
                }));
            } else {
                eprintln!("{}",serde_json::json!({
                    "ev":"leader_log_lag_refused","source":name,
                    "reason":"block_timestamp_ahead_of_socket_receipt"
                }));
            }
        }
    }
    Ok(())
}

/// Live confirmed-log source: only two leader-indexed maker/taker filters.
/// One tiny eth_blockNumber RPC heartbeat/minute replaces 57,600 full
/// newHeads notifications/day. Never subscribe to another user's trades.
pub(crate) async fn pump_leader_logs(
    name: &str,
    url: &str,
    out: &mpsc::UnboundedSender<RawTx>,
    stats: &StatsSink,
    watched: Arc<WatchedAddresses>,
    scanned_through: &mut RecoveryCursor,
    shared: &SharedSeen,
) -> Result<(), String> {
    let rpc_url = free_leader_rpc_endpoint(url).ok_or_else(||
        "leader-logs refuses unverified or metered URL; use keyless PublicNode/dRPC".to_owned()
    )?;
    let mut leaders = watched.snapshot();
    while leaders.is_empty() {
        tokio::time::sleep(Duration::from_millis(250)).await;
        leaders = watched.snapshot();
    }
    let path=cursor_path(name)?;
    let saved=restore_cursor(&path,&leaders)?;
    if scanned_through.scanned_through.is_none() {
        scanned_through.scanned_through=saved;
    } else if scanned_through.scanned_through != saved {
        return Err("leader cursor changed outside this socket".into());
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
    let mut buffered = Vec::new();
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
            if !acknowledged.insert(id) {return Err("duplicate leader subscription acknowledgement".into());}
        } else if let Some(event)=msg.pointer("/params/result") {
            if buffered.len()>=256 {return Err("leader subscription early notification buffer full".into());}
            let received_ns=std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_|"live feed wall clock unavailable")?.as_nanos();
            buffered.push((event.clone(),received_ns));
        } else {return Err("unrecognized leader subscription acknowledgement".into());}
    }
    // Subscribe FIRST, then scan the last 24h on cold boot or the exact
    // unscanned interval after reconnect. Any queued live frames are safe.
    let head=head_number(&http,rpc_url).await?;
    backfill(name,&http,rpc_url,out,&leaders,scanned_through,head,&path,shared).await?;
    for (event,received_ns) in buffered {
        accept_live_event(name,&http,rpc_url,out,stats,&leaders,scanned_through,
            &path,shared,&event,received_ns).await?;
    }
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
            backfill(name,&http,rpc_url,out,&leaders,scanned_through,height,&path,shared).await?;
            continue;
        }
        let Some(event) = json.pointer("/params/result") else {return Err("leader-logs notification has no result".into())};
        let received_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_|"live feed wall clock unavailable")?.as_nanos();
        accept_live_event(name,&http,rpc_url,out,stats,&leaders,scanned_through,
            &path,shared,event,received_ns).await?;
    }
}

async fn proved_raw(name:&str,http:&reqwest::Client,rpc:&str,event:&serde_json::Value,leaders:&[String],stage:FillStage)->Result<RawTx,String>{
    let id=detect_leader_log(event,leaders).map_err(|_|"unverifiable leader event")?;
    let (tx,fills)=resolve_leader_log(http,rpc,event,&id,leaders).await?;
    let input=hex::decode(tx.input.strip_prefix("0x").ok_or("settled calldata no prefix")?)
        .map_err(|_|"invalid settled calldata")?;
    let mut owners:Vec<_>=fills.iter().map(|f|f.wallet).collect();
    owners.sort();owners.dedup();
    let seen_ns=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|_|"local receipt clock unavailable")?.as_nanos();
    Ok(RawTx{source:name.into(),hash:tx.hash,input,
        to_neg_risk:tx.to==NEG_RISK_CTF_EXCHANGE_V2,to:tx.to,
        seen_ns,authenticated_leaders:Some(owners),confirmed_fills:Some(fills),stage})
}
async fn head_number(http:&reqwest::Client,rpc:&str)->Result<u64,String>{
    let v=public_rpc(http,rpc,"eth_blockNumber",serde_json::json!([])).await?;
    v.as_str().and_then(valid_index).filter(|b|*b>0).ok_or_else(||"invalid head height".into())
}
async fn block_time(http:&reqwest::Client,rpc:&str,height:u64)->Result<u64,String>{
    let v=public_rpc(http,rpc,"eth_getBlockByNumber",
        serde_json::json!([format!("0x{height:x}"),false])).await?;
    v["timestamp"].as_str().and_then(valid_index).filter(|x|*x>0)
        .ok_or_else(||"missing mined block timestamp".into())
}
async fn history_start(http:&reqwest::Client,rpc:&str,head:u64)->Result<u64,String>{
    let cutoff=block_time(http,rpc,head).await?.saturating_sub(86_400);
    let mut lo=head.saturating_sub(100_000);
    if block_time(http,rpc,lo).await?>cutoff {
        return Err("24h chain window exceeds 100000-block search cap".into());
    }
    let mut hi=head;
    while lo<hi {
        let mid=lo+(hi-lo)/2;
        if block_time(http,rpc,mid).await?<cutoff {lo=mid+1;} else {hi=mid;}
    }
    Ok(lo)
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
enum BackfillRpcRefusal {
    InvalidRange,Transport,HttpStatus,InvalidJson,ProviderRejectedRange,MalformedReply,
}
impl BackfillRpcRefusal {
    fn kind(self)->&'static str {
        match self {
            Self::InvalidRange=>"invalid_range",
            Self::Transport=>"transport",
            Self::HttpStatus=>"http_non_2xx",
            Self::InvalidJson=>"invalid_json",
            Self::ProviderRejectedRange=>"provider_rejected_range",
            Self::MalformedReply=>"malformed_batch_reply",
        }
    }
}
fn parse_backfill_reply(v:&serde_json::Value)
    ->Result<Vec<serde_json::Value>,BackfillRpcRefusal>{
    crate::feeds::parse_leader_log_batch(v).map_err(|err| {
        if err=="leader-log batch RPC refused a filter" {
            BackfillRpcRefusal::ProviderRejectedRange
        } else {BackfillRpcRefusal::MalformedReply}
    })
}
async fn leader_logs_range(http:&reqwest::Client,rpc:&str,leaders:&[String],from:u64,to:u64)
    ->Result<Vec<serde_json::Value>,BackfillRpcRefusal>{
    if to<from||to-from>=512{return Err(BackfillRpcRefusal::InvalidRange);}
    let batch:Vec<_>=[true,false].into_iter().enumerate().map(|(i,maker)|{
        let mut filter=leader_log_filter(leaders,maker,Some(from));
        filter["toBlock"]=serde_json::json!(format!("0x{to:x}"));
        serde_json::json!({"jsonrpc":"2.0","id":i+1,"method":"eth_getLogs","params":[filter]})
    }).collect();
    let response=http.post(rpc).json(&batch).send().await
        .map_err(|_|BackfillRpcRefusal::Transport)?;
    if !response.status().is_success(){return Err(BackfillRpcRefusal::HttpStatus);}
    let v:serde_json::Value=response.json().await
        .map_err(|_|BackfillRpcRefusal::InvalidJson)?;
    parse_backfill_reply(&v)
}
/// A typed scanned-through cursor is retained across reconnects. It advances
/// only after authenticated historical rows have reached the receiver.
/// Recovered events are inspection-only; never replay a stale order.
async fn backfill(name:&str,http:&reqwest::Client,rpc:&str,out:&mpsc::UnboundedSender<RawTx>,
    leaders:&[String],cursor:&mut RecoveryCursor,head:u64,
    path:&Path,shared:&SharedSeen,
)->Result<(),String>{
    // Include the last scanned block to cover same-block late propagation
    // and a reconnect after an incomplete block. Dedup is observation-only.
    let start=match cursor.scanned_through {Some(last)=>last.checked_add(1).ok_or("cursor at maximum height")?,None=>history_start(http,rpc,head).await?};
    if start>head{return Ok(());}
    if head-start>100_000{return Err("gap beyond 100000 blocks: manual reconciliation required".into());}
    let mut from=start;
    while from<=head {
        let to=from.saturating_add(511).min(head);
        let logs=leader_logs_range(http,rpc,leaders,from,to).await.map_err(|refusal| {
            eprintln!("{}",serde_json::json!({
                "ev":"leader_backfill_refused","source":name,
                "from_block":from,"to_block":to,"reason":refusal.kind()
            }));
            format!("leader backfill range {from}-{to} refused: {}",refusal.kind())
        })?;
        let mut by_tx=std::collections::BTreeMap::new();
        for log in logs {
            let proof=detect_leader_log(&log,leaders).map_err(|_|"backfill returned nonleader event")?;
            if proof.block_number<from||proof.block_number>to {
                return Err("backfill RPC returned event outside requested block range".into());
            }
            if leaders.iter().any(|w|*w==hex::encode(proof.maker)){
                by_tx.entry(proof.tx).or_insert(log);
            }
        }
        for (tx,event) in by_tx {
            if shared.lock().map_err(|_|"leader identity registry poisoned")?.has_tx(&tx){continue;}
            let raw=proved_raw(name,http,rpc,&event,leaders,FillStage::Recovered).await?;
            deliver_unique(shared,out,raw)?;
        }
        let mut next=RecoveryCursor {scanned_through:cursor.scanned_through,chunks:cursor.chunks};
        next.advance(from,to).map_err(|_|"noncontiguous leader backfill refused")?;
        persist_cursor(path,leaders,to)?;
        *cursor=next;
        if to==u64::MAX{break;}
        from=to+1;
    }
    Ok(())
}


/// Out-of-band, typed coverage diagnostics. A missed API witness is never
/// admitted to the live order channel, even when chain data later corroborates it.
#[derive(Debug,serde::Serialize)]
#[serde(tag="ev",rename_all="snake_case")]
enum CoverageEvent {
    MissedLeaderFill {
        tx_hash:String,leader:String,asset:String,grace_secs:u64,
    },
    CorroborationRefused {reason:&'static str,leader:String},
}
fn emit_coverage(event: CoverageEvent) {
    eprintln!("{}",serde_json::to_string(&event).expect("static typed coverage event"));
}
const API_LIMIT:usize=100;
const API_GRACE_SECS:u64=180;
#[derive(Debug,PartialEq,Eq)]
enum ApiDecision {Immature,Matched,Missed}
fn assess_api_trade(start:u64,now:u64,timestamp:u64,matched:bool)->ApiDecision {
    if timestamp<start || now.saturating_sub(timestamp)<API_GRACE_SECS {
        ApiDecision::Immature
    } else if matched {ApiDecision::Matched} else {ApiDecision::Missed}
}
/// Once a minute, compare the leader's bounded /trades page to actual
/// decoded, maker-authenticated WSS/backfill events. A truncated or invalid
/// response is UNKNOWN, never proof that no fills were missed.
pub(crate) async fn corroborate_data_api(
    provider_url:String,out:mpsc::UnboundedSender<RawTx>,watched:Arc<WatchedAddresses>,
    shared:SharedSeen,
) {
    let Some(rpc)=free_leader_rpc_endpoint(&provider_url) else {
        emit_coverage(CoverageEvent::CorroborationRefused {
            reason:"unverified_polygon_rpc",leader:String::new()});return;
    };
    let http=match reqwest::Client::builder().timeout(Duration::from_secs(10)).build(){
        Ok(client)=>client,Err(_)=>{
            emit_coverage(CoverageEvent::CorroborationRefused {
                reason:"http_client_unavailable",leader:String::new()});return;
        }
    };
    let mut baseline=HashMap::<String,u64>::new();
    let mut reported=HashSet::<(String,String,String)>::new();
    let mut reported_order=VecDeque::new();
    let mut tick=tokio::time::interval(Duration::from_secs(60));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let leaders=watched.snapshot();
        if leaders.len()>16 {
            emit_coverage(CoverageEvent::CorroborationRefused {
                reason:"more_than_16_leaders",leader:String::new()});continue;
        }
        let now=match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH){
            Ok(v)=>v.as_secs(),Err(_)=>{
                emit_coverage(CoverageEvent::CorroborationRefused {
                    reason:"clock_unavailable",leader:String::new()});continue;
            }
        };
        for wallet in leaders {
            let url=format!("https://data-api.polymarket.com/trades?user=0x{wallet}&limit={API_LIMIT}&offset=0");
            let response=match http.get(&url).send().await{
                Ok(v)=>v,Err(_)=>{
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"transport",leader:wallet});continue;
                }
            };
            if !response.status().is_success() {
                emit_coverage(CoverageEvent::CorroborationRefused {
                    reason:"http_non_2xx",leader:wallet});continue;
            }
            let records:serde_json::Value=match response.json().await{
                Ok(v)=>v,Err(_)=>{
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"invalid_json",leader:wallet});continue;
                }
            };
            let Some(rows)=records.as_array() else {
                emit_coverage(CoverageEvent::CorroborationRefused {
                    reason:"not_an_array",leader:wallet});continue;
            };
            if rows.len()>=API_LIMIT {
                emit_coverage(CoverageEvent::CorroborationRefused {
                    reason:"truncated_page",leader:wallet});continue;
            }
            if !baseline.contains_key(&wallet) {
                baseline.insert(wallet,now);
                continue; // No invented historical coverage before this monitor booted.
            }
            let since=baseline[&wallet];
            for row in rows {
                let Some(hash)=row["transactionHash"].as_str()
                    .filter(|h|h.len()==66 && h.starts_with("0x")
                        && h[2..].bytes().all(|b|b.is_ascii_hexdigit())) else {
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"invalid_tx_hash",leader:wallet.clone()});continue;
                };
                let Some(asset)=row["asset"].as_str().filter(|s|!s.is_empty()) else {
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"missing_asset",leader:wallet.clone()});continue;
                };
                let Some(ts)=row["timestamp"].as_u64() else {
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"invalid_timestamp",leader:wallet.clone()});continue;
                };
                if row["proxyWallet"].as_str().is_none_or(|p|
                    p.trim_start_matches("0x").to_ascii_lowercase()!=wallet) {
                    emit_coverage(CoverageEvent::CorroborationRefused {
                        reason:"mismatched_wallet",leader:wallet.clone()});continue;
                }
                let hash=hash.to_ascii_lowercase();
                let matched=match shared.lock() {
                    Ok(v)=>v.has_trade(&wallet,&hash,asset,ts),
                    Err(_)=>{
                        emit_coverage(CoverageEvent::CorroborationRefused {
                            reason:"registry_poisoned",leader:wallet.clone()});return;
                    }
                };
                if assess_api_trade(since,now,ts,matched)!=ApiDecision::Missed {continue;}
                let key=(wallet.clone(),hash.clone(),asset.to_owned());
                if !reported.insert(key.clone()){continue;}
                reported_order.push_back(key);
                while reported_order.len()>16_384 {
                    if let Some(old)=reported_order.pop_front(){reported.remove(&old);}
                }
                emit_coverage(CoverageEvent::MissedLeaderFill {
                    tx_hash:hash.clone(),leader:wallet.clone(),
                    asset:asset.to_owned(),grace_secs:API_GRACE_SECS,
                });
                // Independent chain proof remains observation-only. It cannot
                // manufacture a trade from an unverified Data API claim.
                let chain=match public_rpc(&http,rpc,"eth_getTransactionByHash",
                    serde_json::json!([hash])).await {Ok(v)=>v,Err(_)=>continue};
                let Some(block)=chain["blockNumber"].as_str().and_then(valid_index) else {continue};
                let leader=vec![wallet.clone()];
                let logs=match public_leader_logs(&http,rpc,&leader,block).await {
                    Ok(v)=>v,Err(_)=>continue
                };
                let Some(event)=logs.into_iter().find(|event|
                    event["transactionHash"].as_str()
                        .is_some_and(|tx|tx.eq_ignore_ascii_case(&hash))
                        && detect_leader_log(event,&leader)
                            .is_ok_and(|p|hex::encode(p.maker)==wallet)
                ) else {continue};
                let raw=match proved_raw("data-api",&http,rpc,&event,&leader,
                    FillStage::DataApiCorroboration).await {
                    Ok(v)=>v,Err(_)=>continue
                };
                let signed=raw.confirmed_fills.as_ref().is_some_and(|fills|
                    fills.iter().any(|fill| {
                        let side=match row["side"].as_str(){
                            Some("BUY")=>Some(0),Some("SELL")=>Some(1),_=>None
                        };
                        row["asset"].as_str()==Some(fill.order.token_id.as_str())
                            && side==Some(fill.order.side)
                            && row["size"].as_f64().is_some_and(|v|
                                (v-fill.order.fill_size).abs()<0.00001)
                            && row["price"].as_f64().is_some_and(|v|
                                (v-fill.order.price).abs()<0.00001)
                    }));
                if signed && out.send(raw).is_err(){return;}
            }
        }
    }
}

#[cfg(test)]
mod completeness_tests {
    use super::*;
    #[test]
    fn provider_range_rejection_is_typed_and_never_accepted_as_empty() {
        let error=serde_json::json!([
            {"id":1,"result":[]},{"id":2,"error":{"code":-32000,"message":"range too wide"}}
        ]);
        assert_eq!(parse_backfill_reply(&error),Err(BackfillRpcRefusal::ProviderRejectedRange));
        let incomplete=serde_json::json!([{"id":1,"result":[]}]);
        assert_eq!(parse_backfill_reply(&incomplete),Err(BackfillRpcRefusal::MalformedReply));
    }
    #[tokio::test]
    async fn invalid_backfill_range_refuses_before_network_io() {
        let client=reqwest::Client::new();
        assert_eq!(leader_logs_range(&client,"http://127.0.0.1:1",&[],55,54).await,
            Err(BackfillRpcRefusal::InvalidRange));
        assert_eq!(leader_logs_range(&client,"http://127.0.0.1:1",&[],1,513).await,
            Err(BackfillRpcRefusal::InvalidRange));
    }
    #[test]
    fn api_grace_and_asset_match_are_explicit() {
        assert_eq!(assess_api_trade(100,200,150,false),ApiDecision::Immature);
        assert_eq!(assess_api_trade(100,360,150,true),ApiDecision::Matched);
        assert_eq!(assess_api_trade(100,360,150,false),ApiDecision::Missed);
        assert_eq!(assess_api_trade(200,500,150,false),ApiDecision::Immature);
        let mut seen=SharedLeaderLogs::default();
        assert!(!seen.has_trade("aa","0x1","123",150));
        seen.assets.entry(("aa".into(),"0x1".into())).or_default().insert("123".into(),160);
        assert!(seen.has_trade("aa","0x1","123",150));
        assert!(!seen.has_trade("aa","0x1","124",150));
        assert!(!seen.has_trade("aa","0x1","123",400)); // late arrival cannot pass grace
    }

    fn sample_raw(index:u64,asset:&str)->RawTx {
        let hash=format!("0x{}","44".repeat(32));
        let order=crate::calldata::Decoded {
            condition_id:[1;32], token_id:asset.into(),side:0,price:0.5,
            order_size:10.0,fill_size:1.0,role:"maker",salt:[2;32],occurrence:0,
        };
        RawTx {
            source:"primary0".into(),hash:hash.clone(),input:Vec::new(),
            to_neg_risk:false,to:CTF_EXCHANGE_V2.into(),
            seen_ns:1010u128*1_000_000_000,
            authenticated_leaders:Some(vec![[0x11;20]]),
            confirmed_fills:Some(vec![crate::calldata::ConfirmedLeaderFill {
                wallet:[0x11;20],transaction_hash:hash,block_time:1000,
                log_index:index,order,
            }]),
            stage:FillStage::ConfirmedLive,
        }
    }
    #[test]
    fn both_wss_and_backfill_preserve_log_keys_but_deliver_one_tx() {
        let (tx,mut rx)=mpsc::unbounded_channel();
        let shared:SharedSeen=Arc::new(Mutex::new(SharedLeaderLogs::default()));
        assert_eq!(deliver_unique(&shared,&tx,sample_raw(2,"123456")),Ok(true));
        assert!(rx.try_recv().is_ok());
        assert_eq!(deliver_unique(&shared,&tx,sample_raw(2,"123456")),Ok(false));
        assert_eq!(deliver_unique(&shared,&tx,sample_raw(3,"987654")),Ok(false));
        assert!(rx.try_recv().is_err());
        let seen=shared.lock().unwrap();
        assert_eq!(seen.keys.len(),2);
        assert_eq!(seen.txs.len(),1);
        let wallet="11".repeat(20);
        let hash=format!("0x{}","44".repeat(32));
        assert!(seen.has_trade(&wallet,&hash,"123456",1000));
        assert!(seen.has_trade(&wallet,&hash,"987654",1000));
        assert!(!seen.has_trade(&wallet,&hash,"987654",500));
    }
    #[test]
    fn persistent_cursor_rejects_invalid_or_mismatched_wallets() {
        let dir=std::env::temp_dir().join(format!("copybot-feed2-{}-{:?}",
            std::process::id(),std::thread::current().id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file=dir.join("one.cursor");
        let a=vec!["aa".repeat(20)];
        let b=vec!["bb".repeat(20)];
        assert_eq!(restore_cursor(&file,&a).unwrap(),None);
        persist_cursor(&file,&a,100).unwrap();
        assert_eq!(restore_cursor(&file,&a).unwrap(),Some(100));
        assert!(restore_cursor(&file,&b).is_err());
        persist_cursor(&file,&a,164).unwrap();
        assert_eq!(restore_cursor(&file,&a).unwrap(),Some(164));
        std::fs::write(&file,"invalid").unwrap();
        assert!(restore_cursor(&file,&a).is_err());
        std::fs::remove_dir_all(&dir).unwrap(); // only self-created test dir
    }
}
