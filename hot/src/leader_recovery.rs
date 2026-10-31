//! Indexed-leader WebSocket owner: gap recovery, typed receipt admission and Data API corroboration.
use crate::feeds::*;
use crate::feeds::{free_leader_rpc_endpoint,leader_log_filter,public_rpc,public_leader_logs,resolve_leader_log,valid_index};
use futures_util::{SinkExt,StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

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
    // Subscribe FIRST, then scan the last 24h on cold boot or the exact
    // unscanned interval after reconnect. Any queued live frames are safe.
    let head=head_number(&http,rpc_url).await?;
    backfill(name,&http,rpc_url,out,&leaders,scanned_through,head,
        &mut delivered,&mut delivered_order).await?;
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
            backfill(name,&http,rpc_url,out,&leaders,scanned_through,height,
                &mut delivered,&mut delivered_order).await?;
            continue;
        }
        let Some(event) = json.pointer("/params/result") else {return Err("leader-logs notification has no result".into())};
        let lead = detect_leader_log(event, &leaders)
            .map_err(|_| "leader-logs unverified event: refused")?;
        stats.frame();
        if delivered.contains(&lead.tx) { continue; }
        // An event can arrive after a missed interval. Scan complete earlier
        // blocks without treating their historical fills as live orders.
        if lead.block_number>scanned_through.scanned_through.unwrap_or(0).saturating_add(1){
            backfill(name,&http,rpc_url,out,&leaders,scanned_through,
                lead.block_number-1,&mut delivered,&mut delivered_order).await?;
        }
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
        let (tx, confirmed_fills) = resolved.ok_or("leader-logs resolution unavailable")?;
        let mut authenticated_leaders: Vec<_> = confirmed_fills.iter().map(|x| x.wallet).collect();
        authenticated_leaders.sort(); authenticated_leaders.dedup();
        let input = hex::decode(tx.input.strip_prefix("0x").ok_or("settled calldata missing 0x")?)
            .map_err(|_| "invalid settled calldata")?;
        let seen_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "wall clock unavailable")?.as_nanos();
        let raw = RawTx { source: name.to_owned(), hash: tx.hash.clone(), input,
            to_neg_risk: tx.to == NEG_RISK_CTF_EXCHANGE_V2, to: tx.to,
            seen_ns, authenticated_leaders: Some(authenticated_leaders),
            confirmed_fills: Some(confirmed_fills), stage: FillStage::ConfirmedLive };
        if out.send(raw).is_err() { return Ok(()); }
        stats.exchange_tx();
        remember_tx(&mut delivered,&mut delivered_order,tx.hash);
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
async fn leader_logs_range(http:&reqwest::Client,rpc:&str,leaders:&[String],from:u64,to:u64)->Result<Vec<serde_json::Value>,String>{
    if to<from||to-from>=512{return Err("backfill exceeds bounded 512-block RPC range".into());}
    let batch:Vec<_>=[true,false].into_iter().enumerate().map(|(i,maker)|{
        let mut filter=leader_log_filter(leaders,maker,Some(from));
        filter["toBlock"]=serde_json::json!(format!("0x{to:x}"));
        serde_json::json!({"jsonrpc":"2.0","id":i+1,"method":"eth_getLogs","params":[filter]})
    }).collect();
    let response=http.post(rpc).json(&batch).send().await.map_err(|_|"backfill RPC transport unavailable".to_owned())?;
    if !response.status().is_success(){return Err("backfill provider returned non-2xx".into());}
    let v:serde_json::Value=response.json().await.map_err(|_|"backfill RPC malformed JSON".to_owned())?;
    crate::feeds::parse_leader_log_batch(&v).map_err(str::to_owned)
}
fn remember_tx(recent:&mut std::collections::HashSet<String>,order:&mut std::collections::VecDeque<String>,hash:String){
    if recent.insert(hash.clone()){order.push_back(hash);}
    if order.len()>8192 {if let Some(old)=order.pop_front(){recent.remove(&old);}}
}
/// A typed scanned-through cursor is retained across reconnects. It advances
/// only after authenticated historical rows have reached the receiver.
/// Recovered events are inspection-only; never replay a stale order.
async fn backfill(name:&str,http:&reqwest::Client,rpc:&str,out:&mpsc::UnboundedSender<RawTx>,
    leaders:&[String],cursor:&mut RecoveryCursor,head:u64,
    recent:&mut std::collections::HashSet<String>,order:&mut std::collections::VecDeque<String>,
)->Result<(),String>{
    // Include the last scanned block to cover same-block late propagation
    // and a reconnect after an incomplete block. Dedup is observation-only.
    let start=match cursor.scanned_through {Some(last)=>last,None=>history_start(http,rpc,head).await?};
    if start>head{return Ok(());}
    if head-start>100_000{return Err("gap beyond 100000 blocks: manual reconciliation required".into());}
    let mut from=start;
    while from<=head {
        let to=from.saturating_add(511).min(head);
        let logs=leader_logs_range(http,rpc,leaders,from,to).await?;
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
            if recent.contains(&tx){continue;}
            let raw=proved_raw(name,http,rpc,&event,leaders,FillStage::Recovered).await?;
            if out.send(raw).is_err(){return Err("recovery receiver closed".into());}
            remember_tx(recent,order,tx);
        }
        cursor.advance(from,to).map_err(|_|"noncontiguous leader backfill refused")?;
        from=to.saturating_add(1);
    }
    Ok(())
}


/// Independent public Data API observer: two-second polling, no wallet key,
/// with no permission to enter the live order path. Only a matching maker
/// event in canonical Polygon logs may be passed as corroboration.
pub(crate) async fn corroborate_data_api(
    provider_url:String,out:mpsc::UnboundedSender<RawTx>,watched:Arc<WatchedAddresses>,
) {
    let Some(rpc)=free_leader_rpc_endpoint(&provider_url) else {
        eprintln!("[data-api] REFUSED: no verified free Polygon RPC");return;
    };
    let http=match reqwest::Client::builder().timeout(Duration::from_secs(7)).build(){
        Ok(client)=>client,Err(_)=>{eprintln!("[data-api] REFUSED: HTTP client unavailable");return;}
    };
    let mut known=std::collections::HashSet::new();
    let mut first_seen_wallet=std::collections::HashSet::new();
    let mut tick=tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let leaders=watched.snapshot();
        for wallet in leaders {
            // Limit 10 minimizes repeated payload; this is corroboration, not
            // a substitute for complete on-chain gap scans during bursts.
            let url=format!("https://data-api.polymarket.com/trades?user=0x{wallet}&limit=10");
            let response=match http.get(&url).send().await{
                Ok(r)=>r,Err(_)=>{eprintln!("[data-api] trade poll unavailable");continue;}
            };
            if response.status()==reqwest::StatusCode::TOO_MANY_REQUESTS {
                eprintln!("[data-api] rate limited: corroboration deferred, never a trading fallback");
                tokio::time::sleep(Duration::from_secs(4)).await;
                continue;
            }
            if !response.status().is_success() {
                eprintln!("[data-api] corroboration rejected by provider");continue;
            }
            let records:serde_json::Value=match response.json().await{
                Ok(v)=>v,Err(_)=>{eprintln!("[data-api] malformed trades JSON");continue;}
            };
            let Some(rows)=records.as_array() else {eprintln!("[data-api] trades is not an array");continue};
            let baseline=!first_seen_wallet.contains(&wallet);
            for row in rows {
                let Some(hash)=row["transactionHash"].as_str() else {continue};
                if hash.len()!=66 || !hash.starts_with("0x") || !hash[2..].bytes().all(|b|b.is_ascii_hexdigit()) {
                    continue;
                }
                let witness_key=format!("{wallet}:{}",hash.to_ascii_lowercase());
                if !known.insert(witness_key.clone()) {continue;}
                if baseline {continue;} // index was populated before boot, no stale admission
                // The indexed Data API row itself is NOT authenticated chain
                // evidence. Obtain the mined block then re-run our canonical
                // maker-topic detector before reporting it.
                let chain=match public_rpc(&http,rpc,"eth_getTransactionByHash",serde_json::json!([hash])).await{
                    Ok(v)=>v,Err(_)=>{known.remove(&witness_key);continue;}
                };
                let Some(block)=chain["blockNumber"].as_str().and_then(valid_index) else {
                    known.remove(&witness_key);continue;
                };
                let leaders=vec![wallet.clone()];
                let logs=match public_leader_logs(&http,rpc,&leaders,block).await{
                    Ok(l)=>l,Err(_)=>{known.remove(&witness_key);continue;}
                };
                let Some(event)=logs.into_iter().find(|event| {
                    event["transactionHash"].as_str().is_some_and(|h|h.eq_ignore_ascii_case(hash))
                        && detect_leader_log(event,&leaders).is_ok_and(|p|hex::encode(p.maker)==wallet)
                }) else {known.remove(&witness_key);continue;};
                let raw=match proved_raw("data-api",&http,rpc,&event,&leaders,FillStage::DataApiCorroboration).await{
                    Ok(r)=>r,Err(_)=>{eprintln!("[data-api] unprovable indexed trade refused");continue;}
                };
                // If the Data API disagrees with the chain's maker order,
                // reject the witness rather than treating an API claim as a fill.
                let signed=raw.confirmed_fills.as_ref().is_some_and(|fills|fills.iter().any(|fill| {
                    let size=row["size"].as_f64();let price=row["price"].as_f64();
                    let side=match row["side"].as_str(){Some("BUY")=>Some(0),Some("SELL")=>Some(1),_=>None};
                    row["asset"].as_str()==Some(fill.order.token_id.as_str())
                        && side==Some(fill.order.side)
                        && size.is_some_and(|s|(s-fill.order.fill_size).abs()<0.00001)
                        && price.is_some_and(|p|(p-fill.order.price).abs()<0.00001)
                }));
                if signed && out.send(raw).is_err(){return;}
                if !signed {known.remove(&witness_key);
                    eprintln!("[data-api] indexed trade disagrees with on-chain maker fill");}
            }
            first_seen_wallet.insert(wallet);
            // Bound the corroborator's memory; hashes older than the current
            // pagination horizon cannot be copied, only reread for evidence.
            if known.len()>10_000 {known.clear();first_seen_wallet.clear();}
        }
    }
}
