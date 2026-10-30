//! Strict non-resting matched FAK response -> durable monetary commit.
//!
//! Pure parsing, then one explicit journal-first edge. The old live paths do
//! NOT call this yet. A TLS body alone does not independently authenticate a
//! venue fill; the transport's HTTP/venue-identity proof belongs to the caller.

use crate::ledger::{Booked, Ledger};
use crate::order_commit::{
    AppendVerdict, AtomicFakJournal, FakFillCommit, FeeEvidence,
    JournalRefusal, PriceEvidence, RepairCount,
};
use crate::pending::{Pending, PendingLog};
use serde_json::Value;

const MICRO: u64 = 1_000_000;
const MAX_EXACT: u64 = (1u64 << 53) - 1;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictFakRefusal {
    BadJson,
    NotMatched,
    Identity,
    Missing(&'static str),
    Conflicting(&'static str),
    InvalidAmount(&'static str),
    NotPending,
    RestingCannotCommit,
    DurableStateUnavailable,
    UnprovenSellInventory,
    Journal(JournalRefusal),
}
impl From<JournalRefusal> for StrictFakRefusal {
    fn from(e: JournalRefusal) -> Self { Self::Journal(e) }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StrictFakCommitted {
    pub append: AppendVerdict,
    pub projection: RepairCount,
    pub receipt: Booked,
}

/// A positive venue string decimal, with at most six exact decimal places.
/// JSON numbers, rounding, signed inputs, scientific notation, and garbage
/// are *refused* instead of silently replaced with a monetary default.
fn exact_micro(v: &Value, field: &'static str) -> Result<u64, StrictFakRefusal> {
    let raw = v.as_str().ok_or(StrictFakRefusal::Missing(field))?;
    if raw.is_empty() || raw.trim() != raw || raw.starts_with('+')
        || raw.starts_with('-') || raw.contains('e') || raw.contains('E')
    {
        return Err(StrictFakRefusal::InvalidAmount(field));
    }
    let mut parts = raw.split('.');
    let whole = parts.next().unwrap_or("");
    let decimals = parts.next();
    if parts.next().is_some()
        || whole.is_empty() || !whole.bytes().all(|ch| ch.is_ascii_digit())
    {
        return Err(StrictFakRefusal::InvalidAmount(field));
    }
    let fraction = decimals.unwrap_or("");
    if fraction.len() > 6 || !fraction.bytes().all(|ch| ch.is_ascii_digit())
        || decimals.is_some_and(str::is_empty)
    {
        return Err(StrictFakRefusal::InvalidAmount(field));
    }
    let whole_num: u64 = whole.parse()
        .map_err(|_| StrictFakRefusal::InvalidAmount(field))?;
    let fractions: u64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u64>()
            .map_err(|_| StrictFakRefusal::InvalidAmount(field))?
            * 10u64.pow((6-fraction.len()) as u32)
    };
    let amount = whole_num.checked_mul(MICRO)
        .and_then(|v| v.checked_add(fractions))
        .filter(|v| *v <= MAX_EXACT)
        .ok_or(StrictFakRefusal::InvalidAmount(field))?;
    Ok(amount)
}
fn expected_micro(value: f64, field: &'static str) -> Result<u64, StrictFakRefusal> {
    if !value.is_finite() || value <= 0.0 || value * MICRO as f64 > MAX_EXACT as f64 {
        return Err(StrictFakRefusal::InvalidAmount(field));
    }
    let scaled = value * MICRO as f64;
    if (scaled - scaled.round()).abs() > 0.0001 {
        return Err(StrictFakRefusal::InvalidAmount(field));
    }
    Ok(scaled.round() as u64)
}
fn consistent_aliases(
    object: &Value, keys: &'static [&'static str], label: &'static str,
) -> Result<u64, StrictFakRefusal> {
    let mut known: Option<u64> = None;
    for &key in keys {
        if let Some(value) = object.get(key) {
            let amount = exact_micro(value, label)?;
            if known.is_some_and(|prior| prior != amount) {
                return Err(StrictFakRefusal::Conflicting(label));
            }
            known = Some(amount);
        }
    }
    known.ok_or(StrictFakRefusal::Missing(label))
}

/// Converts a matched HTTP response into a strictly sourced FAK fill.
/// Caller must verify this response belongs to an HTTP 2xx venue path.
/// Non-resting FAK only; no status-only or limit-priced provisional fills.
pub fn parse_matched_fak(
    pending: &Pending, raw: &str,
) -> Result<FakFillCommit, StrictFakRefusal> {
    if pending.resting {
        return Err(StrictFakRefusal::RestingCannotCommit);
    }
    let body: Value = serde_json::from_str(raw).map_err(|_| StrictFakRefusal::BadJson)?;
    if !body.is_object() {
        return Err(StrictFakRefusal::BadJson);
    }
    if body.get("success") != Some(&Value::Bool(true)) {
        return Err(StrictFakRefusal::NotMatched);
    }
    if !matches!(body["status"].as_str(), Some("matched" | "filled"))
        || ["error", "errorMsg"].iter().any(|key|
            body.get(*key).is_some_and(|v| !v.is_null() && v.as_str() != Some(""))
        )
    {
        return Err(StrictFakRefusal::NotMatched);
    }
    let ids: Vec<&str> = ["orderID", "orderId", "order_id"]
        .iter().filter_map(|key| body.get(*key).and_then(Value::as_str)).collect();
    let expected = pending.order_hash.trim_start_matches("0x");
    if ids.is_empty() || ids.iter().any(|id| {
        id.trim_start_matches("0x") != expected
    }) {
        return Err(StrictFakRefusal::Identity);
    }
    let ordered = expected_micro(pending.shares, "ordered_shares")?;
    let limit = expected_micro(pending.limit, "submitted_limit")?;
    if limit == 0 || limit > MICRO {
        return Err(StrictFakRefusal::InvalidAmount("submitted_limit"));
    }
    let taken = exact_micro(&body["takingAmount"], "takingAmount")?;
    let made = exact_micro(&body["makingAmount"], "makingAmount")?;
    let (filled, quote) = match pending.side {
        0 => (taken, made),
        1 => (made, taken),
        _ => return Err(StrictFakRefusal::Identity),
    };
    if filled == 0 || filled > ordered || quote == 0 {
        return Err(StrictFakRefusal::InvalidAmount("matched_amounts"));
    }
    for key in ["size_matched", "sizeMatched"] {
        if let Some(matched) = body.get(key) {
            if exact_micro(matched, "size_matched")? != filled {
                return Err(StrictFakRefusal::Conflicting("size_matched"));
            }
        }
    }
    // Round only at the last step, to the required 1e-6 price tick;
    // computed from two separately supplied venue amount legs, not a limit.
    let exact_ratio = (quote as u128) * MICRO as u128;
    let price = (exact_ratio + filled as u128 / 2) / filled as u128;
    if price == 0 || price > MICRO as u128 {
        return Err(StrictFakRefusal::InvalidAmount("derived_price"));
    }
    let price = price as u64;
    if (pending.side == 0 && price > limit) ||
        (pending.side == 1 && price < limit)
    {
        return Err(StrictFakRefusal::InvalidAmount("price_outside_limit"));
    }
    // An explicit zero is a value; a *missing* fee is not zero.
    // Rate-only replies are not actual charged-fee receipts.
    let fee = consistent_aliases(&body,
        &["fee", "feeAmount", "fee_amount"], "fee")?;
    let c = FakFillCommit {
        order_hash: pending.order_hash.clone(),
        lane: pending.lane.clone(),
        token: pending.token.clone(),
        side: pending.side,
        ordered_shares_micros: ordered,
        limit_price_micros: limit as u32,
        filled_shares_micros: filled,
        fill_price_micros: price as u32,
        fee_micros: fee,
        price_evidence: PriceEvidence::VenueAmountLegs,
        fee_evidence: FeeEvidence::ExplicitVenueAmount,
        leader_price_micros: None,
    };
    c.validate().map_err(StrictFakRefusal::Journal)?;
    Ok(c)
}

/// A complete journal-first matched FAK *booking* path; never sends an order.
/// The caller must supply a validated 2xx matched venue body under the
/// appropriate wallet identity, and ensure exclusive process ownership.
pub fn commit_matched_fak(
    journal: &AtomicFakJournal,
    ledger: &mut Ledger,
    pending: &mut PendingLog,
    hash: &str,
    raw: &str,
) -> Result<StrictFakCommitted, StrictFakRefusal> {
    if !ledger.persistence_ok()
        || pending.unreadable_sources != 0
        || !pending.persistence_ok()
        || !pending.durable_path_configured()
    {
        return Err(StrictFakRefusal::DurableStateUnavailable);
    }
    // Complete a prior fsynced commitment before accepting new evidence.
    // A failed old projection must halt this writer, not be skipped.
    journal.repair_legacy(ledger, pending)?;
    let p = pending.open_rows().into_iter()
        .find(|p| p.order_hash == hash).ok_or(StrictFakRefusal::NotPending)?;
    let c = parse_matched_fak(&p, raw)?;
    if p.side == 1 {
        let held = ledger.holdings(&p.lane).get(&p.token).copied().unwrap_or(0.0);
        if held + 1e-9 < c.filled_shares_micros as f64 / MICRO as f64 {
            // Ledger::apply clamps oversized sells. Never append a terminal
            // money record that the ledger would silently book partially.
            return Err(StrictFakRefusal::UnprovenSellInventory);
        }
    }
    let append = journal.append(c)?;
    let projection = journal.repair_legacy(ledger, pending)?;
    let receipt = ledger.recorded_order(hash)
        .ok_or_else(|| StrictFakRefusal::Journal(JournalRefusal::NotYetProven(
            format!("committed order {hash} has no durable inventory receipt"),
        )))?.receipt;
    Ok(StrictFakCommitted { append, projection, receipt })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::RiskConfig;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(1);

    fn fixture(label: &str, side: u8) -> (PathBuf, Ledger, PendingLog, AtomicFakJournal) {
        let dir = std::env::temp_dir().join(format!(
            "copybot-d616-{label}-{}-{}", std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("alpha".into(), RiskConfig::default())],
        );
        let mut pending = PendingLog::open(dir.join("pending.jsonl").to_str().unwrap());
        pending.record(Pending {
            lane:"alpha".into(), token:"T".into(), side,
            order_hash:"h1".into(), shares:10.0,
            limit: if side == 0 {0.6} else {0.3},
            ts:10, why:"synthetic".into(), resting:false,
        }).unwrap();
        let journal = AtomicFakJournal::open(dir.join("atomic.jsonl")).unwrap();
        (dir, ledger, pending, journal)
    }
    fn venue_buy(fee: &str) -> String {
        format!(
            r#"{{"success":true,"status":"matched","orderID":"0xh1","takingAmount":"9","makingAmount":"4.05"{fee}}}"#,
        )
    }
    fn venue_sell(fee: &str) -> String {
        format!(
            r#"{{"success":true,"status":"matched","orderID":"0xh1","makingAmount":"9","takingAmount":"4.05"{fee}}}"#,
        )
    }
    fn read_ledger(dir: &Path) -> Ledger {
        Ledger::new(dir.join("ledger.jsonl").to_str().unwrap(),
            &[("alpha".into(), RiskConfig::default())])
    }
    fn read_pending(dir: &Path) -> PendingLog {
        PendingLog::open(dir.join("pending.jsonl").to_str().unwrap())
    }
    #[test]
    fn exact_matched_buy_commits_journal_before_ledger_and_reopens_once() {
        let (dir, mut led, mut pend, journal) = fixture("matched-buy", 0);
        let result = commit_matched_fak(
            &journal, &mut led, &mut pend, "h1",
            &venue_buy(r#","feeAmount":"0.015""#),
        ).unwrap();
        assert_eq!(result.append, AppendVerdict::DurablyWritten);
        assert_eq!(result.receipt, Booked {shares:9.0,price:0.45});
        assert_eq!(result.projection.fills_booked, 1);
        assert_eq!(result.projection.pending_closed, 1);
        assert!(!pend.is_open("h1"));
        let row = led.recorded_order("h1").unwrap();
        assert!((row.fee - 0.015).abs() < 1e-9);
        assert_eq!(led.lanes["alpha"].positions["T"].shares, 9.0);
        drop(led);
        drop(pend);
        drop(journal);
        let mut led = read_ledger(&dir);
        let mut pend = read_pending(&dir);
        let journal = AtomicFakJournal::open(dir.join("atomic.jsonl")).unwrap();
        assert_eq!(journal.repair_legacy(&mut led, &mut pend).unwrap(),
            RepairCount { fills_booked:0, pending_closed:0, already_closed:1 });
        assert_eq!(led.lanes["alpha"].positions["T"].shares, 9.0);
        assert!(!pend.is_open("h1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn unknown_fee_missing_amount_or_missing_order_identity_are_refusals() {
        let (dir, mut led, mut pend, journal) = fixture("unknown", 0);
        for body in [
            venue_buy(""),
            venue_buy(r#","feeAmount":null"#),
            venue_buy(r#","feeRateBps":"50""#),
            r#"{"success":true,"status":"matched","orderID":"0xh1","takingAmount":"9","feeAmount":"0"}"#.into(),
            r#"{"success":true,"status":"matched","takingAmount":"9","makingAmount":"4.05","feeAmount":"0"}"#.into(),
        ] {
            assert!(commit_matched_fak(
                &journal,&mut led,&mut pend,"h1",&body
            ).is_err(), "incomplete venue fact cannot book");
            assert!(pend.is_open("h1"));
            assert!(!led.was_order_booked("h1"));
        }
        assert!(journal.commits().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn explicit_zero_fee_is_valid_but_never_defaulted() {
        let (dir, mut led, mut pend, journal) = fixture("zero", 0);
        let res = commit_matched_fak(
            &journal, &mut led, &mut pend, "h1",
            &venue_buy(r#","fee":"0""#),
        ).unwrap();
        assert_eq!(res.receipt.price, 0.45);
        assert_eq!(led.recorded_order("h1").unwrap().fee, 0.0);
        assert!(!pend.is_open("h1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn conflicting_monetary_aliases_and_precision_are_refused() {
        let (dir, mut led, mut pend, journal) = fixture("precision", 0);
        for body in [
            venue_buy(r#","feeAmount":"0.01","fee":"0.02""#),
            venue_buy(r#","feeAmount":"0.0000001""#),
            venue_buy(r#","feeAmount":"-1""#),
            venue_buy(r#","feeAmount":"1e-3""#),
            venue_buy(r#","feeAmount":0.01"#),
            r#"{"success":true,"status":"matched","orderID":"0xh1","takingAmount":"9.0000001","makingAmount":"4.05","fee":"0.01"}"#.into(),
        ] {
            assert!(commit_matched_fak(
                &journal, &mut led, &mut pend, "h1", &body,
            ).is_err());
        }
        assert!(pend.is_open("h1"));
        assert!(journal.commits().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn wrong_status_wrong_identity_and_too_many_shares_refuse() {
        let (dir, mut led, mut pend, journal) = fixture("wrong", 0);
        let valid = venue_buy(r#","fee":"0.01""#);
        for body in [
            valid.replace(r#""orderID":"0xh1""#, r#""orderID":"h2""#),
            valid.replace(r#""status":"matched""#, r#""status":"live""#),
            valid.replace(r#""success":true"#, r#""success":false"#),
            valid.replace(r#""takingAmount":"9""#, r#""takingAmount":"11""#),
            valid.replace(r#""makingAmount":"4.05""#, r#""makingAmount":"9.05""#),
            valid.replace(r#""takingAmount":"9""#, r#""takingAmount":"0""#),
            valid.replace(r#""takingAmount":"9""#, r#""takingAmount":"8""#)
                .replace(r#""makingAmount":"4.05""#, r#""makingAmount":"5.6""#),
        ] {
            assert!(commit_matched_fak(
                &journal, &mut led, &mut pend, "h1", &body,
            ).is_err(), "bad response must not commit");
        }
        assert!(pend.is_open("h1"));
        assert!(journal.commits().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn sell_book_requires_proved_inventory_not_ledger_clamping() {
        let (dir, mut led, mut pend, journal) = fixture("sell", 1);
        let body = venue_sell(r#","feeAmount":"0.01""#);
        assert_eq!(commit_matched_fak(
            &journal, &mut led, &mut pend, "h1", &body,
        ), Err(StrictFakRefusal::UnprovenSellInventory));
        assert!(journal.commits().unwrap().is_empty());
        let _seed = led.book_fill_ex("alpha","T",0,12.0,0.4,0.0,None,"seed",false).unwrap();
        let res = commit_matched_fak(
            &journal, &mut led, &mut pend, "h1", &body,
        ).unwrap();
        assert_eq!(res.receipt, Booked {shares:9.0,price:0.45});
        assert!(!pend.is_open("h1"));
        let held = led.holdings("alpha")["T"];
        assert!((held-3.0).abs() < 1e-9);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn non_terminal_resting_order_does_not_commit() {
        let (dir, mut led, mut pend, journal) = fixture("resting", 0);
        let p = pend.open_rows()[0].clone();
        let mut resting = p.clone();
        resting.resting = true;
        assert_eq!(parse_matched_fak(&resting, &venue_buy(r#","fee":"0""#)),
            Err(StrictFakRefusal::RestingCannotCommit));
        assert!(commit_matched_fak(&journal, &mut led, &mut pend, "missing",
            &venue_buy(r#","fee":"0""#)).is_err());
        assert!(journal.commits().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn interrupted_projection_restarts_from_original_proved_money() {
        let (dir, led, mut pend, journal) = fixture("interrupt", 0);
        let p = pend.open_rows().pop().unwrap();
        let c = parse_matched_fak(&p, &venue_buy(r#","feeAmount":"0.01""#)).unwrap();
        journal.append(c).unwrap();
        drop(led);
        drop(pend);
        drop(journal);
        let (mut led, mut pend) = (read_ledger(&dir), read_pending(&dir));
        let journal = AtomicFakJournal::open(dir.join("atomic.jsonl")).unwrap();
        assert_eq!(journal.repair_legacy(&mut led, &mut pend).unwrap(),
            RepairCount {fills_booked:1,pending_closed:1,already_closed:0});
        assert_eq!(led.recorded_order("h1").unwrap().fee, 0.01);
        assert_eq!(led.lanes["alpha"].positions["T"].shares, 9.0);
        assert!(!pend.is_open("h1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
