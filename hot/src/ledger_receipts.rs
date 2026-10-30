//! Typed durable receipt for one exchange order hash.
//!
//! The ledger is authoritative for booked inventory. A repeated hash can
//! return a receipt only if its identity and economics exactly match disk;
//! mismatched replay evidence must retain the pending order for review.

use serde_json::Value;

use super::Booked;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct StoredBooked {
    pub lane: String,
    pub token: String,
    pub side: u8,
    pub shares: f64,
    pub price: f64,
    pub fee: f64,
    pub resting: bool,
}

impl StoredBooked {
    pub fn valid_amounts(side: u8, shares: f64, price: f64, fee: f64) -> bool {
        side <= 1 && shares.is_finite() && shares > 0.0
            && price.is_finite() && (0.0..=1.0).contains(&price)
            && fee.is_finite() && fee >= 0.0
    }

    pub fn from_fill_row(row: &Value) -> Option<(String, Self)> {
        if row["ev"].as_str()? != "fill" {
            return None;
        }
        let hash = row["order_hash"].as_str()
            .filter(|s| !s.is_empty())?;
        let lane = row["lane"].as_str()
            .filter(|s| !s.is_empty())?;
        let token = row["token"].as_str()
            .filter(|s| !s.is_empty())?;
        let side = u8::try_from(row["side"].as_u64()?).ok()?;
        let shares = row["shares"].as_f64()?;
        let price = row["price"].as_f64()?;
        let fee = row["fee"].as_f64()?;
        let resting = match row["exec"].as_str()? {
            "maker" => true,
            "taker" => false,
            _ => return None,
        };
        if !Self::valid_amounts(side, shares, price, fee) {
            return None;
        }
        Some((
            hash.to_string(),
            Self {
                lane: lane.to_owned(), token: token.to_owned(),
                side, shares, price, fee, resting,
            },
        ))
    }

    pub fn agrees(
        &self,
        lane: &str,
        token: &str,
        side: u8,
        shares: f64,
        price: f64,
        fee: f64,
        resting: bool,
    ) -> bool {
        self.lane == lane && self.token == token && self.side == side
            && self.shares == shares && self.price == price && self.fee == fee
            && self.resting == resting
    }

    pub fn receipt(&self) -> Booked {
        Booked { shares: self.shares, price: self.price }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{Ledger, Position};
    use crate::pending::{OrderStage, Pending, PendingLog};
    use crate::risk::RiskConfig;
    use std::io::Write;

    fn paths(label: &str) -> (std::path::PathBuf, String, String) {
        let dir = std::env::temp_dir().join(format!(
            "copybot-receipt-{label}-{}-{}", std::process::id(),
            crate::ledger::now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = dir.join("ledger.jsonl").to_string_lossy().into_owned();
        let pending = dir.join("pending.jsonl").to_string_lossy().into_owned();
        (dir, ledger, pending)
    }
    fn ledger(path: &str) -> Ledger {
        Ledger::new(path, &[("synthetic".into(), RiskConfig::default())])
    }
    fn position(ledger: &Ledger) -> Position {
        ledger.lanes["synthetic"].positions.get("tok").cloned().unwrap_or_default()
    }
    fn book(ledger: &mut Ledger, shares: f64, price: f64, fee: f64) -> Option<Booked> {
        ledger.book_fill_ex_px(
            "synthetic", "tok", 0, shares, price, fee,
            Some(0.42), "hash-one", false, false,
        )
    }
    #[test]
    fn crash_between_ledger_booking_and_pending_resolution_replays_one_exact_fill() {
        let (dir, ledger_path, pending_path) = paths("crash");
        let mut pending = PendingLog::open(&pending_path);
        pending.record(Pending {
            lane: "synthetic".into(), token: "tok".into(),
            side: 0, order_hash: "hash-one".into(), shares: 8.0,
            limit: 0.5, ts: 100, why: "submitting".into(), resting: false,
        }).unwrap();
        let mut owner = ledger(&ledger_path);
        let receipt = book(&mut owner, 8.0, 0.5, 0.04).unwrap();
        assert_eq!(receipt, Booked { shares: 8.0, price: 0.5 });
        assert_eq!(position(&owner).shares, 8.0);
        assert_eq!(pending.stage_of("hash-one"), Some(OrderStage::Reserved));
        // Crash now: the ledger fsync succeeded, the pending close did not.
        drop(owner);
        drop(pending);
        let mut owner = ledger(&ledger_path);
        let mut pending = PendingLog::open(&pending_path);
        assert_eq!(pending.len(), 1);
        assert_eq!(position(&owner).shares, 8.0);
        assert!((position(&owner).cost - 4.04).abs() < 1e-9);
        let before = std::fs::metadata(&ledger_path).unwrap().len();
        let persisted = book(&mut owner, 8.0, 0.5, 0.04).unwrap();
        assert_eq!(persisted, receipt);
        assert_eq!(std::fs::metadata(&ledger_path).unwrap().len(), before);
        pending.resolve_booked("hash-one", "filled", persisted).unwrap();
        drop(owner);
        drop(pending);
        let owner = ledger(&ledger_path);
        let pending = PendingLog::open(&pending_path);
        assert_eq!(position(&owner).shares, 8.0);
        assert_eq!(pending.len(), 0);
        assert!(owner.persistence_ok());
        assert!(pending.persistence_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn conflicting_hash_and_price_refuse_instead_of_closing_pending() {
        let (dir, ledger_path, pending_path) = paths("conflict");
        let mut pending = PendingLog::open(&pending_path);
        pending.record(Pending {
            lane: "synthetic".into(), token: "tok".into(),
            side: 0, order_hash: "hash-one".into(), shares: 8.0,
            limit: 0.5, ts: 100, why: "submitting".into(), resting: false,
        }).unwrap();
        let mut owner = ledger(&ledger_path);
        book(&mut owner, 8.0, 0.5, 0.04).unwrap();
        let before = std::fs::metadata(&ledger_path).unwrap().len();
        assert!(book(&mut owner, 9.0, 0.5, 0.04).is_none());
        assert!(!owner.persistence_ok(), "conflicting receipt must latch a fault");
        assert_eq!(std::fs::metadata(&ledger_path).unwrap().len(), before);
        assert!(pending.is_open("hash-one"));
        assert_eq!(position(&owner).shares, 8.0);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn duplicate_file_rows_do_not_apply_inventory_twice_on_replay() {
        let (dir, path, _) = paths("duplicate");
        let mut owner = ledger(&path);
        book(&mut owner, 8.0, 0.5, 0.04).unwrap();
        drop(owner);
        let row = std::fs::read_to_string(&path).unwrap();
        let mut stream = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        stream.write_all(row.as_bytes()).unwrap();
        stream.sync_all().unwrap();
        let owner = ledger(&path);
        assert_eq!(position(&owner).shares, 8.0);
        assert!((position(&owner).cost - 4.04).abs() < 1e-9);
        assert!(owner.persistence_ok(), "identical repeated rows are idempotent");
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn corrupt_duplicate_and_invented_money_row_latch_replay_fault() {
        let (dir, path, _) = paths("corrupt");
        let mut owner = ledger(&path);
        book(&mut owner, 8.0, 0.5, 0.04).unwrap();
        drop(owner);
        let raw = std::fs::read_to_string(&path).unwrap();
        let mut row: Value = serde_json::from_str(raw.trim()).unwrap();
        row["price"] = serde_json::json!(0.75);
        let mut stream = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(stream, "{row}").unwrap();
        stream.sync_all().unwrap();
        let owner = ledger(&path);
        assert_eq!(position(&owner).shares, 8.0);
        assert!(!owner.persistence_ok());
        assert!(owner.corrupt_lines > 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
