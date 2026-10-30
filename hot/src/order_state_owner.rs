//! One restart reconciliation edge for previously durable order fills.
//!
//! The ledger and pending journal are still two independent on-disk files.
//! This owner verifies both while holding their exclusive runtime locks:
//! it may release only a non-resting FAK reservation already booked on disk.
//! Resting orders need venue confirmation and are deliberately retained.
//! This is a recovery seam, not an atomic cross-file transaction.

use crate::ledger::{Ledger, RecordedOrder};
use crate::pending::{Pending, PendingLog};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartRefusal {
    LedgerUnavailable,
    PendingUnavailable,
    ConflictingBookedOrder { hash: String },
    ReleaseUnavailable { hash: String },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RestartReport {
    pub recovered: usize,
    pub missing_ledger_receipts: usize,
    pub retained_resting: usize,
}

pub struct OrderStateOwner<'a> {
    ledger: &'a Ledger,
    pending: &'a mut PendingLog,
}

impl<'a> OrderStateOwner<'a> {
    pub fn new(ledger: &'a Ledger, pending: &'a mut PendingLog) -> Self {
        Self { ledger, pending }
    }

    /// Require complete, matching durable evidence before doing *any* release.
    /// A booked receipt is an inventory fact, not proof of fee correctness.
    fn agrees(p: &Pending, saved: &RecordedOrder) -> bool {
        let ordered = p.shares;
        let filled = saved.receipt.shares;
        let px = saved.receipt.price;
        let tolerance = 1e-6;
        p.lane == saved.lane
            && p.token == saved.token
            && p.side == saved.side
            && p.resting == saved.resting
            && filled.is_finite()
            && filled > 0.0
            && filled <= ordered + tolerance
            && saved.fee.is_finite()
            && saved.fee >= 0.0
            && px.is_finite()
            && (0.0..=1.0).contains(&px)
            && (if p.side == 0 {
                px <= p.limit + tolerance
            } else {
                px + tolerance >= p.limit
            })
    }

    pub fn reconcile_restart(&mut self) -> Result<RestartReport, RestartRefusal> {
        if !self.ledger.persistence_ok() {
            return Err(RestartRefusal::LedgerUnavailable);
        }
        if self.pending.unreadable_sources > 0 || !self.pending.persistence_ok() {
            return Err(RestartRefusal::PendingUnavailable);
        }
        let pending_rows = self.pending.open_rows();
        // Preflight the full set before freeing any capacity.
        for p in &pending_rows {
            if let Some(saved) = self.ledger.recorded_order(&p.order_hash) {
                if !Self::agrees(p, &saved) {
                    return Err(RestartRefusal::ConflictingBookedOrder {
                        hash: p.order_hash.clone(),
                    });
                }
            }
        }
        let mut report = RestartReport::default();
        for p in pending_rows {
            match self.ledger.recorded_order(&p.order_hash) {
                None => report.missing_ledger_receipts += 1,
                Some(_) if p.resting => {
                    // A resting order could still be live or partially filled.
                    // The ledger alone cannot prove the venue has cancelled it.
                    report.retained_resting += 1;
                }
                Some(saved) => {
                    // Non-resting FAK was already durably booked. Resolve the
                    // *known* receipt, not a newly invented size/price/fee.
                    self.pending.resolve_booked(
                        &p.order_hash, "restart_booked_fak", saved.receipt,
                    ).map_err(|_| RestartRefusal::ReleaseUnavailable {
                        hash: p.order_hash.clone(),
                    })?;
                    report.recovered += 1;
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending::OrderStage;
    use crate::risk::RiskConfig;
    use std::path::PathBuf;

    fn fixture(tag: &str, resting: bool) -> (PathBuf, Ledger, PendingLog) {
        let folder = std::env::temp_dir().join(format!(
            "copybot-d614-{tag}-{}-{}", std::process::id(),
            crate::ledger::now_secs()
        ));
        std::fs::create_dir_all(&folder).unwrap();
        let ledger_path = folder.join("ledger.jsonl");
        let pending_path = folder.join("pending.jsonl");
        let ledger = Ledger::new(
            ledger_path.to_str().unwrap(),
            &[("synthetic".into(), RiskConfig::default())],
        );
        let mut pending = PendingLog::open(pending_path.to_str().unwrap());
        pending.record(Pending {
            lane: "synthetic".into(), token: "T1".into(), side: 0,
            order_hash: "order-1".into(), shares: 12.0, limit: 0.5,
            ts: 100, why: "submitted".into(), resting,
        }).unwrap();
        (folder, ledger, pending)
    }

    fn book(ledger: &mut Ledger, resting: bool, size: f64) {
        ledger.book_fill_ex_px(
            "synthetic", "T1", 0, size, 0.45, 0.01,
            None, "order-1", resting, false,
        ).expect("synthetic fill booking must be durable");
    }

    #[test]
    fn booked_fak_crash_gap_is_closed_once_and_stays_closed_on_reopen() {
        let (dir, mut ledger, pending) = fixture("fak", false);
        book(&mut ledger, false, 9.0);
        assert_eq!(ledger.lanes["synthetic"].positions["T1"].shares, 9.0);
        drop(ledger);
        drop(pending);
        let mut ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("synthetic".into(), RiskConfig::default())],
        );
        let mut pending = PendingLog::open(dir.join("pending.jsonl").to_str().unwrap());
        assert!(pending.is_open("order-1"));
        assert_eq!(pending.stage_of("order-1"), Some(OrderStage::Reserved));
        let first = OrderStateOwner::new(&ledger, &mut pending)
            .reconcile_restart().unwrap();
        assert_eq!(first.recovered, 1);
        assert_eq!(pending.in_flight().buy_shares_by_token.len(), 0);
        assert_eq!(ledger.lanes["synthetic"].positions["T1"].shares, 9.0);
        let second = OrderStateOwner::new(&ledger, &mut pending)
            .reconcile_restart().unwrap();
        assert_eq!(second.recovered, 0);
        drop(ledger);
        drop(pending);
        ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("synthetic".into(), RiskConfig::default())],
        );
        let pending = PendingLog::open(dir.join("pending.jsonl").to_str().unwrap());
        assert!(!pending.is_open("order-1"));
        assert_eq!(ledger.lanes["synthetic"].positions["T1"].shares, 9.0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn resting_fill_never_releases_without_venue_terminal_evidence() {
        let (dir, mut ledger, mut pending) = fixture("rest", true);
        book(&mut ledger, true, 12.0);
        let report = OrderStateOwner::new(&ledger, &mut pending)
            .reconcile_restart().unwrap();
        assert_eq!(report.retained_resting, 1);
        assert_eq!(report.recovered, 0);
        assert!(pending.is_open("order-1"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn no_fill_receipt_is_not_assumed_to_be_no_open_order() {
        let (dir, ledger, mut pending) = fixture("no_book", false);
        let report = OrderStateOwner::new(&ledger, &mut pending)
            .reconcile_restart().unwrap();
        assert_eq!(report.missing_ledger_receipts, 1);
        assert_eq!(report.recovered, 0);
        assert!(pending.is_open("order-1"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn mismatched_order_identity_refuses_before_any_resolution() {
        let (dir, mut ledger, mut pending) = fixture("conflict", false);
        book(&mut ledger, false, 20.0);
        let result = OrderStateOwner::new(&ledger, &mut pending).reconcile_restart();
        assert_eq!(result, Err(RestartRefusal::ConflictingBookedOrder {
            hash: "order-1".into(),
        }));
        assert!(pending.is_open("order-1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
