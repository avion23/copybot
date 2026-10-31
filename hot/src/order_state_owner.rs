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


/// Live-state money authority: the SAME journal lease and Control/Pending
/// locks guard FAK, redemption, correction and native-merge writers.
/// Redemption and correction commit immutable source money to this journal
/// before projecting to Ledger; unproved neutral drift is never booked.
/// A FAK has one witnessed terminal HTTP match with two amount legs and an
/// explicit fee; GTC may rest and fill repeatedly without cumulative fee
/// evidence in our current venue client. Never submit what we cannot book.
pub fn has_strict_terminal_money_booker(order_type: &str) -> bool {
    order_type == "FAK"
}

pub struct CanonicalMoneyOwner<'a> {
    _lease: &'a crate::order_commit::AtomicFakJournal,
    pub ledger: &'a mut Ledger,
    pub pending: &'a PendingLog,
}

impl<'a> CanonicalMoneyOwner<'a> {
    pub fn new(
        lease: &'a crate::order_commit::AtomicFakJournal,
        ledger: &'a mut Ledger,
        pending: &'a PendingLog,
    ) -> Result<Self, String> {
        lease.verify_owner().map_err(|e| format!("money owner lease invalid: {e:?}"))?;
        if ledger.path.is_empty() || !ledger.persistence_ok()
            || !pending.durable_path_configured()
            || pending.unreadable_sources > 0 || !pending.persistence_ok()
        {
            return Err("money owner ledger or pending persistence untrusted".into());
        }
        Ok(Self { _lease: lease, ledger, pending })
    }
    fn no_pending(&self, token: &str) -> Result<(), String> {
        if self.pending.open_rows().iter().any(|p| p.token == token) {
            return Err("money event overlaps an unresolved order for this token".into());
        }
        Ok(())
    }
    pub fn redeem(
        &mut self, lane: &str, token: &str,
        redemption: &crate::settlement::Redemption,
        snapshot: &crate::positions::Positions, stale: bool,
    ) -> Result<bool, String> {
        self.no_pending(token)?;
        // A confirmed redemption creates ONE keyed, fsynced ledger event.
        // No independent PendingLog resolution or invented fee is permitted.
        self.ledger.settle_redeemed_open(
            lane, token, redemption, snapshot, stale, self._lease,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn correct_redeemed_release(
        &mut self, lane: &str, token: &str, shares: f64,
        proceeds: f64, avg_cost: f64,
        redemption: &crate::settlement::Redemption,
        snapshot: &crate::positions::Positions, stale: bool,
    ) -> Result<bool, String> {
        self.no_pending(token)?;
        let key = redemption.lane_key(lane);
        if self.ledger.has_money_key(lane, &key)
            || self.ledger.has_money_key(lane, &crate::settlement::cross_writer_key(lane,token))
        {
            return Ok(false);
        }
        if !redemption.is_bookable() || redemption.asset != token || stale
            || !snapshot.confirms_zero(token, redemption.ts)
            || !shares.is_finite() || shares <= 0.0 || shares > redemption.size + 1e-6
            || !proceeds.is_finite() || proceeds < 0.0
            || (proceeds - shares * redemption.usdc / redemption.size).abs() > 1e-7
            || !avg_cost.is_finite() || avg_cost < 0.0
        {
            return Err("redemption correction lacks matching source, amount or complete zero snapshot".into());
        }
        let previous = self.ledger.last_recon_release(lane, token, 0)
            .ok_or_else(|| "no durable neutral release to correct".to_string())?;
        if previous.0 + 1e-6 < shares || previous.2 > redemption.ts
            || (previous.1 - avg_cost).abs() > 1e-6
        {
            return Err("redemption correction does not match prior durable release".into());
        }
        let commit = crate::order_commit::MoneyCommit::Correction {
            lane: lane.into(), token: token.into(), redemption: redemption.clone(),
            shares, proceeds, avg_cost, neutral_release_t: previous.2,
            zero_snapshot_as_of: snapshot.as_of, ledger_t: crate::ledger::now_secs(),
        };
        self._lease.append_money(commit.clone())
            .map_err(|e| format!("redemption correction journal refused: {e:?}"))?;
        if !self.ledger.apply(&commit.ledger_row(), true) {
            self.ledger.last_write_error = Some("journaled correction Ledger projection refused".into());
            return Err("redemption correction projection failed; journal liability retained".into());
        }
        Ok(true)
    }
    pub fn neutral_reconcile(
        &mut self, lane: &str, token: &str, side: u8,
        shares: f64, price: f64,
    ) -> Result<(), String> {
        self.no_pending(token)?;
        if side > 1 || !shares.is_finite() || shares <= 0.0
            || !price.is_finite() || !(0.0..=1.0).contains(&price)
        {
            return Err("neutral custody correction has invalid amount or price".into());
        }
        if side == 1
            && self.ledger.holdings(lane).get(token).copied().unwrap_or(0.0) + 1e-6 < shares
        {
            return Err("neutral release exceeds proven ledger inventory".into());
        }
        // Portfolio drift is a balance observation, not a single causal
        // custody transfer. The caller supplies neither a verified chain
        // transaction hash/log index nor an unambiguous lane attribution.
        // Do not invent an append/replay key from the poll time or share count.
        let _ = (lane, token, side, shares, price);
        Err("neutral custody unresolved: missing verified transactionHash, logIndex, exact token/share direction and single-lane basis attribution".into())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn merge_pair(
        &mut self, lane: &str, token_a: &str, token_b: &str,
        pairs: f64, mark_a: f64, mark_b: f64, tx: &str,
    ) -> Result<(crate::ledger::Booked, crate::ledger::Booked), String> {
        self.no_pending(token_a)?;
        self.no_pending(token_b)?;
        self.ledger.book_merge(
            lane, token_a, token_b, pairs, mark_a, mark_b, tx,
        ).ok_or_else(|| "atomic keyed merge pair booking refused".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_terminal_fak_is_currently_eligible_for_new_order_submissions() {
        assert!(has_strict_terminal_money_booker("FAK"));
        assert!(!has_strict_terminal_money_booker("GTC"));
        assert!(!has_strict_terminal_money_booker("GTD"));
        assert!(!has_strict_terminal_money_booker(""));
    }

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
