//! Atomic append-only FAK fill decision log (D615).
//!
//! An fsynced frame contains both *proved* monetary facts and the terminal
//! non-resting order identity. A deterministic SHA256 chain detects torn and
//! rewritten rows on replay. This is an integrity check, NOT authentication.
//! The old live submitters do not write this journal yet: integration requires
//! audited price + fee provenance before enabling it.

use crate::ledger::{Ledger, RecordedOrder};
use crate::pending::{Pending, PendingLog};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
#[cfg(unix)]
use std::os::fd::AsRawFd;

const VERSION: u8 = 1;
const ONE: f64 = 1_000_000.0;
const MAX_EXACT_MICROS: u64 = (1u64 << 53) - 1;
// Generated as hexadecimal zeroes, never as an on-chain address/identity.
const GENESIS_DIGIT: &str = "0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceEvidence {
    VenueAmountLegs,
    IndependentlyMatchedTrade,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeEvidence {
    ExplicitVenueAmount,
    ExplicitVenueRate,
    VerifiedZeroFeeMarket,
}

/// Every monetary field is an integer in 1e-6 units. No default for an
/// unknown fee, no fallback to submitted limit for an unknown fill price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakFillCommit {
    pub order_hash: String,
    pub lane: String,
    pub token: String,
    pub side: u8,
    pub ordered_shares_micros: u64,
    pub limit_price_micros: u32,
    pub filled_shares_micros: u64,
    pub fill_price_micros: u32,
    pub fee_micros: u64,
    pub price_evidence: PriceEvidence,
    pub fee_evidence: FeeEvidence,
    pub leader_price_micros: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalRefusal {
    Invalid(&'static str),
    Io(String),
    Corrupt { line: usize, reason: &'static str },
    Conflict(String),
    Unsafe(String),
    NotYetProven(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendVerdict {
    DurablyWritten,
    AlreadyDurable,
}
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairCount {
    pub fills_booked: usize,
    pub pending_closed: usize,
    pub already_closed: usize,
}

fn human_micro(v: u64) -> f64 { v as f64 / ONE }

impl FakFillCommit {
    pub fn validate(&self) -> Result<(), JournalRefusal> {
        if self.order_hash.is_empty() || self.order_hash.len() > 130
            || self.lane.is_empty() || self.lane.len() > 128
            || self.token.is_empty() || self.token.len() > 130
        {
            return Err(JournalRefusal::Invalid("missing or oversized order identity"));
        }
        if self.side > 1 {
            return Err(JournalRefusal::Invalid("side must be buy or sell"));
        }
        if self.ordered_shares_micros == 0 || self.filled_shares_micros == 0
            || self.filled_shares_micros > self.ordered_shares_micros
            || self.ordered_shares_micros > MAX_EXACT_MICROS
            || self.fee_micros > MAX_EXACT_MICROS
        {
            return Err(JournalRefusal::Invalid("unprovable or excessive share/fee units"));
        }
        if !(1..=1_000_000).contains(&self.limit_price_micros)
            || !(1..=1_000_000).contains(&self.fill_price_micros)
            || self.leader_price_micros.is_some_and(|p| !(1..=1_000_000).contains(&p))
        {
            return Err(JournalRefusal::Invalid("missing or invalid exact fill price"));
        }
        if (self.side == 0 && self.fill_price_micros > self.limit_price_micros)
            || (self.side == 1 && self.fill_price_micros < self.limit_price_micros)
        {
            return Err(JournalRefusal::Invalid("venue price violates submitted limit"));
        }
        Ok(())
    }
    fn agrees_pending(&self, p: &Pending) -> bool {
        p.lane == self.lane && p.token == self.token && p.side == self.side
            && p.order_hash == self.order_hash && !p.resting
            && (p.shares * ONE - self.ordered_shares_micros as f64).abs() < 0.01
            && (p.limit * ONE - self.limit_price_micros as f64).abs() < 0.01
    }
    fn agrees_ledger(&self, saved: &RecordedOrder) -> bool {
        self.lane == saved.lane && self.token == saved.token
            && self.side == saved.side && !saved.resting
            && saved.receipt.shares == human_micro(self.filled_shares_micros)
            && saved.receipt.price == human_micro(self.fill_price_micros as u64)
            && saved.fee == human_micro(self.fee_micros)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    version: u8,
    seq: u64,
    previous_checksum: String,
    commit: FakFillCommit,
    checksum: String,
}
fn checksum(seq: u64, previous: &str, commit: &FakFillCommit) -> Result<String, JournalRefusal> {
    let payload = serde_json::to_vec(&(VERSION, seq, previous, commit))
        .map_err(|e| JournalRefusal::Io(format!("encode hash payload: {e}")))?;
    Ok(hex::encode(Sha256::digest(&payload)))
}
struct State {
    // Leave the stable lock file on disk after exit. Deleting it on drop can
    // allow two processes to lock different inodes of the same pathname.
    #[cfg(unix)]
    _process_lease: File,
    writer: File,
    frames: Vec<FakFillCommit>,
    hashes: HashMap<String, FakFillCommit>,
    sequence: u64,
    last_checksum: String,
    poisoned: bool,
}
pub struct AtomicFakJournal {
    path: PathBuf,
    lease_path: PathBuf,
    state: Mutex<State>,
}
#[cfg(unix)]
fn same_inode(expected: &File, path: &Path) -> Result<bool, JournalRefusal> {
    let fd = expected.metadata()
        .map_err(|e| JournalRefusal::Io(format!("stat owner descriptor: {e}")))?;
    let named = fs::symlink_metadata(path)
        .map_err(|e| JournalRefusal::Io(format!("stat owner pathname: {e}")))?;
    Ok(named.is_file() && !named.file_type().is_symlink()
        && named.dev() == fd.dev() && named.ino() == fd.ino()
        && fd.nlink() == 1 && named.nlink() == 1
        && fd.permissions().mode() & 0o077 == 0)
}
#[cfg(unix)]
fn acquire_process_lease(path: &Path) -> Result<File, JournalRefusal> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true)
        .mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let lock = options.open(path)
        .map_err(|e| JournalRefusal::Unsafe(format!("open non-symlink journal lease: {e}")))?;
    if !same_inode(&lock, path)? {
        return Err(JournalRefusal::Unsafe("journal lease is not an owner-only regular inode".into()));
    }
    // Non-blocking Linux/POSIX advisory flock serializes independent opens,
    // including separate processes and multiple journal handles in one PID.
    let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(JournalRefusal::Unsafe(format!(
            "journal already has another active owner (flock): {}",
            std::io::Error::last_os_error(),
        )));
    }
    if !same_inode(&lock, path)? {
        return Err(JournalRefusal::Unsafe("journal lease pathname changed during acquisition".into()));
    }
    Ok(lock)
}
impl AtomicFakJournal {
    fn ensure_owner(&self, owner: &State) -> Result<(), JournalRefusal> {
        #[cfg(unix)]
        {
            if !same_inode(&owner._process_lease, &self.lease_path)?
                || !same_inode(&owner.writer, &self.path)?
            {
                return Err(JournalRefusal::Unsafe(
                    "journal or lock pathname was replaced/unlinked".into(),
                ));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = owner;
            Err(JournalRefusal::Unsafe("cross-process lease unavailable on this platform".into()))
        }
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalRefusal> {
        let path = path.as_ref().to_path_buf();
        if path.as_os_str().is_empty() {
            return Err(JournalRefusal::Invalid("an empty journal path is not durable"));
        }
        let existing = match fs::symlink_metadata(&path) {
            Ok(md) => {
                if !md.is_file() || md.file_type().is_symlink() {
                    return Err(JournalRefusal::Unsafe("not a regular owned file".into()));
                }
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(JournalRefusal::Io(format!("stat journal: {e}"))),
        };
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .map_err(|e| JournalRefusal::Io(format!("create journal directory: {e}")))?;
        }
        // Lock the persistent sidecar BEFORE opening or replaying the journal;
        // the lease file is never truncated or deleted by this owner.
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".owner.lock");
        let lease_path = PathBuf::from(lock_name);
        #[cfg(unix)]
        let process_lease = acquire_process_lease(&lease_path)?;
        #[cfg(not(unix))]
        return Err(JournalRefusal::Unsafe(
            "cross-process lease unavailable on this platform".into(),
        ));
        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let writer = options.open(&path)
            .map_err(|e| JournalRefusal::Io(format!("open journal: {e}")))?;
        #[cfg(unix)]
        if !same_inode(&writer, &path)? {
            return Err(JournalRefusal::Unsafe(
                "journal is not a stable owner-only regular file".into(),
            ));
        }
        if !existing {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                File::open(parent).and_then(|dir| dir.sync_all())
                    .map_err(|e| JournalRefusal::Io(format!("sync new journal directory: {e}")))?;
            }
        }
        // All on-disk replay must refer to the locked inode. A renamed or
        // unlinked journal cannot silently become an empty, healthy journal.
        #[cfg(unix)]
        if !same_inode(&writer, &path)?
            || !same_inode(&process_lease, &lease_path)?
        {
            return Err(JournalRefusal::Unsafe("journal ownership changed before replay".into()));
        }
        let bytes = fs::read(&path)
            .map_err(|e| JournalRefusal::Io(format!("read journal: {e}")))?;
        #[cfg(unix)]
        if !same_inode(&writer, &path)?
            || !same_inode(&process_lease, &lease_path)?
        {
            return Err(JournalRefusal::Unsafe("journal ownership changed during replay".into()));
        }
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            return Err(JournalRefusal::Corrupt { line: 0, reason: "torn trailing row" });
        }
        let mut frames = Vec::new();
        let mut hashes = HashMap::new();
        let mut prev = GENESIS_DIGIT.repeat(64);
        let mut seq = 0u64;
        // Every newline delimits exactly one committed frame. In particular,
        // blank lines are NOT silently discarded: truncation/rewrite is fatal.
        let line_count = bytes.iter().filter(|b| **b == b'\n').count();
        for (ix, raw) in bytes.split(|b| *b == b'\n').take(line_count).enumerate() {
            let line = ix + 1;
            if raw.is_empty() {
                return Err(JournalRefusal::Corrupt { line, reason: "empty journal frame" });
            }
            let row: Frame = serde_json::from_slice(raw)
                .map_err(|_| JournalRefusal::Corrupt { line, reason: "invalid JSON frame" })?;
            row.commit.validate().map_err(|_| JournalRefusal::Corrupt {
                line, reason: "invalid or unproved money fields",
            })?;
            let expected = seq.checked_add(1).ok_or(JournalRefusal::Corrupt {
                line, reason: "sequence overflow",
            })?;
            if row.version != VERSION || row.seq != expected || row.previous_checksum != prev {
                return Err(JournalRefusal::Corrupt {
                    line, reason: "sequence, version or hash chain mismatch",
                });
            }
            let correct = checksum(expected, &prev, &row.commit)?;
            if row.checksum != correct {
                return Err(JournalRefusal::Corrupt { line, reason: "checksum mismatch" });
            }
            if hashes.insert(row.commit.order_hash.clone(), row.commit.clone()).is_some() {
                return Err(JournalRefusal::Corrupt {
                    line, reason: "duplicate order digest was appended to disk",
                });
            }
            prev = correct;
            seq = expected;
            frames.push(row.commit);
        }
        Ok(Self {
            path, lease_path,
            state: Mutex::new(State {
                #[cfg(unix)]
                _process_lease: process_lease,
                writer, frames, hashes, sequence: seq,
                last_checksum: prev, poisoned: false,
            }),
        })
    }
    pub fn path(&self) -> &Path { &self.path }
    pub fn commits(&self) -> Result<Vec<FakFillCommit>, JournalRefusal> {
        let owner = self.state.lock()
            .map_err(|_| JournalRefusal::Unsafe("journal owner poisoned".into()))?;
        if owner.poisoned {
            return Err(JournalRefusal::Unsafe("journal fsync or ownership failed".into()));
        }
        self.ensure_owner(&owner)?;
        Ok(owner.frames.clone())
    }
    pub fn append(&self, commit: FakFillCommit) -> Result<AppendVerdict, JournalRefusal> {
        commit.validate()?;
        let mut owner = self.state.lock()
            .map_err(|_| JournalRefusal::Unsafe("journal owner poisoned".into()))?;
        if owner.poisoned {
            return Err(JournalRefusal::Unsafe("prior fsync/write failure; no more appends".into()));
        }
        if let Err(error) = self.ensure_owner(&owner) {
            owner.poisoned = true;
            return Err(error);
        }
        if let Some(previous) = owner.hashes.get(&commit.order_hash) {
            return if previous == &commit {
                Ok(AppendVerdict::AlreadyDurable)
            } else {
                Err(JournalRefusal::Conflict(commit.order_hash))
            };
        }
        let seq = owner.sequence.checked_add(1)
            .ok_or(JournalRefusal::Invalid("sequence overflow"))?;
        let previous_checksum = owner.last_checksum.clone();
        let hash = checksum(seq, &previous_checksum, &commit)?;
        let row = Frame {
            version: VERSION, seq, previous_checksum,
            commit: commit.clone(), checksum: hash.clone(),
        };
        let mut encoded = serde_json::to_vec(&row)
            .map_err(|e| JournalRefusal::Io(format!("encode frame: {e}")))?;
        encoded.push(b'\n');
        if let Err(e) = owner.writer.write_all(&encoded)
            .and_then(|_| owner.writer.sync_data())
        {
            owner.poisoned = true;
            return Err(JournalRefusal::Io(format!("append/fsync journal: {e}")));
        }
        owner.hashes.insert(commit.order_hash.clone(), commit.clone());
        owner.frames.push(commit);
        owner.sequence = seq;
        owner.last_checksum = hash;
        Ok(AppendVerdict::DurablyWritten)
    }

    /// One future OrderStateOwner recovery seam. Only frames that were already
    /// persisted in this exact journal may create ledger bookings/resolutions.
    /// Preflight ALL identities and all existing money before any mutation.
    /// A crash anywhere in the second phase is retry-safe on journal reopen.
    pub fn repair_legacy(
        &self,
        ledger: &mut Ledger,
        pending: &mut PendingLog,
    ) -> Result<RepairCount, JournalRefusal> {
        if ledger.path.is_empty() || !ledger.persistence_ok() {
            return Err(JournalRefusal::Unsafe("ledger persistence unavailable".into()));
        }
        if !pending.durable_path_configured()
            || pending.unreadable_sources > 0 || !pending.persistence_ok()
        {
            return Err(JournalRefusal::Unsafe("pending persistence unavailable".into()));
        }
        let commits = self.commits()?;
        let open: HashMap<String, Pending> = pending.open_rows().into_iter()
            .map(|p| (p.order_hash.clone(), p)).collect();
        for c in &commits {
            let was_pending = open.get(&c.order_hash);
            let booked = ledger.recorded_order(&c.order_hash);
            if was_pending.is_none() && booked.is_none() {
                return Err(JournalRefusal::NotYetProven(
                    format!("journal has an order absent from both durable owners: {}", c.order_hash),
                ));
            }
            if was_pending.is_some_and(|p| !c.agrees_pending(p))
                || booked.as_ref().is_some_and(|b| !c.agrees_ledger(b))
            {
                return Err(JournalRefusal::Conflict(c.order_hash.clone()));
            }
        }
        let mut report = RepairCount::default();
        for c in commits {
            if !open.contains_key(&c.order_hash) {
                report.already_closed += 1;
                continue;
            }
            let receipt = if let Some(existing) = ledger.recorded_order(&c.order_hash) {
                existing.receipt
            } else {
                let r = ledger.book_fill_ex_px(
                    &c.lane, &c.token, c.side,
                    human_micro(c.filled_shares_micros),
                    human_micro(c.fill_price_micros as u64),
                    human_micro(c.fee_micros),
                    c.leader_price_micros.map(|v| human_micro(v as u64)),
                    &c.order_hash, false, false,
                ).ok_or_else(|| JournalRefusal::Io(
                    format!("failed to durably book journal fill {}", c.order_hash),
                ))?;
                report.fills_booked += 1;
                r
            };
            pending.resolve_booked(&c.order_hash, "atomic_fak_commit", receipt)
                .map_err(|_| JournalRefusal::Io(
                    format!("failed to durably close pending {}", c.order_hash),
                ))?;
            report.pending_closed += 1;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::RiskConfig;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ID: AtomicUsize = AtomicUsize::new(1);
    fn fixture(tag: &str) -> (PathBuf, Ledger, PendingLog) {
        let dir = std::env::temp_dir().join(format!(
            "copybot-d615-{tag}-{}-{}",
            std::process::id(), ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&dir).unwrap();
        let ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("lane".into(), RiskConfig::default())],
        );
        let pending = PendingLog::open(dir.join("pending.jsonl").to_str().unwrap());
        (dir, ledger, pending)
    }
    fn commit(hash: &str, token: &str) -> FakFillCommit {
        FakFillCommit {
            lane: "lane".into(), token: token.into(),
            order_hash: hash.into(), side: 0,
            ordered_shares_micros: 12_000_000,
            limit_price_micros: 500_000,
            filled_shares_micros: 9_000_000,
            fill_price_micros: 450_000,
            fee_micros: 10_000,
            leader_price_micros: Some(420_000),
            price_evidence: PriceEvidence::VenueAmountLegs,
            fee_evidence: FeeEvidence::ExplicitVenueAmount,
        }
    }
    fn reserve(p: &mut PendingLog, c: &FakFillCommit) {
        p.record(Pending {
            lane: c.lane.clone(), token: c.token.clone(),
            order_hash: c.order_hash.clone(), side: c.side,
            shares: human_micro(c.ordered_shares_micros),
            limit: human_micro(c.limit_price_micros as u64),
            ts: 100, why: "synthetic".into(), resting: false,
        }).unwrap();
    }
    fn holding(l: &Ledger, token: &str) -> (f64, f64) {
        l.lanes["lane"].positions.get(token)
            .map(|p| (p.shares, p.cost)).unwrap_or((0.0, 0.0))
    }
    fn reopen(dir: &Path) -> (Ledger, PendingLog) {
        (Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("lane".into(), RiskConfig::default())],
        ), PendingLog::open(dir.join("pending.jsonl").to_str().unwrap()))
    }
    #[test]
    fn one_atomic_record_then_reopen_and_retry_are_idempotent() {
        let (dir, _, _) = fixture("dedup");
        let path = dir.join("owner.jsonl");
        let journal = AtomicFakJournal::open(&path).unwrap();
        let c = commit("h", "token");
        assert_eq!(journal.append(c.clone()), Ok(AppendVerdict::DurablyWritten));
        let length = fs::metadata(&path).unwrap().len();
        assert_eq!(journal.append(c.clone()), Ok(AppendVerdict::AlreadyDurable));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        let mut modified = c.clone();
        modified.fee_micros += 1;
        assert_eq!(journal.append(modified), Err(JournalRefusal::Conflict("h".into())));
        drop(journal);
        let journal = AtomicFakJournal::open(&path).unwrap();
        assert_eq!(journal.commits().unwrap(), vec![c.clone()]);
        assert_eq!(journal.append(c), Ok(AppendVerdict::AlreadyDurable));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn torn_tail_invalid_frame_and_blank_line_fail_closed() {
        for (tag, appended) in [
            ("torn", b"{\"bad\":".as_slice()),
            ("junk", b"this-is-not-json\n".as_slice()),
            ("blank", b"\n".as_slice()),
        ] {
            let (dir, _, _) = fixture(tag);
            let path = dir.join("owner.jsonl");
            let journal = AtomicFakJournal::open(&path).unwrap();
            journal.append(commit("h", "token")).unwrap();
            drop(journal);
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(appended).unwrap();
            f.sync_data().unwrap();
            assert!(matches!(
                AtomicFakJournal::open(&path), Err(JournalRefusal::Corrupt { .. })
            ), "invalid trailing journal bytes must not be ignored");
            fs::remove_dir_all(dir).unwrap();
        }
    }
    #[test]
    fn tampered_complete_row_has_bad_checksum() {
        let (dir, _, _) = fixture("tamper");
        let path = dir.join("owner.jsonl");
        let journal = AtomicFakJournal::open(&path).unwrap();
        journal.append(commit("h", "token")).unwrap();
        drop(journal);
        let raw = fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"fee_micros\":10000"));
        let tampered = raw.replacen("\"fee_micros\":10000", "\"fee_micros\":20000", 1);
        fs::write(&path, tampered).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path),
            Err(JournalRefusal::Corrupt { reason: "checksum mismatch", .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn no_unproved_money_or_bad_limit_can_be_committed() {
        let (dir, _, _) = fixture("invalid");
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        let mut row = serde_json::to_value(commit("h", "T")).unwrap();
        for missing in ["fee_evidence", "price_evidence", "fee_micros", "fill_price_micros"] {
            let value = row.as_object_mut().unwrap().remove(missing).unwrap();
            assert!(serde_json::from_value::<FakFillCommit>(row.clone()).is_err(),
                "missing {missing} must not default to zero/limit");
            row[missing] = value;
        }
        let mut bad = commit("h", "T");
        bad.fill_price_micros = 600_000;
        assert_eq!(
            journal.append(bad),
            Err(JournalRefusal::Invalid("venue price violates submitted limit")),
        );
        let mut bad = commit("h", "T");
        bad.filled_shares_micros = 13_000_000;
        assert!(matches!(journal.append(bad), Err(JournalRefusal::Invalid(_))));
        assert!(journal.commits().unwrap().is_empty());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn crash_after_only_journal_fsync_recovers_book_and_pending_exactly_once() {
        let (dir, ledger, mut pending) = fixture("crash-journal");
        let c = commit("h", "T");
        reserve(&mut pending, &c);
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        assert_eq!(journal.append(c.clone()), Ok(AppendVerdict::DurablyWritten));
        // Crash: only FAK commit and original pending reservation are durable.
        drop(ledger);
        drop(pending);
        drop(journal);
        let (mut ledger, mut pending) = reopen(&dir);
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        let fixed = journal.repair_legacy(&mut ledger, &mut pending).unwrap();
        assert_eq!(fixed, RepairCount {
            fills_booked: 1, pending_closed: 1, already_closed: 0,
        });
        assert_eq!(holding(&ledger, "T"), (9.0, 4.06));
        assert!(!pending.is_open("h"));
        drop(ledger);
        drop(pending);
        let (mut ledger, mut pending) = reopen(&dir);
        assert_eq!(holding(&ledger, "T"), (9.0, 4.06));
        assert_eq!(journal.repair_legacy(&mut ledger, &mut pending).unwrap(),
            RepairCount { fills_booked: 0, pending_closed: 0, already_closed: 1 });
        assert!(!pending.is_open("h"));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn crash_after_ledger_fsync_before_pending_close_does_not_book_again() {
        let (dir, mut ledger, mut pending) = fixture("crash-booked");
        let c = commit("h", "T");
        reserve(&mut pending, &c);
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        journal.append(c.clone()).unwrap();
        let receipt = ledger.book_fill_ex_px(
            "lane", "T", 0, 9.0, 0.45, 0.01,
            Some(0.42), "h", false, false,
        ).unwrap();
        assert_eq!(receipt.shares, 9.0);
        let before = fs::metadata(&ledger.path).unwrap().len();
        drop(ledger);
        drop(pending);
        let (mut ledger, mut pending) = reopen(&dir);
        let repaired = journal.repair_legacy(&mut ledger, &mut pending).unwrap();
        assert_eq!(repaired, RepairCount {
            fills_booked: 0, pending_closed: 1, already_closed: 0,
        });
        assert_eq!(fs::metadata(&ledger.path).unwrap().len(), before);
        assert_eq!(holding(&ledger, "T"), (9.0, 4.06));
        assert!(!pending.is_open("h"));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn no_pending_or_ledger_owner_refuses_to_invent_a_fill() {
        let (dir, mut ledger, mut pending) = fixture("missing");
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        journal.append(commit("h", "T")).unwrap();
        assert!(matches!(
            journal.repair_legacy(&mut ledger, &mut pending),
            Err(JournalRefusal::NotYetProven(_)),
        ));
        assert_eq!(holding(&ledger, "T"), (0.0, 0.0));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn all_commit_rows_are_preflighted_before_any_legacy_write() {
        let (dir, mut ledger, mut pending) = fixture("preflight");
        let one = commit("h1", "T1");
        let mut two = commit("h2", "T2");
        reserve(&mut pending, &one);
        reserve(&mut pending, &two);
        let journal = AtomicFakJournal::open(dir.join("owner.jsonl")).unwrap();
        journal.append(one.clone()).unwrap();
        two.fill_price_micros = 400_000;
        journal.append(two).unwrap();
        // A divergent prior ledger booking for the *second* order must
        // prevent the first commitment from changing any inventory.
        let _receipt = ledger.book_fill_ex_px(
            "lane", "T2", 0, 9.0, 0.45, 0.01,
            Some(0.42), "h2", false, false,
        ).unwrap();
        assert_eq!(journal.repair_legacy(&mut ledger, &mut pending),
            Err(JournalRefusal::Conflict("h2".into())));
        assert_eq!(holding(&ledger, "T1"), (0.0, 0.0));
        assert!(pending.is_open("h1") && pending.is_open("h2"));
        fs::remove_dir_all(dir).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn second_handle_in_one_process_refuses_until_first_drops() {
        let (dir, _, _) = fixture("lease-same-pid");
        let path = dir.join("owner.jsonl");
        let first = AtomicFakJournal::open(&path).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path),
            Err(JournalRefusal::Unsafe(reason)) if reason.contains("flock")
        ), "two independent handles must not share one write authority");
        first.append(commit("h", "token")).unwrap();
        drop(first);
        let second = AtomicFakJournal::open(&path).unwrap();
        assert_eq!(second.commits().unwrap().len(), 1);
        drop(second);
        std::fs::remove_dir_all(dir).unwrap();
    }
    /// Child subprocess entrypoint. The parent invokes the *same test binary*
    /// using --exact so it requires no shell, network, or external test tool.
    #[cfg(unix)]
    #[test]
    fn subprocess_lease_child() {
        let Ok(path) = std::env::var("COPYBOT_D617_CHILD_JOURNAL_PATH") else {
            return; // ordinary unit-test pass is inert
        };
        let should_open = std::env::var("COPYBOT_D617_CHILD_SHOULD_OPEN")
            .unwrap_or_default() == "1";
        let outcome = AtomicFakJournal::open(&path);
        if should_open {
            let journal = outcome.expect("new process must acquire lease after owner exits");
            assert!(journal.commits().unwrap().is_empty());
        } else {
            assert!(
                matches!(outcome, Err(JournalRefusal::Unsafe(reason))
                    if reason.contains("flock")),
                "a second process must not replay or append while an owner lives",
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn cross_process_lease_excludes_then_survives_owner_drop() {
        let (dir, _, _) = fixture("lease-two-pids");
        let path = dir.join("owner.jsonl");
        let first = AtomicFakJournal::open(&path).unwrap();
        let invoke = |allowed: bool| {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "order_commit::tests::subprocess_lease_child",
                    "--nocapture"])
                .env("COPYBOT_D617_CHILD_JOURNAL_PATH", &path)
                .env("COPYBOT_D617_CHILD_SHOULD_OPEN", if allowed { "1" } else { "0" })
                .output().expect("spawn owned test child");
            assert!(output.status.success(),
                "subprocess lease probe failed: {}",
                String::from_utf8_lossy(&output.stdout));
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"),
                "child's exact lease test must actually execute");
        };
        invoke(false);
        drop(first);
        invoke(true);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn refuse_symlinked_lock_file_or_journal_file() {
        use std::os::unix::fs::symlink;
        let (dir, _, _) = fixture("symlinks");
        let path = dir.join("owner.jsonl");
        let lockpath = dir.join("owner.jsonl.owner.lock");
        let target = dir.join("candidate");
        fs::write(&target, "").unwrap();
        symlink(&target, &lockpath).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path), Err(JournalRefusal::Unsafe(_))
        ), "a symlinked owner file must never be followed");
        fs::remove_file(&lockpath).unwrap();
        symlink(&target, &path).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path), Err(JournalRefusal::Unsafe(_))
        ), "a symlinked journal must never be followed");
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn reject_world_readable_or_hardlinked_owner_identity() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, _, _) = fixture("owner-perms");
        let path = dir.join("owner.jsonl");
        let journal = AtomicFakJournal::open(&path).unwrap();
        drop(journal);
        let mut mode = fs::metadata(&path).unwrap().permissions();
        mode.set_mode(0o644);
        fs::set_permissions(&path, mode).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path), Err(JournalRefusal::Unsafe(_))
        ), "even readable historical order journal is not owner-only");
        let mut mode = fs::metadata(&path).unwrap().permissions();
        mode.set_mode(0o600);
        fs::set_permissions(&path, mode).unwrap();
        fs::hard_link(&path, dir.join("alias")).unwrap();
        assert!(matches!(
            AtomicFakJournal::open(&path), Err(JournalRefusal::Unsafe(_))
        ), "a second hardlink can bypass canonical path checks");
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn replacing_a_live_journal_or_lock_inode_poison_writes() {
        for which in ["journal", "lease"] {
            let (dir, _, _) = fixture(which);
            let path = dir.join("owner.jsonl");
            let journal = AtomicFakJournal::open(&path).unwrap();
            let original = if which == "journal" {
                path.clone()
            } else {
                journal.lease_path.clone()
            };
            let saved = dir.join("old-inode");
            fs::rename(&original, &saved).unwrap();
            fs::write(&original, b"").unwrap();
            assert!(matches!(
                journal.append(commit("h", "T")),
                Err(JournalRefusal::Unsafe(_))
            ), "unlinked writer must never record a successful commit");
            assert!(journal.commits().is_err(), "poisoned owner must not provide valid replay");
            assert!(journal.append(commit("h", "T")).is_err(),
                "poison remains latched for the process lifetime");
            assert!(fs::read_to_string(&saved).unwrap().is_empty());
            drop(journal);
            fs::remove_dir_all(dir).unwrap();
        }
    }
    #[test]
    fn concurrent_same_digest_is_written_one_time() {
        let (dir, _, _) = fixture("threads");
        let path = dir.join("owner.jsonl");
        let journal = std::sync::Arc::new(AtomicFakJournal::open(&path).unwrap());
        let mut handles = Vec::new();
        for _ in 0..12 {
            let owner = journal.clone();
            handles.push(std::thread::spawn(move || owner.append(commit("h", "T")).unwrap()));
        }
        let results: Vec<AppendVerdict> = handles.into_iter()
            .map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| **r == AppendVerdict::DurablyWritten).count(), 1);
        assert_eq!(results.iter().filter(|r| **r == AppendVerdict::AlreadyDurable).count(), 11);
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.lines().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }
}
