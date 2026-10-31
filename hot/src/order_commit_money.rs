//! Non-FAK monetary facts in the SAME fsynced, leased journal as terminal fills.
//! A source is a redemption transaction/outcome, never a polling timestamp.
//! Projection is deterministic and refuses a changed basis or a conflicting
//! Ledger row; a durable journal frame alone may authorize crash replay.
use super::*;
use crate::settlement::Redemption;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MoneyCommit {
    Redeemed {
        lane: String,
        token: String,
        redemption: Redemption,
        shares: f64,
        basis_cost: f64,
        payout: f64,
        proceeds: f64,
        zero_snapshot_as_of: i64,
        ledger_t: i64,
    },
    Correction {
        lane: String,
        token: String,
        redemption: Redemption,
        shares: f64,
        proceeds: f64,
        avg_cost: f64,
        neutral_release_t: i64,
        zero_snapshot_as_of: i64,
        ledger_t: i64,
    },
}

impl MoneyCommit {
    fn identity(&self) -> (&str, &str, &Redemption, i64, i64) {
        match self {
            Self::Redeemed { lane, token, redemption, zero_snapshot_as_of, ledger_t, .. }
            | Self::Correction { lane, token, redemption, zero_snapshot_as_of, ledger_t, .. } =>
                (lane, token, redemption, *zero_snapshot_as_of, *ledger_t),
        }
    }
    pub fn key(&self) -> String {
        let (lane, _, redemption, ..) = self.identity();
        redemption.lane_key(lane)
    }
    pub fn validate(&self) -> Result<(), JournalRefusal> {
        let (lane, token, source, snapshot_t, ledger_t) = self.identity();
        let hash = source.tx.strip_prefix("0x").unwrap_or("");
        let condition = source.condition_id.strip_prefix("0x").unwrap_or("");
        if lane.is_empty() || lane.len() > 128 || token.is_empty() || token.len() > 130
            || source.asset != token || !source.is_bookable()
            || hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            || condition.len() != 64 || !condition.bytes().all(|b| b.is_ascii_hexdigit())
            || snapshot_t <= source.ts || ledger_t <= 0
        {
            return Err(JournalRefusal::Invalid("redemption causal source or zero-snapshot proof missing"));
        }
        let finite = |n: f64| n.is_finite() && n >= 0.0 && n <= 9_000_000_000.0;
        let expected_price = source.usdc / source.size;
        match self {
            Self::Redeemed { shares, basis_cost, payout, proceeds, .. } => {
                if !finite(*shares) || *shares <= 0.0 || *shares > source.size + 1e-6
                    || !finite(*basis_cost) || !finite(*proceeds)
                    || !finite(*payout) || *payout > 1.0
                    || (*payout - expected_price).abs() > 1e-10
                    || (*proceeds - *shares * *payout).abs() > 1e-7
                {
                    return Err(JournalRefusal::Invalid("redemption amount/basis cannot be replayed"));
                }
            }
            Self::Correction { shares, proceeds, avg_cost, neutral_release_t, .. } => {
                if !finite(*shares) || *shares <= 0.0 || *shares > source.size + 1e-6
                    || !finite(*proceeds) || !finite(*avg_cost)
                    || *neutral_release_t <= 0 || *neutral_release_t > source.ts
                    || (*proceeds - *shares * expected_price).abs() > 1e-7
                {
                    return Err(JournalRefusal::Invalid("correction money or prior neutral basis unproved"));
                }
            }
        }
        Ok(())
    }
    /// Every field that can affect P&L or duplicate detection is reproduced
    /// from the immutable frame; never re-query a source or use today's price.
    pub(crate) fn ledger_row(&self) -> serde_json::Value {
        match self {
            Self::Redeemed { lane, token, redemption: r, shares, basis_cost,
                payout, proceeds, zero_snapshot_as_of, ledger_t } => serde_json::json!({
                "ev":"settle", "lane":lane, "token":token, "shares":shares,
                "basis_cost":basis_cost, "payout":payout, "proceeds":proceeds,
                "key":r.lane_key(lane), "alt_key":crate::settlement::cross_writer_key(lane,token),
                "redemption_tx":r.tx, "redemption_t":r.ts,
                "zero_snapshot_as_of":zero_snapshot_as_of, "t":ledger_t,
                "why":"auto-redemption confirmed by activity and complete zero-balance snapshot"
            }),
            Self::Correction { lane, token, redemption: r, shares,
                proceeds, avg_cost, neutral_release_t, zero_snapshot_as_of,
                ledger_t } => serde_json::json!({
                "ev":"realised_adjust", "lane":lane, "token":token,
                "shares":shares, "proceeds":proceeds, "avg_cost":avg_cost,
                "pnl":proceeds - shares * avg_cost,
                "why":"redemption corroborated neutral share release",
                "key":r.lane_key(lane), "alt_key":crate::settlement::cross_writer_key(lane,token),
                "redemption_tx":r.tx, "redemption_t":r.ts,
                "neutral_release_t":neutral_release_t, "zero_snapshot_as_of":zero_snapshot_as_of,
                "t":ledger_t
            }),
        }
    }
    fn preflight_projection(&self, ledger: &Ledger) -> Result<(), JournalRefusal> {
        let (lane, token, r, ..) = self.identity();
        match self {
            Self::Redeemed { shares, basis_cost, .. } => {
                let pos = ledger.lanes.get(lane)
                    .and_then(|b| b.positions.get(token))
                    .ok_or_else(|| JournalRefusal::NotYetProven(self.key()))?;
                if (pos.shares - shares).abs() > 1e-6
                    || (pos.cost - basis_cost).abs() > 1e-6
                    || pos.last_fill_t <= 0 || pos.last_fill_t > r.ts
                    || ledger.lanes.values().filter(|b| b.positions.get(token)
                        .is_some_and(|p| p.shares > 1e-9)).count() != 1
                {
                    return Err(JournalRefusal::NotYetProven(format!(
                        "redeemed share/basis changed before journal projection: {}", self.key()
                    )));
                }
            }
            Self::Correction { shares, avg_cost, neutral_release_t, .. } => {
                let previous = ledger.last_recon_release(lane, token, 0)
                    .ok_or_else(|| JournalRefusal::NotYetProven(self.key()))?;
                if previous.0 + 1e-6 < *shares || (previous.1 - avg_cost).abs() > 1e-6
                    || previous.2 != *neutral_release_t
                {
                    return Err(JournalRefusal::NotYetProven(format!(
                        "neutral release basis no longer matches correction: {}", self.key()
                    )));
                }
            }
        }
        Ok(())
    }
}

pub(super) fn money_checksum(seq: u64, prev: &str, c: &MoneyCommit) -> Result<String, JournalRefusal> {
    let bytes = serde_json::to_vec(&(VERSION, seq, prev, c))
        .map_err(|e| JournalRefusal::Io(format!("encode money checksum: {e}")))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

impl AtomicFakJournal {
    pub fn money_commits(&self) -> Result<Vec<MoneyCommit>, JournalRefusal> {
        let owner = self.state.lock()
            .map_err(|_| JournalRefusal::Unsafe("money owner mutex poisoned".into()))?;
        if owner.poisoned { return Err(JournalRefusal::Unsafe("money journal poisoned".into())); }
        self.ensure_owner(&owner)?;
        Ok(owner.money_frames.clone())
    }
    pub fn append_money(&self, commit: MoneyCommit) -> Result<AppendVerdict, JournalRefusal> {
        commit.validate()?;
        let key = commit.key();
        let mut owner = self.state.lock()
            .map_err(|_| JournalRefusal::Unsafe("money owner mutex poisoned".into()))?;
        if owner.poisoned { return Err(JournalRefusal::Unsafe("money journal poisoned".into())); }
        if let Err(err) = self.ensure_owner(&owner) {
            owner.poisoned = true;
            return Err(err);
        }
        if let Some(previous) = owner.money_keys.get(&key) {
            return if previous == &commit {
                Ok(AppendVerdict::AlreadyDurable)
            } else {
                Err(JournalRefusal::Conflict(key))
            };
        }
        let seq = owner.sequence.checked_add(1)
            .ok_or(JournalRefusal::Invalid("money sequence overflow"))?;
        let prev = owner.last_checksum.clone();
        let hash = money_checksum(seq, &prev, &commit)?;
        let row = Frame { version: VERSION, seq, previous_checksum: prev,
            commit: None, money: Some(commit.clone()), checksum: hash.clone() };
        let mut bytes = serde_json::to_vec(&row)
            .map_err(|e| JournalRefusal::Io(format!("encode money frame: {e}")))?;
        bytes.push(b'\n');
        if let Err(e) = owner.writer.write_all(&bytes).and_then(|_| owner.writer.sync_data()) {
            owner.poisoned = true;
            return Err(JournalRefusal::Io(format!("append/fsync money frame: {e}")));
        }
        owner.money_keys.insert(key, commit.clone());
        owner.money_frames.push(commit);
        owner.sequence = seq;
        owner.last_checksum = hash;
        Ok(AppendVerdict::DurablyWritten)
    }

    /// Boot-replay non-FAK money after FAK fills and before trading. A prior
    /// projected key must be byte-for-byte identical in its monetary meaning.
    /// No event is invented from the Ledger or a balance-only discrepancy.
    pub fn repair_money(&self, ledger: &mut Ledger, pending: &PendingLog)
        -> Result<MoneyRepairCount, JournalRefusal>
    {
        if ledger.path.is_empty() || !ledger.persistence_ok()
            || !pending.durable_path_configured()
            || pending.unreadable_sources > 0 || !pending.persistence_ok()
        {
            return Err(JournalRefusal::Unsafe("money replay owners not durable".into()));
        }
        let mut repaired = MoneyRepairCount::default();
        for fact in self.money_commits()? {
            let (lane, token, ..) = fact.identity();
            if pending.open_rows().iter().any(|p| p.token == token) {
                return Err(JournalRefusal::NotYetProven(format!(
                    "open order prevents money replay for token {}", token
                )));
            }
            let expected = fact.ledger_row();
            let raw = fs::read_to_string(&ledger.path)
                .map_err(|e| JournalRefusal::Io(format!("read keyed Ledger: {e}")))?;
            let mut found = false;
            for line in raw.lines() {
                let row: serde_json::Value = serde_json::from_str(line)
                    .map_err(|_| JournalRefusal::Unsafe("malformed Ledger during money replay".into()))?;
                if row["lane"].as_str() == Some(lane)
                    && (row["key"].as_str() == Some(&fact.key())
                        || row["alt_key"].as_str() == expected["alt_key"].as_str())
                {
                    if row != expected {
                        return Err(JournalRefusal::Conflict(fact.key()));
                    }
                    found = true;
                }
            }
            if found {
                repaired.already_projected += 1;
                continue;
            }
            fact.preflight_projection(ledger)?;
            if !ledger.apply(&expected, true) {
                return Err(JournalRefusal::Io(format!(
                    "money journal could not project to Ledger: {}", fact.key()
                )));
            }
            repaired.projected += 1;
        }
        Ok(repaired)
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoneyRepairCount {
    pub projected: usize,
    pub already_projected: usize,
}
