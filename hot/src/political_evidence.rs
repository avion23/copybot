//! Pure, all-row validation for the political-market exclusion book.
//! An incomplete activity history cannot certify that a condition is nonpolitical.

use std::collections::HashSet;
use std::fmt;

pub const PAGE: usize = 500;
pub const MAX_PAGES: usize = 8;
pub type Conditions = HashSet<[u8; 32]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceError {
    OversizedPage { page: usize, rows: usize },
    InvalidType { page: usize, row: usize },
    InvalidTitle { page: usize, row: usize },
    InvalidCondition { page: usize, row: usize },
    Truncated { pages: usize, cap: usize },
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OversizedPage { page, rows } => {
                write!(f, "activity page {} contained {rows} rows, limit {PAGE}", page + 1)
            }
            Self::InvalidType { page, row } => {
                write!(f, "activity page {} row {row} has no type", page + 1)
            }
            Self::InvalidTitle { page, row } => {
                write!(f, "activity page {} row {row} has no title", page + 1)
            }
            Self::InvalidCondition { page, row } => {
                write!(f, "activity page {} row {row} has no valid political condition ID", page + 1)
            }
            Self::Truncated { pages, cap } => {
                write!(f, "activity history truncated at {pages} pages (cap {cap})")
            }
        }
    }
}
impl std::error::Error for EvidenceError {}

/// Validate every row before allowing any response to update the shared
/// exclusion set. Only a short final page proves this history is complete.
pub fn check_page(
    rows: &[serde_json::Value],
    page: usize,
    classify_title: fn(&str) -> bool,
) -> Result<(Conditions, bool), EvidenceError> {
    if rows.len() > PAGE {
        return Err(EvidenceError::OversizedPage { page, rows: rows.len() });
    }
    let mut conditions = Conditions::new();
    for (row, item) in rows.iter().enumerate() {
        let kind = item["type"].as_str()
            .ok_or(EvidenceError::InvalidType { page, row })?;
        let title = item["title"].as_str()
            .ok_or(EvidenceError::InvalidTitle { page, row })?;
        if kind == "SPLIT" || kind == "MERGE" || classify_title(title) {
            let condition = item["conditionId"].as_str()
                .and_then(crate::merge::condition_id_bytes)
                .ok_or(EvidenceError::InvalidCondition { page, row })?;
            conditions.insert(condition);
        }
    }
    let complete = rows.len() < PAGE;
    if !complete && page + 1 >= MAX_PAGES {
        return Err(EvidenceError::Truncated { pages: page + 1, cap: MAX_PAGES });
    }
    Ok((conditions, complete))
}

/// A policy-excluded lane stays blocked while no complete validated snapshot
/// exists. Poisoned read locks are also treated as not ready by the caller.
pub fn blocks_condition(snapshot: Option<&Conditions>, condition: &[u8; 32]) -> bool {
    snapshot.is_none_or(|conditions| conditions.contains(condition))
}
