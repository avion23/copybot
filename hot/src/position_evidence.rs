//! Pure validation for complete position balances used to seed a leader book.
//!
//! A complete page sequence does not prove that a malformed row is absent.
//! No balance map is returned until every row is valid and uniquely identified.

use crate::positions::{Completeness, Positions};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionError {
    Truncated { pages: usize, cap: usize },
    Unavailable { after_pages: usize },
    InvalidAsset { row: usize },
    DuplicateAsset { row: usize },
    InvalidSize { row: usize },
    InvalidAveragePrice { row: usize },
}

impl fmt::Display for PositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { pages, cap } => {
                write!(f, "position snapshot truncated after {pages} pages (cap {cap})")
            }
            Self::Unavailable { after_pages } => {
                write!(f, "position snapshot unavailable after {after_pages} pages")
            }
            Self::InvalidAsset { row } => write!(f, "position row {row} has no valid asset"),
            Self::DuplicateAsset { row } => write!(f, "position row {row} repeats an asset"),
            Self::InvalidSize { row } => write!(f, "position row {row} has invalid size"),
            Self::InvalidAveragePrice { row } => write!(f, "position row {row} has invalid average price"),
        }
    }
}
impl std::error::Error for PositionError {}

pub fn complete_balances(snapshot: &Positions) -> Result<HashMap<String, f64>, PositionError> {
    match &snapshot.completeness {
        Completeness::Complete => {}
        Completeness::Truncated { pages, cap } => {
            return Err(PositionError::Truncated { pages: *pages, cap: *cap });
        }
        Completeness::Failed { after_pages, .. } => {
            return Err(PositionError::Unavailable { after_pages: *after_pages });
        }
    }
    let mut balances = HashMap::with_capacity(snapshot.rows.len());
    for (index, value) in snapshot.rows.iter().enumerate() {
        let token = value["asset"].as_str()
            .filter(|s| !s.is_empty() && s.trim() == *s)
            .ok_or(PositionError::InvalidAsset { row: index })?;
        if balances.contains_key(token) {
            return Err(PositionError::DuplicateAsset { row: index });
        }
        let parsed = value["size"].as_f64()
            .or_else(|| value["size"].as_str().and_then(|text| text.parse().ok()));
        let size = match parsed {
            Some(n) if n.is_finite() && n >= 0.0 => n,
            _ => return Err(PositionError::InvalidSize { row: index }),
        };
        balances.insert(token.to_owned(), size);
    }
    Ok(balances)
}

/// Read-only proposed shadow fills. No ledger writes occur until the entire
/// snapshot and every candidate's recorded average price are validated.
pub fn shadow_adoptions(
    snapshot: &Positions,
    leader: &HashMap<String, f64>,
    held: &HashMap<String, f64>,
) -> Result<Vec<(String, f64, f64)>, PositionError> {
    let balances = complete_balances(snapshot)?;
    let mut planned = Vec::new();
    for (index, row) in snapshot.rows.iter().enumerate() {
        // complete_balances already checked asset existence, uniqueness, and size.
        let token = row["asset"].as_str()
            .ok_or(PositionError::InvalidAsset { row: index })?;
        if !leader.contains_key(token) {
            continue;
        }
        let size = *balances.get(token)
            .ok_or(PositionError::InvalidSize { row: index })?;
        let diff = size - held.get(token).copied().unwrap_or(0.0);
        if diff <= 0.01 {
            continue;
        }
        let avg = row["avgPrice"].as_f64()
            .or_else(|| row["avgPrice"].as_str().and_then(|text| text.parse().ok()))
            .filter(|v| v.is_finite() && *v > 0.0 && *v <= 1.0)
            .ok_or(PositionError::InvalidAveragePrice { row: index })?;
        planned.push((token.to_owned(), diff, avg));
    }
    Ok(planned)
}
