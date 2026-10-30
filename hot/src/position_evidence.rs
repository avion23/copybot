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
