use serde::Deserialize;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Graceful,
    Hybrid,
    Panic,
}
impl Mode {
    pub fn parse(s: &str) -> Option<Mode> {
        match s {
            "graceful" => Some(Mode::Graceful),
            "hybrid" => Some(Mode::Hybrid),
            "panic" => Some(Mode::Panic),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Graceful => "graceful",
            Mode::Hybrid => "hybrid",
            Mode::Panic => "panic",
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct Intent {
    pub lane: String,
    pub mode: String,
    pub phrase: String,
    pub ts: i64,
    #[serde(default)]
    pub acknowledge_forfeit: bool,
}
pub const INTENT_TTL_SECS: i64 = 60;
pub fn gate(
    intent: &Intent,
    lane_name: &str,
    armed: bool,
    now: i64,
) -> Result<Mode, String> {
    let mode = Mode::parse(&intent.mode)
        .ok_or_else(|| format!("unknown flatten mode {:?}", intent.mode))?;
    if intent.lane != lane_name {
        return Err(format!("intent lane {:?} != {lane_name}", intent.lane));
    }
    let expected = format!("{} {}", mode.as_str(), lane_name);
    if intent.phrase != expected {
        return Err(format!("phrase must be {expected:?}, got {:?}", intent.phrase));
    }
    if now - intent.ts > INTENT_TTL_SECS {
        return Err(
            format!("intent is {}s old (max {INTENT_TTL_SECS})", now - intent.ts),
        );
    }
    if intent.ts > now + 5 {
        return Err("intent timestamp is in the future".into());
    }
    match mode {
        Mode::Panic if armed => {
            Err("panic requires the lane to be DISARMED first".into())
        }
        Mode::Hybrid if !intent.acknowledge_forfeit => {
            Err(
                "hybrid requires acknowledge_forfeit=true (it sells winners early)"
                    .into(),
            )
        }
        _ => Ok(mode),
    }
}
pub fn should_sell(mode: Mode, mark: f64, avg_cost: f64) -> bool {
    match mode {
        Mode::Panic => true,
        Mode::Hybrid => mark > avg_cost + 1e-9,
        Mode::Graceful => false,
    }
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketTerms {
    pub mark: f64,
    pub negative_risk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvidenceError {
    Snapshot(crate::position_evidence::PositionError),
    InvalidHolding(String),
    MissingHeldAsset(String),
    InvalidMark(String),
    MissingRiskDomain(String),
}

impl std::fmt::Display for MarketEvidenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snapshot(error) => write!(f, "incomplete or invalid position snapshot: {error}"),
            Self::InvalidHolding(token) => write!(f, "invalid ledger holding for {token}"),
            Self::MissingHeldAsset(token) => write!(f, "missing venue position for held {token}"),
            Self::InvalidMark(token) => write!(f, "invalid venue mark for held {token}"),
            Self::MissingRiskDomain(token) => {
                write!(f, "missing negativeRisk domain for held {token}")
            }
        }
    }
}
impl std::error::Error for MarketEvidenceError {}

/// Validate all visible positions before treating a missing holding as real.
/// No default price or exchange domain is permitted for a held token.
pub fn market_terms_for_held(
    snapshot: &crate::positions::Positions,
    held: &std::collections::HashMap<String, f64>,
) -> Result<std::collections::HashMap<String, MarketTerms>, MarketEvidenceError> {
    crate::position_evidence::complete_balances(snapshot)
        .map_err(MarketEvidenceError::Snapshot)?;

    let rows: std::collections::HashMap<&str, &serde_json::Value> = snapshot.rows
        .iter()
        .filter_map(|row| row["asset"].as_str().map(|asset| (asset, row)))
        .collect();
    let mut terms = std::collections::HashMap::with_capacity(held.len());
    for (token, size) in held {
        if !size.is_finite() || *size < 0.0 {
            return Err(MarketEvidenceError::InvalidHolding(token.clone()));
        }
        if *size <= 1e-9 {
            continue;
        }
        let row = rows.get(token.as_str())
            .ok_or_else(|| MarketEvidenceError::MissingHeldAsset(token.clone()))?;
        let mark = row["curPrice"].as_f64()
            .or_else(|| row["curPrice"].as_str().and_then(|text| text.parse().ok()))
            .filter(|price| price.is_finite() && *price > 0.0 && *price <= 1.0)
            .ok_or_else(|| MarketEvidenceError::InvalidMark(token.clone()))?;
        let negative_risk = row["negativeRisk"].as_bool()
            .ok_or_else(|| MarketEvidenceError::MissingRiskDomain(token.clone()))?;
        terms.insert(token.clone(), MarketTerms { mark, negative_risk });
    }
    Ok(terms)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn intent(mode: &str, lane: &str, phrase: &str, ts: i64) -> Intent {
        Intent {
            lane: lane.into(),
            mode: mode.into(),
            phrase: phrase.into(),
            ts,
            acknowledge_forfeit: false,
        }
    }
    #[test]
    fn a_correct_panic_intent_on_a_disarmed_lane_passes() {
        let i = intent("panic", "example_lane_26", "panic example_lane_26", 1000);
        assert_eq!(gate(& i, "example_lane_26", false, 1010).unwrap(), Mode::Panic);
    }
    #[test]
    fn panic_on_an_ARMED_lane_is_refused() {
        let i = intent("panic", "example_lane_26", "panic example_lane_26", 1000);
        assert!(
            gate(& i, "example_lane_26", true, 1010).is_err(), "cannot panic while still buying"
        );
    }
    #[test]
    fn a_wrong_phrase_is_refused() {
        assert!(
            gate(& intent("panic", "example_lane_26", "panic", 1000), "example_lane_26", false, 1010).is_err()
        );
        assert!(
            gate(& intent("panic", "example_lane_26", "panic peyz", 1000), "example_lane_26", false, 1010)
            .is_err()
        );
        assert!(
            gate(& intent("panic", "example_lane_26", "PANIC example_lane_26", 1000), "example_lane_26", false, 1010)
            .is_err()
        );
    }
    #[test]
    fn a_stale_or_future_intent_is_refused() {
        assert!(
            gate(& intent("panic", "example_lane_26", "panic example_lane_26", 1000), "example_lane_26", false, 1000 + 61)
            .is_err()
        );
        assert!(
            gate(& intent("panic", "example_lane_26", "panic example_lane_26", 1000), "example_lane_26", false, 900)
            .is_err()
        );
    }
    #[test]
    fn hybrid_requires_the_forfeit_acknowledgement() {
        let mut i = intent("hybrid", "example_lane_26", "hybrid example_lane_26", 1000);
        assert!(gate(& i, "example_lane_26", false, 1010).is_err(), "no ack -> refused");
        i.acknowledge_forfeit = true;
        assert_eq!(gate(& i, "example_lane_26", false, 1010).unwrap(), Mode::Hybrid);
    }
    #[test]
    fn the_intent_lane_must_match_the_target() {
        let i = intent("panic", "peyz", "panic peyz", 1000);
        assert!(
            gate(& i, "example_lane_26", false, 1010).is_err(),
            "an intent for peyz cannot flatten example_lane_26"
        );
    }
    #[test]
    fn panic_sells_everything_hybrid_sells_only_winners_graceful_nothing() {
        assert!(should_sell(Mode::Panic, 0.10, 0.90));
        assert!(should_sell(Mode::Panic, 0.90, 0.10));
        assert!(should_sell(Mode::Hybrid, 0.90, 0.50), "a winner is sold");
        assert!(! should_sell(Mode::Hybrid, 0.40, 0.50), "a loser is left for graceful");
        assert!(
            ! should_sell(Mode::Hybrid, 0.50, 0.50), "a flat position is not a winner"
        );
        assert!(! should_sell(Mode::Graceful, 0.90, 0.10));
    }
}
