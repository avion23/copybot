//! Read-side ledger queries: realised series, position claims, and settlement replay evidence.

use super::*;

pub fn equity_series_at(path: &str, lane: Option<&str>) -> Vec<(i64, f64)> {
    {
        let raw = match std::fs::read_to_string(path) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        let mut scratch = Ledger {
            settlement_keys: std::collections::HashSet::new(),
            lane_ids: HashMap::new(),
            path: String::new(),
            lanes: HashMap::new(),
            corrupt_lines: 0,
            events: 0,
            write_failures: 0,
            last_write_error: None,
            booked_receipts: HashMap::new(),
        };
        let mut out: Vec<(i64, f64)> = Vec::new();
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue
            };
            let row_lane = v["lane"].as_str().unwrap_or("");
            if row_lane.is_empty() {
                continue;
            }
            scratch
                .lanes
                .entry(row_lane.to_string())
                .or_insert_with(|| LaneBook::fresh(row_lane, RiskConfig::default()));
            scratch.apply(&v, false);
            if let Some(name) = lane {
                if row_lane != name {
                    continue;
                }
            }
            let Some(t) = v["t"].as_i64().filter(|t| *t > 0) else { continue };
            let total = match lane {
                Some(name) => {
                    scratch.lanes.get(name).map(|b| b.risk.realised_pnl).unwrap_or(0.0)
                }
                None => scratch.lanes.values().map(|b| b.risk.realised_pnl).sum(),
            };
            let total = (total * 100.0).round() / 100.0;
            match out.last_mut() {
                Some(p) if p.0 == t => p.1 = total,
                _ => out.push((t, total)),
            }
        }
        out
    }
}
impl Ledger {
    #[cfg(test)]
    pub fn realised_windows(&self, lane: Option<&str>, now: i64) -> RealisedWindows {
        RealisedWindows::from_series(&self.equity_series(lane), now)
    }
}
impl RealisedWindows {
    pub fn from_series(series: &[(i64, f64)], now: i64) -> RealisedWindows {
        let total = series.last().map(|p| p.1).unwrap_or(0.0);
        let cum_at = |cutoff: i64| -> f64 {
            series.iter().rev().find(|p| p.0 <= cutoff).map(|p| p.1).unwrap_or(0.0)
        };
        let win = |cutoff: i64| ((total - cum_at(cutoff)) * 100.0).round() / 100.0;
        RealisedWindows {
            total: (total * 100.0).round() / 100.0,
            d1: win(now - 86_400),
            d7: win(now - 7 * 86_400),
            d30: win(now - 30 * 86_400),
            ytd: win(year_start_secs(now)),
            y1: win(now - 365 * 86_400),
        }
    }
}
impl Ledger {
    pub fn pool_claim(&self, tok: &str) -> f64 {
        self.lanes.values().filter_map(|b| b.positions.get(tok)).map(|p| p.shares).sum()
    }
    pub fn adoptable(
        &self,
        lane: &str,
        chain: &HashMap<String, f64>,
        quiet_secs: i64,
        now: i64,
        max_shares: f64,
    ) -> Vec<(String, f64, f64)> {
        let Some(b) = self.lanes.get(lane) else { return Vec::new() };
        let mut out = Vec::new();
        for (tok, pos) in b.positions.iter() {
            if pos.shares <= 1e-9 {
                continue;
            }
            if pos.last_fill_t <= 0 {
                continue;
            }
            if now - pos.last_fill_t < quiet_secs {
                continue;
            }
            let on_chain = chain.get(tok).copied().unwrap_or(0.0);
            let extra = on_chain - pos.shares;
            if extra <= 0.001 {
                continue;
            }
            if extra >= max_shares {
                continue;
            }
            let claimed_by_pool: f64 = self.pool_claim(tok);
            let unexplained = on_chain - claimed_by_pool;
            if unexplained <= 1e-9 {
                continue;
            }
            let take = extra.min(unexplained);
            if take <= 0.001 {
                continue;
            }
            out.push((tok.clone(), take, pos.avg_cost()));
        }
        out
    }
    pub fn releasable(
        &self,
        lane: &str,
        chain: &HashMap<String, f64>,
        quiet_secs: i64,
        now: i64,
    ) -> Vec<(String, f64, f64)> {
        let Some(b) = self.lanes.get(lane) else { return Vec::new() };
        let mut out = Vec::new();
        for (tok, pos) in b.positions.iter() {
            if pos.shares <= 1e-9 {
                continue;
            }
            if pos.last_fill_t <= 0 {
                continue;
            }
            if now - pos.last_fill_t < quiet_secs {
                continue;
            }
            let on_chain = chain.get(tok).copied().unwrap_or(0.0);
            let gone = pos.shares - on_chain;
            if gone <= 1e-9 {
                continue;
            }
            if gone <= 0.01 && on_chain > 0.0 {
                continue;
            }
            out.push((tok.clone(), gone, pos.avg_cost()));
        }
        out
    }
    pub fn open_positions(
        &self,
        lane: &str,
    ) -> Vec<(String, f64, f64, i64, Option<f64>, f64)> {
        self.lanes
            .get(lane)
            .map(|b| {
                b.positions
                    .iter()
                    .filter(|(_, p)| p.shares > 1e-9)
                    .map(|(t, p)| (
                        t.clone(),
                        p.shares,
                        p.avg_cost(),
                        p.opened_t,
                        p.avg_his_price(),
                        p.his_shares_known,
                    ))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn persistence_ok(&self) -> bool {
        self.last_write_error.is_none() && self.corrupt_lines == 0
    }
}
pub fn scan_for_settlement(
    path: &str,
) -> (
    std::collections::HashSet<String>,
    std::collections::HashMap<(String, String), (f64, f64, i64)>,
) {
    let mut keys = std::collections::HashSet::new();
    let mut releases = std::collections::HashMap::new();
    let Ok(raw) = std::fs::read_to_string(path) else { return (keys, releases) };
    for line in raw.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
            continue
        };
        for field in ["key", "alt_key"] {
            if let Some(key) = v[field].as_str() {
                keys.insert(key.to_string());
            }
        }
        if v["ev"].as_str() == Some("settle") {
            if let (Some(lane), Some(token)) = (v["lane"].as_str(), v["token"].as_str()) {
                keys.insert(crate::settlement::cross_writer_key(lane, token));
            }
        }
        if v["ev"].as_str() == Some("fill") && v["side"].as_u64() == Some(1)
            && v["recon"].as_bool() == Some(true)
        {
            let (Some(lane), Some(token)) = (v["lane"].as_str(), v["token"].as_str())
            else { continue };
            releases
                .insert(
                    (lane.to_string(), token.to_string()),
                    (
                        v["shares"].as_f64().unwrap_or(0.0),
                        v["price"].as_f64().unwrap_or(0.0),
                        v["t"].as_i64().unwrap_or(0),
                    ),
                );
        }
    }
    (keys, releases)
}
