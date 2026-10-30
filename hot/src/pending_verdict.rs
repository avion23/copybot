//! Pure pending-order and trade-list verdict parsing; no journal writes or network I/O.

use super::Verdict;

fn num(v: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|k| {
            v[*k].as_f64().or_else(|| v[*k].as_str().and_then(|s| s.parse::<f64>().ok()))
        })
}
pub fn parse_order_status(body: &str, side: u8) -> Verdict {
    parse_order_status_kind(body, side, false)
}
pub fn parse_order_status_kind(body: &str, side: u8, resting: bool) -> Verdict {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return Verdict::Unknown
    };
    if !v.is_object() {
        return Verdict::Unknown;
    }
    if v["error"].is_string()
        || v["errorMsg"].as_str().filter(|s| !s.is_empty()).is_some()
    {
        return Verdict::Unknown;
    }
    let status = v["status"].as_str().unwrap_or("").to_ascii_lowercase();
    let share_key = if side == 0 { "takingAmount" } else { "makingAmount" };
    let other_key = if side == 0 { "makingAmount" } else { "takingAmount" };
    let matched = num(&v, &["size_matched", "sizeMatched", "filled_size", "filledSize"])
        .or_else(|| {
            if status == "matched" || status == "filled" {
                num(&v, &[share_key])
            } else {
                None
            }
        })
        .unwrap_or(0.0);
    if resting && matched > 0.0 && (status == "live" || status == "delayed") {
        return Verdict::Unknown;
    }
    if matched > 0.0 {
        let full_fill = (status == "matched" || status == "filled")
            && num(&v, &[share_key])
                .map(|s| (s - matched).abs() <= s.abs() * 1e-6 + 1e-9)
                .unwrap_or(false);
        let price = if full_fill {
            num(&v, &[other_key])
                .filter(|usd| *usd > 0.0)
                .map(|usd| usd / matched)
                .unwrap_or(0.0)
        } else {
            0.0
        };
        return Verdict::Filled {
            shares: matched,
            price,
        };
    }
    match status.as_str() {
        "cancelled" | "canceled" | "unmatched" | "expired" | "rejected" => {
            Verdict::NotFilled
        }
        "live" | "delayed" if !resting => Verdict::NotFilled,
        "live" | "delayed" => Verdict::Unknown,
        _ => Verdict::Unknown,
    }
}
pub fn parse_trades(body: &str, order_hash: &str, side: u8, resting: bool) -> Verdict {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return Verdict::Unknown
    };
    let (rows, complete): (&Vec<serde_json::Value>, bool) = match &v {
        serde_json::Value::Array(a) => (a, true),
        serde_json::Value::Object(o) => {
            let Some(a) = o
                .get("data")
                .or_else(|| o.get("history"))
                .or_else(|| o.get("trades"))
                .and_then(|x| x.as_array()) else { return Verdict::Unknown };
            let more = match o.get("next_cursor") {
                None => false,
                Some(serde_json::Value::Null) => false,
                Some(serde_json::Value::String(c)) => {
                    !(c.is_empty() || c == "LTE=" || c == "END")
                }
                Some(_) => true,
            };
            (a, !more)
        }
        _ => return Verdict::Unknown,
    };
    let want = order_hash.trim_start_matches("0x").to_ascii_lowercase();
    if want.is_empty() {
        return Verdict::Unknown;
    }
    let mut shares = 0.0;
    let mut usd = 0.0;
    let mut hit = false;
    for row in rows {
        let ident = |v: &serde_json::Value, keys: &[&str]| -> bool {
            keys.iter()
                .any(|k| {
                    v[*k]
                        .as_str()
                        .map(|x| x.trim_start_matches("0x").eq_ignore_ascii_case(&want))
                        .unwrap_or(false)
                })
        };
        let we_are_taker = ident(
            row,
            &[
                "taker_order_id",
                "takerOrderId",
                "order_id",
                "orderId",
                "id",
                "order_hash",
            ],
        );
        let maker_entry = row["maker_orders"]
            .as_array()
            .or_else(|| row["makerOrders"].as_array())
            .and_then(|ms| {
                ms
                    .iter()
                    .find(|m| ident(m, &["order_id", "orderId", "id", "order_hash"]))
            });
        if !we_are_taker && maker_entry.is_none() {
            continue;
        }
        let status = row["status"].as_str().unwrap_or("").to_ascii_uppercase();
        if status == "FAILED" || status == "ORDER_STATUS_FAILED" {
            continue;
        }
        hit = true;
        let src = maker_entry.filter(|_| !we_are_taker);
        let sz = src
            .and_then(|m| num(m, &["size", "matched_amount", "matchedAmount"]))
            .or_else(|| num(row, &["size", "matched_amount", "matchedAmount"]))
            .unwrap_or(0.0);
        let px = src
            .and_then(|m| num(m, &["price"]))
            .or_else(|| num(row, &["price"]))
            .unwrap_or(0.0);
        if sz > 0.0 {
            shares += sz;
            usd += sz * px;
        }
    }
    if !hit {
        return if complete && !resting { Verdict::NotFilled } else { Verdict::Unknown };
    }
    if shares <= 0.0 {
        return Verdict::Unknown;
    }
    let _ = side;
    Verdict::Filled {
        shares,
        price: if usd > 0.0 { usd / shares } else { 0.0 },
    }
}
pub fn knows_shares_but_not_price(v: &Verdict) -> bool {
    matches!(v, Verdict::Filled { shares, price } if * shares > 0.0 && * price <= 0.0)
}
pub fn merge_price_from_trades(status: Verdict, trades: Verdict) -> Verdict {
    match (status, trades) {
        (
            Verdict::Filled { shares, price },
            Verdict::Filled { price: tp, .. },
        ) if price <= 0.0 && tp > 0.0 => {
            Verdict::Filled {
                shares,
                price: tp,
            }
        }
        (s, _) => s,
    }
}
pub fn recovered_price(resting: bool, venue_price: f64, limit: f64) -> (f64, bool) {
    if venue_price > 0.0 {
        (venue_price, false)
    } else if resting {
        (limit, false)
    } else {
        (limit, true)
    }
}
