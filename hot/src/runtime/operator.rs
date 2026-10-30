//! Existing operator-file and wallet-registry mutations at the runtime boundary.
//! HTTP authorization is a separate migration step; these helpers preserve current behavior.

use super::http::header_of;
use std::io::Write;

pub(crate) fn handle_wallet_patch(
    reg: &copybot_hot::wallets::WalletRegistry,
    physical: Option<f64>,
    live_lanes: &[String],
    body: &str,
) -> (&'static str, String) {
    use copybot_hot::wallets::WalletSpec;
    let bad = |m: String| (
        "400 Bad Request",
        serde_json::json!({ "error" : m }).to_string(),
    );
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return bad(format!("bad json: {e}")),
    };
    let Some(obj) = v.as_object() else {
        return bad("body must be a JSON object".into())
    };
    const KNOWN: &[&str] = &[
        "name",
        "leader",
        "seed_usd",
        "pct",
        "enabled",
        "min_buy_price",
        "max_buy_price",
        "max_effective_pct",
        "compound",
        "copy_makers",
        "exclude_political",
        "leader_max_order_usd",
        "leader_peak_exposure_usd",
    ];
    let unknown: Vec<&str> = obj
        .keys()
        .map(|k| k.as_str())
        .filter(|k| !KNOWN.contains(k))
        .collect();
    if !unknown.is_empty() {
        return bad(
            format!("unknown field(s): {}. Nothing was changed.", unknown.join(", ")),
        );
    }
    let name = match v["name"].as_str() {
        Some(n) if !n.trim().is_empty() => n.trim().to_string(),
        _ => return bad("name required".into()),
    };
    let finite_pos = |x: f64| x.is_finite() && x > 0.0;
    let result = reg
        .transact(|cur| {
            let mut next = cur.to_vec();
            let existing = next.iter().position(|w| w.name == name);
            const BUILD_ONLY: &[&str] = &[
                "pct",
                "min_buy_price",
                "max_buy_price",
                "max_effective_pct",
                "compound",
                "copy_makers",
                "exclude_political",
            ];
            if existing.is_some() && live_lanes.iter().any(|l| l == &name) {
                let attempted: Vec<&str> = BUILD_ONLY
                    .iter()
                    .copied()
                    .filter(|k| obj.contains_key(*k))
                    .collect();
                if !attempted.is_empty() {
                    return Err(
                        format!(
                            "{} cannot change while {name} is loaded — the running lane keeps its \
built configuration, so accepting this would show one risk level and trade another. \
Disable {name}, then re-enable it, to rebuild the lane with the new settings.",
                            attempted.join(", ")
                        ),
                    );
                }
            }
            match existing {
                Some(i) => {
                    if obj.contains_key("leader") {
                        return Err(
                            format!(
                                "a lane's leader is its identity and cannot change — {name}'s \
positions were copied from its current leader and their exit signals come from that \
wallet. Retire {name} and create a new wallet for the new leader."
                            ),
                        );
                    }
                    if let Some(sd) = obj.get("seed_usd") {
                        let sd = sd.as_f64().unwrap_or(f64::NAN);
                        if !finite_pos(sd) {
                            return Err("seed_usd must be a finite number > 0".into());
                        }
                        next[i].seed_usd = sd;
                    }
                    if let Some(en) = obj.get("enabled") {
                        match en.as_bool() {
                            Some(b) => next[i].enabled = b,
                            None => return Err("enabled must be true or false".into()),
                        }
                    }
                }
                None => {
                    let leader = match v["leader"].as_str() {
                        Some(
                            l,
                        ) if l.starts_with("0x") && l.len() == 42
                            && l[2..].chars().all(|c| c.is_ascii_hexdigit()) => {
                            l.to_lowercase()
                        }
                        _ => {
                            return Err(
                                "a new wallet needs a 0x… leader address (42 hex chars)"
                                    .into(),
                            );
                        }
                    };
                    if let Some(clash) = next
                        .iter()
                        .find(|w| w.leader.eq_ignore_ascii_case(&leader))
                    {
                        return Err(
                            format!(
                                "leader {leader} is already copied by wallet {:?}. One leader may \
back only one wallet — two would copy every fill twice.",
                                clash.name
                            ),
                        );
                    }
                    let seed = v["seed_usd"].as_f64().unwrap_or(f64::NAN);
                    if !finite_pos(seed) {
                        return Err("seed_usd must be a finite number > 0".into());
                    }
                    let pct = v["pct"].as_f64().unwrap_or(f64::NAN);
                    if !(pct.is_finite() && pct > 0.0 && pct <= 1.0) {
                        return Err("pct must be in (0, 1]".into());
                    }
                    let bmin = v["min_buy_price"].as_f64().unwrap_or(0.02);
                    let bmax = v["max_buy_price"].as_f64().unwrap_or(0.95);
                    if !(bmin.is_finite() && bmax.is_finite() && bmin > 0.0 && bmax < 1.0
                        && bmin < bmax)
                    {
                        return Err(
                            "buy band must satisfy 0 < min_buy_price < max_buy_price < 1"
                                .into(),
                        );
                    }
                    let eff = v["max_effective_pct"].as_f64().unwrap_or(0.05);
                    if !(eff.is_finite() && eff > 0.0 && eff <= 1.0) {
                        return Err("max_effective_pct must be in (0, 1]".into());
                    }
                    let leader_max_order_usd = v["leader_max_order_usd"].as_f64();
                    let leader_peak_exposure_usd = v["leader_peak_exposure_usd"]
                        .as_f64();
                    if !leader_max_order_usd.is_some_and(finite_pos)
                        || !leader_peak_exposure_usd.is_some_and(finite_pos)
                    {
                        return Err(
                            format!(
                                "wallet {name}: leader_max_order_usd and leader_peak_exposure_usd are \
                         REQUIRED and must be > 0 — they decide whether this lane's caps \
                         truncate the leader's biggest orders. Measure them with \
                         `tools/leader_stats.py <address>`; do NOT copy another wallet's \
                         numbers."
                            ),
                        );
                    }
                    next.push(WalletSpec {
                        name: name.clone(),
                        leader,
                        seed_usd: seed,
                        pct,
                        leader_max_order_usd,
                        leader_peak_exposure_usd,
                        enabled: v["enabled"].as_bool().unwrap_or(true),
                        stop_mode: "none".into(),
                        min_buy_price: bmin,
                        max_buy_price: bmax,
                        max_effective_pct: eff,
                        compound: v["compound"].as_bool().unwrap_or(true),
                        lane_id: None,
                        copy_makers: v["copy_makers"].as_bool().unwrap_or(true),
                        copy_maker_sells: v["copy_maker_sells"].as_bool(),
                        exclude_political: v["exclude_political"]
                            .as_bool()
                            .unwrap_or(false),
                        risk: serde_json::from_value(v["risk"].clone()).ok().flatten(),
                        buy_slippage_c: None,
                        sell_slippage_c: None,
                        sell_floor_frac: None,
                        min_order_usd: None,
                        min_fill_floor: None,
                        sell_all_frac: None,
                    });
                }
            }
            let sum: f64 = next.iter().filter(|w| w.enabled).map(|w| w.seed_usd).sum();
            let before: f64 = cur.iter().filter(|w| w.enabled).map(|w| w.seed_usd).sum();
            let overcommit = if sum > before + 1e-6 {
                match physical {
                    Some(p) if sum > p + 1e-6 => {
                        Some(
                            format!(
                                "enabled budgets ${sum:.2} exceed the shared wallet ${p:.2} by \
${:.2} — ACCEPTED: a virtual budget is a spending cap, not a reservation. The venue \
refuses an order the wallet cannot fund; it does not lose money. Add funds if lanes \
start missing fills.",
                                sum - p
                            ),
                        )
                    }
                    Some(_) => None,
                    None => {
                        Some(
                            "wallet equity is not currently known, so this INCREASE could not be \
checked against a real balance — ACCEPTED unchecked."
                                .to_string(),
                        )
                    }
                }
            } else {
                None
            };
            if let Some(w) = &overcommit {
                eprintln!("[wallets] ⚠️  {w}");
            }
            Ok(next)
        });
    match result {
        Ok(()) => {
            let sum: f64 = reg
                .specs()
                .iter()
                .filter(|w| w.enabled)
                .map(|w| w.seed_usd)
                .sum();
            let warning = physical
                .filter(|p| sum > p + 1e-6)
                .map(|p| {
                    format!(
                        "enabled budgets ${sum:.2} exceed the wallet ${p:.2} \
by ${:.2} — accepted; the venue refuses what the wallet cannot fund",
                        sum - p
                    )
                });
            (
                "200 OK",
                serde_json::json!(
                    { "ok" : true, "name" : name, "sum_seed" : (sum * 100.0).round() /
                    100.0, "warning" : warning }
                )
                    .to_string(),
            )
        }
        Err(e) if e.starts_with("cannot persist") => {
            ("500 Internal Server Error", serde_json::json!({ "error" : e }).to_string())
        }
        Err(e) => bad(e),
    }
}
pub(crate) fn actor_of(req: &str) -> String {
    match header_of(req, "cf-access-authenticated-user-email") {
        Some(e) if !e.trim().is_empty() => e.trim().to_string(),
        _ => "unidentified (no Cf-Access identity on the request)".to_string(),
    }
}
pub(crate) fn handle_arm_post(path: &str, body: &str, actor: &str) -> (&'static str, String) {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                "400 Bad Request",
                serde_json::json!({ "error" : format!("bad json: {e}") }).to_string(),
            );
        }
    };
    let Some(lane) = v["lane"].as_str().filter(|s| !s.is_empty()) else {
        return (
            "400 Bad Request",
            serde_json::json!({ "error" : "lane required" }).to_string(),
        );
    };
    let state = v["state"].as_str().unwrap_or("");
    let (armed, halt) = match state {
        "armed" => {
            let want = format!("arm {lane}");
            if v["phrase"].as_str() != Some(want.as_str()) {
                return (
                    "400 Bad Request",
                    serde_json::json!(
                        { "error" : format!("arming requires phrase {want:?}") }
                    )
                        .to_string(),
                );
            }
            (true, false)
        }
        "halt_buys" => (true, true),
        "off" => (false, false),
        _ => {
            return (
                "400 Bad Request",
                serde_json::json!({ "error" : "state must be armed | halt_buys | off" })
                    .to_string(),
            );
        }
    };
    let _oplock = copybot_hot::errors::LockFile::acquire(path);
    let mut doc: serde_json::Value = match std::fs::read_to_string(path) {
        Ok(raw) if raw.trim().is_empty() => serde_json::json!({ "lanes" : {} }),
        Ok(raw) => {
            match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    return (
                        "409 Conflict",
                        serde_json::json!(
                            { "error" :
                            format!("the operator file is present but unreadable ({e}); refusing to replace \
it, because doing so would drop every other lane's arm state. Fix or move {path} by \
hand, then retry.")
                            }
                        )
                            .to_string(),
                    );
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            serde_json::json!({ "lanes" : {} })
        }
        Err(e) => {
            return (
                "500 Internal Server Error",
                serde_json::json!(
                    { "error" : format!("cannot read the operator file: {e}") }
                )
                    .to_string(),
            );
        }
    };
    if !doc["lanes"].is_object() {
        return (
            "409 Conflict",
            serde_json::json!(
                { "error" :
                "the operator file has no `lanes` object; refusing to overwrite it" }
            )
                .to_string(),
        );
    }
    doc["lanes"][lane] = serde_json::json!(
        { "armed" : armed, "halt_buys" : halt, "by" : actor, "at" :
        copybot_hot::ledger::now_secs(), "state" : state, }
    );
    doc["_meta"] = serde_json::json!(
        { "by" : "api:/api/arm", "at" : copybot_hot::ledger::now_secs(), "why" :
        format!("{actor} set {lane} to {state}"), "actor" : actor, "pid" :
        std::process::id(), }
    );
    let serialized = match serde_json::to_vec(&doc) {
        Ok(bytes) => bytes,
        Err(_serialization_error) => {
            return (
                "500 Internal Server Error",
                serde_json::json!({
                    "error": "operator state serialization failed; existing state unchanged",
                }).to_string(),
            );
        }
    };
    let tmp = format!(
        "{path}.api.{}.{}.tmp", std::process::id(), std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(| d | d.as_nanos()).unwrap_or(0)
    );
    let done = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&serialized)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        if let Some(dir) = std::path::Path::new(path).parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    })();
    match done {
        Ok(()) => {
            (
                "200 OK",
                serde_json::json!(
                    { "ok" : true, "lane" : lane, "state" : state, "armed" : armed,
                    "halt_buys" : halt }
                )
                    .to_string(),
            )
        }
        Err(e) => {
            (
                "500 Internal Server Error",
                serde_json::json!({ "error" : format!("write: {e}") }).to_string(),
            )
        }
    }
}
pub(crate) fn handle_flatten_post(
    path: &str,
    body: &str,
    armed_of: impl Fn(&str) -> Option<bool>,
) -> (&'static str, String) {
    let intent: copybot_hot::flatten::Intent = match serde_json::from_str(body) {
        Ok(i) => i,
        Err(e) => {
            return (
                "400 Bad Request",
                serde_json::json!({ "error" : format!("bad intent: {e}") }).to_string(),
            );
        }
    };
    if copybot_hot::flatten::Mode::parse(&intent.mode).is_none() {
        return (
            "400 Bad Request",
            serde_json::json!({ "error" : format!("unknown mode {:?}", intent.mode) })
                .to_string(),
        );
    }
    if intent.lane.is_empty() || intent.lane.len() > 32
        || !intent
            .lane
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return (
            "400 Bad Request",
            serde_json::json!({ "error" : "lane must be 1-32 chars of [A-Za-z0-9_-]" })
                .to_string(),
        );
    }
    match armed_of(&intent.lane) {
        None => {
            return (
                "404 Not Found",
                serde_json::json!(
                    { "error" : format!("unknown lane {:?} — nothing has been sold",
                    intent.lane) }
                )
                    .to_string(),
            );
        }
        Some(armed) => {
            if let Err(reason) = copybot_hot::flatten::gate(
                &intent,
                &intent.lane,
                armed,
                copybot_hot::ledger::now_secs(),
            ) {
                return (
                    "409 Conflict",
                    serde_json::json!(
                        { "error" : reason, "note" :
                        "NOT queued — nothing has been sold" }
                    )
                        .to_string(),
                );
            }
        }
    }
    let path = format!("{path}.{}", intent.lane);
    let tmp = format!(
        "{path}.tmp.{}.{}", std::process::id(), std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(| d | d.as_nanos()).unwrap_or(0)
    );
    let write = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    })();
    match write {
        Ok(()) => {
            (
                "200 OK",
                serde_json::json!(
                    { "ok" : true, "lane" : intent.lane, "mode" : intent.mode, "note" :
                    "queued — the runtime gates and executes it" }
                )
                    .to_string(),
            )
        }
        Err(e) => {
            (
                "500 Internal Server Error",
                serde_json::json!({ "error" : format!("write: {e}") }).to_string(),
            )
        }
    }
}
pub(crate) fn seed_operator_file(path: &std::path::Path, body: &[u8]) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| {
                format!("create operator directory {}: {e}", parent.display())
            })?;
    }
    let tmp = std::path::PathBuf::from(
        format!("{}.seed-{}.tmp", path.display(), std::process::id()),
    );
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(body)?;
        file.flush()?;
        file.sync_data()?;
        std::fs::rename(&tmp, path)?;
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(e) = result {
        let _cleanup = std::fs::remove_file(&tmp);
        return Err(format!("seed operator file {}: {e}", path.display()));
    }
    Ok(())
}
