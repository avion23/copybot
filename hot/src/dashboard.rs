//! Single loopback dashboard server; read paths and authenticated mutation adapters
//! preserve their existing route order and share one state context from the entrypoint.
use super::*;

pub(crate) struct Context<'a> {
    pub(crate) root: &'a Root,
    pub(crate) control: Arc<std::sync::Mutex<Control>>,
    pub(crate) router: Arc<Router>,
    pub(crate) books: Arc<BookCache>,
    pub(crate) operator_access: Arc<OperatorAccess>,
    pub(crate) emitter: Arc<Emitter>,
    pub(crate) pending_log: Arc<std::sync::Mutex<copybot_hot::pending::PendingLog>>,
    pub(crate) custody_report: Arc<std::sync::Mutex<Option<copybot_hot::custody::Report>>>,
    pub(crate) leader_pnl: Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>,
    pub(crate) reanchor_pulse: runtime::reconcile::ReanchorPulse,
    pub(crate) resting_book: Arc<copybot_hot::resting::RestingBook>,
    pub(crate) levels: Arc<copybot_hot::book::Levels>,
    pub(crate) prints: Arc<copybot_hot::book::TradePrints>,
    pub(crate) funding: Arc<std::sync::Mutex<copybot_hot::funding::Funding>>,
    pub(crate) incidents: Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    pub(crate) registry: copybot_hot::wallets::WalletRegistry,
    pub(crate) physical_wallet: Arc<std::sync::Mutex<PhysicalWallet>>,
    pub(crate) errors_path: String,
    pub(crate) drift_by_lane: Arc<std::sync::Mutex<std::collections::HashMap<String, f64>>>,
    pub(crate) unattributed: Arc<std::sync::Mutex<std::collections::HashMap<String, (String, f64, f64)>>>,
    pub(crate) response_times: Arc<copybot_hot::response_times::Ring>,
    pub(crate) funder: String,
    pub(crate) creds_slot: Arc<std::sync::Mutex<Option<copybot_hot::auth::ApiCreds>>>,
    pub(crate) signer_addr: String,
    pub(crate) sig_type: u8,
    pub(crate) signal_guard: Option<Arc<copybot_hot::signal_guard::SignalGuard>>,
}

pub(crate) fn spawn(ctx: Context<'_>) {
    let Context {
        root,
        control,
        router,
        books,
        operator_access,
        emitter,
        pending_log,
        custody_report,
        leader_pnl,
        reanchor_pulse,
        resting_book,
        levels,
        prints,
        funding,
        incidents,
        registry,
        physical_wallet,
        errors_path,
        drift_by_lane,
        unattributed,
        response_times,
        funder,
        creds_slot,
        signer_addr,
        sig_type,
        signal_guard,
    } = ctx;
    {
        let port: u16 = std::env::var("DASHBOARD_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8806);
        let (c, r, bk2) = (control.clone(), router.clone(), books.clone());
        let operator_access_d = operator_access.clone();
        let operator_events_d = emitter.clone();
        let pend_dash = pending_log.clone();
        let custody_dash = custody_report.clone();
        let leader_pnl_dash = leader_pnl.clone();
        let reanchor_dash = reanchor_pulse.clone();
        let (rb_dash, levels_dash, prints_dash) = (
            resting_book.clone(),
            levels.clone(),
            prints.clone(),
        );
        let fund_pool = funding.clone();
        let inc_dash = incidents.clone();
        let (registry_d, phys_d, ctrl_path_d) = (
            registry.clone(),
            physical_wallet.clone(),
            root.bot.control_path.clone(),
        );
        let pool2_path_d = std::env::var("POOL_PAGE")
            .or_else(|_| std::env::var("POOL2_PATH"))
            .unwrap_or_else(|_| {
                let dir = std::path::Path::new(&root.bot.control_path)
                    .parent()
                    .map(|d| d.to_string_lossy().into_owned())
                    .unwrap_or_else(|| ".".into());
                format!("{dir}/pool.html")
            });
        eprintln!(
            "[dash] /pool served from {pool2_path_d} (edit + refresh, no restart)"
        );
        let errors_path_d = errors_path.clone();
        let drift_dash = drift_by_lane.clone();
        let unattributed_dash = unattributed.clone();
        let response_times_dash = response_times.clone();
        let dash_lanes: Vec<DashboardLane> = router
            .snapshot()
            .iter()
            .map(|lane| {
                let seed = root
                    .lane
                    .iter()
                    .find(|x| x.name == lane.cfg.name)
                    .and_then(|x| x.budget.bankroll_usd);
                (
                    lane.cfg.name.clone(),
                    format!("0x{}", hex::encode(lane.cfg.wallet20)),
                    seed,
                    lane.policy().caps.daily_usd,
                    Arc::new(copybot_hot::matchup::Matchup::new()),
                )
            })
            .collect();
        let http_mu = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_default();
        let funder_mu = funder.clone();
        let pos_cache: Arc<
            tokio::sync::Mutex<Option<copybot_hot::positions::Positions>>,
        > = Arc::new(tokio::sync::Mutex::new(None));
        let title_cache: Arc<
            std::sync::RwLock<std::collections::HashMap<String, String>>,
        > = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        runtime::dashboard_trades::spawn_title_cache_refresh(&http_mu, &funder_mu, &title_cache);
        let (creds_dash, clob_dash, addr_dash, sig_dash) = (
            creds_slot.clone(),
            root.bot.clob_host.clone(),
            signer_addr.clone(),
            sig_type,
        );
        let signal_guard_dash = signal_guard.clone();
        tokio::spawn(async move {
            let lst = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[dash] bind: {e}");
                    return;
                }
            };
            eprintln!(
                "[dash] http://127.0.0.1:{port} (loopback; mutations {})",
                if operator_access_d.enabled() { "require operator capability" } else { "disabled" },
            );
            const MAX_DASH_CONNS: usize = 32;
            const REQUEST_DEADLINE: Duration = Duration::from_secs(15);
            let conn_limit = std::sync::Arc::new(
                tokio::sync::Semaphore::new(MAX_DASH_CONNS),
            );
            loop {
                let Ok((mut sock, _)) = lst.accept().await else { continue };
                let Ok(permit) = conn_limit.clone().try_acquire_owned() else {
                    use tokio::io::AsyncWriteExt;
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
                        )
                        .await;
                    continue;
                };
                let (c, r, bk2) = (c.clone(), r.clone(), bk2.clone());
                let (http2, funder2, lane_specs) = (
                    http_mu.clone(),
                    funder_mu.clone(),
                    dash_lanes.clone(),
                );
                let pos_cache2 = pos_cache.clone();
                let title_cache2 = title_cache.clone();
                let (creds_d, clob_d, addr_d, sigtype_d) = (
                    creds_dash.clone(),
                    clob_dash.clone(),
                    addr_dash.clone(),
                    sig_dash,
                );
                let signal_guard_d = signal_guard_dash.clone();
                let pend_d = pend_dash.clone();
                let custody_d = custody_dash.clone();
                let leader_pnl_d = leader_pnl_dash.clone();
                let reanchor_d = reanchor_dash.clone();
                let (rb_d, levels_d, prints_d) = (
                    rb_dash.clone(),
                    levels_dash.clone(),
                    prints_dash.clone(),
                );
                let inc_srv = inc_dash.clone();
                let (registry_c, phys_c, ctrl_path_c) = (
                    registry_d.clone(),
                    phys_d.clone(),
                    ctrl_path_d.clone(),
                );
                let ctl_http = c.clone();
                let pool2_path_c = pool2_path_d.clone();
                let errors_path_c = errors_path_d.clone();
                let drift_d = drift_dash.clone();
                let unattributed_d = unattributed_dash.clone();
                let response_times_c = response_times_dash.clone();
                let fund_pool2 = fund_pool.clone();
                let operator_access_c = operator_access_d.clone();
                let operator_events_c = operator_events_d.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let _permit = permit;
                    let served = tokio::time::timeout(
                            REQUEST_DEADLINE,
                            async move {
                                let mut raw: Vec<u8> = Vec::with_capacity(4096);
                                let mut tmp = [0u8; 4096];
                                loop {
                                    match sock.read(&mut tmp).await {
                                        Ok(0) => break,
                                        Ok(n) => raw.extend_from_slice(&tmp[..n]),
                                        Err(_) => break,
                                    }
                                    let s = String::from_utf8_lossy(&raw);
                                    if let Some(idx) = s.find("\r\n\r\n") {
                                        let want = s[..idx]
                                            .lines()
                                            .find_map(|l| {
                                                l.to_ascii_lowercase()
                                                    .strip_prefix("content-length:")
                                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                                            })
                                            .unwrap_or(0);
                                        if raw.len() - (idx + 4) >= want {
                                            break;
                                        }
                                    }
                                    if raw.len() > 65_536 {
                                        break;
                                    }
                                }
                                let req = String::from_utf8_lossy(&raw).into_owned();
                                let body: &str = req
                                    .find("\r\n\r\n")
                                    .map(|i| &req[i + 4..])
                                    .unwrap_or("");
                                let mut parts = req.split_whitespace();
                                let method = parts.next().unwrap_or("");
                                let target = parts.next().unwrap_or("");
                                let path = target.split('?').next().unwrap_or("");
                                let query = target
                                    .split_once('?')
                                    .map(|(_, q)| q)
                                    .unwrap_or("");
                                let _ = query;
                                if method != "GET" {
                                    let action = OperatorAction::from_route(method, path);
                                    let authorization = match action {
                                        Some(action) => operator_access_c.authorize(action, &req),
                                        None => {
                                            let resp = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    };
                                    let authorized = match authorization {
                                        Ok(authorized) => authorized,
                                        Err(error) => {
                                            let (code, message) = error.response();
                                            let out = serde_json::json!({ "error": message }).to_string();
                                            let resp = format!(
                                                "HTTP/1.1 {code}\r\nContent-Type: application/json\r\n\
                                                 WWW-Authenticate: Bearer\r\nCache-Control: no-store\r\n\
                                                 Content-Length: {}\r\n\r\n{}", out.len(), out,
                                            );
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    };
                                    operator_events_c.emit(serde_json::json!({
                                        "t": now_ms(), "ev": "operator_authorized",
                                        "actor": "operator-capability", "action": authorized.action().label(),
                                    }));
                                }
                                if method == "GET"
                                    && (path == "/" || path == "/index"
                                        || path == "/index.html")
                                {
                                    let resp = "HTTP/1.1 302 Found\r\nLocation: /pool\r\n\
                            Content-Length: 0\r\n\r\n";
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && (path == "/pool" || path == "/pool2")
                                {
                                    let (code, body) = match std::fs::read_to_string(
                                        &pool2_path_c,
                                    ) {
                                        Ok(html) => ("200 OK", html),
                                        Err(e) => {
                                            (
                                                "404 Not Found",
                                                format!(
                                                    "<pre style=\"font:14px ui-monospace;padding:24px;\
                                 background:#0b0b0e;color:#f2f2f5\">\
                                 /pool2 is served from a file that is not there yet.\n\n\
                                 expected: {pool2_path_c}\n error:    {e}\n\n\
                                 Drop the page there and refresh — no restart needed.</pre>"
                                                ),
                                            )
                                        }
                                    };
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: text/html; \
                            charset=utf-8\r\nCache-Control: no-store\r\n\
                            Content-Length: {}\r\n\r\n{}",
                                        body.len(), body
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/trades" {
                                    runtime::dashboard_trades::serve_trades(
                                        &mut sock, &req, &c, &title_cache2,
                                    ).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/pool" {
                                    runtime::dashboard_pool::serve_pool(
                                        &mut sock,
                                        runtime::dashboard_pool::PoolView {
                                            registry: &registry_c, physical_wallet: &phys_c,
                                            router: &r, control: &c, reanchor: &reanchor_d,
                                            leader_pnl: &leader_pnl_d, drift: &drift_d,
                                            funding: &fund_pool2, unattributed: &unattributed_d,
                                        },
                                    ).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/equity" {
                                    let path = req.split_whitespace().nth(1).unwrap_or("");
                                    let sel = path
                                        .split_once('?')
                                        .and_then(|(_, q)| {
                                            q.split('&').find_map(|p| p.strip_prefix("lane="))
                                        })
                                        .unwrap_or("all")
                                        .to_string();
                                    let lane_opt = if sel == "all" {
                                        None
                                    } else {
                                        Some(sel.as_str())
                                    };
                                    let lpath = c.lock().unwrap().ledger.path.clone();
                                    if let Err(e) = std::fs::metadata(&lpath) {
                                        if e.kind() != std::io::ErrorKind::NotFound {
                                            let out = serde_json::json!(
                                                { "status" : "stale", "stale_reason" :
                                                format!("ledger unreadable: {e}"), "lane" : sel, "points" :
                                                serde_json::Value::Null }
                                            )
                                                .to_string();
                                            let resp = format!(
                                                "HTTP/1.1 503 Service Unavailable\r\n\
                                    Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                                out.len(), out
                                            );
                                            let _ = sock.write_all(resp.as_bytes()).await;
                                            return;
                                        }
                                    }
                                    let series = copybot_hot::ledger::equity_series_at(
                                        &lpath,
                                        lane_opt,
                                    );
                                    let pts: Vec<serde_json::Value> = series
                                        .iter()
                                        .map(|(t, v)| {
                                            serde_json::json!({ "time" : t, "value" : v })
                                        })
                                        .collect();
                                    let out = serde_json::to_string(
                                            &serde_json::json!({ "lane" : sel, "points" : pts }),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/pnl" {
                                    let now = copybot_hot::ledger::now_secs();
                                    let snap = r.snapshot();
                                    let win = |w: copybot_hot::ledger::RealisedWindows| {
                                        serde_json::json!(
                                            { "total" : w.total, "d1" : w.d1, "d7" : w.d7, "d30" : w
                                            .d30, "ytd" : w.ytd, "y1" : w.y1 }
                                        )
                                    };
                                    let lpath = c.lock().unwrap().ledger.path.clone();
                                    let all = win(
                                        copybot_hot::ledger::Ledger::realised_windows_at(
                                            &lpath,
                                            None,
                                            now,
                                        ),
                                    );
                                    let lanes_out: Vec<serde_json::Value> = snap
                                        .iter()
                                        .map(|lane| {
                                            let mut o = win(
                                                copybot_hot::ledger::Ledger::realised_windows_at(
                                                    &lpath,
                                                    Some(&lane.cfg.name),
                                                    now,
                                                ),
                                            );
                                            o["name"] = serde_json::json!(lane.cfg.name);
                                            o
                                        })
                                        .collect();
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "now" : now, "all" : all, "lanes" : lanes_out }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/positions" {
                                    runtime::dashboard_positions::serve_positions(
                                        &mut sock, &http2, &funder2, &pos_cache2, &title_cache2,
                                        &r, &c,
                                    ).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/response_times" {
                                    let samples = response_times_c.tail(50);
                                    let out = serde_json::to_string(
                                            &serde_json::json!({ "samples" : samples }),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/errors" {
                                    let rows = copybot_hot::errors::tail(&errors_path_c, 200);
                                    let now_s = copybot_hot::ledger::now_secs();
                                    let cutoff = now_s
                                        - copybot_hot::errors::ACTIVE_WINDOW_SECS;
                                    let counts = copybot_hot::errors::counts_by_lane(&rows);
                                    let active = copybot_hot::errors::counts_by_lane_since(
                                        &rows,
                                        cutoff,
                                    );
                                    let out = serde_json::to_string(
                                            &serde_json::json!(
                                                { "errors" : rows, "active_window_secs" :
                                                copybot_hot::errors::ACTIVE_WINDOW_SECS, "active_by_lane" :
                                                active.iter().map(| (l, s, w, n) | serde_json::json!({
                                                "lane" : l, "stops" : s, "warns" : w, "notices" : n }))
                                                .collect::< Vec < _ >> (), "by_lane" : counts.iter().map(|
                                                (l, s, w, n) | serde_json::json!({ "lane" : l, "stops" : s,
                                                "warns" : w, "notices" : n })).collect::< Vec < _ >> (), }
                                            ),
                                        )
                                        .unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/wallets" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let live_names: Vec<String> = r
                                        .snapshot()
                                        .iter()
                                        .map(|l| l.cfg.name.clone())
                                        .collect();
                                    let (code, out) = handle_wallet_patch(
                                        &registry_c,
                                        phys_c.lock().unwrap().equity,
                                        &live_names,
                                        body,
                                    );
                                    if code.starts_with("200") {
                                        ctl_http.lock().unwrap().ledger.lane_ids = registry_c
                                            .id_map();
                                    }
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/incident/clear" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let v: serde_json::Value = serde_json::from_str(body)
                                        .unwrap_or(serde_json::Value::Null);
                                    let lane = v["lane"].as_str().unwrap_or("");
                                    let want = format!("clear {lane}");
                                    let (code, out) = if lane.is_empty() {
                                        (
                                            "400 Bad Request",
                                            serde_json::json!({ "error" : "lane required" }).to_string(),
                                        )
                                    } else if v["phrase"].as_str() != Some(want.as_str()) {
                                        (
                                            "400 Bad Request",
                                            serde_json::json!(
                                                { "error" : format!("clearing requires phrase {want:?}") }
                                            )
                                                .to_string(),
                                        )
                                    } else {
                                        let by = v["by"]
                                            .as_str()
                                            .unwrap_or("api:/api/incident/clear");
                                        let res: Result<bool, String> = inc_srv
                                            .lock()
                                            .map_err(|_| "journal lock poisoned".to_string())
                                            .and_then(|mut g| {
                                                g.clear(lane, by, copybot_hot::ledger::now_secs())
                                            });
                                        match res {
                                            Ok(was_open) => {
                                                let risk_cleared = c
                                                    .lock()
                                                    .ok()
                                                    .map(|mut g| {
                                                        g
                                                            .ledger
                                                            .clear_risk(
                                                                lane,
                                                                "operator cleared via /api/incident/clear",
                                                            )
                                                    })
                                                    .unwrap_or(false);
                                                if let Some(l) = r
                                                    .snapshot()
                                                    .iter()
                                                    .find(|l| l.cfg.name == lane)
                                                {
                                                    l.state.halt_latch.store(false, Ordering::Relaxed);
                                                }
                                                (
                                                    "200 OK",
                                                    serde_json::json!(
                                                        { "ok" : true, "lane" : lane, "was_open" : was_open,
                                                        "risk_breaker_cleared" : risk_cleared, "note" :
                                                        "buys resume at the next control tick" }
                                                    )
                                                        .to_string(),
                                                )
                                            }
                                            Err(e) => {
                                                (
                                                    "500 Internal Server Error",
                                                    serde_json::json!({ "error" : e }).to_string(),
                                                )
                                            }
                                        }
                                    };
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/arm" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let (code, out) = handle_arm_post(
                                        &format!("{ctrl_path_c}.operator"),
                                        body,
                                        &actor_of(&req),
                                    );
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "POST" && path == "/api/flatten" {
                                    if let Some((code, msg)) = mutation_refusal(&req) {
                                        let out = serde_json::json!({ "error" : msg }).to_string();
                                        let resp = format!(
                                            "HTTP/1.1 {code}\r\nContent-Type: \
                                application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    }
                                    let fpath = format!("{ctrl_path_c}.flatten");
                                    let (code, out) = handle_flatten_post(
                                        &fpath,
                                        body,
                                        |name| {
                                            r.snapshot()
                                                .iter()
                                                .find(|l| l.cfg.name == name)
                                                .map(|l| l.state.armed.load(Ordering::Relaxed))
                                        },
                                    );
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if method == "GET" && path == "/api/matchup" {
                                    let path = req.split_whitespace().nth(1).unwrap_or("");
                                    let requested = path
                                        .split_once('?')
                                        .and_then(|(_, q)| {
                                            q.split('&').find_map(|part| part.strip_prefix("lane="))
                                        });
                                    let resolved = lane_specs
                                        .iter()
                                        .find(|x| requested.map(|v| v == x.0).unwrap_or(false))
                                        .cloned()
                                        .or_else(|| {
                                            let want = requested?;
                                            let lane = r
                                                .snapshot()
                                                .into_iter()
                                                .find(|l| l.cfg.name == want)?;
                                            Some((
                                                lane.cfg.name.clone(),
                                                format!("0x{}", hex::encode(lane.cfg.wallet20)),
                                                registry_c.seed_of(&lane.cfg.name),
                                                lane.policy().caps.daily_usd,
                                                std::sync::Arc::new(copybot_hot::matchup::Matchup::new()),
                                            ))
                                        })
                                        .or_else(|| {
                                            if requested.is_none() {
                                                lane_specs.first().cloned()
                                            } else {
                                                None
                                            }
                                        });
                                    let Some(
                                        (lane_name, lane_wallet, seed, lane_budget, mu2),
                                    ) = resolved else {
                                        let out = serde_json::json!(
                                            { "error" : "no such lane", "lane" : requested, }
                                        )
                                            .to_string();
                                        let resp = format!(
                                            "HTTP/1.1 404 Not Found\r\nContent-Type: \
                                    application/json\r\nContent-Length: {}\r\n\r\n{}",
                                            out.len(), out
                                        );
                                        let _ = sock.write_all(resp.as_bytes()).await;
                                        return;
                                    };
                                    let ledger_snapshot = c
                                        .lock()
                                        .ok()
                                        .and_then(|g| {
                                            g.ledger
                                                .lanes
                                                .get(&lane_name)
                                                .map(|b| (
                                                    b.risk.realised_pnl,
                                                    g.ledger.open_usd(&lane_name),
                                                ))
                                        });
                                    let led = ledger_snapshot.map(|x| x.0);
                                    let ledger_open = ledger_snapshot.map(|x| x.1);
                                    let lane_epoch = c
                                        .lock()
                                        .ok()
                                        .and_then(|g| g.ledger.lane_epoch(&lane_name));
                                    let mut body = mu2
                                        .get(&http2, &funder2, &lane_wallet, led, lane_epoch)
                                        .await;
                                    if let Some(open) = ledger_open {
                                        let chain_deployed = body["deployed"].as_f64();
                                        body["chain_deployed"] = serde_json::json!(chain_deployed);
                                        body["deployed"] = serde_json::json!(
                                            (open * 100.0).round() / 100.0
                                        );
                                        body["cost_basis_delta"] = serde_json::json!(
                                            chain_deployed.map(| x | ((open - x) * 100.0).round() /
                                            100.0)
                                        );
                                        body["deployed_source"] = serde_json::json!("lane ledger");
                                    }
                                    let have = creds_d.lock().unwrap().clone();
                                    let cash = match have {
                                        Some(cr) => {
                                            copybot_hot::matchup::free_cash(
                                                    &http2,
                                                    &clob_d,
                                                    &addr_d,
                                                    &cr,
                                                    sigtype_d,
                                                )
                                                .await
                                        }
                                        None => None,
                                    };
                                    let st = c.lock().unwrap().status(&r);
                                    let spent = st["lanes"][&lane_name]["spent_today"]
                                        .as_f64()
                                        .unwrap_or(0.0);
                                    let armed = st["lanes"][&lane_name]["armed"]
                                        .as_bool()
                                        .unwrap_or(false);
                                    let cap_scale = st["lanes"][&lane_name]["cap_scale"]
                                        .as_f64()
                                        .unwrap_or(1.0);
                                    let realised = led.unwrap_or(0.0);
                                    let virtual_bankroll = seed
                                        .map(|s| copybot_hot::budget::virtual_bankroll(
                                            s,
                                            realised,
                                        ));
                                    let portfolio = cash
                                        .map(|c| {
                                            let v = body["wallet_value"].as_f64().unwrap_or(0.0);
                                            ((c + v) * 100.0).round() / 100.0
                                        });
                                    let funding_basis: f64 = registry_c
                                        .specs()
                                        .iter()
                                        .filter(|w| w.enabled)
                                        .map(|w| w.seed_usd)
                                        .sum();
                                    let equity_pnl = wallet_equity_pnl(
                                        portfolio,
                                        funding_basis,
                                    );
                                    body["wallet"] = serde_json::json!(
                                        { "portfolio" : portfolio, "funding_basis" : (funding_basis
                                        * 100.0).round() / 100.0, "equity_pnl" : equity_pnl,
                                        "equity_pnl_source" :
                                        "wallet mark-to-market equity minus enabled virtual seeds; not adjusted for later deposits or withdrawals",
                                        "cash" : cash, "deployed" : body["deployed"],
                                        "chain_deployed" : body["chain_deployed"],
                                        "cost_basis_delta" : body["cost_basis_delta"],
                                        "deployed_source" : body["deployed_source"], "value" :
                                        body["value"], "shared_position_value" :
                                        body["wallet_value"], "shared_position_count" :
                                        body["wallet_positions"], "spent_today" : (spent * 100.0)
                                        .round() / 100.0, "daily_budget" : lane_budget,
                                        "seed_bankroll" : seed, "virtual_bankroll" :
                                        virtual_bankroll, "sizing_bankroll" : seed.map(| s | s *
                                        cap_scale), "cap_scale" : cap_scale, "realised_pnl" :
                                        realised, "physical_shared" : true, }
                                    );
                                    body["selected_lane"] = serde_json::json!(lane_name);
                                    body["strategies"] = serde_json::json!(
                                        lane_specs.iter().map(| x | & x.0).collect::< Vec < _ >> ()
                                    );
                                    body["runtime"] = serde_json::json!(
                                        { "port" : port, "pid" : std::process::id(), "authoritative"
                                        : armed, "warning" : if armed { serde_json::Value::Null }
                                        else {
                                        serde_json::json!("DISARMED SLOT: figures are not the live authority")
                                        } }
                                    );
                                    let out = serde_json::to_string(&body).unwrap_or_default();
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                if let Some((code, msg)) = route_refusal(method, path) {
                                    let out = serde_json::json!(
                                        { "error" : msg, "path" : path, "method" : method }
                                    )
                                        .to_string();
                                    let resp = format!(
                                        "HTTP/1.1 {code}\r\nContent-Type: \
                            application/json\r\nContent-Length: {}\r\n\r\n{}",
                                        out.len(), out
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                    return;
                                }
                                let mut body = c.lock().unwrap().status(&r);
                                for (name, _, seed, _, _) in &lane_specs {
                                    let realised = body["lanes"][name]["risk"]["realised_pnl"]
                                        .as_f64()
                                        .unwrap_or(0.0);
                                    body["lanes"][name]["seed_bankroll"] = serde_json::json!(
                                        seed
                                    );
                                    body["lanes"][name]["virtual_bankroll"] = serde_json::json!(
                                        seed.map(| s | copybot_hot::budget::virtual_bankroll(s,
                                        realised))
                                    );
                                }
                                body["books_cached"] = serde_json::json!(bk2.len());
                                body["resting"] = {
                                    let now = copybot_hot::ledger::now_secs();
                                    let rows: Vec<serde_json::Value> = rb_d
                                        .all()
                                        .into_iter()
                                        .map(|o| {
                                            let v = rest_verdict(&levels_d, &prints_d, &o);
                                            serde_json::json!(
                                                { "lane" : o.lane, "side" : if o.side == 0 { "BUY" } else {
                                                "SELL" }, "tok" : & o.token[..o.token.len().min(14)],
                                                "limit" : o.limit, "shares" : o.shares, "age_secs" : now - o
                                                .placed, "verdict" : format!("{v:?}"), "max_age_secs" :
                                                copybot_hot::restwatch::max_age_for(o.side), "anchored" : o
                                                .his_remaining > 0.0, }
                                            )
                                        })
                                        .collect();
                                    serde_json::json!({ "n" : rows.len(), "orders" : rows })
                                };
                                body["custody"] = match custody_d.lock().unwrap().as_ref() {
                                    Some(r) => {
                                        serde_json::json!(
                                            { "checked" : r.checked, "agreed" : r.agreed, "explained" :
                                            r.explained, "unattributed" : r.unattributed, "unknown" : r
                                            .unknown, "complete" : r.complete, "unattributed_shares" :
                                            (r.unattributed_shares * 10_000.0).round() / 10_000.0,
                                            "alarm" : r.is_alarm(), "rows" : r.rows, "msg" :
                                            copybot_hot::custody::describe(r), }
                                        )
                                    }
                                    None => serde_json::Value::Null,
                                };
                                body["pending_persistence"] = {
                                    let l = pend_d.lock().unwrap();
                                    serde_json::json!(
                                        { "ok" : l.persistence_ok(), "write_failures" : l
                                        .write_failures, "open_orders" : l.len(), "last_error" : l
                                        .last_write_error }
                                    )
                                };
                                body["signal_guard"] = match signal_guard_d {
                                    Some(guard) => {
                                        match guard.active_markets() {
                                            Ok(active) => {
                                                serde_json::json!(
                                                    { "enabled" : true, "healthy" : true, "active_markets" :
                                                    active, "market_ttl_seconds" :
                                                    copybot_hot::signal_guard::MARKET_TTL_SECS, }
                                                )
                                            }
                                            Err(e) => {
                                                serde_json::json!(
                                                    { "enabled" : true, "healthy" : false, "error" :
                                                    format!("{e:?}"), }
                                                )
                                            }
                                        }
                                    }
                                    None => {
                                        serde_json::json!({ "enabled" : false, "healthy" : true })
                                    }
                                };
                                let out = serde_json::to_string_pretty(&body)
                                    .unwrap_or_default();
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                        Content-Length: {}\r\n\r\n{}",
                                    out.len(), out
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            },
                        )
                        .await;
                    if served.is_err() {
                        eprintln!(
                            "[dash] dropped a connection that stalled past the \
{REQUEST_DEADLINE:?} request deadline"
                        );
                    }
                });
            }
        });
    }
}
