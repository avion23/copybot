//! Single startup owner for timed CLOB transports and L2 credential verification.
use super::telemetry::{now_ms, Emitter};
use copybot_hot::config::Root;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct Transport {
    pub(crate) http: reqwest::Client,
    pub(crate) order_paths: Arc<Vec<(String, reqwest::Client)>>,
    pub(crate) clob: String,
    pub(crate) creds: Arc<Option<copybot_hot::auth::ApiCreds>>,
    pub(crate) auth_addr: String,
}

pub(crate) fn mk_client(h2: bool) -> Result<reqwest::Client, reqwest::Error> {
        let b = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(4)
            .timeout(Duration::from_secs(10));
        if h2 { b.build() } else { b.http1_only().build() }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn init(
    root: &Root,
    shadow: bool,
    pk: Option<[u8; 32]>,
    signer_addr: &String,
    creds_slot: &Arc<std::sync::Mutex<Option<copybot_hot::auth::ApiCreds>>>,
    live: bool,
    emitter: &Arc<Emitter>,
) -> Transport {
    let http = mk_client(false).expect("h1 client");
    let mut order_paths: Vec<(String, reqwest::Client)> = vec![
        ("h1".into(), http.clone())
    ];
    if root.bot.race_h2 {
        match mk_client(true) {
            Ok(c) => order_paths.push(("h2".into(), c)),
            Err(e) => eprintln!("[transport] h2 unavailable: {e}"),
        }
        match mk_client(false) {
            Ok(c) => order_paths.push(("h1b".into(), c)),
            Err(e) => eprintln!("[transport] third path unavailable: {e}"),
        }
    }
    eprintln!(
        "[transport] racing {} paths: {}", order_paths.len(), order_paths.iter().map(|
        (n, _) | n.as_str()).collect::< Vec < _ >> ().join(", ")
    );
    let order_paths = Arc::new(order_paths);
    let clob = root.bot.clob_host.clone();
    let creds: Arc<Option<copybot_hot::auth::ApiCreds>> = if shadow {
        eprintln!(
            "[auth] SHADOW — credentials deliberately NOT derived. Every order \
will be rejected 401 by the venue. This process cannot place an order."
        );
        Arc::new(None)
    } else {
        use copybot_hot::auth::derive_or_create_creds;
        match pk {
            Some(pk) => {
                match derive_or_create_creds(&http, &clob, &pk, &signer_addr, 0).await {
                    Ok(c) => {
                        eprintln!("[auth] L2 credentials derived — {}", c.redacted());
                        *creds_slot.lock().unwrap() = Some(c.clone());
                        Arc::new(Some(c))
                    }
                    Err(e) => {
                        eprintln!("[auth] FAILED to obtain L2 credentials: {e}");
                        Arc::new(None)
                    }
                }
            }
            None => {
                eprintln!(
                    "[auth] no PRIVATE_KEY in env — L2 auth UNAVAILABLE (paper mode only)"
                );
                Arc::new(None)
            }
        }
    };
    if let Some(cr) = creds.as_ref() {
        let path = "/data/orders";
        let ts = copybot_hot::auth::now_secs();
        match copybot_hot::auth::l2_headers(&signer_addr, cr, ts, "GET", path, None) {
            Ok(h) => {
                let mut rb = http.get(format!("{clob}{path}"));
                for (k, v) in &h {
                    rb = rb.header(*k, v);
                }
                match rb.send().await {
                    Ok(r) if r.status().is_success() => {
                        eprintln!(
                            "[auth] self-check OK — {path} accepted our L2 headers"
                        );
                        match r.json::<serde_json::Value>().await {
                            Ok(v) => {
                                let n = (v.as_array().map(|a| a.len()))
                                    .or_else(|| v["data"].as_array().map(|a| a.len()))
                                    .unwrap_or(0);
                                if n > 0 {
                                    eprintln!(
                                        "[resting] venue holds {n} live order(s); attribution deferred to the open-orders reconcile (~60s)"
                                    );
                                    emitter
                                        .emit(
                                            serde_json::json!(
                                                { "t" : now_ms(), "ev" : "resting_seen_at_boot", "orders" :
                                                n }
                                            ),
                                        );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "[resting] could not read the open-order list ({e}); a GTC order the venue still holds would be unmanaged"
                                )
                            }
                        }
                    }
                    Ok(r) => {
                        eprintln!(
                            "[auth] ⛔ SELF-CHECK FAILED: {path} -> {}. Credentials \
derived but are NOT accepted; every order would be refused. Refusing to continue.",
                            r.status()
                        );
                        if live && !shadow {
                            std::process::exit(2);
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[auth] ⛔ self-check could not run ({e}); auth is UNVERIFIED"
                        );
                        if live && !shadow {
                            std::process::exit(2);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[auth] ⛔ self-check header build failed: {e}");
                if live && !shadow {
                    std::process::exit(2);
                }
            }
        }
    }
    if live && !shadow && creds.is_none() {
        eprintln!(
            "[auth] ⛔ REFUSING TO ARM: mode=live but there are no L2 credentials.\n\
                   [auth]    Every /order POST would be rejected 401 by the venue.\n\
                   [auth]    Set PRIVATE_KEY, or run mode=paper."
        );
        std::process::exit(2);
    }
    let auth_addr = signer_addr.clone();
    Transport { http, order_paths, clob, creds, auth_addr }
}
