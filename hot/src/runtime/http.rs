//! Existing HTTP route and request-header rules, separate from operator mutations.

pub(crate) const ROUTES: &[(&str, &str)] = &[
    ("GET", "/"),
    ("GET", "/index"),
    ("GET", "/index.html"),
    ("GET", "/pool"),
    ("GET", "/pool2"),
    ("GET", "/api/trades"),
    ("GET", "/api/pool"),
    ("GET", "/api/equity"),
    ("GET", "/api/pnl"),
    ("GET", "/api/positions"),
    ("GET", "/api/response_times"),
    ("GET", "/api/errors"),
    ("GET", "/api/matchup"),
    ("GET", "/api/status"),
    ("POST", "/api/wallets"),
    ("POST", "/api/arm"),
    ("POST", "/api/flatten"),
    ("POST", "/api/incident/clear"),
];
pub(crate) fn header_of<'a>(req: &'a str, name: &str) -> Option<&'a str> {
    let want = format!("{}:", name.to_ascii_lowercase());
    req.lines()
        .take_while(|l| !l.is_empty())
        .find(|l| l.to_ascii_lowercase().starts_with(&want))
        .map(|l| l[want.len()..].trim())
}
pub(crate) fn mutation_refusal(req: &str) -> Option<(&'static str, String)> {
    let ct = header_of(req, "content-type").unwrap_or("");
    let ct_main = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    if ct_main != "application/json" {
        return Some((
            "415 Unsupported Media Type",
            format!(
                "mutations require Content-Type: application/json (got {ct:?}). This is what \
stops a hostile page in your browser from aiming a request at this origin: that content \
type forces a preflight, and we answer none."
            ),
        ));
    }
    if let Some(origin) = header_of(req, "origin") {
        let host = header_of(req, "host").unwrap_or("");
        let authority = origin.split("://").nth(1).unwrap_or("");
        let origin_host = authority.split(':').next().unwrap_or("");
        let ok = origin.is_empty() || (!authority.is_empty() && authority == host)
            || origin_host == "127.0.0.1" || origin_host == "localhost";
        if !ok {
            return Some((
                "403 Forbidden",
                format!(
                    "cross-origin mutation refused (Origin {origin:?}, Host {host:?})"
                ),
            ));
        }
    }
    None
}
pub(crate) fn route_refusal(method: &str, path: &str) -> Option<(&'static str, &'static str)> {
    if method == "GET" && path == "/api/status" {
        return None;
    }
    if ROUTES.iter().any(|(_, p)| *p == path) {
        return Some(("405 Method Not Allowed", "method not allowed for this path"));
    }
    Some(("404 Not Found", "no such endpoint"))
}
