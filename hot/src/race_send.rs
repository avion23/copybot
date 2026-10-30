use std::time::Duration;
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Matched { body: String, winner: String, ms: f64 },
    Resting { order_id: String, body: String, winner: String, ms: f64 },
    DuplicateOnly { winner: String },
    Rejected { body: String },
    Ambiguous { detail: String },
    NoResponse,
}
#[derive(Debug, Clone)]
pub struct PathResult {
    pub path: String,
    pub http: u16,
    pub body: String,
    pub ms: f64,
}
const DUPLICATE_MARKERS: &[&str] = &[
    "duplicate",
    "already exists",
    "already known",
    "order exists",
    "already placed",
    "same order",
    "orderalreadyexists",
];
pub fn looks_duplicate(body: &str) -> bool {
    let b = body.to_ascii_lowercase();
    DUPLICATE_MARKERS.iter().any(|m| b.contains(m))
}
const RESTING_MARKERS: &[&str] = &[
    "\"status\":\"live\"",
    "\"status\": \"live\"",
    "\"status\":\"unmatched\"",
    "\"status\": \"unmatched\"",
    "\"status\":\"delayed\"",
    "\"status\": \"delayed\"",
];
pub fn looks_resting(body: &str) -> Option<String> {
    let b = body.to_ascii_lowercase();
    if b.contains("\"success\": false") || b.contains("\"success\":false") {
        return None;
    }
    if !RESTING_MARKERS.iter().any(|m| b.contains(m)) {
        return None;
    }
    order_id_of(body)
}
pub fn order_id_of(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    for k in ["orderID", "orderId", "order_id", "id"] {
        if let Some(s) = v.get(k).and_then(|x| x.as_str()) {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}
pub fn looks_matched(body: &str) -> bool {
    let b = body.to_ascii_lowercase();
    (b.contains("\"status\":\"matched\"") || b.contains("\"status\": \"matched\"")
        || b.contains("\"status\":\"filled\"") || b.contains("\"status\": \"filled\""))
        && !b.contains("\"success\": false") && !b.contains("\"success\":false")
}
/// A rejection releases the pending reservation only when every attempted
/// path returned a definitive refusal. Silent, missing, throttled or 2xx
/// unclassified replies mean an order may exist at the venue.
pub fn definitive_refusal(r: &PathResult) -> bool {
    if (400..500).contains(&r.http) && !matches!(r.http, 408 | 409 | 425 | 429) {
        return true;
    }
    if !(200..300).contains(&r.http) { return false; }
    let Ok(body) = serde_json::from_str::<serde_json::Value>(&r.body) else {
        return false;
    };
    body["success"].as_bool() == Some(false)
        || ["error", "errorMsg"].iter().any(|key| {
            body[*key].as_str().is_some_and(|msg| !msg.trim().is_empty())
        })
}
pub fn resolve_with_expected(results: &[PathResult], expected_paths: usize) -> Outcome {
    if results.is_empty() {
        return Outcome::NoResponse;
    }
    if let Some(r) = results
        .iter()
        .filter(|r| (200..300).contains(&r.http) && looks_matched(&r.body))
        .min_by(|a, b| a.ms.total_cmp(&b.ms))
    {
        return Outcome::Matched {
            body: r.body.clone(),
            winner: r.path.clone(),
            ms: r.ms,
        };
    }
    if let Some(r) = results
        .iter()
        .filter(|r| (200..300).contains(&r.http) && looks_resting(&r.body).is_some())
        .min_by(|a, b| a.ms.total_cmp(&b.ms))
    {
        return Outcome::Resting {
            order_id: looks_resting(&r.body).unwrap(),
            body: r.body.clone(),
            winner: r.path.clone(),
            ms: r.ms,
        };
    }
    let dupes: Vec<&PathResult> = results
        .iter()
        .filter(|r| looks_duplicate(&r.body))
        .collect();
    if !dupes.is_empty() {
        if let Some(other) = results
            .iter()
            .find(|r| !looks_duplicate(&r.body) && r.http != 0 && r.http < 500)
        {
            eprintln!(
                "[race] duplicate alongside another response — keeping reservation \
                       for reconciliation; sibling HTTP {}", other.http
            );
        }
        return Outcome::DuplicateOnly {
            winner: dupes[0].path.clone(),
        };
    }
    if results.len() != expected_paths || !results.iter().all(definitive_refusal) {
        let mut detail = results.iter()
            .map(|r| format!("{}:{}", r.path, r.http))
            .collect::<Vec<_>>().join(",");
        if results.len() != expected_paths {
            detail.push_str(&format!(",missing_paths={}", expected_paths.saturating_sub(results.len())));
        }
        return Outcome::Ambiguous { detail };
    }
    Outcome::Rejected { body: results[0].body.clone() }
}
/// Pure classifier assumes all paths given; venue execution uses the expected
/// socket count to prevent a dropped task from releasing its reservation.
pub fn resolve(results: &[PathResult]) -> Outcome {
    resolve_with_expected(results, results.len())
}
pub async fn race_submit(
    clients: &[(String, reqwest::Client)],
    url: &str,
    body: &str,
    headers: &[(&'static str, String)],
    timeout: Duration,
) -> (Outcome, Vec<PathResult>) {
    let mut set = tokio::task::JoinSet::new();
    for (name, c) in clients {
        let (name, c, url, body) = (
            name.clone(),
            c.clone(),
            url.to_string(),
            body.to_string(),
        );
        let headers = headers.to_vec();
        set.spawn(async move {
            let t0 = std::time::Instant::now();
            let mut rb = c
                .post(&url)
                .header("content-type", "application/json")
                .body(body);
            for (k, v) in &headers {
                rb = rb.header(*k, v);
            }
            let r = tokio::time::timeout(timeout, rb.send()).await;
            let (http, text) = match r {
                Ok(Ok(resp)) => {
                    let s = resp.status().as_u16();
                    (s, resp.text().await.unwrap_or_default())
                }
                Ok(Err(e)) => (0, format!("{{\"transport_error\":\"{e}\"}}")),
                Err(_) => (0, "{\"transport_error\":\"timeout\"}".into()),
            };
            PathResult {
                path: name,
                http,
                body: text,
                ms: t0.elapsed().as_micros() as f64 / 1000.0,
            }
        });
    }
    let mut results = Vec::new();
    while let Some(r) = set.join_next().await {
        let Ok(pr) = r else { continue };
        // A 5xx error body can echo an earlier matched payload; only an
        // actual successful HTTP response may end the race before all paths.
        let decisive = (200..300).contains(&pr.http) && looks_matched(&pr.body);
        results.push(pr);
        if decisive && !set.is_empty() {
            tokio::spawn(async move { while set.join_next().await.is_some() {} });
            return (resolve_with_expected(&results, clients.len()), results);
        }
    }
    (resolve_with_expected(&results, clients.len()), results)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn pr(path: &str, body: &str, ms: f64) -> PathResult {
        PathResult {
            path: path.into(),
            http: 200,
            body: body.into(),
            ms,
        }
    }
    #[test]
    fn a_LONE_MATCH_resolves_without_its_siblings() {
        let one = vec![pr("a", MATCHED, 12.0)];
        assert!(matches!(resolve(& one), Outcome::Matched { .. }));
    }
    #[test]
    fn a_LONE_REJECT_is_NOT_something_we_may_return_early_on() {
        let reject = r#"{"error":"insufficient balance"}"#;
        assert!(matches!(resolve(& [pr("a", reject, 3.0)]), Outcome::Rejected { .. }));
        let with_sibling = vec![pr("a", reject, 3.0), pr("b", MATCHED, 40.0)];
        assert!(
            matches!(resolve(& with_sibling), Outcome::Matched { .. }),
            "a later match outranks an earlier reject — which is why we must wait"
        );
    }
    #[test]
    fn a_lone_DUPLICATE_never_resolves_early_either() {
        let dupe = r#"{"error":"order already exists"}"#;
        let lone = resolve(&[pr("a", dupe, 2.0)]);
        assert!(
            matches!(lone, Outcome::DuplicateOnly { .. }),
            "a single duplicate looks like DuplicateOnly — which is exactly why \
             race_submit must NOT return on it before the siblings answer"
        );
    }
    const MATCHED: &str = r#"{"status":"matched","success":true,"takingAmount":"5.0"}"#;
    const DUPE: &str = r#"{"errorMsg":"duplicate order","success":false}"#;
    const NSF: &str = r#"{"errorMsg":"not enough balance","success":false}"#;
    #[test]
    fn a_match_on_any_path_wins_however_the_others_replied() {
        let o = resolve(&[pr("h1", DUPE, 18.0), pr("h2", MATCHED, 22.0)]);
        assert!(matches!(o, Outcome::Matched { .. }));
    }
    #[test]
    fn the_FASTEST_match_is_reported_when_several_report_one() {
        let o = resolve(&[pr("slow", MATCHED, 40.0), pr("fast", MATCHED, 12.0)]);
        match o {
            Outcome::Matched { winner, ms, .. } => {
                assert_eq!(winner, "fast");
                assert!((ms - 12.0).abs() < 1e-9);
            }
            _ => panic!("expected Matched"),
        }
    }
    const RESTING: &str = r#"{"success":true,"orderID":"0xdead","status":"live"}"#;
    #[test]
    fn a_GTC_that_RESTS_is_not_a_reject() {
        let o = resolve(&[pr("h1", RESTING, 20.0)]);
        match o {
            Outcome::Resting { order_id, winner, .. } => {
                assert_eq!(order_id, "0xdead", "the id is what makes it cancellable");
                assert_eq!(winner, "h1");
            }
            other => panic!("a resting GTC must not be {other:?}"),
        }
    }
    #[test]
    fn a_MATCH_still_beats_a_resting_reply() {
        let o = resolve(&[pr("slow", RESTING, 30.0), pr("fast", MATCHED, 10.0)]);
        assert!(matches!(o, Outcome::Matched { .. }));
    }
    #[test]
    fn a_resting_reply_beats_a_siblings_duplicate_error() {
        let o = resolve(&[pr("h2", DUPE, 10.0), pr("h1", RESTING, 20.0)]);
        assert!(matches!(o, Outcome::Resting { .. }), "got {o:?}");
    }
    #[test]
    fn a_resting_reply_without_an_order_id_is_UNVERIFIABLE_not_terminal() {
        let body = r#"{"success":true,"status":"live"}"#;
        assert!(looks_resting(body).is_none());
        assert!(matches!(resolve(& [pr("h1", body, 10.0)]), Outcome::Ambiguous { .. }));
    }
    #[test]
    fn a_FAILED_order_is_never_read_as_resting() {
        let body = r#"{"success":false,"orderID":"0xdead","status":"live"}"#;
        assert!(looks_resting(body).is_none());
    }
    #[test]
    fn order_id_is_found_under_any_of_the_venues_spellings() {
        assert_eq!(order_id_of(r#"{"orderID":"0xa"}"#), Some("0xa".into()));
        assert_eq!(order_id_of(r#"{"orderId":"0xb"}"#), Some("0xb".into()));
        assert_eq!(order_id_of(r#"{"order_id":"0xc"}"#), Some("0xc".into()));
        assert_eq!(order_id_of(r#"{"orderID":""}"#), None, "empty is not an id");
        assert_eq!(order_id_of("not json"), None);
    }
    #[test]
    fn an_ALL_TIMEOUT_race_is_AMBIGUOUS_not_rejected() {
        let rs = vec![
            PathResult { path : "h1".into(), http : 0, ms : 10_000.0, body :
            "{\"transport_error\":\"timeout\"}".into() }, PathResult { path : "h2"
            .into(), http : 0, ms : 10_000.0, body : "{\"transport_error\":\"timeout\"}"
            .into() },
        ];
        match resolve(&rs) {
            Outcome::Ambiguous { detail } => assert!(detail.contains("h1:0")),
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }
    #[test]
    fn an_ALL_5xx_race_is_AMBIGUOUS() {
        let rs = vec![
            PathResult { path : "h1".into(), http : 502, ms : 5.0, body : "bad gateway"
            .into() }, PathResult { path : "h2".into(), http : 503, ms : 5.0, body :
            "unavailable".into() },
        ];
        assert!(matches!(resolve(& rs), Outcome::Ambiguous { .. }));
    }
    #[test]
    fn one_4xx_cannot_release_reservation_if_a_sibling_did_not_reply() {
        let rs = vec![
            PathResult { path : "h1".into(), http : 400, ms : 5.0, body :
            "{\"error\":\"not enough balance / allowance\"}".into() }, PathResult { path
            : "h2".into(), http : 0, ms : 10_000.0, body :
            "{\"transport_error\":\"timeout\"}".into() },
        ];
        assert!(
            matches!(resolve(& rs), Outcome::Ambiguous { .. }),
            "one real 4xx cannot prove a silent sibling was rejected"
        );
    }
    #[test]
    fn a_match_on_ANY_path_still_wins_over_silence_elsewhere() {
        let rs = vec![
            PathResult { path : "h1".into(), http : 0, ms : 10_000.0, body :
            "{\"transport_error\":\"timeout\"}".into() }, PathResult { path : "h2"
            .into(), http : 200, ms : 30.0, body :
            "{\"orderID\":\"0xabc\",\"takingAmount\":\"5\",\"makingAmount\":\"5\",\"status\":\"matched\"}"
            .into() },
        ];
        assert!(matches!(resolve(& rs), Outcome::Matched { .. }));
    }
    #[test]
    fn a_duplicate_error_is_NEVER_a_reject_on_its_own() {
        let o = resolve(&[pr("h1", DUPE, 18.0), pr("h2", DUPE, 19.0)]);
        assert!(
            matches!(o, Outcome::DuplicateOnly { .. }),
            "all-duplicate must be AMBIGUOUS, never Rejected"
        );
    }
    #[test]
    fn a_real_error_on_every_path_is_a_reject() {
        let o = resolve(&[pr("h1", NSF, 18.0), pr("h2", NSF, 19.0)]);
        match o {
            Outcome::Rejected { body } => assert!(body.contains("not enough balance")),
            _ => panic!("expected Rejected"),
        }
    }
    #[test]
    fn a_real_error_beside_a_duplicate_is_DUPLICATE_not_a_reject() {
        let o = resolve(&[pr("h1", DUPE, 18.0), pr("h2", NSF, 19.0)]);
        assert!(
            matches!(o, Outcome::DuplicateOnly { .. }),
            "a duplicate is never a reject — got {o:?}"
        );
    }
    #[test]
    fn a_duplicate_beside_a_TIMEOUT_is_also_not_a_reject() {
        let o = resolve(
            &[
                pr("h1", DUPE, 18.0),
                PathResult {
                    path: "h2".into(),
                    http: 0,
                    body: String::new(),
                    ms: 19.0,
                },
            ],
        );
        assert!(matches!(o, Outcome::DuplicateOnly { .. }), "got {o:?}");
    }
    #[test]
    fn a_real_error_with_NO_duplicate_is_still_a_clean_reject() {
        match resolve(&[pr("h1", NSF, 18.0)]) {
            Outcome::Rejected { body } => assert!(body.contains("not enough balance")),
            o => panic!("a lone real error is a reject — got {o:?}"),
        }
    }
    #[test]
    fn an_all_path_refusal_is_required_before_releasing_a_reservation() {
        let reject = PathResult {
            path: "h1".into(), http: 400, ms: 2.0,
            body: r#"{"error":"insufficient allowance"}"#.into(),
        };
        assert!(matches!(
            resolve_with_expected(&[reject.clone()], 2),
            Outcome::Ambiguous { .. }
        ), "a disappeared submit task is not a negative venue verdict");
        let second = PathResult {
            path: "h2".into(), http: 403, ms: 3.0,
            body: r#"{"error":"unauthorized"}"#.into(),
        };
        assert!(matches!(
            resolve_with_expected(&[reject, second], 2),
            Outcome::Rejected { .. }
        ), "both explicit refusals mean the venue cannot hold that order");
    }
    #[test]
    fn throttles_transport_failures_and_unclassified_successes_retain_reservations() {
        for (http, body) in [
            (0, ""),
            (502, "temporary"),
            (408, "timeout"),
            (409, "conflict without known identity"),
            (429, "rate limited"),
            (200, r#"{"success":true}"#),
            (202, r#"{"accepted":true}"#),
        ] {
            let result = PathResult {
                path:"unknown".into(), http, body:body.into(), ms:3.0,
            };
            let reject = PathResult {
                path:"reject".into(), http:400,
                body:r#"{"error":"allowance"}"#.into(), ms:2.0,
            };
            assert!(matches!(
                resolve(&[reject, result]), Outcome::Ambiguous { .. }
            ), "HTTP {http} with unproven sibling cannot release an order");
        }
    }
    #[test]
    fn a_5xx_echoing_matched_does_not_prove_a_fill() {
        let echoed = PathResult {
            path:"bad_gateway".into(), http:503,
            body: r#"{"status":"matched","success":true}"#.into(), ms:1.0,
        };
        assert!(matches!(resolve(&[echoed]), Outcome::Ambiguous { .. }));
    }
    #[test]
    fn no_responses_at_all_is_not_a_reject() {
        assert_eq!(resolve(& []), Outcome::NoResponse);
    }
    #[test]
    fn a_transport_failure_on_one_path_does_not_lose_the_win() {
        let o = resolve(
            &[
                pr("h1", r#"{"transport_error":"timeout"}"#, 10000.0),
                pr("h2", MATCHED, 21.0),
            ],
        );
        assert!(matches!(o, Outcome::Matched { .. }));
    }
    #[test]
    fn duplicate_markers_are_recognised_case_insensitively() {
        for b in [
            r#"{"errorMsg":"Duplicate order"}"#,
            r#"{"error":"ORDER ALREADY EXISTS"}"#,
            r#"{"errorMsg":"order already known"}"#,
        ] {
            assert!(looks_duplicate(b), "missed: {b}");
        }
        assert!(! looks_duplicate(NSF));
    }
    #[test]
    fn a_failed_body_is_not_read_as_matched() {
        let b = r#"{"status":"matched","success":false}"#;
        assert!(! looks_matched(b), "success:false must never read as a fill");
    }
    #[test]
    fn one_order_yields_exactly_one_outcome_however_many_paths() {
        for n in 1..6 {
            let mut rs = vec![pr("win", MATCHED, 20.0)];
            for i in 0..n {
                rs.push(pr(&format!("dupe{i}"), DUPE, 21.0 + i as f64));
            }
            assert!(matches!(resolve(& rs), Outcome::Matched { .. }));
        }
    }
}
