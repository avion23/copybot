//! Existing entrypoint, operator HTTP and restart regression coverage.

    fn req_with(ct: &str, origin: Option<&str>) -> String {
        let mut r = String::from(
            "POST /api/arm HTTP/1.1\r\nHost: box.tailnet.ts.net\r\n",
        );
        if !ct.is_empty() {
            r.push_str(&format!("Content-Type: {ct}\r\n"));
        }
        if let Some(o) = origin {
            r.push_str(&format!("Origin: {o}\r\n"));
        }
        r.push_str("\r\n{}");
        r
    }
    #[test]
    fn a_SIMPLE_cross_origin_content_type_cannot_mutate() {
        for ct in [
            "text/plain",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
            "",
        ] {
            let v = super::mutation_refusal(&req_with(ct, Some("https://evil.example")));
            assert!(v.is_some(), "content-type {ct:?} must be refused");
        }
    }
    #[test]
    fn a_CROSS_ORIGIN_json_request_is_still_refused() {
        let v = super::mutation_refusal(
            &req_with("application/json", Some("https://evil.example")),
        );
        assert_eq!(v.map(| (c, _) | c), Some("403 Forbidden"));
    }
    #[test]
    fn the_DASHBOARDS_OWN_request_passes() {
        assert!(super::mutation_refusal(& req_with("application/json", None)).is_none());
        assert!(
            super::mutation_refusal(& req_with("application/json; charset=utf-8", None))
            .is_none()
        );
        assert!(
            super::mutation_refusal(& req_with("application/json",
            Some("https://box.tailnet.ts.net"))).is_none()
        );
        assert!(
            super::mutation_refusal(& req_with("application/json",
            Some("http://127.0.0.1:8807"))).is_none()
        );
    }
    #[test]
    fn header_lookup_is_case_insensitive_and_stops_at_the_body() {
        let r = "POST /x HTTP/1.1\r\nCoNtEnT-TyPe: application/json\r\n\r\nOrigin: evil";
        assert_eq!(super::header_of(r, "content-type"), Some("application/json"));
        assert_eq!(
            super::header_of(r, "origin"), None,
            "a header-shaped line in the BODY must never be read as a header"
        );
    }
    fn arm_file(tag: &str, body: &str) -> String {
        let dir = std::env::temp_dir()
            .join(format!("cb041-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("control.json.operator").to_string_lossy().into_owned();
        if !body.is_empty() {
            std::fs::write(&p, body).unwrap();
        }
        p
    }
    #[test]
    fn an_arm_write_stamps_provenance_and_REPLACES_a_stale_stamp() {
        let p = arm_file(
            "meta",
            r#"{"lanes":{"example_lane_26":{"armed":true,"halt_buys":true}},
                                     "_meta":{"by":"switch.sh","at":1,"why":"an older edit"}}"#,
        );
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_26","state":"off"}"#,
            "test@example.com",
        );
        assert_eq!(code, "200 OK");
        let doc: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert_eq!(
            doc["_meta"] ["by"], serde_json::json!("api:/api/arm"),
            "the stale switch.sh stamp must not survive"
        );
        assert!(
            doc["_meta"] ["at"].as_i64().unwrap() > 1_700_000_000,
            "the timestamp must be current, not the stale 1"
        );
        assert!(doc["_meta"] ["why"].as_str().unwrap().contains("example_lane_26"));
        assert_eq!(doc["lanes"] ["example_lane_26"] ["armed"], serde_json::json!(false));
    }
    #[test]
    fn each_lane_KEEPS_ITS_OWN_author_when_another_lane_is_changed_after_it() {
        let p = arm_file("perlane", r#"{"lanes":{}}"#);
        super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_26","state":"halt_buys"}"#,
            "alice@example.com",
        );
        super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"halt_buys"}"#,
            "bob@example.com",
        );
        let doc: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert_eq!(
            doc["lanes"] ["example_lane_26"] ["by"], serde_json::json!("alice@example.com"),
            "example_lane_26 must keep ITS author after example_lane_25 was changed later"
        );
        assert_eq!(doc["lanes"] ["example_lane_25"] ["by"], serde_json::json!("bob@example.com"));
        assert_eq!(doc["_meta"] ["actor"], serde_json::json!("bob@example.com"));
        for l in ["example_lane_26", "example_lane_25"] {
            assert_eq!(doc["lanes"] [l] ["armed"], serde_json::json!(true));
            assert_eq!(doc["lanes"] [l] ["halt_buys"], serde_json::json!(true));
            assert!(doc["lanes"] [l] ["at"].as_i64().unwrap() > 1_700_000_000);
        }
    }
    #[test]
    fn a_request_with_no_Cf_Access_identity_is_recorded_as_UNIDENTIFIED() {
        let a = super::actor_of("POST /api/arm HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(a.starts_with("unidentified"), "got {a:?}");
        let b = super::actor_of(
            "POST /api/arm HTTP/1.1\r\nCf-Access-Authenticated-User-Email: sam@example.com\r\n\r\n",
        );
        assert_eq!(b, "sam@example.com");
        let c = super::actor_of(
            "POST /api/arm HTTP/1.1\r\nCf-Access-Authenticated-User-Email:    \r\n\r\n",
        );
        assert!(c.starts_with("unidentified"), "got {c:?}");
    }
    #[test]
    fn provenance_does_not_disturb_the_lanes_object_the_runtime_reads() {
        let p = arm_file("metasafe", r#"{"lanes":{"example_lane_25":{"armed":true}}}"#);
        super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_26","state":"halt_buys"}"#,
            "test@example.com",
        );
        let doc: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert!(doc["lanes"].is_object());
        assert_eq!(doc["lanes"] ["example_lane_25"] ["armed"], serde_json::json!(true));
        assert_eq!(doc["lanes"] ["example_lane_26"] ["halt_buys"], serde_json::json!(true));
    }
    #[test]
    fn arming_ONE_lane_leaves_the_others_untouched() {
        let p = arm_file(
            "merge",
            r#"{"lanes":{"example_lane_26":{"armed":true,"halt_buys":true},
                                               "example_lane_25":{"armed":true,"halt_buys":false}}}"#,
        );
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"off"}"#,
            "test@example.com",
        );
        assert_eq!(code, "200 OK");
        let doc: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert_eq!(doc["lanes"] ["example_lane_25"] ["armed"], serde_json::json!(false));
        assert_eq!(
            doc["lanes"] ["example_lane_26"] ["armed"], serde_json::json!(true),
            "another lane's arm state must survive"
        );
        assert_eq!(
            doc["lanes"] ["example_lane_26"] ["halt_buys"], serde_json::json!(true),
            "and so must its halt_buys"
        );
    }
    #[test]
    fn an_UNREADABLE_operator_file_is_REFUSED_not_replaced() {
        let p = arm_file("corrupt", "{ this is not json");
        let (code, out) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"off"}"#,
            "test@example.com",
        );
        assert_eq!(code, "409 Conflict", "{out}");
        assert_eq!(
            std::fs::read_to_string(& p).unwrap(), "{ this is not json",
            "the file must be left exactly as it was"
        );
    }
    #[test]
    fn a_MISSING_operator_file_is_the_one_case_that_starts_empty() {
        let p = arm_file("absent", "");
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"off"}"#,
            "test@example.com",
        );
        assert_eq!(code, "200 OK");
        let doc: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert_eq!(doc["lanes"] ["example_lane_25"] ["armed"], serde_json::json!(false));
    }
    #[test]
    fn a_document_with_NO_lanes_object_is_refused() {
        let p = arm_file("nolanes", r#"{"something":"else"}"#);
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"off"}"#,
            "test@example.com",
        );
        assert_eq!(code, "409 Conflict");
    }
    #[test]
    fn ARMING_still_requires_the_confirmation_phrase() {
        let p = arm_file("phrase", r#"{"lanes":{}}"#);
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"armed"}"#,
            "test@example.com",
        );
        assert_eq!(code, "400 Bad Request");
        let (code, _) = super::handle_arm_post(
            &p,
            r#"{"lane":"example_lane_25","state":"armed","phrase":"arm example_lane_25"}"#,
            "test@example.com",
        );
        assert_eq!(code, "200 OK");
    }
    #[test]
    fn no_DECISION_event_is_ever_rate_limited() {
        for kind in [
            "fire",
            "would_fire",
            "skip",
            "signal_guard_skip",
            "clob_resp",
            "submit_precondition_failed",
            "settle_correction",
            "pending_recovered",
            "pending_not_filled",
            "ownership_conflict",
            "recon_unattributed",
            "fire_rate_halt",
        ] {
            assert!(
                ! super::RATE_LIMITED.iter().any(| (k, _) | * k == kind),
                "{kind} carries a decision and must never be throttled"
            );
        }
    }
    #[test]
    fn the_throttled_kinds_are_the_MEASURED_noise() {
        let kinds: Vec<&str> = super::RATE_LIMITED.iter().map(|(k, _)| *k).collect();
        assert_eq!(kinds, vec!["warm", "feed_stats", "recon_gap", "latency"]);
        for (_, gap) in super::RATE_LIMITED {
            assert!(
                * gap > 0 && * gap <= 300, "a health gap of {gap}s is not a heartbeat"
            );
        }
    }
    use super::{route_refusal, ROUTES};
    #[test]
    fn the_STATUS_route_is_the_only_one_that_falls_through() {
        assert_eq!(route_refusal("GET", "/api/status"), None);
    }
    #[test]
    fn an_UNKNOWN_path_is_404_not_the_status_document() {
        for p in ["/garbage", "/api/", "/api/statuses", "/api/status/extra", "/.env"] {
            let (code, _) = route_refusal("GET", p).expect("must refuse");
            assert!(code.starts_with("404"), "{p} -> {code}");
        }
    }
    #[test]
    fn a_PREFIX_of_a_control_route_is_NOT_that_route() {
        for p in [
            "/api/armAnything",
            "/api/arm2",
            "/api/flattenEverything",
            "/api/walletsX",
        ] {
            let (code, _) = route_refusal("POST", p).expect("must refuse");
            assert!(code.starts_with("404"), "{p} reached a control handler: {code}");
        }
    }
    #[test]
    fn a_KNOWN_path_with_the_WRONG_method_is_405() {
        assert_eq!(
            route_refusal("GET", "/api/arm").map(| (c, _) | c),
            Some("405 Method Not Allowed")
        );
        assert_eq!(
            route_refusal("POST", "/api/status").map(| (c, _) | c),
            Some("405 Method Not Allowed")
        );
    }
    #[test]
    fn every_path_the_LIVE_dashboards_and_guardian_call_is_routable() {
        for (m, p) in [
            ("GET", "/api/status"),
            ("GET", "/api/pool"),
            ("GET", "/api/matchup"),
            ("GET", "/api/equity"),
            ("GET", "/api/errors"),
            ("GET", "/api/positions"),
            ("GET", "/api/response_times"),
            ("GET", "/api/trades"),
            ("POST", "/api/wallets"),
            ("POST", "/api/arm"),
            ("POST", "/api/flatten"),
            ("GET", "/pool"),
        ] {
            assert!(ROUTES.contains(& (m, p)), "{m} {p} is not routable");
        }
    }
    use super::{
        handle_arm_post, handle_flatten_post, handle_wallet_patch, marked_position_value,
        mutation_refusal, recovery_target_shares, wallet_equity_pnl,
    };
    use copybot_hot::wallets::{WalletRegistry, WalletSpec};
    fn spec(name: &str, seed: f64, enabled: bool) -> WalletSpec {
        WalletSpec {
            name: name.into(),
            leader: format!(
                "0x{:040x}", name.bytes().fold(1u128, | a, b | a.wrapping_mul(31)
                .wrapping_add(b as u128))
            ),
            seed_usd: seed,
            leader_max_order_usd: Some(9_600.0),
            leader_peak_exposure_usd: Some(390_000.0),
            pct: 0.01,
            enabled,
            stop_mode: "none".into(),
            min_buy_price: 0.02,
            max_buy_price: 0.95,
            max_effective_pct: 0.05,
            compound: true,
            lane_id: None,
            copy_makers: false,
            copy_maker_sells: Some(false),
            exclude_political: false,
            risk: None,
            buy_slippage_c: None,
            sell_slippage_c: None,
            sell_floor_frac: None,
            min_order_usd: None,
            min_fill_floor: None,
            sell_all_frac: None,
        }
    }
    fn tmp(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("cb2_http_{tag}_{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }
    #[test]
    fn a_budget_edit_within_the_wallet_is_applied() {
        let p = tmp("edit");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, _) = handle_wallet_patch(
            &reg,
            Some(5000.0),
            &[],
            r#"{"name":"a","seed_usd":2500}"#,
        );
        assert_eq!(code, "200 OK");
        assert_eq!(reg.seed_of("a"), Some(2500.0));
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn patching_leader_on_an_EXISTING_wallet_is_refused_not_ignored() {
        let p = tmp("leader");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let before = reg.specs()[0].leader.clone();
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(5000.0),
            &[],
            r#"{"name":"a","leader":"0x9999999999999999999999999999999999999999"}"#,
        );
        assert_eq!(
            code, "400 Bad Request",
            "a leader change must refuse loudly, not 200-and-ignore: {body}"
        );
        assert!(body.contains("identity"), "the refusal must say WHY: {body}");
        assert_eq!(reg.specs() [0].leader, before, "and nothing may change");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn origin_check_is_equality_not_suffix() {
        let req = |origin: &str| {
            format!(
                "POST /api/arm HTTP/1.1\r\nHost: bot.mysite.com\r\n\
             Content-Type: application/json\r\nOrigin: {origin}\r\n\r\n"
            )
        };
        assert!(mutation_refusal(& req("https://bot.mysite.com")).is_none());
        assert!(
            mutation_refusal(& req("https://evilbot.mysite.com")).is_some(),
            "a suffix-matching origin must be refused"
        );
        assert!(
            mutation_refusal(& req("null")).is_some(),
            "Origin: null has no business mutating a trading bot"
        );
        assert!(mutation_refusal(& req("http://127.0.0.1:8807")).is_none());
        assert!(mutation_refusal(& req("http://localhost:3000")).is_none());
        assert!(
            mutation_refusal(& req("https://127.0.0.1.evil.com")).is_some(),
            "a loopback-PREFIXED host is not loopback"
        );
        assert!(
            mutation_refusal("POST /api/arm HTTP/1.1\r\nHost: b\r\nContent-Type: application/json\r\n\r\n")
            .is_none()
        );
    }
    #[test]
    fn over_allocation_is_ACCEPTED_and_REPORTED_never_refused() {
        let p = tmp("over");
        let reg = WalletRegistry::load_or_seed(
            &p,
            vec![spec("a", 1000.0, true), spec("b", 1000.0, true)],
        );
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(1500.0),
            &[],
            r#"{"name":"a","seed_usd":2000}"#,
        );
        assert_eq!(code, "200 OK", "over-allocation is the operator's call: {body}");
        assert_eq!(reg.seed_of("a"), Some(2000.0), "and it must actually persist");
        assert!(
            body.contains("exceed the wallet"),
            "the breach must be REPORTED in the response, not swallowed: {body}"
        );
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(1500.0),
            &[],
            r#"{"name":"a","seed_usd":400}"#,
        );
        assert_eq!(code, "200 OK", "reducing an over-budget pool must work: {body}");
        assert_eq!(reg.seed_of("a"), Some(400.0));
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn an_unknown_wallet_with_no_leader_is_refused_but_with_one_is_added() {
        let p = tmp("add");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, _) = handle_wallet_patch(
            &reg,
            None,
            &[],
            r#"{"name":"z","seed_usd":500}"#,
        );
        assert_eq!(code, "400 Bad Request", "cannot patch a wallet that does not exist");
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(9999.0),
            &[],
            &format!(
                r#"{{"name":"z","leader":"0x{:040x}","seed_usd":500,"pct":0.01}}"#, 2
            ),
        );
        assert_eq!(
            code, "400 Bad Request", "an UNMEASURED leader must be refused: {body}"
        );
        assert_eq!(reg.seed_of("z"), None, "and nothing enters the pool");
        let (code, _) = handle_wallet_patch(
            &reg,
            Some(9999.0),
            &[],
            &format!(
                r#"{{"name":"z","leader":"0x{:040x}","seed_usd":500,"pct":0.01,
                          "leader_max_order_usd":9600}}"#,
                2
            ),
        );
        assert_eq!(code, "400 Bad Request", "one of the two measurements is not enough");
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(9999.0),
            &[],
            &format!(
                r#"{{"name":"z","leader":"0x{:040x}","seed_usd":500,"pct":0.01,
                          "leader_max_order_usd":9600,"leader_peak_exposure_usd":390000}}"#,
                2
            ),
        );
        assert_eq!(code, "200 OK", "a measured wallet is added: {body}");
        assert_eq!(reg.seed_of("z"), Some(500.0), "the new wallet is in the pool");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn an_unknown_balance_no_longer_BLOCKS_an_increase() {
        let p = tmp("nobal");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, body) = handle_wallet_patch(
            &reg,
            None,
            &[],
            r#"{"name":"a","seed_usd":1000000}"#,
        );
        assert_eq!(
            code, "200 OK", "an unknown balance must not block an increase: {body}"
        );
        assert_eq!(reg.seed_of("a"), Some(1000000.0));
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn a_pool_INSIDE_its_wallet_gets_no_warning_at_all() {
        let p = tmp("under");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(50000.0),
            &[],
            r#"{"name":"a","seed_usd":2000}"#,
        );
        assert_eq!(code, "200 OK");
        assert!(
            body.contains("\"warning\":null"),
            "a pool within its wallet must carry no warning: {body}"
        );
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn one_leader_may_back_only_ONE_wallet() {
        let p = tmp("dupleader");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let same = reg.specs()[0].leader.clone();
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(50_000.0),
            &[],
            &format!(r#"{{"name":"clone","leader":"{same}","seed_usd":500,"pct":0.01}}"#),
        );
        assert_eq!(code, "400 Bad Request", "{body}");
        assert!(body.contains("already copied"));
        assert_eq!(reg.specs().len(), 1, "nothing was added");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn a_BUILD_ONLY_field_is_refused_on_a_LIVE_lane_instead_of_lying() {
        let p = tmp("buildonly");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let live = vec!["a".to_string()];
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(50_000.0),
            &live,
            r#"{"name":"a","pct":0.25}"#,
        );
        assert_eq!(code, "400 Bad Request", "{body}");
        assert!(body.contains("Disable"), "must say how to apply it: {body}");
        assert_eq!(
            handle_wallet_patch(& reg, Some(50_000.0), & live,
            r#"{"name":"a","seed_usd":2500}"#).0, "200 OK"
        );
        assert_eq!(reg.seed_of("a"), Some(2500.0));
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(50_000.0),
            &[],
            r#"{"name":"a","pct":0.02}"#,
        );
        assert_eq!(code, "200 OK", "{body}");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn an_unknown_field_is_a_rejection_not_a_silent_success() {
        let p = tmp("unknown");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, body) = handle_wallet_patch(
            &reg,
            Some(50_000.0),
            &[],
            r#"{"name":"a","seed_usdd":500}"#,
        );
        assert_eq!(code, "400 Bad Request");
        assert!(body.contains("unknown field"), "{body}");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn a_nonpositive_or_nonfinite_seed_is_refused_on_an_EXISTING_wallet_too() {
        let p = tmp("badseed");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        for bad in [r#"{"name":"a","seed_usd":0}"#, r#"{"name":"a","seed_usd":-5}"#] {
            assert_eq!(
                handle_wallet_patch(& reg, Some(50_000.0), & [], bad).0,
                "400 Bad Request", "{bad}"
            );
        }
        assert_eq!(reg.seed_of("a"), Some(1000.0), "unchanged throughout");
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn DISABLING_a_wallet_always_works_even_when_equity_is_unknown() {
        let p = tmp("disable_nobal");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        let (code, body) = handle_wallet_patch(
            &reg,
            None,
            &["a".to_string()],
            r#"{"name":"a","enabled":false}"#,
        );
        assert_eq!(
            code, "200 OK", "must be able to disable during a balance outage: {body}"
        );
        assert!(! reg.specs() [0].enabled);
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn a_malformed_wallet_body_is_refused() {
        let p = tmp("bad");
        let reg = WalletRegistry::load_or_seed(&p, vec![spec("a", 1000.0, true)]);
        assert_eq!(
            handle_wallet_patch(& reg, None, & [], "not json").0, "400 Bad Request"
        );
        assert_eq!(
            handle_wallet_patch(& reg, None, & [], r#"{"seed_usd":1}"#).0,
            "400 Bad Request"
        );
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn arming_needs_a_typed_phrase_but_stopping_never_does() {
        let p = tmp("arm");
        std::fs::remove_file(&p).ok();
        assert_eq!(
            handle_arm_post(& p, r#"{"lane":"example_lane_26","state":"off"}"#, "test@example.com")
            .0, "200 OK"
        );
        assert_eq!(
            handle_arm_post(& p, r#"{"lane":"example_lane_26","state":"halt_buys"}"#,
            "test@example.com").0, "200 OK"
        );
        assert_eq!(
            handle_arm_post(& p, r#"{"lane":"example_lane_26","state":"armed"}"#,
            "test@example.com").0, "400 Bad Request"
        );
        assert_eq!(
            handle_arm_post(& p,
            r#"{"lane":"example_lane_26","state":"armed","phrase":"arm example_lane_25"}"#,
            "test@example.com").0, "400 Bad Request"
        );
        assert_eq!(
            handle_arm_post(& p,
            r#"{"lane":"example_lane_26","state":"armed","phrase":"arm example_lane_26"}"#, "test@example.com")
            .0, "200 OK"
        );
        assert_eq!(
            handle_arm_post(& p, r#"{"lane":"example_lane_26","state":"sideways"}"#,
            "test@example.com").0, "400 Bad Request"
        );
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn the_three_states_map_onto_armed_and_halt_buys() {
        let p = tmp("armstates");
        std::fs::remove_file(&p).ok();
        let read = || -> serde_json::Value {
            serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap()
        };
        handle_arm_post(
            &p,
            r#"{"lane":"example_lane_26","state":"halt_buys"}"#,
            "test@example.com",
        );
        let d = read();
        assert_eq!(
            d["lanes"] ["example_lane_26"] ["armed"], serde_json::json!(true),
            "halt keeps the lane ARMED so it still follows him OUT"
        );
        assert_eq!(d["lanes"] ["example_lane_26"] ["halt_buys"], serde_json::json!(true));
        handle_arm_post(&p, r#"{"lane":"example_lane_26","state":"off"}"#, "test@example.com");
        let d = read();
        assert_eq!(
            d["lanes"] ["example_lane_26"] ["armed"], serde_json::json!(false),
            "off disarms entirely — exits stop too, positions are HELD"
        );
        assert_eq!(d["lanes"] ["example_lane_26"] ["halt_buys"], serde_json::json!(false));
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn setting_one_lane_never_touches_another() {
        let p = tmp("armmerge");
        std::fs::remove_file(&p).ok();
        std::fs::write(&p, r#"{"lanes":{"example_lane_26":{"armed":true},"example_lane_25":{"armed":true}}}"#)
            .unwrap();
        handle_arm_post(&p, r#"{"lane":"example_lane_26","state":"off"}"#, "test@example.com");
        let d: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&p).unwrap(),
            )
            .unwrap();
        assert_eq!(d["lanes"] ["example_lane_26"] ["armed"], serde_json::json!(false));
        assert_eq!(
            d["lanes"] ["example_lane_25"] ["armed"], serde_json::json!(true),
            "example_lane_25 must be untouched"
        );
        std::fs::remove_file(&p).ok();
    }
    #[test]
    fn a_valid_flatten_intent_is_written_and_a_bad_one_is_not() {
        let p = tmp("flat");
        let mbox = format!("{p}.a");
        std::fs::remove_file(&mbox).ok();
        let now = copybot_hot::ledger::now_secs();
        let good = format!(
            r#"{{"lane":"a","mode":"panic","phrase":"panic a","ts":{now}}}"#
        );
        let (code, _) = handle_flatten_post(&p, &good, |_| Some(false));
        assert_eq!(code, "200 OK");
        assert_eq!(
            std::fs::read_to_string(& mbox).unwrap(), good, "the exact body is persisted"
        );
        let (code, _) = handle_flatten_post(&p, "{not json}", |_| Some(false));
        assert_eq!(code, "400 Bad Request");
        let bad_mode = format!(
            r#"{{"lane":"a","mode":"nope","phrase":"x","ts":{now}}}"#
        );
        let (code, _) = handle_flatten_post(&p, &bad_mode, |_| Some(false));
        assert_eq!(code, "400 Bad Request", "an unknown mode is refused");
        assert_eq!(
            std::fs::read_to_string(& mbox).unwrap(), good,
            "the bad writes did not overwrite"
        );
        std::fs::remove_file(&mbox).ok();
    }
    #[test]
    fn an_ARMED_PANIC_is_REFUSED_AT_THE_ROUTE_not_silently_a_second_later() {
        let p = tmp("flatarmed");
        let mbox = format!("{p}.a");
        std::fs::remove_file(&mbox).ok();
        let now = copybot_hot::ledger::now_secs();
        let body = format!(
            r#"{{"lane":"a","mode":"panic","phrase":"panic a","ts":{now}}}"#
        );
        let (code, out) = handle_flatten_post(&p, &body, |_| Some(true));
        assert_eq!(
            code, "409 Conflict",
            "a 200 here IS the bug — the UI reports success and nothing sells"
        );
        assert!(
            out.contains("DISARMED"),
            "the refusal must carry the runtime's own reason, got {out}"
        );
        assert!(
            std::fs::metadata(& mbox).is_err(),
            "a request that cannot execute must not be queued at all"
        );
        let (code, _) = handle_flatten_post(&p, &body, |_| Some(false));
        assert_eq!(code, "200 OK");
        std::fs::remove_file(&mbox).ok();
    }
    #[test]
    fn an_unknown_lane_is_told_so_INSTEAD_of_being_queued_into_silence() {
        let p = tmp("flatunk");
        let now = copybot_hot::ledger::now_secs();
        let body = format!(
            r#"{{"lane":"ghost","mode":"panic","phrase":"panic ghost","ts":{now}}}"#
        );
        let (code, out) = handle_flatten_post(&p, &body, |_| None);
        assert_eq!(code, "404 Not Found");
        assert!(out.contains("nothing has been sold"), "got {out}");
        assert!(std::fs::metadata(format!("{p}.ghost")).is_err());
    }
    #[test]
    fn two_lanes_flattened_in_the_SAME_SECOND_both_survive() {
        let p = tmp("flatbox");
        let now = copybot_hot::ledger::now_secs();
        let a = format!(
            r#"{{"lane":"example_lane_26","mode":"panic","phrase":"panic example_lane_26","ts":{now}}}"#
        );
        let b = format!(
            r#"{{"lane":"example_lane_25","mode":"panic","phrase":"panic example_lane_25","ts":{now}}}"#
        );
        std::fs::remove_file(format!("{p}.example_lane_26")).ok();
        std::fs::remove_file(format!("{p}.example_lane_25")).ok();
        assert_eq!(handle_flatten_post(& p, & a, | _ | Some(false)).0, "200 OK");
        assert_eq!(handle_flatten_post(& p, & b, | _ | Some(false)).0, "200 OK");
        assert_eq!(
            std::fs::read_to_string(format!("{p}.example_lane_26")).unwrap(), a,
            "example_lane_26's intent was erased by example_lane_25's — one wallet would not flatten"
        );
        assert_eq!(std::fs::read_to_string(format!("{p}.example_lane_25")).unwrap(), b);
        std::fs::remove_file(format!("{p}.example_lane_26")).ok();
        std::fs::remove_file(format!("{p}.example_lane_25")).ok();
    }
    #[test]
    fn a_lane_name_can_never_CHOOSE_THE_FILE_the_route_writes() {
        let p = tmp("flattrav");
        let now = copybot_hot::ledger::now_secs();
        for bad in ["../../etc/passwd", "a/b", "", "with space", &"x".repeat(33)] {
            let body = format!(
                r#"{{"lane":"{bad}","mode":"panic","phrase":"panic {bad}","ts":{now}}}"#
            );
            let (code, _) = handle_flatten_post(&p, &body, |_| Some(false));
            assert_eq!(code, "400 Bad Request", "lane {bad:?} must be refused");
        }
    }
    #[test]
    fn wallet_equity_change_uses_total_equity_not_free_cash() {
        assert_eq!(wallet_equity_pnl(Some(10_254.43), 10_000.0), Some(254.43));
        assert_eq!(wallet_equity_pnl(None, 10_000.0), None);
        let marked = marked_position_value(
            &serde_json::json!(
                [{ "currentValue" : 600.25 }, { "currentValue" : "291.47" }]
            ),
        );
        assert_eq!(marked, Some(891.72));
        assert_eq!(
            wallet_equity_pnl(Some(9_362.71 + marked.unwrap()), 10_000.0), Some(254.43)
        );
    }
    #[test]
    fn recovery_honours_a_FIXED_RATE_lane_instead_of_assuming_compounding() {
        let (leader, l_px, our_px, pct, min_usd, cap) = (
            1_000.0,
            0.40,
            0.50,
            0.015,
            1.0,
            0.05,
        );
        let scale = 1.50;
        let compounding = recovery_target_shares(
                leader,
                l_px,
                our_px,
                pct,
                scale,
                min_usd,
                cap,
                true,
            )
            .expect("compounding target");
        let fixed = recovery_target_shares(
                leader,
                l_px,
                our_px,
                pct,
                scale,
                min_usd,
                cap,
                false,
            )
            .expect("fixed target");
        assert!(
            compounding > fixed,
            "a compounding lane should target MORE after gains: {compounding} vs {fixed}"
        );
        let flat = recovery_target_shares(
                leader,
                l_px,
                our_px,
                pct,
                1.0,
                min_usd,
                cap,
                false,
            )
            .expect("unscaled target");
        assert!(
            (fixed - flat).abs() < 1e-9,
            "compound=false must ignore cap_scale entirely: {fixed} vs {flat}"
        );
    }
    #[test]
    fn recovery_and_the_hot_path_agree_on_the_same_lane() {
        for compound in [true, false] {
            for scale in [0.75_f64, 1.0, 1.4] {
                let hot = copybot_hot::budget::effective_pct(
                    0.015,
                    scale,
                    0.05,
                    compound,
                );
                let rec = recovery_target_shares(
                        1_000.0,
                        0.50,
                        0.50,
                        0.015,
                        scale,
                        1.0,
                        0.05,
                        compound,
                    )
                    .expect("target");
                let implied = (rec - 1.0) * 0.50 / (1_000.0 * 0.50);
                assert!(
                    (implied - hot).abs() < 1e-9,
                    "compound={compound} scale={scale}: recovery implies {implied}, hot path {hot}"
                );
            }
        }
    }
    #[test]
    fn restart_recovery_is_sell_only_and_leader_flat_is_unambiguous() {
        let target = recovery_target_shares(
                0.0,
                0.0,
                0.59,
                0.015,
                1.001084,
                1.0,
                0.05,
                true,
            )
            .expect("leader-flat target");
        assert_eq!(target, 0.0);
        assert!(
            (copybot_hot::sweep::excess_shares(20.816325, 20.8328, target) - 20.816325)
            .abs() < 1e-9
        );
        assert_eq!(
            copybot_hot::sweep::excess_shares(10.0, 7.0, 0.0), 7.0,
            "the physical wallet is an availability ceiling"
        );
        assert_eq!(
            copybot_hot::sweep::excess_shares(5.0, 5.0, 8.0), 0.0,
            "normalization must never create a buy requirement"
        );
    }
    #[test]
    fn restart_recovery_preserves_dollar_ratio_and_entry_rounding() {
        let target = recovery_target_shares(
                1_000.0,
                0.40,
                0.50,
                0.015,
                1.0,
                1.0,
                0.05,
                true,
            )
            .expect("priced target");
        assert!((target - 13.0).abs() < 1e-9);
        assert_eq!(copybot_hot::sweep::excess_shares(13.0, 13.0, target), 0.0);
        assert!(
            recovery_target_shares(100.0, 0.0, 0.5, 0.015, 1.0, 1.0, 0.05, true)
            .is_none(),
            "unknown cost basis must fail closed while the leader is still in"
        );
    }
    #[test]
    fn the_incident_clear_route_is_REGISTERED_and_phrase_gated() {
        assert_eq!(
            route_refusal("GET", "/api/incident/clear").map(| (c, _) | c),
            Some("405 Method Not Allowed"),
            "a registered path must 405 on the wrong method, never 404"
        );
        assert!(
            ROUTES.contains(& ("POST", "/api/incident/clear")),
            "the clear route must stay registered, or a safety latch becomes \
                 permanent and the only recovery is editing files by hand"
        );
    }
