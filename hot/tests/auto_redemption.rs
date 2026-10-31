use copybot_hot::{
    ledger::Ledger,
    order_commit::AtomicFakJournal,
    pending::PendingLog,
    positions::{Completeness, Positions},
    risk::RiskConfig,
    settlement::{self, Redemption},
};
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    dir: std::path::PathBuf,
    ledger: Ledger,
    journal: AtomicFakJournal,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "copybot-redemption-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let ledger = Ledger::new(dir.join("ledger.jsonl").to_str().unwrap(), &cfgs());
        let journal = AtomicFakJournal::open(dir.join("money.jsonl")).unwrap();
        Self { dir, ledger, journal }
    }
    fn buy(&mut self) {
        assert!(self.ledger.record_fill("test", "123", 0, 100.0, 0.4, 0.0));
    }
    fn redemption(&self, payout: f64) -> Redemption {
        let now = copybot_hot::ledger::now_secs();
        let rows = serde_json::json!([{"type":"REDEEM", "asset":"123",
            "conditionId":format!("0x{}", "b".repeat(64)),
            "transactionHash":format!("0x{}", "a".repeat(64)),
            "outcomeIndex":0, "size":100.0, "usdcSize":100.0*payout, "timestamp":now}]);
        settlement::parse_redemptions(&rows).remove(0)
    }
    fn reload(&mut self) {
        self.ledger = Ledger::new(&self.ledger.path, &cfgs());
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
fn cfgs() -> Vec<(String, RiskConfig)> {
    vec![("test".into(), RiskConfig::default())]
}
fn flat(r: &Redemption) -> Positions {
    Positions::empty(r.ts + 1)
}
fn assert_closed(f: &Fixture, pnl: f64) {
    assert!(f.ledger.holdings("test").is_empty());
    assert!(f.ledger.open_usd("test").abs() < 1e-9);
    assert!((f.ledger.lanes["test"].risk.realised_pnl - pnl).abs() < 1e-9);
    assert_eq!(f.ledger.lanes["test"].risk.open_positions, 0);
}

#[test]
fn smoke_buy_auto_redeem_empty_wallet_duplicate_and_restart() {
    for (payout, pnl) in [(1.0, 60.0), (0.0, -40.0), (0.5, 10.0)] {
        let mut f = Fixture::new();
        f.buy();
        let r = f.redemption(payout);
        assert_eq!(r.token(&["123".into(), "456".into()]), Some("123"));
        assert!(f
            .ledger
            .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
            .unwrap());
        assert_closed(&f, pnl);
        assert!(!f
            .ledger
            .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
            .unwrap());
        let raw = std::fs::read_to_string(&f.ledger.path).unwrap();
        assert_eq!(
            raw.lines().count(),
            2,
            "one BUY and one atomic settlement, no recon SELL"
        );
        let (keys, releases) = copybot_hot::ledger::scan_for_settlement(&f.ledger.path);
        assert!(releases.is_empty());
        assert!(keys.contains(&r.lane_key("test")));
        assert!(keys.contains(&settlement::cross_writer_key("test", "123")));
        f.reload();
        assert_closed(&f, pnl);
        assert!(!f
            .ledger
            .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
            .unwrap());
        assert!(!f.ledger.record_realised_adjustment_tx(
            "test",
            "123",
            100.0,
            100.0 * payout,
            0.4,
            "must not credit twice",
            &r.lane_key("test")
        ));
        assert_closed(&f, pnl);
        let windows = Ledger::realised_windows_at(
            &f.ledger.path,
            Some("test"),
            copybot_hot::ledger::now_secs(),
        );
        assert!(
            (windows.total - pnl).abs() < 1e-9,
            "dashboard replay agrees with live ledger"
        );
    }
}

#[test]
fn existing_settlement_poll_and_legacy_rows_cannot_be_credited_again() {
    let mut f = Fixture::new();
    f.buy();
    let r = f.redemption(1.0);
    assert!(f.ledger.record_settlement("test", "123", 1.0));
    assert!(!f.ledger.record_settlement("test", "123", 1.0));
    assert!(!f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
        .unwrap());
    assert_closed(&f, 60.0);
    // Replay old keyless settlements and duplicate rows safely too.
    let mut rows: Vec<serde_json::Value> = std::fs::read_to_string(&f.ledger.path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    rows[1].as_object_mut().unwrap().remove("key");
    rows.push(rows[1].clone());
    std::fs::write(
        &f.ledger.path,
        rows.iter().map(|r| format!("{r}\n")).collect::<String>(),
    )
    .unwrap();
    f.reload();
    assert_closed(&f, 60.0);
    assert!(!f.ledger.record_realised_adjustment_tx(
        "test",
        "123",
        100.0,
        100.0,
        0.4,
        "duplicate legacy recovery",
        &r.lane_key("test")
    ));
}

#[test]
fn incomplete_stale_malformed_nonzero_and_pending_evidence_do_not_close() {
    let mut f = Fixture::new();
    f.buy();
    let r = f.redemption(1.0);
    let mut failed = flat(&r);
    failed.completeness = Completeness::Failed {
        after_pages: 0,
        why: "offline".into(),
    };
    let mut partial = flat(&r);
    partial.completeness = Completeness::Truncated { pages: 1, cap: 1 };
    let mut stale = flat(&r);
    stale.as_of -= 2;
    let mut held = flat(&r);
    held.rows = vec![serde_json::json!({"asset":"123","size":1.0})];
    let mut malformed = flat(&r);
    malformed.rows = vec![serde_json::json!({"asset":"123","size":"bad"})];
    for snap in [failed, partial, stale, held, malformed] {
        assert!(f
            .ledger
            .settle_redeemed_open("test", "123", &r, &snap, false, &f.journal)
            .is_err());
    }
    assert!(f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), true, &f.journal)
        .is_err());
    assert_eq!(f.ledger.holdings("test")["123"], 100.0);
    assert_eq!(f.ledger.lanes["test"].risk.realised_pnl, 0.0);
    assert_eq!(
        std::fs::read_to_string(&f.ledger.path)
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[test]
fn ambiguous_amount_time_token_and_pooled_claim_are_deferred() {
    let mut f = Fixture::new();
    f.buy();
    let r = f.redemption(1.0);
    let mut partial = r.clone();
    partial.size = 50.0;
    partial.usdc = 50.0;
    let mut old = r.clone();
    old.ts -= 10;
    let mut wrong = r.clone();
    wrong.asset = "456".into();
    let mut aggregate = r.clone();
    aggregate.asset.clear();
    let mut invalid = r.clone();
    invalid.usdc = f64::NAN;
    for bad in [partial, old, wrong, aggregate, invalid] {
        assert!(f
            .ledger
            .settle_redeemed_open("test", "123", &bad, &flat(&r), false, &f.journal)
            .is_err());
    }
    f.ledger.ensure_lane("second", RiskConfig::default());
    assert!(f.ledger.record_fill("second", "123", 0, 10.0, 0.4, 0.0));
    assert!(f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
        .is_err());
    assert_eq!(f.ledger.holdings("test")["123"], 100.0);
}

#[test]
fn failed_write_keeps_holdings_and_pnl_and_does_not_consume_dedupe_key() {
    let mut f = Fixture::new();
    f.buy();
    let r = f.redemption(1.0);
    let original = f.ledger.path.clone();
    f.ledger.path = f.dir.to_string_lossy().into_owned(); // opening directory for append fails
    assert!(f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
        .is_err());
    assert!(!f.ledger.record_settlement("test", "123", 1.0));
    assert_eq!(
        f.ledger.write_failures, 1,
        "append failure exercised, not just history validation"
    );
    assert_eq!(f.ledger.holdings("test")["123"], 100.0);
    assert_eq!(f.ledger.lanes["test"].risk.realised_pnl, 0.0);
    f.ledger.path = original;
    assert!(f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
        .unwrap());
    f.reload();
    assert_closed(&f, 60.0);
}

#[test]
fn redemption_parser_rejects_missing_outcome_and_invalid_proceeds() {
    let rows = serde_json::json!([{"type":"REDEEM", "transactionHash":"test",
        "conditionId":"test", "size":100.0, "usdcSize":100.0, "timestamp":1000}]);
    assert!(settlement::parse_redemptions(&rows).is_empty());
    let missing_payout = serde_json::json!([{"type":"REDEEM", "transactionHash":"test",
        "conditionId":"test", "outcomeIndex":0, "size":100.0, "timestamp":1000}]);
    assert!(settlement::parse_redemptions(&missing_payout).is_empty());
    let mut f = Fixture::new();
    f.buy();
    let mut r = f.redemption(1.0);
    assert!(r.token(&["456".into()]).is_none());
    r.usdc = 101.0;
    assert!(!r.is_bookable());
    r.usdc = -1.0;
    assert!(!r.is_bookable());
    assert_eq!(
        settlement::resolved_payout(settlement::PositionView {
            redeemable: true,
            cur_price: f64::NAN
        }),
        None
    );
}

#[test]
fn partial_neutral_release_does_not_strand_unbooked_basis() {
    let mut f = Fixture::new();
    f.buy();
    let r = f.redemption(1.0);
    f.ledger.record_recon_fill("test", "123", 1, 25.0, 0.4, 0.0);
    assert!(f
        .ledger
        .settle_redeemed_open("test", "123", &r, &flat(&r), false, &f.journal)
        .unwrap_err()
        .contains("neutral reconciliation release"));
    assert_eq!(f.ledger.holdings("test")["123"], 75.0);
    assert_eq!(f.ledger.lanes["test"].risk.realised_pnl, 0.0);
    assert_eq!(
        std::fs::read_to_string(&f.ledger.path)
            .unwrap()
            .lines()
            .count(),
        2
    );
}

#[test]
fn confirmed_release_correction_uses_one_journal_source_after_restart() {
    let mut f = Fixture::new();
    f.buy();
    // A historical neutral release is test state, not new custody evidence.
    assert!(f.ledger.record_recon_fill("test", "123", 1, 100.0, 0.4, 0.0));
    assert_closed(&f, 0.0);
    let r = f.redemption(1.0);
    let snapshot = flat(&r);
    let pending = PendingLog::open(f.dir.join("pending.jsonl").to_str().unwrap());
    let correct = |ledger: &mut Ledger, journal: &AtomicFakJournal| {
        let mut owner = copybot_hot::order_state_owner::CanonicalMoneyOwner::new(
            journal, ledger, &pending,
        ).unwrap();
        owner.correct_redeemed_release(
            "test", "123", 100.0, 100.0, 0.4, &r, &snapshot, false,
        )
    };
    assert_eq!(correct(&mut f.ledger, &f.journal).unwrap(), true);
    assert_eq!(f.journal.money_commits().unwrap().len(), 1);
    assert_closed(&f, 60.0);
    assert_eq!(correct(&mut f.ledger, &f.journal).unwrap(), false);
    f.reload();
    assert_closed(&f, 60.0);
    assert_eq!(f.journal.repair_money(&mut f.ledger, &pending).unwrap().already_projected, 1);
    assert_eq!(correct(&mut f.ledger, &f.journal).unwrap(), false);
    assert_eq!(f.journal.money_commits().unwrap().len(), 1);
    assert_closed(&f, 60.0);
}

#[tokio::test]
async fn http_smoke_empty_portfolio_closes_but_http_failure_does_not() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (status, body, expected) in [
        ("200 OK", "[]", true),
        ("503 Service Unavailable", "[]", false),
        ("200 OK", "{}", false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(5), async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0); request.extend_from_slice(&chunk[..n]);
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("GET /positions?user=synthetic-wallet&sizeThreshold=0&limit=500&offset=0&includeArchived=true "));
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }).await.unwrap();
        });
        let mut f = Fixture::new();
        f.buy();
        let r = f.redemption(1.0);
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap();
        let snapshot = copybot_hot::positions::fetch(
            &client,
            &base,
            "synthetic-wallet",
            "0",
            "&includeArchived=true",
            r.ts + 1,
        )
        .await;
        server.await.unwrap();
        let result = f
            .ledger
            .settle_redeemed_open("test", "123", &r, &snapshot, false, &f.journal);
        if expected {
            assert!(result.unwrap());
            f.reload();
            assert_closed(&f, 60.0);
        } else {
            assert!(result.is_err());
            assert_eq!(f.ledger.holdings("test")["123"], 100.0);
        }
    }
}
