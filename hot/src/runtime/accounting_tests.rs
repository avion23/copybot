//! Offline runtime accounting and journal crash/replay regressions.
use super::*;
    use copybot_hot::pending::{Pending, PendingLog};
    use copybot_hot::ledger::Ledger;
    use copybot_hot::risk::RiskConfig;
    use copybot_hot::race_send::{Outcome, PathResult};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(1);
    type Fixture = (
        PathBuf, Arc<std::sync::Mutex<Control>>,
        Arc<std::sync::Mutex<PendingLog>>, Arc<Router>,
        Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>, Arc<Emitter>,
        String,
    );
    fn fixture(label: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!(
            "cb-d619-{label}-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let control_path = dir.join("ctl").to_string_lossy().into_owned();
        let ledger = Ledger::new(
            dir.join("ledger.jsonl").to_str().unwrap(),
            &[("alpha".into(), RiskConfig::default())],
        );
        let control = Arc::new(std::sync::Mutex::new(Control::new(
            ledger, dir.join("operator.json").to_str().unwrap(),
        )));
        let pending = Arc::new(std::sync::Mutex::new(PendingLog::open(
            dir.join("pending.jsonl").to_str().unwrap(),
        )));
        pending.lock().unwrap().record(Pending {
            lane: "alpha".into(), token: "tok".into(), side: 0,
            order_hash: "h1".into(), shares: 10.0, limit: 0.6,
            ts: 12, why: "offline_fixture".into(), resting: false,
        }).unwrap();
        let router = Arc::new(Router::new(vec![]));
        let incidents = Arc::new(std::sync::Mutex::new(
            copybot_hot::incidents::Incidents::open(dir.join("incidents.jsonl")),
        ));
        let emitter = Arc::new(Emitter::new(
            dir.join("events.jsonl").to_string_lossy().into_owned(), false,
        ));
        (dir, control, pending, router, incidents, emitter, control_path)
    }
    fn matched(fee: bool) -> String {
        format!(
            r#"{{"status":"matched","success":true,"orderID":"0xh1","takingAmount":"9","makingAmount":"4.05"{}}}"#,
            if fee { r#","feeAmount":"0.015""# } else { "" },
        )
    }
    #[test]
    fn dry_and_shadow_never_open_a_durable_fak_owner_file() {
        let (dir, c, p, r, i, _e, path) = fixture("dry");
        assert!(open_fak_journal_for_live(false, &path, &c, &p, &r, &i).is_none());
        assert!(!dir.join("ctl.fak_commit").exists());
        assert!(!dir.join("ctl.fak_commit.owner.lock").exists());
        assert!(p.lock().unwrap().is_open("h1"));
        assert!(!c.lock().unwrap().ledger.was_order_booked("h1"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn prepared_owner_routes_matched_fak_through_one_journal_then_ledger() {
        let (dir, c, p, r, i, e, path) = fixture("match");
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i)
            .expect("valid empty owner must open");
        let body = matched(true);
        let outcome = Outcome::Matched {
            body: body.clone(), winner: "p".into(), ms: 1.0,
        };
        let results = [PathResult {
            path: "p".into(), http: 200, body, ms: 1.0,
        }];
        let (booked, shares) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e, "h1", "alpha",
            &outcome, &results, 1,
        );
        assert!(booked);
        assert_eq!(shares, 9.0);
        assert!(!p.lock().unwrap().is_open("h1"));
        assert_eq!(journal.commits().unwrap().len(), 1);
        let guard = c.lock().unwrap();
        assert_eq!(guard.ledger.recorded_order("h1").unwrap().fee, 0.015);
        drop(guard);
        // Reuse this existing FAK test to prove heterogeneous, sequential
        // FAK + confirmed-redemption frames share ONE journal hash chain.
        // Both hashes below are offline fixtures, not venue attestations.
        let t = copybot_hot::ledger::now_secs() + 1;
        let source = copybot_hot::settlement::Redemption {
            tx:format!("0x{}", "a".repeat(64)),
            condition_id:format!("0x{}", "b".repeat(64)),
            outcome_index:0, asset:"tok".into(), size:9.0,usdc:9.0,ts:t,
        };
        let zero = copybot_hot::positions::Positions::empty(t+1);
        assert_eq!(with_canonical_money(Some(&journal), &c, &p, |owner|
            owner.redeem("alpha", "tok", &source, &zero, false)
        ).unwrap(),true);
        assert_eq!(journal.commits().unwrap().len(),1);
        assert_eq!(journal.money_commits().unwrap().len(),1);
        assert_eq!(std::fs::read_to_string(journal.path()).unwrap().lines().count(),2);
        drop(journal);
        let reopened=copybot_hot::order_commit::AtomicFakJournal::open(
            dir.join("ctl.fak_commit")).unwrap();
        assert_eq!(reopened.commits().unwrap().len(),1);
        assert_eq!(reopened.money_commits().unwrap().len(),1);
        assert_eq!(reopened.repair_money(&mut c.lock().unwrap().ledger,&p.lock().unwrap())
            .unwrap().already_projected,1);
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn matched_without_fee_latches_result_and_keeps_pending_unchanged() {
        let (dir, c, p, r, i, e, path) = fixture("unknown");
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
        let body = matched(false);
        let outcome = Outcome::Matched {
            body: body.clone(), winner: "p".into(), ms: 1.0,
        };
        let results = [PathResult {
            path: "p".into(), http: 200, body, ms: 1.0,
        }];
        let (booked, shares) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e, "h1", "alpha",
            &outcome, &results, 1,
        );
        assert!(!booked);
        assert_eq!(shares, 0.0);
        assert!(p.lock().unwrap().is_open("h1"));
        assert!(!c.lock().unwrap().ledger.was_order_booked("h1"));
        assert!(journal.commits().unwrap().is_empty());
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }

#[test]
fn absent_sweep_owner_refuses_before_any_order_is_sent() {
    assert!(fak_owner_submit_ready(None).is_err());
}
#[test]
fn sweep_fak_owner_rejects_replaced_journal_before_a_submit() {
    let (dir, c, p, r, i, _e, path) = fixture("replaced-sweep");
    let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i)
        .expect("synthetic FAK owner opens");
    assert!(fak_owner_submit_ready(Some(&journal)).is_ok());
    let origin = journal.path().to_path_buf();
    let displaced = dir.join("displaced.jsonl");
    std::fs::rename(&origin, &displaced).unwrap();
    std::fs::write(&origin, "").unwrap();
    assert!(fak_owner_submit_ready(Some(&journal)).is_err(),
        "stale original writer inode must refuse before sweep POST");
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn synthetic_sweep_sell_uses_exact_fee_and_terminal_journal_projection() {
    let (dir, c, p, r, i, e, path) = fixture("sweep-sell");
    {
        let mut guard = c.lock().unwrap();
        let seed = guard.ledger.book_fill_ex(
            "alpha", "tok", 0, 10.0, 0.4, 0.0, None, "seed", false,
        );
        assert!(seed.is_some(), "test inventory must be durable");
    }
    {
        let mut pend = p.lock().unwrap();
        pend.resolve("h1", "no_fill", None).unwrap();
        pend.record(Pending {
            lane:"alpha".into(), token:"tok".into(), side:1,
            order_hash:"sweep".into(), shares:8.0, limit:0.3,
            ts:22, why:"synthetic_sweep".into(), resting:false,
        }).unwrap();
    }
    let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
    assert!(fak_owner_submit_ready(Some(&journal)).is_ok());
    let body = r#"{"status":"matched","success":true,"orderID":"0xsweep","makingAmount":"8","takingAmount":"3.6","feeAmount":"0.01"}"#;
    let outcome = Outcome::Matched {
        body:body.into(), winner:"sweep_path".into(), ms:2.0,
    };
    let results = [PathResult {
        path:"sweep_path".into(), http:200, body:body.into(), ms:2.0,
    }];
    let (booked, qty) = book_matched_fak_race(
        Some(&journal), &c, &p, &r, &i, &e,
        "sweep", "alpha", &outcome, &results, 1,
    );
    assert!(booked);
    assert_eq!(qty, 8.0);
    assert!(!p.lock().unwrap().is_open("sweep"));
    {
        let guard = c.lock().unwrap();
        let saved = guard.ledger.recorded_order("sweep").unwrap();
        assert_eq!(saved.receipt.price, 0.45);
        assert_eq!(saved.fee, 0.01);
        assert_eq!(guard.ledger.holdings("alpha")["tok"], 2.0);
    }
    assert_eq!(journal.commits().unwrap().len(), 1);
    drop(journal);
    // Simulate a new process replaying already booked money after all writes.
    let reopened = copybot_hot::order_commit::AtomicFakJournal::open(
        dir.join("ctl.fak_commit"),
    ).unwrap();
    {
        let mut ctl = c.lock().unwrap();
        let mut pend = p.lock().unwrap();
        let counts = reopened.repair_legacy(&mut ctl.ledger, &mut pend).unwrap();
        assert_eq!(counts.fills_booked, 0);
        assert_eq!(counts.pending_closed, 0);
        assert_eq!(ctl.ledger.holdings("alpha")["tok"], 2.0);
    }
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn direct_merge_and_flatten_sell_sources_commit_identical_proven_money() {
    // These two production callers now share this one witnessed 2xx journal
    // edge. No network socket or signing key is involved in either fixture.
    for source in ["merge_residual", "flatten_direct", "orphan_exit"] {
        let (dir, c, p, r, i, e, path) = fixture(source);
        let _ = c.lock().unwrap().ledger.book_fill_ex(
            "alpha", "tok", 0, 10.0, 0.40, 0.0, None, "seed", false,
        ).expect("seeded inventory");
        let hash = format!("{source}-hash");
        {
            let mut pending = p.lock().unwrap();
            pending.resolve("h1", "test_closed", None).unwrap();
            pending.reserve_for_submit(Pending {
                lane:"alpha".into(), token:"tok".into(), side:1,
                order_hash:hash.clone(), shares:8.0, limit:0.3,
                ts:23, why:source.into(), resting:false,
            }, false).unwrap();
        }
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i)
            .expect("private journal");
        assert!(fak_owner_submit_ready(Some(&journal)).is_ok());
        let body = format!(
            r#"{{"status":"matched","success":true,"orderID":"0x{hash}","makingAmount":"8","takingAmount":"3.6","feeAmount":"0.01"}}"#
        );
        let outcome = Outcome::Matched {
            body:body.clone(), winner:source.into(), ms:2.0,
        };
        let results = [PathResult {
            path:source.into(), http:200, body, ms:2.0,
        }];
        let (booked, qty) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e,
            &hash, "alpha", &outcome, &results, 1,
        );
        assert!(booked, "{source} must book only proved money");
        assert_eq!(qty, 8.0);
        assert!(!p.lock().unwrap().is_open(&hash));
        assert_eq!(journal.commits().unwrap().len(), 1);
        {
            let ledger = c.lock().unwrap();
            let receipt = ledger.ledger.recorded_order(&hash).unwrap();
            assert_eq!(receipt.receipt.price, 0.45);
            assert_eq!(receipt.fee, 0.01);
            assert_eq!(ledger.ledger.holdings("alpha")["tok"], 2.0);
        }
        drop(journal);
        let reopened = copybot_hot::order_commit::AtomicFakJournal::open(
            dir.join("ctl.fak_commit")
        ).unwrap();
        let mut ledger = c.lock().unwrap();
        let mut pend = p.lock().unwrap();
        let counts = reopened.repair_legacy(&mut ledger.ledger, &mut pend).unwrap();
        assert_eq!(counts.fills_booked, 0);
        assert_eq!(counts.pending_closed, 0);
        drop(pend);
        drop(ledger);
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[test]
fn direct_merge_and_flatten_unknown_fee_refuse_without_legacy_fallback() {
    for source in ["merge_missing_fee", "flatten_missing_fee", "orphan_missing_fee"] {
        let (dir, c, p, r, i, e, path) = fixture(source);
        let _ = c.lock().unwrap().ledger.book_fill_ex(
            "alpha", "tok", 0, 10.0, 0.40, 0.0, None, "seed", false,
        ).unwrap();
        let hash = format!("{source}-hash");
        {
            let mut pending = p.lock().unwrap();
            pending.resolve("h1", "test_closed", None).unwrap();
            pending.reserve_for_submit(Pending {
                lane:"alpha".into(), token:"tok".into(), side:1,
                order_hash:hash.clone(), shares:8.0, limit:0.3,
                ts:24, why:source.into(), resting:false,
            }, false).unwrap();
        }
        let journal = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
        let body = format!(
            r#"{{"status":"matched","success":true,"orderID":"0x{hash}","makingAmount":"8","takingAmount":"3.6"}}"#
        );
        let outcome = Outcome::Matched { body:body.clone(), winner:source.into(), ms:2.0 };
        let results = [PathResult { path:source.into(), http:200, body, ms:2.0 }];
        let (booked, qty) = book_matched_fak_race(
            Some(&journal), &c, &p, &r, &i, &e,
            &hash, "alpha", &outcome, &results, 1,
        );
        assert!(!booked);
        assert_eq!(qty, 0.0);
        assert!(p.lock().unwrap().is_open(&hash),
            "unknown fees cannot release the signed reservation");
        assert!(!c.lock().unwrap().ledger.was_order_booked(&hash));
        assert_eq!(journal.commits().unwrap().len(), 0);
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn unproved_gtc_match_never_books_or_releases_reservation() {
    let (dir,c,p,r,i,e,_path) = fixture("gtc-defer");
    {
        let mut pend = p.lock().unwrap();
        pend.resolve("h1","no_fill",None).unwrap();
        pend.record(Pending {
            lane:"alpha".into(), token:"rest-tok".into(), side:0,
            order_hash:"gtc".into(), shares:8.0, limit:0.6,
            ts:12, why:"resting".into(), resting:true,
        }).unwrap();
    }
    let (booked, shares) = refuse_unproved_resting_money(
        &p,&r,&i,&e,"alpha","gtc","no exact charged fee",
    );
    assert!(!booked);
    assert_eq!(shares, 0.0);
    assert!(p.lock().unwrap().is_open("gtc"));
    assert!(!c.lock().unwrap().ledger.was_order_booked("gtc"));
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn one_owned_atomic_merge_pair_row_survives_reopen_without_split_leg() {
    let (dir,c,p,r,i,_e,path) = fixture("merge-atomic");
    let mut ledger=c.lock().unwrap();
    assert!(ledger.ledger.book_fill_ex(
        "alpha","yes",0,5.0,0.40,0.0,None,"seed-yes",false,
    ).is_some());
    assert!(ledger.ledger.book_fill_ex(
        "alpha","no",0,5.0,0.55,0.0,None,"seed-no",false,
    ).is_some());
    drop(ledger);
    let lease=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    let result=with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.merge_pair("alpha","yes","no",5.0,0.4,0.6,"onchain-merge-tx")
    ).expect("atomic merge pair fsync");
    assert_eq!(result.0.shares,5.0);
    assert_eq!(result.1.shares,5.0);
    assert!(!c.lock().unwrap().ledger.was_order_booked("onchain-merge-tx:a"));
    assert!(!c.lock().unwrap().ledger.was_order_booked("onchain-merge-tx:b"));
    assert!(with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.merge_pair("alpha","yes","no",5.0,0.4,0.6,"onchain-merge-tx")
    ).is_err(), "same tx cannot book a second pair");
    let raw=std::fs::read_to_string(dir.join("ledger.jsonl")).unwrap();
    assert_eq!(raw.lines().filter(|line|
        line.contains(r#""key":"atomic_merge:alpha:onchain-merge-tx""#)
    ).count(),1);
    drop(lease);
    let replay=Ledger::new(dir.join("ledger.jsonl").to_str().unwrap(),
        &[("alpha".into(),RiskConfig::default())]);
    assert_eq!(replay.holdings("alpha").get("yes").copied().unwrap_or(0.0),0.0);
    assert_eq!(replay.holdings("alpha").get("no").copied().unwrap_or(0.0),0.0);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn redeemed_payout_requires_lease_no_pending_and_one_keyed_fsync() {
    use copybot_hot::settlement::Redemption;
    use copybot_hot::positions::Positions;
    let (dir,c,p,r,i,_e,path)=fixture("redemption");
    let _=c.lock().unwrap().ledger.book_fill_ex(
        "alpha","tok",0,10.0,0.4,0.0,None,"seed",false,
    ).unwrap();
    let t=copybot_hot::ledger::now_secs()+1;
    let redemption=Redemption{
        tx:format!("0x{}", "a".repeat(64)),
        condition_id:format!("0x{}", "b".repeat(64)),
        outcome_index:0,asset:"tok".into(),size:10.0,usdc:10.0,ts:t,
    };
    let positions=Positions::empty(t+1);
    let lease=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    assert!(with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.redeem("alpha","tok",&redemption,&positions,false)
    ).is_err(), "signed pending row cannot be settled beneath admission");
    p.lock().unwrap().resolve("h1","no_fill",None).unwrap();
    assert!(with_canonical_money(None,&c,&p,|owner|
        owner.redeem("alpha","tok",&redemption,&positions,false)
    ).is_err(), "no owner must never book settlement");
    assert_eq!(with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.redeem("alpha","tok",&redemption,&positions,false)
    ).unwrap(),true);
    assert_eq!(lease.money_commits().unwrap().len(),1);
    assert_eq!(with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.redeem("alpha","tok",&redemption,&positions,false)
    ).unwrap(),false);
    assert!(c.lock().unwrap().ledger.holdings("alpha").get("tok").is_none());
    drop(lease);
    let replay=Ledger::new(dir.join("ledger.jsonl").to_str().unwrap(),
        &[("alpha".into(),RiskConfig::default())]);
    assert_eq!(replay.holdings("alpha").get("tok").copied().unwrap_or(0.0),0.0);
    assert_eq!(replay.lanes["alpha"].risk.realised_pnl,6.0);
    let lease=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    assert_eq!(lease.money_commits().unwrap().len(),1);
    assert_eq!(lease.repair_money(&mut c.lock().unwrap().ledger,&p.lock().unwrap())
        .unwrap().already_projected,1);
    drop(lease);
    std::fs::remove_dir_all(dir).unwrap();

    // Same existing regression also exercises a crash after journal fsync
    // and before Ledger append. Boot must project that exact source once.
    let (dir,c,p,r,i,_e,path)=fixture("redemption-crash");
    p.lock().unwrap().resolve("h1","no_fill",None).unwrap();
    assert!(c.lock().unwrap().ledger.book_fill_ex(
        "alpha","tok",0,10.0,0.4,0.0,None,"seed",false,
    ).is_some());
    let t=copybot_hot::ledger::now_secs()+1;
    let source=Redemption{ts:t,..redemption};
    let fact=copybot_hot::order_commit::MoneyCommit::Redeemed {
        lane:"alpha".into(),token:"tok".into(),redemption:source,
        shares:10.0,basis_cost:4.0,payout:1.0,proceeds:10.0,
        zero_snapshot_as_of:t+1,ledger_t:t,
    };
    let journal=copybot_hot::order_commit::AtomicFakJournal::open(
        dir.join("ctl.fak_commit")).unwrap();
    journal.append_money(fact).unwrap();
    drop(journal);
    let recovered=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    assert_eq!(c.lock().unwrap().ledger.lanes["alpha"].risk.realised_pnl,6.0);
    assert_eq!(recovered.repair_money(&mut c.lock().unwrap().ledger,&p.lock().unwrap())
        .unwrap().already_projected,1);
    drop(recovered);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn neutral_custody_adjustment_needs_canonical_lease_and_no_pending() {
    let (dir,c,p,r,i,_e,path)=fixture("neutral");
    let lease=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    assert!(with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.neutral_reconcile("alpha","tok",0,1.0,0.4)
    ).is_err(), "unresolved pending blocks independent custody adjustment");
    p.lock().unwrap().resolve("h1","no_fill",None).unwrap();
    let refusal=with_canonical_money(Some(&lease),&c,&p,|owner|
        owner.neutral_reconcile("alpha","tok",0,1.0,0.4)
    ).unwrap_err();
    assert!(refusal.contains("transactionHash") && refusal.contains("logIndex"));
    assert!(c.lock().unwrap().ledger.holdings("alpha").get("tok").is_none());
    assert!(lease.money_commits().unwrap().is_empty());
    assert!(with_canonical_money(None,&c,&p,|owner|
        owner.neutral_reconcile("alpha","tok",1,1.0,0.4)
    ).is_err(), "missing lease cannot release inventory");
    drop(lease);
    let replay=Ledger::new(dir.join("ledger.jsonl").to_str().unwrap(),
        &[("alpha".into(),RiskConfig::default())]);
    assert!(replay.holdings("alpha").get("tok").is_none());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn boot_never_closes_legacy_ledger_booked_pending_without_owner_fact() {
    let (dir,c,p,r,i,e,path) = fixture("legacy-boot");
    let booked = c.lock().unwrap().ledger.book_fill_ex(
        "alpha","tok",0,9.0,0.45,0.0,None,"h1",false,
    ).unwrap();
    assert_eq!(booked.shares,9.0);
    let journal=open_fak_journal_for_live(true,&path,&c,&p,&r,&i).unwrap();
    assert!(journal.commits().unwrap().is_empty());
    audit_unowned_boot_receipts(Some(&journal),&c,&p,&r,&i,&e);
    assert!(p.lock().unwrap().is_open("h1"),
        "unproved old fee=0 ledger receipt must not release original pending");
    assert!(c.lock().unwrap().ledger.was_order_booked("h1"));
    drop(journal);
    std::fs::remove_dir_all(dir).unwrap();
}
    #[test]
    fn journal_only_crash_is_recovered_at_boot_without_network_or_post() {
        let (dir, c, p, r, i, _e, path) = fixture("restart");
        let journal = copybot_hot::order_commit::AtomicFakJournal::open(
            dir.join("ctl.fak_commit"),
        ).unwrap();
        let fact = copybot_hot::venue_fak::parse_matched_fak(
            &p.lock().unwrap().open_rows()[0], &matched(true),
        ).unwrap();
        journal.append(fact).unwrap();
        drop(journal);
        let recovered = open_fak_journal_for_live(true, &path, &c, &p, &r, &i).unwrap();
        assert_eq!(recovered.commits().unwrap().len(), 1);
        assert!(!p.lock().unwrap().is_open("h1"));
        assert_eq!(c.lock().unwrap().ledger.recorded_order("h1").unwrap().fee, 0.015);
        drop(recovered);
        std::fs::remove_dir_all(dir).unwrap();
    }
