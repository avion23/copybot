//! Existing lane decision regression cases and shared offline fixtures.

    impl Router {
        fn decide1(&self, ix: usize, d: &Decoded) -> Result<Intent, Skip> {
            self.decide(
                ix,
                d,
                Progress {
                    his_filled: d.fill_size,
                    our_copied: 0.0,
                },
            )
        }
    }
    use super::*;
    use crate::calldata::Decoded;
    fn w(b: u8) -> [u8; 20] {
        [b; 20]
    }
    fn ready_lane(c: LaneConfig) -> Lane {
        let l = Lane::new(c);
        l.mark_ready();
        l
    }
    fn tick_settled(l: &Lane, token: &str, held: f64) {
        l.set_holding(token, held);
        l.set_sell_reservations(HashMap::new());
    }
    fn cfg(name: &str, wb: u8, sizing: Sizing) -> LaneConfig {
        LaneConfig {
            name: name.into(),
            wallet20: w(wb),
            sizing,
            execution: Execution::Taker,
            buy_slippage_c: 0.02,
            sell_slippage_c: 0.02,
            copy_maker_sells: false,
            sell_floor_frac: 0.5,
            min_order_usd: 1.0,
            max_usd_per_fill: 250.0,
            daily_budget_usd: 500.0,
            per_market_usd: 200.0,
            max_open_usd: 1000.0,
            max_buy_price: 0.95,
            min_buy_price: 0.02,
            min_fill_floor: true,
            sell_all_frac: 0.95,
            max_effective_pct: 0.05,
            copy_makers: false,
            compound: true,
            exclude_political: false,
        }
    }
    fn dec(tok: &str, side: u8, price: f64, fill: f64, order: f64) -> Decoded {
        Decoded {
            condition_id: [7u8; 32],
            token_id: tok.into(),
            side,
            price,
            order_size: order,
            fill_size: fill,
            role: "maker",
            salt: [9u8; 32],
            occurrence: 0,
        }
    }
    fn router2() -> Router {
        let r = Router::new(
            vec![
                ready_lane(cfg("example_lane_26", 1, Sizing::Shares(5.0))), ready_lane(cfg("other",
                2, Sizing::Pct(0.002))),
            ],
        );
        for l in r.snapshot().iter() {
            l.state.armed.store(true, Ordering::Relaxed);
        }
        r
    }
    fn pct_lane(p: f64) -> Router {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(p));
        c.daily_budget_usd = 100_000.0;
        c.per_market_usd = 50_000.0;
        c.max_open_usd = 200_000.0;
        c.max_usd_per_fill = 25_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        for l in r.snapshot().iter() {
            l.state.armed.store(true, Ordering::Relaxed);
        }
        r
    }
    fn replay_tranches(
        r: &Router,
        n: usize,
        order_shares: f64,
        price: f64,
    ) -> (f64, usize) {
        let mut his_cum = 0.0;
        let mut copied = 0.0;
        let mut clips = 0;
        for _ in 0..n {
            his_cum += order_shares / n as f64;
            let d = dec("100000001", 0, price, order_shares / n as f64, order_shares);
            let progress = Progress {
                his_filled: his_cum,
                our_copied: copied,
            };
            if let Ok(i) = r.decide(0, &d, progress) {
                copied += i.shares;
                clips += 1;
            }
        }
        (copied, clips)
    }
    #[test]
    fn DEFAULT_sell_policy_mirrors_a_partial_exit_and_flattens_a_full_exit() {
        assert_eq!(crate::config::RUNTIME_SELL_ALL_FRAC, 1.0);
        for frac in [0.0001_f64, 0.01, 0.1, 0.3, 0.5, 0.9, 0.95] {
            assert!(
                frac < crate::config::RUNTIME_SELL_ALL_FRAC,
                "his {frac} partial exit must remain proportional by default"
            );
        }
        assert!(1.0 >= crate::config::RUNTIME_SELL_ALL_FRAC);
        assert_eq!(crate::config::SELL_ALL_ON_ANY_SELL, 0.0);
    }
    #[test]
    fn a_lane_may_still_OPT_OUT_of_flattening() {
        let thresh = 0.5_f64;
        assert!(! (0.3 >= thresh), "a 30% trim stays proportional on an opted-out lane");
        assert!(0.6 >= thresh, "a 60% exit still flattens it");
    }
    #[test]
    fn CUMULATIVE_targeting_does_NOT_raise_any_CEILING() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.05));
        c.max_usd_per_fill = 416.70;
        c.per_market_usd = 6_000.0;
        c.max_open_usd = 8_500.0;
        c.daily_budget_usd = 7_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        for l in r.snapshot().iter() {
            l.state.armed.store(true, Ordering::Relaxed);
        }
        let d = dec("100000001", 0, 0.50, 500_000.0, 500_000.0);
        let i = r
            .decide(
                0,
                &d,
                Progress {
                    his_filled: 500_000.0,
                    our_copied: 0.0,
                },
            )
            .expect("should fire, clipped");
        let usd = i.shares * i.limit;
        assert!(
            usd <= 416.70 + 1e-6,
            "one clip committed ${usd:.2}, above the ${:.2} per-fill cap", 416.70
        );
        let i2 = r
            .decide(
                0,
                &d,
                Progress {
                    his_filled: 500_000.0,
                    our_copied: i.shares,
                },
            );
        if let Ok(i2) = i2 {
            assert!(
                i2.shares * i2.limit <= 416.70 + 1e-6, "a top-up must obey the cap too"
            );
        }
    }
    #[test]
    fn boot_ownership_restore_REPORTS_a_conflict_instead_of_dropping_it() {
        let r = router2();
        let (restored, conflicts) = r
            .restore_ownership(
                &[
                    (0, "example_lane_26".into(), vec!["TOK_A".into(), "TOK_B".into()]),
                    (1, "other".into(), vec!["TOK_B".into(), "TOK_C".into()]),
                ],
            );
        assert_eq!(restored, 3, "three distinct tokens are claimable");
        assert_eq!(conflicts.len(), 1);
        let (tok, owner, loser) = &conflicts[0];
        assert_eq!(
            (tok.as_str(), owner.as_str(), loser.as_str()), ("TOK_B", "example_lane_26", "other"),
            "the conflict must name BOTH sides so an operator can attribute it"
        );
    }
    #[test]
    fn a_clean_boot_reports_NO_conflicts() {
        let r = router2();
        let (restored, conflicts) = r
            .restore_ownership(
                &[
                    (0, "example_lane_26".into(), vec!["TOK_A".into()]),
                    (1, "other".into(), vec!["TOK_B".into()]),
                ],
            );
        assert_eq!((restored, conflicts.len()), (2, 0));
    }
    #[test]
    fn the_LOSING_lane_of_a_conflict_STOPS_BUYING_BUT_CAN_STILL_SELL() {
        let r = router2();
        for l in r.snapshot().iter() {
            l.state.armed.store(true, Ordering::Relaxed);
        }
        let (_, conflicts) = r
            .restore_ownership(
                &[
                    (0, "example_lane_26".into(), vec!["100000001".into()]),
                    (1, "other".into(), vec!["100000001".into()]),
                ],
            );
        assert_eq!(conflicts.len(), 1, "the conflict must still be REPORTED");
        r.lane(1).unwrap().set_holding("100000001", 50.0);
        r.lane(1).unwrap().set_his("100000001", 100.0);
        assert_eq!(
            r.decide1(1, & dec("100000001", 0, 0.60, 100.0, 100.0)),
            Err(Skip::TokenOwnedByOtherLane),
            "the loser must not OPEN more of a token whose ownership is disputed"
        );
        let exit = r
            .decide1(1, &dec("100000001", 1, 0.60, 100.0, 100.0))
            .expect("the losing lane MUST be able to follow its leader out");
        assert_eq!(exit.side, 1);
        assert!(
            (exit.shares - 50.0).abs() < 1e-9, "sized from ITS OWN holding, got {}", exit
            .shares
        );
        assert_eq!(
            r.owner_of("100000001"), Some(0),
            "exiting must not transfer the disputed claim"
        );
    }
    #[test]
    fn the_120_TRANCHE_ORDER_no_longer_buys_1560_SHARES() {
        let r = pct_lane(0.015);
        let (copied, clips) = replay_tranches(&r, 120, 5_000.0, 0.08);
        assert!(
            copied < 100.0,
            "bought {copied} shares across {clips} clips — the runaway is back"
        );
        assert!((copied - 75.0).abs() <= 15.0, "expected ~75 shares, got {copied}");
    }
    #[test]
    fn a_SINGLE_TRANCHE_order_is_sized_exactly_as_before() {
        let r = pct_lane(0.015);
        let d = dec("100000001", 0, 0.50, 5_000.0, 5_000.0);
        let one = r.decide1(0, &d).unwrap();
        let with_progress = pct_lane(0.015)
            .decide(
                0,
                &d,
                Progress {
                    his_filled: 5_000.0,
                    our_copied: 0.0,
                },
            )
            .unwrap();
        assert_eq!(one.shares, with_progress.shares);
    }
    #[test]
    fn a_TINY_FIRST_TRANCHE_no_longer_sizes_the_whole_copy() {
        let r = pct_lane(0.015);
        let (copied, _) = replay_tranches(&r, 120, 5_000.0, 0.50);
        assert!(
            copied > 60.0,
            "only copied {copied} shares of a 75-share target — still under-copying"
        );
    }
    #[test]
    fn the_VENUE_FLOOR_lifts_only_the_FIRST_clip_of_an_order() {
        let r = pct_lane(0.015);
        let d = dec("100000001", 0, 0.08, 1.0, 5_000.0);
        let first = r
            .decide(
                0,
                &d,
                Progress {
                    his_filled: 1.0,
                    our_copied: 0.0,
                },
            )
            .expect("the first clip may be lifted to the venue minimum");
        assert_eq!(
            first.shares, 10.0, "the first clip is lifted to the $1 venue minimum"
        );
        assert!(
            matches!(r.decide(0, & d, Progress { his_filled : 2.0, our_copied : first
            .shares }), Err(Skip::DustAfterSizing) | Err(Skip::BelowVenueMinimum)),
            "a top-up below the venue minimum must wait for more of his order"
        );
    }
    #[test]
    fn a_target_ALREADY_MET_stops_buying() {
        let r = pct_lane(0.015);
        let d = dec("100000001", 0, 0.50, 100.0, 5_000.0);
        assert!(
            matches!(r.decide(0, & d, Progress { his_filled : 5_000.0, our_copied :
            10_000.0 }), Err(Skip::DustAfterSizing)),
            "we must never buy more of his order than the target"
        );
    }
    #[test]
    fn ONE_signal_never_sizes_beyond_pct_times_his_CONFIRMED_fill() {
        let r = pct_lane(0.015);
        for tranche in [1.16, 500.0, 2587.0] {
            let Ok(i) = r
                .decide1(
                    0,
                    &dec(&format!("1000000{}", tranche as u64), 0, 0.50, tranche, 5000.0),
                ) else { continue };
            let usd = i.usd as f64 / MICRO;
            let intended = tranche * 0.50 * 0.015;
            let ceiling = intended.max(1.0) + 0.52;
            assert!(
                usd <= ceiling,
                "${usd} copied unfilled size from a {tranche}-share tranche (safe ceiling ${ceiling})"
            );
        }
    }
    #[test]
    fn an_exit_sells_a_fraction_of_WHAT_WE_HOLD_not_of_our_intended_target() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_holding("100000001", 225.0);
        r.lane(0).unwrap().set_his("100000001", 30_000.0);
        let i = r.decide1(0, &dec("100000001", 1, 0.60, 3000.0, 30_000.0)).unwrap();
        assert_eq!(i.side, 1);
        assert!(
            (i.shares - 22.5).abs() < 0.6,
            "sold {} on a 225 holding for a 10% exit — must be ~22.5", i.shares
        );
    }
    #[test]
    fn an_exit_can_NEVER_exceed_what_we_hold() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_holding("100000001", 225.0);
        r.lane(0).unwrap().set_his("100000001", 3000.0);
        let i = r.decide1(0, &dec("100000001", 1, 0.60, 3000.0, 3000.0)).unwrap();
        assert!(i.shares <= 225.0 + 1e-9, "tried to sell {} of 225 held", i.shares);
    }
    #[test]
    fn a_REAL_full_exit_closes_us_completely_via_sell_all_frac() {
        let r = pct_lane(0.015);
        let mut his = 10_000.0;
        let mut held = 100.0;
        for sell in [2_500.0, 2_500.0, 2_500.0, 2_500.0] {
            r.lane(0).unwrap().set_his("100000001", his);
            tick_settled(&r.lane(0).unwrap(), "100000001", held);
            if let Ok(i) = r.decide1(0, &dec("100000001", 1, 0.60, sell, his)) {
                held = (held - i.shares).max(0.0);
            }
            his -= sell;
            if his <= 1e-9 {
                break;
            }
        }
        assert!(held <= 1.0, "a completed exit left {held} shares behind");
    }
    #[test]
    fn KNOWN_LIMIT_a_geometric_exit_that_never_completes_strands_a_tail() {
        let r = pct_lane(0.015);
        let mut his = 10_000.0;
        let mut held = 100.0;
        for _ in 0..8 {
            r.lane(0).unwrap().set_his("100000001", his);
            tick_settled(&r.lane(0).unwrap(), "100000001", held);
            let sell = his * 0.25;
            if let Ok(i) = r.decide1(0, &dec("100000001", 1, 0.60, sell, his)) {
                held = (held - i.shares).max(0.0);
            }
            his -= sell;
        }
        assert!(
            held > 1.0,
            "if this now converges the dust sweep may be redundant — re-check it"
        );
        assert!(held < 20.0, "decay should still be substantial, got {held}");
    }
    #[test]
    fn a_sub_dollar_FIRST_clip_is_LIFTED_to_the_venue_floor_not_skipped() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.10));
        c.min_fill_floor = true;
        let r = Router::new(vec![ready_lane(c)]);
        r.snapshot()[0].state.armed.store(true, Ordering::Relaxed);
        let d = dec("T", 0, 0.11, 90.0, 90.0);
        let i = r
            .decide(
                0,
                &d,
                Progress {
                    his_filled: 90.0,
                    our_copied: 0.0,
                },
            )
            .expect("⛔ THE BUG: this returned BelowVenueMinimum and cost the trade");
        let usd = i.shares as f64 * i.limit;
        assert!(usd >= 1.0, "must be lifted to at least the venue floor, got ${usd:.4}");
        assert!(
            i.shares >= crate ::venue::VENUE_MIN_SHARES,
            "and the lift must clear the SHARE floor too, got {} shares", i.shares
        );
    }
    #[test]
    fn above_20_CENTS_a_sub_dollar_clip_is_SKIPPED_because_the_lift_cannot_be_legal() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.10));
        c.min_fill_floor = true;
        let r = Router::new(vec![ready_lane(c)]);
        r.snapshot()[0].state.armed.store(true, Ordering::Relaxed);
        let d = dec("T", 0, 0.73, 13.0, 13.0);
        assert_eq!(
            r.decide(0, & d, Progress { his_filled : 13.0, our_copied : 0.0 }),
            Err(Skip::BelowVenueMinimum),
            "the $1 lift lands at 2 shares here — the venue would refuse it"
        );
    }
    #[test]
    fn with_the_floor_OFF_the_same_clip_is_still_SKIPPED() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.10));
        c.min_fill_floor = false;
        let r = Router::new(vec![ready_lane(c)]);
        r.snapshot()[0].state.armed.store(true, Ordering::Relaxed);
        let d = dec("T", 0, 0.73, 13.0, 13.0);
        assert!(
            matches!(r.decide(0, & d, Progress { his_filled : 13.0, our_copied : 0.0 }),
            Err(Skip::BelowVenueMinimum) | Err(Skip::DustAfterSizing))
        );
    }
    #[test]
    fn the_lift_is_FIRST_CLIP_ONLY_so_a_collapsing_ladder_cannot_repeat_it() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.10));
        c.min_fill_floor = true;
        let r = Router::new(vec![ready_lane(c)]);
        r.snapshot()[0].state.armed.store(true, Ordering::Relaxed);
        let d = dec("T", 0, 0.08, 13.0, 130.0);
        assert!(
            r.decide(0, & d, Progress { his_filled : 13.0, our_copied : 5.0 }).is_err(),
            "a later tranche must NOT be lifted — that is the ladder explosion"
        );
    }
    #[test]
    fn a_clip_under_the_VENUE_SHARE_MINIMUM_is_never_sent() {
        let mut c = cfg("vmin", 61, Sizing::Pct(0.0002));
        c.min_fill_floor = false;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let d = dec("7000001", 0, 0.69, 10_000.0, 10_000.0);
        assert_eq!(
            r.decide1(0, & d), Err(Skip::BelowVenueMinimum),
            "a sub-5-share clip must be refused HERE, not by the venue"
        );
    }
    #[test]
    fn CHEAP_markets_still_trade_because_the_dollar_lift_already_clears_five_shares() {
        let mut c = cfg("vcheap", 62, Sizing::Pct(0.0002));
        c.min_fill_floor = true;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        for (px, tok) in [(0.05, "7000021"), (0.10, "7000022"), (0.20, "7000023")] {
            let i = r
                .decide1(0, &dec(tok, 0, px, 10_000.0, 10_000.0))
                .unwrap_or_else(|e| {
                    panic!("cheap market at {px} must still trade: {e:?}")
                });
            assert!(
                i.shares >= crate ::venue::VENUE_MIN_SHARES,
                "at {px} the dollar lift gave {} shares — under the venue's 5", i
                .shares
            );
            assert!(i.shares * i.limit >= 1.0 - 1e-9, "and still clears the $1 floor");
        }
    }
    #[test]
    fn a_PER_FILL_CAP_that_lands_under_the_floor_SKIPS_instead_of_bouncing() {
        let mut c = cfg("vcap", 63, Sizing::Pct(0.5));
        c.min_fill_floor = true;
        c.max_usd_per_fill = 2.00;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("7000003", 0, 0.90, 1_000.0, 1_000.0)),
            Err(Skip::BelowVenueMinimum),
            "the cap clamped under the venue floor — skip, never bounce"
        );
    }
    #[test]
    fn the_share_floor_NEVER_gates_an_EXIT() {
        let mut c = cfg("vexit", 64, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "7000004";
        r.lane(0).unwrap().set_holding(tok, 2.0);
        r.lane(0).unwrap().set_his(tok, 100.0);
        let i = r
            .decide1(0, &dec(tok, 1, 0.50, 100.0, 100.0))
            .expect("a 2-share position must still be exitable");
        assert_eq!(i.side, 1);
        assert!(
            i.shares > 0.0 && i.shares <= 2.0, "sized from our holding: {}", i.shares
        );
    }
    #[test]
    fn the_MIN_ORDER_FLOOR_over_copies_small_cheap_fills() {
        let r = pct_lane(0.015);
        let i = r.decide1(0, &dec("100000001", 0, 0.20, 100.0, 100.0)).unwrap();
        let requested = 100.0 * 0.015;
        assert!(
            i.shares > requested,
            "floor should LIFT a sub-$1 order, got {} for a requested {}", i.shares,
            requested
        );
        assert!(i.usd as f64 / MICRO >= 1.0 - 1e-6, "must clear the venue $1 minimum");
    }
    #[test]
    fn whole_share_truncation_biases_pct_DOWNWARD_never_up() {
        let r = pct_lane(0.015);
        for fill in [401.0, 667.0, 1001.0, 2003.0] {
            let i = r.decide1(0, &dec("100000001", 0, 0.50, fill, fill)).unwrap();
            assert!(
                i.shares <= fill * 0.015 + 1e-9,
                "{} shares exceeds exact pct {} for fill {}", i.shares, fill * 0.015,
                fill
            );
        }
    }
    #[test]
    fn the_PER_MARKET_cap_truncates_his_biggest_conviction_bets_first() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.015));
        c.per_market_usd = 300.0;
        c.max_usd_per_fill = 300.0;
        c.daily_budget_usd = 100_000.0;
        c.max_open_usd = 100_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let mut total = 0.0;
        for _ in 0..10 {
            match r.decide1(0, &dec("100000001", 0, 0.80, 10_000.0, 100_000.0)) {
                Ok(i) => total += i.usd as f64 / MICRO,
                Err(Skip::PerMarketCap) => break,
                Err(e) => panic!("unexpected {e:?}"),
            }
        }
        assert!(total <= 300.0 + 1e-6, "per-market cap did not bind, got ${total}");
        assert!(
            total < 1500.0,
            "we wanted ~$1,200 of this market and the cap allowed ${total}"
        );
    }
    #[test]
    fn a_pct_lane_still_refuses_to_sell_what_it_never_bought() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_his("100000001", 5000.0);
        assert_eq!(
            r.decide1(0, & dec("100000001", 1, 0.60, 500.0, 5000.0)).unwrap_err(),
            Skip::SellNoPosition
        );
    }
    #[test]
    fn a_sale_that_fits_inside_his_LEGACY_moves_none_of_ours() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_holding("100000001", 10.0);
        r.lane(0).unwrap().set_his("100000001", 1100.0);
        r.lane(0).unwrap().set_legacy("100000001", 1000.0);
        assert_eq!(
            r.decide1(0, & dec("100000001", 1, 0.60, 550.0, 1100.0)).unwrap_err(),
            Skip::SellNoPosition,
            "a sale wholly inside his legacy must not move our position"
        );
    }
    #[test]
    fn a_sale_PAST_his_legacy_mirrors_only_the_matched_excess() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_holding("100000001", 10.0);
        r.lane(0).unwrap().set_his("100000001", 1100.0);
        r.lane(0).unwrap().set_legacy("100000001", 1000.0);
        let i = r.decide1(0, &dec("100000001", 1, 0.60, 1050.0, 1100.0)).unwrap();
        assert!(
            i.shares > 0.0 && i.shares <= 10.0 + 1e-9,
            "expected a partial mirror of our 10 shares, got {}", i.shares
        );
    }
    #[test]
    fn RED_a_tiny_first_tranche_does_not_copy_the_unfilled_remainder() {
        let r = pct_lane(0.015);
        match r.decide1(0, &dec("100000001", 0, 0.50, 1.16, 5000.0)) {
            Err(Skip::BelowVenueMinimum) => {}
            Ok(i) => {
                let usd = i.usd as f64 / MICRO;
                assert!(
                    usd <= 1.52,
                    "a 1.16-share fill incorrectly copied the unfilled 5,000-share order: ${usd}"
                );
            }
            Err(e) => panic!("unexpected refusal: {e:?}"),
        }
    }
    #[test]
    fn RED_his_most_extreme_entries_are_refused_by_the_PRICE_BAND_before_sizing() {
        let r = pct_lane(0.015);
        assert_eq!(
            r.decide1(0, & dec("100000001", 0, 0.994, 150_000.0, 150_000.0))
            .unwrap_err(), Skip::PriceOutOfBand
        );
    }
    #[test]
    fn RED_a_huge_IN_BAND_order_is_clamped_and_the_clamp_binds() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.015));
        c.max_usd_per_fill = 250.0;
        c.per_market_usd = 300.0;
        c.max_open_usd = 100_000.0;
        c.daily_budget_usd = 100_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("100000001", 0, 0.90, 150_000.0, 150_000.0)).unwrap();
        let usd = i.usd as f64 / MICRO;
        assert!(usd <= 250.0 + 1e-6, "clamp failed on a huge in-band order: ${usd}");
        assert!(i.shares > 0.0, "clamping must not produce a zero-share order");
    }
    #[test]
    fn RED_the_venue_floor_can_only_ever_LIFT_never_drop_below_a_dollar() {
        let r = pct_lane(0.015);
        for (price, order) in [(0.02, 100.0), (0.07, 200.0), (0.08, 50.0)] {
            let i = r
                .decide1(
                    0,
                    &dec(
                        &format!("tok{}", (price * 1000.0) as u64),
                        0,
                        price,
                        order,
                        order,
                    ),
                )
                .unwrap();
            let usd = i.usd as f64 / MICRO;
            assert!(
                usd >= 1.0 - 1e-6,
                "clip ${usd} at price {price} is below the venue floor and would bounce"
            );
        }
    }
    #[test]
    fn RED_a_zero_or_absurd_order_size_cannot_produce_a_position() {
        let r = pct_lane(0.015);
        let i = r.decide1(0, &dec("100000001", 0, 0.50, 0.0, 0.0));
        match i {
            Err(_) => {}
            Ok(x) => {
                let usd = x.usd as f64 / MICRO;
                assert!(
                    (1.0 - 1e-6..= 2.0).contains(& usd),
                    "a zero-size order produced a ${usd} clip"
                );
            }
        }
    }
    #[test]
    fn RED_pct_never_exceeds_the_per_market_cap_across_repeated_orders() {
        let mut c = cfg("example_lane_26", 1, Sizing::Pct(0.015));
        c.per_market_usd = 300.0;
        c.max_usd_per_fill = 250.0;
        c.daily_budget_usd = 100_000.0;
        c.max_open_usd = 100_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let mut spent = 0.0;
        for _ in 0..40 {
            match r.decide1(0, &dec("100000001", 0, 0.50, 5000.0, 5000.0)) {
                Ok(i) => spent += i.usd as f64 / MICRO,
                Err(Skip::PerMarketCap) => break,
                Err(e) => panic!("unexpected {e:?}"),
            }
        }
        assert!(spent <= 300.0 + 1e-6, "per-market cap breached: ${spent}");
    }
    #[test]
    fn RED_an_exit_after_a_PARTIAL_fill_sells_only_what_filled() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_holding("100000001", 30.0);
        r.lane(0).unwrap().set_his("100000001", 5000.0);
        let i = r.decide1(0, &dec("100000001", 1, 0.60, 1000.0, 5000.0)).unwrap();
        assert!(
            i.shares <= 30.0 + 1e-9, "tried to sell {} of a 30-share position", i.shares
        );
        assert!((i.shares - 6.0).abs() < 0.6, "20% of 30 is ~6, got {}", i.shares);
    }
    #[test]
    fn RED_a_SELL_signal_can_never_open_a_new_position() {
        let r = pct_lane(0.015);
        r.lane(0).unwrap().set_his("100000001", 5000.0);
        assert_eq!(
            r.decide1(0, & dec("100000001", 1, 0.08, 3000.0, 5000.0)).unwrap_err(),
            Skip::SellNoPosition, "a sell must never be able to open a position"
        );
    }
    #[test]
    fn RED_price_band_still_gates_pct_sized_orders() {
        let r = pct_lane(0.015);
        assert_eq!(
            r.decide1(0, & dec("100000001", 0, 0.99, 5000.0, 5000.0)).unwrap_err(),
            Skip::PriceOutOfBand
        );
        assert_eq!(
            r.decide1(0, & dec("100000001", 0, 0.001, 5000.0, 5000.0)).unwrap_err(),
            Skip::PriceOutOfBand
        );
    }
    #[test]
    fn a_HALTED_lane_still_follows_him_OUT() {
        let r = router2();
        r.lane(0).unwrap().state.halted.store(true, Ordering::Relaxed);
        r.lane(0).unwrap().set_holding("1000001", 100.0);
        r.lane(0).unwrap().set_his("1000001", 1000.0);
        let i = r
            .decide1(0, &dec("1000001", 1, 0.60, 500.0, 1000.0))
            .expect("a halted lane MUST still be able to sell");
        assert_eq!(i.side, 1);
        assert!(i.shares > 0.0);
    }
    #[test]
    fn a_HALTED_lane_refuses_to_BUY() {
        let r = router2();
        r.lane(0).unwrap().state.halted.store(true, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.60, 1000.0, 5000.0)).unwrap_err(),
            Skip::Halted
        );
    }
    #[test]
    fn a_DISARMED_lane_refuses_BOTH_sides() {
        let r = router2();
        r.lane(0).unwrap().state.armed.store(false, Ordering::Relaxed);
        r.lane(0).unwrap().set_holding("1000001", 100.0);
        r.lane(0).unwrap().set_his("1000001", 1000.0);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.60, 1000.0, 5000.0)).unwrap_err(),
            Skip::Disarmed
        );
        assert_eq!(
            r.decide1(0, & dec("1000001", 1, 0.60, 500.0, 1000.0)).unwrap_err(),
            Skip::Disarmed
        );
    }
    #[test]
    fn routes_each_wallet_to_its_own_lane() {
        let r = router2();
        assert_eq!(r.lane_for(& w(1)), Some(0));
        assert_eq!(r.lane_for(& w(2)), Some(1));
        assert_eq!(r.lane_for(& w(9)), None, "unknown wallet must not route");
    }
    #[test]
    fn a_SKIPPED_trade_never_leaves_the_token_owned() {
        let mut c = cfg("a", 1, Sizing::Pct(0.05));
        c.min_buy_price = 0.02;
        c.max_buy_price = 0.95;
        let lane = ready_lane(c);
        lane.state.armed.store(true, Ordering::Relaxed);
        let r = Router::new(vec![lane]);
        assert_eq!(
            r.decide1(0, & dec("TOK", 0, 0.99, 100.0, 100.0)).unwrap_err(),
            Skip::PriceOutOfBand
        );
        assert_eq!(
            r.owner_of("TOK"), None,
            "a skipped trade must leave the token free for another lane"
        );
    }
    #[test]
    fn an_ACTED_trade_does_claim_the_token() {
        let lane = ready_lane(cfg("a", 1, Sizing::Pct(0.05)));
        lane.state.armed.store(true, Ordering::Relaxed);
        let r = Router::new(vec![lane]);
        r.decide1(0, &dec("TOK", 0, 0.50, 1000.0, 1000.0)).expect("a valid buy");
        assert_eq!(r.owner_of("TOK"), Some(0), "acting on a token claims it");
    }
    #[test]
    fn the_collision_guard_still_blocks_a_SECOND_lane() {
        let a = ready_lane(cfg("a", 1, Sizing::Pct(0.05)));
        let b = ready_lane(cfg("b", 2, Sizing::Pct(0.05)));
        a.state.armed.store(true, Ordering::Relaxed);
        b.state.armed.store(true, Ordering::Relaxed);
        let r = Router::new(vec![a, b]);
        r.decide1(0, &dec("TOK", 0, 0.50, 1000.0, 1000.0)).expect("lane a takes it");
        assert_eq!(r.owner_of("TOK"), Some(0));
        assert_eq!(
            r.decide1(1, & dec("TOK", 0, 0.50, 1000.0, 1000.0)).unwrap_err(),
            Skip::TokenOwnedByOtherLane, "lane b must be refused"
        );
    }
    #[test]
    fn a_skipped_trade_does_not_block_the_lane_that_CAN_trade_it() {
        let mut ca = cfg("a", 1, Sizing::Pct(0.05));
        ca.max_buy_price = 0.60;
        let a = ready_lane(ca);
        let b = ready_lane(cfg("b", 2, Sizing::Pct(0.05)));
        a.state.armed.store(true, Ordering::Relaxed);
        b.state.armed.store(true, Ordering::Relaxed);
        let r = Router::new(vec![a, b]);
        assert_eq!(
            r.decide1(0, & dec("TOK", 0, 0.90, 1000.0, 1000.0)).unwrap_err(),
            Skip::PriceOutOfBand
        );
        r.decide1(1, &dec("TOK", 0, 0.90, 1000.0, 1000.0))
            .expect("lane b must still be able to copy this market");
        assert_eq!(r.owner_of("TOK"), Some(1));
    }
    #[test]
    fn an_UNSEEDED_lane_refuses_to_trade_at_all_including_exits() {
        let lane = Lane::new(cfg("fresh", 1, Sizing::Pct(0.05)));
        lane.state.armed.store(true, Ordering::Relaxed);
        lane.set_holding("100000001", 500.0);
        let r = Router::new(vec![lane]);
        let buy = dec("100000001", 0, 0.50, 1000.0, 1000.0);
        assert_eq!(
            r.decide1(0, & buy).unwrap_err(), Skip::NotReady, "no buying while unseeded"
        );
        let sell = dec("100000001", 1, 0.50, 50.0, 50.0);
        assert_eq!(
            r.decide1(0, & sell).unwrap_err(), Skip::NotReady,
            "and NO SELLING — an unsized exit is the 100% liquidation"
        );
    }
    #[test]
    fn the_same_lane_trades_normally_once_activation_completes() {
        let lane = Lane::new(cfg("fresh", 1, Sizing::Pct(0.05)));
        lane.state.armed.store(true, Ordering::Relaxed);
        lane.set_holding("100000001", 500.0);
        lane.set_his("100000001", 1000.0);
        lane.mark_ready();
        let r = Router::new(vec![lane]);
        let sell = dec("100000001", 1, 0.50, 100.0, 100.0);
        let intent = r.decide1(0, &sell).expect("a seeded lane exits normally");
        assert!(
            (intent.shares - 50.0).abs() < 1e-6, "proportional exit, got {}", intent
            .shares
        );
    }
    #[test]
    fn readiness_is_checked_BEFORE_arming_so_the_reason_is_never_misreported() {
        let lane = Lane::new(cfg("fresh", 1, Sizing::Pct(0.05)));
        let r = Router::new(vec![lane]);
        assert_eq!(
            r.decide1(0, & dec("1", 0, 0.5, 10.0, 10.0)).unwrap_err(), Skip::NotReady,
            "an unready lane reports NotReady, not Disarmed"
        );
    }
    #[test]
    fn lanes_size_independently_and_pct_is_of_his_CONFIRMED_fill() {
        let r = router2();
        let a = r.decide1(0, &dec("100000001", 0, 0.60, 5000.0, 5000.0)).unwrap();
        let b = r.decide1(1, &dec("100000002", 0, 0.60, 5000.0, 5000.0)).unwrap();
        assert!((a.shares - 5.0).abs() < 1e-9, "shares lane -> fixed 5");
        let b_usd = b.usd as f64 / MICRO;
        let intended = 5000.0 * 0.60 * 0.002;
        assert!(
            b_usd <= intended + 0.62,
            "whole-share/floor rounding exceeded one-share headroom: ${b_usd} vs ${intended}"
        );
        assert!(
            b_usd > intended - 0.62,
            "shortfall must be under one whole share; got ${b_usd} vs ${intended}"
        );
        let c = r.decide1(1, &dec("100000003", 0, 0.60, 10_000.0, 12_000.0)).unwrap();
        let c_usd = c.usd as f64 / MICRO;
        assert!(
            c_usd > b_usd,
            "confirmed fill size did not affect pct sizing: ${b_usd} vs ${c_usd}"
        );
    }
    #[test]
    fn second_lane_is_skipped_on_a_token_another_lane_owns() {
        let r = router2();
        assert!(r.decide1(0, & dec("55", 0, 0.60, 100.0, 5000.0)).is_ok());
        assert_eq!(
            r.decide1(1, & dec("55", 0, 0.60, 100.0, 5000.0)),
            Err(Skip::TokenOwnedByOtherLane)
        );
        assert_eq!(r.owner_of("55"), Some(0));
    }
    #[test]
    fn owning_lane_may_keep_trading_its_own_token() {
        let r = router2();
        assert!(r.decide1(0, & dec("55", 0, 0.60, 100.0, 5000.0)).is_ok());
        assert!(r.decide1(0, & dec("55", 0, 0.60, 100.0, 5000.0)).is_ok());
    }
    #[test]
    fn releasing_a_token_frees_it_for_another_lane() {
        let r = router2();
        r.decide1(0, &dec("55", 0, 0.60, 100.0, 5000.0)).unwrap();
        r.release("55", 0);
        assert!(r.decide1(1, & dec("55", 0, 0.60, 5000.0, 5000.0)).is_ok());
    }
    #[test]
    fn a_lane_cannot_release_a_token_it_does_not_own() {
        let r = router2();
        r.decide1(0, &dec("55", 0, 0.60, 100.0, 5000.0)).unwrap();
        r.release("55", 1);
        assert_eq!(r.owner_of("55"), Some(0));
    }

mod more;
