//! Existing lane route/resting regression cases, separated from execution logic.

    use super::*;
    use crate::book::Top;
    use std::time::Instant;
    fn top(bb: f64, ba: f64) -> Top {
        Top {
            best_bid: bb,
            best_ask: ba,
            at: Instant::now(),
        }
    }
    fn intent(side: u8) -> Intent {
        Intent {
            lane: 0,
            token_id: "1".into(),
            side,
            shares: 5.0,
            limit: 0.62,
            usd: 0,
            budget_usd: 0,
            execution: Execution::Hybrid,
            his_remaining: 4600.0,
            he_was_maker: false,
            route: Route::Take,
        }
    }
    fn hybrid_cfg() -> LaneConfig {
        LaneConfig {
            name: "h".into(),
            wallet20: [1u8; 20],
            sizing: Sizing::Shares(5.0),
            execution: Execution::Hybrid,
            buy_slippage_c: 0.02,
            sell_slippage_c: 0.02,
            copy_maker_sells: false,
            sell_floor_frac: 0.5,
            min_order_usd: 1.0,
            max_usd_per_fill: 250.0,
            daily_budget_usd: 500.0,
            per_market_usd: 300.0,
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
    #[test]
    fn a_one_tick_spread_falls_back_to_taking() {
        let r = route_for(
            &intent(0),
            &hybrid_cfg(),
            Some(top(0.60, 0.61)),
            0.60,
            false,
            false,
            false,
        );
        assert_eq!(r, Route::Take);
    }
    #[test]
    fn A_MAKER_MIRROR_LANE_NEEDS_A_BOOK_even_though_it_is_not_HYBRID() {
        let plain = Lane::new(taker_cfg(false));
        assert!(
            ! needs_book(& std::sync::Arc::new(plain)), "a pure taker lane needs none"
        );
        let mut m = taker_cfg(false);
        m.copy_makers = true;
        assert!(
            needs_book(& std::sync::Arc::new(Lane::new(m))),
            "a maker-mirror lane needs a book — it cannot check for room without one"
        );
        let mut sells = taker_cfg(false);
        sells.copy_maker_sells = true;
        assert!(
            needs_book(& std::sync::Arc::new(Lane::new(sells))),
            "the SELL mirror needs one too"
        );
        assert!(needs_book(& std::sync::Arc::new(Lane::new(hybrid_cfg()))));
        let mut on = taker_cfg(false);
        on.copy_makers = true;
        let lane = std::sync::Arc::new(Lane::new(on));
        assert!(needs_book(& lane));
        lane.state.rest_buys.store(false, Ordering::Relaxed);
        lane.state.rest_sells.store(false, Ordering::Relaxed);
        assert!(! needs_book(& lane));
    }
    #[test]
    fn THE_KILL_SWITCH_stops_resting_on_each_side_INDEPENDENTLY() {
        let mut c = taker_cfg(false);
        c.copy_makers = true;
        c.copy_maker_sells = true;
        let (mut buy, mut sell) = (intent(0), intent(1));
        buy.he_was_maker = true;
        sell.he_was_maker = true;
        let book = Some(top(0.55, 0.70));
        assert!(
            matches!(resolve_route(& buy, & c, book, 0.60, false, false, false, true,
            true), Route::RestInFront { .. })
        );
        assert!(
            matches!(resolve_route(& sell, & c, book, 0.60, false, false, false, true,
            true), Route::RestInFront { .. })
        );
        assert_eq!(
            resolve_route(& buy, & c, book, 0.60, false, false, false, false, true),
            Route::Take, "a disabled buy side must cross, not rest"
        );
        assert!(
            matches!(resolve_route(& sell, & c, book, 0.60, false, false, false, false,
            true), Route::RestInFront { .. }), "the sell side is unaffected"
        );
        assert_eq!(
            resolve_route(& sell, & c, book, 0.60, false, false, false, true, false),
            Route::Take
        );
        assert_eq!(
            resolve_route(& buy, & c, book, 0.60, false, false, false, false, false),
            Route::Take
        );
        assert_eq!(
            resolve_route(& sell, & c, book, 0.60, false, false, false, false, false),
            Route::Take
        );
    }
    #[test]
    fn the_switch_SEEDS_from_config_so_boot_behaviour_is_unchanged() {
        let mut on = taker_cfg(false);
        on.copy_makers = true;
        on.copy_maker_sells = true;
        let lane = Lane::new(on);
        assert!(lane.state.rest_buys.load(Ordering::Relaxed));
        assert!(lane.state.rest_sells.load(Ordering::Relaxed));
        let off = taker_cfg(false);
        let lane = Lane::new(off);
        assert!(! lane.state.rest_buys.load(Ordering::Relaxed));
        assert!(! lane.state.rest_sells.load(Ordering::Relaxed));
        let hy = hybrid_cfg();
        let lane = Lane::new(hy);
        assert!(lane.state.rest_buys.load(Ordering::Relaxed));
        assert!(lane.state.rest_sells.load(Ordering::Relaxed));
    }
    #[test]
    fn a_HYBRID_lane_still_rests_his_TAKER_fills_under_the_switch() {
        let c = hybrid_cfg();
        let mut taker = intent(0);
        taker.he_was_maker = false;
        assert!(
            matches!(resolve_route(& taker, & c, Some(top(0.55, 0.70)), 0.60, false,
            false, false, true, true), Route::RestInFront { .. }),
            "Hybrid rests regardless of his role"
        );
        let mut m = taker_cfg(false);
        m.copy_makers = true;
        assert_eq!(
            resolve_route(& taker, & m, Some(top(0.55, 0.70)), 0.60, false, false, false,
            true, true), Route::Take, "the maker mirror only mirrors his MAKER fills"
        );
    }
    #[test]
    fn a_wide_spread_rests_one_tick_in_front() {
        let r = route_for(
            &intent(0),
            &hybrid_cfg(),
            Some(top(0.60, 0.65)),
            0.60,
            false,
            false,
            false,
        );
        match r {
            Route::RestInFront { limit, room_ticks } => {
                assert!((limit - 0.61).abs() < 1e-9, "must rest ONE tick above him");
                assert!(room_ticks >= 1);
            }
            Route::Take => panic!("wide spread should rest"),
        }
    }
    #[test]
    fn a_taker_lane_never_rests_however_wide_the_book() {
        let mut c = hybrid_cfg();
        c.execution = Execution::Taker;
        assert_eq!(
            route_for(& intent(0), & c, Some(top(0.30, 0.90)), 0.30, false, false,
            false), Route::Take
        );
    }
    fn taker_cfg(copy_makers: bool) -> LaneConfig {
        let mut c = hybrid_cfg();
        c.execution = Execution::Taker;
        c.copy_makers = copy_makers;
        c
    }
    fn intent_role(maker: bool) -> Intent {
        let mut i = intent(0);
        i.he_was_maker = maker;
        i
    }
    fn sell_cfg(on: bool) -> LaneConfig {
        let mut c = taker_cfg(false);
        c.copy_maker_sells = on;
        c
    }
    fn sell_intent(maker: bool) -> Intent {
        let mut i = intent(0);
        i.side = 1;
        i.he_was_maker = maker;
        i
    }
    #[test]
    fn the_ledger_accumulates_per_order_and_the_flags_track_his_lifecycle() {
        let led = TrancheLedger::new(64);
        let salt = [1u8; 32];
        let c1 = led.observe(&salt, 300.0);
        assert_eq!(sell_tranche_flags(c1, 300.0, 1000.0), (true, false), "opening");
        let c2 = led.observe(&salt, 300.0);
        assert_eq!(sell_tranche_flags(c2, 300.0, 1000.0), (false, false), "middle");
        let c3 = led.observe(&salt, 400.0);
        assert_eq!(
            sell_tranche_flags(c3, 400.0, 1000.0), (false, true),
            "⛔ the final tranche of a MULTI-fill order — per-fill maths called \
                    this (false,false) and would have RESTED after his order completed"
        );
    }
    #[test]
    fn a_single_fill_exit_is_first_AND_done() {
        let led = TrancheLedger::new(64);
        let cum = led.observe(&[2u8; 32], 1000.0);
        assert_eq!(sell_tranche_flags(cum, 1000.0, 1000.0), (true, true));
    }
    #[test]
    fn two_of_his_orders_do_not_share_a_running_sum() {
        let led = TrancheLedger::new(64);
        led.observe(&[3u8; 32], 900.0);
        let c = led.observe(&[4u8; 32], 100.0);
        assert!((c - 100.0).abs() < 1e-9, "salt is the order identity");
    }
    #[test]
    fn an_unknown_order_total_reads_done_and_therefore_TAKES() {
        for bad in [0.0, -5.0] {
            let (_, done) = sell_tranche_flags(300.0, 300.0, bad);
            assert!(done, "order_size={bad} must read done");
        }
    }
    #[test]
    fn eviction_loses_history_in_the_SAFE_direction() {
        let led = TrancheLedger::new(2);
        led.observe(&[5u8; 32], 100.0);
        led.observe(&[6u8; 32], 100.0);
        led.observe(&[7u8; 32], 100.0);
        assert_eq!(led.len(), 2);
        let c = led.observe(&[5u8; 32], 100.0);
        assert_eq!(
            sell_tranche_flags(c, 100.0, 1000.0).0, true,
            "an evicted order reads as an opening clip, which crosses"
        );
    }
    #[test]
    fn the_full_three_tranche_walk_routes_TAKE_REST_TAKE() {
        let led = TrancheLedger::new(64);
        let c = sell_cfg(true);
        let salt = [8u8; 32];
        let mut routes = Vec::new();
        for fill in [300.0, 300.0, 400.0] {
            let cum = led.observe(&salt, fill);
            let (first, done) = sell_tranche_flags(cum, fill, 1000.0);
            routes
                .push(
                    route_for(
                        &sell_intent(true),
                        &c,
                        Some(top(0.60, 0.65)),
                        0.65,
                        false,
                        first,
                        done,
                    ),
                );
        }
        assert_eq!(routes[0], Route::Take, "t1: certainty first");
        assert!(matches!(routes[1], Route::RestInFront { .. }), "t2: rest under him");
        assert_eq!(routes[2], Route::Take, "t3: his last tranche has nothing behind it");
    }
    #[test]
    fn copy_maker_sells_OFF_is_byte_for_byte_TODAYS_exit() {
        let c = sell_cfg(false);
        for (first, done) in [(true, false), (false, false), (false, true)] {
            assert_eq!(
                route_for(& sell_intent(true), & c, Some(top(0.60, 0.65)), 0.65, false,
                first, done), Route::Take, "off ⇒ always cross"
            );
        }
    }
    #[test]
    fn his_OPENING_sell_tranche_CROSSES() {
        let c = sell_cfg(true);
        assert_eq!(
            route_for(& sell_intent(true), & c, Some(top(0.60, 0.65)), 0.65, false, true,
            false), Route::Take
        );
    }
    #[test]
    fn a_LATER_sell_tranche_rests_ONE_TICK_UNDER_HIM() {
        let c = sell_cfg(true);
        match route_for(
            &sell_intent(true),
            &c,
            Some(top(0.60, 0.65)),
            0.65,
            false,
            false,
            false,
        ) {
            Route::RestInFront { limit, .. } => {
                assert!(
                    (limit - 0.64).abs() < 1e-9, "must rest BELOW his 0.65, got {limit}"
                )
            }
            r => panic!("expected a rest, got {r:?}"),
        }
    }
    #[test]
    fn his_FINAL_tranche_CROSSES_because_nothing_follows_it_through() {
        let c = sell_cfg(true);
        assert_eq!(
            route_for(& sell_intent(true), & c, Some(top(0.60, 0.65)), 0.65, false,
            false, true), Route::Take
        );
    }
    #[test]
    fn his_TAKER_sell_is_never_mirrored_as_a_rest() {
        let c = sell_cfg(true);
        assert_eq!(
            route_for(& sell_intent(false), & c, Some(top(0.60, 0.65)), 0.65, false,
            false, false), Route::Take
        );
    }
    #[test]
    fn NO_ROOM_under_him_falls_back_to_crossing() {
        let c = sell_cfg(true);
        assert_eq!(
            route_for(& sell_intent(true), & c, Some(top(0.64, 0.65)), 0.65, false,
            false, false), Route::Take, "no room ⇒ take, never post through the bid"
        );
    }
    #[test]
    fn enabling_sell_resting_does_NOT_change_BUY_routing() {
        let c = sell_cfg(true);
        assert_eq!(
            route_for(& intent_role(true), & c, Some(top(0.60, 0.65)), 0.60, false,
            false, false), Route::Take, "buys are still governed by copy_makers alone"
        );
    }
    #[test]
    fn copy_makers_OFF_always_takes_even_on_his_maker_fill() {
        let c = taker_cfg(false);
        assert_eq!(
            route_for(& intent_role(true), & c, Some(top(0.60, 0.65)), 0.60, false,
            false, false), Route::Take, "copy_makers off ⇒ never rest"
        );
    }
    #[test]
    fn the_FIRST_clip_TAKES_even_when_the_book_is_wide_enough_to_rest() {
        let c = taker_cfg(true);
        assert_eq!(
            route_for(& intent_role(true), & c, Some(top(0.60, 0.65)), 0.60, true, true,
            false), Route::Take,
            "the first clip must cross so we are certainly IN before resting"
        );
    }
    #[test]
    fn AFTER_the_first_clip_the_remainder_rests_in_front() {
        let c = taker_cfg(true);
        match route_for(
            &intent_role(true),
            &c,
            Some(top(0.60, 0.65)),
            0.60,
            false,
            false,
            false,
        ) {
            Route::RestInFront { limit, .. } => {
                assert!(
                    (limit - 0.61).abs() < 1e-9,
                    "later clips rest ONE tick in front of him"
                )
            }
            Route::Take => panic!("only the FIRST clip should take on a wide book"),
        }
    }
    #[test]
    fn the_first_clip_rule_does_NOT_change_a_hybrid_lane() {
        let c = hybrid_cfg();
        match route_for(&intent(0), &c, Some(top(0.60, 0.65)), 0.60, true, true, false) {
            Route::RestInFront { .. } => {}
            Route::Take => panic!("hybrid must still rest on a wide spread"),
        }
    }
    #[test]
    fn copy_makers_ON_rests_in_front_of_his_MAKER_fill() {
        let c = taker_cfg(true);
        match route_for(
            &intent_role(true),
            &c,
            Some(top(0.60, 0.65)),
            0.60,
            false,
            false,
            false,
        ) {
            Route::RestInFront { limit, .. } => assert!((limit - 0.61).abs() < 1e-9),
            Route::Take => panic!("his maker fill on a wide book should rest"),
        }
    }
    #[test]
    fn copy_makers_ON_still_TAKES_his_taker_fill() {
        let c = taker_cfg(true);
        assert_eq!(
            route_for(& intent_role(false), & c, Some(top(0.60, 0.65)), 0.60, false,
            false, false), Route::Take, "a taker fill is always crossed"
        );
    }
    #[test]
    fn copy_makers_ON_a_one_tick_spread_still_takes_his_maker_fill() {
        let c = taker_cfg(true);
        assert_eq!(
            route_for(& intent_role(true), & c, Some(top(0.60, 0.61)), 0.60, false,
            false, false), Route::Take, "no room ⇒ take"
        );
    }
    #[test]
    fn copy_makers_NEVER_rests_a_SELL_however_wide_the_book() {
        let c = taker_cfg(true);
        let mut sell = intent_role(true);
        sell.side = 1;
        assert_eq!(
            route_for(& sell, & c, Some(top(0.60, 0.90)), 0.70, false, false, false),
            Route::Take, "copy_makers is buy-only; exits always take"
        );
    }
    #[test]
    fn no_book_falls_back_to_taking() {
        assert_eq!(
            route_for(& intent(0), & hybrid_cfg(), None, 0.60, false, false, false),
            Route::Take
        );
    }
    #[test]
    fn a_stale_book_falls_back_to_taking() {
        let stale = Top {
            best_bid: 0.60,
            best_ask: 0.70,
            at: Instant::now() - std::time::Duration::from_secs(60),
        };
        assert_eq!(
            route_for(& intent(0), & hybrid_cfg(), Some(stale), 0.60, false, false,
            false), Route::Take,
            "resting off a stale book prices us at a level that no longer exists"
        );
    }
    #[test]
    fn a_resting_bid_never_crosses_the_ask() {
        for (bb, ba, his) in [
            (0.60, 0.65, 0.60),
            (0.10, 0.30, 0.10),
            (0.90, 0.95, 0.90),
        ] {
            if let Route::RestInFront { limit, .. } = route_for(
                &intent(0),
                &hybrid_cfg(),
                Some(top(bb, ba)),
                his,
                false,
                false,
                false,
            ) {
                assert!(
                    limit < ba,
                    "rest {limit} would CROSS the ask {ba} — that is a take"
                );
                assert!(limit > his, "must be IN FRONT of his {his}");
            }
        }
    }
    #[test]
    fn sell_side_rests_below_him_and_above_the_bid() {
        if let Route::RestInFront { limit, .. } = route_for(
            &intent(1),
            &hybrid_cfg(),
            Some(top(0.60, 0.70)),
            0.70,
            false,
            false,
            false,
        ) {
            assert!(limit < 0.70 && limit > 0.60);
        } else {
            panic!("wide spread should rest on the sell side too");
        }
    }
