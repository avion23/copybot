//! Existing lane budget, inventory, policy and repricing regression cases.

use super::*;

    #[test]
    fn disarmed_lane_fires_nothing() {
        let r = router2();
        r.lane(0).unwrap().state.armed.store(false, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.6, 100.0, 5000.0)), Err(Skip::Disarmed)
        );
    }
    #[test]
    fn halted_lane_fires_nothing_but_others_continue() {
        let r = router2();
        r.lane(0).unwrap().state.halted.store(true, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.6, 100.0, 5000.0)), Err(Skip::Halted)
        );
        assert!(
            r.decide1(1, & dec("1000002", 0, 0.6, 5000.0, 5000.0)).is_ok(),
            "one lane halting must not affect another"
        );
    }
    #[test]
    fn daily_budget_binds_and_is_per_lane() {
        let r = router2();
        let mut fired = 0;
        for i in 0..500 {
            if r.decide1(0, &dec(&format!("9{i:07}"), 0, 0.60, 100.0, 5000.0)).is_ok() {
                fired += 1;
            }
        }
        let spent = r.lane(0).unwrap().state.spent_today.load(Ordering::Relaxed) as f64
            / MICRO;
        assert!(spent <= 500.0 + 1e-6, "daily budget overshot: {spent}");
        assert!(fired > 0);
        assert!(r.decide1(1, & dec("777777777", 0, 0.60, 5000.0, 5000.0)).is_ok());
    }
    #[test]
    fn daily_budget_enforcement_uses_the_active_policy_generation() {
        let r = router2();
        let lane = r.lane(0).unwrap();
        let fracs = crate::budget::Fracs {
            open: 1.0,
            per_market: 1.0,
            per_fill: 1.0,
            daily: 0.01,
        };
        let next = SizingPolicy::build(
            0,
            1_000.0,
            Sizing::Pct(0.05),
            0.05,
            false,
            &fracs,
        )
        .unwrap();
        lane.apply_policy(next).unwrap();
        lane.state.spent_today.store((9.0 * MICRO) as Micro, Ordering::Relaxed);

        assert_eq!(
            r.decide1(0, &dec("policy_daily", 0, 0.60, 200.0, 200.0)),
            Err(Skip::DailyBudget),
            "the active policy's $10 daily cap must replace cfg's stale $500 boot cap"
        );
    }
    #[test]
    fn per_market_cap_binds() {
        let r = router2();
        let mut n = 0;
        while r.decide1(0, &dec("123456789", 0, 0.60, 100.0, 5000.0)).is_ok() {
            n += 1;
        }
        let l0 = r.lane(0).unwrap();
        let pt = l0.per_token.lock().unwrap();
        assert!(* pt.get("123456789").unwrap() as f64 / MICRO <= 200.0 + 1e-6);
        assert!(n > 0);
    }
    #[test]
    fn undersized_order_is_lifted_to_the_venue_floor() {
        let r = Router::new(vec![ready_lane(cfg("t", 1, Sizing::Pct(0.0001)))]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("1000001", 0, 0.05, 100.0, 5000.0)).unwrap();
        assert!(i.usd as f64 / MICRO >= 1.0 - 1e-9, "must reach the $1 venue floor");
    }
    #[test]
    fn undersized_order_is_skipped_when_floor_lifting_is_off() {
        let mut c = cfg("t", 1, Sizing::Pct(0.0001));
        c.min_fill_floor = false;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.05, 100.0, 5000.0)),
            Err(Skip::DustAfterSizing)
        );
    }
    #[test]
    fn a_whole_share_order_under_a_dollar_is_reported_as_below_minimum() {
        let mut c = cfg("t", 1, Sizing::Shares(1.0));
        c.min_fill_floor = false;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.03, 100.0, 5000.0)),
            Err(Skip::BelowVenueMinimum)
        );
    }
    #[test]
    fn buys_are_always_whole_shares() {
        let r = router2();
        let i = r.decide1(1, &dec("1000001", 0, 0.60, 6170.0, 8000.0)).unwrap();
        assert_eq!(i.shares, i.shares.trunc(), "buy size must be whole: {}", i.shares);
    }
    #[test]
    fn sell_sizing_is_tick_clean_and_keeps_dust_sellable() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 0.7);
        r.lane(0).unwrap().set_his("1000001", 100.0);
        let i = r.decide1(0, &dec("1000001", 1, 0.22, 100.0, 100.0)).unwrap();
        let cents = i.shares * i.limit * 100.0;
        assert!(
            (cents - cents.round()).abs() < 1e-9, "off-tick exit {} x {}", i.shares, i
            .limit
        );
        assert!(i.shares > 0.0, "a 0.7-share exit must not be abandoned");
    }
    #[test]
    fn price_bands_are_enforced() {
        let r = router2();
        assert_eq!(
            r.decide1(0, & dec("1000001", 0, 0.99, 100.0, 5000.0)),
            Err(Skip::PriceOutOfBand)
        );
        assert_eq!(
            r.decide1(0, & dec("1000002", 0, 0.001, 100.0, 5000.0)),
            Err(Skip::PriceOutOfBand)
        );
    }
    #[test]
    fn a_FULL_exit_flattens_real_holdings_to_under_one_share() {
        for held in [221.1788f64, 191.25, 94.95, 63.0, 32.41, 15.2174, 5.0588, 3.8657] {
            let mut c = cfg("flat", 50, Sizing::Pct(0.015));
            c.sell_slippage_c = 1.0;
            c.sell_floor_frac = 0.0;
            c.sell_all_frac = 0.95;
            let r = Router::new(vec![ready_lane(c)]);
            r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
            let tok = "8000001";
            r.lane(0).unwrap().set_holding(tok, held);
            r.lane(0).unwrap().set_his(tok, 10_000.0);
            let i = r.decide1(0, &dec(tok, 1, 0.50, 10_000.0, 10_000.0)).unwrap();
            let left = held - i.shares;
            assert!(
                left < 1.0,
                "a full exit of {held} left {left} shares behind — more than the \
                     sub-share dust the cents-clean rule makes unavoidable"
            );
            assert!(
                i.limit <= 0.03, "100% slippage must reach the one-cent band, got {}", i
                .limit
            );
        }
    }
    #[test]
    fn a_SUB_SHARE_holding_now_EXITS_instead_of_being_stranded() {
        let mut c = cfg("subshare", 51, Sizing::Pct(0.015));
        c.sell_slippage_c = 1.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "8000002";
        r.lane(0).unwrap().set_holding(tok, 0.61);
        r.lane(0).unwrap().set_his(tok, 10_000.0);
        let i = r.decide1(0, &dec(tok, 1, 0.05, 10_000.0, 10_000.0)).unwrap();
        assert!(i.shares >= 0.60, "0.61 shares must now EXIT, got {}", i.shares);
        assert!(i.shares <= 0.61 + 1e-9, "and never oversell");
    }
    #[test]
    fn a_holding_under_the_SHARE_TICK_is_still_refused() {
        let mut c = cfg("subtick", 52, Sizing::Pct(0.015));
        c.sell_slippage_c = 1.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "8000003";
        r.lane(0).unwrap().set_holding(tok, 0.004);
        r.lane(0).unwrap().set_his(tok, 10_000.0);
        assert_eq!(
            r.decide1(0, & dec(tok, 1, 0.50, 10_000.0, 10_000.0)),
            Err(Skip::DustAfterSizing),
            "under one share-tick there is no legal order to send"
        );
    }
    #[test]
    fn a_GRADUAL_bleed_off_leaves_us_holding_NOTHING_and_the_ratio_never_drifts() {
        let mut c = cfg("bleed", 30, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "5000001";
        r.lane(0).unwrap().set_holding(tok, 808.0);
        r.lane(0).unwrap().set_his(tok, 10_000.0);
        let start_ratio = 808.0 / 10_000.0;
        let mut his = 10_000.0;
        for _ in 0..4 {
            let tranche = 2_500.0;
            let i = r.decide1(0, &dec(tok, 1, 0.50, tranche, 10_000.0)).unwrap();
            let held_before = r.lane(0).unwrap().holding(tok);
            tick_settled(&r.lane(0).unwrap(), tok, (held_before - i.shares).max(0.0));
            his -= tranche;
            r.lane(0).unwrap().set_his(tok, his);
            let held = r.lane(0).unwrap().holding(tok);
            if his > 1e-9 {
                let ratio = held / his;
                assert!(
                    (ratio - start_ratio).abs() < 0.005,
                    "our share of his book drifted: {ratio} vs {start_ratio}"
                );
            }
        }
        assert!(
            r.lane(0).unwrap().holding(tok) <= 1e-6,
            "after he is fully out we must hold NOTHING, still hold {}", r.lane(0)
            .unwrap().holding(tok)
        );
    }
    #[test]
    fn two_of_his_tranches_INSIDE_ONE_TICK_cannot_oversell_our_position() {
        let mut c = cfg("tranche", 40, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "6000001";
        r.lane(0).unwrap().set_holding(tok, 100.0);
        r.lane(0).unwrap().set_his(tok, 100.0);
        let a = r.decide1(0, &dec(tok, 1, 0.50, 30.0, 100.0)).unwrap();
        r.lane(0).unwrap().set_his(tok, 70.0);
        let b = r.decide1(0, &dec(tok, 1, 0.50, 30.0, 100.0)).unwrap();
        assert!(
            a.shares + b.shares <= 100.0 + 1e-9,
            "offered {} + {} = {} shares against a 100-share position", a.shares, b
            .shares, a.shares + b.shares
        );
        assert!(
            (a.shares + b.shares - 60.0).abs() < 0.02,
            "he sold 60% of his book so we must sell 60% of ours, got {} + {} = {}", a
            .shares, b.shares, a.shares + b.shares
        );
    }
    #[test]
    fn tranches_can_never_offer_MORE_SHARES_THAN_WE_HOLD() {
        let mut c = cfg("overshoot", 41, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "6000002";
        r.lane(0).unwrap().set_holding(tok, 100.0);
        r.lane(0).unwrap().set_his(tok, 100.0);
        let a = r.decide1(0, &dec(tok, 1, 0.50, 60.0, 100.0)).unwrap();
        r.lane(0).unwrap().set_his(tok, 40.0);
        let b = r.decide1(0, &dec(tok, 1, 0.50, 30.0, 100.0)).unwrap();
        assert!(
            a.shares + b.shares <= 100.0 + 1e-9,
            "offered {} + {} = {} against 100 held — the venue rejects this and v2 \
                 never retries an exit",
            a.shares, b.shares, a.shares + b.shares
        );
        assert!(
            (a.shares + b.shares - 90.0).abs() < 0.02,
            "expected 90 shares across the two tranches, got {} + {} = {}", a.shares, b
            .shares, a.shares + b.shares
        );
    }
    #[test]
    fn a_fully_reserved_position_has_NOTHING_LEFT_to_sell_and_says_so() {
        let mut c = cfg("reserved", 42, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "6000003";
        r.lane(0).unwrap().set_holding(tok, 100.0);
        r.lane(0).unwrap().set_his(tok, 100.0);
        let a = r.decide1(0, &dec(tok, 1, 0.50, 100.0, 100.0)).unwrap();
        assert!((a.shares - 100.0).abs() < 1e-9, "a full exit sells the position");
        r.lane(0).unwrap().set_his(tok, 0.0);
        assert_eq!(
            r.decide1(0, & dec(tok, 1, 0.50, 10.0, 100.0)),
            Err(Skip::SellAlreadyReserved),
            "every share is already committed to an unresolved sell"
        );
        assert!(
            (r.lane(0).unwrap().holding(tok) - 100.0).abs() < 1e-9,
            "the reservation must not rewrite what we actually hold"
        );
    }
    #[test]
    fn a_single_full_exit_dumps_an_OVERSIZED_position_entirely() {
        let mut c = cfg("fullexit", 31, Sizing::Pct(0.015));
        c.sell_all_frac = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let tok = "5000002";
        r.lane(0).unwrap().set_holding(tok, 808.3783);
        r.lane(0).unwrap().set_his(tok, 10_649.14);
        let i = r.decide1(0, &dec(tok, 1, 0.50, 10_649.14, 10_649.14)).unwrap();
        assert!(
            i.shares > 808.0 - 1.0,
            "a full exit must clear the whole oversized position, got {}", i.shares
        );
        assert!(
            808.3783 - i.shares < 1.0,
            "at most one share of cents-clean dust may remain, left {}", 808.3783 - i
            .shares
        );
    }
    #[test]
    fn COMPOUNDING_scales_the_PCT_so_utilisation_stays_constant() {
        let clip = |scale: f64| -> f64 {
            let mut c = cfg("pctscale", 42, Sizing::Pct(0.015));
            c.per_market_usd = 1_000_000.0;
            c.max_usd_per_fill = 1_000_000.0;
            c.max_open_usd = 10_000_000.0;
            c.daily_budget_usd = 10_000_000.0;
            let r = Router::new(vec![ready_lane(c)]);
            r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
            r.lane(0)
                .unwrap()
                .state
                .cap_scale
                .store((scale * MICRO) as i64, Ordering::Relaxed);
            let i = r.decide1(0, &dec("7000001", 0, 0.50, 10_000.0, 10_000.0)).unwrap();
            i.usd as f64 / MICRO
        };
        let one = clip(1.0);
        let two = clip(2.0);
        assert!(
            (two / one - 2.0).abs() < 0.05,
            "doubling the bankroll must double the clip: {one} -> {two}"
        );
        let half = clip(0.5);
        assert!(
            (half / one - 0.5).abs() < 0.05,
            "halving the bankroll must halve the clip: {one} -> {half}"
        );
    }
    #[test]
    fn the_compounded_pct_is_CEILINGED_however_rich_we_get() {
        use crate::budget::{effective_pct, MAX_EFFECTIVE_PCT};
        let cap = MAX_EFFECTIVE_PCT;
        assert!((effective_pct(0.015, 1.0, cap, true) - 0.015).abs() < 1e-12);
        assert!(
            (effective_pct(0.015, 2.0, cap, true) - 0.030).abs() < 1e-12, "2x -> 3%"
        );
        assert_eq!(
            effective_pct(0.015, 100.0, cap, true), MAX_EFFECTIVE_PCT,
            "must clamp, not run away"
        );
        assert!((effective_pct(0.015, f64::NAN, cap, true) - 0.015).abs() < 1e-12);
        assert_eq!(effective_pct(0.015, 0.0, cap, true), 0.0);
    }
    #[test]
    fn COMPOUNDING_scales_every_cap_and_leaves_the_config_alone() {
        let clip_at = |scale: f64| -> f64 {
            let mut c = cfg("compound", 40, Sizing::Pct(0.015));
            c.max_usd_per_fill = 100.0;
            c.per_market_usd = 200.0;
            c.max_open_usd = 400.0;
            c.daily_budget_usd = 300.0;
            let r = Router::new(vec![ready_lane(c)]);
            r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
            r.lane(0)
                .unwrap()
                .state
                .cap_scale
                .store((scale * MICRO) as i64, Ordering::Relaxed);
            let i = r
                .decide1(0, &dec("6000001", 0, 0.50, 100_000.0, 100_000.0))
                .unwrap();
            i.usd as f64 / MICRO
        };
        let one = clip_at(1.0);
        assert!(
            one <= 100.0 + 1e-6, "at 1.0x the clamp is the configured $100, got ${one}"
        );
        let two = clip_at(2.0);
        assert!(
            two > 150.0 && two <= 200.0 + 1e-6, "at 2.0x expected ~$200, got ${two}"
        );
        let half = clip_at(0.5);
        assert!(half <= 50.0 + 1e-6, "at 0.5x expected ~$50, got ${half}");
        assert!(half < one && one < two, "caps must move monotonically with the scale");
    }
    #[test]
    fn a_lane_that_never_gets_a_balance_reading_runs_its_CONFIGURED_caps() {
        let c = cfg("nofeed", 41, Sizing::Pct(0.015));
        let r = Router::new(vec![ready_lane(c)]);
        assert_eq!(
            r.lane(0).unwrap().state.cap_scale.load(Ordering::Relaxed), MICRO as i64,
            "cap_scale must default to exactly 1.0x"
        );
    }
    #[test]
    fn THE_5X_OVERSIZE_INCIDENT_is_fixed() {
        let mut c = cfg("oversize", 20, Sizing::Pct(0.015));
        c.buy_slippage_c = 0.15;
        c.min_buy_price = 0.01;
        c.per_market_usd = 10_000.0;
        c.max_usd_per_fill = 5_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("4000001", 0, 0.0349, 10_649.14, 10_649.14)).unwrap();
        let usd = i.usd as f64 / MICRO;
        let his_committed = 10_649.14 * 0.0349;
        let intended = his_committed * 0.015;
        assert!(
            (usd - intended).abs() < 0.30,
            "we must commit ~1.5% of his ${his_committed:.2} = ${intended:.2}, got ${usd:.2}"
        );
        assert!(usd < 8.0, "the old code committed $29.41 here; got ${usd:.2}");
        let expect_back = usd / 0.0349;
        assert!(
            (expect_back - 159.7).abs() < 12.0,
            "filling near his price should return ~159 shares, projects {expect_back:.1}"
        );
    }
    #[test]
    fn the_second_measured_fire_is_also_corrected() {
        let mut c = cfg("oversize2", 21, Sizing::Pct(0.015));
        c.buy_slippage_c = 0.15;
        c.per_market_usd = 10_000.0;
        c.max_usd_per_fill = 5_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("4000002", 0, 0.09898, 5_000.0, 5_000.0)).unwrap();
        let usd = i.usd as f64 / MICRO;
        assert!((usd - 7.42).abs() < 0.30, "expected ~$7.42 committed, got ${usd:.2}");
    }
    #[test]
    fn overshoot_is_BOUNDED_across_the_whole_price_range() {
        for px in [0.02f64, 0.05, 0.10, 0.25, 0.50, 0.80] {
            let mut c = cfg("range", 22, Sizing::Pct(0.015));
            c.buy_slippage_c = 0.15;
            c.min_buy_price = 0.01;
            c.per_market_usd = 100_000.0;
            c.max_usd_per_fill = 50_000.0;
            let r = Router::new(vec![ready_lane(c)]);
            r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
            let i = r.decide1(0, &dec("4000003", 0, px, 10_000.0, 10_000.0)).unwrap();
            let usd = i.usd as f64 / MICRO;
            let intended = 10_000.0 * px * 0.015;
            let ratio = usd / intended;
            assert!(
                ratio < 1.35,
                "at price {px} we commit {ratio:.2}x the intended dollars (was 5.3x at 3.5c)"
            );
        }
    }
    #[test]
    fn with_ZERO_slippage_pct_sizing_is_unchanged() {
        let mut c = cfg("nozero", 23, Sizing::Pct(0.015));
        c.buy_slippage_c = 0.0;
        c.per_market_usd = 10_000.0;
        c.max_usd_per_fill = 5_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("4000004", 0, 0.10, 5_000.0, 5_000.0)).unwrap();
        assert_eq!(i.shares, 75.0, "1.5% of 5000 with no pad must still be exactly 75");
    }
    #[test]
    fn BUY_slippage_is_wide_and_SELL_slippage_is_TIGHT_and_they_are_independent() {
        let mut c = cfg("asym", 9, Sizing::Shares(5.0));
        c.buy_slippage_c = 0.15;
        c.sell_slippage_c = 0.02;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let buy = r.decide1(0, &dec("2000001", 0, 0.50, 100.0, 100.0)).unwrap();
        assert!(
            (buy.limit - 0.65).abs() < 1e-9,
            "buy limit should be his 0.50 + 0.15 = 0.65, got {}", buy.limit
        );
        r.lane(0).unwrap().set_holding("2000002", 100.0);
        r.lane(0).unwrap().set_his("2000002", 100.0);
        let sell = r.decide1(0, &dec("2000002", 1, 0.50, 100.0, 100.0)).unwrap();
        assert!(
            sell.limit > 0.40,
            "sell limit must stay near his price, not fall to the buy width: {}", sell
            .limit
        );
    }
    #[test]
    fn a_wide_BUY_slippage_never_escapes_the_price_ceiling() {
        let mut c = cfg("ceil", 10, Sizing::Shares(5.0));
        c.buy_slippage_c = 0.15;
        c.max_buy_price = 0.95;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("2000003", 0, 0.90, 100.0, 100.0)).unwrap();
        assert!(i.limit <= 0.99 + 1e-9, "limit must clamp to 0.99, got {}", i.limit);
    }
    #[test]
    fn sell_slippage_of_1_0_with_zero_floor_reaches_the_tick() {
        let mut c = cfg("anybid", 12, Sizing::Shares(5.0));
        c.sell_slippage_c = 1.0;
        c.sell_floor_frac = 0.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        for (i, px) in [0.05f64, 0.25, 0.50, 0.75, 0.95].iter().enumerate() {
            let tok = format!("30000{i:02}");
            r.lane(0).unwrap().set_holding(&tok, 100.0);
            r.lane(0).unwrap().set_his(&tok, 100.0);
            let o = r.decide1(0, &dec(&tok, 1, *px, 100.0, 100.0)).unwrap();
            assert!(
                o.limit <= 0.03,
                "exit at his {px} must accept the bottom of the book, got {}", o.limit
            );
            assert!(o.limit >= 0.01 - 1e-9, "never below the venue tick");
        }
    }
    #[test]
    fn a_wide_SELL_slippage_never_goes_below_the_floor() {
        let mut c = cfg("floor", 11, Sizing::Shares(5.0));
        c.sell_slippage_c = 0.15;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        r.lane(0).unwrap().set_holding("2000004", 100.0);
        r.lane(0).unwrap().set_his("2000004", 100.0);
        let i = r.decide1(0, &dec("2000004", 1, 0.10, 100.0, 100.0)).unwrap();
        assert!(i.limit >= 0.01 - 1e-9, "limit must floor at 0.01, got {}", i.limit);
    }
    #[test]
    fn max_usd_per_fill_clamps_a_huge_pct_copy() {
        let mut c = cfg("big", 2, Sizing::Pct(0.10));
        c.per_market_usd = 10_000.0;
        let r = Router::new(vec![ready_lane(c)]);
        r.lane(0).unwrap().state.armed.store(true, Ordering::Relaxed);
        let i = r.decide1(0, &dec("1000001", 0, 0.60, 25_000.0, 25_000.0)).unwrap();
        assert!(i.usd as f64 / MICRO <= 250.0 + 1e-6, "max_usd_per_fill did not clamp");
    }
    #[test]
    fn config_with_unreachable_fill_clamp_is_detected() {
        let mut c = cfg("bad", 1, Sizing::Shares(5.0));
        c.per_market_usd = 200.0;
        c.max_usd_per_fill = 250.0;
        assert!(c.validate().is_err());
        c.per_market_usd = 500.0;
        assert!(c.validate().is_ok());
    }
    #[test]
    fn sells_bypass_entry_budgets_because_exiting_must_always_be_possible() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 100.0);
        r.lane(0).unwrap().set_his("1000001", 1000.0);
        r.lane(0)
            .unwrap()
            .state
            .spent_today
            .store((10_000.0 * MICRO) as Micro, Ordering::Relaxed);
        let i = r.decide1(0, &dec("1000001", 1, 0.80, 250.0, 5000.0)).unwrap();
        assert_eq!(i.side, 1);
        assert!(i.limit < 0.80, "exit limit should concede price to get out");
    }
    #[test]
    fn exit_is_sized_from_OUR_holding_not_his_fill() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 5.0);
        r.lane(0).unwrap().set_his("1000001", 10_000.0);
        let i = r.decide1(0, &dec("1000001", 1, 0.60, 5_000.0, 10_000.0)).unwrap();
        assert!(
            i.shares <= 5.0, "tried to sell {} shares of a 5-share position", i.shares
        );
        assert!(
            i.shares > 1.0, "a 50% exit of 5 shares should be ~2.5, got {}", i.shares
        );
    }
    #[test]
    fn a_near_full_exit_of_his_closes_ours_completely() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 10.0);
        r.lane(0).unwrap().set_his("1000001", 1000.0);
        let i = r.decide1(0, &dec("1000001", 1, 0.60, 960.0, 1000.0)).unwrap();
        assert!((i.shares - 10.0).abs() < 1e-6, "96% exit should close us fully");
    }
    #[test]
    fn we_never_mirror_an_exit_of_a_token_we_do_not_hold() {
        let r = router2();
        r.lane(0).unwrap().set_his("1000001", 5000.0);
        assert_eq!(
            r.decide1(0, & dec("1000001", 1, 0.60, 1000.0, 5000.0)),
            Err(Skip::SellNoPosition)
        );
    }
    #[test]
    fn legacy_positions_are_excluded_from_the_exit_fraction() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 10.0);
        r.lane(0).unwrap().set_his("1000001", 1100.0);
        r.lane(0).unwrap().set_legacy("1000001", 1000.0);
        assert_eq!(
            r.decide1(0, & dec("1000001", 1, 0.60, 550.0, 1100.0)),
            Err(Skip::SellNoPosition)
        );
        let r2 = router2();
        r2.lane(0).unwrap().set_holding("2000002", 10.0);
        r2.lane(0).unwrap().set_his("2000002", 1100.0);
        r2.lane(0).unwrap().set_legacy("2000002", 1000.0);
        let i = r2.decide1(0, &dec("2000002", 1, 0.60, 1050.0, 1100.0)).unwrap();
        assert!(i.shares > 1.0 && i.shares <= 10.0, "got {}", i.shares);
    }
    #[test]
    fn an_exit_never_exceeds_what_we_hold_even_if_he_dumps_everything() {
        let r = router2();
        r.lane(0).unwrap().set_holding("1000001", 3.0);
        r.lane(0).unwrap().set_his("1000001", 25_000.0);
        let i = r.decide1(0, &dec("1000001", 1, 0.60, 25_000.0, 25_000.0)).unwrap();
        assert!(i.shares <= 3.0 + 1e-9, "sold {} of a 3-share position", i.shares);
    }
    #[test]
    fn his_remaining_is_carried_for_the_hybrid_decision() {
        let r = router2();
        let i = r.decide1(0, &dec("1000001", 0, 0.60, 400.0, 5000.0)).unwrap();
        assert!(
            (i.his_remaining - 4600.0).abs() < 1e-6,
            "hybrid resting needs his REMAINING size, not his fill"
        );
    }
    #[test]
    fn a_repriced_lane_SIZES_DIFFERENTLY_without_a_restart() {
        let lane = ready_lane(cfg("hot", 1, Sizing::Pct(0.05)));
        let before = lane.policy();
        assert!(matches!(before.sizing, Sizing::Pct(p) if (p - 0.05).abs() < 1e-12));
        let next = SizingPolicy::build(
                0,
                20_000.0,
                Sizing::Pct(0.10),
                0.25,
                false,
                &crate::budget::Fracs::default(),
            )
            .unwrap();
        let gen = lane.apply_policy(next).unwrap();
        assert_eq!(gen, 1, "the applied generation must advance");
        let after = lane.policy();
        assert!(matches!(after.sizing, Sizing::Pct(p) if (p - 0.10).abs() < 1e-12));
        assert!(
            after.caps.max_open_usd > before.caps.max_open_usd,
            "raising the seed must raise the caps in the same swap"
        );
        assert!(after.caps.max_usd_per_fill <= after.caps.per_market_usd);
        assert!(after.caps.per_market_usd <= after.caps.max_open_usd);
    }
    #[test]
    fn an_INVALID_policy_is_REFUSED_and_the_live_one_survives() {
        let lane = ready_lane(cfg("hot", 1, Sizing::Pct(0.05)));
        let f = crate::budget::Fracs::default();
        for bad in [
            SizingPolicy::build(0, -1.0, Sizing::Pct(0.10), 0.25, false, &f),
            SizingPolicy::build(0, 10_000.0, Sizing::Pct(0.0), 0.25, false, &f),
            SizingPolicy::build(0, 10_000.0, Sizing::Pct(1.5), 0.25, false, &f),
            SizingPolicy::build(0, 10_000.0, Sizing::Pct(0.10), 0.0, false, &f),
        ] {
            assert!(bad.is_err(), "an out-of-range policy must not build");
        }
        let bad_fracs = crate::budget::Fracs { daily: -1.0, ..f };
        assert!(
            SizingPolicy::build(0, 10_000.0, Sizing::Pct(0.10), 0.25, false, &bad_fracs)
                .is_err(),
            "a policy with invalid cap fractions must not build"
        );
        assert_eq!(lane.policy().generation, 0, "nothing was applied");
        assert!(
            matches!(lane.policy().sizing, Sizing::Pct(p) if (p - 0.05).abs() < 1e-12)
        );
    }
    #[test]
    fn the_CEILING_still_clamps_after_a_hot_change() {
        let lane = ready_lane(cfg("hot", 1, Sizing::Pct(0.05)));
        let pol = SizingPolicy::build(
                0,
                10_000.0,
                Sizing::Pct(0.10),
                0.05,
                false,
                &crate::budget::Fracs::default(),
            )
            .unwrap();
        lane.apply_policy(pol).unwrap();
        let p = lane.policy();
        let eff = crate::budget::effective_pct(
            match p.sizing {
                Sizing::Pct(v) => v,
                _ => 0.0,
            },
            1.0,
            p.max_effective_pct,
            p.compound,
        );
        assert!(
            (eff - 0.05).abs() < 1e-12,
            "a 10% pct under a 5% ceiling is still 5% — the ceiling must move too"
        );
    }
    #[test]
    fn EVERY_sizing_reader_follows_a_hot_reprice_not_just_decide() {
        let lane = ready_lane(cfg("gen", 1, Sizing::Pct(0.05)));
        let boot = lane.policy();
        assert!(matches!(boot.sizing, Sizing::Pct(p) if (p - 0.05).abs() < 1e-12));
        lane.apply_policy(
                SizingPolicy::build(
                        0,
                        20_000.0,
                        Sizing::Pct(0.10),
                        0.25,
                        true,
                        &crate::budget::Fracs::default(),
                    )
                    .unwrap(),
            )
            .unwrap();
        let live = lane.policy();
        assert!(
            matches!(live.sizing, Sizing::Pct(p) if (p - 0.10).abs() < 1e-12),
            "policy() must report the applied generation"
        );
        assert!((live.max_effective_pct - 0.25).abs() < 1e-12);
        assert!(live.compound);
        assert!(
            live.caps.daily_usd > boot.caps.daily_usd,
            "derived caps must move with the seed, not stay at boot values"
        );
        assert!(
            matches!(lane.cfg.sizing, Sizing::Pct(p) if (p - 0.05).abs() < 1e-12),
            "cfg keeps boot values; only `policy()` is authoritative for sizing"
        );
    }
    #[test]
    fn a_lane_held_by_an_INCIDENT_still_follows_him_OUT() {
        let r = router2();
        r.lane(0).unwrap().state.halt_latch.store(true, Ordering::Relaxed);
        r.lane(0).unwrap().state.halted.store(true, Ordering::Relaxed);
        r.lane(0).unwrap().set_holding("1000001", 100.0);
        r.lane(0).unwrap().set_his("1000001", 1000.0);
        let i = r
            .decide1(0, &dec("1000001", 1, 0.60, 500.0, 1000.0))
            .expect("a lane latched by an INCIDENT must still be able to sell");
        assert_eq!(i.side, 1);
        assert!(i.shares > 0.0);
    }
