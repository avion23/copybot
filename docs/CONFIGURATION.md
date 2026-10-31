# Configuration

The engine retains its existing TOML configuration format; runtime operator and registry state remain JSON. Packaging has not introduced a new configuration parser or strategy model.

## Configuration layers

| Layer | Location | Purpose |
| --- | --- | --- |
| Boot configuration | `deploy/copybot2.toml` | Custody, feeds, initial lanes, sizing, execution |
| Process environment | `deploy/copybot.env` | Signing key, observer configuration, feature gates |
| Operator state | Under configured `control_path` | Arm/off/halt intent and related state |
| Accounting and recovery | Configured ledger and `data/`, `run/` | Durable execution and reconciliation evidence |

Only examples ship. Do not commit any populated layer. The bot does not load `.env` files automatically; the service templates use systemd's `EnvironmentFile` mechanism.

## Leader-fill detection (COPYBOT-FEED)

**Default: leader-only confirmed logs, not a mempool firehose.** The existing `[[feed]]` schema selects the filtered-log channel for ordinary feed names, including `primary` and `leader-logs`. A pending firehose requires an explicit `name = "mempool"` or `name = "pending-paid"` (or another `mempool-`/`pending-` name). There is **no automatic downgrade to pending** if log verification, RPC lookup, subscription or decoding fails. If no feed is configured, the process does not invent a connection.

```toml
[[feed]]
name = "leader-logs"
url = "wss://polygon-bor-rpc.publicnode.com"
sockets = 1
```

This PublicNode endpoint is **keyless/free with no advertised monthly-CU allowance**, but does not promise unlimited traffic, 24-hour uptime, or an SLA. Rate limits/outages are still possible. The free source admits only `wss://polygon-bor-rpc.publicnode.com` and `wss://polygon.drpc.org` (optional trailing slash); any prior Alchemy, unknown, or metered URL is **refused**, not silently used. The program derives the HTTPS JSON-RPC endpoint by replacing `wss://` with `https://` without altering the remaining path. Select a provider supporting `eth_subscribe(logs)`, `eth_blockNumber`, `eth_getLogs` and `eth_getTransactionByHash` on both WSS/HTTPS. The log source refuses `ws://`. A prior public-endpoint test also accepted the filtered subscriptions on dRPC and QuickNode's shared documentation demo, but the latter is not an assured production service; the keyless polygon-rpc.com/Ankr endpoints tested did not work. A free account tier with an exhaustible monthly cap is **not** the designated default. Never place credentials in checked-in TOML or telemetry.

One socket subscribes to **two leader-filtered `OrderFilled` topics** (maker and taker proxy positions, both V2 exchange addresses OR-ed per filter). It does **not** subscribe to all pending transactions or `newHeads`; a small `eth_blockNumber` query once a minute checks liveness without receiving everyone else's block headers. For a leader event, the corresponding canonical block is queried with the *same leader-only `eth_getLogs` filters*: exact log index, contract, data, and block hash must match. Only that leader's transaction and the identified block timestamp are fetched. The V2 event data provides actual token ID, maker BUY/SELL side, execution size and execution price (USDC/share); its maker topic must equal the configured proxy wallet, **not merely its taker-counterparty topic**, and supported `matchOrders` calldata must contain the same signed maker order and real salt/condition. The typed confirmed leader-fill passes the existing replay guard and admission. In an 85-transaction sample, all 85 had a direct maker event matching the Data API size (85/85); merely matching the taker topic would attribute unrelated counterparties incorrectly. Nonleader receipts and unrelated transaction objects are never requested. A reverted transaction cannot emit canonical mined logs. No order signing or submission is triggered by the subscription itself.

A second `[[feed]]` with `name = "mempool"` and a provider-supplied paid pending WSS URL can be configured **only after an explicit operator decision to pay** as a speculative accelerator. It is disabled by default, re-enables broad pending traffic and retains unconfirmed calldata-maker identity limitations. For cost-constrained monitoring use only `leader-logs`; do not claim pending data is authenticated by a mined receipt. The CLOB market WebSocket cannot identify a wallet and must remain market data, never a leader signal. A **single automatic corroboration observer** polls the public Data API `/trades?user=<proxy>&limit=10` every 2 seconds for configured leader wallets while the filtered-log source is selected. It has no signing key or order permission: API rows are checked against canonical maker logs and the signed settlement transaction before a typed `DataApiCorroboration` event may be emitted. A 429 is a logged refusal with a bounded pause, not permission to switch to paid traffic. `/activity` historically had more rows in the 24-hour sample, so `/trades` alone is not proof of coverage.

**Typed gap recovery:** on cold boot the socket subscribes first, then binary-searches Polygon block timestamps for the last 24 hours and backfills only leader-indexed `eth_getLogs` in bounded 512-block chunks. A scanned-through block cursor is retained across WebSocket reconnects; every reconnection and periodic block heartbeat scans only the missing range. A chunk advances the cursor only after all its maker events have been independently verified and delivered. A range exceeding 100,000 blocks, an invalid block timestamp, RPC gap/error, unsupported event, or mismatched calldata explicitly refuses progress; no blind jump to the current head. All recovered and API-corroborated events are **observation-only** and cannot place stale copy orders. The cursor is process-local; a restart replays the last 24 hours as observation evidence rather than claiming a durable uninterrupted cursor. This does not authorize live trading: the separate dry observation and execution/journal safety gates remain binding.

## Sizing

`mode = "pct"` uses a fractional copy percentage: `0.005` means 0.5%, not 5%. `shares` and `usd` are also accepted by the existing parser. Percentage copying is subject to minimum order sizes, budgets, execution prices, and the configured exit policy; it is not a guarantee of an exact share ratio.

Use either `bankroll_usd` with derived budget fractions or the parser's absolute-cap mode. Do not mix derived and absolute caps. Set measured `leader_max_order_usd` and `leader_peak_exposure_usd` for your selected leader. The engine validates these rather than borrowing another leader's statistics.

`compound` controls the existing realized-gain/loss sizing behavior. Set it intentionally. A lane budget is a strategy allocation inside a shared account, not security isolation between different owners.

Entries reserve the worst-case published taker fee at up to 7% base rate: `fee = shares × rate × p × (1-p)`, with the maximum over all prices at or below the buy limit. This reduces available fill, market, daily, and open budgets before submission. For taker buys, the fee is withheld in shares rather than requiring additional USDC. The fee-equivalent reduces strategy budget headroom, while the physical cash check requires only the submitted order notional. The venue-reported fee remains authoritative for ledger P&L; the reserve is a conservative sizing bound, not a per-market fee quote. Update the bound if Polymarket changes its fee schedules or exponent.

## Execution

The parser accepts `taker` or `hybrid`. Split buy/sell slippage fields take precedence over the legacy combined field. `copy_makers` and `copy_maker_sells` are separate choices. Preserve them when transferring a lane between boot configuration and the runtime registry.

Do not infer units from the historical `_c` suffix alone: inspect `hot/src/config.rs` and `hot/src/lanes.rs` for the value's actual use. This export preserves those calculations.

The default `sell_all_frac = 1.0` mirrors an eligible partial leader sell proportionally and flattens only at a full exit. `sell_all_frac = 0.0` explicitly opts into flatten-on-first-sell; existing persisted zero settings are not rewritten. Values outside [0,1] are rejected. `sell_floor_frac` constrains the exit price relative to the leader; allowing broad slippage can fill at a substantially worse price. Setting it to zero does not guarantee a full fill in an empty market.

## Observers

All services need the same install directory and engine port. The legacy observer wallet environment variables must be supplied for your instance. Runtime lane discovery and shared-wallet checks still use the engine's API; these observers are not interchangeable with a multi-tenant permissions system.

Notification credentials and recipients are optional and empty in the template. `GUARDIAN_ENFORCE`, `MERGE_RECONCILE`, `COPYBOT_MERGE_ENABLE`, and `ORPHAN_SWEEP` retain their existing meanings. The example leaves optional live-action gates off; enabling an on-chain feature requires the corresponding custody/RPC setup. Do not set optional gates simply to clear a warning.
