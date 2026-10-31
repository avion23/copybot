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

The low-traffic confirmed-fill channel uses the existing `[[feed]]` schema. Set `name = "leader-logs"` (or `leader-logs-<label>`) to select it explicitly; any other name retains the legacy pending-transaction channel. There is **no automatic downgrade to the pending firehose** if log verification, RPC lookup, subscription or decoding fails.

```toml
[[feed]]
name = "leader-logs"
url = "wss://polygon-bor-rpc.publicnode.com"
sockets = 1
```

This public endpoint is a **no-SLA example**, not a recommended production SLA. The program derives the required transaction/receipt JSON-RPC HTTP endpoint by replacing this URL's `wss://` prefix with `https://` and preserving the host, path and query; choose a provider whose WSS and HTTPS RPC share the same endpoint path and whose plan supports `eth_subscribe` (`logs` and `newHeads`), `eth_getTransactionReceipt` and `eth_getTransactionByHash`. A `ws://` URL is refused for the confirmed log source. Use only a Polygon mainnet provider; never put credentials in a checked-in TOML file, telemetry or a support log.

One socket subscribes to **four filtered `OrderFilled` topics** (two V2 CTF exchanges × maker/taker proxy position), plus `newHeads` for stream liveness and registry-change resubscription. The canonical mined receipt must contain the identical event, successful status and configured leader topic; the corresponding transaction must contain supported `matchOrders` calldata and a matching leader order. Only then does the original decoder, replay guard and decision path receive the confirmed transaction. The leader's index topic is the **proxy wallet**, not the relayer's transaction sender. No order signing or submission is triggered by the subscription itself.

A second `[[feed]]` with a different name and a provider-supplied pending WSS URL can be configured as a **speculative mempool accelerator**, but that re-enables broad pending traffic and the original unconfirmed calldata-maker identity limitation. For cost-constrained monitoring use only `leader-logs`; do not claim pending data is authenticated by a mined receipt. The CLOB market WebSocket cannot identify a wallet and must remain market data, never a leader signal. The public Data API `/activity?user=<proxy>&type=TRADE` is useful as out-of-band reconciliation, not a lower-latency authenticated trigger; `/trades` may show fewer recent records.

This change does not authorize live trading: the 24-hour dry observation and existing order/journal safety gates remain binding. On any prolonged disconnection or resubscription, missing historical events are **not recovered by the WebSocket alone**; independently reconcile Data API and receipts before considering the stream complete.

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
