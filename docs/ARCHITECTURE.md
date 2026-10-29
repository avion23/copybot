# Copybot architecture: current system and target

**Status:** design target; not a claim that this refactor is complete. Baseline: `streamline-1` at `8745ee0` (2026-10-29). The first two migration steps below are deliberately small and independently gateable. **Do not fund/deploy solely because the refactor builds.**

## First-principles product and invariants

The product is a **single-operator, one-process Polygon CLOB copy executor** that responds to *observed* leader settlement activity, sizes follower orders, signs/submits to the configured CLOB, tracks ambiguous fills/resting orders, reconciles inventory, and exposes a local operator view. Its Python services observe and corroborate evidence; they do not independently own trading state. It is **not** an early-leader-order detector, backtester, generic on-chain strategy, or multi-tenant trading service.

1. **One live signal, with an explicit limitation.** Keep **pending `matchOrders` settlement over configured WebSockets** as the sole trading-signal intake. It is the only currently connected input that produces actual copy decisions (`hot/src/feeds.rs`, `calldata.rs`, `main.rs:7556-7750`). The operator's match was *already executed off-chain* before settlement broadcast. Therefore this cannot beat the same leader fill, regardless of provider race. Book-delta infers changes at a *price level*, not authenticated leader ownership, and its receiver is dropped at `main.rs:1959-1970`; it must **not** become a trading signal. The 20-second `txpool_content` poll is a slower parallel intake without measured incremental net value; remove it. **Do not delete the market book:** it still supplies prices, level/print evidence, and resting-order safety.
2. **Measured lead-time decision:** independently matched leader/follower exchange-stage timestamps are unavailable: **n=0** (see evidence `COPYBOT-FP/leader-lead-measurement.md`). Thus the choice is a **provisional correctness/scope choice, not a measured latency victory**. Maintain per-source local receive-to-decision, submit-to-ack, match-corroboration and missing-fill counts; only claim `leader_match_ms` vs `follower_match_ms` when the same stage has independent timestamp/occurrence IDs. The desired comparison is conditional on eligible, matched pairs grouped by market-day and source; do not substitute block time, API indexing time or `fingerprint.leads_ms`. Revisit the choice only with independent samples, no false-positive leader ownership, net-fee positive economics and an explicit change proposal.
3. **Single ownership, one wallet:** a typed state owner must atomically validate and reserve budget, limit, token and cash exposure before signing; one order identity, one WAL/pending journal, explicit commit/release on every success/rejection/unknown. Never lose an ambiguous order to a fallback. Persist what is needed for restart before submission and reconcile against venue *and* on-chain facts. Fee-equivalent strategy reservation and physical USDC cash requirement are different resources for share-denominated buy fees.
4. **Pure decisions, effectful edges:** decode calldata, choose route, size, compute exit fraction, fee bound, and classify custody from immutable typed inputs. Feed/RPC/HTTP/file I/O, clocks, keys, alerts and mutation authorization are passed at the boundary. Do not put `Arc<Mutex<...>>` in pure kernels; return typed `Decision | Refusal` and `Result<_, Error>` from I/O.
5. **Unverifiable is not healthy.** Invalid feed envelope/signature/protocol, unknown fees, stale cash, missing observer dependency or partial ledger snapshot must yield a visible, typed refusal/incident, never fabricated `CONFIRMED`, zeros, or false profit. Non-risk defaults such as display labels remain legitimate; the objective is **eliminating loss-of-truth fallbacks**, not blindly deleting every `unwrap_or`.
6. **Control is separate from observation.** No unauthenticated mutation API. Preserve the loopback read-only dashboard, remove mutating routes or put them behind real capability authentication (Origin/Content-Type are CSRF controls, not authentication). A single operator-control writer handles arm/disarm/flatten commands; one observer reports evidence and can request a halt through an authenticated, auditable interface. Observers never need `PRIVATE_KEY`.

## Current → target

| Current (2026-10-29 source) | Target ownership / observable invariant |
|---|---|
| `hot/src/main.rs` 9,472 lines: startup, feeds, matching, budgets, submission, exit rescue, reconcile, HTTP dashboard, control and tests mixed; 34 `tokio::spawn` sites reported in independent review | `main.rs` only composes dependencies, launches supervised tasks and returns typed process status; task handlers own one distinct operation; every source file **≤1,000 lines** after migration, no duplicated decision engine |
| `feeds.rs` pending WebSocket; optional `txpool.rs` poll every 20 s; `book.rs` public order book; `fingerprint.rs` unused candidate scoring with dropped receiver | **One authoritative pending WebSocket signal**. `book.rs` remains market-data-for-pricing and resting safety only; delete `txpool.rs` and `fingerprint.rs` paths. Keep deliberate provider redundancy *within one feed protocol* and one dedupe point. |
| `RawTx` identifies decoded maker address but not authenticated sender/order intent; V2 exchange-only filters | Typed `ObservedTx` → `VerifiedLeaderFill` with verified settlement origin, contract/version, validated order identity/signature and replay-proof occurrence. Unknown protocol refuses admission; unsupported modern position system not silently treated as CTF |
| `lanes.rs` mixes pure sizing/exits/routing with mutex-backed reservations; `main.rs` cash check double counted own reservation before prior patch | Pure `size_buy`, `price_route`, `exit_rule`, `fee_bound`; one `EngineState::reserve` returning scoped token. Fee-inclusive strategy budget; actual buy physical cash only order notional; pending recovery returns same commitments |
| `main.rs` signs/sends, cancels/retries, manages WAL/pending/ledger/recovery in many spawned closures | Single execution coordinator owns order lifecycle (`Planned → Reserved → Durable → Submitted → Matched/Resting/Rejected/Unknown`), WAL and idempotence; unknown outcome cannot auto-release funds or falsify a fill |
| `guardian.py` 2,077 lines, `watcher.py` 742, `fillwatch.py` 405, `buywatch.py` 154; different assumptions and stub imports | One `deploy/observer/` package with evidence adapters and **one** `Observation` envelope: `source, observed_at, lane, market, token, leader_order, follower_order, fill_occurrence, status(Confirmed/Refuted/Unverifiable), reason`. One schedule, dedup and alert policy; no fake modules and no local signing key |
| HTTP loopback dashboard at `127.0.0.1:8807` has POST arm/flatten/wallets without application authentication | Read-only dashboard by default. If remote mutations are required later, authorize each using a dedicated operator capability, strict same-origin policy, narrow methods and audit log; never trust loopback/proxy identity alone |
| ~372 Rust `unwrap_or*` occurrences by current grep; `watcher.py` and `guardian.py` install synthetic modules | Typed transport/parser/state/admission errors and fallible startup validation. Each ignored error must be proven non-money-impacting; leave benign optional UI defaults alone. Missing monitoring dependency fails the observer, not a fabricated success |

## Target modules, writers and hard line budgets

**Budgets are maximum final source lines per file**, including local tests; files may remain smaller. This is a design allocation, not permission for a cosmetic mass move. Split pure kernels when their current files already exceed the cap. Preserve the existing `hot/src/lib.rs` library and tiny `main.rs` binary entrypoint; compose only the modules that have a real independent responsibility.

| Module / suggested file | Sole responsibility, allowed I/O | Budget |
|---|---|---:|
| `main.rs` | Startup wiring, supervision, top-level exit status; no strategy, HTTP routing or ledger mutation | 250 |
| `runtime.rs`, `startup.rs` | Runtime dependencies, validated config, signer construction; startup is the only place reading live env | 650 each |
| `feeds.rs`, `book.rs` | Pending WebSocket ingest and public book/print cache, respectively; only external reads / bounded queues | 500 / 1,000 |
| `signal.rs`, `calldata.rs`, `signal_guard.rs` | Typed admission and protocol validation, pure decode, durable dedup; no network in decode | 750 / 400 / 1,000 |
| `sizing.rs`, `budget.rs` | Pure entry sizing and cash/exposure constraints; valid fee-rate bounds | 750 / 900 |
| `routing.rs`, `venue.rs`, `order.rs` | Pure FAK/GTC routing, tick/limits, typed signed-order construction; only signing at order adapter | 650 / 600 / 850 |
| `exits.rs`, `exit_rescue.rs`, `resting.rs` | Pure partial/full exit rule; rescue, rest/cancel lifecycle at effectful edges | 800 / 950 / 900 |
| `execution.rs`, `race_send.rs`, `pending.rs`, `wal.rs` | One order coordinator, HTTP transports, journal, durable write-ahead; one authoritative order state machine | 950 / 900 / 1,000 / 850 |
| `accounting.rs`, `ledger.rs`, `risk.rs` | Booking/reconciliation dispatch, pure accounting kernel and risk updates | 850 / 950 / 600 |
| `reconcile.rs`, `custody.rs`, `settlement.rs` | External position checks, pure custody classifications, chain settlement evidence; unknown stays unknown | 950 / 800 / 750 |
| `dashboard.rs`, `control.rs`, `wallets.rs` | Read-only HTTP views, one authenticated command receiver/state writer, lane registry | 950 / 900 / 800 |
| `telemetry.rs`, `errors.rs`, `matchup.rs` | Structured event and comparable-stage metrics, typed error taxonomy, external KPI read models | 650 / 400 / 800 |
| `deploy/observer/{main,adapters,corroborate,alerts}.py` | Single process/scheduler, protocol reads, source-grade classification, alert delivery; zero signer use | 450 / 900 / 850 / 450 |

For other existing small modules (`auth`, `merge`, `txsend`, `funding`, `positions`, etc.), retain them **only where a live caller exists** and keep each ≤1,000 lines. A coherent 9,472-line entrypoint cannot responsibly become only seven ≤1,000-line modules; the adapters/reconciliation/state writer above account for the remaining work. `ledger.rs` (1,125), `control.rs` (1,407), `lanes.rs` (2,926), `pending.rs` (1,360) and `guardian.py` (2,077) also need ownership-based extraction, not cosmetic renaming.

## Message contract, ownership and failure modes

```text
configured pending WS (one live detection protocol)
  -> bounded validated envelope [source, tx, seen_at]
  -> verified and deduplicated leader settlement occurrence
  -> PURE decode, eligibility, sizing, routing, exit rule
  -> SINGLE state-owner plan/reserve+durable WAL -> signer -> CLOB submit
  -> accepted / resting / matched / rejected / unknown
  -> SAME owner updates pending, ledger, inventory, budget and recovery
  -> read-only event snapshot -> dashboard and observer corroboration
  -> observer emits Confirmed / Refuted / Unverifiable + authenticated halt request
```

All async tasks send typed messages to the owner; no spawned closure independently updates wallet, position, pending and reservation maps. The owner may run in a single task and serialize only state transitions, leaving network I/O outside the critical section. Explicit bounded ingress has a tracked overload policy: never silently drop an exit, incident or recovery event; halt new buys when the evidence queue becomes stale. Recovery replays a single durable order identity before re-enabling buys.

## Migration order — smallest complete, four-gate-clean steps

Each step is independently committed only after `cargo build --locked --release --manifest-path hot/Cargo.toml`, `cargo test --locked --manifest-path hot/Cargo.toml --lib --bin copybot-hot`, `.venv/bin/python -m pytest deploy/ -q` and `python3 scripts/check_release.py` pass. Run the generated-doc check for documentation changes. Push `streamline-1` to `origin` after a green commit; never push upstream. Offline tests must compare *behaviour* (fills/limits/reservations/state), not just compilation. Do not change live credentials or service deployments.

1. **A1 — delete untraded fingerprint candidate route.** Remove `fingerprint.rs`, the channel with `_fprx`, scoring/learning, dashboard synthetic "fingerprint" status and watch-list pinning. Keep the real book levels, prints, price cache and resting safety. Gate: Rust tests for book frames/levels/resting still pass; no `fingerprint::` call site or second signal sender; the primary `RawTx` pipeline unchanged. No latency claim.
2. **A2 — delete optional txpool rescue.** Remove `txpool.rs`, `Bot.txpool_rpc`, 20-second poll task, misleading "rescued" counter, and its tests/docs if any. Pending WebSocket and dedup stay. Gate: no `txpool_content` call or config field in the runtime; no unbounded alternative ingress added; source-provenance stats still work. Any old `txpool_rpc` config is obsolete and must be removed before deployment; no compatibility shim unless explicitly ordered.
3. **A3 — verify leader signal before spend.** Trace settlement caller and signed order identity; reject bad signatures, mismatched maker/contract, replay, unexpected protocol. Safe to build before performance work; add adversarial offline fixtures only where genuine protocol evidence is available.
4. **A4 — single reservation owner.** Move all pre-admission checks and reservations into one transactional state update with explicit refusal release and ambiguous retention; prove $100 cash/$60 intent permits the entry when caps allow; simulate concurrent lanes, restart, fees withheld in shares.
5. **A5 — validate fee and exits against market metadata.** Distinguish charged USDC versus withheld shares, rebates, and unsupported fee regimes. Keep new default proportional exits; protect sale floors, per-market maker/taker economics.
6. **A6 — extract pure kernels**, in order decode → route → sizing → exits → custody classification, without network reads or global state. Retain externally witnessed input/output semantics and the gate counts.
7. **A7 — make state and errors explicit.** Introduce one state owner + typed domain errors, remove its shadow `Arc<Mutex>` writes. Eliminate unsafe `unwrap_or*` and fake Python imports from money/health boundaries; fail on missing data. Recheck restart and WAL on each extraction.
8. **A8 — split supervised tasks from `main.rs` by ownership.** Peel feeds, signal, execution, exits, accounting/reconcile, dashboard, control into the target modules, finishing with all source files ≤1,000 lines. No broad rewrite or simultaneous ledger/execution semantics change.
9. **A9 — unify observers.** One `Observation` schema and module package, one alert/incident owner, one stop/kill interface; stop legacy units only in a separate approved rollout. Never let a watchdog absence become `CONFIRMED`.
10. **A10 — close control-plane exposure.** Remove write routes, or add real operator authentication/authorization before enabling any route; keep read-only local dashboard. Change routing/ops docs with migration checks and test rejection for unauthenticated mutations.
11. **A11 — measure and only then optimize.** Emit full event-stage timestamps/IDs per feed source; validate independent leader/follower match pairs and fee-adjusted tracking; use p95/p99 source-attributed useful fills and provider cost, not "30%" marketing claims. Do not add alternative detection until measured independent signal identity and net benefit beat one-path baseline.

**Stop and rollback rules:** If a gate fails, do not commit or push; fix the cause and rerun. If an input identity cannot be verified, record `Unverifiable`, halt new buys and preserve exit/reconcile authority. Pending order recovery must survive restart exactly once. Keep one heavy Cargo job at a time, at most two Cargo workers only after verifying ≥25 GiB available, and delete the owned `target` directory when the phase is complete. Historical advisory `RUSTSEC-2026-0387` in `crypto-bigint 0.5.5` remains a separate major-version signing dependency issue, **not** cleared by this architecture plan.
