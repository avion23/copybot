# Copybot architecture: current system and target

**Status:** design target; not a claim that this refactor is complete. Reviewed source at `streamline-1` after A1 commit `c0452bc` (2026-10-29); A2 removes the optional txpool input in the follow-up code commit. First are the ideal Rust interfaces, then a line-referenced comparison, then the layered module budgets and migration steps. **Do not fund/deploy solely because the refactor builds.**

## First-principles product and invariants

The product is a **single-operator, one-process Polygon CLOB copy executor** that responds to *observed* leader settlement activity, sizes follower orders, signs/submits to the configured CLOB, tracks ambiguous fills/resting orders, reconciles inventory, and exposes a local operator view. Its Python services observe and corroborate evidence; they do not independently own trading state. It is **not** an early-leader-order detector, backtester, generic on-chain strategy, or multi-tenant trading service.

1. **One live signal, with an explicit limitation.** Keep **pending `matchOrders` settlement over configured WebSockets** as the sole trading-signal intake. It is the only currently connected input that produces actual copy decisions (`hot/src/feeds.rs`, `calldata.rs`, `main.rs:7556-7750`). The operator's match was *already executed off-chain* before settlement broadcast. Therefore this cannot beat the same leader fill, regardless of provider race. Book-delta inferred changes at a *price level*, not authenticated leader ownership; A1 `c0452bc` has already deleted its dropped-receiver, scorer and module. It must **not** become a trading signal. The optional 20-second `txpool_content` poll was a slower parallel intake without measured incremental net value; A2 deletes it. **Do not delete the market book:** it still supplies prices, level/print evidence, and resting-order safety.
2. **Measured lead-time decision:** independently matched leader/follower exchange-stage timestamps are unavailable: **n=0** (see evidence `COPYBOT-FP/leader-lead-measurement.md`). Thus the choice is a **provisional correctness/scope choice, not a measured latency victory**. Maintain per-source local receive-to-decision, submit-to-ack, match-corroboration and missing-fill counts; only claim `leader_match_ms` vs `follower_match_ms` when the same stage has independent timestamp/occurrence IDs. The desired comparison is conditional on eligible, matched pairs grouped by market-day and source; do not substitute block time, API indexing time or `fingerprint.leads_ms`. Revisit the choice only with independent samples, no false-positive leader ownership, net-fee positive economics and an explicit change proposal.
3. **Single ownership, one wallet:** a typed state owner must atomically validate and reserve budget, limit, token and cash exposure before signing; one order identity, one WAL/pending journal, explicit commit/release on every success/rejection/unknown. Never lose an ambiguous order to a fallback. Persist what is needed for restart before submission and reconcile against venue *and* on-chain facts. Fee-equivalent strategy reservation and physical USDC cash requirement are different resources for share-denominated buy fees.
4. **Pure decisions, effectful edges:** decode calldata, choose route, size, compute exit fraction, fee bound, and classify custody from immutable typed inputs. Feed/RPC/HTTP/file I/O, clocks, keys, alerts and mutation authorization are passed at the boundary. Do not put `Arc<Mutex<...>>` in pure kernels; return typed `Decision | Refusal` and `Result<_, Error>` from I/O.
5. **Unverifiable is not healthy.** Invalid feed envelope/signature/protocol, unknown fees, stale cash, missing observer dependency or partial ledger snapshot must yield a visible, typed refusal/incident, never fabricated `CONFIRMED`, zeros, or false profit. Non-risk defaults such as display labels remain legitimate; the objective is **eliminating loss-of-truth fallbacks**, not blindly deleting every `unwrap_or`.
6. **Control is separate from observation.** No unauthenticated mutation API. Preserve the loopback read-only dashboard, remove mutating routes or put them behind real capability authentication (Origin/Content-Type are CSRF controls, not authentication). A single operator-control writer handles arm/disarm/flatten commands; one observer reports evidence and can request a halt through an authenticated, auditable interface. Observers never need `PRIVATE_KEY`.

## Ideal interfaces, bottom-up (design contracts, not implemented APIs)

**These are target Rust signatures, not exported symbols of the current library.** Newtypes `WalletId`, `MarketId`, `TokenId`, `OrderId`, `FillId`, `PriceTicks`, `Shares`, `UsdMicros` and `SourceId` encode distinct units/identities; `ReceiptClock` retains both local monotonic and UTC receipt time. Convert exact venue decimals at the transport edge; no float rounding or zero-as-unknown sentinel in core decisions. Every alternative below is a **tagged enum**, not a magic string or boolean. `Skipped` and `Unverifiable` are normal, visible outcomes, never successful fills. `async fn` signatures are illustrative; a final object-safe implementation may use concrete adapters rather than dyn traits.

```rust
// Domain schema crossing a boundary: source ownership and stage clocks travel with data.
struct ReceiptClock { monotonic_ns: u128, utc_ns: u128 }
struct Received<T> { source: SourceId, received_at: ReceiptClock, payload: T }
enum Side { Buy, Sell }
enum SignalConfidence { VerifiedSettlement, Unverifiable(SignalReason) }
struct LeaderEvent {
    wallet: WalletId, market: MarketId, token: TokenId, side: Side,
    price: PriceTicks, size: Shares, observed_at: ReceiptClock,
    source: SourceId, confidence: SignalConfidence,
    order: OrderId, occurrence: FillId,
}
enum Intent {
    Enter { size: Shares, band: PriceBand, reason: DecisionReason },
    Exit  { size: Shares, band: PriceBand, reason: DecisionReason },
    Skip  { reason: SkipReason },
}
enum Admission { Sized(SizedIntent), Rejected(RiskReason), Skipped(SkipReason) }
enum OrderOutcome {
    Accepted { id: OrderId }, Resting { id: OrderId },
    Matched { fills: Vec<FillEvent> }, Rejected { reason: VenueReason },
    Unknown { id: OrderId, reason: VenueReason },
}
enum ObservationStatus { Confirmed, Refuted, Unverifiable }
enum OperatorCommand { Arm { lane: LaneId }, HaltBuys { lane: LaneId },
                       Disarm { lane: LaneId }, Flatten { lane: LaneId, reason: String } }

// L0 / transport edge. Only PendingSettlementTx may become a leader trading signal.
// Contract: stamp receipt *at ingress*, validate envelope/size, and surface disconnect/errors.
trait PendingSource {
    async fn next(&mut self) -> Result<Received<PendingSettlementTx>, TransportError>;
}
// Contract: book/price updates support routing and resting safety; never infer leader identity.
trait MarketDataSource {
    async fn next(&mut self) -> Result<Received<BookUpdate>, TransportError>;
}
// Contract: RPC is for chain state/receipt corroboration only, not a second trade trigger.
trait ChainReader {
    async fn receipt(&self, tx: TxHash) -> Result<ChainEvidence, ChainReadError>;
}

// L1 / signal kernel. No I/O, clocks, signing keys, Mutex, or cached mutable state.
// Contract: return an identified, authentic leader fill or an explicit refusal/unknown protocol.
trait LeaderDetector {
    fn detect(&self, tx: &Received<PendingSettlementTx>,
              leaders: &LeaderSet) -> Result<Option<LeaderEvent>, SignalError>;
}
// Contract: advance replay/order-occurrence watermark exactly once using the state owner.
trait SignalAdmission {
    fn admit(&mut self, event: LeaderEvent)
        -> Result<VerifiedLeaderEvent, AdmissionError>;
}

// L2 / pure decision. Snapshot inputs only; no network or state mutation.
// Contract: one event plus the observed portfolio and lane policy yields an explicit intent.
trait DecisionKernel {
    fn decide(&self, event: &VerifiedLeaderEvent,
              portfolio: &PortfolioSnapshot, lane: &LaneConfig) -> Intent;
}
// Contract: price/route and proportional exit are deterministic functions, not async jobs.
trait RouteAndExit {
    fn route(&self, intent: &Intent, book: &BookSnapshot) -> Result<Route, RouteError>;
    fn exit_rule(&self, event: &VerifiedLeaderEvent,
                 held: &PositionSnapshot, policy: &ExitPolicy) -> Intent;
}

// L3 / pure sizing and risk; two different resources: fee-inclusive exposure and cash debit.
// Contract: validate venue minimum, exposure/cash/daily/market caps or explain rejection.
trait RiskSizer {
    fn size(&self, intent: &Intent, budget: &BudgetSnapshot,
            fees: &VerifiedFeeSchedule) -> Admission;
}

// L4 / state owner + execution edge. Only the actor may mutate reservations/WAL/order status.
// Contract: make a durable, uniquely identified reservation before signing; unknown retains it.
trait OrderStateOwner {
    fn reserve(&mut self, sized: SizedIntent) -> Result<ReservationId, StateError>;
    fn record(&mut self, id: ReservationId, event: OrderOutcome)
        -> Result<StateDelta, StateError>;
    fn snapshot(&self) -> EngineSnapshot;
}
// Contract: pure venue encoding from an admitted order; never sign an unreserved proposal.
trait OrderEncoder {
    fn encode(&self, order: &ReservedOrder) -> Result<OrderRequest, EncodeError>;
}
// Contract: sign locally; only this capability can access signing material.
trait OrderSigner {
    fn sign(&self, request: OrderRequest) -> Result<SignedOrder, SigningError>;
}
// Contract: batch post/cancel returns per-order acknowledgements; fills have distinct identities.
trait VenueClient {
    async fn post_batch(&self, orders: &[SignedOrder]) -> Result<Vec<OrderOutcome>, VenueError>;
    async fn cancel_batch(&self, ids: &[OrderId]) -> Result<Vec<CancelOutcome>, VenueError>;
    async fn next_fill(&mut self) -> Result<Received<FillEvent>, VenueError>;
}

// L5 / accounting (single-owned ledger). Venue evidence, not submit intent, creates inventory.
// Contract: idempotently book a uniquely identified fill with explicit fee currency/units.
trait Accounting {
    fn apply_fill(&mut self, fill: &FillEvent) -> Result<AccountingDelta, AccountingError>;
    fn view(&self) -> PortfolioSnapshot; // positions, realised/unrealised P&L, fees/rebates
}
// Contract: classify independently observed custody with Unverifiable for incomplete reads.
trait CustodyKernel {
    fn classify(&self, chain: &ChainPosition, venue: &VenuePosition,
                book: &PortfolioSnapshot) -> CustodyVerdict;
}

// L6 / oversight. Only one observer owns health, incident correlation and halt requests.
// Contract: signed/identified evidence; absent modules and stale sources are Unverifiable.
trait Observer {
    fn inspect(&mut self, evidence: ObservationEvidence) -> Observation;
    fn health(&self) -> HealthSnapshot;
    fn incidents(&self) -> Vec<Incident>;
    fn halt_request(&self, incident: IncidentId) -> Option<OperatorCommand>;
}

// L7 / operator edge. Reader sees immutable snapshots; command writer authenticates every mutation.
// Contract: dashboard GET cannot acquire signing, control, or reservation capabilities.
trait DashboardView {
    fn snapshot(&self, state: &EngineSnapshot) -> DashboardSnapshot;
}
// Contract: reject missing/invalid credentials, authorize exact action, then audit command.
trait OperatorSurface {
    fn authorize(&self, credential: &OperatorCredential,
                 command: &OperatorCommand) -> Result<AuthorizedCommand, AuthzError>;
    fn apply(&mut self, command: AuthorizedCommand) -> Result<CommandReceipt, ControlError>;
}
```

**Layer flow:** `PendingSource → LeaderDetector → SignalAdmission → DecisionKernel → RiskSizer → OrderStateOwner → OrderEncoder/Signer/VenueClient → Accounting → read models → Observer + DashboardView`. `MarketDataSource` only feeds `BookSnapshot` for route/rest decisions. `ChainReader` only corroborates receipts/custody. **Transport and execution own I/O, the state owner owns WAL/reservations/order transitions and ledger write authority, and other layers receive immutable projections.** Async tasks send messages to the owner; they do not hold `Arc<Mutex<Ledger>>` or edit operator intent directly. Recover durable unknown orders before accepting new buys. The dashboard has no mutation API unless an `OperatorCredential` is actually verified (not a Content-Type or Origin check).

### Ideal interface vs actual Copybot at `c0452bc`

All line references below are to this worktree after the completed fingerprint deletion A1 and before A2. `missing` means **not provided with the interface guarantees above**, not proof that every underlying function is absent.

| Ideal layer/interface | Copybot today (file:line) | Missing or duplicated; wrong-layer coupling |
|---|---|---|
| **Transport / `PendingSource`, `MarketDataSource`, `ChainReader`** | `feeds.rs:8-23,175-216,251-391` exposes `RawTx { source, hash, input, seen_ns }`; `book.rs:208-355` receives separate price/print/level WS; `txpool.rs:13-83` polls `txpool_content` each 20s, attached at `main.rs:4057-4063` | `RawTx` lacks verified sender, protocol and stage-typed receipt. `tokio::mpsc::unbounded_channel` at `main.rs:1955` has no pressure policy. Txpool duplicates leader ingress, with no measured incremental fills. Book WS must stay as market data, not a second detection path. |
| **Signal / `LeaderDetector`, `SignalAdmission`** | Pure-ish `calldata.rs:95-208` decodes `matchOrders`; `main.rs:7528-7650` intersects participants and wallets; `signal_guard.rs:12-60,120-200` retains progress/WAL | Decoding returns `Vec`/empty on invalid input, not typed error; parsed maker field is not cryptographic proof of signed leader intent or correct settlement origin; admission/dedupe/order WAL are interwoven with `main`. |
| **Decision / `DecisionKernel`, `RouteAndExit`** | `lanes.rs:43-80,456-628,705-751` combines sizing, default proportional exits, rest-vs-take routing and state mutation; `main.rs:7730-7950` branches on outcome | Intent uses `side: u8`, numeric `route`/zero values rather than exhaustive Enter/Exit/Skip. The router writes reservations while deciding; cannot replay the same inputs as a pure deterministic kernel without shared state. |
| **Risk / `RiskSizer`** | `budget.rs:102-165` holds pure fee/cash helpers; `lanes.rs:565-629` enforces caps and reserves; `main.rs:7766-7818` checks a separately fetched cash snapshot | Cash/fee economics improved by `a20efed`/`8745ee0`, but caps/admission still cross router and main and lack an atomic shared-wallet owner; live market-specific fee schedule and clear unknown-fee refusal are absent. |
| **Execution / `OrderStateOwner`, `OrderEncoder`, `OrderSigner`, `VenueClient`** | `order.rs:42-190` builds/signs EIP-712; `race_send.rs:3-38,129-193` races signed HTTP; `main.rs:1366-1439,7950-8470` records pending, submits, retries, books; `pending.rs:12-25,225-499` persists unresolved | No one typed durable order state machine; per-order race/async closures, resting cancellations and provisional booking are distributed. Venue batches/fill identities are not expressed as one per-order `Result` contract. |
| **Accounting / `Accounting`, `CustodyKernel`** | `control.rs:134-280` parses receipts and fees then books via `ledger.rs:214-263,509-560,638-730`; `custody.rs:12-55` has pure `classify`; `main.rs:2340-2750` drives recovery | Network response parsing, fee units and ledger writes cross `Control`. Guardian and engine compute overlapping inventory views; missing fee fields may become zero, not typed Unverifiable. Exactly-once fill identity belongs at the ledger edge. |
| **Oversight / `Observer`** | `deploy/{guardian,watcher,fillwatch,buywatch}.py` sizes 2,077/742/405/154; `guardian.py:9-55,574-588`, `watcher.py:13-37` permit substitute imports; `fillwatch.py` is partly corrected | Four independent schedules/health definitions and duplicate position joins, no single `Confirmed/Refuted/Unverifiable` event. Stub import can fabricate `CONFIRMED`; enforcement of guardian halts depends on `GUARDIAN_ENFORCE=1`. |
| **Operator surface / `DashboardView`, `OperatorSurface`** | `main.rs:478-534,929-1152,2935-2995,3612-3759` binds loopback and accepts `POST /api/arm`, `/api/flatten`, `/api/wallets`; `control.rs:285-405` writes arm/halt/health | Origin/JSON Content-Type check is not authentication; requests without Origin may pass. Route handler manipulates filesystem/control state in the dashboard layer, while independent service scripts also request operator changes. Read-only views and authorized writes should have separate capabilities. |

**Boundary rules:** A `LeaderEvent` may carry `SignalConfidence::Unverifiable` only for inspection; `SignalAdmission::admit` may return `VerifiedLeaderEvent` **only** on verified identity, supported protocol and replay-safe occurrence. No observer or dashboard can forge it. Pending `matchOrders` is still **post-leader-match**; `fingerprint.rs` is already removed in A1, so there is no live empirical head-to-head with that detector (independent match-time samples `n=0`). This target must not be mistaken for the present implementation.

## Current → target

| Current (2026-10-29 source) | Target ownership / observable invariant |
|---|---|
| COPYBOT-2 A3 extraction in progress: baseline `hot/src/main.rs` **9,419** lines at `dd1670e`; event telemetry moved unchanged to `runtime/telemetry.rs` (**90** lines); pending recovery/booking to `runtime/accounting.rs` (**313**), tracked submission to `runtime/execution.rs` (**81**), cancellation/rescue to `runtime/exits.rs` (**357**), and control ticking/incidents to `runtime/control.rs` (**106**). Entrypoint remains **8,524** lines; startup, feeds, matching, task wiring, dashboard and tests still need extraction. No trading-semantic changes in these slices | `main.rs` only composes dependencies, launches supervised tasks and returns typed process status; task handlers own one distinct operation; every source file **≤1,000 lines** after migration, no duplicated decision engine |
| `feeds.rs` pending WebSocket; `book.rs` public book; A1 removed fingerprint and A2 removed the optional `txpool.rs` 20-second poll | **One authoritative pending WebSocket signal**. `book.rs` remains market-data-for-pricing and resting safety only; keep deliberate provider redundancy *within one feed protocol* and one dedupe point. |
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

## COPYBOT-2 execution status

The continuation brief labels its phases **A3 module extraction, A4 typed money-path errors, A5 one Observer contract, A6 read-only/authenticated operator surface, A7 leader lead-time evidence**. These labels supersede the older roadmap numbering below; its unfinished invariants still apply.

A3 is **partial**, not complete: the first mechanical slice moves the event emitter and clock into the binary's `runtime/telemetry.rs`. The emitter's channel bound, throttle intervals, persistence/error behavior and call sites are unchanged. Existing tests remain unchanged, including the throttle assertions. Validation retains **741 library + 47 binary + 181 deploy tests**, all passing; the existing synthetic redemption/restart integration suite also passes **9 tests**. Release build, release hygiene (zero findings) and generated-guide check pass. No recorded market replay is shipped, so synthetic replay is not presented as live-data evidence. A second mechanical slice extracts pending recovery/booking, tracked submission, cancellation/rescue and control ticking/incidents. Its call sites, lock order, await points, retry behavior and unresolved-order retention are unchanged; this is module separation, not a completed single-state-owner redesign. The second slice also passes all four gates with **741 + 47 Rust**, **181 Python**, **9 synthetic replay** tests, zero hygiene findings and verified guides. The remaining entrypoint and oversized library files still exceed their budgets. A4-A6 are not implemented by this extraction. No leader-timing benefit is claimed.

## Migration order — smallest complete, four-gate-clean steps

Each step is independently committed only after `cargo build --locked --release --manifest-path hot/Cargo.toml`, `cargo test --locked --manifest-path hot/Cargo.toml --lib --bin copybot-hot`, `.venv/bin/python -m pytest deploy/ -q` and `python3 scripts/check_release.py` pass. Run the generated-doc check for documentation changes. Push `streamline-1` to `origin` after a green commit; never push upstream. Offline tests must compare *behaviour* (fills/limits/reservations/state), not just compilation. Do not change live credentials or service deployments.

1. **A1 — DONE `c0452bc` (all four gates passed): delete untraded fingerprint candidate route.** Removed `fingerprint.rs`, the channel with `_fprx`, scoring/learning, dashboard synthetic "fingerprint" status and watch-list pinning. Keep the real book levels, prints, price cache and resting safety. Gate: Rust tests for book frames/levels/resting still pass; no `fingerprint::` call site or second signal sender; the primary `RawTx` pipeline unchanged. No latency claim.
2. **A2 — implemented in this change: delete optional txpool rescue.** Remove `txpool.rs`, `Bot.txpool_rpc`, 20-second poll task, misleading "rescued" counter, and its tests/docs if any. Pending WebSocket and dedup stay. Gate: no `txpool_content` call or config field in the runtime; no unbounded alternative ingress added; source-provenance stats still work. Any old `txpool_rpc` config now fails startup as an unknown Bot key; remove it before deployment. No compatibility shim.
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
