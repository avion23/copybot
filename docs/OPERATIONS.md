# Operations

## DRY-RUN RUNBOOK (ten steps)

1. **Inputs:** use an isolated local worktree, a private ignored config, an owner-only signer key source, a pending-transaction Polygon WSS, and a bounded HTTP RPC for position evidence; never copy credentials into Git or logs.
2. **Mode:** set `[bot] mode = "dry"` in `deploy/copybot2.toml`; `shadow` and `live` are order-submitting modes, not dry runs.
3. **Wallet:** set the deposit-wallet contract as `funder`, owner EOA as `signer`, and `signature_type = 3` for POLY_1271; confirm they match the local key without printing it.
4. **Leader:** without a leader, disable all lanes (`DryObserveOnly`); with an approved leader, dry mode can model a 5% copy clipped to explicit per-fill/per-market/open/daily caps, but this capped observation is not an admissible live-copy strategy.
5. **Feed and auth:** configure one proven full-object pending-tx WSS and `clob_host`; dry mode may GET existing CLOB API credentials and GET `/data/orders`, never POST `/order` or create an API key.
6. **Safety:** isolate `events_path`, `control_path`, `ledger_path` and `signal_guard_path`; retain owner-only permissions and do not run the systemd live service concurrently.
7. **Start (180-second smoke test):** from the repo root after building, run `env -i PATH="$PATH" HOME="$HOME" PRIVATE_KEY="${PRIVATE_KEY:?load key locally}" timeout 180 hot/target/release/copybot-hot deploy/copybot2.toml`; prior agent-specific environment assignments were not supplied.
8. **Minutes 0–3:** confirm `DRY OBSERVE ONLY`, correct wallet type, successful CLOB `/data/orders` GET, actual full-object feed frames, no POST order, and any 429 marked `RateLimited` rather than empty balances.
9. **Minutes 3–10:** only under an approved longer `timeout 600` dry-only observation, watch reconnection/error counts, feed freshness, positions pagination completion, repeated 429/backoff, latches and stale telemetry; a 180-second run alone cannot cover 10 minutes.
10. **Stop:** let `timeout` terminate the foreground process or send Ctrl-C to that PID; verify it exited and inspect sanitized logs, with no order attempt or live service; no leader/fill/profit/1271 order-acceptance claim without separate evidence and GO.

## Three controls, three different effects

| Operator state | Effect |
| --- | --- |
| Armed | Entries and exits may execute if all other checks permit them |
| Halt buys | Stops new entries while allowing the existing exit path |
| Off | Disarms that lane; both entry and exit execution are stopped |

A guardian halt is not necessarily a full disarm. Read the specific incident and lane state. Do not repeatedly re-arm to suppress an unexplained fault.

## Stop the process

```sh
sudo systemctl stop copybot-hot.service
sudo systemctl is-active copybot-hot.service
```

Stopping the engine does not liquidate holdings and does not prove that venue-resting orders were canceled. Check the venue separately. Stopping a service is different from changing persistent arm intent. If you also stop observation services or timers, document that monitoring is no longer running.

## Restart and recovery

Keep the ledger, signal guard, operator state, registry, funding records, and pending-execution evidence together. A restart is not a reset. Never delete state to make a reconciliation warning disappear. The engine's existing recovery logic depends on those files to avoid repeating or forgetting execution.

Before an upgrade, stop the instance cleanly and take a consistent backup of its configuration, environment, runtime state, and exact executable. Store backups outside Git and restrict access. Record the source commit. Start only one live execution process for a funded account.

Roll back the executable and configuration deliberately. Do not blindly overwrite newer accounting with an old backup after trades have occurred: restoring old state can duplicate orders or discard fills. Reconcile changed holdings and unknown outcomes before resuming.

## Things to check

- Fresh feed and observer timestamps, not just an active process.
- Intended mode and lane state; startup can restore existing operator intent.
- Ledger/custody agreement and unresolved or ambiguous execution.
- Correct funding basis before interpreting P&L.
- Observer observation journal (`run/observer_observations.jsonl`), health receipt (`run/observer_health.json`), audited notification delivery (`run/observer_notifications.jsonl`), and writable state paths.
- Venue-resting orders independently of dashboard connection status.

## One Observer: local mode=dry

The four standalone monitoring CLIs (`guardian.py`, `watcher.py`, `fillwatch.py`, `buywatch.py`), their four services and three timers have been removed. The sole Observer entrypoint is `deploy/observer/main.py`; `deploy/copybot-observer.service` is a template, not enabled or installed by this repository change. Its **one persistent process** polls the fill feed every 15 s and serializes Guardian (first at 30 s, then 11 s after each completion), fillwatch (first 300 s, then every 900 s), and buywatch (first 420 s, then every 900 s). It retains the original source-grade RPC, feed, ledger and risk-audit kernels. A failing or stale external source is not a clean PASS. Observer does not require `PRIVATE_KEY`.

| Check | Independent evidence | Cadence / alert channel |
| --- | --- | --- |
| Runtime/dead arm intent; feed freshness, decoding, dropped fill, unmatched leader fill, rejected orders, Guardian audit liveness | Operator intent, engine event journal, leader fill WebSocket, runtime status, observer health | 15 s / Observer journal and notification sink |
| Signal guard, ledger/chain/custody drift, foreign writer, fire-pattern caps, operator changes, stale halts, alert escalation and audit preflight | Runtime dashboard, ledger, chain positions, operator state, observation journal | 11 s after prior audit / Observer journal, authenticated halt capability, notification sink |
| Missed leader exits and follower purchases without a leader fill | CTF transfer logs, independently queried leader balances, follower positions | 900 s after audit / Observer journal and conservative authenticated buy halt |
| Leader purchases unseen by copybot decision journal | CTF transfers, durable `fire`/`skip`/`signal_guard_skip` events | 900 s after audit / Observer journal, notification sink; no halt |

**Local journal-only verification** (no network, no order API, and no writes to the dry-run instance):

```sh
mkdir -p /home/admin/worktrees/copybot-evidence/COPYBOT-A5/{baseline,fault}
python3 deploy/observer/main.py --dry-offline-events /home/admin/worktrees/copybot-dryrun-s1/data --output-dir /home/admin/worktrees/copybot-evidence/COPYBOT-A5/baseline
python3 deploy/observer/main.py --dry-offline-events /home/admin/worktrees/copybot-dryrun-s1/data --output-dir /home/admin/worktrees/copybot-evidence/COPYBOT-A5/fault --fault-fixture
python3 deploy/observer/preflight.py --unit-dir deploy
```

The first run reads the existing engine journal and reports `UNVERIFIABLE` for missing chain, watcher and fill evidence; it must not call these absent observations healthy. The second deliberately injects **synthetic** stale feed, lost fills, reject, unacknowledged armed intent, duplicate inventory, drift, missed exit, unattributed purchase and unseen leader buy. Do not mistake these generated alerts for real incidents. The local runs save only to the explicitly chosen evidence directory. The persistent Observer is **not** started by these commands; do not start a live service or modify a funded operator state to test monitoring.

Engine logs go to journald through the service template. Configure bounded journal retention on the host. The provided logrotate template covers file-based observer logs; adjust paths before installing it. Never commit logs, screenshots, backup archives, or portfolio exports to report a bug. Use a synthetic reproduction.
