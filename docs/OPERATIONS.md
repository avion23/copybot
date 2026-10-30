# Operations

## Local 180-second no-order dry run

From the repository root, set `bot.mode = "dry"` in `deploy/copybot2.toml`. Dry mode with zero enabled leader lanes is an explicit **observe-only** boot: it can inspect feed and CLOB authentication without placing orders. Never use `mode = "shadow"` for this check; shadow is an order-submitting mode.

The D-600 operator reported the command form `env -i ... timeout 180 hot/target/release/copybot-hot deploy/copybot2.toml`. Its actual environment assignments were not supplied, so they cannot be reproduced verbatim. This is the equivalent isolated-environment command with an externally loaded signing key, not a claim of the exact agent environment:

```sh
env -i PATH="$PATH" HOME="$HOME" PRIVATE_KEY="${PRIVATE_KEY:?load local signer key}" \
  timeout 180 hot/target/release/copybot-hot deploy/copybot2.toml
```

Use only a locally provisioned key, never put the key in Git or an output log, and do not enable live/shadow execution or arm a lane. `signature_type = 3` uses the Deposit Wallet contract as `funder` and the EOA owner as `signer`; CLOB authentication alone does not prove the venue will accept a signed 1271 order. A transient HTTP 429 on a Data API positions page receives at most three bounded retries across the complete snapshot. Exhaustion remains **RateLimited / unverifiable**; it never means zero holdings.

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
- Guardian and watcher journals, notification delivery, and writable state paths.
- Venue-resting orders independently of dashboard connection status.

Engine logs go to journald through the service template. Configure bounded journal retention on the host. The provided logrotate template covers file-based observer logs; adjust paths before installing it. Never commit logs, screenshots, backup archives, or portfolio exports to report a bug. Use a synthetic reproduction.
