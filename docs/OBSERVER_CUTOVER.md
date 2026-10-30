# Single Observer scheduler cutover — not yet deployed

`deploy/copybot-observer.service` is an **opt-in alternative** to the existing
watcher daemon and Guardian, fillwatch and buywatch timers. Merely merging or
installing the file does not start it. The service invokes the existing audit
kernels from **one Python process**, with one WebSocket watcher listener and
one serialized background audit worker. No trading key is used.

## Offline-verified behavior

- Watcher: one listener, sweep/heartbeat at minimum 15-second intervals
- Guardian: first check after 30 seconds; next check at least 11 seconds
  after the preceding check finishes
- Fillwatch: first check after 5 minutes; then at least 15 minutes after
  the preceding check finishes
- Buywatch: first check after 7 minutes; then at least 15 minutes after
  the preceding check finishes
- An audit overrun delays other audits rather than overlapping writers;
  each first overdue sensor emits an observer_deferred record
- The scheduler emits Observer **health** records. They do not assert an
  independently confirmed market event or imply that a halt was applied
- Guardian error events now share the one opt-in `AlertRouter` and preserve
  the exact `run/errors.jsonl` row schema and bounded history. A pre-existing
  insecure errors file refuses cutover until an operator repairs its ownership
  and 0600 permissions; failed durable error writes still trigger Guardian's
  conservative exception/disarm path.
- Guardian and watcher email calls share one serialized Observer delivery
  owner with owner-only `run/observer_notifications.jsonl`. Each provider
  attempt is fsynced before transport, and the outcome is fsynced afterward.
  Journal rows contain only a digest and metadata, never message text,
  email addresses, private keys, or API tokens. Failed evidence writes do
  **not** authorize a successful delivery claim; crash-time outcomes may
  remain unknown, so no exactly-once provider guarantee is assumed.
- When explicitly enabled, watcher/fillwatch/buywatch critical alerts
  flow through one `AlertRouter` to protected `run/observer_alerts.jsonl`;
  the same writer maintains `watcher_alerts.jsonl` and
  `buywatch_alerts.jsonl` for Guardian's existing alert cursors. It also
  projects watcher critical-alert lines to `run/watcher.log` for the
  existing Guardian log-reader after the legacy watcher's stdout is stopped.
  This projection does not reproduce every informational watcher print. Legacy
  standalone services retain their original independent writers.
- When explicitly enabled, checked corroborations from the four sensors
  use one `run/observer_observations.jsonl` journal (owner-only 0600, fsynced)
  and durable occurrence dedup across sources and restarts, only where a
  real fill/order identity exists. Anonymous token sightings never dedup.
  Corrupt or unwritable evidence refuses startup or marks a check
  `UNVERIFIABLE`; it is never treated as confirmed evidence.
- A graceful SIGTERM joins the current audit before closing the three
  Observer evidence/alert/notification sinks and releasing the exclusive
  owner lease. The opt-in service allows up to 120 seconds for that join;
  systemd may still terminate an indefinitely stuck process afterward.
- The observer-owner exclusive file lock refuses a second Observer process.
  The legacy Guardian, watcher, fillwatch and buywatch CLI entrypoints now
  take **shared** locks on the same file, allowing legacy sensors to run
  together but excluding the Observer; likewise an active legacy sensor
  blocks an Observer startup. This also protects manual legacy launches.
- The systemd unit additionally declares Conflicts + After for all seven
  legacy sensor services/timers; explicit stop/disable is still required
  for a reliable boot-time cutover.

## Offline preflight — no service changes

Before considering a cutover, the staged unit inventory can be checked without
contacting the host's service manager or reading `copybot.env`:

```sh
python3 deploy/observer/preflight.py --phase staged --unit-dir deploy
```

The output must report `status=OFFLINE_SNAPSHOT_PASS`, `errors=[]`, and
`live_parity=UNKNOWN`. This **only** checks the checked-in service contracts:
all seven legacy conflicts, execution user, signing-key exclusion, permissions,
network order and legacy timer intervals.

For the later `ready` (old units stopped, new Observer not yet started)
and `cutover` (old units stopped, new Observer started) phases, an operator
must explicitly supply a **real**, recently collected (at most five minutes
old) JSON snapshot containing `captured_at` (Unix seconds),
`source="operator-collected-systemctl"`, and exactly nine entries in
`units`. Each entry must have the service-manager's actual `active`
(`active`, `inactive` or `failed`, etc.) and `unit_file`
(`enabled`, `disabled`, `static`, etc.) strings. The nine units are:
`copybot-observer.service`, `copybot-hot.service`, all four legacy
Guardian/watcher/fillwatch/buywatch services, and all three
Guardian/fillwatch/buywatch timers. No secrets belong in the snapshot.

An explicitly authorized operator can then run, **as the intended service
user**, from the staged deployment directory (not from this migration task):

```sh
python3 deploy/observer/preflight.py --phase ready --unit-dir deploy \
  --state-snapshot /path/to/operator-collected-service-states.json \
  --run-dir /opt/copybot/run
# After an explicitly authorized systemd cutover and fresh state collection:
python3 deploy/observer/preflight.py --phase cutover --unit-dir deploy \
  --state-snapshot /path/to/new-service-states.json \
  --run-dir /opt/copybot/run
```

The checker **does not** collect, authenticate, or modify those unit states.
It verifies the supplied evidence, the expected engine state, and read-only
run-directory/owner-lock metadata, and refuses stale, missing or
contradictory snapshots. Even a passing supplied snapshot reports
`live_parity=UNKNOWN`; actual feed coverage, provider delivery, halt
accuracy, order safety and operating equivalence require separate authorized
observations. Do not fabricate unit-state evidence to satisfy the gate.

## Explicit operator cutover, on a deployment host only

Do **not** run the following during offline migration or until evidence
from deployment parity is reviewed. With sufficient service-management
authority, after copying the new unit and source and reloading systemd,
stop and disable the four legacy service jobs and three legacy timers
**before** enabling the single owner:

```sh
sudo systemctl disable --now copybot-guardian.timer copybot-fillwatch.timer copybot-buywatch.timer copybot-watcher.service
sudo systemctl stop copybot-guardian.service copybot-fillwatch.service copybot-buywatch.service
sudo systemctl daemon-reload
sudo systemctl enable --now copybot-observer.service
sudo systemctl status copybot-observer.service
sudo journalctl -u copybot-observer.service -n 60 --no-pager
```

Before enabling the Observer, verify that the `run/` directory is a real
(non-symlink) directory owned by the Observer's service user and not writable
by its group or other accounts. `run/observer.owner.lock` must be a regular,
single-link file owned by the same user, mode `0600`. Both the legacy and
Observer entrypoints now refuse an insecure directory/file rather than
starting without their mutual-exclusion guarantee. If the existing paths do
not meet those requirements, arrange an operator-reviewed ownership/mode
repair before cutover; do not silently relax the file checks.

Verify that the new process holds `run/observer.owner.lock` and that
the legacy units remain inactive. The unit also enforces conflicts and
removes PRIVATE_KEY from its environment; only the existing narrowly
scoped operator control capability may be present for fillwatch.

## Rollback

Stop and disable the observer **first**, then re-enable legacy schedules:

```sh
sudo systemctl disable --now copybot-observer.service
sudo systemctl enable --now copybot-guardian.timer copybot-fillwatch.timer copybot-buywatch.timer copybot-watcher.service
```

The shared Observation schema is wired through the existing corroboration
path. **One persistent corroboration journal and occurrence dedup are now
active in the opt-in Observer process**; prior per-sensor critical-alert
files and policy effects are retained deliberately, so a sole downstream
Guardian's separate error journal remains outside the alert router. Guardian and watcher email sends now share a single audited owner in the opt-in process; delivery to the actual provider and production parity remain unverified. Do not mark A5 DONE or claim live
equivalence from synthetic probes.
