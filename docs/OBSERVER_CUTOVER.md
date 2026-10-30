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
- The observer-owner exclusive file lock refuses a second Observer process.
  The systemd unit declares Conflicts + After for all seven legacy sensor
  services/timers; this is not a mechanism to detect manually launched
  legacy Python processes

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
path. **Central persistent occurrence deduplication and a single alert
writer are not yet active**; prior per-sensor alert files and policy
effects are retained deliberately. Do not mark A5 DONE or claim live
equivalence from synthetic probes.
