"""Single opt-in observer schedule, using the existing sensor adapters.

The service is not enabled by installing this source. The companion systemd
unit conflicts with all legacy sensor services/timers. Run no live audits
unless that cutover has been explicitly carried out.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import time

# Script mode and python -m observer.main both import the same deploy modules.
_DEPLOY = str(Path(__file__).resolve().parent.parent)
if _DEPLOY not in sys.path:
    sys.path.insert(0, _DEPLOY)

import corroborate
from observer.adapters import AUDITS, AuditResult
from observer.alerts import ObservationJournal
from observer.lease import ObserverLease


# Start delays match the legacy timer inventory; intervals are minimum elapsed
# time AFTER the preceding run finishes. No overlap or result-as-confirmation.
CADENCES = {
    "guardian": (30.0, 11.0),
    "fillwatch": (300.0, 900.0),
    "buywatch": (420.0, 900.0),
}
WATCH_INTERVAL_SECS = 15.0


def report_event(event):
    """Only the scheduler emits these health records; no halt is inferred."""
    print(json.dumps(event, sort_keys=True, separators=(",", ":")), flush=True)


class ObserverScheduler:
    """One watcher plus one serialized, never-overlapping audit worker."""

    def __init__(self, watcher, *, audits=None, executor=None,
                 clock=None, report=None, cadences=None):
        self.watcher = watcher
        self.audits = AUDITS if audits is None else audits
        self.cadences = CADENCES if cadences is None else cadences
        if set(self.audits) != set(self.cadences):
            raise ValueError("sensor inventory and cadence list must match")
        for delay, interval in self.cadences.values():
            if delay < 0 or interval <= 0:
                raise ValueError("nonnegative start delay and positive interval required")
        self.executor = executor if executor is not None else ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="copybot-audit"
        )
        self.clock = clock or time.monotonic
        self.report = report or report_event
        self.started = False
        self.next_due = {}
        self.watch_due = None
        self.active = None
        self.active_name = None
        self.deferred = set()

    def start(self):
        if self.started:
            raise RuntimeError("Observer already started")
        self.watcher.start()
        self.started = True
        now = self.clock()
        self.watch_due = now + WATCH_INTERVAL_SECS
        self.next_due = {
            name: now + delay for name, (delay, _interval) in self.cadences.items()
        }

    def step(self):
        if not self.started:
            raise RuntimeError("Observer must start before polling")
        now = self.clock()
        # First record the completed audit before considering any new work.
        if self.active is not None and self.active.done():
            name, future = self.active_name, self.active
            try:
                result = future.result()
                if not isinstance(result, AuditResult) or result.source != name:
                    result = AuditResult(
                        name, 3, "UNVERIFIABLE", "audit returned an invalid health record"
                    )
            except Exception as error:
                result = AuditResult(name, 3, "UNVERIFIABLE",
                                     "audit worker failed: %s" % type(error).__name__)
            now = self.clock()
            self.next_due[name] = now + self.cadences[name][1]
            self.report({
                "ev": "observer_audit", "source": result.source,
                "status": result.status, "exit_code": result.exit_code,
                "reason": result.reason, "observed_at": time.time(),
            })
            self.active = self.active_name = None
            self.deferred.discard(name)

        # The single WS watcher keeps its own original sweep and heartbeat.
        now = self.clock()
        if now >= self.watch_due:
            try:
                with corroborate.sensor_source("watcher"):
                    self.watcher.poll()
            except Exception as error:
                self.watcher.alert(
                    "CRITICAL", "OBSERVER",
                    "watcher poll failed: %s" % type(error).__name__
                )
            self.watch_due = self.clock() + WATCH_INTERVAL_SECS

        now = self.clock()
        due = sorted(
            ((when, name) for name, when in self.next_due.items() if when <= now),
            key=lambda item: (item[0], item[1]),
        )
        if self.active is None:
            if due:
                _when, name = due[0]
                self.active_name = name
                self.active = self.executor.submit(self.audits[name])
                self.deferred.discard(name)
        else:
            # Emit one bounded notice per overdue sensor, never silently mark
            # a delayed audit healthy. The one audit worker cannot overlap.
            for _when, name in due:
                if name != self.active_name and name not in self.deferred:
                    self.deferred.add(name)
                    self.report({
                        "ev": "observer_deferred", "source": name,
                        "reason": "single_audit_worker_busy",
                        "active": self.active_name, "observed_at": time.time(),
                    })

    def close(self):
        self.executor.shutdown(wait=False, cancel_futures=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description="single-owner COPYBOT observer")
    parser.add_argument("--once", action="store_true",
                        help="not supported: this is a persistent owner, not an audit timer")
    args = parser.parse_args(argv)
    if args.once:
        parser.error("one-shot audits are owned by the persistent Observer scheduler")
    # Monitoring never needs the order-signing key, even if the caller exported it.
    os.environ.pop("PRIVATE_KEY", None)
    import watcher
    bot_dir = os.environ.get("BOT_DIR", "/opt/copybot")
    path = Path(bot_dir) / "run" / "observer.owner.lock"
    with ObserverLease(path):
        journal_path = Path(bot_dir) / "run" / "observer_observations.jsonl"
        with ObservationJournal(journal_path) as journal:
            previous = corroborate.configure_observation_sink(journal.record)
            try:
                owner = ObserverScheduler(watcher.WatchSession(45))
                try:
                    owner.start()
                    while True:
                        owner.step()
                        time.sleep(1)
                finally:
                    owner.close()
            finally:
                corroborate.configure_observation_sink(previous)


if __name__ == "__main__":
    sys.exit(main())
