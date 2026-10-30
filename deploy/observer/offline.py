"""Read-only local dry Observer. No websocket, RPC, wallet, or control writes.

The baseline uses only the given engine journals. A labeled synthetic fixture
injects missing independent data and faults; it never claims those occurred.
All generated records are written to a separate operator-chosen evidence dir.
"""
from collections import Counter
from pathlib import Path
import json
import time

from observer.alerts import AlertRouter, ObservationJournal
from observer.checks import Snapshot, Source, Verdict, evaluate
from observer.health import ObserverHealth

EXPECTED_FAULTS = frozenset((
    "feed_disconnected", "feed_stale", "fill_loss", "runtime_dead",
    "venue_reject", "unmatched_leader_fill", "signal_guard",
    "ledger_persistence", "duplicate_ownership", "share_drift",
    "missed_exit", "unattributed_buy", "unseen_leader_buy",
))


def read_engine_events(source):
    path = Path(source)
    if not path.is_dir():
        raise ValueError("dry-run engine event directory is unavailable")
    rows = []
    for file in sorted(path.glob("events-*.jsonl")):
        if file.is_symlink() or not file.is_file():
            raise ValueError("event source must be ordinary journal files")
        with file.open(encoding="utf-8") as stream:
            for line in stream:
                if len(line) > 1_000_000 or len(rows) >= 100_000:
                    raise ValueError("dry event read exceeds safety bound")
                row = json.loads(line)
                if not isinstance(row, dict) or not isinstance(row.get("ev"), str):
                    raise ValueError("malformed engine event journal row")
                rows.append(row)
    if not rows:
        raise ValueError("no engine events to observe; missing is not healthy")
    return tuple(rows)


def constructed_faults(events, now):
    """Deliberately invented faults; lane/token labels are synthetic."""
    leader = "synthetic-leader"
    a, b, t, x, y = "synthetic-A", "synthetic-B", "token-T", "token-X", "token-Y"
    return Snapshot(
        now=now,
        events=events + ({"ev": "clob_resp", "ok": False, "lane": a},),
        status={
            "armed": True, "runtime_alive": False,
            "signal_guard": {"enabled": False, "healthy": False},
            "ledger_persistence": {"ok": False},
            "leader_for_lane": {a: leader},
        },
        feed_health={"connected": False, "useful": False, "dropped_unjudged": 2},
        chain={t: {"size": 10.0}, x: {"size": 0.0}},
        holdings={a: {t: 12.0, x: 3.0}, b: {t: 10.0}},
        ws_fills=({"ts": now - 300, "side": "BUY", "token": t,
                   "size": 20.0, "leader_lane": a},),
        watch_decisions=(),
        leader_fills=(
            {"owner": leader, "side": 1, "token": t, "shares": 10.0, "t": now - 600},
            {"owner": leader, "side": 0, "token": y, "shares": 10.0, "t": now - 500},
        ),
        follower_fills=(
            {"side": 0, "token": x, "t": now - 500},),
        leader_balances={(leader, t): 0.0, (leader, x): 0.0},
        decisions={},
        leader_armed_at={a: now - 3600},
    )


def run(events_dir, output_dir, *, fault_fixture=False):
    events = read_engine_events(events_dir)
    now = time.time()
    baseline = Snapshot(now=now, events=events)
    s = constructed_faults(events, now) if fault_fixture else baseline
    findings = evaluate(s)
    names = frozenset(f.code for f in findings if f.verdict == Verdict.ALERT)
    checks = {
        "base_event_count": len(events),
        "base_event_kinds": dict(sorted(Counter(r["ev"] for r in events).items())),
        "fault_fixture": fault_fixture,
        "expected_fault_count": len(EXPECTED_FAULTS) if fault_fixture else 0,
        "observed_fault_count": len(names),
        "fault_codes": sorted(names),
        "unverifiable": sorted(f.source.value for f in findings
                               if f.verdict == Verdict.UNVERIFIABLE),
        "live_parity": "NOT_CLAIMED",
        "network_calls": 0,
        "order_actions": 0,
    }
    correct = names == EXPECTED_FAULTS if fault_fixture else (
        not names and {f.source for f in findings} == set(Source))
    checks["status"] = "PASS" if correct else "FAIL"
    out = Path(output_dir)
    if not out.is_dir() or out.is_symlink():
        raise ValueError("evidence output directory must pre-exist")
    journal_path = out / "observer_observations.jsonl"
    health_path = out / "observer_health.json"
    with ObservationJournal(journal_path) as journal:
        router = AlertRouter(journal, error_path=out / "errors.jsonl")
        health = ObserverHealth(health_path, output=lambda _: None)
        health.record({"ev": "observer_started", "observed_at": now})
        for finding in findings:
            router.emit(finding.source.value, now,
                        "CRITICAL" if finding.verdict == Verdict.ALERT else "WARNING",
                        finding.code, finding.reason,
                        [finding.lane] if finding.lane else [])
        for source in Source:
            source_rows = [f for f in findings if f.source == source]
            status = ("ALERT" if any(f.verdict == Verdict.ALERT for f in source_rows)
                      else "UNVERIFIABLE" if source_rows else "PASS")
            health.record({
                "ev": "observer_audit", "source": source.value,
                "exit_code": 1 if status == "ALERT" else 3 if status == "UNVERIFIABLE" else 0,
                "status": status, "reason": "offline source-grade fixture",
                "observed_at": now,
            })
        health.record({"ev": "observer_stopping", "observed_at": now,
                       "reason": "local offline audit complete"})
    print(json.dumps(checks, sort_keys=True))
    return 0 if correct else 2
