#!/usr/bin/env python3
"""Offline, provenance-strict capture and measurement of exchange-stage copy lag.

No socket, wallet, order or provider connection. Each input must be independently
exported from the exchange execution stage and explicitly joined by copy_attempt_id.
Pending-settlement receipt, indexer/block, and local order-ack times are rejected.
The exporter, not this program, must attest to source authenticity and clock error.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum
import json
import math
import os
from pathlib import Path
from statistics import median
import sys


class Role(StrEnum):
    LEADER = "leader"
    FOLLOWER = "follower"


class Stage(StrEnum):
    EXCHANGE_MATCH = "exchange_match"


FIELDS = frozenset({
    "schema", "role", "stage", "source_kind", "source", "source_event_id",
    "copy_attempt_id", "chain", "protocol", "market", "token", "side",
    "leader_order_id", "leader_fill_occurrence", "follower_order_id",
    "match_utc_ns", "clock_error_ns",
})
KEY = (
    "copy_attempt_id", "chain", "protocol", "market", "token", "side",
    "leader_order_id", "leader_fill_occurrence", "follower_order_id",
)
MAX_EVENTS = 100_000
MAX_LINE = 8192


def parse_event(raw, line_number):
    if not isinstance(raw, dict) or set(raw) != FIELDS:
        raise ValueError(f"line {line_number}: exact event schema required")
    if type(raw["schema"]) is not int or raw["schema"] != 1:
        raise ValueError(f"line {line_number}: unsupported schema version")
    try:
        raw["role"] = Role(raw["role"]).value
        raw["stage"] = Stage(raw["stage"]).value
    except (ValueError, TypeError) as error:
        raise ValueError(f"line {line_number}: invalid role or lifecycle stage") from error
    if raw["source_kind"] != "exchange_execution":
        raise ValueError(f"line {line_number}: only exchange execution events qualify")
    if raw["side"] not in ("BUY", "SELL"):
        raise ValueError(f"line {line_number}: invalid side")
    for field in (*KEY, "source", "source_event_id"):
        value = raw[field]
        if not isinstance(value, str) or not 1 <= len(value) <= 256:
            raise ValueError(f"line {line_number}: missing or oversized {field}")
        if any(ord(ch) < 33 or ord(ch) > 126 for ch in value):
            raise ValueError(f"line {line_number}: nonprintable {field}")
    for field in ("match_utc_ns", "clock_error_ns"):
        if type(raw[field]) is not int or raw[field] < 0:
            raise ValueError(f"line {line_number}: invalid {field}")
    if raw["match_utc_ns"] == 0:
        raise ValueError(f"line {line_number}: no exchange match timestamp")
    if raw["clock_error_ns"] > 1_000_000_000:
        raise ValueError(f"line {line_number}: exchange clock uncertainty exceeds 1 second")
    return raw


def read_events(source):
    events = []
    for number, line in enumerate(source, 1):
        if len(line) > MAX_LINE:
            raise ValueError(f"line {number}: input record is oversized")
        if not line.strip():
            raise ValueError(f"line {number}: empty event")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"line {number}: invalid JSON") from error
        events.append(parse_event(row, number))
        if len(events) > MAX_EVENTS:
            raise ValueError(f"at most {MAX_EVENTS} events per capture")
    return events


def summarize(events):
    by_attempt = defaultdict(lambda: {role.value: [] for role in Role})
    for event in events:
        by_attempt[tuple(event[field] for field in KEY)][event["role"]].append(event)

    measured = []
    missing = 0
    duplicate = 0
    invalid_identity = 0
    for identity, roles in by_attempt.items():
        leaders = roles[Role.LEADER.value]
        followers = roles[Role.FOLLOWER.value]
        if len(leaders) > 1 or len(followers) > 1:
            duplicate += 1
            continue
        if not leaders or not followers:
            missing += 1
            continue
        leader, follower = leaders[0], followers[0]
        if (leader["source"], leader["source_event_id"]) == (
            follower["source"], follower["source_event_id"]
        ):
            invalid_identity += 1
            continue
        delay_ns = follower["match_utc_ns"] - leader["match_utc_ns"]
        bound_ns = leader["clock_error_ns"] + follower["clock_error_ns"]
        day = datetime.fromtimestamp(
            leader["match_utc_ns"] / 1_000_000_000, tz=timezone.utc
        ).date().isoformat()
        measured.append({
            "market_day": (identity[3], day),
            "source_pair": (leader["source"], follower["source"]),
            "lag_ms": delay_ns / 1_000_000,
            "clock_bound_ms": bound_ns / 1_000_000,
            "unambiguously_earlier": delay_ns < -bound_ns,
        })
    values = sorted(item["lag_ms"] for item in measured)
    by_source = defaultdict(list)
    for item in measured:
        by_source[item["source_pair"]].append(item["lag_ms"])
    return {
        "events": len(events),
        "attempts": len(by_attempt),
        "matched_pairs_declared_exchange_stage": len(measured),
        "missing_side_attempts": missing,
        "duplicate_side_attempts": duplicate,
        "invalid_shared_event_identity": invalid_identity,
        "independent_market_days": len({item["market_day"] for item in measured}),
        "median_lag_ms": median(values) if values else None,
        "p95_lag_ms": values[math.ceil(0.95 * len(values)) - 1] if values else None,
        "negative_lag_pairs": sum(item["lag_ms"] < 0 for item in measured),
        "negative_beyond_clock_error_pairs": sum(
            item["unambiguously_earlier"] for item in measured
        ),
        "source_pair_counts": [
            {"leader_source": pair[0], "follower_source": pair[1], "pairs": len(rows)}
            for pair, rows in sorted(by_source.items())
        ],
        "qualification": "Self-declared venue-stage provenance; source authenticity "
                         "and clock calibration require external audit. Synthetic "
                         "fixtures are not live observations.",
    }


def capture(events, destination):
    """Create a private, never-overwritten capture; caller supplies output path."""
    path = Path(destination)
    if not path.is_absolute():
        raise ValueError("capture output must be an absolute path")
    parent = path.parent
    info = parent.stat(follow_symlinks=False)
    if parent.is_symlink() or not parent.is_dir() or info.st_uid != os.geteuid():
        raise ValueError("capture directory must be real and owner-controlled")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as sink:
        for event in events:
            sink.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
        sink.flush()
        os.fsync(sink.fileno())
    return {"events_recorded": len(events), "capture_path": str(path)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    record = commands.add_parser("capture", help="validate stdin, create mode-0600 JSONL")
    record.add_argument("--output", required=True, help="new absolute JSONL file")
    summary = commands.add_parser("summary", help="report matched exchange-stage pairs")
    summary.add_argument("--input", required=True, help="recorded JSONL")
    args = parser.parse_args(argv)
    try:
        if args.operation == "capture":
            rows = read_events(sys.stdin)
            result = capture(rows, args.output)
        else:
            with open(args.input, encoding="utf-8") as source:
                result = summarize(read_events(source))
    except (OSError, ValueError, OverflowError) as error:
        parser.exit(2, "lead recorder refused: %s\n" % error)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
