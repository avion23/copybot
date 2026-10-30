"""Pure, typed fault decisions for a serialized Observer snapshot.

Only the caller acquires snapshots or routes findings. Missing source-grade
evidence is UNVERIFIABLE; a local journal cannot invent chain or feed truth.
"""
from dataclasses import dataclass
from enum import StrEnum
import math
from typing import Mapping

from guardian_checks import conservation_breaches, check_share_drift, net_chain_for_lane
from observer import buy_audit, feed


class Source(StrEnum):
    GUARDIAN = "guardian"
    WATCHER = "watcher"
    FILLWATCH = "fillwatch"
    BUYWATCH = "buywatch"


class Verdict(StrEnum):
    ALERT = "ALERT"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class Finding:
    source: Source
    code: str
    verdict: Verdict
    reason: str
    lane: str | None = None


@dataclass(frozen=True)
class Snapshot:
    """One immutable cut of external inputs; empty tuples mean observed empty.

    None means a sensor was NOT observed and must not mean a clean pass.
    """
    now: float
    events: tuple[dict, ...]
    status: Mapping | None = None
    feed_health: Mapping | None = None
    chain: Mapping | None = None
    holdings: Mapping | None = None
    ws_fills: tuple[dict, ...] | None = None
    watch_decisions: tuple[dict, ...] | None = None
    leader_fills: tuple[dict, ...] | None = None
    follower_fills: tuple[dict, ...] | None = None
    leader_balances: Mapping | None = None
    decisions: Mapping | None = None
    leader_armed_at: Mapping | None = None

    def __post_init__(self):
        if (type(self.now) not in (float, int) or not math.isfinite(self.now)
                or self.now < 0 or not isinstance(self.events, tuple)):
            raise ValueError("invalid snapshot time or event source")


def _unknown(source, missing):
    return Finding(source, "evidence_unavailable", Verdict.UNVERIFIABLE,
                   missing + " not present in this snapshot")


def check_watcher(s):
    if s.feed_health is None:
        return (_unknown(Source.WATCHER, "WebSocket heartbeat"),)
    out = []
    if s.feed_health.get("connected") is not True:
        out.append(Finding(Source.WATCHER, "feed_disconnected", Verdict.ALERT,
                           "leader fill websocket not connected"))
    if s.feed_health.get("useful") is not True:
        out.append(Finding(Source.WATCHER, "feed_stale", Verdict.ALERT,
                           "feed lacks recent decodable frames"))
    if s.feed_health.get("dropped_unjudged", 0) > 0:
        out.append(Finding(Source.WATCHER, "fill_loss", Verdict.ALERT,
                           "unjudged leader fills evicted"))
    if s.status is None:
        out.append(_unknown(Source.WATCHER, "operator/runtime status"))
    elif s.status.get("armed") and s.status.get("runtime_alive") is not True:
        out.append(Finding(Source.WATCHER, "runtime_dead", Verdict.ALERT,
                           "armed operator intent not acknowledged by runtime"))
    for event in s.events:
        if event.get("ev") == "clob_resp" and event.get("ok") is False:
            out.append(Finding(Source.WATCHER, "venue_reject", Verdict.ALERT,
                               "CLOB order path rejected an attempted order",
                               event.get("lane")))
    if s.ws_fills is None or s.watch_decisions is None:
        out.append(_unknown(Source.WATCHER, "leader websocket fills / decision journal"))
    else:
        for fill in s.ws_fills:
            if not feed.fill_is_covered(fill, s.watch_decisions, 45):
                out.append(Finding(Source.WATCHER, "unmatched_leader_fill",
                                   Verdict.ALERT, "leader fill lacks a matching fire/skip",
                                   fill.get("leader_lane")))
    return tuple(out)


def check_guardian(s):
    if s.status is None:
        return (_unknown(Source.GUARDIAN, "runtime/guard state"),)
    out = []
    guard = s.status.get("signal_guard")
    if not isinstance(guard, dict) or guard.get("healthy") is not True or guard.get("enabled") is not True:
        out.append(Finding(Source.GUARDIAN, "signal_guard", Verdict.ALERT,
                           "durable signal guard absent or unhealthy"))
    ledger = s.status.get("ledger_persistence")
    if not isinstance(ledger, dict) or ledger.get("ok") is not True:
        out.append(Finding(Source.GUARDIAN, "ledger_persistence", Verdict.ALERT,
                           "ledger persistence not confirmed healthy"))
    if s.chain is None or s.holdings is None:
        out.append(_unknown(Source.GUARDIAN, "chain positions / lane holdings"))
        return tuple(out)
    for breach in conservation_breaches(s.holdings, s.chain):
        out.append(Finding(Source.GUARDIAN, "duplicate_ownership", Verdict.ALERT,
                           "claims exceed chain position: " + breach["token"]))
    for lane, held in s.holdings.items():
        physical = net_chain_for_lane(s.chain, s.holdings, lane)
        _ok, reasons, _dust, _notice = check_share_drift(held, physical)
        for reason in reasons:
            out.append(Finding(Source.GUARDIAN, "share_drift", Verdict.ALERT,
                               reason, lane))
    return tuple(out)


def check_fillwatch(s):
    if (s.leader_fills is None or s.follower_fills is None or s.holdings is None
            or s.leader_balances is None):
        return (_unknown(Source.FILLWATCH, "CTF fills / follower holdings / leader balance"),)
    out = []
    for lane, held in s.holdings.items():
        leader = s.status.get("leader_for_lane", {}).get(lane) if s.status else None
        if not leader:
            out.append(_unknown(Source.FILLWATCH, "lane leader identity"))
            continue
        for event in s.leader_fills:
            if (event.get("owner") != leader or event.get("side") != 1
                    or event.get("token") not in held):
                continue
            token = event["token"]
            balance = s.leader_balances.get((leader, token))
            if balance is None:
                out.append(_unknown(Source.FILLWATCH, "post-exit leader token balance"))
            elif (balance <= 1.0 and s.now - event["t"] > 240
                  and not any(o.get("token") == token and o.get("side") == 1
                              for o in s.follower_fills)):
                out.append(Finding(Source.FILLWATCH, "missed_exit", Verdict.ALERT,
                                   "leader exited and follower is still holding", lane))
        for event in s.follower_fills:
            if (event.get("side") == 0 and event.get("token") in held
                    and s.now - event["t"] > 240
                    and not any(f.get("token") == event["token"] and
                                f.get("side") == 0 and f.get("owner") == leader
                                for f in s.leader_fills)):
                balance = s.leader_balances.get((leader, event["token"]))
                if balance is None:
                    out.append(_unknown(Source.FILLWATCH, "leader buy balance"))
                elif balance <= 1.0:
                    out.append(Finding(Source.FILLWATCH, "unattributed_buy", Verdict.ALERT,
                                       "follower bought with no leader fill", lane))
    return tuple(out)


def check_buywatch(s):
    if s.leader_fills is None or s.decisions is None or s.status is None:
        return (_unknown(Source.BUYWATCH, "leader transfer/bot decision join"),)
    out = []
    leaders = {address: name for name, address in
               (s.status.get("leader_for_lane") or {}).items()}
    armed = s.leader_armed_at or {}
    for event in s.leader_fills:
        if event.get("side") != 0:
            continue
        lane = leaders.get(event.get("owner"))
        if lane is None:
            out.append(_unknown(Source.BUYWATCH, "buy's leader lane"))
            continue
        if lane in armed and event["t"] < armed[lane]:
            continue
        verdict = buy_audit.classify(event, lane, s.decisions)
        if verdict == "UNSEEN":
            out.append(Finding(Source.BUYWATCH, "unseen_leader_buy", Verdict.ALERT,
                               "leader buy has no fire or decline decision", lane))
    return tuple(out)


def evaluate(s):
    return (check_watcher(s) + check_guardian(s)
            + check_fillwatch(s) + check_buywatch(s))
