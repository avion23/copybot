"""One immutable Observation envelope; no feeds, signing keys, or I/O.

A corroborator's legacy LAGGING result maps to Unverifiable for the common
contract while retaining the original verdict for existing runtime callers.
Absent event identifiers stay absent: token-only guesses are not fill IDs.
"""
from dataclasses import dataclass, field
import math
import time

STATUSES = frozenset(("Confirmed", "Refuted", "Unverifiable"))
_LEGACY = {
    "CONFIRMED": "Confirmed",
    "REFUTED": "Refuted",
    "UNVERIFIABLE": "Unverifiable",
    "LAGGING": "Unverifiable",
}
_FIELDS = ("source", "observed_at", "lane", "market", "token",
           "leader_order", "follower_order", "fill_occurrence", "status", "reason")


def _text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    return value if value.strip() else None


@dataclass(frozen=True)
class Observation:
    source: str
    observed_at: float
    lane: str | None
    market: str | None
    token: str | None
    leader_order: str | None
    follower_order: str | None
    fill_occurrence: str | None
    status: str
    reason: str
    _legacy_verdict: str = field(default="UNVERIFIABLE", repr=False, compare=False)

    def __post_init__(self):
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("observation source is required")
        if (isinstance(self.observed_at, bool) or
                not isinstance(self.observed_at, (int, float)) or
                not math.isfinite(self.observed_at) or self.observed_at < 0):
            raise ValueError("observation timestamp must be finite and nonnegative")
        if self.status not in STATUSES:
            raise ValueError("observation status must be canonical")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("observation evidence reason is required")
        for key in ("lane", "market", "token", "leader_order",
                    "follower_order", "fill_occurrence"):
            value = getattr(self, key)
            if value is not None and _text(value) is None:
                raise ValueError("invalid observation identity: " + key)
        if self.status == "Confirmed" and not any((
                self.market, self.token, self.leader_order,
                self.follower_order, self.fill_occurrence)):
            raise ValueError("independent confirmation must identify an object")

    @property
    def legacy_verdict(self):
        return self._legacy_verdict

    def as_dict(self):
        return {name: getattr(self, name) for name in _FIELDS}


def from_corroboration(family, fact, verdict, reason, *,
                       supported, observed_at=None):
    """Map checked evidence to the common contract, never invent a fill ID."""
    when = time.time() if observed_at is None else observed_at
    fact = fact if isinstance(fact, dict) else {}
    normalized = _LEGACY.get(verdict, "Unverifiable")
    legacy = verdict if verdict in _LEGACY else "UNVERIFIABLE"
    explanation = reason if isinstance(reason, str) else repr(reason)
    if not explanation.strip():
        explanation = "independent corroboration returned no evidence reason"
        if normalized == "Confirmed":
            normalized, legacy = "Unverifiable", "UNVERIFIABLE"
    if not supported and normalized == "Confirmed":
        normalized, legacy = "Unverifiable", "UNVERIFIABLE"
        explanation = "no registered independent corroborator for this claim"
    token = _text(fact.get("token"))
    market = _text(fact.get("market")) or _text(fact.get("condition"))
    leader_order = _text(fact.get("leader_order"))
    follower_order = _text(fact.get("follower_order"))
    fill_occurrence = _text(fact.get("fill_occurrence"))
    if normalized == "Confirmed" and not any((
            token, market, leader_order, follower_order, fill_occurrence)):
        normalized, legacy = "Unverifiable", "UNVERIFIABLE"
        explanation = "confirmed claim lacks a specific market, token or occurrence"
    return Observation(
        source="corroborate:" + (family if isinstance(family, str) and family else "unknown"),
        observed_at=when,
        lane=_text(fact.get("lane")), market=market, token=token,
        leader_order=leader_order, follower_order=follower_order,
        fill_occurrence=fill_occurrence, status=normalized,
        reason=explanation, _legacy_verdict=legacy,
    )
