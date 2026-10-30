"""Pure bounded observation deduplication and one conservative halt policy.

No token-only cross-source dedup: independent fills may share a token.
No alert writer or scheduler is introduced by this module.
"""
from collections import OrderedDict


def halts_legacy(verdict):
    """Preserve the existing contract: REFUTED alone prevents a halt."""
    return verdict != "REFUTED"


def alert_level(observation):
    """Policy classification; only the future single scheduler may emit alerts."""
    if observation.status == "Confirmed":
        return "CRITICAL"
    if observation.status == "Unverifiable":
        return "WARNING"
    return None


def occurrence_key(observation):
    """Only explicit event/order identity can correlate independent observers."""
    if observation.fill_occurrence:
        return ("fill", observation.lane, observation.market,
                observation.token, observation.fill_occurrence,
                observation.status)
    if observation.leader_order or observation.follower_order:
        return ("order", observation.lane, observation.market,
                observation.token, observation.leader_order,
                observation.follower_order, observation.status)
    return None


class OccurrenceDeduper:
    """Bounded in-memory cache; caller supplies the observation's own timestamp."""

    def __init__(self, ttl_seconds=900, max_keys=4096):
        if not isinstance(ttl_seconds, (int, float)) or ttl_seconds <= 0:
            raise ValueError("positive dedup TTL required")
        if not isinstance(max_keys, int) or isinstance(max_keys, bool) or max_keys < 1:
            raise ValueError("positive dedup key capacity required")
        self.ttl_seconds = ttl_seconds
        self.max_keys = max_keys
        self._seen = OrderedDict()

    def contains(self, observation):
        """Read-only membership; record only after durable journal append."""
        key = occurrence_key(observation)
        if key is None:
            return False
        previous = self._seen.get(key)
        return (previous is not None
                and 0 <= observation.observed_at - previous < self.ttl_seconds)

    def duplicate(self, observation):
        key = occurrence_key(observation)
        if key is None:
            return False
        now = observation.observed_at
        for known, t in tuple(self._seen.items()):
            if now >= t and now - t >= self.ttl_seconds:
                del self._seen[known]
        previous = self._seen.get(key)
        if previous is not None and 0 <= now - previous < self.ttl_seconds:
            self._seen.move_to_end(key)
            return True
        self._seen[key] = now
        self._seen.move_to_end(key)
        while len(self._seen) > self.max_keys:
            self._seen.popitem(last=False)
        return False

    def __len__(self):
        return len(self._seen)
