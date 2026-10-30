"""Single Observer evidence journal and alert router.

The same owner-only JSONL contains corroboration verdicts and monitor alerts.
There are no watcher/buywatch alert sidecars, replay shims, or independent
alert sequence stores. Engine errors remain in its existing runtime journal.
"""
from collections import deque
from dataclasses import fields
import json
import os
from pathlib import Path
import stat
import threading
import time

from guardian_state_store import persist_error_row
from observer.contract import Observation
from observer.policy import OccurrenceDeduper, alert_level


class ObservationJournal:
    def __init__(self, path, *, ttl_seconds=900, max_keys=4096):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._deduper = OccurrenceDeduper(ttl_seconds=ttl_seconds, max_keys=max_keys)
        self._fd = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                raise PermissionError("Observer evidence file must be owned mode 0600")
            self._fd = fd
            self._restore(max_keys * 2)
        except Exception:
            os.close(fd)
            raise

    def _restore(self, limit):
        os.lseek(self._fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(self._fd), "rb") as stream:
            tail = deque(stream, maxlen=limit)
        for raw in tail:
            row = json.loads(raw)
            if row.get("ev") != "observation":
                raise ValueError("invalid Observer journal row")
            kwargs = {
                f.name: row[f.name] for f in fields(Observation)
                if f.name != "_legacy_verdict"
            }
            self._deduper.duplicate(Observation(**kwargs))

    def record(self, observation):
        if not isinstance(observation, Observation):
            raise TypeError("Observer journal requires a typed Observation")
        with self._lock:
            if self._fd is None:
                raise RuntimeError("Observer journal is closed")
            if self._deduper.contains(observation):
                return False
            row = {"ev": "observation", **observation.as_dict(),
                   "alert_level": alert_level(observation)}
            data = (json.dumps(row, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":")) + "\n").encode()
            cursor = 0
            while cursor < len(data):
                n = os.write(self._fd, data[cursor:])
                if n <= 0:
                    raise OSError("Observer journal write made no progress")
                cursor += n
            os.fsync(self._fd)
            self._deduper.duplicate(observation)
            return True

    def close(self):
        with self._lock:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_unused):
        self.close()


class AlertRouter:
    """One validated router; notifications are owned by notify's single sink."""

    def __init__(self, journal, *, error_path):
        if not isinstance(journal, ObservationJournal):
            raise TypeError("router requires the Observer journal")
        self.journal = journal
        self.error_path = Path(error_path)

    def emit(self, source, stamp, level, kind, message, lanes=()):
        if source not in ("watcher", "fillwatch", "buywatch", "guardian", "observer"):
            raise ValueError("unknown monitor source")
        if level not in ("INFO", "WARN", "WARNING", "CRITICAL"):
            raise ValueError("unknown monitor alert level")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("alert kind required")
        if not isinstance(message, str) or not message.strip():
            raise ValueError("alert message required")
        if (not isinstance(lanes, (list, tuple))
                or any(not isinstance(n, str) or not n for n in lanes)):
            raise ValueError("invalid alert lane list")
        marker = "[lanes=%s] " % ",".join(sorted(set(lanes))) if lanes else ""
        return self.journal.record(Observation(
            source="alert:" + source + ":" + kind,
            observed_at=float(stamp), lane=lanes[0] if len(lanes) == 1 else None,
            market=None, token=None, leader_order=None, follower_order=None,
            fill_occurrence=None, status="Unverifiable",
            reason=marker + "[" + level + "] " + kind + ": " + message,
        ))

    def record(self, source, record):
        if not isinstance(record, dict):
            raise TypeError("alert must be a dictionary")
        message = record.get("msg")
        if source == "buywatch":
            count = record.get("count")
            if type(count) is not int or count <= 0:
                raise ValueError("buywatch unseen count must be positive")
            message = "unseen leader buys: %d" % count
        return self.emit(source, record["t"],
                         record.get("level", "WARNING" if source == "buywatch" else "CRITICAL"),
                         record["kind"], message, record.get("lanes") or [])

    def record_guardian_error(self, row, path):
        if Path(path) != self.error_path:
            raise ValueError("Guardian error path is outside the Observer owner")
        if (not isinstance(row, dict)
                or row.get("severity") not in ("stop", "notice")
                or not isinstance(row.get("kind"), str)
                or not isinstance(row.get("human"), str)):
            raise ValueError("invalid Guardian error")
        # The engine's errors journal is part of its operator/runtime contract,
        # not a second Observer alert stream.
        persist_error_row(row, str(self.error_path), durable=True)
        return self.emit("guardian", row["t"],
                         "CRITICAL" if row["severity"] == "stop" else "INFO",
                         row["kind"], row["human"], [row["lane"]] if row["lane"] else [])


def read_alerts(path, after=None, *, source=None):
    """Pure incremental projection of one append journal using a byte cursor.

    None baselines the current end. A truncated/replaced/corrupt file refuses
    consumption rather than treating lost alerts as a healthy empty stream.
    """
    rows = []
    with open(path, 'rb') as stream:
        stream.seek(0, os.SEEK_END)
        end = stream.tell()
        if after is None:
            return [], end
        if type(after) is not int or after < 0 or after > end:
            raise ValueError('invalid or truncated Observer alert cursor')
        stream.seek(after)
        while stream.tell() < end:
            row = json.loads(stream.readline())
            if not isinstance(row, dict) or row.get('ev') != 'observation':
                raise ValueError('Observer observation journal is invalid')
            name = row.get('source')
            if not isinstance(name, str) or not name.startswith('alert:'):
                continue
            if source is not None and name.split(':', 2)[1] != source:
                continue
            if not isinstance(row.get('reason'), str):
                raise ValueError('Observer alert reason is missing')
            rows.append(row)
        return rows, stream.tell()
