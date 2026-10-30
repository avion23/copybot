"""One durable corroboration evidence journal for the opt-in Observer only.

No signing key, operator command, alert side effect or remote feed is used
here. A failed write must remain visible to the caller as Unverifiable.
"""
from collections import deque
from dataclasses import fields
import json
import os
from pathlib import Path
import stat
import threading

from observer.contract import Observation
from observer.policy import OccurrenceDeduper, alert_level


class ObservationJournal:
    """Owner-only append log with cross-restart explicit-occurrence dedup."""

    def __init__(self, path, *, ttl_seconds=900, max_keys=4096):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._deduper = OccurrenceDeduper(
            ttl_seconds=ttl_seconds, max_keys=max_keys
        )
        self._fd = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or
                    info.st_uid != os.geteuid() or
                    stat.S_IMODE(info.st_mode) != 0o600):
                raise PermissionError("Observer evidence file must be owned mode 0600")
            self._fd = fd
            self._restore(max_keys * 2)
        except Exception:
            os.close(fd)
            self._fd = None
            raise

    def _restore(self, limit):
        # A bounded memory tail is sufficient for short-lived occurrence TTL.
        # The file is still scanned sequentially; no entire JSONL is buffered.
        os.lseek(self._fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(self._fd), "rb") as stream:
            tail = deque(stream, maxlen=limit)
        for raw in tail:
            row = json.loads(raw)
            if row.get("ev") != "observation":
                raise ValueError("invalid Observer journal record type")
            kwargs = {
                f.name: row[f.name] for f in fields(Observation)
                if f.name != "_legacy_verdict"
            }
            obs = Observation(**kwargs)
            self._deduper.duplicate(obs)

    def record(self, observation):
        if not isinstance(observation, Observation):
            raise TypeError("only validated Observation instances may be recorded")
        with self._lock:
            if self._fd is None:
                raise RuntimeError("Observer evidence journal is closed")
            if self._deduper.contains(observation):
                return False
            payload = {
                "ev": "observation",
                **observation.as_dict(),
                "alert_level": alert_level(observation),
            }
            data = (json.dumps(payload, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":")) + "\n").encode("utf-8")
            pos = 0
            while pos < len(data):
                wrote = os.write(self._fd, data[pos:])
                if wrote <= 0:
                    raise OSError("Observer evidence append made no progress")
                pos += wrote
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

    def __exit__(self, _exc_type, _exc, _trace):
        self.close()
