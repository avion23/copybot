"""One durable corroboration evidence journal for the opt-in Observer only.

No signing key, operator command, alert side effect or remote feed is used
here. A failed write must remain visible to the caller as Unverifiable.
"""
from collections import deque
from dataclasses import fields
import json
import math
import os
from pathlib import Path
import stat
import threading
import time

from guardian_state_store import persist_error_row
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


class AlertRouter:
    """One opt-in writer for canonical alerts and Guardian-compatible sidecars.

    The original watcher/fillwatch/buywatch standalone writers remain active
    only when no AlertRouter sink has been installed by the single Observer.
    This is not an email notifier or an order/halt authority.
    """

    def __init__(self, run_dir, *, watcher_path=None, buywatch_path=None,
                 watcher_log_path=None):
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.canonical_path = self.run_dir / "observer_alerts.jsonl"
        self.error_path = self.run_dir / "errors.jsonl"
        self.watcher_path = (Path(watcher_path) if watcher_path is not None
                             else self.run_dir / "watcher_alerts.jsonl")
        self.buywatch_path = (Path(buywatch_path) if buywatch_path is not None
                              else self.run_dir / "buywatch_alerts.jsonl")
        self.watcher_log_path = (
            Path(watcher_log_path) if watcher_log_path is not None
            else self.run_dir / "watcher.log"
        )
        self._lock = threading.Lock()
        self._seq = self._read_watcher_seq()
        # Validate the destination before installing a sensor sink.
        fd = self._open(self.canonical_path, create=True, strict=True)
        os.close(fd)
        # Guardian still reads the legacy watcher log after the old watcher
        # service is stopped. This compatibility file must exist at cutover.
        fd = self._open(self.watcher_log_path, create=True)
        os.close(fd)
        # Guardian's existing error-journal consumers need this same path
        # after a cutover. Unsafe preexisting permissions refuse startup.
        fd = self._open(self.error_path, create=True, strict=True)
        os.close(fd)

    @staticmethod
    def _open(path, *, create, strict=False):
        flags = os.O_CLOEXEC | os.O_NOFOLLOW | (os.O_RDWR if create else os.O_RDONLY)
        if create:
            flags |= os.O_CREAT | os.O_APPEND
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            mode = stat.S_IMODE(info.st_mode)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or (mode != 0o600 if strict else bool(mode & 0o022))):
                raise PermissionError("unsafe Observer alert destination: %s" % path)
        except Exception:
            os.close(fd)
            raise
        return fd

    def _read_watcher_seq(self):
        try:
            fd = self._open(self.watcher_path, create=False)
        except FileNotFoundError:
            return 0
        seq = 0
        with os.fdopen(fd, "rb") as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("invalid watcher alert row")
                current = row.get("seq")
                if type(current) is not int or current <= 0:
                    raise ValueError("watcher alert sequence must be positive")
                seq = max(seq, current)
        return seq

    def _write(self, path, row, *, strict=False):
        data = (json.dumps(row, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")) + "\n").encode("utf-8")
        fd = self._open(path, create=True, strict=strict)
        try:
            cursor = 0
            while cursor < len(data):
                count = os.write(fd, data[cursor:])
                if count <= 0:
                    raise OSError("Observer alert write made no progress")
                cursor += count
            os.fsync(fd)
        finally:
            os.close(fd)

    def _write_watcher_log(self, stamp, level, kind, message, lanes):
        """Only alert lines, in the exact Guardian-readable legacy format."""
        marker = "[lanes=%s] " % ",".join(sorted(set(lanes))) if lanes else ""
        clean_kind = kind.replace("\n", " ").replace("\r", " ")
        clean_message = message.replace("\n", " ").replace("\r", " ")
        line = "%s %s[%s] %s: %s\n" % (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stamp)),
            marker, level, clean_kind, clean_message,
        )
        data = line.encode("utf-8")
        fd = self._open(self.watcher_log_path, create=True)
        try:
            cursor = 0
            while cursor < len(data):
                count = os.write(fd, data[cursor:])
                if count <= 0:
                    raise OSError("Observer watcher-log projection made no progress")
                cursor += count
            os.fsync(fd)
        finally:
            os.close(fd)

    def record_guardian_error(self, row, path):
        """One opt-in writer for Guardian's legacy error and alert evidence."""
        if Path(path) != self.error_path:
            raise ValueError("Guardian error path is outside the Observer owner")
        if not isinstance(row, dict):
            raise TypeError("Guardian error must be a dictionary")
        if (type(row.get("t")) is not int or row["t"] < 0
                or not isinstance(row.get("lane"), str)
                or not isinstance(row.get("kind"), str) or not row["kind"].strip()
                or not isinstance(row.get("detail"), str)
                or not isinstance(row.get("human"), str)
                or row.get("severity") not in ("stop", "notice")):
            raise ValueError("invalid Guardian error-journal schema")
        with self._lock:
            # Validate the existing file on EVERY write, not only startup,
            # because an external process may have rotated or replaced it.
            fd = self._open(self.error_path, create=True, strict=True)
            os.close(fd)
            persist_error_row(row, str(self.error_path), durable=True)
            event = {
                "ev": "observer_alert", "source": "guardian", "t": row["t"],
                "level": "CRITICAL" if row["severity"] == "stop" else "INFO",
                "kind": row["kind"], "msg": row["human"],
                "lanes": [row["lane"]] if row["lane"] else [],
            }
            self._write(self.canonical_path, event, strict=True)
            return event

    def record(self, source, record):
        """Commit one legacy projection and one canonical alert, serially."""
        if source not in ("watcher", "fillwatch", "buywatch"):
            raise ValueError("unsupported Observer alert source")
        if not isinstance(record, dict):
            raise TypeError("alert must be a dictionary")
        stamp = record.get("t")
        if (isinstance(stamp, bool) or not isinstance(stamp, (int, float))
                or not math.isfinite(stamp) or stamp < 0):
            raise ValueError("alert timestamp must be finite and nonnegative")
        kind = record.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("alert kind required")
        level = record.get("level", "WARNING" if source == "buywatch" else "CRITICAL")
        if level not in ("INFO", "WARN", "WARNING", "CRITICAL"):
            raise ValueError("invalid alert level")
        message = record.get("msg")
        if source == "buywatch":
            count = record.get("count")
            if type(count) is not int or count <= 0:
                raise ValueError("buywatch unseen count must be positive")
            message = "unseen leader buys: %d" % count
        if not isinstance(message, str) or not message.strip():
            raise ValueError("alert must carry a message")
        lanes = record.get("lanes") or []
        if (not isinstance(lanes, list)
                or any(not isinstance(lane, str) or not lane for lane in lanes)):
            raise ValueError("alert lanes must be a list of nonempty names")

        with self._lock:
            sidecar = dict(record)
            if source != "buywatch":
                next_seq = self._seq + 1
                sidecar["seq"] = next_seq
                sidecar["src"] = source
                self._write(self.watcher_path, sidecar)
                self._seq = next_seq
                if source == "watcher":
                    self._write_watcher_log(
                        stamp, level, kind, message, lanes,
                    )
            else:
                # This format is read using the original timestamp cursor.
                self._write(self.buywatch_path, sidecar)

            event = {
                "ev": "observer_alert", "source": source, "t": stamp,
                "level": level, "kind": kind, "msg": message,
                "lanes": sorted(set(lanes)),
            }
            if source != "buywatch":
                event["legacy_seq"] = self._seq
            if source == "buywatch":
                event["count"] = record["count"]
            # A central write failure is explicit. Already committed legacy
            # critical sidecars remain visible to Guardian for safe retry.
            self._write(self.canonical_path, event, strict=True)
            return event
