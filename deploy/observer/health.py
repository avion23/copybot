"""Durable Observer-owner audit completion receipt; no market claims or I/O.

A new Observer clears any prior process receipt while holding its exclusive
owner lease. The scheduler reports actual finished audit results into one
atomically replaced, fsynced health snapshot. A running process is not proof
of correct market-data provenance, order safety or delivery.
"""
import json
import math
import os
from pathlib import Path
import stat
import threading
import time
import uuid


class ObserverHealth:
    """Single-owner, atomic source-grade health receipt for cutover preflight."""

    SCHEMA = 1

    def __init__(self, path, *, output=None):
        self.path = Path(path)
        self.output = output or (
            lambda row: print(json.dumps(row, sort_keys=True, separators=(",", ":")),
                              flush=True)
        )
        self._lock = threading.Lock()
        self._state = {
            "schema": self.SCHEMA,
            "owner_state": "INIT",
            "started_at": None,
            "guardian": None,
            "audits": {},
            "updated_at": time.time(),
        }
        self._persist()

    def _safe_parent(self):
        info = os.stat(self.path.parent, follow_symlinks=False)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o022):
            raise PermissionError("Observer health parent is not a safe owned directory")

    def _safe_destination(self):
        try:
            info = os.stat(self.path, follow_symlinks=False)
        except FileNotFoundError:
            return
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise PermissionError("Observer health receipt must be owned mode-0600")

    def _persist(self):
        self._safe_parent()
        self._safe_destination()
        data = (json.dumps(self._state, sort_keys=True,
                           separators=(",", ":")) + "\n").encode("utf-8")
        temp = self.path.with_name(self.path.name + "." + uuid.uuid4().hex + ".tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                     | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            parent = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY
                             | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        finally:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _timestamp(row):
        stamp = row.get("observed_at")
        if (type(stamp) not in (int, float) or not math.isfinite(stamp)
                or stamp < 0):
            raise ValueError("Observer event timestamp is missing or invalid")
        return stamp

    def record(self, event):
        """Only actual Observer events may update this independent receipt."""
        if not isinstance(event, dict):
            raise TypeError("Observer event must be a dictionary")
        with self._lock:
            kind = event.get("ev")
            if kind in ("observer_started", "observer_audit", "observer_stopping"):
                stamp = self._timestamp(event)
                if kind == "observer_started":
                    if self._state["owner_state"] != "INIT":
                        raise RuntimeError("Observer owner started more than once")
                    self._state["owner_state"] = "STARTED"
                    self._state["started_at"] = stamp
                    self._state["guardian"] = None
                    self._state["audits"] = {}
                elif kind == "observer_stopping":
                    if self._state["owner_state"] != "STARTED":
                        raise RuntimeError("Observer cannot stop without starting")
                    self._state["owner_state"] = "STOPPING"
                else:
                    if self._state["owner_state"] != "STARTED":
                        raise RuntimeError("audit cannot complete outside active Observer")
                    if stamp < self._state["started_at"]:
                        raise ValueError("audit result predates current owner process")
                    source = event.get("source")
                    status = event.get("status")
                    code = event.get("exit_code")
                    if (source not in ("guardian", "watcher", "fillwatch", "buywatch")
                            or status not in ("PASS", "ALERT", "UNVERIFIABLE")
                            or type(code) is not int):
                        raise ValueError("invalid Observer audit completion record")
                    result = {"completed_at": stamp, "status": status,
                              "exit_code": code}
                    self._state["audits"][source] = result
                    if source == "guardian":
                        self._state["guardian"] = result
                self._state["updated_at"] = stamp
                self._persist()
            self.output(event)

    def snapshot(self):
        """Read-only for offline tests; runtime readers use the on-disk file."""
        with self._lock:
            return json.loads(json.dumps(self._state))
