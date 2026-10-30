"""Exclusive, audited email delivery owner for opt-in single Observer.

No private key, recipient or message body is stored in the delivery journal.
A durable attempt precedes network I/O; a durable outcome follows it. The
external provider's acceptance cannot be made atomic with local fsync.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time
import uuid


class NotificationDelivery:
    """One synchronized sender; no independent queue or retry side effects."""

    def __init__(self, path, transport, *, source=None):
        if not callable(transport):
            raise TypeError("notification transport must be callable")
        if source is not None and not callable(source):
            raise TypeError("notification source must be callable")
        self.path = Path(path)
        self.transport = transport
        self.source = source or (lambda: None)
        self._lock = threading.Lock()
        self._fd = None
        self.pending = set()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise PermissionError("Observer delivery journal must be owner-only mode 0600")
            self._fd = fd
            self._restore()
        except Exception:
            os.close(fd)
            self._fd = None
            raise

    def _restore(self):
        # Stream records, retaining only unsettled attempt IDs.
        os.lseek(self._fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(self._fd), "rb") as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("invalid Observer notification journal record")
                event = row.get("ev")
                key = row.get("attempt")
                if not isinstance(key, str) or len(key) != 32:
                    raise ValueError("invalid Observer notification attempt ID")
                if event == "notification_attempt":
                    if key in self.pending:
                        raise ValueError("duplicate Observer notification attempt")
                    self.pending.add(key)
                elif event == "notification_outcome":
                    if key not in self.pending:
                        raise ValueError("outcome without Observer notification attempt")
                    if row.get("status") not in ("SENT", "FAILED"):
                        raise ValueError("invalid Observer notification status")
                    self.pending.remove(key)
                else:
                    raise ValueError("unexpected Observer notification record type")

    def _append(self, record):
        if self._fd is None:
            raise RuntimeError("Observer notification delivery journal is closed")
        data = (json.dumps(record, ensure_ascii=False, separators=(",", ":"),
                           sort_keys=True) + "\n").encode("utf-8")
        pos = 0
        while pos < len(data):
            n = os.write(self._fd, data[pos:])
            if n <= 0:
                raise OSError("Observer notification journal append made no progress")
            pos += n
        os.fsync(self._fd)

    def send(self, subject, body, log=print):
        """Fail closed on an unwritable attempt; preserve notify.send bool API."""
        if not isinstance(subject, str) or not subject.strip():
            raise ValueError("notification subject must be nonempty")
        if not isinstance(body, str):
            raise TypeError("notification body must be text")
        with self._lock:
            key = uuid.uuid4().hex
            # No PII, body, email address or API token appears in this row.
            signature = hashlib.sha256(
                (subject + "\0" + body).encode("utf-8")
            ).hexdigest()
            try:
                provenance = self.source()
            except Exception:
                provenance = None
            if not isinstance(provenance, str) or not provenance.strip():
                provenance = "unattributed"
            attempt = {
                "ev": "notification_attempt",
                "attempt": key,
                "source": provenance,
                "at": time.time(),
                "sha256": signature,
            }
            try:
                self._append(attempt)
                self.pending.add(key)
            except (OSError, RuntimeError) as error:
                log("  ALERT NOT SENT (Observer attempt evidence unavailable: %s)" %
                    type(error).__name__)
                return False

            try:
                result = self.transport(subject, body, log=log)
                sent = result is True
                reason = "provider_accepted" if sent else "provider_not_confirmed"
            except Exception as error:
                sent = False
                reason = "transport_exception:" + type(error).__name__
                log("  ALERT FAILED (Observer transport %s)" % type(error).__name__)

            outcome = {
                "ev": "notification_outcome",
                "attempt": key,
                "at": time.time(),
                "status": "SENT" if sent else "FAILED",
                "reason": reason,
            }
            try:
                self._append(outcome)
                self.pending.remove(key)
            except (OSError, RuntimeError) as error:
                # Delivery may already have been accepted; do not claim a
                # durable success or manufacture an exactly-once guarantee.
                log("  ALERT UNVERIFIABLE (Observer outcome evidence unavailable: %s)" %
                    type(error).__name__)
                return False
            return sent

    def close(self):
        with self._lock:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _trace):
        self.close()
