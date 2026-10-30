"""One cross-process Observer/legacy sensor lease; no network or credentials.

The new Observer takes an exclusive lock. Legacy sensors take shared locks,
allowing the old independent schedules to coexist until explicit cutover.
"""
import fcntl
import os
from pathlib import Path
import stat
import sys


def owner_lock_path(bot_dir):
    return Path(bot_dir) / "run" / "observer.owner.lock"


class _Lease:
    _mode = None

    def __init__(self, path):
        self.path = Path(path)
        self._fd = None

    def acquire(self):
        if self._fd is not None:
            raise RuntimeError("Observer owner lease already acquired")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise RuntimeError("Observer owner lease must be a regular owned file")
            fcntl.flock(fd, self._mode | fcntl.LOCK_NB)
        except Exception:
            os.close(fd)
            raise
        self._fd = fd
        return self

    def close(self):
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, _exc_type, _exc, _trace):
        self.close()


class ObserverLease(_Lease):
    """Exclusive single-Observer process owner."""
    _mode = fcntl.LOCK_EX


class LegacyLease(_Lease):
    """Shared legacy-script lease; incompatible with active Observer."""
    _mode = fcntl.LOCK_SH


def legacy_entrypoint(entrypoint, source, bot_dir, unavailable_exit):
    """Refuse a CLI monitor if the new Observer currently owns the lock.

    Does not catch exceptions inside the audit: legacy fail-closed handling
    and exit codes stay owned by the original entrypoint.
    """
    lease = LegacyLease(owner_lock_path(bot_dir))
    try:
        lease.acquire()
    except (OSError, RuntimeError) as error:
        print("%s UNVERIFIABLE: observer owner active or lease unavailable (%s)" %
              (source, type(error).__name__), file=sys.stderr, flush=True)
        return unavailable_exit
    try:
        return entrypoint() or 0
    finally:
        lease.close()
