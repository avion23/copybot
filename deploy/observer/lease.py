"""One exclusive Observer-owner lease; no other monitoring process exists."""
import fcntl
import os
from pathlib import Path
import stat


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
        parent = os.stat(self.path.parent, follow_symlinks=False)
        if (not stat.S_ISDIR(parent.st_mode)
                or parent.st_uid != os.geteuid()
                or stat.S_IMODE(parent.st_mode) & 0o022):
            raise PermissionError(
                "Observer run directory must be owned and not group/other writable"
            )
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_nlink != 1):
                raise PermissionError(
                    "Observer lease must be a single-link owned mode-0600 file"
                )
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
