"""Local Guardian state and alert-journal writes; all destinations and log callbacks explicit."""

import fcntl
import hashlib
import json
import os
import time

from guardian_diagnostics import humanise

def tmp_path(path):
    return '%s.%d.tmp' % (path, os.getpid())

def fsync_parent(path):
    directory = os.path.dirname(path) or '.'
    flags = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0)
    fd = os.open(directory, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

ERROR_LOG_MAX = 400
_ERROR_SINK = None


def configure_error_sink(sink):
    """Only the opt-in Observer installs a common Guardian error writer."""
    global _ERROR_SINK
    if sink is not None and not callable(sink):
        raise TypeError("Guardian error sink must be callable")
    previous = _ERROR_SINK
    _ERROR_SINK = sink
    return previous


def persist_error_row(row, path, *, durable=False):
    """Original Guardian error schema, file lock and 400-row rotation.

    Observer mode additionally fsyncs data and rotated file/parent.
    Filesystem failures propagate so a claimed write cannot be fabricated.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + '.lock', 'w') as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            with open(path, 'a') as f:
                f.write(json.dumps(row) + '\n')
                if durable:
                    f.flush()
                    os.fsync(f.fileno())
            with open(path) as f:
                lines = f.readlines()
            if len(lines) > ERROR_LOG_MAX * 2:
                tmp = tmp_path(path)
                if durable:
                    flags = (os.O_WRONLY | os.O_CREAT | os.O_TRUNC
                             | os.O_CLOEXEC | os.O_NOFOLLOW)
                    fd = os.open(tmp, flags, 0o600)
                    with os.fdopen(fd, 'w') as f:
                        f.writelines(lines[-ERROR_LOG_MAX:])
                        f.flush()
                        os.fsync(f.fileno())
                else:
                    with open(tmp, 'w') as f:
                        f.writelines(lines[-ERROR_LOG_MAX:])
                os.replace(tmp, path)
                if durable:
                    fsync_parent(path)
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)


def record_error(lane, kind, detail, path, enabled):
    if not enabled:
        return
    sink = _ERROR_SINK
    try:
        row = {
            't': int(time.time()), 'lane': lane, 'kind': kind,
            'detail': detail, 'human': humanise(kind, detail),
            'severity': 'stop' if kind == 'trip' else 'notice',
        }
        if sink is not None:
            # Observer-only evidence errors must reach guardian_once, whose
            # existing exception path conservatively disarms buys.
            return sink(row, path)
        persist_error_row(row, path)
    except Exception:
        if sink is not None:
            raise
        # Preserve the legacy standalone behavior without changing timers.
        pass
ADVISORY_RESAY_SECS = int(os.environ.get('ADVISORY_RESAY_SECS', '900'))

def record_advisories(advisory, now, path, record_error):
    codes = {c: m for c, m in advisory or []}
    state = {}
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, ValueError):
        pass
    open_adv = state if isinstance(state, dict) else {}
    changed = False
    for code in sorted(codes):
        prev = open_adv.get(code) if isinstance(open_adv.get(code), dict) else None
        said = prev.get('said') if prev else None
        if prev and isinstance(said, (int, float)) and (now - said < ADVISORY_RESAY_SECS):
            continue
        record_error(code.split(':', 1)[-1] if ':' in code else '', 'advisory:%s' % code.split(':', 1)[0], codes[code])
        open_adv[code] = {'since': (prev or {}).get('since', now), 'said': now}
        changed = True
    for code in [c for c in list(open_adv) if c not in codes]:
        since = (open_adv[code] or {}).get('since')
        how_long = ' after %.0f min' % ((now - since) / 60.0) if isinstance(since, (int, float)) else ''
        record_error(code.split(':', 1)[-1] if ':' in code else '', 'advisory_cleared:%s' % code.split(':', 1)[0], '%s has cleared%s — no action needed.' % (code, how_long))
        del open_adv[code]
        changed = True
    if not changed:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = tmp_path(path)
        with open(tmp, 'w') as f:
            json.dump(open_adv, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        fsync_parent(path)
    except OSError:
        pass
EQUITY_POINT_SECS = int(os.environ.get('EQUITY_POINT_SECS', '60'))
EQUITY_HISTORY_MAX = int(os.environ.get('EQUITY_HISTORY_MAX', '20160'))

def record_equity_point(pool, now, path):
    try:
        if not isinstance(pool, dict):
            return
        w = pool.get('wallet')
        if not isinstance(w, dict) or w.get('equity_pnl') is None:
            return
        try:
            last = os.path.getmtime(path)
            if now - last < EQUITY_POINT_SECS:
                return
        except OSError:
            pass
        lanes = pool.get('lanes')
        if isinstance(lanes, dict):
            lanes = list(lanes.values())
        realised = sum((float(l.get('realised_pnl') or 0.0) for l in lanes or [] if isinstance(l, dict)))
        eq = float(w['equity_pnl'])
        row = {'t': int(now), 'equity_pnl': round(eq, 2), 'portfolio': round(float(w.get('portfolio') or 0.0), 2), 'realised': round(realised, 2), 'unrealised': round(eq - realised, 2), 'funding': round(float(w.get('funding_basis') or 0.0), 2)}
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'a') as f:
            f.write(json.dumps(row) + '\n')
        if int(now) % 3600 < EQUITY_POINT_SECS:
            with open(path) as f:
                lines = f.readlines()
            if len(lines) > EQUITY_HISTORY_MAX * 2:
                tmp = tmp_path(path)
                with open(tmp, 'w') as f:
                    f.writelines(lines[-EQUITY_HISTORY_MAX:])
                os.replace(tmp, path)
    except Exception:
        pass

REFUTED_QUIET_SECS = 3600

def refutation_is_new(lane, reason, now, path):
    key = '%s|%s' % (lane, hashlib.sha256(str(reason).encode()).hexdigest()[:16])
    try:
        with open(path) as f:
            seen = json.load(f)
        if not isinstance(seen, dict):
            seen = {}
    except (OSError, ValueError):
        seen = {}
    last = seen.get(key)
    fresh = not (isinstance(last, (int, float)) and now - last < REFUTED_QUIET_SECS)
    if fresh:
        seen[key] = now
        seen = {k: t for k, t in seen.items() if isinstance(t, (int, float)) and now - t < REFUTED_QUIET_SECS * 24}
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = tmp_path(path)
            with open(tmp, 'w') as f:
                json.dump(seen, f)
            os.replace(tmp, path)
        except OSError:
            pass
    return fresh
LAG_DEFER_WINDOW_SECS = 120

def defer_for_lag(lane, token, now, path):
    key = '%s|%s' % (lane, str(token)[-16:])
    try:
        with open(path) as f:
            seen = json.load(f)
        if not isinstance(seen, dict):
            seen = {}
    except (OSError, ValueError):
        seen = {}
    last = seen.get(key)
    if isinstance(last, (int, float)) and now - last < LAG_DEFER_WINDOW_SECS:
        return False
    seen[key] = now
    seen = {k: t for k, t in seen.items() if isinstance(t, (int, float)) and now - t < LAG_DEFER_WINDOW_SECS * 10}
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = tmp_path(path)
        with open(tmp, 'w') as f:
            json.dump(seen, f)
        os.replace(tmp, path)
    except OSError:
        return False
    return True
def persist_state(state, path, log):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = tmp_path(path)
        with open(tmp, 'w') as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        fsync_parent(path)
        return True
    except OSError as e:
        log('could not persist guardian state: %s' % e)
        return False
