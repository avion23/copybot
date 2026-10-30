"""Watcher health and evidence decisions. No operator writes or trading requests."""

import glob
import json
import os
import re

EVENT_ORDER_SLOP_SECS = 300
WATCHER_MAX_AGE = 90
STALE_HALT_SECS = 15 * 60
WATCHER_BLIND_HALT_SECS = int(os.environ.get('WATCHER_BLIND_HALT_SECS', '600'))
RECONNECT_RATE_PER_HOUR = float(os.environ.get('RECONNECT_RATE_PER_HOUR', '12'))
RESUMABLE_CODES = frozenset({'watcher_feed', 'watcher_heartbeat', 'watcher_leaders'})
RESUME_CLEAN_CHECKS = int(os.environ.get('GUARDIAN_RESUME_CHECKS', '11'))
RESUME_MAX_PER_HOUR = int(os.environ.get('GUARDIAN_RESUME_MAX_PER_HOUR', '3'))

def resumable_halt(meta, codes=None):
    if not isinstance(meta, dict):
        return False
    if str(meta.get('by') or '') != 'guardian':
        return False
    got = codes if codes is not None else meta.get('codes')
    if not isinstance(got, (list, tuple, set)) or not got:
        return False
    return all((str(c).split(':', 1)[0] in RESUMABLE_CODES for c in got))
def reconnect_trend(feed, state, now):
    st = dict(state or {})
    n = (feed or {}).get('reconnects')
    if not isinstance(n, (int, float)):
        return (None, st)
    prev_n, prev_t = (st.get('reconnects_seen'), st.get('reconnects_t'))
    st['reconnects_seen'], st['reconnects_t'] = (int(n), now)
    if not isinstance(prev_n, (int, float)) or not isinstance(prev_t, (int, float)):
        return (None, st)
    dt = now - prev_t
    if dt <= 0 or n < prev_n:
        return (None, st)
    rate = (n - prev_n) / dt * 3600.0
    if rate > RECONNECT_RATE_PER_HOUR:
        return ('the watcher feed is repairing itself %.0f times/hour (threshold %.0f) — the provider is degrading; it is being fixed automatically, but this is how the 2026-08-13 outage began' % (rate, RECONNECT_RATE_PER_HOUR), st)
    return (None, st)
def plan_resume(lanes, meta, faults_active, state, now):
    st = dict(state or {})
    halted = sorted((n for n, l in (lanes or {}).items() if l.get('halted')))
    if not halted:
        st.pop('resume_clean', None)
        return ([], st, '')
    if not resumable_halt(meta):
        st.pop('resume_clean', None)
        return ([], st, '')
    scope = meta.get('lanes') if isinstance(meta, dict) else None
    if isinstance(scope, (list, tuple, set)) and scope:
        halted = [n for n in halted if n in {str(x) for x in scope}]
        if not halted:
            st.pop('resume_clean', None)
            return ([], st, '')
    if faults_active:
        st['resume_clean'] = 0
        return ([], st, '')
    clean = int(st.get('resume_clean') or 0) + 1
    st['resume_clean'] = clean
    if clean < RESUME_CLEAN_CHECKS:
        return ([], st, 'clean %d/%d' % (clean, RESUME_CLEAN_CHECKS))
    recent = [t for t in st.get('resume_history') or [] if now - t < 3600]
    if len(recent) >= RESUME_MAX_PER_HOUR:
        st['resume_history'] = recent
        st['resume_clean'] = 0
        return ([], st, 'RESUME SUPPRESSED: %d automatic resumes already this hour — a fault that keeps returning needs a human, not another restart' % len(recent))
    st['resume_history'] = recent + [now]
    st['resume_clean'] = 0
    return (halted, st, "the halt's cause (%s) has been absent for %d consecutive checks" % (', '.join(meta.get('codes') or ['?']), clean))

def stale_halts(lanes, now, threshold=STALE_HALT_SECS):
    out = []
    for name, l in sorted((lanes or {}).items()):
        if not l.get('halted'):
            continue
        if l.get('armed') is False:
            continue
        if 'halted_since' not in l:
            continue
        try:
            since = int(l.get('halted_since') or 0)
        except (TypeError, ValueError):
            since = 0
        age = now - since if since > 0 else threshold
        if age >= threshold:
            out.append((name, int(age)))
    out.sort(key=lambda r: -r[1])
    return out

def human_secs(n):
    n = int(n)
    if n < 3600:
        return '%dm' % (n // 60)
    if n < 86400:
        return '%.1fh' % (n / 3600.0)
    return '%.1fd' % (n / 86400.0)

def check_watcher_criticals(lines):
    hits = [l for l in lines if '[CRITICAL]' in l]
    if hits:
        seen, uniq = ({}, [])
        for h in hits:
            h = h.strip()
            kind = h.split('[CRITICAL]', 1)[-1].split(':', 1)[0].strip() or h
            if kind in seen:
                i = seen[kind]
                if LANES_MARKER in h and LANES_MARKER not in uniq[i]:
                    uniq[i] = h
                continue
            seen[kind] = len(uniq)
            uniq.append(h)
        return (False, ['watcher CRITICAL: %s' % h for h in uniq[:5]])
    return (True, [])
LANES_MARKER = '[lanes='

def scoped_lanes_from_marker(reason, known):
    r = str(reason)
    i = r.find(LANES_MARKER)
    if i < 0:
        return None
    j = r.find(']', i)
    if j < 0:
        return None
    named = {x.strip() for x in r[i + len(LANES_MARKER):j].split(',') if x.strip()}
    return {x for x in named if not known or x in known}

def critical_scope(reasons, lanes):
    known = [l for l in lanes or [] if l]
    if not known or not reasons:
        return list(known)
    named = set()
    for r in reasons:
        marked = scoped_lanes_from_marker(r, set(known))
        if marked is not None:
            if len(marked) != 1:
                return list(known)
            named |= marked
            continue
        hits = {l for l in known if re.search('\\b%s\\b' % re.escape(l), r)}
        if len(hits) != 1:
            return list(known)
        named |= hits
    if len(named) != 1:
        return list(known)
    return sorted(named)

def read_new_alerts(path, last_seq, log):
    try:
        with open(path) as f:
            raw = f.readlines()
    except FileNotFoundError:
        return ([], last_seq)
    except OSError as e:
        return (['[CRITICAL] watcher alert sidecar unavailable: %s' % e], last_seq)
    out, hi = ([], int(last_seq or 0))
    seqs = []
    for line in raw:
        try:
            seqs.append(int(json.loads(line.strip() or '{}').get('seq') or 0))
        except (ValueError, AttributeError):
            continue
    if seqs and hi > 0 and (max(seqs) < hi):
        log('watcher alert sequence went BACKWARDS (file max %d < cursor %d) — the watcher restarted; replaying its alerts rather than discarding them' % (max(seqs), hi))
        last_seq, hi = (0, 0)
    for line in raw:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        seq = int(row.get('seq') or 0)
        if last_seq is not None and seq <= int(last_seq):
            continue
        hi = max(hi, seq)
        lanes_field = row.get('lanes')
        marker = ''
        if isinstance(lanes_field, list) and lanes_field:
            marker = '[lanes=%s] ' % ','.join(sorted((str(x) for x in lanes_field if x)))
        out.append('%s[%s] %s: %s' % (marker, row.get('level', 'CRITICAL'), row.get('kind', '?'), row.get('msg', '')))
    if last_seq is None:
        return ([], hi)
    return (out, hi)

def read_new_lines(path, last_offset):
    try:
        size = os.path.getsize(path)
    except OSError as e:
        return (['[CRITICAL] watcher log unavailable: %s' % e], last_offset)
    if last_offset is None:
        return ([], size)
    start = last_offset if last_offset <= size else 0
    try:
        with open(path) as f:
            f.seek(start)
            new = f.read()
    except OSError as e:
        return (['[CRITICAL] watcher log unreadable: %s' % e], last_offset)
    lines = new.splitlines()
    return (lines, size)

def reverse_lines(path, chunk_size=65536):
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        pending = b''
        while pos:
            take = min(chunk_size, pos)
            pos -= take
            f.seek(pos)
            pending = f.read(take) + pending
            parts = pending.split(b'\n')
            pending = parts[0]
            for line in reversed(parts[1:]):
                if line:
                    yield line.decode('utf-8', 'replace')
        if pending:
            yield pending.decode('utf-8', 'replace')

def event_ts(event):
    try:
        ts = float(event.get('t', 0))
    except (TypeError, ValueError):
        return 0.0
    return ts / 1000.0 if ts >= 100000000000 else ts

def recent_fires(bot_dir, since_ts):
    out = []
    for path in sorted(glob.glob(os.path.join(bot_dir, 'data', 'events-*.jsonl')), reverse=True):
        for line in reverse_lines(path):
            try:
                event = json.loads(line)
            except Exception:
                continue
            ts = event_ts(event)
            if ts and ts < since_ts - EVENT_ORDER_SLOP_SECS:
                return out
            if event.get('ev') == 'fire' and ts >= since_ts:
                out.append(event)
    return out

def _feed_blind_over_tolerance(feed):
    if feed.get('booting'):
        return False
    blind = feed.get('decode_age_secs')
    if not isinstance(blind, (int, float)):
        return True
    return blind >= WATCHER_BLIND_HALT_SECS

def watcher_health_issues(now, path):
    issues = []
    try:
        with open(path) as f:
            heartbeat = json.load(f)
        age = now - float(heartbeat['t'])
        if age > WATCHER_MAX_AGE:
            issues.append(('watcher_heartbeat', 'watcher heartbeat stale by %.0fs' % age))
        if heartbeat.get('leaders_error'):
            issues.append(('watcher_leaders', 'watcher cannot read the wallet registry, so lane coverage is unknown: %s' % str(heartbeat['leaders_error'])[:120]))
        feed = heartbeat.get('feed')
        if feed is None:
            issues.append(('watcher_feed', 'watcher is not reporting feed health — it cannot prove it can still see his fills (old watcher build?)'))
        elif not feed.get('useful') or _feed_blind_over_tolerance(feed):
            blind = feed.get('decode_age_secs')
            detail = 'connected=%s frame_age=%ss decode_age=%ss reconnects=%s last_error=%s' % (feed.get('connected'), feed.get('frame_age_secs'), feed.get('decode_age_secs'), feed.get('reconnects'), str(feed.get('last_error'))[:80])
            if isinstance(blind, (int, float)) and blind < WATCHER_BLIND_HALT_SECS:
                issues.append(('watcher_feed_brief', 'watcher feed blind %.0fs (tolerance %ss) — REPORTED, NOT HALTED: the trade path is a separate feed and exits are unaffected. %s' % (blind, WATCHER_BLIND_HALT_SECS, detail)))
            else:
                issues.append(('watcher_feed', 'watcher feed is not usable for %s (over the %ss tolerance): %s' % ('%.0fs' % blind if isinstance(blind, (int, float)) else 'an unknown time (it has never decoded)', WATCHER_BLIND_HALT_SECS, detail)))
    except (OSError, ValueError, KeyError, TypeError) as e:
        issues.append(('watcher_heartbeat', 'watcher heartbeat unavailable: %s' % e))
    return issues
