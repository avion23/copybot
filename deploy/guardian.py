import argparse
import collections
import glob
import fcntl
import hashlib
import json
import re
import socket
from guardian_foreign_writer import FOREIGN_PAGE_LIMIT, FOREIGN_EVERY_SECS, foreign_writer_check
from guardian_watcher_evidence import (
    event_ts, recent_fires, read_new_lines, read_new_alerts,
    reconnect_trend, resumable_halt, plan_resume, stale_halts, human_secs,
    check_watcher_criticals, scoped_lanes_from_marker, critical_scope,
    watcher_health_issues, reverse_lines,
)
from guardian_checks import (
    FIRE_PER_TOKEN_LIMIT, FIRE_PER_TOKEN_ABS, EXPOSURE_MARGIN, EXPOSURE_MIN_SHARES,
    TOTAL_FIRE_LIMIT, SHARE_API_EPSILON, MAX_STRANDED_DUST_SHARES, MAX_STRANDED_DUST_USD,
    MAX_ORPHAN_VALUE_USD, DRIFT_TRIP_SHARES,
    check_three_way, lane_caps, _num, benign_chain_surplus, venue_shares,
    conservation_breaches, net_chain_for_lane, check_share_drift,
    check_fire_patterns, worthless_orphan, check_unattributed,
)
import resolution
import scorecard
import preflight
import corroborate
import notify
import os
import sys
import time
import urllib.request
HIM = os.environ.get('WATCH_WALLET', '').lower()
OUR = os.environ.get('OUR_WALLET', '')
DATA_API = 'https://data-api.polymarket.com'
BOT_DIR = os.environ.get('BOT_DIR', '/opt/copybot')
BOT_PORT = int(os.environ.get('BOT_PORT', '8807'))
BOT_CONFIG = os.environ.get('BOT_CONFIG', os.path.join(BOT_DIR, 'deploy', 'copybot2.toml'))
WINDOW_SECS = 15 * 60
FRONTEND_LEDGER_TOLERANCE_USD = 0.011
SETTLE_QUIET_SECS = 120
GUARDIAN_DIR = os.path.join(BOT_DIR, 'run')
STATE_PATH = os.path.join(GUARDIAN_DIR, 'guardian_state.json')
LOG_PATH = os.path.join(GUARDIAN_DIR, 'guardian.log')
WATCHER_LOG = os.path.join(GUARDIAN_DIR, 'watcher.log')
WATCHER_HEARTBEAT = os.path.join(GUARDIAN_DIR, 'watcher_heartbeat.json')
WATCHER_ALERTS = os.path.join(GUARDIAN_DIR, 'watcher_alerts.jsonl')
BUYWATCH_ALERTS = os.path.join(BOT_DIR, 'run', 'buywatch_alerts.jsonl')
UNATTRIBUTED_ALLOW = {t.strip() for t in os.environ.get('GUARDIAN_UNATTRIBUTED_ALLOW', '').split(',') if t.strip()}
CONFIRM_REQUIRED_CHECKS = 2
GUARDIAN_PERIOD_SECS = 11
CONFIRM_MIN_INTERVAL_SECS = GUARDIAN_PERIOD_SECS
CONFIRM_PATH = os.path.join(GUARDIAN_DIR, 'guardian_confirmation.json')

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

def log(msg):
    line = '%s %s' % (time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), msg)
    print(line, flush=True)
    try:
        os.makedirs(GUARDIAN_DIR, exist_ok=True)
        with open(LOG_PATH, 'a') as f:
            f.write(line + '\n')
    except OSError as e:
        print('GUARDIAN LOG WRITE FAILED: %s' % e, file=sys.stderr, flush=True)

def http_json(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {'User-Agent': 'guardian'})
    return json.load(urllib.request.urlopen(req, timeout=timeout))
POLYMARKET_STATUS_SUMMARY = 'https://status.polymarket.com/v3/summary.json'
POLYMARKET_STATUS_COMPONENTS = 'https://status.polymarket.com/v3/components.json'
VENUE_TRADING_COMPONENTS = ('Trading API (CLOB)', 'Clob Websocket')
VENUE_OK = 'OPERATIONAL'

def _walk_components(items):
    for c in items or []:
        if isinstance(c, dict):
            yield c
            for kid in _walk_components(c.get('children')):
                yield kid

def check_venue_status():
    state = {'t': int(time.time()), 'ok': None, 'page': None, 'degraded': [], 'maintenances': [], 'incidents': [], 'error': None}
    try:
        summary = http_json(POLYMARKET_STATUS_SUMMARY)
        components = http_json(POLYMARKET_STATUS_COMPONENTS)
    except Exception as exc:
        state['error'] = str(exc)[:200]
        return ([('venue_status_unknown', 'Cannot read status.polymarket.com, so we do NOT know whether the venue is up: %s' % state['error'])], state)
    page = (summary or {}).get('page') or {}
    state['page'] = page.get('status')
    state['maintenances'] = [m.get('name') for m in (summary or {}).get('activeMaintenances') or []]
    state['incidents'] = [i.get('name') for i in (summary or {}).get('activeIncidents') or []]
    raw = components if isinstance(components, list) else (components or {}).get('components')
    by_name = {str(c.get('name') or '').strip(): c.get('status') for c in _walk_components(raw)}
    issues = []
    for name in VENUE_TRADING_COMPONENTS:
        status = by_name.get(name)
        if status is None:
            issues.append(('venue_status_unknown', "status.polymarket.com no longer lists '%s' — the component names this check depends on have changed." % name))
            continue
        if status != VENUE_OK:
            state['degraded'].append({'component': name, 'status': status})
            detail = ''
            if state['maintenances']:
                detail = ' (%s)' % '; '.join(state['maintenances'][:2])
            elif state['incidents']:
                detail = ' (%s)' % '; '.join(state['incidents'][:2])
            issues.append(('venue_maintenance:%s' % name, "Polymarket says '%s' is %s%s — orders may fail or hang, and leader fills stop arriving because THEY cannot trade either. Trading is not stopped by this check." % (name, status, detail)))
    state['ok'] = not state['degraded'] and (not issues)
    return (issues, state)

def configured_lanes(bot_dir):
    try:
        import tomllib
        path = BOT_CONFIG if bot_dir == BOT_DIR else os.path.join(bot_dir, 'deploy', 'copybot2.toml')
        with open(path, 'rb') as f:
            cfg = tomllib.load(f)
        return [l['name'] for l in cfg.get('lane', []) if l.get('enabled', True)]
    except Exception:
        return []

def runtime_wallets(bot_dir):
    try:
        with open(os.path.join(bot_dir, 'run', 'control.json.wallets')) as f:
            v = json.load(f)
        return [w for w in v.get('wallets') or [] if w.get('enabled', True)]
    except (OSError, ValueError):
        return []

def runtime_lane_cfg(spec):
    return {'name': spec.get('name'), 'wallet': spec.get('leader', ''), 'budget': {'bankroll_usd': spec.get('seed_usd', 0.0)}, 'sizing': {}}

def all_lane_cfgs(bot_dir, toml_lanes):
    by_name = {l['name']: l for l in toml_lanes}
    for spec in runtime_wallets(bot_dir):
        if spec.get('name'):
            by_name[spec['name']] = runtime_lane_cfg(spec)
    return by_name

def operator_intent(bot_dir):
    path = os.path.join(bot_dir, 'run', 'control.json.operator')
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            v = json.load(f)
        return {name: bool((state or {}).get('armed')) for name, state in (v.get('lanes') or {}).items()}
    except (OSError, ValueError):
        return None

def activation_warnings(st, pool):
    out = []
    if not isinstance(st, dict) or not isinstance(pool, dict):
        return out
    lanes = pool.get('lanes')
    if not isinstance(lanes, list):
        return out
    resting_lanes = [l.get('name') for l in lanes if isinstance(l, dict) and l.get('enabled') and (not l.get('retired')) and l.get('copy_makers')]
    if not resting_lanes:
        return out
    books = st.get('books_cached')
    if not isinstance(books, (int, float)):
        return out
    if books <= 0:
        out.append('WARN: %d lane(s) are set to rest (%s) but books_cached is 0 — the maker mirror cannot check for room without a live book, so every order is silently crossing. This is exactly how it shipped inert on 2026-08-15.' % (len(resting_lanes), ', '.join((str(x) for x in resting_lanes[:4]))))
    return out
def _foreign_fetch(funder):
    return http_json('%s/trades?user=%s&limit=%d' % (DATA_API, funder, FOREIGN_PAGE_LIMIT))

def reanchor_warnings(pool):
    out = []
    if not isinstance(pool, dict):
        return out
    lanes = pool.get('lanes')
    if not isinstance(lanes, list):
        return out
    blind = []
    for l in lanes:
        if not isinstance(l, dict) or l.get('retired') or (not l.get('enabled')):
            continue
        r = l.get('reanchor')
        if not isinstance(r, dict):
            continue
        if r.get('blind') is True:
            blind.append('%s (last complete: %s)' % (l.get('name'), r.get('last_complete_secs_ago') if r.get('last_complete_secs_ago') is not None else 'never'))
    if blind:
        out.append('WARN: the leader-book re-anchor is BLIND on %d lane(s): %s. It cannot repair a modelled-zero position while in this state, and a modelled zero makes his next routine trim liquidate our ENTIRE position (frac = his_fill/his_before = 1.0). Usually a truncated leader portfolio read — check the position endpoint for those leaders.' % (len(blind), ', '.join(blind[:4])))
    return out

def resting_warnings(st):

    def num(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return 0.0
    out = []
    if not isinstance(st, dict):
        return out
    r = st.get('resting')
    if not isinstance(r, dict):
        return out
    orders = r.get('orders') or []
    if not orders:
        return out
    BLIND_TTL = 1800
    stuck = [o for o in orders if isinstance(o, dict) and num(o.get('age_secs')) > BLIND_TTL and (str(o.get('verdict')) != 'Corroborated')]
    for o in stuck[:5]:
        out.append('WARN: %s %s rest on ...%s is %ds old and %s — past the blind TTL without corroboration, so the sweeper should have pulled it' % (o.get('lane'), o.get('side'), str(o.get('tok'))[-8:], int(num(o.get('age_secs'))), o.get('verdict')))
    if len(stuck) > 5:
        out.append('WARN: ...and %d more uncorroborated rests past the TTL' % (len(stuck) - 5))
    anchored = [o for o in orders if isinstance(o, dict) and o.get('anchored')]
    if orders and (not anchored):
        out.append('WARN: %d resting order(s) and NONE carry a cancel-detector anchor — the corroborated lifetime is inert and every rest is on the blind timer' % len(orders))
    return out

def sizing_fit_warnings(pool):
    out = []
    if not isinstance(pool, dict):
        return out
    for lane in pool.get('lanes') or []:
        fit = lane.get('sizing_fit')
        if not isinstance(fit, dict) or not fit.get('clipped'):
            continue
        if not lane.get('enabled') or lane.get('retired'):
            continue
        out.append("WARN: %s delivers %.0f%% of intent on its leader's largest orders (his max $%.0f needs a $%.0f clip; per-fill cap is $%.0f) — raise seed_usd or lower pct for a full copy" % (lane.get('name'), 100.0 * float(fit.get('delivered_frac_on_his_largest') or 0.0), float(fit.get('his_max_order_usd') or 0.0), float(fit.get('need_usd') or 0.0), float(fit.get('per_fill_cap_usd') or 0.0)))
    return out

def dash(port, path):
    try:
        return http_json('http://127.0.0.1:%d%s' % (port, path))
    except Exception:
        return None

def find_armed():
    intent = operator_intent(BOT_DIR)
    if intent is None:
        return ('UNKNOWN', BOT_DIR, [], None)
    wanted = [n for n, value in intent.items() if value]
    st = dash(BOT_PORT, '/api/status')
    if not st:
        if wanted:
            return ('DEAD', BOT_DIR, wanted, None)
        return (None, None, None, None)
    lanes = st.get('lanes') or {}
    active = [name for name, lane in lanes.items() if isinstance(lane, dict) and lane.get('armed')]
    if active:
        return (BOT_PORT, BOT_DIR, active, st)
    if wanted:
        return ('DEAD', BOT_DIR, wanted, None)
    return (None, None, None, None)
ERROR_LOG_MAX = 400

def error_log_path():
    return os.path.join(BOT_DIR, 'run', 'errors.jsonl')

def humanise(kind, detail):
    d = detail or ''
    if kind == 'drift':
        m = re.search('\\(([+-][\\d.]+)\\)', d)
        gap = abs(float(m.group(1))) if m else None
        tok = re.search('token \\.\\.\\.(\\w+)', d)
        where = ' on market …%s' % tok.group(1) if tok else ''
        if gap is not None and gap < 0.01:
            return 'Our records show %.4f of a share more%s than the wallet holds. That is a rounding remainder worth well under a cent — the reconciler clears these automatically. Nothing to do.' % (gap, where)
        if 'OVERSELL' in d:
            return 'Our records show %s more shares%s than the wallet actually holds. A sell of the full amount would be refused by the venue. No money is at risk, but the position size is wrong.' % ('%.4f' % gap if gap is not None else 'some', where)
        return 'The wallet holds %s shares%s that our records do not track. They will not be sold automatically.' % ('%.4f' % gap if gap is not None else 'some', where)
    if kind == 'trip':
        return 'Trading was stopped automatically. Reason: %s' % d
    return d
_ALLOW_ERROR_LOG = True

def record_error(lane, kind, detail):
    if not _ALLOW_ERROR_LOG:
        return
    try:
        row = {'t': int(time.time()), 'lane': lane, 'kind': kind, 'detail': detail, 'human': humanise(kind, detail), 'severity': 'stop' if kind == 'trip' else 'notice'}
        path = error_log_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + '.lock', 'w') as lockf:
            fcntl.flock(lockf, fcntl.LOCK_EX)
            try:
                with open(path, 'a') as f:
                    f.write(json.dumps(row) + '\n')
                with open(path) as f:
                    lines = f.readlines()
                if len(lines) > ERROR_LOG_MAX * 2:
                    tmp = tmp_path(path)
                    with open(tmp, 'w') as f:
                        f.writelines(lines[-ERROR_LOG_MAX:])
                    os.replace(tmp, path)
            finally:
                fcntl.flock(lockf, fcntl.LOCK_UN)
    except Exception:
        pass
ADVISORY_RESAY_SECS = int(os.environ.get('ADVISORY_RESAY_SECS', '900'))
ADVISORY_STATE_PATH = os.path.join(GUARDIAN_DIR, 'guardian_advisories.json')

def record_advisories(advisory, now):
    codes = {c: m for c, m in advisory or []}
    state = {}
    try:
        with open(ADVISORY_STATE_PATH) as f:
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
        os.makedirs(GUARDIAN_DIR, exist_ok=True)
        tmp = tmp_path(ADVISORY_STATE_PATH)
        with open(tmp, 'w') as f:
            json.dump(open_adv, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, ADVISORY_STATE_PATH)
        fsync_parent(ADVISORY_STATE_PATH)
    except OSError:
        pass
EQUITY_POINT_SECS = int(os.environ.get('EQUITY_POINT_SECS', '60'))
EQUITY_HISTORY_MAX = int(os.environ.get('EQUITY_HISTORY_MAX', '20160'))
EQUITY_HISTORY_PATH = os.path.join(BOT_DIR, 'run', 'equity_history.jsonl')

def record_equity_point(pool, now):
    try:
        if not isinstance(pool, dict):
            return
        w = pool.get('wallet')
        if not isinstance(w, dict) or w.get('equity_pnl') is None:
            return
        try:
            last = os.path.getmtime(EQUITY_HISTORY_PATH)
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
        os.makedirs(os.path.dirname(EQUITY_HISTORY_PATH), exist_ok=True)
        with open(EQUITY_HISTORY_PATH, 'a') as f:
            f.write(json.dumps(row) + '\n')
        if int(now) % 3600 < EQUITY_POINT_SECS:
            with open(EQUITY_HISTORY_PATH) as f:
                lines = f.readlines()
            if len(lines) > EQUITY_HISTORY_MAX * 2:
                tmp = tmp_path(EQUITY_HISTORY_PATH)
                with open(tmp, 'w') as f:
                    f.writelines(lines[-EQUITY_HISTORY_MAX:])
                os.replace(tmp, EQUITY_HISTORY_PATH)
    except Exception:
        pass

def lanes_in(codes, known=None):
    out = set()
    for c in codes:
        c = str(c)
        if ':' in c:
            lane = c.split(':')[1].strip()
            if lane and (not known or lane in known):
                out.add(lane)
    return out

def all_lane_names():
    return sorted(set(configured_lanes(BOT_DIR)) | {w['name'] for w in runtime_wallets(BOT_DIR) if w.get('name')} | set(operator_intent(BOT_DIR) or {}))

def disarm_lanes(names, reason, dry_run):
    return stop_lanes(names, reason, dry_run, buys_only=False)
ENFORCE_HALTS = os.environ.get('GUARDIAN_ENFORCE') == '1'

def halt_buy_lanes(names, reason, dry_run, codes=None):
    return stop_lanes(names, reason, dry_run, buys_only=True, codes=codes)

def stop_lanes(names, reason, dry_run, buys_only=False, codes=None):
    names = sorted(set(names))
    what = 'HALT-BUYS' if buys_only else 'TRIP'
    log('%s (%s): %s' % (what, ', '.join(names) or 'no lane identified', reason))
    for n in names:
        record_error(n, 'trip', reason)
    if not ENFORCE_HALTS:
        log('  (ADVISORY MODE: recorded, NOT halting — set GUARDIAN_ENFORCE=1 to enforce. Drift and over-buying are audited daily by daily_audit.py)')
        for n in names:
            record_error(n, 'advisory:would_have_halted', reason)
        return True
    if dry_run:
        log('  (dry-run — not writing operator files)')
        return True
    op = os.path.join(BOT_DIR, 'run', 'control.json.operator')
    try:
        os.makedirs(os.path.dirname(op), exist_ok=True)
        if not names:
            raise OSError('cannot identify lanes to disarm')
        with open(op + '.lock', 'w') as oplockf:
            fcntl.flock(oplockf, fcntl.LOCK_EX)
            _write_operator_stop(op, names, buys_only, reason, codes)
        log('  %s %s on %s (%d)' % ('halted BUYS for' if buys_only else 'disarmed', ', '.join(names), BOT_DIR, BOT_PORT))
        return True
    except OSError as e:
        log('  ⛔ FAILED to stop %s: %s' % (BOT_DIR, e))
        return False

def _read_state_quietly():
    try:
        with open(STATE_PATH) as f:
            v = json.load(f)
        return v if isinstance(v, dict) else {}
    except (OSError, ValueError):
        return {}

def _write_operator_stop(op, names, buys_only, reason, codes=None):
    corrupt = False
    try:
        with open(op) as f:
            doc = json.load(f)
    except FileNotFoundError:
        doc = {'lanes': {}}
    except (OSError, ValueError):
        doc = {}
        corrupt = True
    if not isinstance(doc, dict) or not isinstance(doc.get('lanes'), dict):
        corrupt = True
    if corrupt:
        seen = (_read_state_quietly() or {}).get('operator_seen') or {}
        lanes_doc = {}
        for n in all_lane_names():
            prev = seen.get(n) if isinstance(seen.get(n), dict) else None
            lanes_doc[n] = {'armed': bool(prev.get('armed')) if prev else True, 'halt_buys': True}
        doc = {'lanes': lanes_doc}
        recovered = sum((1 for n in lanes_doc if isinstance(seen.get(n), dict)))
        log('  ⛔ operator file unreadable — REBUILT it as buys-halted for every known lane, recovering `armed` for %d of %d from the last observed intent.' % (recovered, len(lanes_doc)))
        record_error('', 'trip', "operator file was unreadable and has been rebuilt as buys-halted; per-lane `armed` was recovered for %d of %d lanes from the guardian's last observation, and defaulted to armed (exits live) for the rest" % (recovered, len(lanes_doc)))
    for n in names:
        entry = doc['lanes'].get(n)
        base = entry if isinstance(entry, dict) else {}
        doc['lanes'][n] = {**base, 'halt_buys': True} if buys_only else {**base, 'armed': False}
    doc['_meta'] = operator_meta('guardian', reason, codes, lanes=names)
    tmp = tmp_path(op + '.guardian')
    with open(tmp, 'w') as f:
        json.dump(doc, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, op)
    fsync_parent(op)

def resume_buy_lanes(names, why, dry_run):
    names = sorted(set(names))
    if not names:
        return False
    if dry_run:
        log('  (dry-run — would RESUME buys on %s)' % ', '.join(names))
        return True
    op = os.path.join(BOT_DIR, 'run', 'control.json.operator')
    reason = 'automatic resume: %s' % why
    try:
        with open(op + '.lock', 'w') as lockf:
            fcntl.flock(lockf, fcntl.LOCK_EX)
            try:
                with open(op) as f:
                    doc = json.load(f)
            except (OSError, ValueError):
                log('  resume aborted: operator file unreadable')
                return False
            if not isinstance(doc, dict) or not isinstance(doc.get('lanes'), dict):
                log('  resume aborted: operator file has no lane map')
                return False
            touched = []
            for n in names:
                lane = doc['lanes'].get(n)
                if not isinstance(lane, dict) or not lane.get('halt_buys'):
                    continue
                lane['halt_buys'] = False
                touched.append(n)
            if not touched:
                return False
            doc['_meta'] = operator_meta('guardian', reason)
            tmp = tmp_path(op + '.guardian')
            with open(tmp, 'w') as f:
                json.dump(doc, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, op)
            fsync_parent(op)
        for n in touched:
            record_error(n, 'resume', reason)
        return True
    except OSError as e:
        log('  resume failed: %s' % e)
        return False

def disarm_all(reason, dry_run):
    return halt_buy_lanes(all_lane_names(), reason, dry_run)

def confirmation_step(previous, codes, now):
    codes = sorted(set((str(c) for c in codes if c)))
    if not codes:
        return ([], None)
    prev = previous.get('by_code') if isinstance(previous, dict) else None
    if not isinstance(prev, dict):
        prev = {}
    by_code, confirmed = ({}, [])
    for c in codes:
        p = prev.get(c)
        p = p if isinstance(p, dict) else None
        if p is not None:
            first_seen = float(p.get('first_seen') or now)
            last_seen = float(p.get('last_seen') or first_seen)
            if now - last_seen < CONFIRM_MIN_INTERVAL_SECS:
                by_code[c] = p
                if int(p.get('checks') or 0) >= CONFIRM_REQUIRED_CHECKS:
                    confirmed.append(c)
                continue
            checks = int(p.get('checks') or 0) + 1
        else:
            first_seen, checks = (now, 1)
        by_code[c] = {'checks': checks, 'first_seen': first_seen, 'last_seen': now}
        if checks >= CONFIRM_REQUIRED_CHECKS:
            confirmed.append(c)
    return (confirmed, {'by_code': by_code})

def _read_confirmation():
    try:
        with open(CONFIRM_PATH) as f:
            value = json.load(f)
        return value if isinstance(value, dict) else {}
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        raise OSError('cannot read guardian confirmation state: %s' % e)

def _write_confirmation(value):
    os.makedirs(os.path.dirname(CONFIRM_PATH), exist_ok=True)
    tmp = tmp_path(CONFIRM_PATH)
    with open(tmp, 'w') as f:
        json.dump(value or {}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CONFIRM_PATH)
    fsync_parent(CONFIRM_PATH)

def clear_confirmation():
    _write_confirmation({})

def operator_meta(by, why, codes=None, lanes=None):
    meta = {'by': by, 'at': int(time.time()), 'at_iso': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'why': str(why)[:200], 'pid': os.getpid()}
    if codes:
        meta['codes'] = sorted({str(c) for c in codes})[:12]
    if lanes is not None:
        meta['lanes'] = sorted({str(x) for x in lanes if x})[:32]
    return meta

def audit_operator_change(state, doc, now):
    findings = []
    try:
        lanes = (doc or {}).get('lanes') or {}
        seen = {n: {'armed': bool(v.get('armed')), 'halt_buys': bool(v.get('halt_buys'))} for n, v in lanes.items() if isinstance(v, dict)}
        meta = (doc or {}).get('_meta') or {}
        prev = (state or {}).get('operator_seen') or {}
        prev_by = (state or {}).get('operator_by')
        if prev and seen != prev:
            for name in sorted(set(prev) | set(seen)):
                a, b = (prev.get(name), seen.get(name))
                if a == b:
                    continue
                if a is None:
                    findings.append('lane %s APPEARED in the operator file as %s' % (name, b))
                elif b is None:
                    findings.append('lane %s VANISHED from the operator file (was %s)' % (name, a))
                elif a.get('halt_buys') and (not b.get('halt_buys')):
                    findings.append('⛔ lane %s: a BUY HALT WAS CLEARED (%s -> %s) by %s' % (name, a, b, meta.get('by') or 'an UNIDENTIFIED writer'))
                else:
                    findings.append('lane %s: %s -> %s' % (name, a, b))
            if findings:
                stamped_at = float(meta.get('at') or 0)
                last_seen = float((state or {}).get('operator_seen_at') or 0)
                fresh = stamped_at >= last_seen - 1
                findings.append('written by %s at %s%s' % (meta.get('by') if meta.get('by') and fresh else 'AN UNIDENTIFIED WRITER (%s)' % ("stale stamp claiming '%s'" % meta.get('by') if meta.get('by') else 'no _meta stamp'), meta.get('at_iso') or (time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(float(meta['at']))) if meta.get('at') else 'an unrecorded time') if fresh else 'an unrecorded time', ' — %s' % meta['why'] if meta.get('why') and fresh else ''))
        state['operator_seen_at'] = now
        state['operator_seen'] = seen
        state['operator_by'] = meta.get('by') or prev_by
    except Exception as e:
        findings.append('operator-file audit failed: %r' % e)
    return findings

def confirm_or_disarm(issues, dry_run, now=None):
    now = time.time() if now is None else now
    codes = [code for code, _ in issues]
    reason = '; '.join((message for _, message in issues))
    try:
        confirmed, state = confirmation_step(_read_confirmation(), codes, now)
        _write_confirmation(state)
    except OSError as e:
        disarm_all('guardian confirmation persistence failed: %s; original fault: %s' % (e, reason), dry_run)
        return True
    if confirmed:
        confirmed_set = set(confirmed)
        named = lanes_in(confirmed, set(all_lane_names()))
        attributable = named and all((':' in str(c) for c in confirmed))
        targets = sorted(named) if attributable else all_lane_names()
        why = '; '.join((m for c, m in issues if c in confirmed_set)) or reason
        checks = max((int(v.get('checks') or 0) for k, v in (state or {}).get('by_code', {}).items() if k in confirmed_set), default=CONFIRM_REQUIRED_CHECKS)
        halted_ok = halt_buy_lanes(targets, 'CONFIRMED on %d consecutive checks: %s' % (checks, why), dry_run, codes=sorted(confirmed_set))
        if not halted_ok:
            log('⛔ CRITICAL: the confirmed halt could NOT be written — the lanes are still buying. Retrying next pass; fix the operator file NOW.')
            record_error('', 'halt_failed', 'confirmed halt could not be written for: %s' % ', '.join(targets))
        return bool(halted_ok)
    progress = max((int(v.get('checks') or 0) for v in (state or {}).get('by_code', {}).values()), default=1)
    log('PENDING CONFIRMATION %d/%d (will re-check in %ds): %s' % (progress, CONFIRM_REQUIRED_CHECKS, GUARDIAN_PERIOD_SECS, reason))
    return False
ADVISORY_ISSUE_CODES = ('cost_drift', 'matchup', 'watcher_feed_brief', 'watcher_feed', 'venue_maintenance', 'venue_status_unknown')

def is_advisory(code):
    return str(code).split(':', 1)[0] in ADVISORY_ISSUE_CODES

def split_by_severity(issues):
    halting = [i for i in issues if not is_advisory(i[0])]
    advisory = [i for i in issues if is_advisory(i[0])]
    return (halting, advisory)

def ledger_activity(path, now, quiet_secs=None, settle_window_secs=48 * 3600):
    quiet = SETTLE_QUIET_SECS if quiet_secs is None else quiet_secs
    recent, settled = (set(), set())
    holdings = collections.defaultdict(float)
    try:
        with open(path, errors='ignore') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                tok = str(r.get('token') or '')
                if not tok:
                    continue
                t = float(r.get('t') or 0)
                ev = r.get('ev')
                if ev == 'fill':
                    holdings[r.get('lane'), tok] += float(r.get('shares') or 0) * (1 if r.get('side') == 0 else -1)
                elif ev == 'settle':
                    holdings[r.get('lane'), tok] = 0.0
                    if now - t <= settle_window_secs:
                        settled.add(tok)
                if t and now - t <= quiet:
                    recent.add(tok)
    except OSError as e:
        log('WARN: cannot read the accounting ledger (%s) — drift/unattributed checks run without ledger-derived exemptions this pass' % e)
        return (set(), set(), set())
    all_held = {tok for (_lane, tok), sh in holdings.items() if sh > 1e-09}
    return (recent, settled, all_held)
POSITIONS_PAGE = 500
POSITIONS_MAX_PAGES = 50

def fetch_all_positions(user, page=POSITIONS_PAGE, max_pages=POSITIONS_MAX_PAGES):
    chain, pages = ({}, [])
    for i in range(max_pages):
        url = '%s/positions?user=%s&sizeThreshold=0&limit=%d&offset=%d' % (DATA_API, user, page, i * page)
        try:
            rows = http_json(url, headers={'User-Agent': 'guardian'})
        except Exception as e:
            return (chain, False, 'page %d failed: %s' % (i + 1, e))
        if not isinstance(rows, list):
            return (chain, False, 'page %d was not a list' % (i + 1))
        for p in rows:
            try:
                chain[str(p['asset'])] = p
            except (KeyError, TypeError):
                continue
        pages.append(len(rows))
        if len(rows) < page:
            return (chain, True, 'complete')
    return (chain, False, 'truncated at the %d-page budget' % max_pages)
REFUTED_QUIET_SECS = 3600

def refutation_is_new(lane, reason, now, path=None):
    path = path or os.path.join(GUARDIAN_DIR, 'refuted_seen.json')
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
PREFLIGHT_EVERY_SECS = 900

def preflight_records(bot_dir, port, secs=6 * 3600):
    cache = {}

    def fetch(source):
        if source in cache:
            return cache[source]
        out = None
        try:
            if source.startswith('events:'):
                want = source.split(':', 1)[1]
                now = time.time()
                rows = []
                paths = sorted(glob.glob(os.path.join(bot_dir, 'data', 'events-*.jsonl')))
                for path in paths[-3:]:
                    with open(path) as f:
                        for line in f:
                            if '"%s"' % want not in line:
                                continue
                            try:
                                e = json.loads(line)
                            except ValueError:
                                continue
                            if e.get('ev') != want:
                                continue
                            if now - (e.get('t') or 0) / 1000.0 <= secs:
                                rows.append(e)
                out = rows
            elif source.startswith('api:'):
                _, path, key = source.split(':', 2)
                doc = dash(port, path)
                if isinstance(doc, dict):
                    got = doc.get(key)
                    if isinstance(got, list):
                        out = got
                    elif isinstance(got, dict):
                        out = [v for v in got.values() if isinstance(v, dict)]
                    else:
                        out = []
        except Exception:
            out = None
        cache[source] = out
        return out
    return fetch

def preflight_value_sets(bot_dir):
    sets = {}
    try:
        with open(WATCHER_HEARTBEAT) as f:
            hb = json.load(f)
        sets['watcher_fill_lanes'] = set(hb['fill_lanes']) if isinstance(hb.get('fill_lanes'), list) else None
    except (OSError, ValueError, KeyError, TypeError):
        sets['watcher_fill_lanes'] = None
    try:
        with open(os.path.join(bot_dir, 'run', 'control.json.operator')) as f:
            doc = json.load(f)
        sets['operator_lane_names'] = {str(n) for n in doc.get('lanes') or {}}
    except (OSError, ValueError, TypeError):
        sets['operator_lane_names'] = None
    return sets
SCORECARD_PATH = os.path.join(GUARDIAN_DIR, 'halt_cases.jsonl')
_LAST_CORROBORATION = None

def buys_declined_since(bot_dir, since_t, now=None):
    now = time.time() if now is None else now
    n = 0
    try:
        for path in sorted(glob.glob(os.path.join(bot_dir, 'data', 'events-*.jsonl')))[-3:]:
            with open(path) as f:
                for line in f:
                    if '"skip"' not in line or 'Halted' not in line:
                        continue
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e.get('ev') != 'skip':
                        continue
                    t = (e.get('t') or 0) / 1000.0
                    if since_t <= t <= now and str(e.get('why') or '').startswith('Halted') and (str(e.get('side') or '').upper() == 'BUY'):
                        n += 1
    except OSError:
        return None
    return n
LAG_DEFER_WINDOW_SECS = 120

def defer_for_lag(lane, token, now, path=None):
    path = path or os.path.join(GUARDIAN_DIR, 'lag_deferred.json')
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
DIGEST_EVERY_SECS = 7 * 86400
DIGEST_RETRY_SECS = 3600

def digest_due(state, now):
    last = state.get('digest_t')
    if not isinstance(last, (int, float)):
        return True
    return now - last >= DIGEST_EVERY_SECS

def digest_body(lanes, score, preflight_summary, feed, now):
    armed = sum((1 for l in (lanes or {}).values() if l.get('armed')))
    halted = sum((1 for l in (lanes or {}).values() if l.get('halted')))
    p = '%.0f%%' % score['precision_pct'] if score.get('precision_pct') is not None else 'n/a (nothing judged yet)'
    return 'Routine weekly check. Nothing here needs action — it exists so you know this\nchannel still works, and so halt precision is a number you see on a calm day.\n\nLANES        %d armed, %d halted\nHALTS        %s\n  judged     %d true, %d false, %d unknown (last 7 days)\n  cost       %d leader buy(s) declined by halts later judged false\nALARMS       %s\nFEED         %s reconnect(s) since the watcher started\n\nA halt scored FALSE means the fault was gone on re-check, nothing independent\never confirmed it, and it cost us copies. That number is the one to drive down.\n\n%s\n' % (armed, halted, p, score.get('true', 0), score.get('false', 0), score.get('unknown', 0), score.get('buys_lost_to_false_halts', 0), preflight_summary, (feed or {}).get('reconnects', 'unknown'), notify.local_stamp(now))

def persist_state(state):
    try:
        os.makedirs(GUARDIAN_DIR, exist_ok=True)
        tmp = tmp_path(STATE_PATH)
        with open(tmp, 'w') as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_PATH)
        fsync_parent(STATE_PATH)
        return True
    except OSError as e:
        log('could not persist guardian state: %s' % e)
        return False

def leader_book_fetcher(cache):

    def fetch(addr):
        key = str(addr or '').lower()
        if not key:
            return None
        if key not in cache:
            try:
                book, complete, _why = fetch_all_positions(key)
                cache[key] = venue_shares(book) if complete else None
            except Exception:
                cache[key] = None
        return cache[key]
    return fetch

def market_resolved(condition, timeout=8):
    return resolution.is_resolved(condition)

def token_conditions(bot_dir, tokens):
    want = {str(t) for t in tokens}
    out = {}
    try:
        for path in sorted(glob.glob(os.path.join(bot_dir, 'data', 'events-*.jsonl')))[-3:]:
            with open(path) as f:
                for line in f:
                    if '"condition"' not in line:
                        continue
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    tok = str(e.get('tok') or '')
                    if tok in want and e.get('condition'):
                        out[tok] = str(e['condition'])
    except OSError:
        return {}
    return out

CHAIN_BLIND_WARN_RUNS = 20

HALT_REMIND_SECS = [900, 3600, 7200, 14400, 28800]
HALT_REMIND_EVERY = 28800

def halt_notifications(lanes, meta, state, now):
    st = dict(state or {})
    halted = sorted((n for n, l in (lanes or {}).items() if l.get('halted')))
    prev = sorted(st.get('notify_halted') or [])
    msgs = []
    why = str((meta or {}).get('why') or 'no reason recorded')[:300]
    st['notify_halted'] = halted
    if halted and (not prev):
        st['notify_halted_since'] = now
        st['notify_sent'] = 0
        msgs.append(('Abomination81 Copybot: TRADING HALTED (%d lane%s)' % (len(halted), '' if len(halted) == 1 else 's'), 'Buying has stopped on: %s\nStopped at: %s\n%s\n\nReason recorded by the halting process:\n  %s\n\nExits are UNAFFECTED - the bot still follows each leader out of positions it\nholds. Only new buys are stopped.\n\nA halt does not clear itself unless its cause was a loss of visibility.\n' % (', '.join(halted), notify.local_stamp(now), scorecard.summary_line(scorecard.summarise(SCORECARD_PATH, now)), why)))
        return (msgs, st)
    if halted and prev:
        added = sorted(set(halted) - set(prev))
        if added:
            msgs.append(('Abomination81 Copybot: %d MORE lane(s) halted' % len(added), 'Newly stopped: %s\nAlready stopped: %s\n\nReason:\n  %s\n' % (', '.join(added), ', '.join(prev), why)))
        since = st.get('notify_halted_since')
        since = float(since) if since is not None else now
        down = now - since
        sent = int(st.get('notify_sent') or 0)
        if sent < len(HALT_REMIND_SECS):
            due = HALT_REMIND_SECS[sent]
        else:
            due = HALT_REMIND_SECS[-1] + HALT_REMIND_EVERY * (sent - len(HALT_REMIND_SECS) + 1)
        if down >= due:
            st['notify_sent'] = sent + 1
            msgs.append(('Abomination81 Copybot: STILL HALTED - %s' % human_secs(down), 'Buying has been stopped for %s on: %s\nStopped at: %s\n\nNothing has resumed it. Original reason:\n  %s\n\nEvery leader BUY during this window has been declined.\n' % (human_secs(down), ', '.join(halted), notify.local_stamp(since), why)))
        return (msgs, st)
    if prev and (not halted):
        _since = st.get('notify_halted_since')
        down = now - (float(_since) if _since is not None else now)
        msgs.append(('Abomination81 Copybot: trading resumed', 'Buying is enabled again on all lanes.\n\nIt was stopped for %s.\n' % human_secs(down)))
        st.pop('notify_halted_since', None)
        st.pop('notify_sent', None)
    return (msgs, st)

def process_watcher_criticals(state, dry_run):
    immediate = []
    prev_offset = state.get('watcher_offset')
    new_lines, new_offset = read_new_lines(WATCHER_LOG, prev_offset)
    prev_seq = state.get('watcher_alert_seq')
    alert_lines, new_seq = read_new_alerts(WATCHER_ALERTS, prev_seq, log)
    _ok, r = check_watcher_criticals(new_lines + alert_lines)
    watcher_reasons = list(r)
    halted_ok = True
    if watcher_reasons:
        scope = critical_scope(watcher_reasons, all_lane_names())
        halted_ok = halt_buy_lanes(scope, '; '.join(watcher_reasons), dry_run)
    if halted_ok:
        state['watcher_offset'] = new_offset
        state['watcher_alert_seq'] = new_seq
    else:
        state['watcher_offset'] = prev_offset
        state['watcher_alert_seq'] = prev_seq
        immediate.append('watcher CRITICAL could not be acted on (the operator file could not be written) — the alert is DELIBERATELY UNCONSUMED so the next run retries it')
    return (immediate, bool(watcher_reasons))

def check_buywatch_alerts(state, now, log=None):
    log = log or globals()['log']
    last_t = int(state.get('buywatch_alert_t') or 0)
    try:
        with open(BUYWATCH_ALERTS) as f:
            raw = f.readlines()
    except FileNotFoundError:
        return []
    except OSError as e:
        return ['buywatch alert sidecar unavailable: %s' % e]
    fresh, hi = ([], last_t)
    for line in raw:
        try:
            r = json.loads(line)
        except ValueError:
            continue
        t = int(r.get('t') or 0)
        if t <= last_t:
            continue
        hi = max(hi, t)
        fresh.append(r)
    if not fresh:
        return []
    msgs = []
    for r in fresh[-5:]:
        msgs.append('buywatch UNSEEN: %d leader buy(s) reached NO decision (lanes: %s) — the decode may be silent; check the feed and leader addresses' % (int(r.get('count') or 0), ','.join(r.get('lanes') or [])))
    for m in msgs:
        log('WARN: ' + m)
    if notify.configured():
        try:
            notify.send('Abomination81 Copybot: buywatch UNSEEN leader buys', '\n'.join(msgs) + '\n\n(Nothing was halted — buywatch reports, a human decides.)\n' + notify.local_stamp(now) + '\n', log=log)
        except Exception as e:
            log('WARN: buywatch alert email failed: %r' % (e,))
    state['buywatch_alert_t'] = hi
    return msgs

def baseline_watcher_log():
    state = {}
    try:
        with open(STATE_PATH) as f:
            state = json.load(f)
    except (OSError, ValueError):
        pass
    try:
        size = os.path.getsize(WATCHER_LOG)
        state['watcher_offset'] = size
        os.makedirs(GUARDIAN_DIR, exist_ok=True)
        tmp = tmp_path(STATE_PATH)
        with open(tmp, 'w') as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_PATH)
        fsync_parent(STATE_PATH)
        return None
    except OSError as e:
        return 'cannot baseline watcher log while disarmed: %s' % e

def gather_and_check(dry_run):
    port, bot_dir, active_lanes, st = find_armed()
    if port == 'UNKNOWN':
        issue = ('operator_unreadable', 'operator file exists but is unreadable — arm state unknowable')
        return 2 if confirm_or_disarm([issue], dry_run) else 0
    if port == 'DEAD':
        issue = ('dead_runtime', 'operator intent is armed but its bot/dashboard is DEAD (%s)' % ','.join(active_lanes or []))
        return 2 if confirm_or_disarm([issue], dry_run) else 0
    if port is None:
        try:
            clear_confirmation()
        except OSError as e:
            log('WARN: neither bot armed, but cannot clear confirmation state: %s' % e)
        error = baseline_watcher_log()
        if error:
            log('WARN: neither bot armed, but %s' % error)
            return 1
        log('PASS: neither bot armed — watcher history baselined; nothing to guard')
        return 0
    try:
        chain, chain_complete, chain_why = fetch_all_positions(OUR)
        if not chain_complete:
            skipped = 'ledger persistence, re-anchor warnings, split-brain detection'
            state_blind = {}
            try:
                with open(STATE_PATH) as _f:
                    state_blind = json.load(_f)
            except (OSError, ValueError):
                pass
            runs = int(state_blind.get('chain_blind_runs') or 0) + 1
            state_blind['chain_blind_runs'] = runs
            try:
                _imm, _halted = process_watcher_criticals(state_blind, dry_run)
                for _r in _imm:
                    log('⛔ %s' % _r)
            except Exception as _e:
                log('WARN: watcher CRITICAL processing failed on the blind path: %s' % _e)
            try:
                os.makedirs(GUARDIAN_DIR, exist_ok=True)
                _tmp = tmp_path(STATE_PATH)
                with open(_tmp, 'w') as _f:
                    json.dump(state_blind, _f)
                    _f.flush()
                    os.fsync(_f.fileno())
                os.replace(_tmp, STATE_PATH)
            except OSError:
                pass
            log('⛔ DEGRADED: chain positions incomplete (%s; %d row(s) read) — no verdict this run, and the chain-derived checks did NOT run: %s [%d consecutive]. Watcher CRITICALs WERE processed.' % (chain_why, len(chain), skipped, runs))
            if runs >= CHAIN_BLIND_WARN_RUNS:
                log('⛔ WARN: the chain view has been incomplete for %d consecutive runs — the guardian has not been able to act on a watcher MISS, a persistence fault, or a second writer for that entire period. This is a blind guardian, not a healthy one.' % runs)
            return 0
    except Exception as e:
        issue = ('chain_unavailable', 'cannot read chain positions: %s' % e)
        return 2 if confirm_or_disarm([issue], dry_run) else 0
    try:
        _sb = {}
        try:
            with open(STATE_PATH) as _f:
                _sb = json.load(_f)
        except (OSError, ValueError):
            _sb = {}
        if int(_sb.get('chain_blind_runs') or 0):
            _sb['chain_blind_runs'] = 0
            os.makedirs(GUARDIAN_DIR, exist_ok=True)
            _t = tmp_path(STATE_PATH)
            with open(_t, 'w') as _f:
                json.dump(_sb, _f)
                _f.flush()
                os.fsync(_f.fileno())
            os.replace(_t, STATE_PATH)
            log('chain view recovered — blindness counter cleared')
    except OSError:
        pass
    corr_ctx = corroborate.Ctx(our_positions=lambda: venue_shares(chain), leader_positions=leader_book_fetcher({}))
    lane_leader = {}
    try:
        _pool = dash(port, '/api/pool')
        for _l in (_pool or {}).get('lanes') or []:
            if isinstance(_l, dict) and _l.get('name') and _l.get('leader'):
                lane_leader[str(_l['name'])] = str(_l['leader'])
    except Exception:
        lane_leader = {}
    if not lane_leader:
        log('NOTE: no lane->leader map available; exposure claims cannot be independently checked this run and will halt on their own evidence, as before')
    confirm_issues = []
    immediate_reasons = []
    immediate_lane_reasons = []
    guard_status = st.get('signal_guard') if isinstance(st, dict) else None
    if not isinstance(guard_status, dict) or not guard_status.get('enabled') or (not guard_status.get('healthy')):
        immediate_reasons.append('durable signal guard is absent or unhealthy: %r' % guard_status)
    try:
        import tomllib
        with open(BOT_CONFIG, 'rb') as f:
            cfg = tomllib.load(f)
        cfg_by_name = all_lane_cfgs(bot_dir, cfg['lane'])
    except Exception as e:
        issue = ('config_unavailable', "cannot read armed bot's own config: %s" % e)
        return 2 if confirm_or_disarm([issue], dry_run) else 0
    now = time.time()
    fires_by_lane = collections.defaultdict(list)
    try:
        for e in recent_fires(bot_dir, now - WINDOW_SECS):
            usd = float(e.get('shares') or 0) * float(e.get('limit') or 0)
            fires_by_lane[str(e.get('lane', ''))].append({'tok': str(e.get('tok', '')), 'ts': event_ts(e), 'condition': str(e.get('condition', '')), 'side': str(e.get('side', '')), 'usd': usd, 'his_order': e.get('his_order'), 'shares': e.get('shares'), 'his_fill': e.get('his_fill')})
    except OSError as e:
        confirm_issues.append(('event_ledger', 'cannot read event ledger: %s' % e))
    all_backend_tokens = set()
    lane_holdings = {}
    for lane_name in active_lanes:
        lane = (st.get('lanes') or {}).get(lane_name)
        if isinstance(lane, dict):
            all_backend_tokens |= set(lane.get('holdings') or {})
            lane_holdings[lane_name] = lane.get('holdings') or {}
    for br in conservation_breaches(lane_holdings, chain):
        confirm_issues.append(('conservation:%s' % br['token'], 'DUPLICATE OWNERSHIP %s…: %s lanes claim %.4f shares but the wallet holds %.4f (excess %.4f) — %s' % (br['token'][:12], len(br['claimants']), br['claimed'], br['physical'], br['excess'], ', '.join(('%s=%.4f' % (l, sh) for l, sh in br['claimants'])))))
    quarantined = {str(r['token']): r for r in st.get('unattributed') or [] if isinstance(r, dict) and r.get('token')}
    quarantined_tokens = set(quarantined)
    led_recent, led_settled, led_all_held = ledger_activity(os.path.join(BOT_DIR, 'data', 'ledger.jsonl'), now)
    all_backend_tokens |= led_all_held
    try:
        awaiting = led_settled & set(chain) - all_backend_tokens
        for tok in sorted(awaiting):
            log("WARN: ...%s is settled in our books and awaiting Polymarket's on-chain redemption ($%.2f) — known state, not a fault" % (tok[-10:], _num(chain[tok], 'currentValue') or 0.0))
        _, reasons, tolerated_orphans = check_unattributed(chain, all_backend_tokens, quarantined_tokens, set(UNATTRIBUTED_ALLOW) | awaiting)
        confirm_issues += [('unattributed::%s' % hashlib.sha1(x.encode('utf-8')).hexdigest()[:8], x) for x in reasons]
        if tolerated_orphans:
            log('WARN: ignored bounded unattributed dust on token(s): %s' % ', '.join((t[-10:] for t in tolerated_orphans)))
        for tok in sorted(quarantined_tokens & set(chain)):
            row = quarantined[tok]
            log('WARN: %s holds $%.2f of …%s that nothing proves is ours — left unattributed, so the bot will NOT sell it; attribute it or flatten it by hand' % (row.get('lane'), float(row.get('usd') or 0.0), tok[-10:]))
    except Exception as e:
        confirm_issues.append(('unattributed_check', 'cannot check for unattributed inventory: %s' % e))
    lane_summaries = []
    for lane_name in active_lanes:
        lane = (st.get('lanes') or {}).get(lane_name)
        if not isinstance(lane, dict):
            confirm_issues.append(('lane_status:%s' % lane_name, 'armed lane %s absent from status' % lane_name))
            continue
        m = dash(port, '/api/matchup?lane=%s' % lane_name)
        if not m:
            confirm_issues.append(('matchup:%s' % lane_name, 'matchup unavailable for armed lane %s' % lane_name))
            continue
        held = lane.get('holdings') or {}
        chain_lane = net_chain_for_lane(chain, {n: l.get('holdings') or {} for n, l in (st.get('lanes') or {}).items() if isinstance(l, dict)}, lane_name)
        backend_open_usd = float(lane.get('open_usd') or 0.0)
        frontend_deployed = (m.get('wallet') or {}).get('deployed')
        chain_cost = sum((float(chain[t]['size']) * float(chain[t]['avgPrice']) for t in held if t in chain))
        settling = {f['tok'] for f in fires_by_lane.get(lane_name, []) if f.get('ts') and now - f['ts'] < SETTLE_QUIET_SECS} | led_recent
        if settling:
            log('INFO: %s skipping drift on %d token(s) traded in the last %ds (still settling)' % (lane_name, len(settling), SETTLE_QUIET_SECS))
        resolved = set()
        try:
            suspect = [t for t, sh in (held or {}).items() if float(sh) - float((chain_lane.get(t) or {}).get('size', 0.0) or 0.0) > SHARE_API_EPSILON and (not any((str(t).startswith(p) for p in settling if p)))]
            if suspect:
                conds = token_conditions(BOT_DIR, suspect)
                for tok in suspect[:8]:
                    if market_resolved(conds.get(tok)) is True:
                        resolved.add(str(tok))
                if resolved:
                    log('INFO: %s %d drifting token(s) belong to RESOLVED markets — settlement, not drift' % (lane_name, len(resolved)))
        except Exception as e:
            log('INFO: %s could not check market resolution (%r) — treating every gap as drift, as before' % (lane_name, e))
            resolved = set()
        _, reasons, tolerated_dust, noticed = check_share_drift(held, chain_lane, settling=settling, resolved=resolved)

        def _drift_code(msg):
            m = re.search('token \\.\\.\\.(\\w+)', msg)
            return 'share_drift:%s:%s' % (lane_name, m.group(1)) if m else 'share_drift:%s' % lane_name
        confirm_issues += [(_drift_code(x), '%s: %s' % (lane_name, x)) for x in reasons]
        if tolerated_dust:
            log('WARN: %s tolerated bounded dust: %s' % (lane_name, '; '.join(tolerated_dust)))
        for x in noticed:
            record_error(lane_name, 'drift', x)
            log('NOTICE: %s drift below the %.2f sh trip bound: %s' % (lane_name, DRIFT_TRIP_SHARES, x))
        _, reasons = check_three_way(chain_cost, backend_open_usd, frontend_deployed, FRONTEND_LEDGER_TOLERANCE_USD)
        confirm_issues += [('cost_drift:%s' % lane_name, '%s: %s' % (lane_name, x)) for x in reasons]
        try:
            lane_cfg = cfg_by_name[lane_name]
            max_usd_per_fill, _, per_market_usd = lane_caps(lane_cfg)
        except Exception as e:
            confirm_issues.append(('caps:%s' % lane_name, '%s: cannot derive caps: %s' % (lane_name, e)))
            continue
        lane_fires = fires_by_lane[lane_name]
        try:
            scale = float(lane.get('cap_scale') or 1.0) if isinstance(lane, dict) else 1.0
        except (TypeError, ValueError):
            scale = 1.0
        try:
            lane_pct = float(lane.get('pct') or 0.0) if isinstance(lane, dict) else 0.0
        except (TypeError, ValueError):
            lane_pct = 0.0
        fire_facts = []
        _, reasons = check_fire_patterns(lane_fires, max_usd_per_fill, per_market_usd, scale, lane_pct, facts_out=fire_facts)
        for reason in reasons:
            f = next((x for x in fire_facts if x.get('reason') == reason), None)
            verdict, evidence = (corroborate.CONFIRMED, '')
            if f is not None:
                f = dict(f, leader=lane_leader.get(lane_name))
                verdict, evidence = corroborate.check(f['family'], f, corr_ctx)
            if not corroborate.halts(verdict):
                log('REFUTED (not halting) %s: %s || independent check: %s' % (lane_name, reason[:90], evidence))
                if refutation_is_new(lane_name, reason, now):
                    record_error(lane_name, 'refuted', '%s || %s' % (reason, evidence))
                continue
            if f is not None:
                global _LAST_CORROBORATION
                _LAST_CORROBORATION = verdict
            if f is not None and verdict == corroborate.LAGGING and defer_for_lag(lane_name, f.get('token'), now):
                log('DEFERRED one cycle %s: %s || %s' % (lane_name, reason[:80], evidence))
                continue
            if f is not None and verdict != corroborate.CONFIRMED:
                log('  (halting anyway — %s)' % evidence)
            immediate_lane_reasons.append((lane_name, '%s: %s' % (lane_name, reason)))
        lane_summaries.append((lane_name, len(held), backend_open_usd, len(lane_fires), float(lane.get('spent_today') or 0.0)))
    pool = dash(port, '/api/pool')
    record_equity_point(pool, now)
    if isinstance(pool, dict) and pool.get('headroom') is not None:
        head = float(pool['headroom'])
        if head < 0:
            log('WARN: virtual budgets $%.2f exceed the shared wallet $%s by $%.2f' % (float(pool.get('sum_seed') or 0.0), pool.get('physical_equity'), -head))
    for warn in sizing_fit_warnings(pool):
        log(warn)
    for warn in resting_warnings(st):
        log(warn)
    for warn in activation_warnings(st, pool):
        log(warn)
    for warn in reanchor_warnings(pool):
        log(warn)
    persistence = st.get('ledger_persistence') or {}
    if persistence.get('ok') is False:
        immediate_reasons.append('ledger persistence fault: %s' % persistence.get('last_error'))
    state = {}
    try:
        with open(STATE_PATH) as f:
            state = json.load(f)
    except (OSError, ValueError):
        pass
    state[str(port)] = {'t': now, 'lanes': {name: {'spent_today': spent} for name, _, _, _, spent in lane_summaries}}
    try:
        with open(os.path.join(BOT_DIR, 'run', 'control.json.operator')) as _f:
            _opdoc = json.load(_f)
    except Exception:
        _opdoc = None
    for finding in audit_operator_change(state, _opdoc, now):
        log('OPERATOR-FILE: %s' % finding)
        if 'BUY HALT WAS CLEARED' in finding:
            record_error('', 'operator', finding)
    _imm, watcher_halted_buys = process_watcher_criticals(state, dry_run)
    immediate_reasons.extend(_imm)
    confirm_issues.extend(watcher_health_issues(now, WATCHER_HEARTBEAT))
    try:
        os.makedirs(GUARDIAN_DIR, exist_ok=True)
        tmp = tmp_path(STATE_PATH)
        with open(tmp, 'w') as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_PATH)
        fsync_parent(STATE_PATH)
    except OSError as e:
        immediate_reasons.append('cannot persist guardian state: %s' % e)
    if immediate_reasons or immediate_lane_reasons:
        try:
            clear_confirmation()
        except OSError:
            pass
        if immediate_reasons:
            disarm_all('; '.join(immediate_reasons + [m for _, m in immediate_lane_reasons]), dry_run)
        else:
            for lane_name in sorted({l for l, _ in immediate_lane_reasons}):
                halt_buy_lanes([lane_name], '; '.join((m for l, m in immediate_lane_reasons if l == lane_name)), dry_run)
        return 1
    confirm_issues, advisory = split_by_severity(confirm_issues)
    for code, msg in advisory:
        log('WARN: %s (advisory — reported, not halting): %s' % (code, msg))
    record_advisories(advisory, now)
    if confirm_issues:
        return 1 if confirm_or_disarm(confirm_issues, dry_run, now=now) else 0
    try:
        clear_confirmation()
    except OSError as e:
        disarm_all('cannot clear guardian confirmation state: %s' % e, dry_run)
        return 1
    detail = '; '.join(('%s positions=%d open=$%.2f fires=%d spent=$%.2f' % row for row in lane_summaries))
    if watcher_halted_buys:
        log('HALTED-BUYS: armed_slot=%d lanes=%d %s' % (port, len(lane_summaries), detail))
        return 1
    try:
        last_fw = float(state.get('foreign_t') or 0)
        if now - last_fw >= FOREIGN_EVERY_SECS:
            state['foreign_t'] = now
            _fw_state, _fw_warnings = foreign_writer_check(OUR, now, state.get('foreign'), fetch=_foreign_fetch, ledger_path=os.path.join(BOT_DIR, 'data', 'ledger.jsonl'))
            state['foreign'] = _fw_state
            for w in _fw_warnings:
                log(w)
    except Exception as e:
        log('foreign-writer check failed (advisory, ignored): %s' % e)
    try:
        last_pf = float(state.get('preflight_t') or 0)
        if now - last_pf >= PREFLIGHT_EVERY_SECS:
            state['preflight_t'] = now
            results = preflight.run(preflight_records(BOT_DIR, port), preflight_value_sets(BOT_DIR))
            dead = preflight.failures(results)
            state['preflight_last'] = preflight.summary(results)
            log('PREFLIGHT: %s' % preflight.summary(results))
            for sensor, detail in dead:
                log('!!! DEAD ALARM: %s (%s) — %s' % (sensor.name, sensor.alarm, detail))
                record_error('', 'preflight', '%s: %s' % (sensor.name, detail))
            was = set(state.get('preflight_dead') or [])
            state['preflight_dead'] = sorted((x.name for x, _ in dead))
            fresh = [x for x in dead if x[0].name not in was]
            if fresh and notify.configured():
                body = '\n\n'.join(('%s  (%s)\n  %s\n  WHY IT MATTERS: %s' % (x.name, x.alarm, d, x.why) for x, d in fresh))
                notify.send('Abomination81 Copybot: %d ALARM(S) CANNOT FIRE' % len(fresh), 'A guardian self-check found alarms whose sensors are no longer present in live data. They are SILENT, which looks exactly like healthy.\n\n' + body + '\n\nTrading is unaffected; this is a check on the checkers.\n' + notify.local_stamp(now) + '\n', log=log)
    except Exception as e:
        log('preflight sweep failed (nothing else affected): %r' % (e,))
    try:
        check_buywatch_alerts(state, now)
    except Exception as e:
        log('buywatch alert check failed (nothing else affected): %r' % (e,))
    try:
        if digest_due(state, now) and notify.configured():
            _feed = {}
            try:
                with open(WATCHER_HEARTBEAT) as _f:
                    _feed = (json.load(_f) or {}).get('feed') or {}
            except (OSError, ValueError):
                _feed = {}
            _pf = state.get('preflight_last') or 'not yet run'
            state['digest_t'] = now - DIGEST_EVERY_SECS + DIGEST_RETRY_SECS
            if notify.send('Abomination81 Copybot: weekly check — %s' % ('all quiet' if not any((l.get('halted') for l in (st.get('lanes') or {}).values())) else 'LANES HALTED'), digest_body(st.get('lanes') or {}, scorecard.summarise(SCORECARD_PATH, now), _pf, _feed, now), log=log):
                state['digest_t'] = now
                log('weekly digest sent')
            else:
                log('weekly digest FAILED to send — the alert path is not working; retrying in %dm' % (DIGEST_RETRY_SECS // 60))
    except Exception as e:
        log('weekly digest failed (nothing else affected): %r' % (e,))
    op_meta = (_opdoc or {}).get('_meta') or {}
    try:
        _halted_now = sorted((n for n, l in (st.get('lanes') or {}).items() if l.get('halted')))
        _was = sorted(state.get('notify_halted') or [])
        if _halted_now and (not _was):
            scorecard.open_case(SCORECARD_PATH, _halted_now, op_meta, _LAST_CORROBORATION, now)
        elif _was and (not _halted_now):
            _open = [r for r in scorecard._read(SCORECARD_PATH) if not r.get('cleared_at')]
            _since = float(_open[-1].get('at')) if _open else now
            scorecard.close_case(SCORECARD_PATH, now, buys_declined=buys_declined_since(BOT_DIR, _since, now))
        _active = {str(c).split(':', 1)[0] for c, _m in confirm_issues}

        def _still(case):
            codes = case.get('codes') or []
            if not codes:
                return None
            return any((str(c).split(':', 1)[0] in _active for c in codes))
        for _r in scorecard.adjudicate(SCORECARD_PATH, now, _still):
            log('HALT SCORED %s: %s (%s) — %s' % (_r['verdict'], ', '.join(_r.get('lanes') or []), ', '.join(_r.get('codes') or ['unattributed']), _r.get('verdict_why')))
    except Exception as e:
        log('halt scoring failed (nothing else affected): %r' % (e,))
    try:
        notes, nstate = halt_notifications(st.get('lanes') or {}, op_meta, state, now)
        state.update(nstate)
        for subject, body in notes:
            if notify.configured():
                sent = notify.send(subject, body + '\n-- \nguardian on %s\n%s\n' % (socket.gethostname(), notify.local_stamp(now)), log=log)
                log('ALERT %s: %s' % ('sent' if sent else 'FAILED', subject))
            else:
                log('ALERT not sent (%s): %s' % (notify.why_not_configured(), subject))
    except Exception as e:
        log('halt notification failed (nothing else affected): %r' % (e,))
    to_resume, rstate, rwhy = plan_resume(st.get('lanes') or {}, op_meta, faults_active=False, state=state, now=now)
    state.update(rstate)
    if rwhy and (not to_resume):
        log('resume: %s' % rwhy)
    if to_resume:
        if resume_buy_lanes(to_resume, rwhy, dry_run):
            log('RESUMED BUYS on %s — %s' % (', '.join(to_resume), rwhy))
            persist_state(state)
            return 1
        log('could not resume %s — leaving the halt in place' % ', '.join(to_resume))
    stale = stale_halts(st.get('lanes') or {}, now)
    if stale:
        for name, age in stale:
            log('!!! STALE HALT: lane %s has bought NOTHING for %s and no fault is currently active. It will not resume on its own — clear it or diagnose it. (POST /api/incident/clear, or set halt_buys=false)' % (name, human_secs(age)))
        persist_state(state)
        log('NOT-PASS: %d lane(s) halted and forgotten: %s' % (len(stale), ', '.join(('%s=%s' % (n, human_secs(a)) for n, a in stale))))
        return 1
    persist_state(state)
    log('PASS: armed_slot=%d lanes=%d %s' % (port, len(lane_summaries), detail))
    return 0

def redirect_state_for_dry_run():
    global STATE_PATH, CONFIRM_PATH, LOG_PATH, _ALLOW_ERROR_LOG
    _ALLOW_ERROR_LOG = False
    for live, name in ((STATE_PATH, 'STATE_PATH'), (CONFIRM_PATH, 'CONFIRM_PATH')):
        shadow = live + '.dryrun'
        try:
            if os.path.exists(live):
                with open(live) as src, open(shadow, 'w') as dst:
                    dst.write(src.read())
            elif os.path.exists(shadow):
                os.unlink(shadow)
        except OSError:
            pass
        globals()[name] = shadow
    LOG_PATH = LOG_PATH + '.dryrun'

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--once', action='store_true', help='ignored; guardian always runs once per invocation')
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    if a.dry_run:
        redirect_state_for_dry_run()
    try:
        return gather_and_check(a.dry_run)
    except Exception as e:
        log('GUARDIAN ITSELF FAILED: %s — disarming out of caution' % e)
        if not a.dry_run:
            disarm_all('guardian crashed: %s' % e, dry_run=False)
        return 3
if __name__ == '__main__':
    sys.exit(main())
