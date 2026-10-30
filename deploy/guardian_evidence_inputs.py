"""Guardian read-side evidence adapters. They do not write operator state."""

import collections
import glob
import json
import os
import time

from guardian_checks import venue_shares

SETTLE_QUIET_SECS = 120

def ledger_activity(path, now, log, quiet_secs=None, settle_window_secs=48 * 3600):
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

def fetch_all_positions(user, http_json, data_api, page=POSITIONS_PAGE, max_pages=POSITIONS_MAX_PAGES):
    chain, pages = ({}, [])
    for i in range(max_pages):
        url = '%s/positions?user=%s&sizeThreshold=0&limit=%d&offset=%d' % (data_api, user, page, i * page)
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
PREFLIGHT_EVERY_SECS = 900

def preflight_records(bot_dir, port, dash, secs=6 * 3600):
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

def preflight_value_sets(bot_dir, heartbeat_path):
    sets = {}
    try:
        with open(heartbeat_path) as f:
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
def leader_book_fetcher(cache, fetch_positions):

    def fetch(addr):
        key = str(addr or '').lower()
        if not key:
            return None
        if key not in cache:
            try:
                book, complete, _why = fetch_positions(key)
                cache[key] = venue_shares(book) if complete else None
            except Exception:
                cache[key] = None
        return cache[key]
    return fetch

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
