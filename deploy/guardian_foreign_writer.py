"""Foreign-writer evidence and fail-closed split-brain diagnostics."""

import json

FOREIGN_TOLERANCE = 3
FOREIGN_STRIKES = 3
FOREIGN_WINDOW_SECS = 900
FOREIGN_PAGE_LIMIT = 500
FOREIGN_EVERY_SECS = 120
FOREIGN_BLIND_SECS = 1800
FOREIGN_TS_MIN = 1500000000
FOREIGN_TS_FUTURE_SLACK = 86400
FOREIGN_TS_BAD_FRACTION = 0.1

def page_covers(rows_returned, oldest_ts, since):
    if rows_returned < FOREIGN_PAGE_LIMIT:
        return True
    if oldest_ts is None:
        return False
    return oldest_ts < since

def foreign_compare(venue_fills, our_fills, complete):
    if not complete:
        return ('venue page is full AND starts inside the window — trades in the window may have been cut off', 0)
    if venue_fills > our_fills + FOREIGN_TOLERANCE:
        return ('foreign_writer', venue_fills - our_fills)
    return ('consistent', 0)

def foreign_observe(verdict, extra, prior):
    prior = prior if isinstance(prior, dict) else {}
    try:
        strikes = int(prior.get('strikes') or 0)
    except (TypeError, ValueError):
        strikes = 0
    try:
        worst = int(prior.get('worst') or 0)
    except (TypeError, ValueError):
        worst = 0
    if verdict == 'foreign_writer':
        strikes += 1
        worst = max(worst, int(extra))
        fire = worst if strikes == FOREIGN_STRIKES else None
        return ({'strikes': strikes, 'worst': worst, 'verdict': verdict}, fire)
    if verdict == 'consistent':
        return ({'strikes': 0, 'worst': 0, 'verdict': verdict}, None)
    return ({'strikes': strikes, 'worst': worst, 'verdict': verdict}, None)

def ledger_fills_since(path, since):
    try:
        n = 0
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if isinstance(r, dict) and r.get('ev') == 'fill' and (r.get('recon') is not True) and (float(r.get('t') or 0) >= since):
                    n += 1
        return n
    except OSError:
        return None

def foreign_writer_check(funder, now, prior, fetch, ledger_path):
    warnings = []
    since = now - FOREIGN_WINDOW_SECS
    prior = prior if isinstance(prior, dict) else {}
    st = dict(prior)
    st['last_t'] = now
    ours = ledger_fills_since(ledger_path, since)
    rows = None
    if ours is None:
        st['verdict'] = 'our own ledger is unreadable'
    else:
        try:
            rows = fetch(funder)
            if not isinstance(rows, list):
                raise ValueError('trade list is not a list')
        except Exception as e:
            st['verdict'] = 'could not read the venue: %s' % str(e)[:80]
            rows = None
    stamps = []
    if rows is not None:
        invalid = 0
        for r in rows:
            if not isinstance(r, dict):
                invalid += 1
                continue
            try:
                t = int(r.get('timestamp'))
            except (TypeError, ValueError):
                invalid += 1
                continue
            if FOREIGN_TS_MIN <= t <= now + FOREIGN_TS_FUTURE_SLACK:
                stamps.append(t)
            else:
                invalid += 1
        total_parsed = len(stamps) + invalid
        if rows and (not stamps):
            rows = None
            st['verdict'] = 'venue returned %d row(s) and not one had a usable timestamp — the trade feed is unreadable' % total_parsed
        elif total_parsed and invalid > total_parsed * FOREIGN_TS_BAD_FRACTION:
            rows = None
            st['verdict'] = 'venue timestamps are not usable (%d/%d invalid) — the trade feed format may have changed' % (invalid, total_parsed)
    if rows is not None:
        oldest = min(stamps) if stamps else None
        complete = page_covers(len(rows), oldest, since)
        venue = sum((1 for t in stamps if t >= since))
        verdict, extra = foreign_compare(venue, ours, complete)
        observed, fire = foreign_observe(verdict, extra, prior)
        st.update(observed)
        st['venue_fills'] = venue
        st['our_fills'] = ours
        if complete:
            st['last_complete_t'] = now
        if fire is not None:
            warnings.append('WARN: SPLIT BRAIN — the venue executed %d more fill(s) for this wallet than this instance booked, on %d consecutive checks (venue %d vs ours %d in the last %dm). Another writer is trading this Safe — most likely an old host that was not proven dead. Run deploy/prove_dead.sh against it. This is a WARNING ONLY and nothing has been halted.' % (fire, FOREIGN_STRIKES, venue, ours, FOREIGN_WINDOW_SECS // 60))
    lc = st.get('last_complete_t')
    try:
        stale = not lc or now - float(lc) > FOREIGN_BLIND_SECS
    except (TypeError, ValueError):
        stale = True
    if stale:
        warnings.append('WARN: the split-brain detector is BLIND — no complete venue read in %s (last verdict: %s). It cannot catch a second writer while in this state.' % ('%.0fs' % (now - float(lc)) if lc else 'its entire life', st.get('verdict')))
    return (st, warnings)
