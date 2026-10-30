"""Pure alert and diagnostic decisions used by the guardian I/O coordinator."""

import re
import notify
import scorecard
from guardian_watcher_evidence import human_secs

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
ADVISORY_ISSUE_CODES = ('cost_drift', 'matchup', 'watcher_feed_brief', 'watcher_feed', 'venue_maintenance', 'venue_status_unknown')

def is_advisory(code):
    return str(code).split(':', 1)[0] in ADVISORY_ISSUE_CODES

def split_by_severity(issues):
    halting = [i for i in issues if not is_advisory(i[0])]
    advisory = [i for i in issues if is_advisory(i[0])]
    return (halting, advisory)

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

HALT_REMIND_SECS = [900, 3600, 7200, 14400, 28800]
HALT_REMIND_EVERY = 28800

def halt_notifications(lanes, meta, state, now, scorecard_path):
    st = dict(state or {})
    halted = sorted((n for n, l in (lanes or {}).items() if l.get('halted')))
    prev = sorted(st.get('notify_halted') or [])
    msgs = []
    why = str((meta or {}).get('why') or 'no reason recorded')[:300]
    st['notify_halted'] = halted
    if halted and (not prev):
        st['notify_halted_since'] = now
        st['notify_sent'] = 0
        msgs.append(('Abomination81 Copybot: TRADING HALTED (%d lane%s)' % (len(halted), '' if len(halted) == 1 else 's'), 'Buying has stopped on: %s\nStopped at: %s\n%s\n\nReason recorded by the halting process:\n  %s\n\nExits are UNAFFECTED - the bot still follows each leader out of positions it\nholds. Only new buys are stopped.\n\nA halt does not clear itself unless its cause was a loss of visibility.\n' % (', '.join(halted), notify.local_stamp(now), scorecard.summary_line(scorecard.summarise(scorecard_path, now)), why)))
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
