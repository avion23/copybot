"""Pure guardian inventory, sizing and exposure checks. No I/O or global mutable state."""

import collections

FIRE_PER_TOKEN_LIMIT = 8
FIRE_PER_TOKEN_ABS = 40
EXPOSURE_MARGIN = 1.25
EXPOSURE_MIN_SHARES = 25.0
TOTAL_FIRE_LIMIT = 25
SHARE_API_EPSILON = 0.001
MAX_STRANDED_DUST_SHARES = 1.0
MAX_STRANDED_DUST_USD = 0.1
MAX_ORPHAN_VALUE_USD = 1.0
DRIFT_TRIP_SHARES = 1.0

def check_three_way(chain_cost, backend_open_usd, frontend_deployed, tol, settle_tol=None):
    reasons = []
    if frontend_deployed is None:
        reasons.append('frontend deployed value is missing')
    elif abs(float(frontend_deployed) - backend_open_usd) > tol:
        reasons.append('frontend deployed $%.2f vs backend ledger $%.2f (diff $%+.2f)' % (frontend_deployed, backend_open_usd, float(frontend_deployed) - backend_open_usd))
    return (len(reasons) == 0, reasons)

def lane_caps(lane_cfg):
    sizing, budget = (lane_cfg.get('sizing', {}), lane_cfg.get('budget', {}))
    bankroll = budget.get('bankroll_usd')
    if bankroll is None:
        return (float(sizing['max_usd_per_fill']), float(budget['daily_usd']), float(budget['per_market_usd']))
    max_open = float(bankroll) * float(budget.get('open_frac', 0.85))
    per_market = max_open * float(budget.get('per_market_frac', 0.6))
    return (per_market * float(budget.get('per_fill_frac', 0.4167)), max_open * float(budget.get('daily_frac', 0.7)), per_market)

def _num(row, key):
    try:
        return float((row or {}).get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0

def benign_chain_surplus(shares, chain_row, max_shares=MAX_STRANDED_DUST_SHARES, max_usd=MAX_STRANDED_DUST_USD):
    if not 0.0 < shares < max_shares:
        return False
    price = _num(chain_row, 'curPrice')
    if price <= 0.0:
        return False
    return shares * price <= max_usd + 1e-09

def venue_shares(chain):
    out = {}
    for tok, row in (chain or {}).items():
        try:
            out[str(tok)] = float(row.get('size'))
        except (TypeError, ValueError, AttributeError):
            continue
    return out

def conservation_breaches(lane_holdings, chain, rel_tol=0.0001, abs_tol=0.001):
    claims = {}
    for lane, held in (lane_holdings or {}).items():
        for token, shares in (held or {}).items():
            try:
                sh = float(shares)
            except (TypeError, ValueError):
                continue
            if sh <= abs_tol:
                continue
            claims.setdefault(token, []).append((lane, sh))
    out = []
    for token, rows in claims.items():
        if token not in chain:
            continue
        try:
            physical = float(chain[token].get('size') or 0.0)
        except (TypeError, ValueError):
            continue
        if len(rows) < 2:
            continue
        total = sum((sh for _, sh in rows))
        tol = max(abs_tol, physical * rel_tol)
        if total > physical + tol:
            out.append({'token': token, 'physical': physical, 'claimed': round(total, 6), 'excess': round(total - physical, 6), 'claimants': sorted(((l, round(sh, 6)) for l, sh in rows), key=lambda r: -r[1])})
    out.sort(key=lambda r: -r['excess'])
    return out

def net_chain_for_lane(chain, all_holdings, lane_name):
    siblings = {}
    for name, held in (all_holdings or {}).items():
        if name == lane_name:
            continue
        for tok, sh in (held or {}).items():
            try:
                siblings[str(tok)] = siblings.get(str(tok), 0.0) + float(sh or 0.0)
            except (TypeError, ValueError):
                continue
    if not siblings:
        return chain
    out = {}
    for tok, row in (chain or {}).items():
        claim = siblings.get(str(tok), 0.0)
        if claim <= 0.0:
            out[tok] = row
            continue
        adj = dict(row or {})
        adj['size'] = max(float((row or {}).get('size', 0.0) or 0.0) - claim, 0.0)
        out[tok] = adj
    return out

def check_share_drift(held, chain, tol_shares=SHARE_API_EPSILON, settling=(), resolved=()):
    reasons = []
    ignored_dust = []
    noticed = []
    total_drift = 0.0
    for tok, sh in held.items():
        have = float(chain.get(tok, {}).get('size', 0.0) or 0.0)
        d = sh - have
        if abs(d) <= tol_shares:
            continue
        if any((tok.startswith(p) for p in settling if p)):
            continue
        if d < 0 and benign_chain_surplus(-d, chain.get(tok, {})):
            ignored_dust.append('token ...%s: %.4f untracked sh ($%.4f at mark)' % (tok[-10:], -d, -d * _num(chain.get(tok, {}), 'curPrice')))
            continue
        if d > 0 and str(tok) in resolved:
            ignored_dust.append('token ...%s: %.4f sh the ledger still shows after the market RESOLVED — awaiting settlement accounting; resolution alone does not prove redemption' % (tok[-10:], d))
            continue
        total_drift += abs(d)
        noticed.append('token ...%s: ledger %.4f sh vs chain %.4f sh (%+.4f) — %s' % (tok[-10:], sh, have, d, 'we would OVERSELL and be refused' if d > 0 else 'STRANDED shares the ledger cannot see'))
    if total_drift > DRIFT_TRIP_SHARES:
        detail = list(noticed)
        if len(detail) > 3:
            detail = detail[:3] + ['... and %d more' % (len(detail) - 3)]
        reasons = detail + ['TOTAL drift %.4f sh exceeds the %.2f sh limit' % (total_drift, DRIFT_TRIP_SHARES)]
        noticed = []
    if len(ignored_dust) > 3:
        ignored_dust = ignored_dust[:3] + ['... and %d more' % (len(ignored_dust) - 3)]
    if len(noticed) > 3:
        noticed = noticed[:3] + ['... and %d more' % (len(noticed) - 3)]
    return (len(reasons) == 0, reasons, ignored_dust, noticed)
CAP_SCALE_TRUST_MAX = 3.0
RAW_FIRE_BACKSTOP = 100

def check_fire_patterns(fires, max_usd_per_fill, per_market_usd=None, cap_scale=1.0, pct=None, fire_per_token_limit=FIRE_PER_TOKEN_LIMIT, total_fire_limit=TOTAL_FIRE_LIMIT, raw_fire_backstop=RAW_FIRE_BACKSTOP, facts_out=None):
    buys = [f for f in fires if f.get('side', 'BUY') == 'BUY']
    reasons = []
    scale = min(max(float(cap_scale or 1.0), 0.5), CAP_SCALE_TRUST_MAX)
    distinct = {f.get('condition') or f['tok'] for f in buys}
    if len(distinct) > total_fire_limit:
        reasons.append('BUYs on %d distinct markets in the window (limit %d; his measured ceiling is 13)' % (len(distinct), total_fire_limit))
    if len(buys) > raw_fire_backstop:
        reasons.append('%d raw BUY fires in the window (backstop %d) — pathological churn whatever the market count' % (len(buys), raw_fire_backstop))
    by_cond = collections.defaultdict(set)
    for f in buys:
        by_cond[f.get('condition') or f['tok']].add(f.get('tok'))
    for cond, toks in sorted(by_cond.items(), key=lambda kv: -len(kv[1]))[:1]:
        if len(toks) > 1:
            reasons.append('BUYs on %d different outcomes of one market ...%s — the 24h market quarantine should have refused the second' % (len(toks), cond[-10:]))
    if pct and pct > 0:
        agg = collections.defaultdict(lambda: [0.0, 0.0])
        for f in buys:
            try:
                ours = float(f.get('shares') or 0.0)
                hisf = float(f.get('his_fill') or 0.0)
            except (TypeError, ValueError):
                continue
            if ours <= 0 or hisf <= 0:
                continue
            a = agg[f.get('tok'), f.get('his_order')]
            a[0] += ours
            a[1] += hisf

        def _ratio(kv):
            (_tok, _h), (ours_, his_) = kv
            return ours_ / his_ if his_ > 0 else 0.0
        for (tok, _his), (ours, hisf) in sorted(agg.items(), key=_ratio, reverse=True)[:1]:
            allowed = pct * hisf * EXPOSURE_MARGIN
            if ours > allowed and ours > EXPOSURE_MIN_SHARES:
                if facts_out is not None:
                    facts_out.append({'family': 'exposure', 'reason': None, 'token': tok, 'ours_shares': ours, 'his_shares': hisf, 'pct': pct, 'allowed_ratio': pct * EXPOSURE_MARGIN})
                reasons.append('bought %.1f sh against ONE order of his on ...%s while he filled %.1f sh — %.1f%% copied where the lane asks %.1f%% (allowance %.1f sh at %.0f%% margin). THIS is the runaway shape: buying past the ratio, not merely firing often.' % (ours, str(tok)[-10:], hisf, 100.0 * ours / hisf, 100.0 * pct, allowed, 100.0 * (EXPOSURE_MARGIN - 1.0)))
                if facts_out is not None:
                    facts_out[-1]['reason'] = reasons[-1]
    by_tok = collections.Counter((f.get('tok') for f in buys))
    for tok, n in by_tok.most_common(1):
        if n > FIRE_PER_TOKEN_ABS:
            reasons.append('%d BUY fires on one outcome ...%s (absolute cap %d)' % (n, str(tok)[-10:], FIRE_PER_TOKEN_ABS))
    eps = 1e-06
    if per_market_usd:
        spend = collections.defaultdict(float)
        for f in buys:
            spend[f.get('condition') or f['tok']] += f.get('usd', 0.0)
        for tok, usd in sorted(spend.items(), key=lambda kv: -kv[1])[:1]:
            if usd > per_market_usd * scale + eps:
                reasons.append('$%.2f of BUYs on one market ...%s in the window, over the $%.2f per-market cap (x%.3f scale) — the clamp failed' % (usd, tok[-10:], per_market_usd * scale, scale))
    for f in buys:
        if f['usd'] > max_usd_per_fill * scale + eps:
            reasons.append('a fire cost $%.2f, exceeding max_usd_per_fill $%.2f (x%.3f scale) — the clamp failed' % (f['usd'], max_usd_per_fill * scale, scale))
            break
    return (len(reasons) == 0, reasons)

def worthless_orphan(row):
    if 'currentValue' not in (row or {}):
        return False
    try:
        v = float(row['currentValue'])
    except (TypeError, ValueError):
        return False
    return 0.0 <= v <= MAX_ORPHAN_VALUE_USD

def check_unattributed(chain, backend_tokens, quarantined=(), allowed=()):
    quarantined = set(quarantined or ())
    allowed = set(allowed or ())
    candidates = sorted(set(chain) - set(backend_tokens) - quarantined - allowed)
    ignored_dust = [t for t in candidates if benign_chain_surplus(_num(chain[t], 'size'), chain[t]) or worthless_orphan(chain[t])]
    orphans = [t for t in candidates if t not in ignored_dust]
    if orphans:
        worth = sum((_num(chain[t], 'currentValue') or 0.0 for t in orphans))
        return (False, ['unattributed inventory on %d token(s) worth $%.2f that no lane tracks: %s' % (len(orphans), worth, ', '.join((t[-10:] for t in orphans[:5])))], ignored_dust)
    return (True, [], ignored_dust)
