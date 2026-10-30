"""Existing monitor audit kernels under one owner's scheduler.

All adapters run within the Observer process. No subprocess, new venue query
logic, signing key, or fabricated independent corroboration is introduced.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class AuditResult:
    source: str
    exit_code: int
    status: str
    reason: str

    def __post_init__(self):
        if self.status not in ("PASS", "ALERT", "UNVERIFIABLE"):
            raise ValueError("invalid audit health status")


def _outcome(name, code, unknown_codes, alert_codes):
    if type(code) is not int:
        return AuditResult(name, 3, "UNVERIFIABLE", "audit returned noninteger status")
    if code == 0:
        return AuditResult(name, code, "PASS", "audit completed without findings")
    if code in alert_codes:
        return AuditResult(name, code, "ALERT", "audit reported a fault or halt")
    if code in unknown_codes:
        return AuditResult(name, code, "UNVERIFIABLE",
                           "audit could not independently verify its evidence")
    return AuditResult(name, code, "UNVERIFIABLE", "unexpected audit return code")


def _scoped(source, fn):
    import corroborate
    with corroborate.sensor_source(source):
        return fn()


def guardian_once():
    return _scoped("guardian", _guardian_once)


def _guardian_once():
    """Same exception/halt policy as guardian.main without argparse/sys.argv."""
    import guardian
    try:
        code = guardian.gather_and_check(False)
    except Exception as error:
        guardian.log("GUARDIAN ITSELF FAILED: %s — disarming out of caution" % error)
        try:
            guardian.disarm_all("guardian crashed: %s" % error, dry_run=False)
        except Exception as halt_error:
            guardian.log("GUARDIAN HALT REQUEST FAILED: %s" % halt_error)
        code = 3
    return _outcome("guardian", code, unknown_codes=(3,), alert_codes=(1, 2))


def fillwatch_once():
    return _scoped("fillwatch", _fillwatch_once)


def _fillwatch_once():
    """Same result and error semantics as fillwatch.main with --once."""
    import fillwatch as fw
    try:
        code = fw.run(fw.WINDOW_SECS, False)
    except fw.Unverifiable as error:
        fw.log("UNVERIFIED (no conclusion drawn, nothing halted): %s" % error)
        code = 2
    except Exception as error:
        fw.log("FILLWATCH ITSELF FAILED: %r — nothing halted" % error)
        code = 2
    return _outcome("fillwatch", code, unknown_codes=(2,), alert_codes=(1,))


def buywatch_once():
    return _scoped("buywatch", _buywatch_once)


def _buywatch_once():
    """Same result and error semantics as buywatch.main with --once."""
    import buywatch as bw
    try:
        code = bw.run(bw.WINDOW_SECS, False)
    except bw.fw.Unverifiable as error:
        bw.log("UNVERIFIED (nothing halted, nothing concluded): %s" % error)
        code = 3
    except Exception as error:
        bw.log("BUYWATCH ITSELF FAILED: %r — nothing halted" % error)
        code = 3
    return _outcome("buywatch", code, unknown_codes=(3,), alert_codes=(1,))


AUDITS = {
    "guardian": guardian_once,
    "fillwatch": fillwatch_once,
    "buywatch": buywatch_once,
}
