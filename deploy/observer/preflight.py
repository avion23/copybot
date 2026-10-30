"""Side-effect-free opt-in Observer cutover preflight.

This module reads staged unit files and *operator-supplied* service states.
It never invokes systemctl, starts/stops services, reads secrets or validates
real market safety. A passing snapshot is not production attestation.
"""
import argparse
import json
import math
import os
from pathlib import Path
import pwd
import stat
import time

LEGACY_SERVICES = (
    "copybot-guardian.service",
    "copybot-watcher.service",
    "copybot-fillwatch.service",
    "copybot-buywatch.service",
)
LEGACY_TIMERS = (
    "copybot-guardian.timer",
    "copybot-fillwatch.timer",
    "copybot-buywatch.timer",
)
LEGACY = frozenset((*LEGACY_SERVICES, *LEGACY_TIMERS))
OBSERVER = "copybot-observer.service"
HOT = "copybot-hot.service"
ALL_STATES = frozenset((*LEGACY, OBSERVER, HOT))
MAX_SNAPSHOT_AGE = 300
WATCHER_HEARTBEAT_MAX_AGE = 90
MAX_HEARTBEAT_BYTES = 65536
START_DELAYS = {
    "copybot-guardian.timer": ("30s", "11s", "copybot-guardian.service"),
    "copybot-fillwatch.timer": ("5min", "15min", "copybot-fillwatch.service"),
    "copybot-buywatch.timer": ("7min", "15min", "copybot-buywatch.service"),
}
ENTRYPOINTS = {
    "copybot-guardian.service": "guardian.py --once",
    "copybot-watcher.service": "watcher.py --grace 45",
    "copybot-fillwatch.service": "fillwatch.py --once",
    "copybot-buywatch.service": "buywatch.py --once",
}


def unit_fields(text):
    """Parse only literal unit assignments, including repeated After=."""
    if not isinstance(text, str):
        raise ValueError("unit content must be text")
    section = None
    fields = {}
    for line in text.splitlines():
        item = line.strip()
        if not item or item.startswith(("#", ";")):
            continue
        if item.startswith("[") and item.endswith("]"):
            section = item[1:-1]
            continue
        if "=" not in item or section is None:
            raise ValueError("invalid unit directive")
        key, value = item.split("=", 1)
        fields.setdefault((section, key), []).append(value.strip())
    return fields


def get_one(fields, section, key):
    values = fields.get((section, key), ())
    if len(values) != 1:
        return None
    return values[0]


def require_equal(fields, section, key, value, errors, unit):
    if get_one(fields, section, key) != value:
        errors.append("%s: expected %s.%s=%s" % (unit, section, key, value))


def check_unit_inventory(unit_dir):
    """Validate installed *source* contracts; never inspect the unit manager."""
    root = Path(unit_dir)
    errors = []
    parsed = {}
    for name in sorted(LEGACY | {OBSERVER}):
        path = root / name
        try:
            if not path.is_file() or path.is_symlink():
                raise ValueError("unit must be an ordinary staged file")
            parsed[name] = unit_fields(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            errors.append("%s: not inspectable (%s)" % (name, type(exc).__name__))
    if OBSERVER not in parsed:
        return errors
    f = parsed[OBSERVER]
    conflicts = get_one(f, "Unit", "Conflicts")
    if conflicts is None or set(conflicts.split()) != LEGACY or len(conflicts.split()) != 7:
        errors.append("Observer must conflict with exactly seven legacy units")
    after = set()
    for value in f.get(("Unit", "After"), ()):
        after.update(value.split())
    if not LEGACY.issubset(after) or "network-online.target" not in after:
        errors.append("Observer startup must order after all legacy units and network")
    invariants = (
        ("Service", "Type", "simple"),
        ("Service", "User", "copybot"),
        ("Service", "Group", "copybot"),
        ("Service", "WorkingDirectory", "/opt/copybot"),
        ("Service", "ExecStart", "/usr/bin/python3 /opt/copybot/deploy/observer/main.py"),
        ("Service", "EnvironmentFile", "/opt/copybot/deploy/copybot.env"),
        ("Service", "UnsetEnvironment", "PRIVATE_KEY"),
        ("Service", "UMask", "0077"),
        ("Service", "NoNewPrivileges", "true"),
        ("Service", "ProtectSystem", "full"),
        ("Service", "PrivateTmp", "true"),
        ("Service", "ReadWritePaths", "/opt/copybot/run"),
        ("Service", "TimeoutStopSec", "120s"),
        ("Service", "Restart", "on-failure"),
        ("Service", "CapabilityBoundingSet", ""),
        ("Install", "WantedBy", "multi-user.target"),
    )
    for section, key, value in invariants:
        require_equal(f, section, key, value, errors, OBSERVER)
    for forbidden in ("ExecStartPre", "ExecStartPost", "ExecStopPost"):
        if ("Service", forbidden) in f:
            errors.append("Observer must not add a side-effectful %s" % forbidden)
    for name, (boot, interval, target) in START_DELAYS.items():
        legacy = parsed.get(name)
        if legacy is not None:
            for key, value in (
                ("OnBootSec", boot), ("OnUnitActiveSec", interval),
                ("Unit", target),
            ):
                require_equal(legacy, "Timer", key, value, errors, name)
    for name, script in ENTRYPOINTS.items():
        legacy = parsed.get(name)
        if legacy is not None:
            want = "/usr/bin/python3 /opt/copybot/deploy/" + script
            require_equal(legacy, "Service", "ExecStart", want, errors, name)
    return errors


def check_run_ownership(run_dir, service_uid, *, require_lock=False):
    """Read-only filesystem metadata; never chmod, create, or open secrets."""
    errors = []
    path = Path(run_dir)
    if type(service_uid) is not int or service_uid < 0:
        return ["invalid service user identifier"]
    try:
        info = os.stat(path, follow_symlinks=False)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != service_uid
                or stat.S_IMODE(info.st_mode) & 0o022):
            errors.append("run directory must be real, owned, not group/other writable")
    except OSError as exc:
        errors.append("run directory unavailable: %s" % type(exc).__name__)
        return errors
    lock = path / "observer.owner.lock"
    try:
        info = os.stat(lock, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != service_uid
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            errors.append("owner lock must be single-link owned regular mode-0600")
    except FileNotFoundError:
        # A pre-start check may legitimately have no lock yet; an active
        # Observer cannot claim an owner lease without its lock inode.
        if require_lock:
            errors.append("active Observer owner lock is missing")
    except OSError as exc:
        errors.append("owner lock unavailable: %s" % type(exc).__name__)
    return errors


def check_cutover_artifacts(run_dir, service_uid, *, now):
    """Verify readable, owner-controlled Observer artifacts, not live parity.

    This is an explicit path supplied by an operator. No systemctl, provider,
    signing key, hostname or environment-file access occurs here.
    """
    errors = []
    root = Path(run_dir)
    for name in ("observer_observations.jsonl", "observer_notifications.jsonl",
                 "observer_alerts.jsonl", "errors.jsonl", "watcher.log"):
        path = root / name
        try:
            info = os.stat(path, follow_symlinks=False)
            mode = stat.S_IMODE(info.st_mode)
            exact = name != "watcher.log"
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != service_uid
                    or info.st_nlink != 1
                    or (mode != 0o600 if exact else bool(mode & 0o022))):
                errors.append("Observer artifact has unsafe ownership or mode: " + name)
        except OSError as exc:
            errors.append("Observer artifact unavailable: %s (%s)" %
                          (name, type(exc).__name__))

    # The Guardian must have completed a recent audit. An old state file from
    # the disabled timer cannot establish cutover health indefinitely.
    guardian_path = root / "guardian_state.json"
    try:
        guardian = os.stat(guardian_path, follow_symlinks=False)
        if (not stat.S_ISREG(guardian.st_mode)
                or guardian.st_uid != service_uid
                or guardian.st_nlink != 1
                or stat.S_IMODE(guardian.st_mode) & 0o022):
            errors.append("Guardian state inode is unsafe")
        if not 0 <= now - guardian.st_mtime <= WATCHER_HEARTBEAT_MAX_AGE:
            errors.append("Guardian state has not been refreshed recently")
    except OSError as exc:
        errors.append("Guardian state unavailable: %s" % type(exc).__name__)

    # A fresh file alone does not prove the watcher can decode leader fills.
    # Read one bounded heartbeat from a verified, non-symlink regular inode.
    hb_path = root / "watcher_heartbeat.json"
    try:
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        fd = os.open(hb_path, flags)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != service_uid
                    or info.st_nlink != 1
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_size > MAX_HEARTBEAT_BYTES):
                raise ValueError("watcher heartbeat inode is unsafe or oversized")
            with os.fdopen(os.dup(fd), "rb") as stream:
                data = stream.read(MAX_HEARTBEAT_BYTES + 1)
            if len(data) > MAX_HEARTBEAT_BYTES:
                raise ValueError("watcher heartbeat exceeds bounded read")
            heartbeat = json.loads(data)
        finally:
            os.close(fd)
        if not isinstance(heartbeat, dict):
            raise ValueError("watcher heartbeat is not an object")
        stamp = heartbeat.get("t")
        if (type(stamp) not in (int, float)
                or not math.isfinite(stamp)
                or not 0 <= now - stamp <= WATCHER_HEARTBEAT_MAX_AGE):
            errors.append("watcher heartbeat is missing, future or stale")
        feed = heartbeat.get("feed")
        if (not isinstance(feed, dict)
                or feed.get("useful") is not True
                or feed.get("decoded_ever") is not True):
            errors.append("watcher feed cannot prove useful decoded activity")
        if heartbeat.get("leaders_error"):
            errors.append("watcher cannot verify its leader registry")
        leaders = heartbeat.get("leaders")
        if (not isinstance(leaders, list) or not leaders
                or any(not isinstance(n, str) or not n for n in leaders)):
            errors.append("watcher heartbeat has no verified leader coverage")
        fills = heartbeat.get("fills")
        if (not isinstance(fills, dict)
                or type(fills.get("dropped_unjudged")) is not int
                or fills["dropped_unjudged"] != 0):
            errors.append("watcher reports missing or dropped unjudged fill evidence")
    except (OSError, ValueError, TypeError) as exc:
        errors.append("watcher heartbeat unreadable or invalid: %s" %
                      type(exc).__name__)
    return errors


def check_state_snapshot(snapshot, phase, *, now):
    """Require recent explicit evidence; never query systemctl implicitly."""
    errors = []
    if not isinstance(snapshot, dict):
        return ["operator service-state snapshot required"]
    captured_at = snapshot.get("captured_at")
    if (type(captured_at) not in (int, float)
            or not 0 <= now - captured_at <= MAX_SNAPSHOT_AGE):
        errors.append("service-state snapshot is invalid, future or stale")
    if snapshot.get("source") != "operator-collected-systemctl":
        errors.append("service states must identify their operator-collected origin")
    units = snapshot.get("units")
    if not isinstance(units, dict):
        return errors + ["service states missing a units object"]
    if set(units) != ALL_STATES:
        errors.append("service states must contain exactly eight observer/legacy plus hot units")
    for name in sorted(ALL_STATES):
        state = units.get(name)
        if not isinstance(state, dict) or set(state) != {"active", "unit_file"}:
            errors.append("missing or malformed unit state: %s" % name)
            continue
        active, unit_file = state["active"], state["unit_file"]
        if (active not in ("active", "inactive", "failed",
                           "activating", "deactivating")
                or unit_file not in ("enabled", "disabled", "static",
                                      "indirect", "masked")):
            errors.append("unsupported unit state: %s" % name)
            continue
        if name == HOT:
            if active != "active":
                errors.append("the execution engine must remain active: %s" % HOT)
            continue
        if name == OBSERVER:
            expected = ("inactive", "disabled") if phase == "ready" else (
                "active", "enabled")
            if (active, unit_file) != expected:
                errors.append("Observer state mismatches %s phase" % phase)
            continue
        if active != "inactive":
            errors.append("legacy unit still active or failed: %s" % name)
        if name in LEGACY_TIMERS or name == "copybot-watcher.service":
            if unit_file != "disabled":
                errors.append("legacy timer/watcher not disabled: %s" % name)
        elif unit_file not in ("static", "disabled"):
            errors.append("legacy one-shot not static/disabled: %s" % name)
    return errors


def evaluate(unit_dir, *, phase="staged", snapshot=None, run_dir=None,
             service_uid=None, now=None):
    """Explain exactly what was checked; never claim a live cutover happened."""
    errors = check_unit_inventory(unit_dir)
    if phase not in ("staged", "ready", "cutover"):
        errors.append("unknown preflight phase")
    if phase in ("ready", "cutover"):
        errors.extend(check_state_snapshot(
            snapshot, phase, now=time.time() if now is None else now,
        ))
        if run_dir is None or service_uid is None:
            errors.append("run directory and service UID required for phase %s" % phase)
        else:
            errors.extend(check_run_ownership(
                run_dir, service_uid, require_lock=phase == "cutover"
            ))
            if phase == "cutover":
                errors.extend(check_cutover_artifacts(
                    run_dir, service_uid, now=time.time() if now is None else now,
                ))
    return {
        "phase": phase,
        "status": "BLOCKED" if errors else "OFFLINE_SNAPSHOT_PASS",
        "errors": errors,
        "unit_contract": "staged_source_files",
        "runtime_states": "operator_supplied_not_authenticated"
        if phase != "staged" else "not_checked",
        "live_parity": "UNKNOWN",
        "service_actions": 0,
        "network_calls": 0,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit-dir", default=str(Path(__file__).resolve().parent.parent))
    parser.add_argument("--phase", choices=("staged", "ready", "cutover"),
                        default="staged")
    parser.add_argument("--state-snapshot", type=Path,
                        help="recent operator-collected JSON; no systemctl calls")
    parser.add_argument("--run-dir", type=Path,
                        help="staged deployment run dir; inspected, never modified")
    parser.add_argument("--service-user", default="copybot")
    args = parser.parse_args(argv)
    snapshot = None
    if args.state_snapshot:
        try:
            snapshot = json.loads(args.state_snapshot.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            snapshot = None
    uid = None
    if args.phase != "staged":
        try:
            uid = pwd.getpwnam(args.service_user).pw_uid
        except KeyError:
            pass
    result = evaluate(args.unit_dir, phase=args.phase, snapshot=snapshot,
                      run_dir=args.run_dir, service_uid=uid)
    print(json.dumps(result, sort_keys=True))
    return 2 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
