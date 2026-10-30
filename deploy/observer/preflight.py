"""Offline static contract for the sole Observer unit.

Local mode=dry has no systemd cutover to attest. This checker never reads
service state, credentials, or the network and cannot authorize live trading.
"""
import argparse
import json
from pathlib import Path

LEGACY = frozenset((
    "copybot-guardian.service", "copybot-watcher.service",
    "copybot-fillwatch.service", "copybot-buywatch.service",
    "copybot-guardian.timer", "copybot-fillwatch.timer",
    "copybot-buywatch.timer",
))
OBSERVER = "copybot-observer.service"


def unit_fields(content):
    """Parse systemd assignments without interpreting shell expressions."""
    if not isinstance(content, str):
        raise TypeError("unit content must be text")
    section = None
    fields = {}
    for line in content.splitlines():
        value = line.strip()
        if not value or value.startswith(("#", ";")):
            continue
        if value.startswith("[") and value.endswith("]"):
            section = value[1:-1]
        elif section is not None and "=" in value:
            key, item = value.split("=", 1)
            fields.setdefault((section, key), []).append(item)
        else:
            raise ValueError("malformed unit directive")
    return fields


REQUIRED = {
    ("Service", "Type"): "simple",
    ("Service", "User"): "copybot",
    ("Service", "Group"): "copybot",
    ("Service", "WorkingDirectory"): "/opt/copybot",
    ("Service", "ExecStart"): "/usr/bin/python3 /opt/copybot/deploy/observer/main.py",
    ("Service", "UnsetEnvironment"): "PRIVATE_KEY",
    ("Service", "UMask"): "0077",
    ("Service", "NoNewPrivileges"): "true",
    ("Service", "ProtectSystem"): "full",
    ("Service", "PrivateTmp"): "true",
    ("Service", "ReadWritePaths"): "/opt/copybot/run",
    ("Service", "CapabilityBoundingSet"): "",
    ("Install", "WantedBy"): "multi-user.target",
}


def check_unit_inventory(unit_dir):
    root = Path(unit_dir)
    errors = []
    for name in sorted(LEGACY):
        if (root / name).exists() or (root / name).is_symlink():
            errors.append("obsolete legacy unit remains: " + name)
    if tuple(sorted(root.glob("copybot-observer.*"))) != (root / OBSERVER,):
        errors.append("exactly one Observer service and no Observer timer required")
    path = root / OBSERVER
    if path.is_symlink() or not path.is_file():
        return errors + ["Observer unit unavailable or a symlink"]
    try:
        fields = unit_fields(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as error:
        return errors + ["Observer unit malformed: " + type(error).__name__]
    for (section, key), value in REQUIRED.items():
        if fields.get((section, key)) != [value]:
            errors.append("Observer %s.%s must equal %r" % (section, key, value))
    if fields.get(("Unit", "After")) != ["network-online.target"]:
        errors.append("Observer must depend only on network-online.target")
    for forbidden in ("Conflicts", "BindsTo", "Requires"):
        if ("Unit", forbidden) in fields:
            errors.append("Observer cannot reference legacy or live units: " + forbidden)
    for forbidden in ("ExecStartPre", "ExecStartPost", "ExecStopPost"):
        if ("Service", forbidden) in fields:
            errors.append("Observer must not add side-effectful " + forbidden)
    return errors


def evaluate(unit_dir):
    errors = check_unit_inventory(unit_dir)
    return {
        "status": "BLOCKED" if errors else "LOCAL_SOURCE_PASS",
        "errors": errors,
        "unit_contract": "one_observer_source_only",
        "service_actions": 0,
        "network_calls": 0,
        "live_parity": "NOT_CLAIMED",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit-dir", type=Path,
                        default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args(argv)
    result = evaluate(args.unit_dir)
    print(json.dumps(result, sort_keys=True))
    return 2 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
