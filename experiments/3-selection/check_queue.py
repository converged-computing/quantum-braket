#!/usr/bin/env python3
"""
check_queue.py — show live queue depth + online status for the queue-experiment
candidate QPUs, BEFORE committing real-money runs.

This is a standalone preview tool. It does not run anything on the cluster, does
not submit any task, and does not touch run_selection.py. It uses the SAME AWS
call (`aws braket get-device`) and the SAME cost-attributes.yaml that the
orchestrator's queue snapshot uses, so what you see here is exactly what
`--experiment queue` would record as `queue_at_submit`, plus the device status
that an enforcing `online-only` policy would gate on.

Usage:
  python3 check_queue.py                      # the queue pool (QPUs)
  python3 check_queue.py --all                # every backend in the file
  python3 check_queue.py sv1 iqm_emerald      # specific backends
  python3 check_queue.py --attributes path/to/cost-attributes.yaml

Requires: the `aws` CLI authenticated with Braket access in the device regions,
and PyYAML. Read-only; costs nothing.
"""
import argparse
import json
import os
import subprocess
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml")

HERE = os.path.dirname(os.path.abspath(__file__))

# Mirror run_selection.py: queue pool is the real QPUs (simulators have no queue)
POOL_QUEUE = ["rigetti_cepheus", "iqm_garnet", "iqm_emerald"]


def load_attrs(path):
    with open(path) as f:
        doc = yaml.safe_load(f)
    return {b["name"]: b for b in doc.get("backends", [])}


def get_device(b):
    """Return the raw get-device JSON for a backend attr dict, or None."""
    if not b or "device_arn" not in b:
        return None
    try:
        out = subprocess.check_output(
            ["aws", "braket", "get-device",
             "--device-arn", b["device_arn"],
             "--region", b.get("region", "us-east-1"),
             "--output", "json"],
            stderr=subprocess.PIPE)
        return json.loads(out)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode().strip().splitlines()
        msg = err[-1] if err else "unknown error"
        return {"_error": msg}
    except Exception as e:  # noqa: BLE001
        return {"_error": str(e)}


def normal_queue_depth(info):
    """Same parse as run_selection.queue_depth: the NORMAL (non-priority)
    quantum-task queue size; '>4000' becomes 4001."""
    for q in info.get("deviceQueueInfo", []):
        if "QUANTUM_TASKS" in q.get("queue", "").upper() and \
           q.get("queuePriority", "Normal") != "Priority":
            qs = q.get("queueSize", "0")
            return 4001 if str(qs).startswith(">") else int(qs)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backends", nargs="*",
                    help="backend names to check (default: the queue pool)")
    ap.add_argument("--all", action="store_true",
                    help="check every backend in the attributes file")
    ap.add_argument("--attributes",
                    default=os.path.join(HERE, "cost-attributes.yaml"))
    args = ap.parse_args()

    attrs = load_attrs(args.attributes)
    if args.all:
        names = list(attrs.keys())
    elif args.backends:
        names = args.backends
    else:
        names = POOL_QUEUE

    print(f"{'backend':<16} {'status':<10} {'queue':>7}   region / arn")
    print("-" * 78)
    depths = {}
    for name in names:
        b = attrs.get(name)
        if not b:
            print(f"{name:<16} {'(not in file)':<10}")
            continue
        if "device_arn" not in b:
            print(f"{name:<16} {'(no arn)':<10} {'-':>7}   simulator / not a QPU")
            continue
        info = get_device(b)
        if info is None:
            print(f"{name:<16} {'(skip)':<10}")
            continue
        if "_error" in info:
            print(f"{name:<16} {'ERROR':<10} {'-':>7}   {info['_error']}")
            continue
        status = info.get("deviceStatus", "?")
        depth = normal_queue_depth(info)
        depths[name] = (status, depth)
        depth_str = ">4000" if depth == 4001 else ("-" if depth is None else str(depth))
        region = b.get("region", "-")
        print(f"{name:<16} {status:<10} {depth_str:>7}   {region}")

    # Highlight what min-queue WOULD pick among online candidates with a depth.
    online = {n: d for n, (s, d) in depths.items()
              if s == "ONLINE" and d is not None}
    print("-" * 78)
    if online:
        pick = min(online, key=online.get)
        print(f"min-queue would pick: {pick} "
              f"(queue={'>4000' if online[pick] == 4001 else online[pick]}, "
              f"among online candidates {sorted(online)})")
    else:
        print("min-queue: no online candidate with a readable queue depth — "
              "selection would fall back to candidate order, or fail if all are "
              "unavailable. Re-check AWS access / regions / device status.")


if __name__ == "__main__":
    main()
