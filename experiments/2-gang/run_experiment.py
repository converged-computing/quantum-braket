#!/usr/bin/env python3
"""
run_experiment.py — Gang scheduling experiment (Experiment 2).

Compares Fluence gated scheduling vs default scheduler for a
leader+worker quantum gang workflow. The key metric is wasted classical
node-seconds: time workers spend running before they have useful work.

  Without Fluence: workers start immediately, idle during QPU queue wait.
  With Fluence:    workers are gated until QPU reaches position==1, then
                   ungated with high priority. Idle time ≈ 0.

Timing model per gang run:
  t_leader_start       leader pod created
  t_submit             leader called device.run()         [TIMING log]
  t_queued             task submitted to vendor queue     [TIMING log]
  t_result             QPU result received                [TIMING log]
  t_leader_ready       leader-ready signal written to S3  [TIMING log]
  t_workers_done       all workers completed              [TIMING log]
  t_leader_end         leader pod Succeeded

  t_worker_start_i     worker i pod created
  t_leader_ready_seen_i  worker i saw leader-ready signal [TIMING log]
  t_worker_done_i      worker i pod Succeeded

Key derived metrics:
  qpu_queue_wait_s     = t_result - t_queued
  worker_idle_s_i      = t_leader_ready_seen_i - t_worker_start_i
  total_worker_idle_s  = sum(worker_idle_s_i)             ← THE KEY METRIC
  worker_node_seconds  = total_worker_idle_s × n_workers  (per-node normalised)
  leader_wall_s        = t_leader_end - t_leader_start
  batch_wall_s         = max(all pod finish times) - t_leader_start

Usage:
  # Start here — free, no queue wait, validates pipeline
  python3 run_experiment.py --backend sv1 --scheduler default
  python3 run_experiment.py --backend sv1 --scheduler fluence

  # Real QPU — demonstrates actual savings
  python3 run_experiment.py --backend iqm_garnet --scheduler default --n-shots 100
  python3 run_experiment.py --backend iqm_garnet --scheduler fluence --n-shots 100

  # Sweep schedulers automatically
  python3 run_experiment.py --backend sv1 --schedulers default fluence

  # Vary worker count
  python3 run_experiment.py --backend sv1 --n-workers 2 4 8

  # List backends
  python3 run_experiment.py --list-backends
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ── Backend registry ───────────────────────────────────────────────────────────

BACKENDS = {
    "sv1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
        "cost_note": "~$0.075/min — start here",
        "is_qpu":    False,
    },
    "tn1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/tn1",
        "cost_note": "~$0.275/min",
        "is_qpu":    False,
    },
    "dm1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/dm1",
        "cost_note": "~$0.075/min",
        "is_qpu":    False,
    },
    "rigetti_cepheus": {
        "arn":       "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q",
        "cost_note": "$0.30/task + $0.000425/shot",
        "is_qpu":    True,
        "cost_per_shot": 0.000425,
    },
    "iqm_garnet": {
        "arn":       "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet",
        "cost_note": "$0.30/task + $0.00145/shot",
        "is_qpu":    True,
        "cost_per_shot": 0.00145,
    },
    "iqm_emerald": {
        "arn":       "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald",
        "cost_note": "$0.30/task + $0.00160/shot",
        "is_qpu":    True,
        "cost_per_shot": 0.00160,
    },
}

SHOT_COST = {b: v.get("cost_per_shot", 0) for b, v in BACKENDS.items()}


def list_backends():
    print("\nAvailable backends:")
    print(f"  {'name':<20} {'type':<12} {'cost'}")
    print(f"  {'-'*20} {'-'*12} {'-'*40}")
    for name, info in BACKENDS.items():
        kind = "QPU" if info["is_qpu"] else "simulator"
        print(f"  {name:<20} {kind:<12} {info['cost_note']}")
    print()


# ── kubectl helpers ────────────────────────────────────────────────────────────

def kubectl(args, check=True, capture=True):
    cmd = ["kubectl"] + args
    result = subprocess.run(cmd, capture_output=capture, text=True)
    if check and result.returncode != 0:
        print(f"[orchestrator] kubectl error: {result.stderr.strip()}", file=sys.stderr)
        raise RuntimeError(f"kubectl {' '.join(args)} failed")
    return result.stdout.strip() if capture else None


def apply_manifest(manifest_yaml, namespace):
    result = subprocess.run(
        ["kubectl", "apply", "-n", namespace, "-f", "-"],
        input=manifest_yaml, text=True, capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {result.stderr.strip()}")
    return result.stdout.strip()


def delete_gang_pods(run_name, n_workers, namespace, scheduler):
    # Delete the PODS of a (prior) run, to clear stragglers before starting the
    # next run with the same names.
    names = [f"{run_name}-leader"] + \
            [f"{run_name}-worker-{i}" for i in range(n_workers)]
    kubectl(["delete", "pod", "-n", namespace, "--ignore-not-found=true",
             "--wait=true"] + names, check=False)

    # PodGroup cleanup follows the "whoever created it cleans it up" rule:
    #   - default arm: the orchestrator creates the PodGroup (<run_name>-pg) from
    #     the manifest, so the orchestrator deletes it here. (It holds no Fluxion
    #     allocation — the default arm uses the native scheduler — but leaving it
    #     would collide when the same config is re-run, and clutter the cluster.)
    #   - fluence arm: the webhook creates the PodGroup (<run_name>) and Fluence's
    #     reconciler deletes it when the gang completes. The orchestrator must NOT
    #     delete it, so we can validate that the reconciler does its job (a leak
    #     shows up as a stuck leader rather than being masked here).
    if scheduler == "default":
        kubectl(["delete", "podgroup", "-n", namespace, "--ignore-not-found=true",
                 "--wait=true", f"{run_name}-pg"], check=False)


def get_pod_phase(pod_name, namespace):
    try:
        return kubectl(["get", "pod", pod_name, "-n", namespace,
                        "-o", "jsonpath={.status.phase}"])
    except RuntimeError:
        return "Unknown"


def wait_for_pod(pod_name, namespace, phase="Succeeded",
                 timeout=7200, poll=10):
    t0 = time.time()
    while True:
        current = get_pod_phase(pod_name, namespace)
        if current == phase:
            return time.time() - t0
        if current == "Failed":
            raise RuntimeError(f"Pod {pod_name} Failed")
        if time.time() - t0 > timeout:
            raise TimeoutError(
                f"Pod {pod_name} timed out after {timeout}s (phase={current})")
        time.sleep(poll)


def get_pod_timestamps(pod_name, namespace):
    try:
        out = kubectl([
            "get", "pod", pod_name, "-n", namespace, "-o",
            "jsonpath={.status.startTime}|"
            "{.status.containerStatuses[0].state.terminated.finishedAt}"
        ])
        parts = out.split("|")

        def parse_ts(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(
                    s.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None

        return {
            "start_ts":  parse_ts(parts[0]) if parts else None,
            "finish_ts": parse_ts(parts[1]) if len(parts) > 1 else None,
        }
    except RuntimeError:
        return {"start_ts": None, "finish_ts": None}


def get_logs(pod_name, namespace, container=None):
    args = ["logs", pod_name, "-n", namespace]
    if container:
        args += ["-c", container]
    try:
        return kubectl(args)
    except RuntimeError:
        return ""


# ── Log parsing ────────────────────────────────────────────────────────────────

def parse_timing(log_text, prefix):
    """Extract TIMING key=value lines from logs matching [prefix]."""
    result = {}
    for line in log_text.splitlines():
        m = re.search(rf'\[{re.escape(prefix)}\] TIMING (\w+)=([0-9.]+)', line)
        if m:
            result[m.group(1)] = float(m.group(2))
    return result


def parse_summary(log_text, prefix):
    """Extract SUMMARY JSON line from logs matching [prefix]."""
    for line in log_text.splitlines():
        m = re.search(rf'\[{re.escape(prefix)}\] SUMMARY (.+)$', line)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
    return {}


# ── Manifest patching ──────────────────────────────────────────────────────────

def _resize_worker_blocks(manifest, n_workers):
    """
    The manifest ships with a fixed set of worker pod documents (worker-0..3).
    A sweep with a different worker count must produce EXACTLY n_workers worker
    pods, or the leader (told N_WORKERS=n) waits forever for workers that were
    never created (e.g. --n-workers 8 with only 4 blocks hangs the gang).

    Rebuild the doc list as: [non-worker docs...] + n_workers clones of the
    worker-0 template, each with its WORKER_INDEX and name corrected. Keeps the
    leader/PodGroup/other docs untouched and order stable (leader first).
    """
    docs = manifest.split("\n---\n")
    worker_tmpl = None
    kept = []
    for d in docs:
        # a worker doc is identified by the worker-0 name or role: worker +
        # WORKER_INDEX; use worker-0 as the canonical template and drop the rest.
        if re.search(r'name:\s*qaoa-gang-worker-0\b', d):
            worker_tmpl = d
            continue
        if re.search(r'name:\s*qaoa-gang-worker-\d+\b', d):
            continue  # drop worker-1..N; we regenerate all workers from worker-0
        kept.append(d)
    if worker_tmpl is None:
        return manifest  # no worker template found; leave as-is

    workers = []
    for i in range(n_workers):
        w = worker_tmpl
        w = re.sub(r'name:\s*qaoa-gang-worker-0\b', f'name: qaoa-gang-worker-{i}', w)
        w = re.sub(r'(name:\s*WORKER_INDEX\s*\n\s*value:\s*")[^"]*(")',
                   rf'\g<1>{i}\g<2>', w)
        workers.append(w)
    return "\n---\n".join(kept + workers)


def patch_manifest(base, run_name, run_id, device_arn, backend_name, n_workers, n_shots,
                   n_nodes, seed, scheduler):
    """
    Patch a gang pipeline manifest with run-specific values.
    Replaces pod names, ARN, counts, and scheduler name.
    """
    # First make the manifest carry exactly n_workers worker pods (the file ships
    # with a fixed 4; a sweep needs the real count or the leader hangs).
    m = _resize_worker_blocks(base, n_workers)

    def set_env(text, var, val):
        return re.sub(
            rf'(name:\s*{var}\s*\n\s*value:\s*")[^"]*(")',
            rf'\g<1>{val}\g<2>',
            text
        )

    # Pod names: leader and workers
    m = re.sub(r'name:\s*qaoa-gang-leader',
               f'name: {run_name}-leader', m)
    for i in range(n_workers):  # rename exactly the workers we generated
        m = re.sub(rf'name:\s*qaoa-gang-worker-{i}\b',
                   f'name: {run_name}-worker-{i}', m)

    # Default condition: rename the native PodGroup and its references so
    # concurrent/sequential runs don't collide. (Fluence has no PodGroup in the
    # manifest — the webhook creates it from the group label.)
    m = m.replace('qaoa-gang-default', f'{run_name}-pg')

    # The native PodGroup's minCount must equal the gang size (leader + workers),
    # or gang scheduling is wrong: too high and a small gang never starts, too low
    # and it starts before all pods are ready. The manifest ships minCount:5 (for
    # 4 workers); set it to 1 + n_workers. (No-op on the fluence manifest, which
    # has no PodGroup — the webhook creates one with minCount:1.)
    m = re.sub(r'(minCount:\s*)\d+', rf'\g<1>{1 + n_workers}', m)

    # Group label — keep unique per run (fluence condition)
    m = m.replace(
        'fluence.flux-framework.org/group: qaoa-gang',
        f'fluence.flux-framework.org/group: {run_name}'
    )

    # Env vars. BRAKET_DEVICE only exists in the default manifest; set_env is a
    # no-op if the var is absent (fluence reads Fluence-injected FLUXION_ARN).
    m = set_env(m, "RUN_ID",        run_id)   # unique per run -> isolated S3 prefix, no cleanup needed
    # Region of the Braket device (result buckets are per-region). Derive from the
    # device ARN; simulators have an empty region field -> default us-east-1. Set
    # on ALL pods so leader and workers agree on the result bucket cross-region.
    braket_region = (device_arn.split(":")[3] or "us-east-1") if device_arn else "us-east-1"
    m = set_env(m, "BRAKET_REGION", braket_region)
    # Expected-workers annotation (fluence arm): N-1 gated workers the sidecar
    # waits for. No-op on the default manifest, which has no such annotation.
    m = re.sub(r'(fluence\.flux-framework\.org/expected-workers:\s*")[^"]*(")',
               rf'\g<1>{n_workers}\g<2>', m)
    # Pin Fluence's backend selection to this run's backend (fluence arm only;
    # no-op on the default manifest, which has no such annotation).
    m = re.sub(r'(fluence\.flux-framework\.org/require-backend:\s*")[^"]*(")',
               rf'\g<1>{backend_name}\g<2>', m)
    if scheduler == "default":
        m = set_env(m, "BRAKET_DEVICE", device_arn)
    m = set_env(m, "N_WORKERS",     n_workers)
    m = set_env(m, "N_SHOTS",       n_shots)
    m = set_env(m, "N_NODES",       n_nodes)
    m = set_env(m, "SEED",          seed)
    # Workers must wait as long as the leader's QPU task realistically takes —
    # on a queued device the queue wait IS the phenomenon under measurement, so
    # the 600s manifest default is far too short. Use a generous bound (default
    # 4h) overridable via FLUENCE_GANG_TIMEOUT_S. The worker logs a heartbeat
    # each minute, so a long (correct) wait is visible rather than looking hung.
    gang_timeout = os.environ.get("FLUENCE_GANG_TIMEOUT_S", "14400")
    m = set_env(m, "LEADER_TIMEOUT_S", gang_timeout)
    m = set_env(m, "WORKER_TIMEOUT_S", gang_timeout)

    # Worker WORKER_INDEX values stay as-is (0,1,2,3) in the template

    return m


# ── Gang run ───────────────────────────────────────────────────────────────────

def run_gang(backend_name, device_arn, scheduler, n_workers, n_shots,
             n_nodes, seed, manifest,
             namespace, out_dir, keep_pods):

    # run_name names the Kubernetes objects (pods, PodGroup). It is intentionally
    # STABLE for a given config and is recreated each run (delete_gang_pods cleans
    # the prior pods), so reusing the name is correct and avoids orphaned pods.
    run_name = (f"gang-{backend_name}-{scheduler}-w{n_workers}-"
                f"q{n_nodes}").replace("_", "-")
    # run_id is the S3 isolation key, and MUST be unique per run — otherwise two
    # runs of the same config share the fluence-gang/<run_id>/ prefix and a
    # worker can read a previous run's leader-ready marker (stale-completion bug),
    # which is exactly why runs used to require manual S3 cleanup between them.
    run_id = f"{run_name}-{time.strftime('%Y%m%dT%H%M%S')}"
    print(f"\n[orchestrator] {'─'*60}")
    print(f"[orchestrator] backend={backend_name}  scheduler={scheduler}  "
          f"n_workers={n_workers}  n_nodes={n_nodes}  shots={n_shots}")
    print(f"[orchestrator] run_id={run_id}")

    # Clean up previous run
    delete_gang_pods(run_name, n_workers, namespace, scheduler)
    time.sleep(2)

    # Patch the single condition manifest (PodGroup + leader + workers for
    # default; leader + workers for fluence).
    gang_yaml = patch_manifest(
        manifest, run_name, run_id, device_arn, backend_name,
        n_workers, n_shots, n_nodes, seed, scheduler
    )

    t_batch_start = time.time()
    print(f"[orchestrator] Submitting at {datetime.now().isoformat()}")

    # Submit (one document stream — PodGroup ordering handled by kubectl apply).
    apply_manifest(gang_yaml, namespace)

    leader_name  = f"{run_name}-leader"
    worker_names = [f"{run_name}-worker-{i}" for i in range(n_workers)]
    all_names    = [leader_name] + worker_names

    # The orchestrator must wait at least as long as the in-pod gang timeout, or
    # it gives up before the pods do (defeating the long in-pod wait for a queued
    # QPU). Add a small margin for pod startup/teardown around the in-pod bound.
    orch_timeout = int(os.environ.get("FLUENCE_GANG_TIMEOUT_S", "14400")) + 300

    print(f"[orchestrator] Waiting for leader: {leader_name}")
    leader_failed = False
    try:
        wait_for_pod(leader_name, namespace, timeout=orch_timeout)
        print(f"[orchestrator]   ✓ {leader_name}")
    except (RuntimeError, TimeoutError) as e:
        print(f"[orchestrator]   ✗ {leader_name}: {e}", file=sys.stderr)
        leader_failed = True

    print(f"[orchestrator] Waiting for {n_workers} workers...")
    worker_failures = []
    for wname in worker_names:
        try:
            wait_for_pod(wname, namespace, timeout=orch_timeout)
            print(f"[orchestrator]   ✓ {wname}")
        except (RuntimeError, TimeoutError) as e:
            print(f"[orchestrator]   ✗ {wname}: {e}", file=sys.stderr)
            worker_failures.append(wname)

    t_batch_end  = time.time()
    batch_wall   = t_batch_end - t_batch_start

    # ── Collect timing from logs ────────────────────────────────────────────

    # Leader logs
    leader_logs    = get_logs(leader_name, namespace, "gang")
    leader_timing  = parse_timing(leader_logs, "gang-leader")
    leader_summary = parse_summary(leader_logs, "gang-leader")
    leader_ts      = get_pod_timestamps(leader_name, namespace)

    # Worker logs
    worker_data = []
    for i, wname in enumerate(worker_names):
        wlogs   = get_logs(wname, namespace, "gang")
        wtiming = parse_timing(wlogs, f"gang-worker-{i}")
        wts     = get_pod_timestamps(wname, namespace)
        worker_data.append({
            "name":    wname,
            "index":   i,
            "timing":  wtiming,
            "pod_ts":  wts,
            "failed":  wname in worker_failures,
        })

    # ── Derive key metrics ──────────────────────────────────────────────────

    qpu_queue_wait = (
        leader_timing.get("result_ts", 0) -
        leader_timing.get("queued_ts", 0)
        if "result_ts" in leader_timing and "queued_ts" in leader_timing
        else None
    )

    # Worker idle = time from worker pod start to seeing leader-ready signal
    # This is the wasted classical compute time
    worker_idle_times = []
    for wd in worker_data:
        if wd["failed"]:
            continue
        t_wstart = wd["pod_ts"]["start_ts"]
        t_ready  = wd["timing"].get("leader_ready_seen_ts")
        if t_wstart and t_ready:
            worker_idle_times.append(t_ready - t_wstart)

    total_worker_idle   = sum(worker_idle_times)
    avg_worker_idle     = (total_worker_idle / len(worker_idle_times)
                           if worker_idle_times else None)
    # Node-seconds: each idle worker holds one node
    worker_node_seconds = total_worker_idle  # already summed across workers

    leader_wall = (
        (leader_ts["finish_ts"] or 0) - (leader_ts["start_ts"] or 0)
        if leader_ts["finish_ts"] and leader_ts["start_ts"] else None
    )

    # ── Build result row ────────────────────────────────────────────────────

    row = {
        # Identity
        "run_name":                 run_name,
        "backend":                  backend_name,
        "scheduler":                scheduler,
        "n_workers":                n_workers,
        "n_nodes":                  n_nodes,
        "n_shots":                  n_shots,
        "seed":                     seed,
        # Batch timing
        "batch_wall_s":             round(batch_wall, 3),
        "leader_wall_s":            round(leader_wall, 3) if leader_wall else "",
        # QPU timing
        "qpu_queue_wait_s":         round(qpu_queue_wait, 3) if qpu_queue_wait else "",
        "leader_submit_to_result_s": round(
            leader_timing.get("result_ts", 0) -
            leader_timing.get("submit_ts", 0), 3)
            if "result_ts" in leader_timing and "submit_ts" in leader_timing
            else "",
        # THE KEY METRIC: wasted classical compute
        "total_worker_idle_s":      round(total_worker_idle, 3),
        "avg_worker_idle_s":        round(avg_worker_idle, 3) if avg_worker_idle else "",
        "worker_node_seconds":      round(worker_node_seconds, 3),
        "n_workers_completed":      n_workers - len(worker_failures),
        "leader_failed":            leader_failed,
        # Quality
        "cost_aggregated":          leader_summary.get("cost_aggregated", ""),
        "workers_completed":        leader_summary.get("workers_completed", ""),
        "timestamp":                datetime.now(timezone.utc).isoformat(),
    }

    # Per-worker idle times as individual columns
    for i in range(n_workers):
        row[f"worker_{i}_idle_s"] = (
            round(worker_idle_times[i], 3)
            if i < len(worker_idle_times) else ""
        )

    # Write CSV
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out = Path(out_dir) / f"combined-{backend_name}-{scheduler}-{ts}.csv"
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
    print(f"[orchestrator] → {out}")

    print(f"\n[orchestrator] Results for {run_name}:")
    print(f"  QPU queue wait:        {row['qpu_queue_wait_s']}s")
    print(f"  Total worker idle:     {row['total_worker_idle_s']}s")
    print(f"  Worker node-seconds:   {row['worker_node_seconds']}")
    print(f"  Batch wall time:       {row['batch_wall_s']}s")

    if not keep_pods:
        delete_gang_pods(run_name, n_workers, namespace, scheduler)

    return row


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Gang scheduling experiment — Fluence vs default",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate pipeline end-to-end (free, no queue wait)
  python3 run_experiment.py --backend sv1 --schedulers default fluence

  # Single scheduler
  python3 run_experiment.py --backend sv1 --scheduler default
  python3 run_experiment.py --backend sv1 --scheduler fluence

  # Vary worker count
  python3 run_experiment.py --backend sv1 --n-workers 2 4 8

  # Real QPU — demonstrates actual node-second savings
  python3 run_experiment.py --backend iqm_garnet --n-shots 100

  # Full comparison sweep
  python3 run_experiment.py --backend iqm_garnet --schedulers default fluence --n-shots 100

  # List backends
  python3 run_experiment.py --list-backends
        """
    )

    parser.add_argument("--backend",     default="sv1",
                        help="Backend name (default: sv1)")
    parser.add_argument("--scheduler",   default=None,
                        help="Scheduler: default or fluence (use --schedulers for both)")
    parser.add_argument("--schedulers",  nargs="+",
                        choices=["default", "fluence"],
                        default=None,
                        help="Run both schedulers (overrides --scheduler)")
    parser.add_argument("--n-workers",   nargs="+", type=int, default=[4],
                        help="Worker counts to sweep (default: 4)")
    parser.add_argument("--n-shots",     type=int, default=1000,
                        help="Shots per task (default: 1000)")
    parser.add_argument("--repeat",      type=int, default=1,
                        help="Repeat each configuration N times for mean±stdev "
                             "(default: 1)")
    parser.add_argument("--n-nodes",     nargs="+", type=int, default=[10],
                        help="Problem sizes in qubits (default: 10)")
    parser.add_argument("--seed",        type=int, default=42)
    parser.add_argument("--namespace",   default="default")
    parser.add_argument("--out",         default="results/",
                        help="Output directory (default: results/)")
    parser.add_argument("--keep-pods",   action="store_true")
    parser.add_argument("--list-backends", action="store_true")

    args = parser.parse_args()

    if args.list_backends:
        list_backends()
        sys.exit(0)

    if args.backend not in BACKENDS:
        print(f"ERROR: unknown backend '{args.backend}'", file=sys.stderr)
        list_backends()
        sys.exit(1)

    # Determine which schedulers to run
    if args.schedulers:
        schedulers = args.schedulers
    elif args.scheduler:
        schedulers = [args.scheduler]
    else:
        schedulers = ["default"]

    backend_info = BACKENDS[args.backend]
    device_arn   = backend_info["arn"]

    # Cost warning
    if backend_info["is_qpu"]:
        cost = 0.30 + args.n_shots * backend_info.get("cost_per_shot", 0)
        total = cost * len(schedulers) * len(args.n_workers) * len(args.n_nodes)
        print(f"\n[orchestrator] ⚠️  Cost estimate: ~${cost:.2f}/run × "
              f"{len(schedulers)*len(args.n_workers)*len(args.n_nodes)} runs "
              f"= ~${total:.2f} total")
        print(f"               Backend: {args.backend} ({backend_info['cost_note']})")
        response = input("               Continue? [y/N] ").strip().lower()
        if response != "y":
            print("Aborted.")
            sys.exit(0)

    # Resolve manifests
    repo_root = Path(__file__).parent.parent.parent

    def get_manifest(scheduler):
        # One file per condition now (PodGroup + leader + workers for default;
        # leader + workers for fluence — the webhook creates the PodGroup).
        base = repo_root / "pods" / scheduler / "gang"
        return (base / "pipeline-gang.yaml").read_text()

    print(f"\n[orchestrator] Starting experiment 2 — gang scheduling")
    print(f"  backend    : {args.backend}")
    print(f"  schedulers : {schedulers}")
    print(f"  n_workers  : {args.n_workers}")
    print(f"  n_nodes    : {args.n_nodes}")
    print(f"  n_shots    : {args.n_shots}")
    print(f"  output     : {args.out}")

    all_rows = []
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    for scheduler in schedulers:
        manifest = get_manifest(scheduler)
        for n_nodes in args.n_nodes:
            for n_workers in args.n_workers:
                for rep in range(1, args.repeat + 1):
                    if args.repeat > 1:
                        print(f"[orchestrator] — repeat {rep}/{args.repeat} "
                              f"(scheduler={scheduler} n_workers={n_workers} "
                              f"n_nodes={n_nodes})")
                    try:
                        row = run_gang(
                            backend_name    = args.backend,
                            device_arn      = device_arn,
                            scheduler       = scheduler,
                            n_workers       = n_workers,
                            n_shots         = args.n_shots,
                            n_nodes         = n_nodes,
                            seed            = args.seed,
                            manifest        = manifest,
                            namespace       = args.namespace,
                            out_dir         = args.out,
                            keep_pods       = args.keep_pods,
                        )
                        row["repeat"] = rep
                        all_rows.append(row)
                    except Exception as e:
                        print(f"[orchestrator] FAILED scheduler={scheduler} "
                              f"n_workers={n_workers} n_nodes={n_nodes} "
                              f"rep={rep}: {e}", file=sys.stderr)

    # Write combined CSV
    if all_rows:
        combined = Path(args.out) / f"combined-{args.backend}-{ts}.csv"
        # Rows from different configurations have different per-worker columns
        # (worker_0..N_idle_s scales with n_workers), so the fieldnames must be
        # the UNION of all rows' keys, not just the first row's — otherwise a
        # later wider row (e.g. w8 after w2) is rejected by DictWriter. Preserve
        # first-seen order, and keep the variable worker_* columns grouped/sorted
        # at the end for readability.
        base_keys, worker_keys, seen = [], set(), set()
        for r in all_rows:
            for k in r.keys():
                if k in seen:
                    continue
                seen.add(k)
                if re.match(r"worker_\d+_idle_s$", k):
                    worker_keys.add(k)
                else:
                    base_keys.append(k)
        ordered_worker = sorted(worker_keys, key=lambda k: int(k.split("_")[1]))
        fieldnames = base_keys + ordered_worker
        with open(combined, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n[orchestrator] Combined → {combined}")
        print(f"[orchestrator] Total runs: {len(all_rows)}")

        # Print summary table
        print(f"\n{'scheduler':<12} {'backend':<18} {'workers':<8} "
              f"{'qpu_wait_s':<12} {'worker_idle_s':<15} {'node_seconds':<14}")
        print("-" * 80)
        for r in all_rows:
            print(f"{r['scheduler']:<12} {r['backend']:<18} {r['n_workers']:<8} "
                  f"{str(r['qpu_queue_wait_s']):<12} "
                  f"{str(r['total_worker_idle_s']):<15} "
                  f"{str(r['worker_node_seconds']):<14}")

        # Aggregated mean ± stdev per configuration (only meaningful with repeats).
        import statistics
        groups = {}
        for r in all_rows:
            key = (r["scheduler"], r["n_workers"], r["n_nodes"])
            groups.setdefault(key, []).append(r)

        if any(len(v) > 1 for v in groups.values()):
            agg_rows = []
            print(f"\n[orchestrator] Aggregated over repeats (mean ± stdev):")
            print(f"\n{'scheduler':<12} {'workers':<8} {'nodes':<7} {'n':<4} "
                  f"{'qpu_wait_s':<20} {'worker_idle_s':<22}")
            print("-" * 80)

            def ms(values):
                m = statistics.mean(values)
                s = statistics.stdev(values) if len(values) > 1 else 0.0
                return m, s

            for (sched, nw, nn), rows in sorted(groups.items()):
                qm, qs = ms([r["qpu_queue_wait_s"] for r in rows])
                im, isd = ms([r["total_worker_idle_s"] for r in rows])
                nm, nsd = ms([r["worker_node_seconds"] for r in rows])
                print(f"{sched:<12} {nw:<8} {nn:<7} {len(rows):<4} "
                      f"{f'{qm:.2f} ± {qs:.2f}':<20} "
                      f"{f'{im:.2f} ± {isd:.2f}':<22}")
                agg_rows.append({
                    "scheduler": sched, "backend": args.backend,
                    "n_workers": nw, "n_nodes": nn, "n_repeats": len(rows),
                    "qpu_wait_s_mean": round(qm, 3),  "qpu_wait_s_stdev": round(qs, 3),
                    "worker_idle_s_mean": round(im, 3), "worker_idle_s_stdev": round(isd, 3),
                    "node_seconds_mean": round(nm, 3),  "node_seconds_stdev": round(nsd, 3),
                })

            agg_path = Path(args.out) / f"aggregated-{args.backend}-{ts}.csv"
            with open(agg_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
                w.writeheader()
                w.writerows(agg_rows)
            print(f"\n[orchestrator] Aggregated → {agg_path}")


if __name__ == "__main__":
    main()
