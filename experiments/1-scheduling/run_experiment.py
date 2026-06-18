#!/usr/bin/env python3
"""
run_experiment.py — Two-queue characterization experiment.

Runs ONE backend at a time. Start with the cheapest (sv1) and only
proceed to more expensive backends once you are confident the pipeline
works correctly.

Usage:
  # free, deterministic
  python3 run_experiment.py --backend sv1

  # Once sv1 is working, TN1
  python3 run_experiment.py --backend tn1

  # Cheapest real QPU
  python3 run_experiment.py --backend rigetti_cepheus --n-shots 100 --max-iter 5 --counts 1 5

  # AHS pipeline (different manifest)
  python3 run_experiment.py --backend ahs_local --pipeline ahs

  # See all options
  python3 run_experiment.py --help

Backend ladder (cheapest to most expensive per run at 100 shots, 5 iters):
  sv1             ~$0.01/run   simulator, always start here
  tn1             ~$0.05/run   simulator
  ahs_local       free         local AHS simulator
  aquila          ~$1.30/run   QuEra neutral atom, 1 task
  rigetti_cepheus ~$1.73/run   superconducting QPU
  iqm_garnet      ~$2.23/run   superconducting QPU
  iqm_emerald     ~$2.29/run   superconducting QPU
  aqt_ibex        ~$13.25/run  trapped ion QPU
  ionq_forte      ~$41/run     trapped ion QPU  ← expensive, use --max-iter 1

Timing model per pod:
  t_pod_start      pod created (from kubectl)
  t_submit         gateway called device.run()     [from TIMING log line]
  t_queued         task object returned to client  [from TIMING log line]
  t_result         result received                 [from TIMING log line]
  t_pod_finish     pod Succeeded                   (from kubectl)

  qpu_queue_wait_s  = t_result - t_queued   (time waiting for QPU/simulator)
  classical_idle_s  = sum(t_result - t_submit) across all iterations
  pod_wall_s        = t_pod_finish - t_pod_start
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

# Backend registry mirrors fluence-resources.yaml.
# qrmi_type drives which pipeline manifest is used:
#   braket-gate -> pods/<scheduler>/pipeline.yaml
#   braket-ahs  -> pods/<scheduler>/pipeline-ahs.yaml
# When running with Fluence, arn is injected via FLUXION_ARN by the webhook.
# When running without Fluence (default scheduler), arn is passed as BRAKET_DEVICE.
BACKENDS = {
    "sv1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
        "qrmi_type": "braket-gate",
        "cost_note": "~$0.075/min — start here",
    },
    "tn1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/tn1",
        "qrmi_type": "braket-gate",
        "cost_note": "~$0.275/min",
    },
    "dm1": {
        "arn":       "arn:aws:braket:::device/quantum-simulator/amazon/dm1",
        "qrmi_type": "braket-gate",
        "cost_note": "~$0.075/min",
    },
    "ahs_local": {
        "arn":       "local",
        "qrmi_type": "braket-ahs",
        "cost_note": "free — local AHS simulator",
    },
    "aquila": {
        "arn":       "arn:aws:braket:us-east-1::device/qpu/quera/Aquila",
        "qrmi_type": "braket-ahs",
        "cost_note": "$0.30/task + $0.01/shot",
    },
    "rigetti_cepheus": {
        "arn":       "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q",
        "qrmi_type": "braket-gate",
        "cost_note": "$0.30/task + $0.000425/shot",
    },
    "iqm_garnet": {
        "arn":       "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet",
        "qrmi_type": "braket-gate",
        "cost_note": "$0.30/task + $0.00145/shot",
    },
    "iqm_emerald": {
        "arn":       "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald",
        "qrmi_type": "braket-gate",
        "cost_note": "$0.30/task + $0.00160/shot",
    },
    "aqt_ibex": {
        "arn":       "arn:aws:braket:eu-north-1::device/qpu/aqt/IBEX-Q1",
        "qrmi_type": "braket-gate",
        "cost_note": "$0.30/task + $0.02350/shot",
    },
    "ionq_forte": {
        "arn":       "arn:aws:braket:us-east-1::device/qpu/ionq/Forte-Enterprise-1",
        "qrmi_type": "braket-gate",
        "cost_note": "$0.30/task + $0.08000/shot — use --max-iter 1",
    },
}


def list_backends():
    print("\nAvailable backends (cheapest to most expensive):")
    print(f"  {'name':<20} {'qrmi_type':<12} {'cost'}")
    print(f"  {'-'*20} {'-'*12} {'-'*40}")
    for name, info in BACKENDS.items():
        print(f"  {name:<20} {info['qrmi_type']:<12} {info['cost_note']}")
    print()


# ── kubectl helpers ────────────────────────────────────────────────────────────

def kubectl(args, check=True, capture=True):
    cmd = ["kubectl"] + args
    result = subprocess.run(cmd, capture_output=capture, text=True)
    if check and result.returncode != 0:
        print(f"[orchestrator] kubectl error: {result.stderr.strip()}", file=sys.stderr)
        raise RuntimeError(f"kubectl {' '.join(args)} failed")
    return result.stdout.strip() if capture else None


def apply_pod(manifest_yaml, namespace):
    result = subprocess.run(
        ["kubectl", "apply", "-n", namespace, "-f", "-"],
        input=manifest_yaml, text=True, capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {result.stderr.strip()}")
    return result.stdout.strip()


def delete_pods(pod_names, namespace):
    if not pod_names:
        return
    kubectl(["delete", "pod", "-n", namespace,
             "--ignore-not-found=true"] + pod_names)


def get_pod_phase(pod_name, namespace):
    try:
        return kubectl(["get", "pod", pod_name, "-n", namespace,
                        "-o", "jsonpath={.status.phase}"])
    except RuntimeError:
        return "Unknown"


def wait_for_pod(pod_name, namespace, phase="Succeeded", timeout=3600, poll=5):
    t0 = time.time()
    while True:
        current = get_pod_phase(pod_name, namespace)
        if current == phase:
            return time.time() - t0
        if current in ("Failed",):
            raise RuntimeError(f"Pod {pod_name} entered Failed state")
        if time.time() - t0 > timeout:
            raise TimeoutError(f"Pod {pod_name} timed out after {timeout}s "
                               f"(current phase: {current})")
        time.sleep(poll)


def get_pod_timestamps(pod_name, namespace):
    out = kubectl([
        "get", "pod", pod_name, "-n", namespace,
        "-o", "jsonpath={.status.startTime}|"
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
        "pod_start_ts":  parse_ts(parts[0]) if len(parts) > 0 else None,
        "pod_finish_ts": parse_ts(parts[1]) if len(parts) > 1 else None,
    }


def get_container_logs(pod_name, namespace, container):
    try:
        return kubectl(["logs", pod_name, "-n", namespace, "-c", container])
    except RuntimeError:
        return ""


# ── Log parsing ────────────────────────────────────────────────────────────────

def parse_timing_events(log_text):
    """Parse TIMING lines from gateway logs into per-iteration dicts."""
    events = []
    current = {}
    for line in log_text.splitlines():
        m = re.search(r'\[(?:braket|ahs)-gateway\] TIMING (\w+)=([0-9.]+)', line)
        if not m:
            continue
        key, val = m.group(1), float(m.group(2))
        if key == "submit_ts":
            if current:
                events.append(current)
            current = {"submit_ts": val}
        else:
            current[key] = val
    if current:
        events.append(current)
    return events


def parse_summary(log_text, container="optimizer"):
    """Extract the SUMMARY JSON line from optimizer or mis-postprocessor logs."""
    tag = "optimizer" if container == "optimizer" else "mis-postprocessor"
    for line in log_text.splitlines():
        m = re.search(rf'\[{tag}\] SUMMARY (.+)$', line)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
    return {}


# ── Manifest patching ──────────────────────────────────────────────────────────

def patch_manifest(base, pod_name, seed, device_arn, n_shots, max_iter, n_nodes):
    """Patch a pipeline manifest with experiment-specific values."""
    m = base

    # Pod name (first occurrence only)
    m = re.sub(r'(name:\s*)qaoa-pipeline(?:-ahs)?',
               f'\\g<1>{pod_name}', m, count=1)

    # Env vars
    def replace_env(text, var_name, new_val):
        return re.sub(
            rf'(name:\s*{var_name}\s*\n\s*value:\s*")[^"]*(")',
            f'\\g<1>{new_val}\\g<2>',
            text
        )

    m = replace_env(m, "SEED",          seed)
    m = replace_env(m, "BRAKET_DEVICE", device_arn)
    m = replace_env(m, "N_SHOTS",       n_shots)
    m = replace_env(m, "MAX_ITER",      max_iter)
    m = replace_env(m, "N_NODES",       n_nodes)
    m = replace_env(m, "N_ATOMS",       n_nodes)  # AHS pipeline uses N_ATOMS
    return m


# ── Batch runner ───────────────────────────────────────────────────────────────

def run_batch(backend_name, device_arn, pipeline, n_pods, base_manifest,
              scheduler, namespace, seed_base, n_shots, max_iter, n_nodes,
              out_dir, keep_pods):

    print(f"\n[orchestrator] backend={backend_name}  N={n_pods}  "
          f"n_nodes/atoms={n_nodes}  shots={n_shots}  max_iter={max_iter}")

    suffix = f"{backend_name}-n{n_pods}-q{n_nodes}"
    pod_names = [f"qaoa-exp-{suffix}-{i}".replace('_', '-') for i in range(n_pods)]

    # Clean up any leftover pods
    delete_pods(pod_names, namespace)
    time.sleep(1)

    # Submit all pods at once
    t_batch_start = time.time()
    for i, pod_name in enumerate(pod_names):
        manifest = patch_manifest(
            base_manifest, pod_name,
            seed=seed_base + i,
            device_arn=device_arn,
            n_shots=n_shots,
            max_iter=max_iter,
            n_nodes=n_nodes,
        )
        apply_pod(manifest, namespace)

    print(f"[orchestrator] Submitted {n_pods} pods at {datetime.now().isoformat()}")

    # Wait for all pods
    failed = []
    for pod_name in pod_names:
        try:
            wait_for_pod(pod_name, namespace)
            print(f"[orchestrator]   ✓ {pod_name}")
        except (RuntimeError, TimeoutError) as e:
            print(f"[orchestrator]   ✗ {pod_name}: {e}", file=sys.stderr)
            failed.append(pod_name)

    t_batch_end = time.time()
    batch_wall  = t_batch_end - t_batch_start
    print(f"[orchestrator] Batch wall time: {batch_wall:.1f}s  "
          f"failures: {len(failed)}/{n_pods}")

    # Collect data
    rows = []
    summary_container = "optimizer" if pipeline == "gate" else "mis-postprocessor"
    gateway_container  = "braket-gateway" if pipeline == "gate" else "ahs-gateway"

    for pod_name in pod_names:
        if pod_name in failed:
            continue

        pod_ts  = get_pod_timestamps(pod_name, namespace)
        gw_log  = get_container_logs(pod_name, namespace, gateway_container)
        sum_log = get_container_logs(pod_name, namespace, summary_container)

        timing  = parse_timing_events(gw_log)
        summary = parse_summary(sum_log, container=summary_container)

        n_iters          = len(timing)
        total_qpu_wait   = sum(
            e.get("result_ts", 0) - e.get("queued_ts", 0)
            for e in timing if "result_ts" in e and "queued_ts" in e
        )
        total_circuit_t  = sum(
            e.get("result_ts", 0) - e.get("submit_ts", 0)
            for e in timing if "result_ts" in e and "submit_ts" in e
        )
        pod_wall = (
            (pod_ts["pod_finish_ts"] or 0) - (pod_ts["pod_start_ts"] or 0)
            if pod_ts["pod_finish_ts"] and pod_ts["pod_start_ts"] else None
        )

        rows.append({
            "backend":              backend_name,
            "pipeline":             pipeline,
            "scheduler":            scheduler,
            "n_pods_batch":         n_pods,
            "n_nodes_or_atoms":     n_nodes,
            "n_shots":              n_shots,
            "max_iter":             max_iter,
            "pod_name":             pod_name,
            "seed":                 summary.get("seed", ""),
            # Timing
            "pod_wall_s":           round(pod_wall, 3) if pod_wall else "",
            "batch_wall_s":         round(batch_wall, 3),
            "n_iterations":         n_iters,
            "total_qpu_wait_s":     round(total_qpu_wait, 3),
            "total_circuit_time_s": round(total_circuit_t, 3),
            "avg_qpu_wait_s":       round(total_qpu_wait / n_iters, 3) if n_iters else "",
            "avg_circuit_time_s":   round(total_circuit_t / n_iters, 3) if n_iters else "",
            # Quality
            "approximation_ratio":  summary.get("approximation_ratio", ""),
            "converged":            summary.get("converged", ""),
            "total_elapsed_s":      summary.get("total_elapsed_s", ""),
            "timestamp":            datetime.now(timezone.utc).isoformat(),
        })

    # Write CSV
    if rows:
        out_path = Path(out_dir) / f"{backend_name}-n{n_pods}-q{n_nodes}.csv"
        write_header = not out_path.exists()
        with open(out_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if write_header:
                writer.writeheader()
            writer.writerows(rows)
        print(f"[orchestrator] → {out_path}")

    if not keep_pods:
        delete_pods(pod_names, namespace)

    return rows


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Two-queue characterization — one backend at a time",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start here — free simulator
  python3 run_experiment.py --backend sv1

  # Vary concurrency
  python3 run_experiment.py --backend sv1 --counts 1 5 10 20

  # Vary qubit count
  python3 run_experiment.py --backend sv1 --qubit-sizes 8 12 20

  # Cheapest real QPU, minimal iterations
  python3 run_experiment.py --backend rigetti_cepheus --counts 1 5 --max-iter 5

  # Expensive QPU — single iteration only
  python3 run_experiment.py --backend ionq_forte --counts 1 --max-iter 1

  # AHS pipeline
  python3 run_experiment.py --backend ahs_local --counts 1 5 10

  # List all backends with cost info
  python3 run_experiment.py --list-backends
        """
    )

    parser.add_argument("--backend",      default="sv1",
                        help="Backend name (default: sv1)")
    parser.add_argument("--counts",       nargs="+", type=int, default=[1, 5, 10, 20],
                        help="Concurrent pod counts to sweep (default: 1 5 10 20)")
    parser.add_argument("--qubit-sizes",  nargs="+", type=int, default=[10],
                        help="Problem sizes in qubits/atoms (default: 10)")
    parser.add_argument("--n-shots",      type=int, default=100,
                        help="Shots per Braket task (default: 100)")
    parser.add_argument("--max-iter",     type=int, default=5,
                        help="Max COBYLA iterations per pod (default: 5)")
    parser.add_argument("--scheduler",    default="default",
                        help="Scheduler variant: default or fluence (default: default)")
    parser.add_argument("--namespace",    default="default")
    parser.add_argument("--seed-base",    type=int, default=100)
    parser.add_argument("--out",          default="results/",
                        help="Output directory (default: results/)")
    parser.add_argument("--keep-pods",    action="store_true",
                        help="Do not delete pods after run")
    parser.add_argument("--manifest",     default=None,
                        help="Override path to pipeline.yaml")
    parser.add_argument("--list-backends", action="store_true",
                        help="List backends with cost info and exit")

    args = parser.parse_args()

    if args.list_backends:
        list_backends()
        sys.exit(0)

    if args.backend not in BACKENDS:
        print(f"ERROR: unknown backend '{args.backend}'", file=sys.stderr)
        list_backends()
        sys.exit(1)

    backend_info = BACKENDS[args.backend]
    device_arn   = backend_info["arn"]
    pipeline     = "gate" if backend_info["qrmi_type"] == "braket-gate" else "ahs"

    # Resolve manifest
    if args.manifest:
        manifest_path = Path(args.manifest)
    else:
        manifest_name = "pipeline.yaml" if pipeline == "gate" else "pipeline-ahs.yaml"
        manifest_path = (
            Path(__file__).parent.parent.parent
            / "pods" / args.scheduler / manifest_name
        )

    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    base_manifest = manifest_path.read_text()
    Path(args.out).mkdir(parents=True, exist_ok=True)

    # Cost estimate
    if pipeline == "gate" and args.backend not in ("sv1", "tn1", "dm1"):
        cost_per_run = (
            args.max_iter * (0.30 + args.n_shots *
            {"rigetti_cepheus": 0.000425, "iqm_garnet": 0.00145,
             "iqm_emerald": 0.00160, "aqt_ibex": 0.02350,
             "ionq_forte": 0.08000}.get(args.backend, 0))
        )
        total_runs = sum(args.counts) * len(args.qubit_sizes)
        print(f"\n[orchestrator] ⚠️  Cost estimate: ~${cost_per_run:.2f}/run × "
              f"{total_runs} runs = ~${cost_per_run * total_runs:.2f} total")
        print(f"               Backend: {args.backend} ({backend_info['cost_note']})")
        response = input("               Continue? [y/N] ").strip().lower()
        if response != "y":
            print("Aborted.")
            sys.exit(0)

    print(f"\n[orchestrator] Starting experiment")
    print(f"  backend     : {args.backend} ({device_arn})")
    print(f"  pipeline    : {pipeline} ({backend_info["qrmi_type"]})")
    print(f"  scheduler   : {args.scheduler}")
    print(f"  counts      : {args.counts}")
    print(f"  qubit sizes : {args.qubit_sizes}")
    print(f"  shots       : {args.n_shots}")
    print(f"  max_iter    : {args.max_iter}")
    print(f"  manifest    : {manifest_path}")
    print(f"  output      : {args.out}")

    all_rows = []
    for n_nodes in args.qubit_sizes:
        for n_pods in args.counts:
            try:
                rows = run_batch(
                    backend_name=args.backend,
                    device_arn=device_arn,
                    pipeline=pipeline,
                    n_pods=n_pods,
                    base_manifest=base_manifest,
                    scheduler=args.scheduler,
                    namespace=args.namespace,
                    seed_base=args.seed_base,
                    n_shots=args.n_shots,
                    max_iter=args.max_iter,
                    n_nodes=n_nodes,
                    out_dir=args.out,
                    keep_pods=args.keep_pods,
                )
                all_rows.extend(rows)
            except Exception as e:
                print(f"[orchestrator] FAILED n_pods={n_pods} "
                      f"n_nodes={n_nodes}: {e}", file=sys.stderr)

    # Write combined CSV across all batches in this run
    if all_rows:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        combined = Path(args.out) / f"combined-{args.backend}-{ts}.csv"
        with open(combined, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n[orchestrator] Combined → {combined}")
        print(f"[orchestrator] Total pods measured: {len(all_rows)}")


if __name__ == "__main__":
    main()
