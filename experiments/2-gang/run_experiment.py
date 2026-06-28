#!/usr/bin/env python3
"""
run_experiment.py — Gang scheduling experiment (Experiment 2): Fluence vs default.

A gang of N identical pods runs ONE quantum task and shares its result: completion
index 0 is the PRODUCER (submits the QPU task), the other N-1 are CONSUMERS (fetch
the shared result). The question is what the N-1 consumers cost while the QPU queue
drains. Two arms, same workload, differing only in scheduler:

  fluence   coordination=shared. Fluence places the QPU work (injects FLUXION_ARN
            from a backend NAME), elects the producer, and GATES the consumers
            until the task is at queue position ~1. Gated consumers hold no
            classical node, so consumer idle ≈ 0.
  default   native PodGroup gang on the default scheduler. No gating: all N pods
            start together and the N-1 consumers idle (burning classical
            node-time) while the QPU queue drains, discovering the producer's task
            via S3. The producer names its device MANUALLY (BRAKET_DEVICE) because
            there is no Fluxion to inject it.

THE KEY METRIC — total_consumer_idle_s: summed over consumers of
(result_ready_ts − consumer_start_ts), i.e. node-seconds a consumer holds without
the result. fluence ≈ 0; default ≈ (N−1) × T_queue. This is the wasted classical
compute Fluence reclaims by gang-gating.

BACKENDS. The fluence arm sets only a backend NAME (require-backend); Fluxion
matches the graph and injects the ARN — the experiment never names an ARN there.
The default arm has no Fluxion, so the orchestrator resolves the NAME -> device
ARN from the hardcoded BACKENDS map and stamps it onto BRAKET_DEVICE. Both arms
therefore hit the same device from a single --backend. --device-arn overrides the
default arm for an off-map / one-off device.

Usage:
  # Offline: render the patched manifests (no cluster)
  python3 run_experiment.py --schedulers fluence --n-consumers 4 --render

  # SV1 simulator, both arms (mechanism check; no real queue)
  python3 run_experiment.py --backend sv1 --schedulers default fluence

  # DM1 simulator, both arms (default-arm ARN resolved from the name)
  python3 run_experiment.py --backend dm1 --schedulers default fluence

  # Sweep consumer count, repeat for mean ± stdev
  python3 run_experiment.py --backend sv1 --schedulers default fluence \\
      --n-consumers 2 4 8 --repeat 5

  # A real QPU (per-task charges); both arms hit it from the one name
  python3 run_experiment.py --backend iqm_garnet --schedulers default fluence --n-shots 100
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

SCHEDULERS = ("default", "fluence")

# Hardcoded backend NAME -> device ARN. The fluence arm never uses these (it sets
# only require-backend: <name> and Fluxion injects the ARN from the resource
# graph). They exist solely so the DEFAULT (non-Fluence) arm can stamp the right
# BRAKET_DEVICE for whichever --backend you pick — no graph parsing, one place to
# edit. Keep this in sync with hack/fluence-resources.yaml. The ARN encodes the
# region, so gang.py derives the region from it; no separate region field needed.
BACKENDS = {
    # simulators (no real queue; billed per-minute)
    "sv1": "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
    "tn1": "arn:aws:braket:::device/quantum-simulator/amazon/tn1",
    "dm1": "arn:aws:braket:::device/quantum-simulator/amazon/dm1",
    # real QPUs (per-task charges; real queues) — region is baked into the ARN
    "rigetti_cepheus": "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q",
    "iqm_garnet":      "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet",
    "iqm_emerald":     "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald",
}
SV1_ARN = BACKENDS["sv1"]
# Simulators get no real queue and only per-minute billing -> used for the cost
# guardrail. Anything not here is treated as a real (billable, queued) QPU.
KNOWN_SIMULATORS = {"sv1", "dm1"}



# ── kubectl helpers ──────────────────────────────────────────────────────────────

def kubectl(args, check=True):
    r = subprocess.run(["kubectl"] + args, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"[orchestrator] kubectl error: {r.stderr.strip()}", file=sys.stderr)
        raise RuntimeError(f"kubectl {' '.join(args)} failed")
    return r.stdout.strip()


def apply_manifest(manifest_yaml, namespace):
    r = subprocess.run(["kubectl", "apply", "-n", namespace, "-f", "-"],
                       input=manifest_yaml, text=True, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {r.stderr.strip()}")
    return r.stdout.strip()


def delete_run(run_name, namespace):
    # Deleting the Job cascades to its pods. The default arm also has a native
    # PodGroup (<run>-pg) the orchestrator created; the fluence arm's PodGroups are
    # created and reaped by Fluence's reconciler, so we never touch those.
    kubectl(["delete", "job", "-n", namespace, "--ignore-not-found=true",
             "--wait=true", run_name], check=False)
    kubectl(["delete", "podgroup.scheduling.k8s.io", "-n", namespace,
             "--ignore-not-found=true", f"{run_name}-pg"], check=False)


def parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def get_job_succeeded(job_name, namespace):
    try:
        out = kubectl(["get", "job", job_name, "-n", namespace,
                       "-o", "jsonpath={.status.succeeded}"])
        return int(out) if out else 0
    except (RuntimeError, ValueError):
        return 0


def wait_for_job(job_name, namespace, completions, timeout, poll=10):
    """Wait until the Job has `completions` succeeded pods. timeout<=0 waits
    indefinitely (a real QPU queue can run for days); failures still abort early.
    Heartbeats every ~5 min so a long, correct wait looks alive, not hung."""
    t0 = time.time()
    infinite = timeout is None or timeout <= 0
    last_beat = t0
    while True:
        if get_job_succeeded(job_name, namespace) >= completions:
            return time.time() - t0
        try:
            failed = kubectl(["get", "job", job_name, "-n", namespace,
                              "-o", "jsonpath={.status.failed}"])
            if failed and int(failed) > 0:
                raise RuntimeError(f"Job {job_name} has {failed} failed pod(s)")
        except ValueError:
            pass
        now = time.time()
        if not infinite and now - t0 > timeout:
            raise TimeoutError(f"Job {job_name} timed out after {timeout}s "
                               f"(succeeded={get_job_succeeded(job_name, namespace)}"
                               f"/{completions})")
        if now - last_beat >= 300:
            print(f"[orchestrator]   … still waiting on {job_name} "
                  f"({int(now - t0)}s; succeeded "
                  f"{get_job_succeeded(job_name, namespace)}/{completions}) — "
                  f"QPU queue wait is expected to be long", flush=True)
            last_beat = now
        time.sleep(poll)


def list_job_pods(job_name, namespace):
    out = kubectl(["get", "pods", "-n", namespace,
                   "-l", f"batch.kubernetes.io/job-name={job_name}", "-o", "json"])
    pods = []
    for item in json.loads(out).get("items", []):
        meta = item.get("metadata", {})
        idx = meta.get("labels", {}).get("batch.kubernetes.io/job-completion-index")
        status = item.get("status", {})
        finish = None
        for c in status.get("containerStatuses", []):
            if c.get("name") == "gang":
                finish = parse_ts(c.get("state", {}).get("terminated", {}).get("finishedAt"))
        pods.append({
            "name":   meta.get("name"),
            "index":  int(idx) if idx is not None and idx.isdigit() else None,
            "start_ts":  parse_ts(status.get("startTime")),
            "finish_ts": finish,
        })
    return pods


def get_logs(pod_name, namespace, container="gang"):
    try:
        return kubectl(["logs", pod_name, "-n", namespace, "-c", container])
    except RuntimeError:
        return ""


def parse_timing(log_text):
    """gang.py logs '[gang-producer]'/'[gang-consumer-N] TIMING key=ts'."""
    out = {}
    for line in log_text.splitlines():
        m = re.search(r'\[gang-[^\]]+\] TIMING (\w+)=([0-9.]+)', line)
        if m:
            out[m.group(1)] = float(m.group(2))
    return out


def parse_summary(log_text):
    for line in log_text.splitlines():
        m = re.search(r'\[gang-[^\]]+\] SUMMARY (.+)$', line)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
    return {}


# ── Metric derivation (PURE — unit-tested offline) ───────────────────────────────

def derive_metrics(pods):
    """
    pods: list of dicts with keys index, start_ts, finish_ts, timing{}, summary{}.

    Consumer idle = result_ready_ts − consumer_start_ts (node-seconds a consumer is
    up but has no result yet). total_consumer_idle_s sums it across consumers:
    fluence ≈ 0 (gated), default ≈ (N−1) × T_queue.
    """
    producer = None
    consumer_idles = []
    per_consumer = []
    for p in pods:
        role = (p["summary"].get("role")
                or ("producer" if p["index"] == 0 else "consumer"))
        if role == "producer":
            producer = p
            continue
        cs = p["timing"].get("consumer_start_ts")
        rr = p["timing"].get("result_ready_ts")
        idle = (rr - cs) if (cs and rr) else None
        if idle is not None:
            consumer_idles.append(idle)
        per_consumer.append({"index": p["index"], "idle_s":
                             round(idle, 3) if idle is not None else ""})

    total_idle = sum(consumer_idles)
    mean_idle = (total_idle / len(consumer_idles)) if consumer_idles else None
    q_wait = None
    if producer:
        pt = producer["timing"]
        if "result_ts" in pt and "submit_ts" in pt:
            q_wait = pt["result_ts"] - pt["submit_ts"]
    return {
        "total_consumer_idle_s": round(total_idle, 3),
        "mean_consumer_idle_s":  round(mean_idle, 3) if mean_idle is not None else "",
        "qpu_queue_wait_s":      round(q_wait, 3) if q_wait is not None else "",
        "n_consumers_observed":  len(consumer_idles),
        "_per_consumer":         per_consumer,
    }


# ── Manifest patching ────────────────────────────────────────────────────────────

def set_env(text, var, val):
    return re.sub(rf'(name:\s*{var}\s*\n\s*value:\s*")[^"]*(")', rf'\g<1>{val}\g<2>', text)


def patch_manifest(scheduler, base, run_name, run_id, backend, device_arn,
                   n, n_shots, n_nodes, seed):
    m = base
    # Names / counts. N = total pods; consumers = N-1.
    m = re.sub(r'name:\s*qaoa-gang\b(?!-)', f'name: {run_name}', m)  # Job name
    m = m.replace('fluence.flux-framework.org/group: qaoa-gang',
                  f'fluence.flux-framework.org/group: {run_name}')
    m = re.sub(r'(completions:\s*)\d+', rf'\g<1>{n}', m)
    m = re.sub(r'(parallelism:\s*)\d+', rf'\g<1>{n}', m)
    m = re.sub(r'(fluence\.flux-framework\.org/group-size:\s*")[^"]*(")', rf'\g<1>{n}\g<2>', m)
    # default arm: native PodGroup minCount = N, and unique PodGroup name per run.
    m = re.sub(r'(minCount:\s*)\d+', rf'\g<1>{n}', m)
    m = m.replace('qaoa-gang-default', f'{run_name}-pg')
    # fluence arm: only the backend NAME annotation (Fluxion injects the ARN).
    m = re.sub(r'(fluence\.flux-framework\.org/require-backend:\s*)"?[^"\s#]+"?',
               rf'\g<1>{backend}', m)
    # default arm: the device ARN is named manually (no Fluxion to inject it).
    if scheduler == "default":
        m = set_env(m, "BRAKET_DEVICE", device_arn)
    # Env values shared by both arms.
    m = set_env(m, "RUN_ID", run_id)
    m = set_env(m, "N_CONSUMERS", n - 1)
    m = set_env(m, "N_SHOTS", n_shots)
    m = set_env(m, "N_NODES", n_nodes)
    m = set_env(m, "SEED", seed)
    # Waits default to indefinite / very long: a real QPU queue can run for days.
    # CONSUMER_TIMEOUT_S<=0 = wait forever for the producer's task id (default arm);
    # POLL_TIMEOUT_S bounds the Braket result poll in both arms.
    m = set_env(m, "CONSUMER_TIMEOUT_S", os.environ.get("FLUENCE_GANG_TIMEOUT_S", "0"))
    m = set_env(m, "POLL_TIMEOUT_S", os.environ.get("POLL_TIMEOUT_S", "2592000"))
    return m


# ── One arm run ──────────────────────────────────────────────────────────────────

def run_arm(scheduler, backend, device_arn, n, n_shots, n_nodes, seed,
            manifest, namespace, out_dir, keep):
    run_name = f"gang-{backend}-{scheduler}-n{n}-q{n_nodes}".replace("_", "-")
    run_id = f"{run_name}-{time.strftime('%Y%m%dT%H%M%S')}"
    print(f"\n[orchestrator] {'-'*60}")
    print(f"[orchestrator] scheduler={scheduler} backend={backend} N={n} "
          f"(1 producer + {n-1} consumers) shots={n_shots} run_id={run_id}")

    delete_run(run_name, namespace)
    time.sleep(2)
    yaml_doc = patch_manifest(scheduler, manifest, run_name, run_id, backend,
                              device_arn, n, n_shots, n_nodes, seed)
    t0 = time.time()
    apply_manifest(yaml_doc, namespace)

    timeout = int(os.environ.get("FLUENCE_GANG_TIMEOUT_S", "0"))  # <=0 = wait indefinitely
    try:
        wait_for_job(run_name, namespace, completions=n, timeout=timeout)
        job_ok = True
    except (RuntimeError, TimeoutError) as e:
        print(f"[orchestrator]   x job {run_name}: {e}", file=sys.stderr)
        job_ok = False
    batch_wall = time.time() - t0

    pods = []
    for p in list_job_pods(run_name, namespace):
        logs = get_logs(p["name"], namespace)
        p["timing"] = parse_timing(logs)
        p["summary"] = parse_summary(logs)
        pods.append(p)
    metrics = derive_metrics(pods)

    row = {
        "run_name": run_name, "scheduler": scheduler, "backend": backend,
        "n_pods": n, "n_consumers": n - 1, "n_shots": n_shots, "n_nodes": n_nodes,
        "seed": seed, "job_ok": job_ok, "batch_wall_s": round(batch_wall, 3),
        "total_consumer_idle_s": metrics["total_consumer_idle_s"],
        "mean_consumer_idle_s": metrics["mean_consumer_idle_s"],
        "qpu_queue_wait_s": metrics["qpu_queue_wait_s"],
        "n_consumers_observed": metrics["n_consumers_observed"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out = Path(out_dir) / f"combined-{backend}-{scheduler}-n{n}-{ts}.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader(); w.writerow(row)

    print(f"[orchestrator] -> {out}")
    print(f"  total consumer idle : {row['total_consumer_idle_s']}s  "
          f"(expect ~0 for fluence, ~(N-1)xT_queue for default)")
    print(f"  QPU queue wait      : {row['qpu_queue_wait_s']}s")
    if not keep:
        delete_run(run_name, namespace)
    return row


# ── Main ───────────────────────────────────────────────────────────────────────

def render_only(args):
    repo_root = Path(__file__).parent.parent.parent
    for scheduler in args.schedulers:
        base = (repo_root / "pods" / scheduler / "gang" / "pipeline-gang.yaml").read_text()
        for n in args.n_pods:
            doc = patch_manifest(scheduler, base,
                                 f"gang-{args.backend}-{scheduler}-n{n}".replace("_", "-"),
                                 "render-run", args.backend, args.device_arn,
                                 n, args.n_shots, args.n_nodes[0], args.seed)
            print(f"\n# ===== rendered: scheduler={scheduler} N={n} =====")
            print(doc)


def main():
    p = argparse.ArgumentParser(
        description="Gang scheduling experiment — Fluence vs default scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--backend", default="sv1", choices=sorted(BACKENDS),
                   help="backend NAME. Fluence arm sets require-backend=<name> "
                        "(Fluxion injects the ARN); the default arm's BRAKET_DEVICE "
                        "is resolved from this name via the BACKENDS map.")
    p.add_argument("--device-arn", default=None,
                   help="override the default-arm device ARN (otherwise resolved "
                        "from --backend). Use for an off-map / one-off device.")
    p.add_argument("--scheduler", choices=SCHEDULERS, default=None)
    p.add_argument("--schedulers", nargs="+", choices=SCHEDULERS, default=None,
                   help="run multiple scheduler arms (overrides --scheduler)")
    p.add_argument("--n-consumers", dest="n_consumers", nargs="+", type=int, default=[4],
                   help="consumer counts to sweep; total pods N = consumers + 1 "
                        "(default: 4 -> N=5)")
    p.add_argument("--n-shots", type=int, default=1000)
    p.add_argument("--n-nodes", nargs="+", type=int, default=[10])
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--namespace", default="default")
    p.add_argument("--out", default="results/")
    p.add_argument("--keep", action="store_true")
    p.add_argument("--render", action="store_true",
                   help="print patched manifests and exit (no cluster)")
    p.add_argument("--yes", action="store_true", help="skip the QPU cost confirmation")
    args = p.parse_args()

    # total pods per run = consumers + 1 producer
    args.n_pods = [c + 1 for c in args.n_consumers]
    args.schedulers = (args.schedulers or ([args.scheduler] if args.scheduler
                       else ["default", "fluence"]))

    # Default-arm device ARN: explicit override, else resolved from --backend so
    # both arms always hit the SAME device (no silent sv1 fallback).
    args.device_arn = args.device_arn or BACKENDS[args.backend]

    if args.render:
        render_only(args); sys.exit(0)

    # Cost guardrail: a real QPU (anything that isn't a known simulator, or an ARN
    # with /qpu/) incurs per-task charges and may queue for a long time.
    qpu = (args.backend not in KNOWN_SIMULATORS) or ("/qpu/" in args.device_arn)
    if qpu and not args.yes:
        runs = len(args.schedulers) * len(args.n_pods) * len(args.n_nodes) * args.repeat
        print(f"\n[orchestrator] WARNING: this looks like a real QPU. ~{runs} run(s), "
              f"one real task per run, will incur per-task charges. Check AWS Braket "
              f"pricing. (fluence backend={args.backend}, default device={args.device_arn})")
        if input("               Continue? [y/N] ").strip().lower() != "y":
            print("Aborted."); sys.exit(0)

    repo_root = Path(__file__).parent.parent.parent
    manifests = {s: (repo_root / "pods" / s / "gang" / "pipeline-gang.yaml").read_text()
                 for s in args.schedulers}

    print(f"\n[orchestrator] Experiment 2 — gang scheduling (consumer idle)")
    print(f"  schedulers : {args.schedulers}")
    print(f"  backend    : {args.backend}  (default-arm device: {args.device_arn})")
    print(f"  n_consumers: {args.n_consumers}  n_nodes: {args.n_nodes}  shots: {args.n_shots}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    all_rows = []
    for scheduler in args.schedulers:
        for n in args.n_pods:
            for rep in range(1, args.repeat + 1):
                try:
                    row = run_arm(scheduler, args.backend, args.device_arn, n,
                                  args.n_shots, args.n_nodes[0], args.seed,
                                  manifests[scheduler], args.namespace, args.out, args.keep)
                    row["repeat"] = rep
                    all_rows.append(row)
                except Exception as e:
                    print(f"[orchestrator] FAILED scheduler={scheduler} N={n} "
                          f"rep={rep}: {e}", file=sys.stderr)

    if all_rows:
        combined = Path(args.out) / f"combined-{args.backend}-{ts}.csv"
        fields = list(all_rows[0].keys())
        for r in all_rows:
            for k in r:
                if k not in fields:
                    fields.append(k)
        with open(combined, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, restval="")
            w.writeheader(); w.writerows(all_rows)
        print(f"\n[orchestrator] Combined -> {combined}  ({len(all_rows)} runs)")

        print(f"\n{'scheduler':<10} {'N':<4} {'consumer_idle_s':<16} {'T_queue':<9}")
        print("-" * 45)
        for r in all_rows:
            print(f"{r['scheduler']:<10} {r['n_pods']:<4} "
                  f"{str(r['total_consumer_idle_s']):<16} {str(r['qpu_queue_wait_s']):<9}")

        import statistics
        groups = {}
        for r in all_rows:
            groups.setdefault((r["scheduler"], r["n_pods"]), []).append(r)
        if any(len(v) > 1 for v in groups.values()):
            agg = []
            for (sch, n), rows in sorted(groups.items()):
                vals = [r["total_consumer_idle_s"] for r in rows]
                agg.append({"scheduler": sch, "backend": args.backend, "n_pods": n,
                            "n_consumers": n - 1, "n_repeats": len(rows),
                            "idle_mean_s": round(statistics.mean(vals), 3),
                            "idle_stdev_s": round(statistics.stdev(vals), 3)
                            if len(vals) > 1 else 0.0})
            agg_path = Path(args.out) / f"aggregated-{args.backend}-{ts}.csv"
            with open(agg_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(agg[0].keys()))
                w.writeheader(); w.writerows(agg)
            print(f"[orchestrator] Aggregated -> {agg_path}")


if __name__ == "__main__":
    main()
