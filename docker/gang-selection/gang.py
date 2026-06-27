#!/usr/bin/env python3
"""
gang.py — Unified gang workflow container for Fluence quantum experiments.

Role is determined by FLUENCE_ROLE (injected by the Fluence webhook from
the fluence.flux-framework.org/role annotation), falling back to GANG_ROLE:
  leader  — submits QAOA circuit, writes S3 leader-ready signal, aggregates
  worker  — polls S3 for leader-ready, fetches result, processes shot partition

Environment variables (all roles):
  GANG_ROLE          "leader" or "worker" (required)
  AWS_ACCESS_KEY_ID  }
  AWS_SECRET_ACCESS_KEY  } AWS credentials
  AWS_DEFAULT_REGION }
  S3_BUCKET          override default Braket result bucket

Leader-specific:
  WORKSPACE          shared emptyDir from initContainers (default: /workspace)
  BRAKET_DEVICE      device ARN, used in the DEFAULT (non-Fluence) condition where
                     the pod selects its own device and Fluence does not place it
  FLUXION_ARN        device ARN injected by Fluence when it PLACES the quantum
                     resource (takes precedence over BRAKET_DEVICE); the matched
                     backend NAME arrives separately as FLUXION_BACKEND
  N_SHOTS            shots per task (default: 1000)
  N_WORKERS          number of worker pods to wait for (default: 4)
  (no timeout)       leader/workers wait indefinitely — a QPU queue can take
                     30-60+ min; we never abort a healthy gang on a clock.

Worker-specific:
  WORKER_INDEX       this worker's index 0-based (default: 0)
  N_WORKERS          total number of workers (default: 4)
  FLUENCE_QUANTUM_JOB_ID  vendor-neutral job id injected by the Fluence sidecar
                     via the quantum-job-id annotation / downward API
                     (optional — discovered from S3 if not set)

S3 coordination paths (derived from task_id):
  fluence-gang/<task_id>/leader-ready     written by leader when QPU done
  fluence-gang/<task_id>/worker-<i>.json  written by each worker
  fluence-gang/<task_id>/final.json       written by leader after aggregation
"""

import json
import os
import math
import random
import sys
import time
from datetime import datetime, timezone

import boto3


# ── S3 key helpers ─────────────────────────────────────────────────────────────

def _run_prefix():
    # Per-run S3 prefix so a run's pods can never match a previous run's
    # objects. RUN_ID is injected by the orchestrator (unique per run); falls
    # back to a stable default only for ad-hoc single runs.
    return f"fluence-gang/{os.environ.get('RUN_ID', 'default')}"

def key_leader_ready(task_id):
    return f"{_run_prefix()}/leader-ready"

def key_worker(task_id, worker_index):
    return f"{_run_prefix()}/worker-{worker_index}.json"

def key_final(task_id):
    return f"{_run_prefix()}/final.json"


# ── Shared helpers ─────────────────────────────────────────────────────────────

SV1_ARN = "arn:aws:braket:::device/quantum-simulator/amazon/sv1"

def log(role, msg):
    print(f"[gang-{role}] {msg}", flush=True)


def get_s3_bucket(s3, sts, region):
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        account_id = sts.get_caller_identity()["Account"]
        bucket = f"amazon-braket-{region}-{account_id}"
    return bucket


def compute_maxcut_cost(counts, edges, n_qubits):
    total_shots = sum(counts.values())
    if total_shots == 0:
        return 0.0
    total_cost = 0.0
    for bitstring, shots in counts.items():
        bs = bitstring.zfill(n_qubits)
        bits = [int(b) for b in bs]
        cut_value = sum(w for u, v, w in edges if bits[u] != bits[v])
        total_cost += shots * cut_value
    return total_cost / total_shots


def build_circuit(n_qubits, edges, gammas, betas):
    from braket.circuits import Circuit
    circ = Circuit()
    for i in range(n_qubits):
        circ.h(i)
    for layer in range(len(gammas)):
        gamma, beta = gammas[layer], betas[layer]
        for u, v, _ in edges:
            circ.cnot(u, v)
            circ.rz(v, gamma)
            circ.cnot(u, v)
        for i in range(n_qubits):
            circ.rx(i, 2 * beta)
    return circ


# ── Leader ─────────────────────────────────────────────────────────────────────

def _bootstrap_problem_if_missing(workspace, lg):
    """Generate /workspace/problem.json and params.json in-process if absent, so
    this image is self-contained (no problem-generator / transpiler init
    containers, no shared workspace volume required). Mirrors
    docker/problem-generator/generate.py and the param init in
    docker/transpiler/transpile.py. No-op when the files already exist."""
    os.makedirs(workspace, exist_ok=True)
    prob_path = os.path.join(workspace, "problem.json")
    params_path = os.path.join(workspace, "params.json")
    if os.path.exists(prob_path) and os.path.exists(params_path):
        return

    n_nodes = int(os.environ.get("N_NODES", 10))
    k = int(os.environ.get("K_REGULAR", 3))
    seed = int(os.environ.get("SEED", 42))
    p = int(os.environ.get("P_LAYERS", 1))
    lg(f"bootstrap: generating problem in-process (n_nodes={n_nodes} k={k} "
       f"seed={seed} p={p})")

    if n_nodes * k % 2 != 0:
        raise ValueError(f"n*k must be even (n={n_nodes}, k={k})")
    if k >= n_nodes:
        raise ValueError(f"k must be < n (k={k}, n={n_nodes})")
    rng = random.Random(seed)
    edges = None
    for _ in range(100):
        stubs = []
        for node in range(n_nodes):
            stubs.extend([node] * k)
        rng.shuffle(stubs)
        es, valid = set(), True
        for i in range(0, len(stubs), 2):
            u, v = stubs[i], stubs[i + 1]
            if u == v or (min(u, v), max(u, v)) in es:
                valid = False
                break
            es.add((min(u, v), max(u, v)))
        if valid:
            edges = [(u, v, 1.0) for u, v in sorted(es)]
            break
    if edges is None:
        raise RuntimeError(f"could not build a {k}-regular graph on {n_nodes} "
                           f"nodes after 100 attempts")
    with open(prob_path, "w") as f:
        json.dump({"n_nodes": n_nodes, "edges": edges, "seed": seed, "k": k,
                   "n_qubits": n_nodes}, f, indent=2)

    prng = random.Random(seed)
    gammas = [prng.uniform(0.1, math.pi - 0.1) for _ in range(p)]
    betas = [prng.uniform(0.1, math.pi / 2 - 0.1) for _ in range(p)]
    with open(params_path, "w") as f:
        json.dump({"gammas": gammas, "betas": betas, "p": p}, f, indent=2)
    lg(f"bootstrap: wrote problem.json ({len(edges)} edges, {n_nodes} qubits) "
       f"+ params.json (p={p})")


def run_leader():
    workspace      = os.environ.get("WORKSPACE", "/workspace")
    # Fluence injects the matched backend as FLUXION_BACKEND (the backend NAME,
    # e.g. "sv1") and its attributes as FLUXION_<KEY>. The device ARN comes from
    # the graph's `arn` attribute, injected as FLUXION_ARN — that is what the
    # Braket SDK needs. In the DEFAULT (non-Fluence) condition no FLUXION_* env is
    # injected and the pod uses its own BRAKET_DEVICE.
    device_arn     = os.environ.get("FLUXION_ARN") or \
                     os.environ.get("BRAKET_DEVICE", SV1_ARN)
    placed_by_fluence = bool(os.environ.get("FLUXION_ARN"))
    n_shots        = int(os.environ.get("N_SHOTS", 1000))
    n_workers      = int(os.environ.get("N_WORKERS", 4))

    t_start = time.time()
    lg = lambda msg: log("leader", msg)

    lg(f"device={device_arn} n_shots={n_shots} n_workers={n_workers}")
    lg(f"TIMING leader_start_ts={t_start:.6f}")

    # Self-contained: if the workspace files are absent (no problem-generator /
    # transpiler init containers), generate them in-process. No-op when they
    # already exist, so this image also works behind the original init pipeline.
    _bootstrap_problem_if_missing(workspace, lg)

    # Load problem and params (from init containers, or the bootstrap above)
    with open(f"{workspace}/problem.json") as f:
        problem = json.load(f)
    with open(f"{workspace}/params.json") as f:
        params = json.load(f)

    n_qubits = problem["n_qubits"]
    edges    = problem["edges"]
    gammas   = params["gammas"]
    betas    = params["betas"]
    lg(f"Problem: n_qubits={n_qubits} edges={len(edges)}")

    # Build and submit circuit
    from braket.aws import AwsDevice
    circ   = build_circuit(n_qubits, edges, gammas, betas)
    t_submit = time.time()
    lg(f"TIMING submit_ts={t_submit:.6f}")
    device = AwsDevice(device_arn)
    # Tag the task with our pod UID so the Fluence sidecar can find it
    # (sidecar searches search_quantum_tasks and matches the `fluence-pod-uid`
    # tag client-side, then ungates the workers when this task reaches the front
    # of the queue). We set the tag EXPLICITLY rather than relying solely on the
    # PYTHONPATH interceptor monkeypatch — the SDK accepts a `tags` dict on run(),
    # and an explicit tag is robust to interceptor import-ordering issues. If the
    # interceptor also fires it sets the same key to the same value (harmless).
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    run_kwargs = {"shots": n_shots}
    if pod_uid:
        run_kwargs["tags"] = {"fluence-pod-uid": pod_uid}
        lg(f"tagging task fluence-pod-uid={pod_uid}")
    else:
        lg("WARNING: FLUENCE_POD_UID unset — task will be untagged; the sidecar "
           "cannot find it and workers will not ungate")
    task   = device.run(circ, **run_kwargs)
    t_queued = time.time()
    lg(f"TIMING queued_ts={t_queued:.6f}")
    lg(f"Task ARN: {task.id}")

    result = task.result()
    t_result = time.time()
    lg(f"TIMING result_ts={t_result:.6f}")

    if result is None:
        lg(f"ERROR: task {task.id} returned no result (state={task.state()})")
        sys.exit(1)

    counts = result.measurement_counts
    cost   = compute_maxcut_cost(counts, edges, n_qubits)
    lg(f"Cost={cost:.6f} elapsed={t_result-t_submit:.2f}s")

    task_id = task.id.split("/")[-1]
    region  = os.environ.get("BRAKET_REGION") or \
              device_arn.split(":")[3] or \
              os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if not region:
        region = "us-east-1"

    s3  = boto3.client("s3", region_name=region)
    sts = boto3.client("sts", region_name=region)
    s3_bucket = get_s3_bucket(s3, sts, region)
    lg(f"S3 bucket: {s3_bucket}  task_id: {task_id}")

    # Write leader-ready signal — includes task ARN for workers without Fluence
    ready = {
        "task_arn":  task.id,
        "task_id":   task_id,
        "s3_bucket": s3_bucket,
        "n_shots":   n_shots,
        "n_qubits":  n_qubits,
        "n_workers": n_workers,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(Bucket=s3_bucket, Key=key_leader_ready(task_id),
                  Body=json.dumps(ready).encode())
    lg(f"Wrote leader-ready: s3://{s3_bucket}/{key_leader_ready(task_id)}")
    lg(f"TIMING leader_ready_ts={time.time():.6f}")

    # Wait for worker partial results
    lg(f"Waiting for {n_workers} workers (no timeout)...")
    received = set()
    wait_start = time.time()
    next_heartbeat = wait_start + 60
    # No deadline: wait for every worker to finish. Workers run after ungate, so
    # this is normally quick, but processing time varies — we never abort a
    # healthy gang on a clock.
    while len(received) < n_workers:
        for i in range(n_workers):
            if i in received:
                continue
            try:
                s3.head_object(Bucket=s3_bucket, Key=key_worker(task_id, i))
                received.add(i)
                lg(f"  worker {i} done ({len(received)}/{n_workers})")
            except Exception:
                pass
        if len(received) < n_workers:
            if time.time() >= next_heartbeat:
                waited = int(time.time() - wait_start)
                lg(f"  still waiting for workers "
                   f"({len(received)}/{n_workers} done, {waited // 60}m{waited % 60}s elapsed)")
                next_heartbeat += 60
            time.sleep(5)

    t_workers_done = time.time()
    lg(f"TIMING workers_done_ts={t_workers_done:.6f}")
    lg(f"Workers completed: {len(received)}/{n_workers}")

    # Aggregate
    merged = {}
    for i in range(n_workers):
        try:
            obj = s3.get_object(Bucket=s3_bucket, Key=key_worker(task_id, i))
            pr  = json.loads(obj["Body"].read())
            for bs, cnt in pr.get("counts", {}).items():
                merged[bs] = merged.get(bs, 0) + cnt
        except Exception as e:
            lg(f"  WARNING: could not read worker {i}: {e}")

    merged_cost = compute_maxcut_cost(merged, edges, n_qubits) if merged else cost
    best_bs = max(merged or counts, key=(merged or counts).get)

    t_end = time.time()
    final = {
        "task_arn":           task.id,
        "task_id":            task_id,
        "backend":            device_arn,
        "n_qubits":           n_qubits,
        "n_shots":            n_shots,
        "n_workers":          n_workers,
        "workers_completed":  len(received),
        "cost_leader":        cost,
        "cost_aggregated":    merged_cost,
        "best_bitstring":     best_bs,
        "qpu_queue_wait_s":   t_result - t_queued,
        "leader_elapsed_s":   t_end - t_start,
        "worker_wait_s":      t_workers_done - t_result,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(Bucket=s3_bucket, Key=key_final(task_id),
                  Body=json.dumps(final, indent=2).encode())

    with open(f"{workspace}/final.json", "w") as f:
        json.dump(final, f, indent=2)

    lg(f"SUMMARY {json.dumps({'task_id': task_id, 'cost': merged_cost, 'workers_completed': len(received), 'total_elapsed_s': t_end-t_start, 'qpu_queue_wait_s': final['qpu_queue_wait_s']})}")
    lg(f"Done in {t_end-t_start:.1f}s")


# ── Worker ─────────────────────────────────────────────────────────────────────

def run_worker():
    # Fluence injects the (vendor-neutral) job id as FLUENCE_QUANTUM_JOB_ID via
    # the quantum-job-id annotation at ungate time. For Braket the job id is the
    # task ARN. Without Fluence the worker discovers the task from S3 instead.
    task_arn       = os.environ.get("FLUENCE_QUANTUM_JOB_ID", "")
    worker_index   = int(os.environ.get("WORKER_INDEX", 0))
    n_workers      = int(os.environ.get("N_WORKERS", 4))

    t_start = time.time()
    lg = lambda msg: log(f"worker-{worker_index}", msg)

    lg(f"task_arn={'set' if task_arn else 'not set (will discover)'} "
       f"index={worker_index}/{n_workers}")
    lg(f"TIMING worker_start_ts={t_start:.6f}")

    # Region of the result bucket. Prefer BRAKET_REGION (set per-run by the
    # orchestrator from the device ARN, on every pod) so leader and workers agree
    # even for cross-region devices; fall back to the device ARN, then env.
    device_arn = os.environ.get("FLUXION_ARN") or \
                 os.environ.get("BRAKET_DEVICE", SV1_ARN)
    region = os.environ.get("BRAKET_REGION") or \
             device_arn.split(":")[3] or \
             os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if not region:
        region = "us-east-1"
    s3  = boto3.client("s3", region_name=region)
    sts = boto3.client("sts", region_name=region)
    s3_bucket = get_s3_bucket(s3, sts, region)

    # Poll for the leader-ready signal at this run's deterministic key. Because
    # the key is scoped to RUN_ID, the worker can only ever see THIS run's
    # leader — never a stale object from a previous run (which previously caused
    # workers to "complete" instantly and report zero idle time).
    lg(f"Waiting for leader-ready (no timeout — QPU queue can take 30-60+ min)...")
    lg(f"  key=s3://{s3_bucket}/{key_leader_ready(None)}")
    leader_info = None
    wait_start = time.time()
    next_heartbeat = wait_start + 60

    # No deadline: the leader's quantum task may sit in the vendor queue for a
    # long time, and a worker must wait for it however long it takes. We poll
    # until leader-ready appears, logging a heartbeat so a long (healthy) wait is
    # not mistaken for a hang.
    while True:
        try:
            obj = s3.get_object(Bucket=s3_bucket, Key=key_leader_ready(None))
            leader_info = json.loads(obj["Body"].read())
            break
        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            if "NoSuchKey" not in str(e) and "Not Found" not in str(e):
                lg(f"  poll error: {e}")
        if time.time() >= next_heartbeat:
            waited = int(time.time() - wait_start)
            lg(f"  still waiting for leader-ready ({waited // 60}m{waited % 60}s elapsed)")
            next_heartbeat += 60
        time.sleep(5)

    t_ready = time.time()
    task_arn = leader_info["task_arn"]
    task_id  = task_arn.split("/")[-1]
    lg(f"Leader ready. task_id={task_id}")
    lg(f"TIMING leader_ready_seen_ts={t_ready:.6f}")

    n_qubits = leader_info["n_qubits"]

    # Fetch result
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    from braket.aws import AwsQuantumTask
    result = AwsQuantumTask(arn=task_arn).result()
    if result is None:
        lg("ERROR: task result is None")
        sys.exit(1)

    t_fetched = time.time()
    lg(f"TIMING result_fetched_ts={t_fetched:.6f}")

    # Partition shots: worker i takes every n_workers-th shot
    all_shots = []
    for bs, cnt in result.measurement_counts.items():
        all_shots.extend([bs] * cnt)
    my_shots = all_shots[worker_index::n_workers]
    my_counts = {}
    for bs in my_shots:
        my_counts[bs] = my_counts.get(bs, 0) + 1

    lg(f"Shot partition: {len(my_shots)}/{len(all_shots)}")

    t_processed = time.time()
    lg(f"TIMING processing_done_ts={t_processed:.6f}")

    partial = {
        "worker_index":      worker_index,
        "n_workers":         n_workers,
        "task_arn":          task_arn,
        "task_id":           task_id,
        "shots_total":       len(all_shots),
        "shots_assigned":    len(my_shots),
        "counts":            my_counts,
        "worker_start_ts":   t_start,
        "leader_ready_ts":   t_ready,
        "fetch_elapsed_s":   t_fetched - t_ready,
        "process_elapsed_s": t_processed - t_fetched,
        "idle_before_ready_s": t_ready - t_start,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(Bucket=s3_bucket, Key=key_worker(task_id, worker_index),
                  Body=json.dumps(partial, indent=2).encode())

    t_end = time.time()
    lg(f"TIMING worker_done_ts={t_end:.6f}")
    lg(f"Done in {t_end-t_start:.1f}s "
       f"(idle={t_ready-t_start:.1f}s "
       f"fetch={t_fetched-t_ready:.1f}s "
       f"process={t_processed-t_fetched:.1f}s)")


# ── Entrypoint ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    fluence_role = os.environ.get("FLUENCE_ROLE")
    gang_role = os.environ.get("GANG_ROLE")
    role = (fluence_role or gang_role or "worker").lower()
    source = ("FLUENCE_ROLE" if fluence_role else
              "GANG_ROLE" if gang_role else "default(worker)")
    print(f"[gang-selection] role={role} (from {source}; "
          f"FLUENCE_ROLE={fluence_role!r} GANG_ROLE={gang_role!r})", flush=True)
    if not fluence_role and not gang_role:
        print("[gang-selection] WARNING: no role signal (FLUENCE_ROLE/GANG_ROLE "
              "both unset) — defaulting to worker. A would-be leader will hang; "
              "rebuild/redeploy Fluence (FLUENCE_ROLE injection) or set GANG_ROLE.",
              flush=True)
    if role == "leader":
        run_leader()
    elif role == "worker":
        run_worker()
    else:
        print(f"ERROR: role must be 'leader' or 'worker' (FLUENCE_ROLE/GANG_ROLE), got '{role}'")
        sys.exit(1)
