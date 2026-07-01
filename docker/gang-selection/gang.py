#!/usr/bin/env python3
"""
gang.py — unified quantum gang container (producer/consumer; no leader/worker).

Built for Fluence's CURRENT producer/consumer model (no separate submitter pod).
A run is an Indexed Job of N pods that Fluence coordinates per coordination mode:

  coordination=shared (what this experiment uses):
    - PRODUCER  = the completion-index-0 pod. Fluence puts it in its own
      group-of-one <group>-producer, ungated, and it runs the SINGLE real submit
      to the selected backend, tagging the task with its pod-uid so the sidecar
      can find it, poll it, and ungate the consumers.
    - CONSUMERS = the other N-1 pods. Gated until the producer's task is ready,
      then each fetches THAT task's result by FLUENCE_QUANTUM_JOB_ID. A consumer
      never submits. So there is exactly ONE real submission per run.
    Fluence tells each pod which it is via FLUENCE_COORDINATION_ROLE.

  coordination=independent (Fluence default): every pod is its own producer
  (real submit, no gating). A lone quantum pod (no group) is also a producer.

So there is no leader/worker, no role annotation, and no S3 coordination: one
code path here that branches only on FLUENCE_COORDINATION_ROLE.

Backend honoring
----------------
Fluence injects the matched/pinned backend NAME as FLUXION_BACKEND and (when the
graph carries it) the device ARN as FLUXION_ARN, and it SKIPS any env already set
on the container. So the experiment bakes the SELECTED backend into the Job pod
template env for the selection arm; the producer (a normal Job pod) inherits it
directly and submits there. No Fluence change.

Environment
-----------
  FLUENCE_COORDINATION_ROLE  "consumer" => fetch by id; "producer"/unset => submit
  FLUENCE_QUANTUM_JOB_ID     the producer's task id, injected onto consumers
  FLUXION_BACKEND            selected/matched backend NAME (e.g. "sv1")
  FLUXION_ARN               device ARN, if the graph exposes it (else resolved here)
  FLUENCE_POD_UID           this pod's uid; tagged on the real task for discovery
  N_SHOTS                   shots for the real submission (default 1000)
  WORKSPACE                 scratch dir for the in-process problem bootstrap
  N_NODES/K_REGULAR/SEED/P_LAYERS  problem-generation params (bootstrap)
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION  Braket creds
"""

import json
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

SV1_ARN = "arn:aws:braket:::device/quantum-simulator/amazon/sv1"

# name -> ARN, mirroring cost-attributes.yaml. Lets a pod honor the SELECTED
# backend from FLUXION_BACKEND (the name Fluence reliably injects) even when the
# device ARN is not surfaced as FLUXION_ARN. Keep in sync with the attribute file.
NAME_TO_ARN = {
    "sv1": "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
    "dm1": "arn:aws:braket:::device/quantum-simulator/amazon/dm1",
    "tn1": "arn:aws:braket:::device/quantum-simulator/amazon/tn1",
    "rigetti_cepheus": "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q",
    "iqm_garnet": "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet",
    "iqm_emerald": "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald",
}


def log(msg):
    print(f"[gang] {msg}", flush=True)


# -- problem + circuit (self-contained; no init containers) ------------------

def _bootstrap_problem_if_missing(workspace):
    """Generate problem.json + params.json in-process if absent, so this image is
    self-contained. Deterministic from SEED, so the producer (and any pod that
    bootstraps) builds the SAME problem. No-op when the files already exist."""
    os.makedirs(workspace, exist_ok=True)
    prob_path = os.path.join(workspace, "problem.json")
    params_path = os.path.join(workspace, "params.json")
    if os.path.exists(prob_path) and os.path.exists(params_path):
        return

    n_nodes = int(os.environ.get("N_NODES", 10))
    k = int(os.environ.get("K_REGULAR", 3))
    seed = int(os.environ.get("SEED", 42))
    p = int(os.environ.get("P_LAYERS", 1))
    log(f"bootstrap: problem n_nodes={n_nodes} k={k} seed={seed} p={p}")
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
        raise RuntimeError(f"could not build a {k}-regular graph on {n_nodes} nodes")
    with open(prob_path, "w") as f:
        json.dump({"n_nodes": n_nodes, "edges": edges, "seed": seed, "k": k,
                   "n_qubits": n_nodes}, f, indent=2)
    prng = random.Random(seed)
    gammas = [prng.uniform(0.1, math.pi - 0.1) for _ in range(p)]
    betas = [prng.uniform(0.1, math.pi / 2 - 0.1) for _ in range(p)]
    with open(params_path, "w") as f:
        json.dump({"gammas": gammas, "betas": betas, "p": p}, f, indent=2)


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


def compute_maxcut_cost(counts, edges, n_qubits):
    total_shots = sum(counts.values())
    if total_shots == 0:
        return 0.0
    total = 0.0
    for bitstring, shots in counts.items():
        bits = [int(b) for b in bitstring.zfill(n_qubits)]
        cut = sum(w for u, v, w in edges if bits[u] != bits[v])
        total += shots * cut
    return total / total_shots


def resolve_device():
    """(arn, name) for the SELECTED/matched backend. Prefer FLUXION_ARN; else map
    FLUXION_BACKEND -> ARN; else fall back to BRAKET_DEVICE / SV1 (non-Fluence)."""
    name = os.environ.get("FLUXION_BACKEND", "").strip()
    arn = os.environ.get("FLUXION_ARN", "").strip()
    if arn:
        return arn, (name or arn.split("/")[-1])
    if name and name in NAME_TO_ARN:
        return NAME_TO_ARN[name], name
    fallback = os.environ.get("BRAKET_DEVICE", SV1_ARN)
    return fallback, (name or fallback.split("/")[-1])


# -- producer (the one real submission) --------------------------------------

def run_producer():
    workspace = os.environ.get("WORKSPACE", "/workspace")
    n_shots = int(os.environ.get("N_SHOTS", 1000))
    arn, backend = resolve_device()

    t_start = time.time()
    log(f"role=producer backend={backend} device={arn} n_shots={n_shots}")
    # The orchestrator parses these two lines for the realized backend.
    log(f"FLUXION_BACKEND={backend}")
    log(f"device={arn}")
    log(f"TIMING start_ts={t_start:.6f}")

    _bootstrap_problem_if_missing(workspace)
    problem = json.load(open(f"{workspace}/problem.json"))
    params = json.load(open(f"{workspace}/params.json"))
    n_qubits, edges = problem["n_qubits"], problem["edges"]
    log(f"problem: n_qubits={n_qubits} edges={len(edges)}")

    from braket.aws import AwsDevice
    circ = build_circuit(n_qubits, edges, params["gammas"], params["betas"])
    device = AwsDevice(arn)

    # Tag the real task with our pod-uid so the Fluence sidecar can find it, poll
    # it, and ungate the consumers. Explicit tag is robust to interceptor import
    # ordering.
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    run_kwargs = {"shots": n_shots}
    if pod_uid:
        run_kwargs["tags"] = {"fluence-pod-uid": pod_uid}
        log(f"tagging task fluence-pod-uid={pod_uid}")
    else:
        log("WARNING: FLUENCE_POD_UID unset - task untagged; sidecar cannot find "
            "it and the consumers will not ungate")

    t_submit = time.time()
    log(f"TIMING submit_ts={t_submit:.6f}")
    task = device.run(circ, **run_kwargs)
    t_queued = time.time()
    log(f"TIMING queued_ts={t_queued:.6f}")
    log(f"Task ARN: {task.id}")

    result = task.result()
    t_result = time.time()
    log(f"TIMING result_ts={t_result:.6f}")
    if result is None:
        log(f"ERROR: task {task.id} returned no result (state={task.state()})")
        sys.exit(1)

    counts = result.measurement_counts
    cost = compute_maxcut_cost(counts, edges, n_qubits)
    t_end = time.time()
    log(f"TIMING end_ts={t_end:.6f}")
    log("SUMMARY " + json.dumps({
        "role": "producer", "backend": backend,
        "task_id": task.id.split("/")[-1], "cost": cost,
        "queue_wait_s": t_result - t_queued, "elapsed_s": t_end - t_start,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }))
    log(f"Done in {t_end - t_start:.1f}s")


# -- consumer (fetch the producer's result) ----------------------------------

def run_consumer():
    arn = os.environ.get("FLUENCE_QUANTUM_JOB_ID", "").strip()
    backend = os.environ.get("FLUXION_BACKEND", "").strip()
    t_start = time.time()
    log(f"role=consumer backend={backend} "
        f"job_id={'set' if arn else 'UNSET'}")
    log(f"TIMING start_ts={t_start:.6f}")
    if not arn:
        log("ERROR: FLUENCE_QUANTUM_JOB_ID unset - the sidecar did not stamp the "
            "producer's task id before ungating; cannot fetch the result")
        sys.exit(1)

    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    from braket.aws import AwsQuantumTask
    result = AwsQuantumTask(arn=arn).result()
    t_result = time.time()
    log(f"TIMING result_ts={t_result:.6f}")
    if result is None:
        log("ERROR: fetched task result is None")
        sys.exit(1)
    shots = sum(result.measurement_counts.values())
    log(f"fetched producer task {arn.split('/')[-1]} ({shots} shots)")
    log(f"Done in {time.time() - t_start:.1f}s")


# -- entrypoint --------------------------------------------------------------

if __name__ == "__main__":
    role = os.environ.get("FLUENCE_COORDINATION_ROLE", "").strip().lower()
    log(f"start role={role or '(unset->producer)'} "
        f"FLUXION_BACKEND={os.environ.get('FLUXION_BACKEND', '')!r}")
    if role == "consumer":
        run_consumer()
    else:
        # producer (shared mode index 0), or independent/standalone: real submit
        run_producer()
