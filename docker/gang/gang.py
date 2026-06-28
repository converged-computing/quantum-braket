#!/usr/bin/env python3
"""
gang.py — Role-aware producer/consumer gang workload (Experiment 2).

A gang of N pods runs ONE quantum task and shares its result. Roles follow the
Fluence coordination model:

  producer  builds the QAOA circuit, submits the ONE real QPU task, and publishes
            the task id (so non-Fluence consumers can find it).
  consumer  does NOT submit — it obtains the producer's task id, fetches the
            SHARED result, and processes its shot partition.

ROLE comes from FLUENCE_COORDINATION_ROLE (producer|consumer):
  - Fluence arm:  the webhook injects it (index 0 -> producer, rest -> consumer).
  - default arm:  the manifest sets it explicitly (no Fluence to inject it).
Unset is treated as producer (a lone/standalone submit).

TASK-ID HAND-OFF:
  - Fluence arm:  the ungating sidecar stamps the producer's task id and the
                  webhook surfaces it as FLUENCE_QUANTUM_JOB_ID; the consumer is
                  GATED until the QPU task is at queue position ~1, so it holds no
                  classical node during the queue wait.
  - default arm:  there is no Fluence, so the producer PUBLISHES its task id to S3
                  and consumers POLL for it. They start immediately with the
                  producer and idle (burning classical node-time) until the result
                  is ready. That wasted idle is exactly what Fluence reclaims.

DEVICE selection is annotation-driven in the Fluence arm: the producer requests
the qpu resource and Fluence injects the matched device as FLUXION_ARN. The
default arm has no Fluence, so it names the device manually via BRAKET_DEVICE.

Environment:
  FLUENCE_COORDINATION_ROLE  producer | consumer (see above)
  FLUENCE_QUANTUM_JOB_ID     producer's task ARN, injected by the sidecar (Fluence
                             consumers only; absent => discover via S3)
  FLUXION_ARN / FLUXION_BACKEND   device ARN / name injected by Fluence (producer)
  BRAKET_DEVICE              device ARN for the non-Fluence (default) arm
  RUN_ID                     unique per run; scopes the S3 hand-off prefix
  N_CONSUMERS                number of consumers (N-1)
  CONSUMER_INDEX             this consumer's index (default: JOB_COMPLETION_INDEX)
  N_SHOTS, N_NODES, K_REGULAR, SEED, P_LAYERS   problem definition
  AWS_*                      Braket credentials; BRAKET_REGION optional region hint

Logs TIMING <key>=<epoch> lines the orchestrator parses, and one SUMMARY <json>.
"""

import json
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

import boto3

SV1_ARN = "arn:aws:braket:::device/quantum-simulator/amazon/sv1"


def log(role, msg):
    print(f"[gang-{role}] {msg}", flush=True)


# ── S3 hand-off (default / non-Fluence arm only) ─────────────────────────────────

def _run_prefix():
    # Per-run prefix so a run's pods never match a previous run's objects.
    return f"fluence-gang/{os.environ.get('RUN_ID', 'default')}"


def key_producer_task():
    return f"{_run_prefix()}/producer-task.json"


def get_s3_bucket(s3, sts, region):
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        account_id = sts.get_caller_identity()["Account"]
        bucket = f"amazon-braket-{region}-{account_id}"
    return bucket


def resolve_region(arn=None):
    arn = arn or os.environ.get("FLUXION_ARN") or os.environ.get("BRAKET_DEVICE", "")
    parts = arn.split(":")
    if len(parts) > 3 and parts[3]:
        return parts[3]
    return (os.environ.get("FLUXION_REGION")
            or os.environ.get("BRAKET_REGION")
            or os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


# ── problem + circuit (self-contained; identical to the sampler) ─────────────────

def build_problem():
    n_nodes = int(os.environ.get("N_NODES", 10))
    k = int(os.environ.get("K_REGULAR", 3))
    seed = int(os.environ.get("SEED", 42))
    p = int(os.environ.get("P_LAYERS", 1))
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
    prng = random.Random(seed)
    gammas = [prng.uniform(0.1, math.pi - 0.1) for _ in range(p)]
    betas = [prng.uniform(0.1, math.pi / 2 - 0.1) for _ in range(p)]
    return n_nodes, edges, gammas, betas


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


def _aws_session(region):
    from braket.aws import AwsSession
    return AwsSession(boto_session=boto3.Session(region_name=region))


# ── producer ─────────────────────────────────────────────────────────────────────

def run_producer():
    lg = lambda m: log("producer", m)
    n_shots = int(os.environ.get("N_SHOTS", 1000))
    n_consumers = int(os.environ.get("N_CONSUMERS", 4))
    device_arn = os.environ.get("FLUXION_ARN") or os.environ.get("BRAKET_DEVICE", SV1_ARN)
    backend_name = os.environ.get("FLUXION_BACKEND", "")
    region = resolve_region(device_arn)

    t_start = time.time()
    lg(f"TIMING producer_start_ts={t_start:.6f}")
    if backend_name:
        lg(f"FLUXION_BACKEND={backend_name}")
    lg(f"device={device_arn} region={region} n_shots={n_shots} n_consumers={n_consumers}")

    n_qubits, edges, gammas, betas = build_problem()
    from braket.aws import AwsDevice
    session = _aws_session(region)
    device = AwsDevice(device_arn, aws_session=session)
    circ = build_circuit(n_qubits, edges, gammas, betas)

    t_submit = time.time()
    lg(f"TIMING submit_ts={t_submit:.6f}")
    # QPU vendor queues can be hours-to-days, so the Braket result poll must be
    # very long (POLL_TIMEOUT_S, default 30d) or .result() would give up mid-queue.
    poll_timeout = int(os.environ.get("POLL_TIMEOUT_S", 2592000))
    task = device.run(circ, shots=n_shots, poll_timeout_seconds=poll_timeout)
    task_arn = task.id
    lg(f"submitted task {task_arn}")

    # Publish the task id for non-Fluence (default-arm) consumers to discover.
    # In the Fluence arm consumers receive it via FLUENCE_QUANTUM_JOB_ID and never
    # read this; the publish is best-effort so a missing S3 bucket can't fail the
    # Fluence run.
    try:
        s3 = boto3.client("s3", region_name=region)
        sts = boto3.client("sts", region_name=region)
        bucket = get_s3_bucket(s3, sts, region)
        body = json.dumps({"task_arn": task_arn, "n_qubits": n_qubits,
                           "n_consumers": n_consumers, "region": region})
        s3.put_object(Bucket=bucket, Key=key_producer_task(), Body=body.encode())
        lg(f"published task id -> s3://{bucket}/{key_producer_task()}")
    except Exception as e:
        lg(f"WARNING: could not publish task id to S3 "
           f"(Fluence-arm consumers don't need it): {e}")

    result = task.result()
    t_result = time.time()
    lg(f"TIMING result_ts={t_result:.6f}")
    cost = compute_maxcut_cost(result.measurement_counts, edges, n_qubits)
    t_done = time.time()
    lg(f"TIMING done_ts={t_done:.6f}")

    summary = {
        "task_id": task_arn.split("/")[-1],
        "backend": backend_name or device_arn,
        "device_arn": device_arn,
        "role": "producer",
        "n_qubits": n_qubits,
        "n_shots": n_shots,
        "n_consumers": n_consumers,
        "maxcut_cost": cost,
        "queue_wait_s": t_result - t_submit,
        "total_elapsed_s": t_done - t_start,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    lg(f"SUMMARY {json.dumps(summary)}")
    lg(f"Done (producer) in {t_done - t_start:.1f}s "
       f"(task {summary['task_id']}, maxcut {cost:.4f})")


# ── consumer ─────────────────────────────────────────────────────────────────────

def poll_s3_for_task(region, timeout, lg):
    s3 = boto3.client("s3", region_name=region)
    sts = boto3.client("sts", region_name=region)
    bucket = get_s3_bucket(s3, sts, region)
    key = key_producer_task()
    infinite = timeout <= 0
    lg(f"polling s3://{bucket}/{key} for the producer's task id "
       f"(timeout={'infinite' if infinite else str(timeout) + 's'})")
    deadline = None if infinite else time.time() + timeout
    waited = 0
    while infinite or time.time() < deadline:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            info = json.loads(obj["Body"].read())
            return info["task_arn"], info.get("region", region)
        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            lg(f"  poll error (retry): {e}")
        time.sleep(5)
        waited += 5
        if waited % 60 == 0:
            lg(f"  still waiting for the producer's task id ({waited}s elapsed)")
    return None, region


def run_consumer():
    idx = os.environ.get("CONSUMER_INDEX") or os.environ.get("JOB_COMPLETION_INDEX", "0")
    lg = lambda m: log(f"consumer-{idx}", m)
    n_consumers = int(os.environ.get("N_CONSUMERS", 4))
    timeout = int(os.environ.get("CONSUMER_TIMEOUT_S", 0))  # <=0 => wait indefinitely
    region = resolve_region()

    t_start = time.time()
    lg(f"TIMING consumer_start_ts={t_start:.6f}")

    # Obtain the producer's task id. Fluence hands it over directly (the consumer
    # was gated until ~now); without Fluence we discover it from the producer's S3
    # publish, idling here until it appears.
    task_arn = os.environ.get("FLUENCE_QUANTUM_JOB_ID", "").strip()
    if task_arn:
        lg(f"got task id from FLUENCE_QUANTUM_JOB_ID: {task_arn}")
        region = resolve_region(task_arn)
    else:
        lg("no FLUENCE_QUANTUM_JOB_ID (non-Fluence run) — discovering via S3")
        task_arn, region = poll_s3_for_task(region, timeout, lg)
        if not task_arn:
            lg("ERROR: producer task id not found within timeout — "
               "the producer may have failed or the QPU queue exceeded the timeout")
            sys.exit(1)
    t_got_id = time.time()
    lg(f"TIMING got_id_ts={t_got_id:.6f}")

    from braket.aws import AwsQuantumTask
    session = _aws_session(region)
    poll_timeout = int(os.environ.get("POLL_TIMEOUT_S", 2592000))
    result = AwsQuantumTask(arn=task_arn, aws_session=session,
                            poll_timeout_seconds=poll_timeout).result()
    t_result_ready = time.time()
    lg(f"TIMING result_ready_ts={t_result_ready:.6f}")
    if result is None:
        lg(f"ERROR: shared task {task_arn} returned no result")
        sys.exit(1)

    # Process this consumer's shot partition of the SHARED result.
    n_qubits, edges, _, _ = build_problem()
    counts = result.measurement_counts
    shots = []
    for bitstring, c in counts.items():
        shots.extend([bitstring] * c)
    my_shots = shots[int(idx)::n_consumers] if n_consumers > 0 else shots
    my_counts = {}
    for b in my_shots:
        my_counts[b] = my_counts.get(b, 0) + 1
    partition_cost = compute_maxcut_cost(my_counts, edges, n_qubits)
    t_done = time.time()
    lg(f"TIMING done_ts={t_done:.6f}")

    idle_s = t_result_ready - t_start  # node held without the result this long
    summary = {
        "task_id": task_arn.split("/")[-1],
        "role": "consumer",
        "consumer_index": int(idx),
        "n_qubits": n_qubits,
        "partition_shots": len(my_shots),
        "partition_maxcut_cost": partition_cost,
        "idle_s": idle_s,
        "total_elapsed_s": t_done - t_start,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    lg(f"SUMMARY {json.dumps(summary)}")
    lg(f"Done (consumer-{idx}) in {t_done - t_start:.1f}s "
       f"(idle {idle_s:.1f}s, {len(my_shots)} shots, partition maxcut {partition_cost:.4f})")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    role = os.environ.get("FLUENCE_COORDINATION_ROLE", "").strip().lower()
    # Fluence injects the role in the fluence arm. In the default (non-Fluence)
    # arm nothing injects it, so fall back to the Job completion index: index 0 is
    # the producer, every other index is a consumer. (Both arms are indexed Jobs.)
    if not role:
        idx = os.environ.get("JOB_COMPLETION_INDEX")
        if idx is not None:
            role = "producer" if idx == "0" else "consumer"
    print(f"[gang] role={role or '(unset -> producer)'} "
          f"(FLUENCE_COORDINATION_ROLE={os.environ.get('FLUENCE_COORDINATION_ROLE')!r} "
          f"JOB_COMPLETION_INDEX={os.environ.get('JOB_COMPLETION_INDEX')!r})",
          flush=True)
    if role == "consumer":
        run_consumer()
    elif role in ("producer", ""):
        if not role:
            print("[gang] WARNING: no role and no completion index — defaulting to "
                  "producer. In a gang every pod must have a role (the Fluence webhook "
                  "injects it; the default arm derives it from the completion index).",
                  flush=True)
        run_producer()
    else:
        print(f"ERROR: role must be 'producer' or 'consumer', got {role!r}",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
