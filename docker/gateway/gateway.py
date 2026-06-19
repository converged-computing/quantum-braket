#!/usr/bin/env python3
"""
braket-gateway: Submit QAOA circuits to AWS Braket and return cost values.

This pod is the only one that touches AWS. It reads the current variational
parameters, rebuilds the circuit for those parameters, submits it to the
SV1 state vector simulator, and appends the measured cost to a results file.

It is designed to be called repeatedly by the optimizer (once per iteration).
On each call it:
  1. Reads /workspace/params.json  (written by transpiler or optimizer)
  2. Reads /workspace/problem.json
  3. Builds the circuit for the current (gamma, beta)
  4. Submits to Braket SV1 (synchronously, waits for result)
  5. Computes the QAOA cost <C> from the measurement counts
  6. Appends {"iteration": i, "cost": c, "params": {...}} to /workspace/history.json
  7. Writes /workspace/cost.json  {"cost": float}  (read by optimizer)

Environment variables:
  BRAKET_DEVICE   Braket device ARN (default: SV1)
  N_SHOTS         Number of measurement shots (default: 1000)
  WORKSPACE       Shared volume path (default: /workspace)
  ITERATION       Current optimizer iteration number (default: 0)
"""

import json
import os
import sys
import time

# SV1 device ARN — deterministic, no queue wait
SV1_ARN = "arn:aws:braket:::device/quantum-simulator/amazon/sv1"


def compute_maxcut_cost(counts, edges, n_qubits):
    """
    Compute the expected max-cut value from measurement counts.

    For each bitstring, count the number of edges (u,v) where z_u != z_v
    (i.e., the edge is cut). Return the weighted average over all shots.

    counts: dict mapping bitstring -> number of shots (Braket result format)
    edges:  list of [u, v, weight]
    """
    total_shots = sum(counts.values())
    total_cost = 0.0

    for bitstring, shots in counts.items():
        # Braket returns bitstrings as e.g. "0110" (leftmost = qubit 0)
        if len(bitstring) != n_qubits:
            # Some backends zero-pad; handle gracefully
            bitstring = bitstring.zfill(n_qubits)
        bits = [int(b) for b in bitstring]
        cut_value = sum(
            w for u, v, w in edges if bits[u] != bits[v]
        )
        total_cost += shots * cut_value

    return total_cost / total_shots


def build_circuit_for_params(n_qubits, edges, gammas, betas):
    """
    Build a Braket Circuit object for the current (gammas, betas) parameters.
    Supports p >= 1 layers.
    """
    from braket.circuits import Circuit

    circ = Circuit()

    # Initial superposition
    for i in range(n_qubits):
        circ.h(i)

    # QAOA layers
    for layer in range(len(gammas)):
        gamma = gammas[layer]
        beta  = betas[layer]

        # Cost unitary: CNOT - RZ(gamma) - CNOT for each edge
        for u, v, _ in edges:
            circ.cnot(u, v)
            circ.rz(v, gamma)
            circ.cnot(u, v)

        # Mixer unitary: RX(2*beta) on each qubit
        for i in range(n_qubits):
            circ.rx(i, 2 * beta)

    return circ


def main():
    workspace   = os.environ.get("WORKSPACE", "/workspace")
    # FLUXION_ARN is injected by the Fluence webhook when scheduled via Fluence.
    # Fall back to BRAKET_DEVICE for local/non-Fluence runs.
    device_arn  = os.environ.get("FLUXION_ARN") or os.environ.get("BRAKET_DEVICE", SV1_ARN)
    qrmi_type   = os.environ.get("FLUXION_QRMI_TYPE", "braket-gate")
    n_shots     = int(os.environ.get("N_SHOTS", 1000))
    iteration   = int(os.environ.get("ITERATION", 0))

    # --- load problem ---
    problem_path = os.path.join(workspace, "problem.json")
    if not os.path.exists(problem_path):
        print(f"[braket-gateway] ERROR: {problem_path} not found", file=sys.stderr)
        sys.exit(1)
    with open(problem_path) as f:
        problem = json.load(f)
    n_qubits = problem["n_qubits"]
    edges    = problem["edges"]

    # --- load current params ---
    params_path = os.path.join(workspace, "params.json")
    if not os.path.exists(params_path):
        print(f"[braket-gateway] ERROR: {params_path} not found", file=sys.stderr)
        sys.exit(1)
    with open(params_path) as f:
        params = json.load(f)
    gammas = params["gammas"]
    betas  = params["betas"]

    print(f"[braket-gateway] iteration={iteration}, device={device_arn}, shots={n_shots}")
    print(f"[braket-gateway] gammas={[round(g,4) for g in gammas]}, "
          f"betas={[round(b,4) for b in betas]}")

    # --- build circuit ---
    try:
        from braket.aws import AwsDevice
        circ = build_circuit_for_params(n_qubits, edges, gammas, betas)
    except ImportError:
        print("[braket-gateway] ERROR: amazon-braket-sdk not installed", file=sys.stderr)
        sys.exit(1)

    # --- submit to Braket ---
    t_submit = time.time()
    print(f"[braket-gateway] TIMING submit_ts={t_submit:.6f}")
    try:
        device = AwsDevice(device_arn)
        task   = device.run(circ, shots=n_shots)
        t_queued = time.time()
        print(f"[braket-gateway] TIMING queued_ts={t_queued:.6f}")
        result = task.result()
        t_result = time.time()
        print(f"[braket-gateway] TIMING result_ts={t_result:.6f}")
    except Exception as e:
        print(f"[braket-gateway] ERROR submitting to Braket: {e}", file=sys.stderr)
        sys.exit(1)

    if result is None:
        print(f"[braket-gateway] ERROR: task {task.id} returned no result "
              f"(state={task.state()}). Task may have been cancelled or failed.",
              file=sys.stderr)
        sys.exit(1)

    elapsed = t_result - t_submit
    qpu_queue_wait = t_result - t_queued

    # --- compute cost ---
    counts = result.measurement_counts
    cost   = compute_maxcut_cost(counts, edges, n_qubits)

    print(f"[braket-gateway] Cost={cost:.6f}  (elapsed {elapsed:.2f}s)")

    # --- write cost.json (read by optimizer) ---
    cost_path = os.path.join(workspace, "cost.json")
    with open(cost_path, "w") as f:
        json.dump({"cost": cost, "iteration": iteration, "elapsed_s": elapsed}, f, indent=2)

    # --- append to history.json ---
    history_path = os.path.join(workspace, "history.json")
    history = []
    if os.path.exists(history_path):
        with open(history_path) as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = []
    history.append({
        "iteration":       iteration,
        "cost":            cost,
        "gammas":          gammas,
        "betas":           betas,
        "elapsed_s":       elapsed,
        "qpu_queue_wait_s": qpu_queue_wait,
        "submit_ts":       t_submit,
        "queued_ts":       t_queued,
        "result_ts":       t_result,
        "n_shots":         n_shots,
        "device":          device_arn,
    })
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)

    print(f"[braket-gateway] Wrote cost -> {cost_path}")
    print(f"[braket-gateway] Appended   -> {history_path} ({len(history)} entries)")


if __name__ == "__main__":
    main()
