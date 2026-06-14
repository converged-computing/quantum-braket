#!/usr/bin/env python3
"""
transpiler: Build the QAOA ansatz circuit for a max-cut problem instance.

Reads  /workspace/problem.json
Writes /workspace/circuit.json  (Braket IR / OpenQASM 3 serialized circuit)
       /workspace/params.json   (initial variational parameters)

The QAOA circuit for max-cut has two layers per repetition (p):
  - A cost unitary  C(gamma): RZZ gates on each edge
  - A mixer unitary B(beta):  RX gates on each qubit

We use p=1 by default (one QAOA layer), which is sufficient for small
benchmarking instances and keeps circuit depth low for SV1.

Environment variables:
  P_LAYERS    Number of QAOA repetitions p (default: 1)
  WORKSPACE   Shared volume path (default: /workspace)
"""

import json
import math
import os
import sys


def build_qaoa_openqasm(n_qubits, edges, gamma, beta):
    """
    Return an OpenQASM 3 string for a single-layer (p=1) QAOA max-cut circuit.

    Cost unitary:   exp(-i * gamma/2 * (1 - Z_u Z_v)) for each edge (u,v)
                    Implemented as CNOT - RZ(gamma) - CNOT
    Mixer unitary:  RX(2*beta) on each qubit
    Measurement:    all qubits
    """
    lines = [
        "OPENQASM 3.0;",
        'include "stdgates.inc";',
        "",
        f"qubit[{n_qubits}] q;",
        f"bit[{n_qubits}] c;",
        "",
        "// Initial superposition",
    ]
    for i in range(n_qubits):
        lines.append(f"h q[{i}];")

    lines.append("")
    lines.append("// Cost unitary (gamma layer)")
    for u, v, _ in edges:
        lines.append(f"cnot q[{u}], q[{v}];")
        rz_angle = round(gamma, 10)
        lines.append(f"rz({rz_angle}) q[{v}];")
        lines.append(f"cnot q[{u}], q[{v}];")

    lines.append("")
    lines.append("// Mixer unitary (beta layer)")
    rx_angle = round(2 * beta, 10)
    for i in range(n_qubits):
        lines.append(f"rx({rx_angle}) q[{i}];")

    lines.append("")
    lines.append("// Measurement")
    for i in range(n_qubits):
        lines.append(f"c[{i}] = measure q[{i}];")

    return "\n".join(lines)


def initial_params(p, seed=42):
    """
    Return deterministic initial (gamma, beta) parameters for p QAOA layers.
    Uses evenly spaced values in (0, pi) to avoid symmetry traps.
    """
    import random
    rng = random.Random(seed)
    gammas = [rng.uniform(0.1, math.pi - 0.1) for _ in range(p)]
    betas  = [rng.uniform(0.1, math.pi / 2 - 0.1) for _ in range(p)]
    return {"gammas": gammas, "betas": betas, "p": p}


def main():
    p = int(os.environ.get("P_LAYERS", 1))
    workspace = os.environ.get("WORKSPACE", "/workspace")

    problem_path = os.path.join(workspace, "problem.json")
    if not os.path.exists(problem_path):
        print(f"[transpiler] ERROR: {problem_path} not found. "
              "Has the problem-generator pod completed?", file=sys.stderr)
        sys.exit(1)

    with open(problem_path) as f:
        problem = json.load(f)

    n_qubits = problem["n_qubits"]
    edges    = problem["edges"]
    seed     = problem.get("seed", 42)

    print(f"[transpiler] Building QAOA circuit: n_qubits={n_qubits}, edges={len(edges)}, p={p}")

    params = initial_params(p, seed=seed)
    gamma0 = params["gammas"][0]
    beta0  = params["betas"][0]

    # Build the OpenQASM 3 circuit for the initial parameters
    qasm = build_qaoa_openqasm(n_qubits, edges, gamma0, beta0)

    circuit_doc = {
        "format": "openqasm3",
        "n_qubits": n_qubits,
        "p": p,
        "source": qasm,
        "n_gates": len(edges) * 3 + n_qubits,  # 3 gates per edge + RX per qubit
        "depth_estimate": p * (len(edges) + 1),
    }

    circuit_path = os.path.join(workspace, "circuit.json")
    params_path  = os.path.join(workspace, "params.json")

    with open(circuit_path, "w") as f:
        json.dump(circuit_doc, f, indent=2)

    with open(params_path, "w") as f:
        json.dump(params, f, indent=2)

    print(f"[transpiler] Wrote circuit  -> {circuit_path}")
    print(f"[transpiler] Wrote params   -> {params_path}")
    print(f"[transpiler] Circuit stats:")
    print(f"  n_qubits : {n_qubits}")
    print(f"  p layers : {p}")
    print(f"  est gates: {circuit_doc['n_gates']}")
    print(f"  est depth: {circuit_doc['depth_estimate']}")
    print(f"  gamma_0  : {gamma0:.4f}")
    print(f"  beta_0   : {beta0:.4f}")


if __name__ == "__main__":
    main()
