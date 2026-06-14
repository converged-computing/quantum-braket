#!/usr/bin/env python3
"""
problem-generator: Generate a random k-regular graph max-cut instance.

Writes to /workspace/problem.json:
  {
    "n_nodes": int,
    "edges": [[u, v, weight], ...],
    "seed": int,
    "k": int,
    "n_qubits": int          # == n_nodes for max-cut QAOA
  }

Environment variables:
  N_NODES   Number of graph nodes (default: 10)
  K_REGULAR Degree of regularity (default: 3)
  SEED      Random seed for reproducibility (default: 42)
  WORKSPACE Output directory (default: /workspace)
"""

import json
import os
import random
import sys

def generate_k_regular_graph(n, k, seed):
    """
    Generate a random k-regular graph on n nodes using a pairing model.
    Returns a list of (u, v, weight) edges with unit weights.
    Raises ValueError if a valid k-regular graph cannot be formed.
    """
    if n * k % 2 != 0:
        raise ValueError(f"n*k must be even for a k-regular graph (got n={n}, k={k})")
    if k >= n:
        raise ValueError(f"k must be less than n (got k={k}, n={n})")

    rng = random.Random(seed)

    # Configuration model: create k stubs per node, pair them randomly
    for attempt in range(100):
        stubs = []
        for node in range(n):
            stubs.extend([node] * k)
        rng.shuffle(stubs)

        edges = set()
        valid = True
        for i in range(0, len(stubs), 2):
            u, v = stubs[i], stubs[i + 1]
            if u == v or (min(u, v), max(u, v)) in edges:
                valid = False
                break
            edges.add((min(u, v), max(u, v)))

        if valid:
            return [(u, v, 1.0) for u, v in sorted(edges)]

    raise RuntimeError(
        f"Could not generate a valid {k}-regular graph on {n} nodes after 100 attempts. "
        "Try a different seed or smaller k."
    )


def main():
    n_nodes = int(os.environ.get("N_NODES", 10))
    k = int(os.environ.get("K_REGULAR", 3))
    seed = int(os.environ.get("SEED", 42))
    workspace = os.environ.get("WORKSPACE", "/workspace")

    print(f"[problem-generator] n_nodes={n_nodes}, k={k}, seed={seed}")

    try:
        edges = generate_k_regular_graph(n_nodes, k, seed)
    except (ValueError, RuntimeError) as e:
        print(f"[problem-generator] ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    problem = {
        "n_nodes": n_nodes,
        "edges": edges,
        "seed": seed,
        "k": k,
        "n_qubits": n_nodes,  # one qubit per node for max-cut QAOA
    }

    os.makedirs(workspace, exist_ok=True)
    out_path = os.path.join(workspace, "problem.json")
    with open(out_path, "w") as f:
        json.dump(problem, f, indent=2)

    print(f"[problem-generator] Wrote {len(edges)} edges to {out_path}")
    print(f"[problem-generator] Problem instance:")
    print(f"  nodes : {n_nodes}")
    print(f"  edges : {len(edges)}")
    print(f"  qubits: {problem['n_qubits']}")
    print(f"  seed  : {seed}")


if __name__ == "__main__":
    main()
