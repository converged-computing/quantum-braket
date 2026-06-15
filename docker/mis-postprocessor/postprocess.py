#!/usr/bin/env python3
"""
mis-postprocessor: Classical greedy post-processing for AHS MIS results.

AHS measurements can produce bitstrings that:
  1. Violate the independent set constraint (two excited atoms within R_ud)
  2. Are valid but not maximal (more atoms could be added)

This postprocessor applies a two-phase greedy correction:
  Phase 1 (repair):   remove violating atoms one by one until IS is valid
  Phase 2 (grow):     greedily add atoms that don't violate any constraint

This mirrors the approach in Ebadi et al. 2022 and the QuEra whitepaper.

Reads  /workspace/ahs_result.json   (written by ahs-gateway)
       /workspace/problem.json
Writes /workspace/results.json      (final results, same schema as QAOA optimizer)

Environment variables:
  WORKSPACE     Shared volume path (default: /workspace)
"""

import json
import os
import random
import sys
import time


def repair_independent_set(bitstring, edges, rng):
    """
    Remove atoms until the bitstring is a valid independent set.
    Randomly breaks ties among violating atoms.
    """
    excited = list(i for i, b in enumerate(bitstring) if b == 1)
    excited_set = set(excited)

    changed = True
    while changed:
        changed = False
        violations = []
        for u, v in edges:
            if u in excited_set and v in excited_set:
                violations.append((u, v))
        if violations:
            # Remove one atom from a random violating pair
            pair = rng.choice(violations)
            remove = rng.choice(pair)
            excited_set.discard(remove)
            changed = True

    return sorted(excited_set)


def grow_independent_set(is_nodes, n_atoms, edges, rng):
    """
    Greedily add atoms to the independent set until no more can be added.
    """
    is_set = set(is_nodes)
    edge_set = set(map(tuple, [sorted(e) for e in edges]))

    candidates = list(set(range(n_atoms)) - is_set)
    rng.shuffle(candidates)

    for atom in candidates:
        # Check if adding this atom violates any constraint
        conflicts = any(
            (min(atom, m), max(atom, m)) in edge_set
            for m in is_set
        )
        if not conflicts:
            is_set.add(atom)

    return sorted(is_set)


def postprocess_shot(bitstring, n_atoms, edges, seed):
    """Full repair + grow pipeline for a single shot."""
    rng = random.Random(seed)
    repaired = repair_independent_set(bitstring, edges, rng)
    grown    = grow_independent_set(repaired, n_atoms, edges, rng)
    return grown


def main():
    workspace = os.environ.get("WORKSPACE", "/workspace")

    problem_path = os.path.join(workspace, "problem.json")
    result_path  = os.path.join(workspace, "ahs_result.json")

    for p in [problem_path, result_path]:
        if not os.path.exists(p):
            print(f"[mis-postprocessor] ERROR: {p} not found", file=sys.stderr)
            sys.exit(1)

    with open(problem_path) as f:
        problem = json.load(f)
    with open(result_path) as f:
        ahs_result = json.load(f)

    n_atoms = problem["n_atoms"]
    edges   = problem["edges"]
    seed    = problem.get("seed", 42)

    measurements = ahs_result["measurements"]
    print(f"[mis-postprocessor] Post-processing {len(measurements)} shots, "
          f"n_atoms={n_atoms}, edges={len(edges)}")

    t0 = time.time()

    processed = []
    for i, m in enumerate(measurements):
        bitstring = m["post_sequence"]
        is_nodes  = postprocess_shot(bitstring, n_atoms, edges, seed + i)
        processed.append({
            "shot":           i,
            "raw_post":       bitstring,
            "raw_n_excited":  m["n_excited"],
            "is_nodes":       is_nodes,
            "is_size":        len(is_nodes),
        })

    elapsed = time.time() - t0

    # Best independent set across all processed shots
    best = max(processed, key=lambda x: x["is_size"])
    avg_size = sum(x["is_size"] for x in processed) / len(processed)

    # Approximation ratio: IS size / n_atoms
    # (upper bound is n_atoms; tighter bound is chromatic number but hard to compute)
    approx_ratio = best["is_size"] / n_atoms if n_atoms > 0 else 0.0

    print(f"[mis-postprocessor] best IS size : {best['is_size']} / {n_atoms} atoms")
    print(f"[mis-postprocessor] avg IS size  : {avg_size:.2f}")
    print(f"[mis-postprocessor] approx ratio : {approx_ratio:.4f}")
    print(f"[mis-postprocessor] elapsed      : {elapsed:.3f}s")

    results = {
        "problem": {
            "type":             "ahs",
            "n_atoms":          n_atoms,
            "n_edges":          len(edges),
            "seed":             seed,
            "lattice_spacing":  problem.get("lattice_spacing"),
            "unit_disk_radius": problem.get("unit_disk_radius"),
        },
        "execution": {
            "device":         ahs_result["device"],
            "n_shots":        ahs_result["n_shots"],
            "elapsed_s":      ahs_result["elapsed_s"],
            "valid_fraction": ahs_result["valid_fraction"],
        },
        "optimization": {
            "best_is_size":        best["is_size"],
            "best_is_nodes":       best["is_nodes"],
            "avg_is_size":         round(avg_size, 4),
            "approximation_ratio": round(approx_ratio, 4),
            "postprocess_elapsed_s": round(elapsed, 3),
        },
        "shots": processed,
    }

    out_path = os.path.join(workspace, "results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    summary = {
        "n_atoms":             n_atoms,
        "n_edges":             len(edges),
        "seed":                seed,
        "n_shots":             ahs_result["n_shots"],
        "best_is_size":        best["is_size"],
        "approximation_ratio": round(approx_ratio, 4),
        "avg_is_size":         round(avg_size, 4),
        "valid_fraction":      ahs_result["valid_fraction"],
        "total_elapsed_s":     round(elapsed + ahs_result["elapsed_s"], 3),
    }
    print(f"[mis-postprocessor] SUMMARY {json.dumps(summary)}")
    print(f"[mis-postprocessor] Wrote -> {out_path}")


if __name__ == "__main__":
    main()
