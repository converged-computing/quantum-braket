#!/usr/bin/env python3
"""
ahs-problem-generator: Generate a unit disk graph instance for Maximum
Independent Set (MIS) via Analog Hamiltonian Simulation on QuEra Aquila.

A unit disk graph places n atoms in 2D space. Two atoms share an edge
(i.e. will interact via Rydberg blockade) if their distance is less than
the unit disk radius R_ud. The MIS problem is to find the largest set of
atoms with no two within R_ud of each other.

Atom positions are chosen on a 2D grid with spacing a (lattice constant),
with a fraction of sites randomly dropped to create irregular geometry.
This mirrors the King's graph construction used in the QuEra whitepaper.

Writes to /workspace/problem.json:
  {
    "type": "ahs",
    "n_atoms": int,
    "atom_positions": [[x, y], ...],   # meters
    "unit_disk_radius": float,          # meters
    "lattice_spacing": float,           # meters
    "edges": [[i, j], ...],             # pairs within R_ud
    "seed": int
  }

Environment variables:
  N_ATOMS       Target number of atoms (default: 16)
  DROPOUT       Fraction of grid sites to remove (default: 0.3)
  SEED          Random seed (default: 42)
  WORKSPACE     Output directory (default: /workspace)
"""

import json
import math
import os
import random
import sys


# Aquila hardware constraints (from Braket docs)
# Lattice spacing must be >= 4 µm to avoid unwanted interactions
MIN_SPACING = 4e-6       # 4 µm
DEFAULT_SPACING = 5.5e-6 # 5.5 µm — safe working value
# Unit disk radius is set between lattice spacing and sqrt(2)*spacing
# so nearest neighbors interact but diagonal neighbors may not
BLOCKADE_MULTIPLIER = 1.3  # R_ud = BLOCKADE_MULTIPLIER * spacing


def build_grid_positions(n_atoms, spacing, dropout, seed):
    """
    Build atom positions on a 2D square grid with random dropout.
    Returns list of (x, y) tuples in meters.
    """
    rng = random.Random(seed)

    # Determine grid size: smallest square grid that can hold n_atoms/(1-dropout)
    target = n_atoms / (1.0 - dropout)
    side = math.ceil(math.sqrt(target))

    all_sites = [(i * spacing, j * spacing) for i in range(side) for j in range(side)]
    rng.shuffle(all_sites)

    # Keep sites until we have enough; drop the rest
    kept = all_sites[:n_atoms]
    kept.sort()  # deterministic ordering

    return kept


def build_unit_disk_graph(positions, r_ud):
    """
    Build edges for the unit disk graph: edge (i,j) if dist(i,j) < r_ud.
    """
    edges = []
    n = len(positions)
    for i in range(n):
        for j in range(i + 1, n):
            dx = positions[i][0] - positions[j][0]
            dy = positions[i][1] - positions[j][1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist < r_ud:
                edges.append([i, j])
    return edges


def main():
    n_atoms  = int(os.environ.get("N_ATOMS", 16))
    dropout  = float(os.environ.get("DROPOUT", 0.3))
    seed     = int(os.environ.get("SEED", 42))
    spacing  = float(os.environ.get("LATTICE_SPACING", DEFAULT_SPACING))
    workspace = os.environ.get("WORKSPACE", "/workspace")

    r_ud = spacing * BLOCKADE_MULTIPLIER

    print(f"[ahs-problem-generator] n_atoms={n_atoms}, dropout={dropout}, "
          f"seed={seed}, spacing={spacing*1e6:.1f}µm, R_ud={r_ud*1e6:.2f}µm")

    positions = build_grid_positions(n_atoms, spacing, dropout, seed)
    actual_n  = len(positions)
    edges     = build_unit_disk_graph(positions, r_ud)

    problem = {
        "type": "ahs",
        "n_atoms": actual_n,
        "atom_positions": [[p[0], p[1]] for p in positions],
        "unit_disk_radius": r_ud,
        "lattice_spacing": spacing,
        "edges": edges,
        "seed": seed,
        "dropout": dropout,
    }

    os.makedirs(workspace, exist_ok=True)
    out_path = os.path.join(workspace, "problem.json")
    with open(out_path, "w") as f:
        json.dump(problem, f, indent=2)

    print(f"[ahs-problem-generator] atoms={actual_n}, edges={len(edges)}")
    print(f"[ahs-problem-generator] Wrote -> {out_path}")


if __name__ == "__main__":
    main()
