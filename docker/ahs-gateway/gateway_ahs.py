#!/usr/bin/env python3
"""
ahs-gateway: Build and submit an Analog Hamiltonian Simulation program
for Maximum Independent Set to QuEra Aquila or the local AHS simulator.

Reads  /workspace/problem.json  (written by ahs-problem-generator)
Writes /workspace/ahs_result.json

The AHS program uses a global Rabi drive with an adiabatic detuning sweep:
  - Start: large negative detuning (all atoms in ground state)
  - End:   large positive detuning (atoms want to be in Rydberg state)
  - Rydberg blockade enforces MIS constraint: two atoms within R_ud
    cannot both be excited simultaneously

This is a single-shot (non-variational) quantum program. The result is
a set of bitstrings indicating which atoms are in the Rydberg (excited)
state — each valid bitstring is a candidate independent set.

Environment variables:
  BRAKET_DEVICE   Device ARN or "local" for local AHS simulator (default: local)
  N_SHOTS         Number of shots (default: 100)
  WORKSPACE       Shared volume path (default: /workspace)
  TOTAL_TIME      AHS evolution time in µs (default: 4.0)
"""

import json
import os
import sys
import time


# Aquila / AHS physics constants
OMEGA_MAX   = 1.5e7   # rad/s  — max Rabi frequency (~15 MHz)
DELTA_START = -5e7    # rad/s  — initial detuning (negative = ground state favored)
DELTA_END   =  5e7    # rad/s  — final detuning   (positive = Rydberg state favored)
RISE_TIME   = 0.5e-6  # s      — ramp up/down time

LOCAL_AHS_ARN = "local"
AQUILA_ARN    = "arn:aws:braket:us-east-1::device/qpu/quera/Aquila"


def build_ahs_program(atom_positions, total_time_us=4.0):
    """
    Build an AnalogHamiltonianSimulation program for MIS.
    Uses a trapezoidal Rabi drive and linear detuning sweep.
    """
    from braket.ahs.atom_arrangement import AtomArrangement
    from braket.ahs.analog_hamiltonian_simulation import AnalogHamiltonianSimulation
    from braket.ahs.driving_field import DrivingField
    from braket.timings.time_series import TimeSeries

    total_time = total_time_us * 1e-6  # convert µs -> s

    # Atom arrangement
    register = AtomArrangement()
    for x, y in atom_positions:
        register.add(x, y)

    # Rabi frequency: trapezoid (0 -> OMEGA_MAX -> OMEGA_MAX -> 0)
    omega_times  = [0, RISE_TIME, total_time - RISE_TIME, total_time]
    omega_values = [0, OMEGA_MAX, OMEGA_MAX, 0]
    omega_ts = TimeSeries()
    for t, v in zip(omega_times, omega_values):
        omega_ts.put(t, v)

    # Phase: constant 0
    phi_ts = TimeSeries()
    phi_ts.put(0, 0)
    phi_ts.put(total_time, 0)

    # Detuning: linear sweep from DELTA_START to DELTA_END
    delta_ts = TimeSeries()
    delta_ts.put(0, DELTA_START)
    delta_ts.put(total_time, DELTA_END)

    drive = DrivingField(
        amplitude=omega_ts,
        phase=phi_ts,
        detuning=delta_ts,
    )

    return AnalogHamiltonianSimulation(register=register, hamiltonian=drive)


def run_local(ahs_program, n_shots):
    """Run on the local AHS simulator (no AWS credentials needed)."""
    from braket.devices import LocalSimulator
    device = LocalSimulator(backend="braket_ahs")
    result = device.run(ahs_program, shots=n_shots).result()
    return result


def run_aquila(ahs_program, n_shots, device_arn):
    """Run on Aquila QPU (requires AWS credentials and QPU access)."""
    from braket.aws import AwsDevice
    device = AwsDevice(device_arn)
    discretized = ahs_program.discretize(device)
    task   = device.run(discretized, shots=n_shots)
    result = task.result()
    return result


def parse_results(result, n_atoms):
    """
    Extract measurement outcomes from AHS result.
    Returns list of bitstrings and pre_sequence validity masks.
    """
    measurements = result.measurements
    records = []
    for m in measurements:
        # post_sequence: 1 = Rydberg (excited), 0 = ground
        # pre_sequence:  1 = atom present (should be all 1s for local sim)
        post = list(m.post_sequence)
        pre  = list(m.pre_sequence)
        records.append({
            "post_sequence": post,
            "pre_sequence":  pre,
            "n_excited":     sum(post),
        })
    return records


def is_independent_set(bitstring, edges):
    """Check whether a bitstring (list of 0/1) is a valid independent set."""
    excited = set(i for i, b in enumerate(bitstring) if b == 1)
    for u, v in edges:
        if u in excited and v in excited:
            return False
    return True


def main():
    # FLUXION_ARN is injected by the Fluence webhook when scheduled via Fluence.
    # Fall back to BRAKET_DEVICE for local/non-Fluence runs.
    device_arn = os.environ.get("FLUXION_ARN") or os.environ.get("BRAKET_DEVICE", LOCAL_AHS_ARN)
    n_shots    = int(os.environ.get("N_SHOTS", 100))
    workspace  = os.environ.get("WORKSPACE", "/workspace")
    total_time = float(os.environ.get("TOTAL_TIME", 4.0))

    problem_path = os.path.join(workspace, "problem.json")
    if not os.path.exists(problem_path):
        print(f"[ahs-gateway] ERROR: {problem_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(problem_path) as f:
        problem = json.load(f)

    if problem.get("type") != "ahs":
        print("[ahs-gateway] ERROR: problem.json is not an AHS problem "
              f"(type={problem.get('type')}). Use braket-gateway for gate circuits.",
              file=sys.stderr)
        sys.exit(1)

    atom_positions = problem["atom_positions"]
    edges          = problem["edges"]
    n_atoms        = problem["n_atoms"]

    print(f"[ahs-gateway] n_atoms={n_atoms}, edges={len(edges)}, "
          f"shots={n_shots}, device={device_arn}, total_time={total_time}µs")

    ahs_program = build_ahs_program(atom_positions, total_time_us=total_time)

    t0 = time.time()
    try:
        if device_arn == LOCAL_AHS_ARN:
            result = run_local(ahs_program, n_shots)
        else:
            result = run_aquila(ahs_program, n_shots, device_arn)
    except Exception as e:
        print(f"[ahs-gateway] ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    elapsed = time.time() - t0

    records = parse_results(result, n_atoms)

    # Find best independent set in this batch
    valid = [r for r in records if is_independent_set(r["post_sequence"], edges)]
    if valid:
        best = max(valid, key=lambda r: r["n_excited"])
        best_size = best["n_excited"]
        best_set  = [i for i, b in enumerate(best["post_sequence"]) if b == 1]
    else:
        best_size = 0
        best_set  = []

    valid_fraction = len(valid) / len(records) if records else 0.0

    print(f"[ahs-gateway] valid shots: {len(valid)}/{len(records)} "
          f"({valid_fraction*100:.1f}%)")
    print(f"[ahs-gateway] best IS size: {best_size}, nodes: {best_set}")
    print(f"[ahs-gateway] elapsed: {elapsed:.2f}s")

    output = {
        "n_atoms":         n_atoms,
        "n_edges":         len(edges),
        "n_shots":         n_shots,
        "device":          device_arn,
        "elapsed_s":       round(elapsed, 3),
        "total_time_us":   total_time,
        "n_valid":         len(valid),
        "valid_fraction":  round(valid_fraction, 4),
        "best_is_size":    best_size,
        "best_is_nodes":   best_set,
        "measurements":    records,
    }

    out_path = os.path.join(workspace, "ahs_result.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[ahs-gateway] Wrote -> {out_path}")


if __name__ == "__main__":
    main()
