#!/usr/bin/env python3
"""
optimizer: COBYLA optimizer loop for QAOA variational parameters.

This pod orchestrates the full optimization loop. On each iteration it:
  1. Reads the current cost from /workspace/cost.json
  2. Updates (gamma, beta) using COBYLA
  3. Writes the new params to /workspace/params.json
  4. Signals the braket-gateway pod to run (in a Kubernetes workflow this
     is done by spawning a new Job; in local/script mode the gateway is
     called as a subprocess)
  5. Repeats until convergence or MAX_ITER is reached
  6. Writes final results to /workspace/results.json

In Kubernetes the optimizer is the "driver" pod. It runs the COBYLA loop
internally and calls the Braket gateway via a subprocess exec OR waits for
a gateway Job to complete (configurable via GATEWAY_MODE env var).

Environment variables:
  MAX_ITER        Maximum COBYLA iterations (default: 50)
  TOL             Convergence tolerance on cost improvement (default: 1e-4)
  GATEWAY_MODE    "subprocess" (local) or "job" (Kubernetes Job) (default: subprocess)
  WORKSPACE       Shared volume path (default: /workspace)
  N_SHOTS         Passed through to gateway (default: 1000)
"""

import json
import os
import subprocess
import sys
import time


def read_cost(workspace):
    cost_path = os.path.join(workspace, "cost.json")
    with open(cost_path) as f:
        return json.load(f)["cost"]


def write_params(workspace, gammas, betas, p):
    params_path = os.path.join(workspace, "params.json")
    with open(params_path, "w") as f:
        json.dump({"gammas": list(gammas), "betas": list(betas), "p": p}, f, indent=2)


def call_gateway_subprocess(workspace, iteration, n_shots):
    """Call the braket-gateway entrypoint directly as a subprocess."""
    env = os.environ.copy()
    env["WORKSPACE"]  = workspace
    env["ITERATION"]  = str(iteration)
    env["N_SHOTS"]    = str(n_shots)
    result = subprocess.run(
        [sys.executable, "/app/gateway.py"],
        env=env,
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gateway exited with code {result.returncode}")


def cobyla_optimize(workspace, p, max_iter, tol, n_shots, gateway_mode):
    """
    Run COBYLA optimization over (gammas, betas).

    SciPy's COBYLA minimizes, so we minimize -cost (maximizing cut value).
    """
    from scipy.optimize import minimize
    import numpy as np

    # Load initial params written by the transpiler
    params_path = os.path.join(workspace, "params.json")
    with open(params_path) as f:
        init_params = json.load(f)
    gammas0 = init_params["gammas"]
    betas0  = init_params["betas"]

    x0 = np.array(gammas0 + betas0, dtype=float)
    iteration_counter = [0]
    cost_history      = []

    def objective(x):
        it = iteration_counter[0]
        gammas = list(x[:p])
        betas  = list(x[p:])

        write_params(workspace, gammas, betas, p)

        if gateway_mode == "subprocess":
            call_gateway_subprocess(workspace, it, n_shots)
        else:
            raise NotImplementedError(
                "gateway_mode='job' requires external Kubernetes Job orchestration. "
                "Use gateway_mode='subprocess' for local/single-pod runs."
            )

        cost = read_cost(workspace)
        cost_history.append({"iteration": it, "cost": cost, "gammas": gammas, "betas": betas})
        print(f"[optimizer] iter={it:3d}  cost={cost:.6f}  "
              f"gamma0={gammas[0]:.4f}  beta0={betas[0]:.4f}")

        iteration_counter[0] += 1
        return -cost  # COBYLA minimizes; we maximize cut

    result = minimize(
        objective,
        x0,
        method="COBYLA",
        options={"maxiter": max_iter, "rhobeg": 0.5, "catol": tol},
    )

    best_x      = result.x
    best_gammas = list(best_x[:p])
    best_betas  = list(best_x[p:])
    best_cost   = -result.fun

    return {
        "best_cost": best_cost,
        "best_gammas": best_gammas,
        "best_betas": best_betas,
        "n_iterations": iteration_counter[0],
        "converged": result.success,
        "scipy_message": result.message,
        "cost_history": cost_history,
    }


def compute_approximation_ratio(best_cost, problem):
    """
    Compute the QAOA approximation ratio = best_cost / max_possible_cut.
    For unit-weight graphs, max cut <= |E|.
    (Exact max-cut is NP-hard in general; we use |E| as the upper bound.)
    """
    n_edges = len(problem["edges"])
    return best_cost / n_edges if n_edges > 0 else 0.0


def main():
    max_iter     = int(os.environ.get("MAX_ITER", 50))
    tol          = float(os.environ.get("TOL", 1e-4))
    gateway_mode = os.environ.get("GATEWAY_MODE", "subprocess")
    workspace    = os.environ.get("WORKSPACE", "/workspace")
    n_shots      = int(os.environ.get("N_SHOTS", 1000))

    # Load problem to get p and n_qubits
    problem_path = os.path.join(workspace, "problem.json")
    if not os.path.exists(problem_path):
        print(f"[optimizer] ERROR: {problem_path} not found", file=sys.stderr)
        sys.exit(1)
    with open(problem_path) as f:
        problem = json.load(f)

    params_path = os.path.join(workspace, "params.json")
    if not os.path.exists(params_path):
        print(f"[optimizer] ERROR: {params_path} not found. "
              "Has the transpiler pod completed?", file=sys.stderr)
        sys.exit(1)
    with open(params_path) as f:
        p = json.load(f).get("p", 1)

    print(f"[optimizer] Starting COBYLA: max_iter={max_iter}, tol={tol}, "
          f"p={p}, n_shots={n_shots}, mode={gateway_mode}")

    # Run the initial circuit evaluation (iteration 0) before the loop
    # so that cost.json exists when COBYLA calls objective() the first time
    write_params(workspace, json.load(open(params_path))["gammas"],
                 json.load(open(params_path))["betas"], p)
    if gateway_mode == "subprocess":
        call_gateway_subprocess(workspace, 0, n_shots)

    t0 = time.time()
    opt = cobyla_optimize(workspace, p, max_iter, tol, n_shots, gateway_mode)
    elapsed = time.time() - t0

    approx_ratio = compute_approximation_ratio(opt["best_cost"], problem)

    results = {
        "problem": {
            "n_nodes": problem["n_nodes"],
            "n_edges": len(problem["edges"]),
            "n_qubits": problem["n_qubits"],
            "k": problem["k"],
            "seed": problem["seed"],
        },
        "qaoa": {
            "p": p,
            "n_shots": n_shots,
            "device": os.environ.get("BRAKET_DEVICE",
                "arn:aws:braket:::device/quantum-simulator/amazon/sv1"),
        },
        "optimization": {
            "best_cost": opt["best_cost"],
            "best_gammas": opt["best_gammas"],
            "best_betas": opt["best_betas"],
            "n_iterations": opt["n_iterations"],
            "converged": opt["converged"],
            "scipy_message": opt["scipy_message"],
            "approximation_ratio": approx_ratio,
            "total_elapsed_s": elapsed,
        },
        "cost_history": opt["cost_history"],
    }

    results_path = os.path.join(workspace, "results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[optimizer] ===== Optimization complete =====")
    print(f"  best cost          : {opt['best_cost']:.6f}")
    print(f"  approximation ratio: {approx_ratio:.4f}")
    print(f"  iterations         : {opt['n_iterations']}")
    print(f"  converged          : {opt['converged']}")
    print(f"  total elapsed      : {elapsed:.2f}s")
    print(f"  results written to : {results_path}")


if __name__ == "__main__":
    main()
