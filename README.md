# quantum-braket

Hybrid quantum-classical workflow experiments using [AWS Braket](https://aws.amazon.com/braket/) as the quantum backend. These experiments implement a Quantum Approximate Optimization Algorithm (QAOA) for graph max-cut, structured as a pipeline of four Kubernetes pods that can be scheduled independently or as a group.

The experiments are designed to run with the [Fluence](https://github.com/converged-computing/fluence) scheduler plugin for Kubernetes, which uses the Fluxion graph-based scheduler to do informed pod placement. However, the pods themselves have no dependency on Fluence and can run under any Kubernetes scheduler (or other containerized environment) or directly via `docker run`.

All quantum execution uses the **AWS Braket SV1 state vector simulator**, which is deterministic and reproducible — suitable for paper experiments. No real QPU access or QPU credits are required.

## Pipeline overview

```console
┌─────────────────────┐
│  problem-generator  │  Generates a random k-regular graph and writes
│                     │  the max-cut problem instance to a shared volume.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│     transpiler      │  Builds the QAOA ansatz circuit for the given
│                     │  problem and target backend topology.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   braket-gateway    │  Submits circuits to AWS Braket SV1, polls for
│                     │  results, and writes cost values back.
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│      optimizer      │  Runs COBYLA to update variational parameters;
│                     │  loops until convergence, writes final results.
└─────────────────────┘
```

## Repository layout

```
quantum-braket/
├── docker/
│   ├── problem-generator/   Dockerfile + entrypoint for the problem pod
│   ├── transpiler/          Dockerfile + entrypoint for the transpiler pod
│   ├── braket-gateway/      Dockerfile + entrypoint for the Braket gateway pod
│   └── optimizer/           Dockerfile + entrypoint for the optimizer pod
├── pods/
│   ├── problem-generator.yaml
│   ├── transpiler.yaml
│   ├── braket-gateway.yaml
│   └── optimizer.yaml
├── experiments/
│   ├── 1-scheduling/        Scheduler overhead and placement quality
│   ├── 2-routing/           Simulator vs. QPU adaptive routing
│   ├── 3-colocation/        Classical pod co-location latency
│   └── 4-scaling/           Problem size scaling (5–50 qubits)
├── scripts/
│   ├── run-pipeline.sh      Run all four pods end-to-end locally
│   └── setup-aws.sh         Configure AWS credentials and Braket region
└── hack/
    └── kind-config.yaml     Local kind cluster config for development
```

## Prerequisites

- Kubernetes cluster (or [kind](https://kind.sigs.k8s.io/) for local dev)
- AWS account with Braket enabled in `us-east-1` (SV1 is available in all Braket regions)
- AWS credentials available as a Kubernetes Secret (see below)
- `kubectl` configured to point at your cluster
- Docker (to build images) or access to the pre-built images at `ghcr.io/converged-computing/quantum-braket-*`

## Quick start

### 1. Create the AWS credentials secret

```bash
kubectl create secret generic aws-braket-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<your-key-id> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-secret-key> \
  --from-literal=AWS_DEFAULT_REGION=us-east-1
```

### 2. Create the shared workspace PVC

```bash
kubectl apply -f hack/workspace-pvc.yaml
```

### 3. Run the pipeline

```bash
# Apply all four pods in order (each waits for the previous via initContainers)
kubectl apply -f pods/problem-generator.yaml
kubectl apply -f pods/transpiler.yaml
kubectl apply -f pods/braket-gateway.yaml
kubectl apply -f pods/optimizer.yaml

# Or use the convenience script
./scripts/run-pipeline.sh
```

### 4. Check results

```bash
kubectl logs pod/qaoa-optimizer
# Results are also written to /workspace/results.json on the PVC
```

## Experiments

Each experiment directory contains a `README.md` with the hypothesis, method, and expected metrics, plus the YAML manifests and any analysis scripts needed.

| Experiment | What it measures |
|---|---|
| [1-scheduling](experiments/1-scheduling/) | Scheduler overhead; pod placement quality under Fluence vs. default |
| [2-routing](experiments/2-routing/) | Adaptive SV1 vs. QPU routing based on queue depth |
| [3-colocation](experiments/3-colocation/) | Classical pod co-location effect on hybrid loop latency |
| [4-scaling](experiments/4-scaling/) | Makespan and energy ratio across 5–50 qubit problem sizes |

## Building images

```bash
docker build -t ghcr.io/converged-computing/quantum-braket-problem-generator:latest \
  docker/problem-generator/

docker build -t ghcr.io/converged-computing/quantum-braket-transpiler:latest \
  docker/transpiler/

docker build -t ghcr.io/converged-computing/quantum-braket-braket-gateway:latest \
  docker/braket-gateway/

docker build -t ghcr.io/converged-computing/quantum-braket-optimizer:latest \
  docker/optimizer/
```

## AWS Braket costs

SV1 charges per task-second of simulation time. For the circuit sizes used in these experiments (≤ 20 qubits, ≤ 100 shots), costs are minimal (typically < $0.01 per full pipeline run). See [Braket pricing](https://aws.amazon.com/braket/pricing/) for details.

## Related projects

- [converged-computing/fluence](https://github.com/converged-computing/fluence) — Kubernetes scheduler plugin using Fluxion
- [ohtanim/SCA-HPCAsia-2026](https://github.com/ohtanim/SCA-HPCAsia-2026) — Hybrid quantum-classical HPC workflows with Prefect + Slurm
- [flux-framework/flux-sched](https://github.com/flux-framework/flux-sched) — Fluxion graph-based scheduler

## License

DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](LICENSE), [COPYRIGHT](COPYRIGHT), and [NOTICE](NOTICE) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-842614
