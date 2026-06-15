# quantum-braket

Hybrid quantum-classical workflow experiments using [AWS Braket](https://aws.amazon.com/braket/) as the quantum backend. These experiments implement a Quantum Approximate Optimization Algorithm (QAOA) for graph max-cut, structured as a pipeline of four Kubernetes pods that can be scheduled independently or as a group.

The experiments are designed to run with the [Fluence](https://github.com/converged-computing/fluence) scheduler plugin for Kubernetes, which uses the Fluxion graph-based scheduler to do informed pod placement. However, the pods themselves have no dependency on Fluence and can run under any Kubernetes scheduler or directly via `docker run`.

All quantum execution uses the **AWS Braket SV1 state vector simulator**, which is deterministic and reproducible — suitable for paper experiments. No real QPU access or QPU credits are required.

## Pipelines

Two workload types, each a separate pod pipeline sharing the same emptyDir workspace:

### Gate-based QAOA (max-cut on k-regular graphs)

```console
problem-generator → transpiler → gateway → optimizer
```

Backends: SV1 (default), TN1, IonQ Forte, Rigetti Ankaa-3 / Cepheus.
Fluence resource type: `gate-simulator` or `gate-qpu`.

### Analog Hamiltonian Simulation (MIS on unit disk graphs)

```console
ahs-problem-generator → ahs-gateway → mis-postprocessor
```

Backends: local AHS simulator (default), QuEra Aquila.
Fluence resource type: `ahs`.

**These pipelines are mutually incompatible at the backend level.** Submitting
a gate circuit to an AHS backend (or vice versa) fails at the AWS API. Fluence
enforces correct routing at schedule time via typed QPU resource requests —
a pod requesting `fluxion.flux-framework.org/ahs` will never be matched to
a gate-simulator or gate-qpu backend, and vice versa.


## Prerequisites

- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installing-from-release-binaries) (for local dev) or an existing Kubernetes cluster
- AWS account with Braket enabled in `us-east-1` (SV1 is available in all Braket regions)
- AWS credentials available as a Kubernetes Secret (see below)
- `kubectl` configured to point at your cluster
- Docker (to build images) or access to the pre-built images at `ghcr.io/converged-computing/quantum-braket-*`

## Cluster setup with Fluence

These experiments are designed to run with [Fluence](https://github.com/converged-computing/fluence),
the Kubernetes scheduler plugin that uses the Fluxion graph-based scheduler.
The pods work with any Kubernetes scheduler, but the scheduling experiments
(experiment 1) require Fluence.

### 1. Create a kind cluster with gang scheduling feature gates

Fluence requires `GangScheduling` and `GenericWorkload` feature gates. Download
the latest kind config directly from the Fluence repo:

```bash
wget https://raw.githubusercontent.com/converged-computing/fluence/main/deploy/kind-config.yaml
kind create cluster --image kindest/node:v1.36.1 --config kind-config.yaml
```

### 2. Install Fluence

```bash
docker pull ghcr.io/converged-computing/fluence:latest
kind load docker-image ghcr.io/converged-computing/fluence:latest
kubectl apply -f https://raw.githubusercontent.com/converged-computing/fluence/main/deploy/fluence.yaml
```

Verify:

```bash
kubectl get pods -n kube-system | grep fluence
```

### 3. Install the quantum resources add-on

This registers QPU backends in the Fluxion graph and advertises them via a
device plugin so the scheduler can match quantum resource requests:

```bash
# Use our AWS Braket-specific resources config (not the IBM one from upstream)
kubectl apply -f hack/fluence-resources.yaml
kubectl rollout restart deployment/fluence -n kube-system

# Confirm QPU resources are visible on nodes
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable}{"\n"}{end}' \
  | grep fluxion.flux-framework.org
```

The `hack/fluence-resources.yaml` in this repo registers the AWS Braket SV1 and
TN1 simulators as QPU vertices. Real QPU backends (IonQ Aria, Rigetti Ankaa-3)
are present but commented out — uncomment to enable them if you have QPU access
on your AWS account.

To use Fluence scheduling, uncomment `schedulerName: fluence` in the pod
manifests under `pods/`. Leave it commented to use the default scheduler.

## Quick start

### 1. Enable the Braket service-linked IAM role

This is a one-time step per AWS account. If you have never used Braket before,
the required IAM service role won't exist yet:

```bash
aws iam create-service-linked-role --aws-service-name braket.amazonaws.com
```

If the role already exists you will get a harmless error saying so — safe to ignore.

### 2. Create the AWS credentials secret

```bash
kubectl create secret generic aws-braket-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<your-key-id> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-secret-key> \
  --from-literal=AWS_DEFAULT_REGION=us-east-1
```

### 2. Run the pipeline

```bash
# Default scheduler
SCHEDULER=default bash experiments/1-scheduling/run-pipeline.sh

# Or with Fluence (requires Fluence installed — see Cluster setup above)
SCHEDULER=fluence bash experiments/1-scheduling/run-pipeline.sh
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
