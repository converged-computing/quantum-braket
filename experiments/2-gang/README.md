# Experiment 2 — Gang Scheduling: Fluence vs Default Scheduler

## What this experiment measures

The two-queue problem with a realistic gang workflow: one leader pod submits a
QAOA circuit to a QPU, and N worker pods process the measurement shots in
parallel. Both conditions **gang-schedule** the pods (1 leader + N workers)
all-or-nothing; the only variable is whether the scheduler is aware of the QPU
queue.

**Default (baseline):** native Kubernetes gang scheduling via a
`scheduling.k8s.io/v1alpha2` PodGroup on the default scheduler. All pods start
together. The leader submits to the QPU; the workers then idle on classical
nodes for the entire QPU queue wait, consuming CPU/memory while doing nothing
useful. The leader selects its device explicitly via `BRAKET_DEVICE` (Fluence is
not placing the work).

**Fluence:** the same gang, but Fluence is QPU-aware. The leader requests
`fluxion.flux-framework.org/qpu`, so Fluence places the quantum work (injecting
the `FLUXION_*` env contract, including `FLUXION_ARN`) and the quantum handler
injects a sidecar. The workers are admitted `SchedulingGated` — zero node
resources — and carry the `fluence-quantum-classical` priority class (set by the
webhook at admission, since it is immutable afterward). The sidecar finds the
leader's QPU task, watches its queue position, and ungates the workers as the
result becomes ready.

The comparison isolates one variable: scheduler awareness of the quantum queue.

## Key metric

`total_worker_idle_s` = sum over workers of (leader-ready-seen − worker-start) —
the wasted classical compute the paper characterizes. With Fluence this is small
(workers stay gated, consuming nothing, until the QPU task is ready); without it
the workers idle through the leader's whole startup + QPU wait. `worker_idle_s`
is reported per run alongside `qpu_wait_s` and `worker_node_seconds` (the summed
idle across workers).

> Note on backends: on a simulator (sv1) there is no real queue, so the contrast
> reflects co-scheduling overhead (workers idling through leader startup), not a
> QPU backlog. The headline queue contrast requires a **busy** QPU — check the
> device queue before paying for a run (see "Choosing a backend").

HOWEVER - we need to use the simulators so the queue wait time is fairly comparable.

## S3 coordination

Both conditions use S3 for leader→worker coordination, namespaced by a unique
per-run id (`RUN_ID`, set by the orchestrator) so concurrent or repeated runs
never collide — no manual cleanup between runs:

```
s3://<bucket>/fluence-gang/<RUN_ID>/leader-ready     # leader writes when QPU done
s3://<bucket>/fluence-gang/<RUN_ID>/worker-<i>.json  # each worker writes partial result
s3://<bucket>/fluence-gang/<RUN_ID>/final.json       # leader writes aggregated result
```

`<bucket>` is the per-region Braket bucket `amazon-braket-<region>-<account>`.
The region is the **device's** region (a cross-region device such as Rigetti in
`us-west-1` writes to the us-west-1 bucket); both leader and workers derive it
from `BRAKET_REGION`, set per run by the orchestrator, so they always agree.

With Fluence, workers also receive the vendor-neutral job id via the
`fluence.flux-framework.org/quantum-job-id` annotation (stamped by the sidecar at
ungate time, read as `FLUENCE_QUANTUM_JOB_ID`), so they fetch the result directly
without scanning S3.

## Prerequisites

The cluster runs on **GKE with alpha features enabled** (`--enable-kubernetes-alpha`
turns on all alpha API groups, including `scheduling.k8s.io/v1alpha2` PodGroups,
with no extra runtime-config). `cluster/setup.sh` provisions the cluster and
installs Fluence, the device plugin, and the Braket resource graph.

```bash
cd cluster && bash setup.sh    # creates the GKE alpha cluster + installs Fluence
```

`setup.sh` also creates the AWS Braket credentials secret if you provide the
values; otherwise create it yourself before running:

```bash
kubectl create secret generic aws-braket-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --from-literal=AWS_DEFAULT_REGION=us-east-1
```

The Braket resource graph (`../../hack/fluence-resources.yaml`) is applied by
`setup.sh`. It contains only the devices this experiment uses (the sv1/tn1/dm1
gate simulators plus a small set of gate QPUs); it deliberately excludes AHS
devices and the most expensive QPUs. If you edit it, re-apply and restart the
scheduler and webhook so both re-read it:

```bash
kubectl apply -f ../../hack/fluence-resources.yaml
kubectl rollout restart -n kube-system deployment/fluence deployment/fluence-webhook
```

## Choosing a backend (and avoiding surprise QPU cost)

Real QPUs cost real money per task, and the queue-contrast result only appears
when the device actually has a queue. Before a paid run, check the device:

```bash
aws braket get-device \
  --device-arn arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q \
  --region us-west-1 \
  --query '{status:deviceStatus,queue:deviceQueueInfo}'
```

If `queueSize` is `0`, a run will only reproduce the simulator-style result (no
queue to hide) while still charging for the task — wait for a non-zero queue, or
use a simulator for mechanism checks.

The Fluence arm pins the device with leader annotations so an unconstrained
`qpu` request can't wander onto a more expensive device:

- `fluence.flux-framework.org/require-qrmi_type: braket-gate` — keep a gate
  circuit off analog/AHS devices.
- `fluence.flux-framework.org/require-backend: <name>` — pin a specific device
  (e.g. `sv1`). The orchestrator sets this to the run's `--backend`.

## Running the experiment

The orchestrator drives both conditions, patches the manifests per run (unique
`RUN_ID`, `BRAKET_REGION`, `require-backend`, worker count, etc.), collects
timing from pod logs, and writes per-run and combined CSVs to `--out` (default
`results/`). No S3 cleanup is needed between runs.

```bash
# List the backends the orchestrator knows:
python3 run_experiment.py --list-backends

# SV1 simulator, both conditions (mechanism check; no real queue):
python3 run_experiment.py --backend sv1 --schedulers default fluence

# Sweep worker counts (both conditions):
python3 run_experiment.py --backend sv1 --schedulers default fluence --n-workers 2 4 8

# Vary problem size (qubits) and shots:
python3 run_experiment.py --backend sv1 --schedulers default fluence --n-nodes 8 10 12 --n-shots 1000

# A real QPU — check the queue first (see above). This charges per task:
python3 run_experiment.py --backend rigetti_cepheus --schedulers default fluence

# Single condition only:
python3 run_experiment.py --backend sv1 --scheduler fluence

# Plot results
python3 plot_results.py results/combined-*.csv --workers 4 -o cross.png
```
```console
 note: dm1/default@2w averages 10 runs (idle each=[25, 26, 25, 25, 24, 24, 25, 25, 25, 25])
  note: dm1/fluence@2w averages 10 runs (idle each=[5, 5, 5, 4, 5, 5, 5, 4, 5, 6])
  note: sv1/default@2w averages 10 runs (idle each=[25, 25, 25, 25, 25, 25, 25, 24, 25, 25])
  note: sv1/fluence@2w averages 10 runs (idle each=[4, 4, 5, 5, 5, 4, 5, 5, 5, 4])
  note: tn1/default@2w averages 10 runs (idle each=[76, 54, 55, 56, 55, 55, 55, 54, 57, 55])
  note: tn1/fluence@2w averages 10 runs (idle each=[5, 4, 5, 5, 5, 5, 5, 4, 4, 5])
  note: dm1/default@4w averages 10 runs (idle each=[50, 52, 51, 54, 50, 49, 51, 51, 50, 51])
  note: dm1/fluence@4w averages 10 runs (idle each=[11, 11, 12, 10, 11, 10, 10, 11, 10, 11])
  note: sv1/default@4w averages 10 runs (idle each=[49, 97, 50, 48, 49, 50, 51, 49, 50, 49])
  note: sv1/fluence@4w averages 10 runs (idle each=[11, 11, 13, 11, 11, 10, 11, 10, 11, 10])
  note: tn1/default@4w averages 10 runs (idle each=[111, 110, 109, 110, 111, 111, 112, 110, 111, 110])
  note: tn1/fluence@4w averages 10 runs (idle each=[11, 11, 11, 11, 10, 11, 11, 11, 10, 11])
  note: dm1/default@8w averages 10 runs (idle each=[103, 134, 105, 104, 101, 102, 102, 103, 105, 104])
  note: dm1/fluence@8w averages 10 runs (idle each=[31, 29, 30, 29, 33, 32, 32, 31, 29, 30])
  note: sv1/default@8w averages 10 runs (idle each=[104, 101, 183, 105, 106, 101, 101, 99, 100, 99])
  note: sv1/fluence@8w averages 10 runs (idle each=[31, 29, 30, 31, 30, 31, 32, 30, 33, 30])
  note: tn1/default@8w averages 10 runs (idle each=[347, 223, 225, 273, 221, 222, 226, 227, 226, 226])
  note: tn1/fluence@8w averages 10 runs (idle each=[34, 30, 35, 32, 31, 32, 29, 33, 35, 30])
wrote img/combined-dm1-20260623T184650-cross-all.png

stats:
  dm1 2w default  n=10 mean=24.94 median=24.82 stdev=0.58
  dm1 4w default  n=10 mean=50.98 median=51.02 stdev=1.48
  dm1 8w default  n=10 mean=106.38 median=103.39 stdev=9.64
  dm1 2w fluence  n=10 mean=4.85 median=4.79 stdev=0.54
  dm1 4w fluence  n=10 mean=10.63 median=10.70 stdev=0.57
  dm1 8w fluence  n=10 mean=30.43 median=30.48 stdev=1.30
  sv1 2w default  n=10 mean=24.95 median=25.02 stdev=0.37
  sv1 4w default  n=10 mean=54.14 median=49.42 stdev=14.99
  sv1 8w default  n=10 mean=109.76 median=100.96 stdev=25.88
  sv1 2w fluence  n=10 mean=4.56 median=4.62 stdev=0.34
  sv1 4w fluence  n=10 mean=10.94 median=10.79 stdev=0.76
  sv1 8w fluence  n=10 mean=30.72 median=30.49 stdev=1.38
  tn1 2w default  n=10 mean=57.29 median=55.19 stdev=6.59
  tn1 4w default  n=10 mean=110.47 median=110.44 stdev=1.04
  tn1 8w default  n=10 mean=241.67 median=226.04 stdev=40.07
  tn1 2w fluence  n=10 mean=4.76 median=4.91 stdev=0.45
  tn1 4w fluence  n=10 mean=10.89 median=11.13 stdev=0.56
  tn1 8w fluence  n=10 mean=32.07 median=31.72 stdev=2.16

```

If you need cleanup between tests (I did this a lot during development):

```bash
kubectl delete podgroup -A --all --ignore-not-found
kubectl delete pods -A -l app=quantum-braket-gang --ignore-not-found
kubectl rollout restart -n kube-system deployment/fluence
kubectl rollout status -n kube-system deployment/fluence --timeout=120s
kubectl rollout restart -n kube-system deployment/fluence-webhook
kubectl rollout status -n kube-system deployment/fluence-webhook --timeout=120s
```

Here is what I ran to generate the final data for the simulation experiments:

```bash
for b in sv1 dm1 tn1; do
  python3 run_experiment.py --backend $b --schedulers default fluence \
    --n-workers 2 4 8 --repeat 10
done
```

### Useful flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--backend` | `sv1` | Backend name (see `--list-backends`). |
| `--schedulers` | — | One or both of `default fluence` (overrides `--scheduler`). |
| `--scheduler` | — | A single scheduler (`default` or `fluence`). |
| `--n-workers` | `4` | Worker counts to sweep (space-separated). |
| `--n-shots` | `1000` | Shots per task. |
| `--n-nodes` | `10` | Problem sizes in qubits (space-separated). |
| `--seed` | `42` | RNG seed. |
| `--namespace` | `default` | Namespace to run in. |
| `--out` | `results/` | Output directory for CSVs. |
| `--keep-pods` | off | Don't delete pods after a run (for debugging). |
| `--list-backends` | — | Print known backends and exit. |

For the default condition the orchestrator sets `BRAKET_DEVICE` to the same
backend the Fluence condition is pinned to, so the QPU queue wait is comparable;
the methods section notes the default backend is fixed manually.

## Output

Each run writes `results/combined-<backend>-<scheduler>-<timestamp>.csv`, and a
combined `results/combined-<backend>-<timestamp>.csv` across the runs in the
invocation, with `qpu_wait_s`, `worker_idle_s`, and `worker_node_seconds` per
condition.

## Manifests

- `pods/default/gang/pipeline-gang.yaml` — PodGroup (minCount = 1 + N) + leader +
  N workers, native gang on the default scheduler; leader sets `BRAKET_DEVICE`.
- `pods/fluence/gang/pipeline-gang.yaml` — leader (requests `qpu`, carries the
  `require-*` pinning annotations) + N workers with the group label; the Fluence
  webhook creates the PodGroup, gates the workers, sets their priority class, and
  injects the sidecar onto the leader.
