# Experiment 2 — Gang Scheduling: Fluence vs Default Scheduler

## What this experiment measures

The two-queue problem with a realistic gang workflow. A gang of **N identical
pods** runs **one** quantum task and shares its result:

- **producer** (completion index 0) builds the QAOA circuit, submits the single
  real QPU task, and publishes its task id;
- **consumers** (the other N−1) do **not** submit — they obtain the producer's
  task id, fetch the **shared** result, and process their shot partition.

Both arms run this same gang. The only variable is the **scheduler**, and with it
whether the consumers can be held off the cluster while the QPU queue drains.

**Fluence (`coordination: shared`).** The pods request
`fluxion.flux-framework.org/qpu`, so Fluence places the quantum work — it matches
a backend from the resource graph by **name** and injects `FLUXION_ARN` — elects
the producer, and admits the N−1 consumers `SchedulingGated` (zero node
resources). A sidecar watches the producer's QPU task and ungates the consumers
as the result becomes ready, handing each the task id via `FLUENCE_QUANTUM_JOB_ID`.
Gated consumers hold **no** classical node during the queue wait, so consumer
idle ≈ 0.

**Default (baseline).** Native Kubernetes gang scheduling via a
`scheduling.k8s.io/v1alpha2` PodGroup on the default scheduler — all N pods start
together, all-or-nothing. There is no Fluence, so the producer/consumer role is
derived from the Job completion index, the producer names its device **manually**
via `BRAKET_DEVICE` (no Fluxion to inject it), and nothing gates the consumers:
they idle on classical nodes for the whole QPU queue wait, discovering the
producer's task id from S3.

The comparison isolates one variable: scheduler awareness of the quantum queue.
(Experiment 4 isolates a different variable — shared vs independent coordination
*mode* under Fluence, for cost minimization. This one is about the *scheduler*.)

## Key metric

`total_consumer_idle_s` = Σ over consumers of (`result_ready_ts` −
`consumer_start_ts`) — node-seconds a consumer is up but has no result yet, i.e.
the wasted classical compute.

- **fluence** ≈ 0 (consumers stay gated, consuming nothing, until the task is
  ~ready);
- **default** ≈ (N−1) × T_queue (every consumer idles through the whole wait).

`mean_consumer_idle_s` and `qpu_queue_wait_s` are reported per run alongside it.

> On a simulator (sv1/dm1/tn1) there is no real queue, so the contrast reflects
> co-scheduling overhead, not a QPU backlog. The headline queue contrast needs a
> **busy** QPU — check the device's queue depth before paying for a run.

## How roles and devices are wired

|                       | fluence arm                              | default arm                          |
|-----------------------|------------------------------------------|--------------------------------------|
| pod set               | indexed Job, N pods, `coordination: shared` | indexed Job, N pods, native PodGroup |
| role                  | injected by the webhook (index 0 = producer) | from `JOB_COMPLETION_INDEX` (0 = producer) |
| device                | `require-backend: <name>` → Fluxion injects `FLUXION_ARN` | `BRAKET_DEVICE: <arn>` set by hand    |
| consumers during wait | `SchedulingGated` (no node)              | running and idle                     |
| task-id hand-off      | `FLUENCE_QUANTUM_JOB_ID` (sidecar)       | producer publishes to S3, consumers poll |

The experiment **never names an ARN for the fluence arm** — it sets only a
backend name and lets Fluxion resolve it. The ARN appears only on the default
arm, because without Fluence you must pick the device yourself; that asymmetry is
the point.

## S3 task-id hand-off (default arm only)

The producer writes `{task_arn, n_qubits, n_consumers, region}` to
`s3://<braket-bucket>/fluence-gang/<RUN_ID>/producer-task.json` right after
submitting. Default-arm consumers poll that key (the Fluence arm never reads it —
it gets the id from `FLUENCE_QUANTUM_JOB_ID`). `RUN_ID` is unique per run, so a
run can never read a previous run's object — no manual S3 cleanup between runs.
The bucket defaults to `amazon-braket-<region>-<account>`; override with
`S3_BUCKET`.

## Prerequisites

- A cluster with Fluence deployed (the role-aware webhook + sidecar) and the
  default scheduler serving `scheduling.k8s.io/v1alpha2` PodGroups. See
  `cluster/setup.sh`.
- The resource graph ConfigMap (`hack/fluence-resources.yaml`) loaded, so
  `require-backend` names resolve.
- A Braket credentials secret in the namespace:
  ```
  kubectl create secret generic aws-braket-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --from-literal=AWS_DEFAULT_REGION=us-east-1
  ```
- The gang image `ghcr.io/converged-computing/quantum-braket-gang:latest`
  (built from `docker/gang/`).

## Choosing a backend (and avoiding surprise QPU cost)

The fluence arm picks the device by **name** (`--backend`, e.g. `sv1`, `dm1`,
`tn1`, `rigetti_cepheus`, `iqm_garnet`); Fluxion resolves it to an ARN. The
default arm uses the ARN you pass with **`--device-arn`**. For a fair comparison
point both at the **same device**.

Simulators (`sv1`/`dm1`/`tn1`) are billed per minute and have no real queue; real
QPUs (`rigetti_*`, `iqm_*`) charge **per task** and may have long queues. The
orchestrator prints a cost warning and asks for confirmation when the fluence
backend isn't a known simulator, or the default `--device-arn` contains `/qpu/`
(skip with `--yes`).

## Running the experiment

```bash
# Offline: render the patched manifests (no cluster), to inspect what gets applied
python3 run_experiment.py --schedulers fluence --n-consumers 4 --render

# SV1 simulator, both arms (mechanism check; no real queue)
python3 run_experiment.py --backend sv1 --schedulers default fluence

# Sweep consumer count, repeat for mean ± stdev  (N = consumers + 1) DO TWICE
python3 run_experiment.py --backend sv1 --schedulers default fluence --n-consumers 2 4 8 --repeat 5
python3 run_experiment.py --backend dm1 --schedulers default fluence --n-consumers 2 4 8 --repeat 5
# python3 run_experiment.py --backend tn1 --schedulers default fluence --n-consumers 2 4 8 --repeat 5
python3 run_experiment.py --backend rigetti_cepheus --schedulers default fluence --n-consumers 2 4 8 --repeat 5 --n-shots 100
python3 run_experiment.py --backend iqm_garnet --schedulers default fluence --n-shots 100


# Single arm only
python3 run_experiment.py --backend sv1 --scheduler fluence

# Plot
python3 plot_results.py results/combined-sv1-*.csv -o img/gang-sv1.png
```
```console
stats:
  dm1 2w default  n=10 mean=17.32 median=17.26 stdev=0.30
  dm1 4w default  n=10 mean=34.84 median=34.64 stdev=0.56
  dm1 8w default  n=10 mean=74.18 median=74.20 stdev=0.75
  dm1 2w fluence  n=5 mean=6.26 median=6.28 stdev=0.12
  dm1 4w fluence  n=5 mean=16.90 median=16.79 stdev=0.30
  dm1 8w fluence  n=5 mean=62.80 median=62.14 stdev=1.28
  rigetti_cepheus 2w default  n=5 mean=21.87 median=18.11 stdev=5.57
  rigetti_cepheus 4w default  n=5 mean=56.41 median=55.89 stdev=1.45
  rigetti_cepheus 8w default  n=5 mean=95.72 median=96.46 stdev=19.89
  rigetti_cepheus 2w fluence  n=5 mean=6.66 median=6.42 stdev=0.61
  rigetti_cepheus 4w fluence  n=5 mean=17.17 median=17.17 stdev=0.49
  rigetti_cepheus 8w fluence  n=5 mean=64.07 median=63.24 stdev=1.37
  sv1 2w default  n=5 mean=17.12 median=17.10 stdev=0.10
  sv1 4w default  n=6 mean=38.14 median=34.48 stdev=8.95
  sv1 8w default  n=5 mean=75.68 median=75.27 stdev=1.08
  sv1 2w fluence  n=5 mean=6.37 median=6.33 stdev=0.17
  sv1 4w fluence  n=6 mean=17.09 median=16.82 stdev=0.76
  sv1 8w fluence  n=5 mean=63.67 median=62.34 stdev=2.60
  tn1 2w default  n=15 mean=44.68 median=54.91 stdev=19.34
  tn1 4w default  n=15 mean=85.15 median=109.57 stdev=37.07
  tn1 8w default  n=15 mean=186.98 median=222.97 stdev=86.37
  tn1 2w fluence  n=14 mean=3.40 median=4.47 stdev=2.26
  tn1 4w fluence  n=10 mean=10.89 median=11.13 stdev=0.56
  tn1 8w fluence  n=10 mean=32.07 median=31.72 stdev=2.16
```

### Useful flags

- `--backend NAME` — fluence arm's `require-backend` (default `sv1`).
- `--device-arn ARN` — default arm's `BRAKET_DEVICE` (default the SV1 ARN).
- `--schedulers default fluence` / `--scheduler <one>`.
- `--n-consumers 2 4 8` — consumer counts to sweep; total pods N = consumers + 1.
- `--n-shots`, `--n-nodes`, `--seed`, `--repeat`, `--namespace`, `--out`.
- `--render` — print patched manifests and exit. `--yes` — skip the cost prompt.
- `--keep` — leave the Job/PodGroup in the cluster after the run.
- `FLUENCE_GANG_TIMEOUT_S` (env) — orchestrator job wait AND the in-pod
  `CONSUMER_TIMEOUT_S`; **default 0 = wait indefinitely** (a real QPU queue can
  run for days). Set a positive number of seconds to cap it.
- `POLL_TIMEOUT_S` (env) — bound on the Braket result poll in both arms
  (default 2592000 = 30 days). This is what keeps `.result()` from giving up
  mid-queue on a busy QPU.

## Output

Per-run CSVs `combined-<backend>-<scheduler>-n<N>-<ts>.csv` and a combined
`combined-<backend>-<ts>.csv` with: `scheduler`, `backend`, `n_pods`,
`n_consumers`, `total_consumer_idle_s`, `mean_consumer_idle_s`,
`qpu_queue_wait_s`, `n_consumers_observed`, `batch_wall_s`, `job_ok`. With
`--repeat > 1` an `aggregated-<backend>-<ts>.csv` adds `idle_mean_s` /
`idle_stdev_s` per (scheduler, N).

## Manifests

- `pods/fluence/gang/pipeline-gang.yaml` — indexed Job, `coordination: shared`,
  `require-backend` (name only), pods request the qpu resource; the webhook
  elects the producer, gates consumers, and injects roles + the task id.
- `pods/default/gang/pipeline-gang.yaml` — native `PodGroup` (minCount N) + an
  indexed Job whose pods join it; `BRAKET_DEVICE` set by hand; role from the
  completion index.

The orchestrator patches names, counts (`completions`/`parallelism`/`group-size`/
`minCount` = N, `N_CONSUMERS` = N−1), `require-backend` (fluence), `BRAKET_DEVICE`
(default), and the problem env, so both manifests scale with `--n-consumers`.
