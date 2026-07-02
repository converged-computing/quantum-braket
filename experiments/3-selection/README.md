# Experiment 5 — Cost / queue-aware backend selection

Demonstrates that making backend selection **policy-driven** (via the
[`kubectl-fluence`](https://github.com/converged-computing/kubectl-fluence)
client-side plugin) reduces realized cost — and, for real QPUs, can route around
queues — compared with a capability-only request that is blind to cost/queue.

The plugin resolves the backend on the client (using your credentials for the
queue case) and pins it via the `fluence.flux-framework.org/backend` annotation,
which Fluence honors at schedule time. Credentials never enter the cluster.

The selection arm uses `kubectl fluence select --attributes cost-attributes.yaml`
to resolve the backend from the Job's policy; the orchestrator then bakes the
chosen backend (`FLUXION_ARN`/`FLUXION_BACKEND`) into the Job's pod template env
and applies it. The Fluence-created producer is a verbatim container copy, so it
inherits that env and submits to the selected backend — Fluence honors it without
any Fluence-side change. The orchestrator reads the chosen backend from the
plugin's stderr. (For a human at
a terminal, `kubectl fluence apply --confirm` shows the selection and prompts for
approval before applying; the experiment does not use `--confirm` so runs proceed
unattended.)

See [`DESIGN.md`](DESIGN.md) for the full rationale, the match-policy caveat, and
the budget math.

**Workload shape.** The workload is a **gang expressed as a `batch/v1` Job**
(`--group-size` = parallelism, default 2). Fluence turns the Job's N pods into a
gated, consumer gang (the "workers" — marked by Fluence via `FLUENCE_COORDINATION_ROLE`,
no role annotation) and creates one `<job> producer (index 0)` pod (the "leader") that
makes the single real submission to the selected backend; the gang members fetch
that one task's result by job id. Exactly one quantum task is submitted per run
regardless of N. The Job name is the gang/PodGroup name and its parallelism is
the gang size — Fluence reads both from the Job owner, so there is no group label
or group-size annotation to manage. We measure *which backend the producer ran
on and what it cost*; the gang is the group abstraction the paper's story uses.

```
3-selection/
├── README.md               # this file
├── DESIGN.md               # hypotheses, methodology, caveats, budget
├── cost-attributes.yaml    # backend cost/capability table (the plugin's cost source)
├── run_selection.py        # orchestrator (both arms, both sub-experiments)
├── plot_selection.py       # renders results into img/
├── manifests/
│   └── gang-template.yaml      # the gang as a batch/v1 Job, templated
├── results/                # CSVs land here
└── img/                    # plots land here
```

## What it compares

Two sub-experiments, each with two arms:

| sub-exp | baseline arm | selection arm | metric |
|---------|--------------|---------------|--------|
| **cost**  | capability-only request (cost-blind) | `+ min-cost` policy | realized USD/run |
| **queue** | capability-only request (queue-blind) | `+ online-only,min-queue` | queue depth at submit |

- **cost** uses simulators + all real QPUs (Rigetti, IQM Garnet, IQM Emerald).
  A cost-blind match can land on the
  expensive QPU; `min-cost` always pins the cheapest satisfying backend. There
  is **no qubit constraint in the request** — capability matching (qubit count,
  etc.) is the scheduler's job against the resource graph, not something the
  client stamps. The candidate list scopes the pool and the policy is simply
  `min-cost`.
- **queue** is **QPU-only** (simulators have no queue) — so it costs real money
  and the queue is exogenous. Treat it as illustrative/feasibility, keep shots
  tiny, run few repeats.

## Prerequisites

1. **A Fluence-enabled cluster** (Kubernetes **1.36.x** — Fluence is version
   sensitive). Your `kubectl` context must point at it. See `experiments/2-gang`
   setup for bringing one up.

2. **The `kubectl fluence` plugin** on your `PATH`:
   ```sh
   git clone https://github.com/converged-computing/kubectl-fluence
   cd kubectl-fluence && go build -o kubectl-fluence ./cmd/kubectl-fluence
   sudo install -m 0755 kubectl-fluence /usr/local/bin/
   kubectl fluence version
   ```

3. **The `fluence-resources` ConfigMap** in the cluster must list the candidate
   backend names — and they must match the names in `cost-attributes.yaml`
   **exactly**, or the plugin's intersection silently drops them. Check:
   ```sh
   kubectl get configmap fluence-resources -n kube-system -o yaml | grep -E 'sv1|dm1|tn1|rigetti|iqm'
   ```

4. **Python deps**: `pip install pyyaml matplotlib numpy`.

5. **AWS credentials for your machine** (`aws configure`) — used locally by the
   orchestrator/plugin for `braket get-device` queue snapshots (queue
   sub-experiment) and for the plugin's `braket-live` provider.

6. **AWS credentials inside the cluster** — the gang pods submit/read Braket
   tasks, so they mount an `aws-braket-credentials` secret as env vars (the
   template wires `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_DEFAULT_REGION` via `secretKeyRef` on each gang pod). Create it
   in the experiment's namespace before running:

   ```sh
   kubectl create secret generic aws-braket-credentials \
     --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
     --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
     --from-literal=AWS_DEFAULT_REGION=us-east-1
   ```

   (Add `-n <namespace>` if not using `default`; it must match `FLUENCE_NS` /
   where the pods run. Without this secret the pods fail to authenticate to
   Braket and the producer never produces a result.)

7. **Verify prices** in `cost-attributes.yaml` against the current AWS Braket
   pricing page. Braket has no per-device price API, so this table is the source
   of truth for the experiment's cost accounting.

## Run it

### Step 0 — the match-policy check (do this first)

The baseline's realized backend depends on Fluence's instance-wide match policy.
Before trusting the baseline, see what a capability-only request actually picks:

```sh
# submit the cost baseline a few times, dry-run, and watch realized backends
python3 run_selection.py --experiment cost --repeat 5 --dry-run
```

If the baseline deterministically picks one backend, your comparison is a *point*
("baseline landed on X; min-cost guarantees the cheapest"); if it spreads, you
get a *distribution*. Both are valid — just report which. See `DESIGN.md`.

### Sub-experiment A — cost (cheap)

```sh
# 10 repeats per arm; simulators + Rigetti + IQM; 100 shots (default)
python3 run_selection.py --experiment cost --repeat 10
```
```console
Experiment cost: arms=['baseline', 'min-cost'] pool=['sv1', 'dm1', 'tn1', 'rigetti_cepheus', 'iqm_garnet', 'iqm_emerald'] repeats=10
  policy(selection arm) = min-cost
  writing -> /home/vanessa/Desktop/Code/quantum-braket/experiments/3-selection/results/selection-cost-20260630T020131.csv

  [baseline rep0] submitted as sel-cost-baseline-0-6abaf1
  [min-cost rep0] submitted as sel-cost-min-cost-0-0dc07e  (stamped=dm1)
  [baseline rep1] submitted as sel-cost-baseline-1-c9f8ac
  [min-cost rep1] submitted as sel-cost-min-cost-1-ec4855  (stamped=dm1)
  [baseline rep2] submitted as sel-cost-baseline-2-bfd199
  [min-cost rep2] submitted as sel-cost-min-cost-2-d80792  (stamped=dm1)
  [baseline rep3] submitted as sel-cost-baseline-3-4922cc
  [min-cost rep3] submitted as sel-cost-min-cost-3-0594dd  (stamped=dm1)
  [baseline rep4] submitted as sel-cost-baseline-4-04d485
  [min-cost rep4] submitted as sel-cost-min-cost-4-cc12ca  (stamped=dm1)
  [baseline rep5] submitted as sel-cost-baseline-5-598df1
  [min-cost rep5] submitted as sel-cost-min-cost-5-acb911  (stamped=dm1)
  [baseline rep6] submitted as sel-cost-baseline-6-f28e23
  [min-cost rep6] submitted as sel-cost-min-cost-6-db1e10  (stamped=dm1)
  [baseline rep7] submitted as sel-cost-baseline-7-2ae19a
  [min-cost rep7] submitted as sel-cost-min-cost-7-8b931e  (stamped=dm1)
  [baseline rep8] submitted as sel-cost-baseline-8-bf1edf
  [min-cost rep8] submitted as sel-cost-min-cost-8-5b2628  (stamped=dm1)
  [baseline rep9] submitted as sel-cost-baseline-9-c69997
  [min-cost rep9] submitted as sel-cost-min-cost-9-4cda6c  (stamped=dm1)

wrote 20 rows -> /home/vanessa/Desktop/Code/quantum-braket/experiments/3-selection/results/selection-cost-20260630T020131.csv

summary:
  baseline   cost $0.445±0.000  backends={'iqm_emerald': 10}
  min-cost   cost $0.004±0.000  backends={'dm1': 10}
```
```bash
# render
python3 plot_selection.py        # writes img/selection-cost-<ts>.png
```

Shot count does not affect selection — it scales every backend's cost identically,
so the ranking (and which backend min-cost picks) is the same at 100 or 1000
shots. This experiment measures *which backend was selected and what it cost*, not
quantum-result fidelity, so 100 shots is the default (≈10× cheaper than 1000 on
any run that lands on a real QPU). Worst case (every baseline run hits the most
expensive candidate — IQM at ~$0.445/task at 100 shots — for 10 reps): ~$4.45 for
the baseline arm; the min-cost arm always hits a simulator (~$0). Re-check current
prices first.

### Sub-experiment B — queue (REAL MONEY, QPU-only)

```sh
# few repeats; QPUs only; 100 shots (default)
python3 run_selection.py --experiment queue --repeat 3
python3 plot_selection.py --combined both
```

**Before paying:** snapshot the candidate queues so you don't run into a deep
queue when the result would be uninformative, and so ≥2 QPUs are actually
available for the choice to be meaningful:

```sh
for arn in \
  "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q" \
  "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet" \
  "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald"; do
  region=$(echo "$arn" | cut -d: -f4)
  aws braket get-device --device-arn "$arn" --region "$region" \
    --query '{status:deviceStatus,queue:deviceQueueInfo}' --output json
done
```

Budget: per-task ≈ $0.30 + shots×per_shot. At 100 shots that's ≈ $0.39–0.45/task;
only the producer submits, so it is ONE task per run regardless of group size. 3 reps × 2 arms × 1 task ≈ 6 tasks ≈ ~$2.40 at 100 shots. Stay well under $50.

## Output

Each run appends a row to `results/selection-<exp>-<timestamp>.csv` with:

```
experiment, arm, policy, repeat, n_shots, group_size,
realized_backend, stamped_backend, realized_cost_usd, queue_at_submit,
qpu_queue_wait_s, producer_wall_s, producer_phase, timestamp
```

- `realized_backend` — what the producer ran on (from its `FLUXION_BACKEND=<name>`
  / `device=<arn>` log line; falls back to the plugin's chosen backend).
- `stamped_backend` — what `kubectl fluence` pinned (selection arms only).
- `realized_cost_usd` — computed from `cost-attributes.yaml` + shot count, using
  the **same** formula the plugin uses, so accounting and selection agree.

The orchestrator prints a summary at the end (mean cost/queue per arm, and which
backends each arm landed on).

## Expected result

- **cost**: baseline mean cost > min-cost mean cost, and baseline has higher
  variance (it sometimes hits Rigetti or IQM). min-cost is a flat low bar (always a
  simulator). Headline: cost-aware selection cuts realized cost and removes the
  risk of accidentally landing expensive work on a QPU.
- **queue**: min-queue pins the shorter-queue device at submit; baseline does
  not. Report honestly as single, uncontrolled observations (submit-time
  snapshot, exogenous queue), not controlled means.

## Caveats (see DESIGN.md for detail)

- **Submit-time snapshot**: queue depth is read at submit; the job may sit gated
  before it dispatches, so the value can be stale. Client-side hint, not a
  dispatch-time decision.
- **Pricing is configured, not fetched**: keep `cost-attributes.yaml` current.
- **Name parity**: attribute-file names must equal the resource-graph names.
- **Realized backend from producer logs**: the orchestrator reads the chosen
  backend from the producer's logs — it prefers a `FLUXION_BACKEND=<name>` line and
  falls back to the `device=<arn>` line (mapping the ARN to a name via
  `cost-attributes.yaml`). Timings come from gang.py's `TIMING <key>_ts=<epoch>`
  lines (`start_ts`, `submit_ts`, `queued_ts`, `result_ts`, `end_ts`). For the
  selection arm the backend is also captured from the plugin's stamp at submit,
  so it's recorded even if log parsing misses. If gang.py's log format changes,
  update `_parse_timing` in `run_selection.py`.
