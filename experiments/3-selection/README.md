# Experiment 5 — Cost / queue-aware backend selection

Demonstrates that making backend selection **policy-driven** (via the
[`kubectl-fluence`](https://github.com/converged-computing/kubectl-fluence)
client-side plugin) reduces realized cost — and, for real QPUs, can route around
queues — compared with a capability-only request that is blind to cost/queue.

The plugin resolves the backend on the client (using your credentials for the
queue case) and pins it via the `fluence.flux-framework.org/backend` annotation,
which Fluence honors at schedule time. Credentials never enter the cluster.

The selection arm submits with a **single** `kubectl fluence apply -f sampler.yaml
--attributes cost-attributes.yaml`: the plugin resolves the backend, stamps the
annotation, and applies in one shot — the user never hand-edits a manifest. The
orchestrator reads the chosen backend from the plugin's stderr. (For a human at
a terminal, `kubectl fluence apply --confirm` shows the selection and prompts for
approval before applying; the experiment does not use `--confirm` so runs proceed
unattended.)

See [`DESIGN.md`](DESIGN.md) for the full rationale, the match-policy caveat, and
the budget math.

**Workload shape.** This experiment measures *which backend is selected and what
it costs*, not gang coordination. So the workload is the simplest thing that
exercises selection: `--group-size` (default 2) **independent** quantum sampler
pods per run, each requesting a QPU (`fluxion.flux-framework.org/qpu`), each
having a backend selected (baseline: capability-only; selection arm: the plugin
pins min-cost), each running and exiting on its own. There is **no gang, no
leader/worker, no PodGroup, no gating** — those exist for the idle-reclamation
story (Experiment 2), which is orthogonal to cost selection. Per the Fluence
webhook, a quantum pod with no group label gets the backend injected and runs
standalone (see `examples/quantum-pod.yaml` in the fluence repo).

```
5-selection/
├── README.md               # this file
├── DESIGN.md               # hypotheses, methodology, caveats, budget
├── cost-attributes.yaml    # backend cost/capability table (the plugin's cost source)
├── run_selection.py        # orchestrator (both arms, both sub-experiments)
├── plot_selection.py       # renders results into img/
├── manifests/
│   └── sampler-template.yaml  # independent QPU-requesting sampler pod, templated
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
   `AWS_DEFAULT_REGION` via `secretKeyRef` on each sampler pod). Create it
   in the experiment's namespace before running:

   ```sh
   kubectl create secret generic aws-braket-credentials \
     --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
     --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
     --from-literal=AWS_DEFAULT_REGION=us-east-1
   ```

   (Add `-n <namespace>` if not using `default`; it must match `FLUENCE_NS` /
   where the pods run. Without this secret the pods fail to authenticate to
   Braket and the sampler pods never produce a result.)

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

python3 plot_selection.py        # writes img/selection-queue-<ts>.png
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
each sampler pod submits one task. With group-size 2: 3 reps × 2 arms × 2 pods ≈ 12 tasks ≈ ~$5 at 100 shots. Stay well under $50.

## Output

Each run appends a row to `results/selection-<exp>-<timestamp>.csv` with:

```
experiment, arm, policy, repeat, n_shots, group_size,
realized_backend, stamped_backend, realized_cost_usd, queue_at_submit,
qpu_queue_wait_s, leader_wall_s, leader_phase, timestamp
```

- `realized_backend` — what the sampler used (from its `TIMING
  backend <name>` log line; falls back to the plugin's stamped choice).
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
- **Realized backend from leader logs**: the orchestrator reads the chosen
  backend from the leader's logs — it prefers a `FLUXION_BACKEND=<name>` line and
  falls back to the `device=<arn>` line (mapping the ARN to a name via
  `cost-attributes.yaml`). Timings come from gang.py's `TIMING <key>_ts=<epoch>`
  lines (`leader_start_ts`, `queued_ts`, `result_ts`, `workers_done_ts`). For the
  selection arm the backend is also captured from the plugin's stamp at submit,
  so it's recorded even if log parsing misses. If gang.py's log format changes,
  update `_parse_timing` in `run_selection.py`.
