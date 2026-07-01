# Experiment: cost- and queue-aware backend selection with `kubectl-fluence`

Demonstrates that making the scheduler **selection-aware** (via the
`kubectl-fluence` client-side plugin) reduces realized cost — and, for real
QPUs, can route around queues — compared with a capability-only request that is
blind to cost/queue.

This experiment uses the plugin built at
`github.com/converged-computing/kubectl-fluence`: it resolves a backend
client-side from a policy annotation and pins it via
`fluence.flux-framework.org/backend`, which Fluence honors.

---

## Sub-experiment A — cost selection (simulators + Rigetti)

### Hypothesis

A job that requests a quantum backend **by capability only** (no cost policy) is
cost-blind: Fluence satisfies the capability request with whatever its match
policy picks, so realized cost is uncontrolled. Adding a `min-cost` selection
policy makes realized cost consistently lower and lower-variance, because the
plugin pins the cheapest satisfying backend every time.

### Arms

- **baseline (no selection):** the gang Job's pods carry a capability
  request (e.g. gate-based, `require:min_qubits=N`) but **no**
  `select-policy` annotation and **no** `backend` pin. Fluence matches it against
  the resource graph using its configured match policy. Realized backend =
  whatever matching returns.
- **min-cost (with selection):** identical manifest plus
  `fluence.flux-framework.org/select-policy: "require:min_qubits=N,min-cost"`.
  Submitted via `kubectl fluence apply`, which pins the cheapest satisfying
  backend.

Candidate pool (both arms): `sv1`, `dm1`, `tn1`, `rigetti_cepheus`. Simulators
are cheap; Rigetti is ~$7/task. A cost-blind match can land on Rigetti; min-cost
never will (a simulator always satisfies and is cheaper).

### IMPORTANT design check before running — what does the baseline pick?

The baseline's realized cost depends on Fluence's **match policy** (set at
Fluxion init, instance-wide: `first`/`low`/`high`/...). Two cases:

1. **Deterministic match (e.g. `first`):** the no-selection arm may hit the SAME
   backend every run (whatever is first among satisfying vertices). Then the
   baseline is a single backend, not a spread — its "cost variance" is ~0 and the
   story becomes "baseline happened to pick backend X; min-cost guarantees the
   cheapest." Still valid, but it's a *point*, not a *distribution*.
2. **Varying match:** if matching spreads across satisfying backends (or you
   vary the request so different backends match), the baseline shows a cost
   DISTRIBUTION — some runs cheap, some expensive — and min-cost collapses it to
   the floor. This is the stronger visual.

**Action:** before running, confirm what your Fluxion match policy does with
multiple satisfying quantum backends (submit the capability-only request a few
times, observe the realized `backend`). If it's deterministic, either (a) accept
the point-comparison framing, or (b) intentionally create a spread — e.g. submit
a mix of request sizes/qubit-floors so different runs match different backends,
making the baseline's cost-blindness visible as a distribution. Document whichever
you choose; do not silently assume a spread.

### Method

1. Build the attribute file (`cost-attributes.yaml`) with per-backend cost
   components (see below). This is the plugin's cost source; the scheduler graph
   need not carry cost.
2. For `R` repeats:
   - submit the **baseline** gang (no policy), record the realized backend +
     compute its cost for the run's shot count.
   - submit the **min-cost** gang via `kubectl fluence apply`, record realized
     backend + cost.
3. Aggregate: mean/stdev realized cost per arm; bar chart baseline vs min-cost.

`R` can be large and cheap IF the baseline match is deterministic onto a
simulator. If the baseline can land on Rigetti, cap `R` so the worst case
(all-Rigetti baseline) stays within budget — e.g. R=10 with a Rigetti per-task
~$0.30 + shots*per_shot at small shots is bounded; check before running (see
budget note).

### Metrics

- `realized_backend` per run per arm.
- `realized_cost_usd` per run = `per_task + shots*per_shot` (QPU) or nominal
  (sim), computed from the attribute file — the SAME formula the plugin uses, so
  the experiment's cost accounting and the selector agree.
- Aggregate: mean ± stdev cost per arm; fraction of baseline runs that hit a QPU.

### Expected result

baseline mean cost > min-cost mean cost, and baseline has higher variance (it
sometimes hits Rigetti). min-cost is a flat low bar (always a simulator here).
Headline: *cost-aware selection cuts realized cost by Nx and removes the risk of
accidentally landing expensive work on a QPU.*

---

## Sub-experiment B — queue selection (QPUs only)

### Why QPU-only

Simulators have no meaningful task queue, so `min-queue` among simulators is
trivial (all ~0). Queue selection only *shows* something when the candidate pool
is real QPUs with real, differing queue depths. This makes B a real-QPU
experiment: it costs money and the queue is exogenous (uncontrolled), so — like
the idle real-QPU runs — it is **illustrative/feasibility**, not a controlled
mean.

### Arms

- **baseline (no selection):** capability request for a gate QPU, no queue
  policy → Fluence matches some QPU regardless of its queue.
- **min-queue (with selection):** add
  `select-policy: "online-only,min-queue"`. The plugin's `braket-live` provider
  queries each candidate QPU's live `deviceQueueInfo` (using your AWS creds,
  client-side) and pins the shortest-queue online device.

Candidate pool: the real QPUs you have access to (e.g. `rigetti_cepheus`,
`iqm_garnet`, `iqm_emerald`). At least two must be simultaneously available for
the choice to be meaningful.

### Method (budget-bounded)

- **Tiny shots, few jobs** (~$30–50 total). Per-task ~$0.30 + shots*per_shot;
  at e.g. 100 shots and per_shot ~$0.0009–0.0015, a task is ~$0.39–0.45, so a
  handful of jobs across arms stays in budget. Confirm with the budget formula
  before running.
- Before EACH run, snapshot every candidate's queue depth (`aws braket
  get-device ... deviceQueueInfo`) so you can report what the plugin saw and
  verify it pinned the shortest.
- For a small `R` (e.g. 3–5): submit baseline (records which QPU matched + that
  QPU's queue-at-submit), then submit min-queue (records pinned QPU + its
  queue-at-submit). Record realized total time / queue wait.

### Metrics

- `pinned_backend` + `queue_at_submit` per run per arm (from the get-device
  snapshot).
- realized queue wait / producer wall time per run.
- Did min-queue pick the lowest-queue candidate at submit? (yes/no per run).

### Expected result (stated honestly)

min-queue pins the shortest-queue device at submit; baseline does not and
sometimes lands on a deeply-queued one. Because queue is exogenous and these are
single observations, report as: *queue-aware selection routed to the
shorter-queue device in K/R runs, reducing observed wait; real-queue variability
means these are illustrative single runs, not controlled means.* The
**submit-time snapshot** limitation applies: the queue is read at submit, the job
may dispatch later — note it.

---

## Shared assets

### `cost-attributes.yaml` (the plugin's attribute file)

```yaml
version: 1
backends:
  - name: sv1
    provider: braket
    device_arn: arn:aws:braket:::device/quantum-simulator/amazon/sv1
    region: us-east-1
    cost_per_minute: 0.075
    qubits: 34
  - name: dm1
    provider: braket
    device_arn: arn:aws:braket:::device/quantum-simulator/amazon/dm1
    region: us-east-1
    cost_per_minute: 0.075
    qubits: 17
  - name: tn1
    provider: braket
    device_arn: arn:aws:braket:::device/quantum-simulator/amazon/tn1
    region: us-east-1
    cost_per_minute: 0.275
    qubits: 50
  - name: rigetti_cepheus
    provider: braket
    device_arn: arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q
    region: us-west-1
    cost_per_task: 0.30
    cost_per_shot: 0.00090
    qubits: 108
  - name: iqm_garnet
    provider: braket
    device_arn: arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet
    region: eu-north-1
    cost_per_task: 0.30
    cost_per_shot: 0.00145
    qubits: 20
```
(Verify all prices against the current Braket pricing page before publishing —
these are placeholders in the right ballpark.)

### `fluence-resources` ConfigMap (scheduler offerings)

Must list every candidate backend by the SAME name used in the attribute file,
or the plugin's intersection drops it. Confirm the names match exactly.

### Orchestration

Reuse the existing experiment runner pattern (per-arm submit, collect a CSV row
per run with backend + cost + timing). Add columns: `arm`
(baseline|min-cost|min-queue), `policy`, `realized_backend`, `realized_cost_usd`,
`queue_at_submit`.

### Plotting

A bar chart per sub-experiment: x = arm, y = cost (A) or queue wait (B), with
per-run scatter so the baseline's spread (if any) is visible. Reuse the
plotting conventions from the idle experiment (per-run points + mean±stdev bar).

---

## Budget note (run this math before any QPU run)

QPU task cost = `per_task + shots * per_shot`. With per_task=$0.30:
- 100 shots @ $0.0009 (Rigetti) = $0.39/task
- 100 shots @ $0.00145 (IQM)   = $0.445/task

A gang submits ONE quantum task (the Fluence-created producer). So cost A worst case (all baseline
runs hit a QPU, R=10) ≈ 10 * ~$0.40 = ~$4 for the baseline arm; min-cost arm ≈ $0
(simulators). Cost B (QPU-only, both arms QPU, R=5, 2 arms) ≈ 10 * ~$0.40 = ~$4.
Both well under $50 at small shots — but RE-CHECK current per-shot prices and
your actual shot count before running, and snapshot queues first to avoid paying
into a deep queue when the result would be uninformative.

## Open checks before running

1. **Match policy behavior** (sub-exp A baseline): deterministic vs spread —
   determines whether the baseline is a point or a distribution. CHECK FIRST.
2. **Backend name parity** between attribute file and `fluence-resources` graph
   (exact string match for the intersection).
3. **Current Braket prices** (the attribute file values are placeholders).
4. **≥2 QPUs simultaneously available** for sub-exp B to be meaningful; snapshot
   queues before paying.
5. **ConfigMap RBAC** so the plugin can read scheduler offerings in your context.
