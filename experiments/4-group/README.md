# Multi-gang contention experiment (the real gang-scheduling test)

Submits MANY gangs of DIFFERENT apps and sizes AT ONCE into a pool too small to
hold them, and measures how each scheduler copes. This is the production
scenario gang scheduling exists for — and where the default scheduler fails:
it places pods piecemeal, so a gang can get some pods onto nodes and HOLD them
while waiting for the rest, blocking other gangs (partial placement / deadlock).
Fluence places a whole gang or none.

## The batch

A batch = the grid of apps x sizes, one gang per cell, all submitted at once.
Default: apps {amg, lammps, qmcpack} x sizes {1, 2, 4} = 9 gangs.
Total demand = sum of sizes (pods). On 4 nodes that's heavily oversubscribed —
which is the point. Each gang is a MiniCluster, one pod per node.

## Run (4-node cluster)

```bash
# default arm first (before installing Fluence):
python3 run_experiment.py --arms default --nodes 4
```

Then install fluence (see after exit 0 in [cluster](cluster)

```bash
python3 run_experiment.py --arms fluence --nodes 4
```

Flags: --apps, --sizes (the grid), --reps (repeat the whole batch),
--batch-timeout, --nodes (contention math + logging).

## Output

results/contention.csv — one row per gang per rep:
  final_status   placed | done | partial | pending | failed
  placed_after_s submit -> whole gang scheduled   (gang placement latency)
  ready_after_s  submit -> whole gang ready
  done_after_s   submit -> gang finished          (-> batch makespan = max)
  nodes, node_layout

The comparison:
  - default-scheduler under contention: expect some gangs `partial` (pods placed,
    holding nodes, waiting) or `pending`, possibly deadlock (nothing finishes);
    makespan inflated by piecemeal placement.
  - Fluence: gangs are `placed` whole or wait as a unit; no partial gangs holding
    nodes; cleaner makespan and no deadlock.

Console prints per arm: "placed N/M gangs; partial/pending: K; makespan=...".

## Notes

- Demand must exceed nodes to create contention; the runner prints CONTENDED/fits.
- partial/pending gangs have blank timings (they never fully placed) — that's the
  signal, not missing data.
- Problems are small/fast (amg -n 32^3, lammps light replication, qmcpack H2 test)
  so makespan reflects SCHEDULING, not science. Raise --reps for statistics.
- Image refs / namespace at the top of the runner must match your setup.
