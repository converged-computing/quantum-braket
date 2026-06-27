# Queue-depth selection experiment (sub-experiment B)

This is the **queue** arm of the selection experiment. It is already implemented in `run_selection.py` (`--experiment queue`) and shares the same orchestrator, template, gang image, and plugin as the cost experiment. Nothing here changes the cost experiment, which is working. This document is the how-to for the queue variant specifically, because it has stricter prerequisites (real money, QPU access, the AWS CLI) than the cost variant.

## What it measures

The same comparison as the cost experiment, but the selection objective is queue wait instead of price, and the candidate pool is real QPUs only (simulators have no meaningful queue).

The `baseline` arm sends a capability-only request (`require-qrmi_type: braket-gate`) with no policy, so the scheduler lands on some QPU queue-blind. The `min-queue` arm sends the same request plus `select-policy: online-only,min-queue`, so the plugin pins the shortest-queue online QPU at submit time.

Candidate pool (`POOL_QUEUE` in `run_selection.py`): `rigetti_cepheus`, `iqm_garnet`, `iqm_emerald`.

The selection policy is a pipeline: `online-only` filters to devices that are currently online, then `min-queue` orders the survivors by ascending `queue_size`. The plugin pins the winner via the `fluence.flux-framework.org/require-backend` annotation, the same pin mechanism the cost experiment and the gang experiments use.

### Note on `online-only` (read this)

The filter checks a `status` attribute and is fail-open: a backend with no `status` field passes through. The current `cost-attributes.yaml` does not carry a `status` field, so as shipped, `online-only` filters nothing and every candidate is eligible. To make it actually gate on availability, populate a `status: ONLINE|OFFLINE` attribute per backend (for example from a live `aws braket get-device` query before the run, or in the resource graph). Until then, treat `online-only` as a no-op placeholder and rely on the device-availability pre-check below to avoid pinning an offline QPU.

## How it differs from the cost experiment

Both run from the identical `run_selection.py`; the only difference is the flag.

```sh
python3 run_selection.py --experiment cost  --repeat 10   # simulators + QPUs, cheap
python3 run_selection.py --experiment queue --repeat 3    # QPUs ONLY, real money
```

The queue arm additionally costs real money (every run submits to a real QPU on both arms, no simulators in the pool), snapshots live queue depth at submit via `aws braket get-device` (recorded as `queue_at_submit`, the signal the policy acts on), and records the realized wait the task actually experienced (`qpu_queue_wait_s`, from the gang leader's `queued_ts` to `result_ts`).

## Prerequisites beyond the cost experiment

### Everything the cost experiment needs

Redeployed Fluence (sidecar with the multi-region task search), the `quantum-braket-gang-selection` image, the rebuilt `kubectl-fluence` plugin that stamps `require-backend`, and the `aws-braket-credentials` secret in the cluster.

### The AWS CLI on the orchestrator machine

Authenticated with the same account. The orchestrator calls `aws braket get-device` to snapshot queue depth. Verify:

```sh
aws braket get-device \
  --device-arn arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald \
  --region eu-north-1 --output json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("deviceQueueInfo"))'
```

If this prints queue info, the snapshot works. If it errors, `queue_at_submit` will be blank in the CSV; the run still proceeds, you just lose the snapshot column.

### IAM access to the QPU regions

The candidate QPUs live in two regions: `us-west-1` (Rigetti Cepheus) and `eu-north-1` (IQM Garnet/Emerald). Your account must be allowed `braket:GetDevice`, `braket:CreateQuantumTask`, and `braket:SearchQuantumTasks` in both. We hit an `AccessDeniedException` in `eu-west-2` from a service-control policy earlier; that region isn't in the pool so it doesn't matter here, but confirm us-west-1 and eu-north-1 are allowed.

### Device availability

QPUs go offline on schedules. As noted above, `online-only` does not currently gate on this (no `status` attribute is populated), so the selection could pin an offline device whose submission would then sit unschedulable at Braket. Until a `status` attribute is wired in, manually check availability before running and drop any offline device from `POOL_QUEUE` for that session:

```sh
for arn in \
  arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q \
  arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet \
  arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald ; do
    region=$(echo "$arn" | cut -d: -f4)
    echo -n "$arn : "
    aws braket get-device --device-arn "$arn" --region "$region" \
      --query deviceStatus --output text 2>/dev/null || echo "ERR"
done
```

If you want `online-only` to enforce this automatically, that's a small follow-up: populate `status` per backend from this same query.

## Cost ceiling

QPU-only, at the default 100 shots, worst case (every run lands on the most expensive candidate, IQM at about $0.445/task), across both arms:

```
--repeat 1   up to ~$0.89
--repeat 3   up to ~$2.67
--repeat 5   up to ~$4.45
```

Re-check current Braket prices before running; these use the values in `cost-attributes.yaml`.

## Run procedure

Money is on the line, so go slowly through this ladder.

### Confirm the cost experiment passes first

The queue experiment shares all the same machinery; if cost works end-to-end, queue's only new risks are money, regions, and device availability. Don't debug coordination on the real-money arm.

### Dry run

No apply, no cost; shows what would be pinned.

```sh
python3 run_selection.py --experiment queue --repeat 1 --dry-run
```

Confirm 1 leader (`role: leader`, `select-policy: online-only,min-queue`, `require-qrmi_type: braket-gate`) plus worker(s), and that the dry-run prints the device the plugin would pin. The `dropped sv1/tn1/dm1` lines are expected: those are simulators, not queue candidates.

### One real rep first

A single real-money run, watched.

```sh
python3 run_selection.py --experiment queue --repeat 1
kubectl get pods -w        # leader Running+sidecar, worker gated -> ungates -> Succeeded
```

Then verify the pin actually bound (the min-queue leader must run on the device the plugin chose, not whatever the scheduler preferred):

```sh
kubectl get pod <min-queue-leader> -o jsonpath='{.metadata.annotations}' \
  | tr ',' '\n' | grep require-backend     # want: require-backend: <chosen QPU>
kubectl logs <min-queue-leader> | grep 'device='   # device ARN should match the pin
```

### Full set

Only after one rep is clean.

```sh
python3 run_selection.py --experiment queue --repeat 3
python3 plot_selection.py        # newest results/selection-queue-*.csv
```

## Output

CSV at `results/selection-queue-<timestamp>.csv`, same schema as the cost experiment. The columns that matter for queue: `arm` (`baseline` or `min-queue`), `realized_backend` (the QPU the run actually used), `queue_at_submit` (queue depth the policy saw at submit, the signal), `qpu_queue_wait_s` (the wait the task actually experienced, the outcome), and `backend_latency_s` (full submit-to-result turnaround).

`plot_selection.py` renders the queue figure as two panels: queue depth at submit (policy signal) and realized queue wait (measured outcome).

## Important caveat for the paper

Queue depth is exogenous: it's set by other users' jobs, not by your experiment, and it changes minute to minute. So the queue result is a real-world illustration, not a controlled result. On any given run, the shortest-queue device at submit may or may not deliver the shortest realized wait, because queues move after you commit. Present it as "the policy pins the shortest-queue online device at decision time," not as a guaranteed wait reduction. The cost experiment is the controlled, repeatable result; the queue experiment shows the mechanism working against live conditions.
