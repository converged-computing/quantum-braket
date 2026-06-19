# Experiment 1 — Scheduler overhead and placement quality

## Hypothesis

The Fluence scheduler (graph-based, HPC-grade) places pods with fewer
resource conflicts than the default Kubernetes scheduler when N hybrid
pipeline instances are submitted simultaneously. This should reduce
total makespan and idle time on the Braket gateway pods.

## Setup

Follow the cluster setup steps in the [root README](../../README.md) before
running this experiment.

## Costs

### Backend Cost Estimates

Cost per pipeline run at the recommended settings for this experiment
(`--n-shots 100`, `--max-iter 5`). One "run" = one pod completing fully.

### Pricing components

Gate QPUs charge **per task** (fixed) + **per shot** (variable).
Simulators charge **per minute** of simulation time.
AHS (QuEra Aquila) charges **per task** + **per shot** (one task per pipeline run).

All prices USD, on-demand, no reservation.

### Per-run cost table

| Backend | Type | Per-task | Per-shot | Shots | Tasks (iters) | Cost/run |
|---|---|---|---|---|---|---|
| `sv1` | Simulator | $0.075/min | — | — | 5 × ~1s | ~$0.01 |
| `tn1` | Simulator | $0.275/min | — | — | 5 × ~2s | ~$0.05 |
| `dm1` | Simulator | $0.075/min | — | — | 5 × ~1s | ~$0.01 |
| `ahs_local` | Local sim | free | free | — | — | $0.00 |
| `aquila` | AHS QPU | $0.30 | $0.01000 | 100 | 1 | **$1.30** |
| `rigetti_cepheus` | Gate QPU | $0.30 | $0.000425 | 100 | 5 | **$1.73** |
| `iqm_garnet` | Gate QPU | $0.30 | $0.00145 | 100 | 5 | **$2.23** |
| `iqm_emerald` | Gate QPU | $0.30 | $0.00160 | 100 | 5 | **$2.30** |
| `aqt_ibex` | Gate QPU | $0.30 | $0.02350 | 100 | 5 | **$13.25** |
| `ionq_forte` | Gate QPU | $0.30 | $0.08000 | 100 | 1 | **$41.30** |

> **IonQ Forte**: use `--max-iter 1` only. At 5 iterations and 100 shots
> the cost is $41/run. At the default 30-iteration convergence it is ~$249/run.

**Estimated total across all backends: ~$298**

### Environment

```bash
python -m venv env
source env/bin/activate
pip install boto3
```

### Notes on QPU availability

Real QPU backends are not available 24/7. Check device status before submitting:

```bash
python3 - << 'PYEOF'
import boto3
client = boto3.client("braket", region_name="us-east-1")
client_west = boto3.client("braket", region_name="us-west-1")
client_eu = boto3.client("braket", region_name="eu-north-1")
for arn in [
    "arn:aws:braket:us-east-1::device/qpu/ionq/Forte-Enterprise-1",
    "arn:aws:braket:us-east-1::device/qpu/quera/Aquila",
    "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
    "arn:aws:braket:::device/quantum-simulator/amazon/tn1",
    "arn:aws:braket:::device/quantum-simulator/amazon/dm1",
    "arn:aws:braket:us-west-1::device/qpu/rigetti/Cepheus-1-108Q",
#    "arn:aws:braket:eu-north-1::device/qpu/iqm/Garnet",
#    "arn:aws:braket:eu-north-1::device/qpu/iqm/Emerald",
#    "arn:aws:braket:eu-north-1::device/qpu/aqt/IBEX-Q1"
]:
    if "west" in arn:
        r = client_west.get_device(deviceArn=arn)
    elif "eu-" in arn:
        r = client_eu.get_device(deviceArn=arn)
    else:
        r = client.get_device(deviceArn=arn)
    print(f"{r['deviceName']}: {r['deviceStatus']}")
PYEOF
```

## AWS Free Tier

New Braket accounts get **1 free hour of simulator time per month for
the first 12 months**. SV1 and TN1 runs under that hour are free.
QPU charges are never covered by the free tier.


## Run the experiment

```console
# Step 1 — see what's available and what it costs
python3 run_experiment.py --list-backends
```
```console
Available backends (cheapest to most expensive):
  name                 qrmi_type    cost
  -------------------- ------------ ----------------------------------------
  sv1                  braket-gate  ~$0.075/min — start here
  tn1                  braket-gate  ~$0.275/min
  dm1                  braket-gate  ~$0.075/min
  ahs_local            braket-ahs   free — local AHS simulator
  aquila               braket-ahs   $0.30/task + $0.01/shot
  rigetti_cepheus      braket-gate  $0.30/task + $0.000425/shot
  iqm_garnet           braket-gate  $0.30/task + $0.00145/shot
  iqm_emerald          braket-gate  $0.30/task + $0.00160/shot
  aqt_ibex             braket-gate  $0.30/task + $0.02350/shot
  ionq_forte           braket-gate  $0.30/task + $0.08000/shot — use --max-iter 1
```

Note that before using actual quantum devices you need to accept user agreements, per region. The URLs look like this [https://us-west-1.console.aws.amazon.com/braket/home?region=us-west-1#/permissions?tab=general](https://us-west-1.console.aws.amazon.com/braket/home?region=us-west-1#/permissions?tab=general)

```bash
# problem size 10
python3 run_experiment.py --backend sv1
python3 run_experiment.py --backend tn1
python3 run_experiment.py --backend dm1
python3 run_experiment.py --backend rigetti_cepheus
python3 run_experiment.py --backend iqm_garnet
python3 run_experiment.py --backend iqm_emerald
# python3 run_experiment.py --backend aqt_ibex
# python3 run_experiment.py --backend ionq_forte

# problem size 14
python3 run_experiment.py --backend sv1 --qubit-sizes 14
python3 run_experiment.py --backend tn1 --qubit-sizes 14
python3 run_experiment.py --backend dm1 --qubit-sizes 14
python3 run_experiment.py --backend rigetti_cepheus --qubit-sizes 14
python3 run_experiment.py --backend iqm_garnet --qubit-sizes 14
python3 run_experiment.py --backend iqm_emerald --qubit-sizes 14
```

Note from V- the first day with TN1, DM1, SV1, and rigetti was about ~18 so the cost estimates are too high.

Look at results

```bash
pip install matplotlib seaborn pandas
python3 plot_results.py --results results/ --out figures/
```

## Method

For each scheduler, N concurrent QAOA pipeline instances are submitted
simultaneously. Each instance gets its own unique seed. We measure:

- Total wall time from first `kubectl apply` to last optimizer `Succeeded`
- Per-pod scheduling latency (creation timestamp → `Running` timestamp)
- Pod placement distribution across nodes

## Metrics

| Metric | Unit | How to collect |
|---|---|---|
| Total makespan | seconds | `run.sh` records start/end timestamps |
| Scheduling latency per pod | seconds | `kubectl get events --field-selector reason=Scheduled` |
| Pod placement distribution | node names | `kubectl get pod -o wide` |

## Why?

We want to understand the latency of different systems, and how it is influenced by batch and problem size.
