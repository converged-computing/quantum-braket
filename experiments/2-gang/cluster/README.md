# Experiment 2 cluster (GKE alpha cluster)

A cluster for experiment 2 on **GKE**, using a GKE **alpha cluster**. This is the
simplest path that satisfies the experiment's requirements; the `kops/`
subdirectory has an AWS/kOps alternative if you prefer self-managed.

## Why a GKE alpha cluster

Both experiment conditions need the alpha gang-scheduling machinery: the
`scheduling.k8s.io/v1alpha2` PodGroup API and the `GenericWorkload` /
`GangScheduling` feature gates. A normal managed cluster (GKE/EKS) won't enable
alpha APIs or let you set these gates. A GKE **alpha cluster** does both:

- `--enable-kubernetes-alpha` turns on **all** alpha API groups automatically,
  so the PodGroup API is served with no `--runtime-config` needed.
- `--alpha-cluster-feature-gates=GenericWorkload=true,GangScheduling=true` sets
  the specific gates the gang scheduler needs.

And because it's managed, there's no control-plane bring-up, DNS, or load
balancer to configure — `gcloud container clusters create` just works and
`get-credentials` writes a correct kubeconfig.

**Caveats (fine for a short experiment):** an alpha cluster can't be upgraded,
auto-deletes after 30 days, and isn't SLA-covered.

## One-time prerequisites

```bash
gcloud auth login
gcloud config set project PROJECT_ID
gcloud services enable container.googleapis.com
gcloud components install kubectl     # if you don't have kubectl
```

## Create the cluster

```bash
export CLUSTER_NAME=exp2
export ZONE=us-central1-a
export AWS_ACCESS_KEY_ID=...           # for the Braket secret (optional here)
export AWS_SECRET_ACCESS_KEY=...

bash setup.sh
```

`setup.sh` creates the alpha cluster, fetches credentials, **verifies the
PodGroup API is served**, installs Fluence + the Braket resource graph, and runs
a native gang-scheduling smoke test before you spend any QPU credits.

## Run the experiment

```bash
cd ..      # experiments/2-gang
python3 run_experiment.py --backend sv1 --schedulers default fluence
```

## Tear down

```bash
bash teardown.sh
```

## Files

- `setup.sh` — create the alpha cluster + verify + install Fluence.
- `teardown.sh` — delete the cluster.
- `kops/` — AWS/kOps alternative (self-managed control plane).

## Cost note

3× e2-standard-4 nodes in us-central1 is roughly $0.40–0.50/hr for the cluster,
plus Braket charges per run. The cluster auto-deletes after 30 days, but tear
down when idle to stop billing.
