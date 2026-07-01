#!/usr/bin/env bash
# experiments/2-gang/cluster/setup.sh
#
# Stand up the cluster for experiment 2 on GKE and install Fluence + the Braket
# resource graph.
#
# WHY A GKE ALPHA CLUSTER: the experiment needs the alpha gang-scheduling
# machinery — the scheduling.k8s.io/v1alpha2 PodGroup API plus the
# GenericWorkload/GangScheduling feature gates. A GKE *alpha cluster*
# (--enable-kubernetes-alpha) turns on ALL alpha API groups automatically (so
# the PodGroup API is served with no runtime-config flags) and lets us set the
# specific feature gates with --alpha-cluster-feature-gates. This is the managed
# path: no control-plane bring-up, no DNS/load-balancer/kOps to fight.
#
# Alpha-cluster caveats (both fine for a short experiment): the cluster cannot
# be upgraded and AUTO-DELETES after 30 days, and is not SLA-covered.
#
# PREREQUISITES (one-time):
#   1. gcloud CLI installed and authenticated:  gcloud auth login
#   2. A GCP project with billing enabled:      gcloud config set project PROJECT_ID
#   3. Kubernetes Engine API enabled:           gcloud services enable container.googleapis.com
#   4. kubectl installed (gcloud can install it: gcloud components install kubectl)
#
# CONFIGURE (edit or export before running):
#   export CLUSTER_NAME=exp2
#   export ZONE=us-central1-a
#   export AWS_ACCESS_KEY_ID=...        # for the Braket secret (optional here)
#   export AWS_SECRET_ACCESS_KEY=...
#
# Then:  bash setup.sh
# Tear down when done:  bash teardown.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-fluence-selection}"
ZONE="${ZONE:-us-central1-a}"
NUM_NODES="${NUM_NODES:-3}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
FLUENCE_BRANCH="${FLUENCE_BRANCH:-main}"
echo "branch: $FLUENCE_BRANCH"
sleep 3

# Gang scheduling needs the Workload API (GenericWorkload) + GangScheduling.
FEATURE_GATES="${FEATURE_GATES:-GenericWorkload=true,GangScheduling=true}"

log()  { echo "=== $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

command -v gcloud  >/dev/null || fail "gcloud not installed (https://cloud.google.com/sdk/docs/install)"
command -v kubectl >/dev/null || fail "kubectl not installed (gcloud components install kubectl)"

PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
[ -n "$PROJECT" ] || fail "no GCP project set (gcloud config set project PROJECT_ID)"
log "Project: $PROJECT   Cluster: $CLUSTER_NAME   Zone: $ZONE"

# ── 1. Create the alpha cluster ───────────────────────────────────────────────
# --enable-kubernetes-alpha enables all alpha APIs (incl. scheduling.k8s.io/
# v1alpha2). --alpha-cluster-feature-gates sets the specific gates we need.
# --no-enable-autorepair/autoupgrade are required for alpha clusters.
if gcloud container clusters describe "$CLUSTER_NAME" --zone "$ZONE" >/dev/null 2>&1; then
  log "Cluster $CLUSTER_NAME already exists — skipping create"
else
  log "Creating GKE alpha cluster (this takes ~5 min)"
  gcloud container clusters create "$CLUSTER_NAME" \
    --zone "$ZONE" \
    --enable-kubernetes-alpha \
    --no-enable-autorepair \
    --no-enable-autoupgrade \
    --cluster-version=1.36.0-gke.2684000 \
    --num-nodes "$NUM_NODES" \
    --machine-type "$MACHINE_TYPE" \
    --alpha-cluster-feature-gates "$FEATURE_GATES" \
    --quiet
fi

# ── 2. Get credentials (writes a correct kubeconfig) ──────────────────────────
log "Fetching cluster credentials"
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE"

# ── 3. Verify the PodGroup API is served + the feature gates took effect ──────
log "Verifying scheduling.k8s.io/v1alpha2 (PodGroup API) is served"
kubectl api-resources --api-group=scheduling.k8s.io 2>/dev/null | grep -q podgroups \
  && log "  ✓ PodGroup API available" \
  || fail "PodGroup API not served — alpha APIs not enabled (is this an alpha cluster?)"

# ── 4. Install Fluence + Braket resource graph ────────────────────────────────
# IMPORTANT: the webhook reads the resource graph ONCE, at startup. The graph
# (the fluence-resources ConfigMap) must therefore exist BEFORE the webhook
# boots, or the webhook comes up with an empty attribute set and cannot inject
# the FLUXION_* provider-routing contract — the sidecar then fails with
# "could not resolve a quantum provider from the backend" and workers never
# ungate. So we apply the resource graph FIRST, then Fluence.
log "Applying Braket resource graph (before Fluence, so the webhook loads it)"
FLUENCE_REPO="https://raw.githubusercontent.com/converged-computing/fluence/${FLUENCE_BRANCH}"
kubectl apply -f "$HERE/../../../hack/fluence-resources.yaml"

log "Installing Fluence"
kubectl apply -f "$FLUENCE_REPO/deploy/fluence.yaml"
kubectl rollout status -n kube-system deployment/fluence --timeout=180s
kubectl rollout status -n kube-system deployment/fluence-webhook --timeout=120s

# Belt-and-suspenders: if the graph were ever (re)applied after boot, both the
# scheduler AND the webhook must be restarted to re-read it. Restart both here so
# a fresh cluster is always in a correct state regardless of apply timing.
log "Restarting scheduler and webhook so both load the resource graph"
kubectl rollout restart -n kube-system deployment/fluence deployment/fluence-webhook
kubectl rollout status  -n kube-system deployment/fluence --timeout=120s
kubectl rollout status  -n kube-system deployment/fluence-webhook --timeout=120s

# The device plugin advertises the fluxion.flux-framework.org/qpu extended
# resource on nodes. Without it the scheduler cannot satisfy the leader's QPU
# request, so the quantum handler never fires. (Required for the Fluence arm.)
log "Installing Fluence device plugin (advertises the qpu resource)"
kubectl apply -f "$FLUENCE_REPO/deploy/device-plugin.yaml"
kubectl rollout status -n kube-system daemonset/fluence-deviceplugin --timeout=120s

log "Waiting for the webhook to be ready"
for i in $(seq 1 60); do
  cab=$(kubectl get mutatingwebhookconfiguration fluence-webhook \
    -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)
  [ -n "$cab" ] && break
  sleep 2
done
log "  webhook ready"

# ── 5. Smoke test: native gang scheduling on the DEFAULT scheduler ────────────
log "Smoke testing native gang scheduling (default scheduler, PodGroup of 2)"
kubectl apply -f - <<'YAML'
apiVersion: scheduling.k8s.io/v1alpha2
kind: PodGroup
metadata:
  name: smoke-gang
spec:
  schedulingPolicy:
    gang:
      minCount: 2
---
apiVersion: v1
kind: Pod
metadata: { name: smoke-0 }
spec:
  schedulingGroup: { podGroupName: smoke-gang }
  restartPolicy: Never
  containers: [{ name: c, image: busybox, command: ["sh","-c","echo ok && sleep 5"], resources: { requests: { cpu: "100m", memory: "64Mi" }}}]
---
apiVersion: v1
kind: Pod
metadata: { name: smoke-1 }
spec:
  schedulingGroup: { podGroupName: smoke-gang }
  restartPolicy: Never
  containers: [{ name: c, image: busybox, command: ["sh","-c","echo ok && sleep 5"], resources: { requests: { cpu: "100m", memory: "64Mi" }}}]
YAML
kubectl wait pod/smoke-0 pod/smoke-1 --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s \
  && log "  ✓ native gang scheduling works" \
  || log "  WARNING: gang smoke test failed — check 'kubectl get podgroup smoke-gang -o yaml' and the scheduler feature gates"
kubectl delete pod smoke-0 smoke-1 --ignore-not-found --wait=false
kubectl delete podgroup smoke-gang --ignore-not-found

# ── 6. AWS credentials secret for Braket ──────────────────────────────────────
if ! kubectl get secret aws-braket-credentials >/dev/null 2>&1; then
  if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    kubectl create secret generic aws-braket-credentials \
      --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
      --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
      --from-literal=AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
    log "  created aws-braket-credentials secret"
  else
    log "  NOTE: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY and create the"
    log "        aws-braket-credentials secret before running the experiment."
  fi
fi

log "Setup complete. Next:"
echo "  cd experiments/2-gang"
echo "  python3 run_experiment.py --backend sv1 --schedulers default fluence"
echo ""
echo "Note: this alpha cluster AUTO-DELETES after 30 days and cannot be upgraded."
echo "Tear down sooner with:  bash $HERE/teardown.sh"

