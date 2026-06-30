#!/usr/bin/env bash
# experiments/2-gang/cluster/setup.sh
#
# Then:  bash setup.sh
# Tear down when done:  bash teardown.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

export FLUENCE_BRANCH=add-jobs-rbac
export CLUSTER_NAME=fluence-gang
export ZONE=us-central1-a          # confirm h3 is offered here (see note)
export MACHINE_TYPE=h3-standard-88 # 88 physical cores, 352 GB, no SMT
export NUM_NODES=4                 # was 2 baseline; +2 for headroom / bigger gangs

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


exit 0

# ── 4. Install Fluence
export FLUENCE_BRANCH=main
FLUENCE_REPO="https://raw.githubusercontent.com/converged-computing/fluence/${FLUENCE_BRANCH}"

log "Installing Fluence"
kubectl apply -f "$FLUENCE_REPO/deploy/fluence.yaml"
kubectl rollout status -n kube-system deployment/fluence --timeout=180s
kubectl rollout status -n kube-system deployment/fluence-webhook --timeout=120s

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

echo "Note: this alpha cluster AUTO-DELETES after 30 days and cannot be upgraded."
echo "Tear down sooner with:  bash $HERE/teardown.sh"

