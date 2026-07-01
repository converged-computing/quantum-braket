#!/usr/bin/env bash
# experiments/2-gang/cluster/teardown.sh
#
# Delete the GKE cluster for experiment 2. (Alpha clusters also auto-delete
# after 30 days, but tear down sooner to stop billing.)
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-fluence-selection}"
ZONE="${ZONE:-us-central1-a}"

command -v gcloud >/dev/null || { echo "gcloud not installed" >&2; exit 1; }

echo "=== Deleting GKE cluster $CLUSTER_NAME (zone $ZONE)"
echo "    (Ctrl-C within 5s to abort)"
sleep 5
gcloud container clusters delete "$CLUSTER_NAME" --zone "$ZONE" --quiet
echo "=== Done."
