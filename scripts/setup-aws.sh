#!/usr/bin/env bash
# setup-aws.sh — create the Kubernetes secret for AWS Braket credentials
# Usage: ./scripts/setup-aws.sh [--namespace <ns>] [--region <region>]
#
# Reads AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from the environment
# (or from ~/.aws/credentials via the aws CLI if not set).
set -euo pipefail

NAMESPACE=${NAMESPACE:-default}
REGION=${AWS_DEFAULT_REGION:-us-east-1}

while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --region)    REGION="$2";    shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# If credentials aren't already exported, try to pull them from aws CLI
if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
  AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id 2>/dev/null || true)
fi
if [[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key 2>/dev/null || true)
fi

if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "ERROR: AWS credentials not found."
  echo "Export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or configure the AWS CLI."
  exit 1
fi

echo "==> Creating secret aws-braket-credentials in namespace '${NAMESPACE}'"
kubectl create secret generic aws-braket-credentials \
  --namespace="${NAMESPACE}" \
  --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  --from-literal=AWS_DEFAULT_REGION="${REGION}" \
  --dry-run=client -o yaml | kubectl apply -f -


echo "==> Creating Braket service-linked IAM role (safe to run if it already exists)"
aws iam create-service-linked-role --aws-service-name braket.amazonaws.com 2>&1 \
  | grep -v "has been taken in this account" || true


echo "==> Done. Verify with:"
echo "    kubectl get secret aws-braket-credentials -n ${NAMESPACE}"
