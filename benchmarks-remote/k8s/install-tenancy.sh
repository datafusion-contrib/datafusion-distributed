#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; run npm run foundation-deploy first" >&2
  exit 2
fi
cluster_name=$(jq -er '.clusterName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"

ensure_kubeconfig
HELM_CACHE_HOME=/tmp/datafusion-distributed-helm-cache \
  HELM_CONFIG_HOME=/tmp/datafusion-distributed-helm-config \
  HELM_DATA_HOME=/tmp/datafusion-distributed-helm-data \
  helm upgrade --install benchmark-tenancy \
  "${root}/benchmarks-remote/k8s/benchmark-tenancy" \
  --kube-context "${cluster_name}" \
  --rollback-on-failure \
  --cleanup-on-fail \
  --wait \
  --timeout 10m

echo "Installed benchmark tenancy on EKS cluster ${cluster_name}"
