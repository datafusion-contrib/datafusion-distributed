#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
engine=${1:?usage: npm run command -- ENGINE COMMAND...}
shift
if [[ $# -eq 0 ]]; then
  echo "pass a command to run" >&2
  exit 2
fi
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
cluster_name=$(jq -er '.clusterName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"

case ${engine} in
  datafusion) selector='app.kubernetes.io/name=datafusion-worker' ;;
  trino | spark) selector="app.kubernetes.io/name=${engine},app.kubernetes.io/component=worker" ;;
  ballista) selector='app.kubernetes.io/name=ballista,app.kubernetes.io/component=executor' ;;
  *) echo "Unknown engine '${engine}'" >&2; exit 2 ;;
esac

ensure_kubeconfig
pod=$(kubectl_cli get pods \
  --namespace "benchmark-${engine}" \
  --selector "${selector}" \
  --field-selector status.phase=Running \
  --output jsonpath='{.items[0].metadata.name}')
if [[ -z ${pod} ]]; then
  echo "No running ${engine} worker pod" >&2
  exit 2
fi
kubectl_cli exec --namespace "benchmark-${engine}" "${pod}" -- "$@"
