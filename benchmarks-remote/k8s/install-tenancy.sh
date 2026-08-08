#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; deploy the Pulumi stack first" >&2
  exit 2
fi
server_instance_id=$(jq -er '.k3sServerInstanceId' "${outputs_file}")
results_bucket=$(jq -er '.resultsBucketName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"

for attempt in $(seq 1 60); do
  ping_status=$(aws_cli ssm describe-instance-information \
    --filters Key=InstanceIds,Values="${server_instance_id}" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text)
  if [[ ${ping_status} == Online ]]; then
    break
  fi
  if [[ ${attempt} -eq 60 ]]; then
    echo "Timed out waiting for the k3s server to register with SSM" >&2
    exit 1
  fi
  sleep 5
done

ssm_run "for attempt in \$(seq 1 60); do if systemctl is-active --quiet k3s && k3s kubectl get node >/dev/null 2>&1; then exit 0; fi; sleep 5; done; journalctl -u k3s --no-pager | tail -n 100; exit 1" 330

manifest=$(mktemp)
HELM_CACHE_HOME=/tmp/datafusion-distributed-helm-cache \
  HELM_CONFIG_HOME=/tmp/datafusion-distributed-helm-config \
  HELM_DATA_HOME=/tmp/datafusion-distributed-helm-data \
  helm template benchmark-tenancy "${root}/benchmarks-remote/k8s/benchmark-tenancy" >"${manifest}"
artifact="s3://${results_bucket}/runs/bootstrap/benchmark-tenancy.yaml"
aws_cli s3 cp "${manifest}" "${artifact}"
rm -f "${manifest}"
ssm_run "aws s3 cp ${artifact} /tmp/benchmark-tenancy.yaml; k3s kubectl apply -f /tmp/benchmark-tenancy.yaml"

echo "Installed benchmark tenancy on ${server_instance_id}"
