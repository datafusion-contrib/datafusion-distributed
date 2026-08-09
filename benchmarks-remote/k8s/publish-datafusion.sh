#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
runtime_file=${K8S_RUNTIME_FILE:-${root}/benchmarks-remote/k8s/.runtime.json}

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; run npm run foundation-deploy first" >&2
  exit 2
fi
dataset_bucket=$(jq -er '.datasetBucketName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"

ZIG_GLOBAL_CACHE_DIR=${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/datafusion-distributed-zig-global} \
ZIG_LOCAL_CACHE_DIR=${ZIG_LOCAL_CACHE_DIR:-${TMPDIR:-/tmp}/datafusion-distributed-zig-local} \
  cargo zigbuild \
  --manifest-path "${root}/benchmarks-remote/engines/datafusion/Cargo.toml" \
  --package datafusion-distributed-benchmark-worker \
  --release \
  --bin worker \
  --target x86_64-unknown-linux-gnu

worker_binary="${root}/target/x86_64-unknown-linux-gnu/release/worker"
binary_sha=$(shasum -a 256 "${worker_binary}" | awk '{print $1}')
artifact="s3://${dataset_bucket}/.benchmark-artifacts/datafusion/${binary_sha}/worker"
if ! aws_cli s3api head-object \
  --bucket "${dataset_bucket}" \
  --key ".benchmark-artifacts/datafusion/${binary_sha}/worker" >/dev/null 2>&1; then
  aws_cli s3 cp "${worker_binary}" "${artifact}"
fi

current='{}'
if [[ -f ${runtime_file} ]]; then
  current=$(cat "${runtime_file}")
fi
runtime_tmp=$(mktemp "${runtime_file}.XXXXXX")
trap 'rm -f "${runtime_tmp}"' EXIT INT TERM HUP
jq \
  --arg workerArtifact "${artifact}" \
  --arg workerBinarySha "${binary_sha}" \
  '.workerArtifact = $workerArtifact | .workerBinarySha = $workerBinarySha' \
  <<<"${current}" >"${runtime_tmp}"
mv "${runtime_tmp}" "${runtime_file}"
trap - EXIT INT TERM HUP
echo "Published ${artifact}"
