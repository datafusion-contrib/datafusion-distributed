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
ZIG_LOCAL_CACHE_DIR=${ZIG_LOCAL_CACHE_DIR:-${TMPDIR:-/tmp}/datafusion-distributed-zig-ballista} \
CARGO_HOME=${CARGO_HOME:-${TMPDIR:-/tmp}/datafusion-distributed-ballista-cargo-home} \
  cargo zigbuild \
  --manifest-path "${root}/benchmarks-remote/engines/ballista/Cargo.toml" \
  --release \
  --target x86_64-unknown-linux-gnu
target="${root}/benchmarks-remote/engines/ballista/target/x86_64-unknown-linux-gnu/release"
artifacts='{}'
for binary in ballista-scheduler ballista-executor ballista-http; do
  sha=$(shasum -a 256 "${target}/${binary}" | awk '{print $1}')
  key=".benchmark-artifacts/ballista/${sha}/${binary}"
  if ! aws_cli s3api head-object --bucket "${dataset_bucket}" --key "${key}" >/dev/null 2>&1; then
    aws_cli s3 cp "${target}/${binary}" "s3://${dataset_bucket}/${key}"
  fi
  artifacts=$(jq --arg binary "${binary}" --arg uri "s3://${dataset_bucket}/${key}" \
    '.[$binary] = $uri' <<<"${artifacts}")
done
current='{}'
if [[ -f ${runtime_file} ]]; then
  current=$(cat "${runtime_file}")
fi
runtime_tmp=$(mktemp "${runtime_file}.XXXXXX")
trap 'rm -f "${runtime_tmp}"' EXIT INT TERM HUP
jq --argjson artifacts "${artifacts}" '.ballistaArtifacts = $artifacts' \
  <<<"${current}" >"${runtime_tmp}"
mv "${runtime_tmp}" "${runtime_file}"
trap - EXIT INT TERM HUP
echo "Published Ballista binaries"
