#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
runtime_file=${K8S_RUNTIME_FILE:-${root}/benchmarks-remote/k8s/.runtime.json}

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; deploy the Pulumi stack first" >&2
  exit 2
fi
server_instance_id=$(jq -er '.k3sServerInstanceId' "${outputs_file}")
results_bucket=$(jq -er '.resultsBucketName' "${outputs_file}")
repository=$(jq -er '.repositoryUrls.datafusion' "${outputs_file}")
tag=${IMAGE_TAG:-$(git -C "${root}" rev-parse --short=12 HEAD)}
worker_image="${repository}:${tag}"
source "${root}/benchmarks-remote/k8s/lib.sh"

describe_error=$(mktemp)
if aws_cli ecr describe-images \
  --repository-name "${repository#*/}" \
  --image-ids imageTag="${tag}" >/dev/null 2>"${describe_error}"; then
  jq -n --arg workerImage "${worker_image}" '{workerImage: $workerImage}' >"${runtime_file}"
  rm -f "${describe_error}"
  echo "Using existing immutable worker image ${worker_image}"
  exit 0
fi
if ! rg -q 'ImageNotFoundException' "${describe_error}"; then
  cat "${describe_error}" >&2
  rm -f "${describe_error}"
  exit 1
fi
rm -f "${describe_error}"

CARGO_BUILD_RUSTC_WRAPPER= \
  cargo zigbuild \
  --manifest-path "${root}/Cargo.toml" \
  --package datafusion-distributed-benchmark-worker \
  --release \
  --bin worker \
  --target x86_64-unknown-linux-gnu

context=$(mktemp -d)
archive=$(mktemp)
cleanup_context() {
  rm -rf "${context}"
  rm -f "${archive}"
}
trap cleanup_context EXIT
cp "${root}/benchmarks-remote/k8s/images/datafusion/Dockerfile" "${context}/Dockerfile"
cp "${root}/target/x86_64-unknown-linux-gnu/release/worker" "${context}/worker"
COPYFILE_DISABLE=1 tar -czf "${archive}" -C "${context}" Dockerfile worker
artifact="s3://${results_bucket}/runs/bootstrap/images/datafusion-worker-${tag}.tar.gz"
aws_cli s3 cp "${archive}" "${artifact}"

ssm_run "set -euo pipefail; if ! command -v docker >/dev/null; then dnf install -y docker; systemctl enable --now docker; fi; build_dir=/tmp/datafusion-worker-${tag}; rm -rf \"\${build_dir}\"; mkdir -p \"\${build_dir}\"; aws s3 cp ${artifact} /tmp/datafusion-worker-${tag}.tar.gz; tar -xzf /tmp/datafusion-worker-${tag}.tar.gz -C \"\${build_dir}\"; aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository%%/*}; docker build --pull -t ${worker_image} \"\${build_dir}\"; docker push ${worker_image}" 3600

jq -n --arg workerImage "${worker_image}" '{workerImage: $workerImage}' >"${runtime_file}"
echo "Published ${worker_image}"
