#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
engine=${1:?usage: publish-image.sh ENGINE}
if [[ ${engine} != spark ]]; then
  echo "No container image publisher is defined for ${engine}" >&2
  exit 2
fi
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
runtime_file=${K8S_RUNTIME_FILE:-${root}/benchmarks-remote/k8s/.runtime.json}
if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; run npm run foundation-deploy first" >&2
  exit 2
fi
results_bucket=$(jq -er '.resultsBucketName' "${outputs_file}")
repository=$(jq -er --arg engine "${engine}" '.repositoryUrls[$engine]' "${outputs_file}")
builder=$(jq -er '.imageBuilderProjectName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"
archive=
runtime_tmp=
cleanup() {
  rm -f "${archive:-}" "${runtime_tmp:-}"
}
trap cleanup EXIT INT TERM HUP

context="${root}/benchmarks-remote/engines/${engine}"
tag=$(shasum -a 256 "${context}/Dockerfile" "${context}/spark_http.py" |
  shasum -a 256 | awk '{print substr($1, 1, 24)}')
image="${repository}:${tag}"
if ! aws_cli ecr describe-images \
  --repository-name "${repository#*/}" \
  --image-ids imageTag="${tag}" >/dev/null 2>&1; then
  archive=$(mktemp)
  COPYFILE_DISABLE=1 tar -czf "${archive}" -C "${context}" Dockerfile spark_http.py
  artifact="s3://${results_bucket}/runs/bootstrap/images/${engine}-${tag}.tar.gz"
  aws_cli s3 cp "${archive}" "${artifact}"
  build_id=$(aws_cli codebuild start-build \
    --project-name "${builder}" \
    --environment-variables-override \
      name=BUILD_CONTEXT,value="${artifact}",type=PLAINTEXT \
      name=IMAGE_URI,value="${image}",type=PLAINTEXT \
    --query 'build.id' \
    --output text)
  for attempt in $(seq 1 240); do
    status=$(aws_cli codebuild batch-get-builds \
      --ids "${build_id}" \
      --query 'builds[0].buildStatus' \
      --output text)
    case ${status} in
      SUCCEEDED) break ;;
      FAILED | FAULT | STOPPED | TIMED_OUT)
        if aws_cli ecr describe-images \
          --repository-name "${repository#*/}" \
          --image-ids imageTag="${tag}" >/dev/null 2>&1; then
          break
        fi
        aws_cli codebuild batch-get-builds --ids "${build_id}" >&2
        exit 1
        ;;
    esac
    if [[ ${attempt} -eq 240 ]]; then
      echo "Timed out waiting for CodeBuild build ${build_id}" >&2
      exit 1
    fi
    sleep 5
  done
fi

current='{}'
if [[ -f ${runtime_file} ]]; then
  current=$(cat "${runtime_file}")
fi
runtime_tmp=$(mktemp "${runtime_file}.XXXXXX")
jq --arg engine "${engine}" --arg image "${image}" \
  '.images = (.images // {}) | .images[$engine] = $image' <<<"${current}" >"${runtime_tmp}"
mv "${runtime_tmp}" "${runtime_file}"
runtime_tmp=
echo "Published ${image}"
