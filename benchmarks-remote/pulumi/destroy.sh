#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pulumi_bin=${PULUMI_BIN:-pulumi}
cd "${script_dir}"
export AWS_REGION=${AWS_REGION:-us-east-1}
if ! AWS_PAGER='' aws --region "${AWS_REGION}" sts get-caller-identity >/dev/null; then
  echo "AWS credentials are missing or expired; select AWS_PROFILE and run aws sso login" >&2
  exit 1
fi

if [[ -n ${PULUMI_BACKEND_URL:-} ]]; then
  "${pulumi_bin}" login "${PULUMI_BACKEND_URL}"
fi
rm -f "${script_dir}/.pulumi-outputs.json" "${script_dir}/../k8s/.runtime.json"
"${pulumi_bin}" stack select benchmark
"${pulumi_bin}" state unprotect --stack benchmark --all --yes
"${pulumi_bin}" destroy --stack benchmark --yes
