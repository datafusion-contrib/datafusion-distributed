#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pulumi_bin=${PULUMI_BIN:-pulumi}
stack=${PULUMI_STACK:-benchmark}
cd "${script_dir}"
export AWS_REGION=${AWS_REGION:-us-east-1}
if ! AWS_PAGER='' aws --region "${AWS_REGION}" sts get-caller-identity >/dev/null; then
  echo "AWS credentials are missing or expired; select AWS_PROFILE and run aws sso login" >&2
  exit 1
fi

if [[ -n ${PULUMI_BACKEND_URL:-} ]]; then
  "${pulumi_bin}" login "${PULUMI_BACKEND_URL}"
fi
if [[ ${stack} == benchmark ]]; then
  outputs_file=${PULUMI_OUTPUTS_FILE:-${script_dir}/.pulumi-outputs.json}
  kubeconfig=${KUBECONFIG:-${script_dir}/../k8s/.kubeconfig}
else
  outputs_file=${PULUMI_OUTPUTS_FILE:-${script_dir}/.pulumi-outputs.${stack}.json}
  kubeconfig=${KUBECONFIG:-${script_dir}/../k8s/.kubeconfig.${stack}}
fi
rm -f "${outputs_file}" "${kubeconfig}"
if [[ ${stack} == benchmark ]]; then
  rm -f "${script_dir}/../k8s/.runtime.json"
fi
"${pulumi_bin}" stack select "${stack}"
"${pulumi_bin}" state unprotect --stack "${stack}" --all --yes
"${pulumi_bin}" destroy --stack "${stack}" --yes
