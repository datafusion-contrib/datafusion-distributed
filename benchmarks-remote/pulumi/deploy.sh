#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pulumi_bin=${PULUMI_BIN:-pulumi}
cd "${script_dir}"
export AWS_REGION=${AWS_REGION:-us-east-1}
secrets_provider=${PULUMI_SECRETS_PROVIDER:-awskms://alias/datafusion-bench-pulumi-state?region=${AWS_REGION}}
if ! AWS_PAGER='' aws --region "${AWS_REGION}" sts get-caller-identity >/dev/null; then
  echo "AWS credentials are missing or expired; select AWS_PROFILE and run aws sso login" >&2
  exit 1
fi
if [[ -z ${KUBERNETES_API_ALLOWED_CIDRS:-} ]]; then
  public_ip=$(curl --fail --silent --show-error https://checkip.amazonaws.com)
  export KUBERNETES_API_ALLOWED_CIDRS="${public_ip}/32"
fi

if [[ -n ${PULUMI_BACKEND_URL:-} ]]; then
  "${pulumi_bin}" login "${PULUMI_BACKEND_URL}"
fi
"${pulumi_bin}" stack select benchmark --create --secrets-provider "${secrets_provider}"
"${pulumi_bin}" up --stack benchmark --yes
outputs_tmp=$(mktemp "${script_dir}/.pulumi-outputs.json.XXXXXX")
trap 'rm -f "${outputs_tmp}"' EXIT INT TERM HUP
"${pulumi_bin}" stack output --stack benchmark --json >"${outputs_tmp}"
mv "${outputs_tmp}" "${script_dir}/.pulumi-outputs.json"
trap - EXIT INT TERM HUP
REFRESH_KUBECONFIG=true "${script_dir}/../k8s/install-tenancy.sh"
