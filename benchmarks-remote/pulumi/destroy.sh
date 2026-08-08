#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pulumi_bin=${PULUMI_BIN:-pulumi}

"${pulumi_bin}" state unprotect --stack benchmark --all --yes
"${pulumi_bin}" destroy --stack benchmark --yes
rm -f "${script_dir}/.pulumi-outputs.json" "${script_dir}/../k8s/.runtime.json"
