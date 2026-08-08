#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pulumi_bin=${PULUMI_BIN:-pulumi}

"${pulumi_bin}" up --stack benchmark --yes
"${pulumi_bin}" stack output --stack benchmark --json >"${script_dir}/.pulumi-outputs.json"
"${script_dir}/../k8s/install-tenancy.sh"
