#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
rendered=$(mktemp)
trap 'rm -f "${rendered}"' EXIT

helm lint "${chart_dir}"
helm template benchmark "${chart_dir}" >"${rendered}"

for engine in datafusion ballista spark trino; do
  rg --quiet "name: benchmark-${engine}" "${rendered}"
  rg --quiet "benchmark.datafusion.apache.org/engine: ${engine}" "${rendered}"
done

if rg --quiet '^kind: (Role|RoleBinding|ClusterRole|ClusterRoleBinding)$' "${rendered}"; then
  echo "local benchmark runners must not require in-cluster RBAC" >&2
  exit 1
fi
