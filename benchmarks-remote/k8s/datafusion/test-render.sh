#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
helm lint "${chart_dir}" \
  --set worker.image=example.invalid/datafusion:test \
  --set worker.datasetBucket=test-datasets
helm template test "${chart_dir}" \
  --set worker.image=example.invalid/datafusion:test \
  --set worker.datasetBucket=test-datasets >/dev/null
