#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
worker_values="${chart_dir}/../worker-resources.yaml"
helm lint "${chart_dir}" \
  --values "${worker_values}" \
  --set worker.artifact=s3://test-datasets/artifacts/worker \
  --set worker.datasetBucket=test-datasets
helm template test "${chart_dir}" \
  --values "${worker_values}" \
  --set worker.artifact=s3://test-datasets/artifacts/worker \
  --set worker.datasetBucket=test-datasets >/dev/null
