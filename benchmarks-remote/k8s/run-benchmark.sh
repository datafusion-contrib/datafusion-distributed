#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
engine=${1:?usage: run-benchmark.sh ENGINE [benchmark options]}
shift
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
run_id="$(date -u +%Y%m%dt%H%M%sz)-$$"

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; run npm run foundation-deploy first" >&2
  exit 2
fi
cluster_name=$(jq -er '.clusterName' "${outputs_file}")
dataset_bucket=$(jq -er '.datasetBucketName' "${outputs_file}")
source "${root}/benchmarks-remote/k8s/lib.sh"

namespace="benchmark-${engine}"

ensure_kubeconfig

lock_acquired=false
heartbeat_pid=
port_forward_pid=
port_forward_log=$(mktemp)
cleanup() {
  local exit_code=$?
  local cleanup_code=0
  trap - EXIT
  trap '' INT TERM HUP
  set +e
  for pid in "${port_forward_pid}" "${heartbeat_pid}"; do
    [[ -z ${pid} ]] || kill "${pid}" 2>/dev/null
    [[ -z ${pid} ]] || wait "${pid}" 2>/dev/null
  done
  if ${lock_acquired}; then
    benchmark_lock_release "${run_id}" || cleanup_code=$?
  fi
  rm -f -- "${port_forward_log}"
  if [[ ${exit_code} -eq 0 && ${cleanup_code} -ne 0 ]]; then
    exit_code=${cleanup_code}
  fi
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM HUP

benchmark_lock_acquire "${run_id}"
lock_acquired=true
runner_pid=$$
(
  sleep_pid=
  trap 'kill "${sleep_pid}" 2>/dev/null; exit 0' INT TERM HUP
  while true; do
    sleep "${benchmark_lock_heartbeat_seconds}" &
    sleep_pid=$!
    wait "${sleep_pid}"
    sleep_pid=
    if ! benchmark_lock_heartbeat "${run_id}"; then
      echo "Lost ownership of the benchmark cluster lock; aborting run ${run_id}" >&2
      kill -TERM "${runner_pid}"
      exit 1
    fi
  done
) &
heartbeat_pid=$!

kubectl --context "${cluster_name}" port-forward \
  --namespace "${namespace}" \
  "service/${engine}" \
  "9000:9000" >"${port_forward_log}" 2>&1 &
port_forward_pid=$!
until grep -q '^Forwarding from ' "${port_forward_log}"; do
  if ! kill -0 "${port_forward_pid}" 2>/dev/null; then
    cat "${port_forward_log}" >&2
    wait "${port_forward_pid}" || true
    exit 1
  fi
  sleep 0.1
done

cd "${root}/benchmarks-remote"
env "BENCHMARK_BUCKET=s3://${dataset_bucket}" \
  npm run "runner:${engine}-bench" -- "$@"

echo "Benchmark run completed"
