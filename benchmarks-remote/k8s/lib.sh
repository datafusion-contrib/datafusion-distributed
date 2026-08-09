#!/usr/bin/env bash

k8s_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export KUBECONFIG=${KUBECONFIG:-${k8s_dir}/.kubeconfig}

aws_cli() {
  AWS_PAGER='' aws --region "${region}" "$@"
}

require_aws_credentials() {
  if ! aws_cli sts get-caller-identity >/dev/null; then
    echo "AWS credentials are missing or expired; select AWS_PROFILE and run aws sso login" >&2
    return 1
  fi
}

ensure_kubeconfig() {
  if [[ ${REFRESH_KUBECONFIG:-false} != true && -f ${KUBECONFIG} ]] && \
    [[ $(kubectl config get-contexts "${cluster_name}" -o name 2>/dev/null) == "${cluster_name}" ]]; then
    return
  fi
  aws_cli eks update-kubeconfig \
    --name "${cluster_name}" \
    --alias "${cluster_name}" \
    --kubeconfig "${KUBECONFIG}" >/dev/null
}

kubectl_cli() {
  kubectl --context "${cluster_name}" "$@"
}

benchmark_lock_name=benchmark-run-lock
benchmark_lock_namespace=benchmark-system
benchmark_lock_timeout_seconds=${BENCHMARK_LOCK_TIMEOUT_SECONDS:-300}
benchmark_lock_heartbeat_seconds=${BENCHMARK_LOCK_HEARTBEAT_SECONDS:-30}

benchmark_lock_is_active() {
  local lock_json status heartbeat now
  if ! lock_json=$(kubectl_cli get configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" -o json 2>/dev/null); then
    return 1
  fi
  status=$(jq -r '.data.status // "active"' <<<"${lock_json}")
  heartbeat=$(jq -r '.data.heartbeat // "0"' <<<"${lock_json}")
  now=$(date +%s)
  [[ ${status} == active && ${heartbeat} =~ ^[0-9]+$ && \
    $((now - heartbeat)) -lt ${benchmark_lock_timeout_seconds} ]]
}

benchmark_lock_acquire() {
  local run_id=$1 now lock_json resource_version current_run_id status heartbeat patch
  now=$(date +%s)
  if kubectl_cli create configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" \
    --from-literal=run-id="${run_id}" \
    --from-literal=status=active \
    --from-literal=heartbeat="${now}" >/dev/null 2>&1; then
    return
  fi

  lock_json=$(kubectl_cli get configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" -o json)
  resource_version=$(jq -r '.metadata.resourceVersion // ""' <<<"${lock_json}")
  current_run_id=$(jq -r '.data["run-id"] // ""' <<<"${lock_json}")
  status=$(jq -r '.data.status // "active"' <<<"${lock_json}")
  heartbeat=$(jq -r '.data.heartbeat // "0"' <<<"${lock_json}")
  if [[ ${status} == releasing && ${heartbeat} =~ ^[0-9]+$ && \
    $((now - heartbeat)) -lt ${benchmark_lock_timeout_seconds} ]]; then
    echo "Benchmark run ${current_run_id} is still releasing the cluster lock; retry shortly" >&2
    return 1
  fi
  if [[ ${status} == active && ${heartbeat} =~ ^[0-9]+$ && \
    $((now - heartbeat)) -lt ${benchmark_lock_timeout_seconds} ]]; then
    echo "Benchmark run ${current_run_id} already holds the cluster lock" >&2
    return 1
  fi

  patch=$(jq -nc \
    --arg resourceVersion "${resource_version}" \
    --arg currentRunId "${current_run_id}" \
    --arg runId "${run_id}" \
    --arg heartbeat "${now}" \
    '[
      {op: "test", path: "/metadata/resourceVersion", value: $resourceVersion},
      {op: "test", path: "/data/run-id", value: $currentRunId},
      {op: "add", path: "/data/run-id", value: $runId},
      {op: "add", path: "/data/status", value: "active"},
      {op: "add", path: "/data/heartbeat", value: $heartbeat}
    ]')
  if ! kubectl_cli patch configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" --type=json -p "${patch}" >/dev/null; then
    echo "Another benchmark acquired the cluster lock" >&2
    return 1
  fi
}

benchmark_lock_heartbeat() {
  local run_id=$1 now patch
  now=$(date +%s)
  patch=$(jq -nc --arg runId "${run_id}" --arg heartbeat "${now}" \
    '[
      {op: "test", path: "/data/run-id", value: $runId},
      {op: "test", path: "/data/status", value: "active"},
      {op: "add", path: "/data/heartbeat", value: $heartbeat}
    ]')
  kubectl_cli patch configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" --type=json -p "${patch}" >/dev/null
}

benchmark_lock_release() {
  local run_id=$1 now patch
  now=$(date +%s)
  patch=$(jq -nc --arg runId "${run_id}" --arg heartbeat "${now}" \
    '[
      {op: "test", path: "/data/run-id", value: $runId},
      {op: "add", path: "/data/status", value: "releasing"},
      {op: "add", path: "/data/heartbeat", value: $heartbeat}
    ]')
  if ! kubectl_cli patch configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" --type=json -p "${patch}" >/dev/null; then
    return 1
  fi
  kubectl_cli delete configmap "${benchmark_lock_name}" \
    --namespace "${benchmark_lock_namespace}" --wait=false >/dev/null
}
