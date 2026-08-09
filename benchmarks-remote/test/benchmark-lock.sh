#!/usr/bin/env bash
set -euo pipefail

test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
region=us-east-1
cluster_name=test
source "${test_dir}/../k8s/lib.sh"

lock_state=
kubectl_cli() {
  if [[ $1 == create ]]; then
    if [[ -n ${lock_state} ]]; then
      return 1
    fi
    local run_id= status= heartbeat= argument
    for argument in "$@"; do
      case ${argument} in
        --from-literal=run-id=*) run_id=${argument#--from-literal=run-id=} ;;
        --from-literal=status=*) status=${argument#--from-literal=status=} ;;
        --from-literal=heartbeat=*) heartbeat=${argument#--from-literal=heartbeat=} ;;
      esac
    done
    lock_state=$(jq -nc --arg runId "${run_id}" --arg status "${status}" \
      --arg heartbeat "${heartbeat}" \
      '{metadata: {resourceVersion: "1"}, data: {"run-id": $runId, status: $status, heartbeat: $heartbeat}}')
    return
  fi
  if [[ $1 == get ]]; then
    [[ -n ${lock_state} ]] || return 1
    printf '%s\n' "${lock_state}"
    return
  fi
  if [[ $1 == patch ]]; then
    local patch= argument expected_resource_version current_resource_version
    local expected_run_id expected_status current_run_id current_status
    local next_run_id next_status next_heartbeat
    for argument in "$@"; do
      [[ ${argument} == \[* ]] && patch=${argument}
    done
    expected_resource_version=$(jq -r \
      '[.[] | select(.op == "test" and .path == "/metadata/resourceVersion")][0].value // empty' \
      <<<"${patch}")
    current_resource_version=$(jq -r '.metadata.resourceVersion' <<<"${lock_state}")
    expected_run_id=$(jq -r \
      '[.[] | select(.op == "test" and .path == "/data/run-id")][0].value // empty' \
      <<<"${patch}")
    expected_status=$(jq -r \
      '[.[] | select(.op == "test" and .path == "/data/status")][0].value // empty' \
      <<<"${patch}")
    current_run_id=$(jq -r '.data["run-id"]' <<<"${lock_state}")
    current_status=$(jq -r '.data.status // "active"' <<<"${lock_state}")
    [[ -z ${expected_resource_version} || ${current_resource_version} == "${expected_resource_version}" ]] || return 1
    [[ ${current_run_id} == "${expected_run_id}" ]] || return 1
    [[ -z ${expected_status} || ${current_status} == "${expected_status}" ]] || return 1
    next_run_id=$(jq -r \
      '[.[] | select(.op == "add" and .path == "/data/run-id")][0].value // empty' \
      <<<"${patch}")
    next_status=$(jq -r \
      '[.[] | select(.op == "add" and .path == "/data/status")][0].value // empty' \
      <<<"${patch}")
    next_heartbeat=$(jq -r \
      '[.[] | select(.op == "add" and .path == "/data/heartbeat")][0].value // empty' \
      <<<"${patch}")
    lock_state=$(jq --arg runId "${next_run_id}" --arg status "${next_status}" \
      --arg heartbeat "${next_heartbeat}" '
        if $runId != "" then .data["run-id"] = $runId else . end |
        if $status != "" then .data.status = $status else . end |
        if $heartbeat != "" then .data.heartbeat = $heartbeat else . end |
        .metadata.resourceVersion = ((.metadata.resourceVersion | tonumber) + 1 | tostring)
      ' <<<"${lock_state}")
    return
  fi
  if [[ $1 == delete ]]; then
    lock_state=
    return
  fi
  return 2
}

benchmark_lock_acquire run-1
benchmark_lock_is_active
benchmark_lock_heartbeat run-1
if benchmark_lock_heartbeat wrong-owner 2>/dev/null; then
  exit 9
fi
if benchmark_lock_acquire run-2 2>/dev/null; then
  exit 10
fi
benchmark_lock_release run-1
[[ -z ${lock_state} ]]
if benchmark_lock_is_active; then
  exit 11
fi
benchmark_lock_acquire run-2
lock_state=$(jq '.data.heartbeat = "0"' <<<"${lock_state}")
benchmark_lock_acquire run-3
if benchmark_lock_release run-2 2>/dev/null; then
  exit 12
fi
[[ $(jq -r '.data["run-id"]' <<<"${lock_state}") == run-3 ]]
benchmark_lock_release run-3
[[ -z ${lock_state} ]]
