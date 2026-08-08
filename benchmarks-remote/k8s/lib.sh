#!/usr/bin/env bash

aws_cli() {
  AWS_PAGER='' aws --region "${region}" "$@"
}

ssm_run() {
  local command=$1
  local timeout=${2:-600}
  local deadline=$((SECONDS + timeout + 30))
  local invocation
  local parameters
  local command_id
  local command_status
  parameters=$(mktemp)
  jq -n --arg command "${command}" '{commands: [$command]}' >"${parameters}"
  command_id=$(aws_cli ssm send-command \
    --instance-ids "${server_instance_id}" \
    --document-name AWS-RunShellScript \
    --timeout-seconds "${timeout}" \
    --parameters "file://${parameters}" \
    --query Command.CommandId \
    --output text)
  rm -f "${parameters}"

  while ((SECONDS < deadline)); do
    if invocation=$(aws_cli ssm get-command-invocation \
      --command-id "${command_id}" \
      --instance-id "${server_instance_id}" \
      --output json 2>/dev/null); then
      command_status=$(jq -r .Status <<<"${invocation}")
      case "${command_status}" in
        Success)
          jq -r .StandardOutputContent <<<"${invocation}"
          if [[ -n $(jq -r .StandardErrorContent <<<"${invocation}") ]]; then
            jq -r .StandardErrorContent <<<"${invocation}" >&2
          fi
          return 0
          ;;
        Pending | InProgress | Delayed)
          ;;
        *)
          jq -r .StandardOutputContent <<<"${invocation}" >&2
          jq -r .StandardErrorContent <<<"${invocation}" >&2
          echo "SSM command ${command_id} finished with status ${command_status}" >&2
          return 1
          ;;
      esac
    fi
    sleep 10
  done

  echo "Timed out waiting for SSM command ${command_id}" >&2
  return 1
}
