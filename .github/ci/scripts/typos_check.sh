#!/usr/bin/env bash
# Runs typos spell-check. Pass --write to auto-fix in place.
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
TYPOS_CONFIG="typos.toml"

MODE="check"

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write]

Checks spelling with \`typos --config ${TYPOS_CONFIG}\`.
--write    Auto-fix spelling issues in place.
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --write)
      MODE="write"
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

if [[ "$MODE" == "write" ]]; then
  echo "[${SCRIPT_NAME}] typos --write-changes --config ${TYPOS_CONFIG}"
  typos --write-changes --config "${TYPOS_CONFIG}"
else
  echo "[${SCRIPT_NAME}] typos --config ${TYPOS_CONFIG}"
  typos --config "${TYPOS_CONFIG}"
fi
