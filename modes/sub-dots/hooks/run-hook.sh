#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SUBDOTS_PY="${SCRIPT_DIR}/../../../tools/subdots.py"
if [ ! -f "${SUBDOTS_PY}" ]; then
  printf '{}\n'
  exit 0
fi
MODE="${1:-doctor}"
if [ "$#" -gt 0 ]; then shift; fi
if ! out=$(python3 -B "${SUBDOTS_PY}" hook "${MODE}" "$@" 2>/dev/null); then
  printf '{}\n'
  exit 0
fi
if [ -n "${out}" ]; then
  printf '%s\n' "${out}"
else
  printf '{}\n'
fi
