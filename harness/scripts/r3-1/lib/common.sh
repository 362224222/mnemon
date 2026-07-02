#!/usr/bin/env bash
set -uo pipefail

mnemon_r3_script_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

mnemon_r3_root() {
  local dir
  dir="$(mnemon_r3_script_dir)"
  cd "$dir/../../.." && pwd
}

mnemon_r3_timestamp() {
  date -u +"%Y%m%dT%H%M%SZ"
}

mnemon_r3_out_dir() {
  local root="${MNEMON_R3_ROOT:-$(mnemon_r3_root)}"
  local stamp="${MNEMON_R3_RUN_ID:-$(mnemon_r3_timestamp)}"
  printf "%s/.mnemon-dev/tmp/r3-1-test/%s" "$root" "$stamp"
}

mnemon_r3_phase_dir() {
  local phase="$1"
  local out="${MNEMON_R3_OUT_DIR:-$(mnemon_r3_out_dir)}"
  printf "%s/%s" "$out" "$phase"
}

mnemon_r3_prepare_phase() {
  local phase="$1"
  local dir
  dir="$(mnemon_r3_phase_dir "$phase")"
  mkdir -p "$dir"
  printf "%s" "$dir"
}

mnemon_r3_log() {
  printf "[r3-1] %s\n" "$*"
}
