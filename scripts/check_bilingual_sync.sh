#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-.}"; EN_DIR="${ROOT}/docs/harness"; ZH_DIR="${ROOT}/docs/zh/harness"
if [[ ! -d "${EN_DIR}" || ! -d "${ZH_DIR}" ]]; then
  echo "missing docs/harness or docs/zh/harness" >&2
  exit 1
fi
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
failed=0; shopt -s nullglob
count_heading() {
  awk -v pat="^$1 " '$0 ~ pat { n++ } END { print n + 0 }' "$2"
}
h2_keys() {
  grep '^## ' "$1" | sed -E 's/^##[[:space:]]+(([0-9]+\.)+).*/## \1/' || true
}
compare_pair() {
  local en="$1" base zh en_h2 zh_h2 en_h3 zh_h3
  base="$(basename "${en}")"
  zh="${ZH_DIR}/${base}"
  if [[ ! -f "${zh}" ]]; then
    echo "missing Chinese mirror: ${zh}" >&2
    failed=1
    return
  fi
  en_h2="$(count_heading '##' "${en}")"
  zh_h2="$(count_heading '##' "${zh}")"
  en_h3="$(count_heading '###' "${en}")"
  zh_h3="$(count_heading '###' "${zh}")"
  if [[ "${en_h2}/${en_h3}" != "${zh_h2}/${zh_h3}" ]]; then
    echo "${base}: heading count mismatch EN H2/H3=${en_h2}/${en_h3} ZH H2/H3=${zh_h2}/${zh_h3}" >&2
    failed=1
  fi
  h2_keys "${en}" >"${tmpdir}/${base}.en.h2"
  h2_keys "${zh}" >"${tmpdir}/${base}.zh.h2"
  diff -u "${tmpdir}/${base}.en.h2" "${tmpdir}/${base}.zh.h2" || {
    echo "${base}: H2 headline order mismatch" >&2
    failed=1
  }
}
for zh in "${ZH_DIR}"/*.md; do
  base="$(basename "${zh}")"
  en="${EN_DIR}/${base}"
  [[ -f "${en}" || "${base}" == "README.md" ]] || {
    echo "missing English mirror: ${EN_DIR}/${base}" >&2
    failed=1
    continue
  }
  [[ -f "${en}" ]] && compare_pair "${en}"
done
exit "${failed}"
