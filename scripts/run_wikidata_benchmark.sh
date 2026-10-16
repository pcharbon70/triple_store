#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ${1:-} == "--help" || ${1:-} == "-h" || $# -eq 0 ]]; then
  cat <<'EOF'
Usage: ./scripts/run_wikidata_benchmark.sh <mode> [options]

Modes:
  parser | corpus-smoke | smoke | medium | full

All remaining options are passed to `mix benchmark.wikidata`.
EOF
  [[ $# -eq 0 ]] && exit 2
  exit 0
fi

cd "$repo_root"
exec mix benchmark.wikidata "$@"
