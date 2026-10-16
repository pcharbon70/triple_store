#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ${1:-} == "--help" || ${1:-} == "-h" || $# -eq 0 ]]; then
  cat <<'EOF'
Usage: ./scripts/run_watdiv_benchmark.sh [options]

Run a correctness-gated benchmark using official WatDiv artifacts.

Required options:
  --dataset FILE
  --dataset-manifest FILE
  --workload-dir DIR
  --reference-dir DIR
  --output-dir DIR

Optional settings are forwarded to `mix benchmark.watdiv run`; use the Mix task
documentation for --warmup, --iterations, --timeout-ms, --batch-size,
--store-path, and --keep-store.
EOF
  [[ $# -eq 0 ]] && exit 2
  exit 0
fi

cd "$repo_root"
exec mix benchmark.watdiv run "$@"
