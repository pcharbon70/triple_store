#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
  cat <<'EOF'
Usage: ./scripts/run_gmark_benchmark.sh [options]

Options:
  --scale N        Dataset scale factor (default: 1)
  --warmup N       Warmup iterations per query (default: 2)
  --iterations N   Measured iterations per query (default: 5)
  --include-q3     Include the unbounded transitive-closure stress query
  --output-dir DIR Result directory (default: tmp/gmark_benchmark_runs/<timestamp>)
EOF
  exit 0
fi

cd "$repo_root"
exec mix run scripts/run_gmark_benchmark.exs -- "$@"
