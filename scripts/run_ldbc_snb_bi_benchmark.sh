#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
  cat <<'EOF'
Usage: ./scripts/run_ldbc_snb_bi_benchmark.sh [ExUnit options]

Run the complete reduced LDBC SNB BI v1.0.3 qualification workflow, including
all 31 read variants and accepted-answer comparisons. The resulting evidence is
diagnostic and is not an official or comparable LDBC score.
EOF
  exit 0
fi

cd "$repo_root"
exec mix test test/triple_store/benchmark/ldbc/phase_5d_qualification_integration_test.exs "$@"
