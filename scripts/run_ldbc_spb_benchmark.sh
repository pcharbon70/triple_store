#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
  cat <<'EOF'
Usage: ./scripts/run_ldbc_spb_benchmark.sh [ExUnit options]

Run the deterministic LDBC SPB 2.0.2 smoke workflow. This workflow produces
diagnostic evidence and is not an official or comparable LDBC run.
EOF
  exit 0
fi

cd "$repo_root"
exec mix test test/triple_store/benchmark/ldbc/phase_4_spb_integration_test.exs "$@"
