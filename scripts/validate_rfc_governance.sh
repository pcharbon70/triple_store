#!/usr/bin/env bash
set -euo pipefail

ROOT="${RFC_GOVERNANCE_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$ROOT"

if [[ ! -d "rfcs" ]]; then
  echo "Skipping RFC governance validation: missing rfcs/ directory."
  exit 0
fi

if [[ ! -f "rfcs/index.md" ]]; then
  echo "ERROR: missing RFC index: rfcs/index.md"
  exit 1
fi

if [[ ! -f "rfcs/templates/rfc-template.md" ]]; then
  echo "ERROR: missing RFC template: rfcs/templates/rfc-template.md"
  exit 1
fi

echo "RFC governance validation passed."
