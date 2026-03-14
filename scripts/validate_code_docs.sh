#!/usr/bin/env bash
set -euo pipefail

ROOT="${CODE_DOCS_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$ROOT"

if [ -d "${HOME}/.asdf/shims" ]; then
  PATH="/opt/homebrew/bin:${HOME}/.asdf/shims:${PATH}"
else
  PATH="/opt/homebrew/bin:${PATH}"
fi
export PATH

if [ -x /opt/homebrew/bin/elixir ]; then
  ELIXIR_BIN=/opt/homebrew/bin/elixir
else
  ELIXIR_BIN="$(command -v elixir)"
fi

required_docs=(
  "README.md"
  "guides/user/README.md"
  "guides/developer/README.md"
)

for f in "${required_docs[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing required documentation entrypoint: $f"
    exit 1
  fi
done

TARGET_FILES="$(
  {
    find lib -type f -name '*.ex' 2>/dev/null
    find apps -type f -name '*.ex' 2>/dev/null | rg '/lib/' || true
  } | sort -u | sed '/^$/d'
)"

if [[ -z "$TARGET_FILES" ]]; then
  echo "Skipping code-doc validation: no Elixir source files found under lib or apps/*/lib."
  exit 0
fi

FILE_ARGS=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  FILE_ARGS+=("$file")
done < <(printf '%s\n' "$TARGET_FILES")

if [[ "${#FILE_ARGS[@]}" -eq 0 ]]; then
  echo "Skipping code-doc validation: no candidate files after filtering."
  exit 0
fi

exec "${ELIXIR_BIN}" ./scripts/validate_code_docs.exs -- "${FILE_ARGS[@]}"
