#!/usr/bin/env bash
set -euo pipefail

ROOT="${GUIDES_GOVERNANCE_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$ROOT"

required_files=(
  "guides/README.md"
  "guides/user/README.md"
  "guides/developer/README.md"
)

for f in "${required_files[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing required guide index: $f"
    exit 1
  fi
done

GUIDE_FILES="$(find guides -type f -name '*.md' | sort)"

if [[ -z "$GUIDE_FILES" ]]; then
  echo "ERROR: no guide markdown files found under guides/"
  exit 1
fi

failures=0

fail() {
  echo "FAIL: $1"
  failures=1
}

extract_links() {
  local file="$1"
  rg -o '\[[^]]+\]\(([^)]+)\)' "$file" \
    | sed -E 's/^[^()]*\(([^)]+)\).*$/\1/' \
    | sort -u || true
}

echo "Checking guide entry points..."
if ! rg -q 'specs/README\.md' guides/developer/README.md; then
  fail "guides/developer/README.md must reference specs/README.md"
fi

if ! rg -q 'user/README\.md' guides/README.md; then
  fail "guides/README.md must reference user/README.md"
fi

if ! rg -q 'developer/README\.md' guides/README.md; then
  fail "guides/README.md must reference developer/README.md"
fi

echo "Checking guide files for headings and valid local markdown links..."
while IFS= read -r guide; do
  [[ -z "$guide" ]] && continue

  if ! rg -q '^# ' "$guide"; then
    fail "guide is missing a top-level heading: $guide"
  fi

  while IFS= read -r target; do
    [[ -z "$target" ]] && continue

    case "$target" in
      http://*|https://*|mailto:*|\#*)
        continue
        ;;
    esac

    clean_target="${target%%#*}"
    [[ -z "$clean_target" ]] && continue

    candidate="$(dirname "$guide")/$clean_target"
    if [[ ! -e "$candidate" ]]; then
      fail "guide references missing local path '$clean_target' in $guide"
    fi
  done < <(extract_links "$guide")
done <<< "$GUIDE_FILES"

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "Guides governance validation failed."
  exit 1
fi

echo "Guides governance validation passed."
