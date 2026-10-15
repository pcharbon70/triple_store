#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)
python_executable=${TRIPLESTORE_PYTHON:-$(command -v python3 || true)}
if [[ -n "${TRIPLESTORE_MKDOCS:-}" ]]; then
    mkdocs_executable=$TRIPLESTORE_MKDOCS
elif [[ -x "$repo_root/.venv-docs/bin/mkdocs" ]]; then
    mkdocs_executable="$repo_root/.venv-docs/bin/mkdocs"
else
    mkdocs_executable=$(command -v mkdocs || true)
fi

if [[ -z "$python_executable" ]]; then
    echo "Code Companion verification requires Python 3" >&2
    exit 2
fi
"$python_executable" "$script_dir/check-code-companion.py"

if [[ -z "$mkdocs_executable" ]]; then
    echo "Code Companion verification requires MkDocs; install requirements-docs.txt or set TRIPLESTORE_MKDOCS" >&2
    exit 2
fi

site_dir=$(mktemp -d /tmp/triple-store-code-companion.XXXXXX)
trap 'rm -rf -- "$site_dir"' EXIT

cd "$repo_root"
"$mkdocs_executable" build --strict --site-dir "$site_dir"
echo "TripleStore Code Companion verification passed"
