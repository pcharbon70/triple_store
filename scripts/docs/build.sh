#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)
if [[ -n "${TRIPLESTORE_MKDOCS:-}" ]]; then
    mkdocs_executable=$TRIPLESTORE_MKDOCS
elif [[ -x "$repo_root/.venv-docs/bin/mkdocs" ]]; then
    mkdocs_executable="$repo_root/.venv-docs/bin/mkdocs"
else
    mkdocs_executable=$(command -v mkdocs || true)
fi

if [[ -z "$mkdocs_executable" ]]; then
    echo "MkDocs is required; install requirements-docs.txt or set TRIPLESTORE_MKDOCS" >&2
    exit 2
fi

cd "$repo_root"
"$mkdocs_executable" build --strict
