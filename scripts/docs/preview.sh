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
python_executable=${TRIPLESTORE_PYTHON:-$(command -v python3 || true)}

if [[ -z "$mkdocs_executable" || -z "$python_executable" ]]; then
    echo "Preview requires MkDocs and Python 3" >&2
    exit 2
fi

cd "$repo_root"
"$mkdocs_executable" build --strict
exec "$python_executable" -m http.server 8000 --bind 127.0.0.1 --directory "$repo_root/site-code-companion"
