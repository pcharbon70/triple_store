#!/usr/bin/env sh
set -eu

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

if [ -d "${HOME}/.asdf/shims" ]; then
  PATH="/opt/homebrew/bin:${HOME}/.asdf/shims:${PATH}"
else
  PATH="/opt/homebrew/bin:${PATH}"
fi
export PATH

if [ -z "${ASDF_RUST_VERSION:-}" ] && [ -x "${HOME}/.asdf/shims/cargo" ]; then
  export ASDF_RUST_VERSION="1.93.1"
fi

if [ -x /opt/homebrew/bin/mix ]; then
  MIX_BIN=/opt/homebrew/bin/mix
else
  MIX_BIN="$(command -v mix)"
fi

export MIX_ENV="${MIX_ENV:-test}"
export ERLANG_ROCKSDB_OPTS="${ERLANG_ROCKSDB_OPTS:--DCMAKE_POLICY_VERSION_MINIMUM=3.5}"

./scripts/validate_specs_governance.sh
./scripts/validate_guides_governance.sh
./scripts/validate_rfc_governance.sh
./scripts/validate_code_docs.sh

exec "${MIX_BIN}" conformance "$@"
