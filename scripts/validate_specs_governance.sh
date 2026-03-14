#!/usr/bin/env sh
set -eu

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

exec "${MIX_BIN}" conformance --governance-only "$@"
