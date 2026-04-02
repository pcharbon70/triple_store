#!/usr/bin/env bash

set -euo pipefail

# Compile dependencies first without promoting upstream warnings to errors.
# This keeps CI strict for this repository's code while avoiding GCC warnings
# in third-party rocksdb sources from failing the build before our app compiles.
mix deps.compile
mix compile --warnings-as-errors --no-deps-check
