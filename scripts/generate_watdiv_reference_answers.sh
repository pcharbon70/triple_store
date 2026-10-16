#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dataset=""
dataset_manifest=""
workload_dir=""
output_dir=""

usage() {
  cat <<'EOF'
Usage: ./scripts/generate_watdiv_reference_answers.sh [options]

Generate independent WatDiv accepted answers with Apache Jena ARQ. Each query
is written as W3C SPARQL Results JSON and bound to the exact dataset and workload
fingerprints in a reference manifest.

Required options:
  --dataset FILE             Official WatDiv N-Triples dataset
  --dataset-manifest FILE    Verified dataset provenance manifest
  --workload-dir DIR         One-query-per-file workload directory
  --output-dir DIR           Reference-answer directory

Prerequisite: Apache Jena's `arq` command must be on PATH.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)
      dataset="$2"
      shift 2
      ;;
    --dataset-manifest)
      dataset_manifest="$2"
      shift 2
      ;;
    --workload-dir)
      workload_dir="$2"
      shift 2
      ;;
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for value in "$dataset" "$dataset_manifest" "$workload_dir" "$output_dir"; do
  if [[ -z "$value" ]]; then
    echo "All dataset, manifest, workload, and output options are required." >&2
    usage >&2
    exit 2
  fi
done

if ! command -v arq >/dev/null 2>&1; then
  echo "Apache Jena ARQ is required to generate independent reference answers." >&2
  exit 1
fi

dataset="$(cd "$(dirname "$dataset")" && pwd)/$(basename "$dataset")"
dataset_manifest="$(cd "$(dirname "$dataset_manifest")" && pwd)/$(basename "$dataset_manifest")"
workload_dir="$(cd "$workload_dir" && pwd)"
output_dir="$(mkdir -p "$output_dir" && cd "$output_dir" && pwd)"

while IFS= read -r -d '' query_path; do
  relative_path="${query_path#"$workload_dir"/}"
  answer_path="$output_dir/${relative_path%.*}.srj"
  mkdir -p "$(dirname "$answer_path")"
  arq --data="$dataset" --query="$query_path" --results=JSON > "$answer_path"
done < <(find "$workload_dir" -type f \( -name '*.rq' -o -name '*.sparql' \) -print0 | sort -z)

engine_version="$(arq --version 2>&1 | head -n 1)"

cd "$repo_root"
mix benchmark.watdiv reference-manifest \
  --dataset "$dataset" \
  --dataset-manifest "$dataset_manifest" \
  --workload-dir "$workload_dir" \
  --reference-dir "$output_dir" \
  --engine "Apache Jena ARQ" \
  --engine-version "$engine_version"

echo "WatDiv reference answers written: $output_dir"
