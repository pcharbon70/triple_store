#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace="$repo_root/tmp/watdiv_official"
scale=1
query_count=1
recurrence=1

usage() {
  cat <<'EOF'
Usage: ./scripts/prepare_watdiv_benchmark.sh [options]

Download, verify, compile, and run the official WatDiv v0.6 generator. The
script creates an official dataset, individually instantiated Basic Testing,
Incremental Linear, and Mixed Linear query files, and a provenance manifest.

Options:
  --workspace DIR              Artifact workspace (default: tmp/watdiv_official)
  --scale N                    Official WatDiv scale factor (default: 1)
  --queries-per-template N     Instances per query template (default: 1)
  --recurrence N               Upstream recurrence factor (default: 1)
  -h, --help                   Show this help

Prerequisites: g++, make, Boost development headers/libraries, wget, tar, and
/usr/share/dict/words. Upstream source is not vendored into this repository.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace)
      workspace="$2"
      shift 2
      ;;
    --scale)
      scale="$2"
      shift 2
      ;;
    --queries-per-template)
      query_count="$2"
      shift 2
      ;;
    --recurrence)
      recurrence="$2"
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

for value in "$scale" "$query_count" "$recurrence"; do
  if [[ ! $value =~ ^[1-9][0-9]*$ ]]; then
    echo "Scale, query count, and recurrence must be positive integers." >&2
    exit 2
  fi
done

for command in wget tar make g++ md5sum sha256sum; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done

boost_home="${BOOST_HOME:-/usr}"

if [[ ! -f "$boost_home/include/boost/random.hpp" ]]; then
  echo "Missing Boost development headers under $boost_home/include." >&2
  echo "Install Boost or set BOOST_HOME to its installation prefix." >&2
  exit 1
fi

if [[ ! -f /usr/share/dict/words ]]; then
  echo "Missing /usr/share/dict/words; install a system word-list package." >&2
  exit 1
fi

workspace="$(mkdir -p "$workspace" && cd "$workspace" && pwd)"
archive="$workspace/watdiv_v06.tar"
source_parent="$workspace/upstream"
source_root="$source_parent/watdiv"
binary="$source_root/bin/Release/watdiv"
model="$source_root/model/wsdbm-data-model.txt"
dataset="$workspace/watdiv-sf${scale}.nt"
manifest="$workspace/dataset-manifest.json"
workload="$workspace/workload"
raw_root="$workspace/raw-workload"

if [[ ! -f "$archive" ]]; then
  wget -O "$archive" https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar
fi

expected_md5="9eac247dfdec044d7fa0141ea3ad361f"
actual_md5="$(md5sum "$archive" | cut -d' ' -f1)"

if [[ "$actual_md5" != "$expected_md5" ]]; then
  echo "WatDiv archive checksum mismatch: expected $expected_md5, got $actual_md5" >&2
  exit 1
fi

if [[ ! -d "$source_root" ]]; then
  mkdir -p "$source_parent"
  tar -xf "$archive" -C "$source_parent"
fi

if [[ ! -x "$binary" ]]; then
  make -C "$source_root" release BOOST_HOME="$boost_home"
fi

rm -rf "$workload" "$raw_root"
mkdir -p "$workload" "$raw_root"

(
  cd "$source_root/bin/Release"
  ./watdiv -d ../../model/wsdbm-data-model.txt "$scale" > "$dataset"
)

cd "$repo_root"
mix benchmark.watdiv dataset-manifest \
  --dataset "$dataset" \
  --model "$model" \
  --scale "$scale" \
  --output "$manifest"

generate_suite() {
  local suite_name="$1"
  local template_dir="$2"
  local raw_dir="$raw_root/$suite_name"
  local output_dir="$workload/$suite_name"

  mkdir -p "$raw_dir" "$output_dir"

  while IFS= read -r -d '' template_path; do
    local template_id
    local raw_path
    template_id="$(basename "$template_path" .txt)"
    raw_path="$raw_dir/${template_id}.txt"

    (
      cd "$source_root/bin/Release"
      ./watdiv -q ../../model/wsdbm-data-model.txt "$template_path" \
        "$query_count" "$recurrence" > "$raw_path"
    )

    mix benchmark.watdiv split \
      --input "$raw_path" \
      --output-dir "$output_dir" \
      --template "$template_id"
  done < <(find "$template_dir" -maxdepth 1 -type f -name '*.txt' -print0 | sort -z)
}

generate_suite basic "$source_root/testsuite"
generate_suite linear_incremental "$source_root/testsuite/linear_incremental"
generate_suite linear_mixed "$source_root/testsuite/linear_mixed"

cat <<EOF

Official WatDiv artifacts prepared.
Dataset: $dataset
Dataset manifest: $manifest
Workload: $workload

Next, generate independent reference answers with:
  ./scripts/generate_watdiv_reference_answers.sh \\
    --dataset "$dataset" \\
    --dataset-manifest "$manifest" \\
    --workload-dir "$workload" \\
    --output-dir "$workspace/reference"
EOF
