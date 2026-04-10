# Wikidata Benchmark Report

- Report ID: `wikidata-smoke-accepted`
- Report Version: `2`
- Generated At: `2026-04-10T17:58:04.679829Z`
- Run Kind: `matrix`
- Git SHA: `87e3be4b760b`

## Overall Summary

| Group | Queries | Completion | Raw Mean | Adjusted Mean | Raw QPS | Adjusted Iter/s | Accepted | Divergences | Timeouts | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| matrix | 4 | 100.00% | 2.10ms | 2.10ms | 475.14 | 475.14 | 0 | 0 | 0 | 0 |

## Dataset Provenance

| Field | Value |
| --- | --- |
| dataset_id | wikidata-built-in-smoke |
| tier | smoke |
| dump_version | 2024-10-smoke |
| triple_count | 33 |
| checksum | sha256:05b94760b7cf4f9d6bd4d42dea4770812fb7aedd7cae94508093b00d48eceeba |
| source_url | repo://priv/benchmarks/wikidata/fixtures/smoke.nt |

## Runtime Configuration

| Field | Value |
| --- | --- |
| dataset_tier | smoke |
| warmup_iterations | 0 |
| measurement_iterations | 2 |
| timeout_ms | 500 |
| penalty_us | 500000 |
| long_running_threshold_us | 250000 |

## Hardware Metadata

| Field | Value |
| --- | --- |
| hostname | SDMM660 |
| elixir_version | 1.19.5 |
| otp_release | 28 |
| system_architecture | aarch64-apple-darwin24.6.0 |
| logical_processors | 10 |
| logical_processors_available | 10 |
| schedulers | 10 |
| schedulers_online | 10 |
| dirty_cpu_schedulers | 10 |
| dirty_io_schedulers | 10 |

## Suite Summaries

| Group | Queries | Completion | Raw Mean | Adjusted Mean | Raw QPS | Adjusted Iter/s | Accepted | Divergences | Timeouts | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| scholia | 1 | 100.00% | 542.50us | 542.50us | 1843.32 | 1843.32 | 0 | 0 | 0 | 0 |
| wdbench | 1 | 100.00% | 371.50us | 371.50us | 2691.79 | 2691.79 | 0 | 0 | 0 | 0 |
| wdqs | 1 | 100.00% | 2.46ms | 2.46ms | 406.67 | 406.67 | 0 | 0 | 0 | 0 |
| wgpb | 1 | 100.00% | 5.05ms | 5.05ms | 198.20 | 198.20 | 0 | 0 | 0 | 0 |

## Query Shape Aggregates

| Group | Queries | Completion | Raw Mean | Adjusted Mean | Raw QPS | Adjusted Iter/s | Accepted | Divergences | Timeouts | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| path_bgp | 1 | 100.00% | 542.50us | 542.50us | 1843.32 | 1843.32 | 0 | 0 | 0 | 0 |
| single_bgp | 2 | 100.00% | 2.71ms | 2.71ms | 369.21 | 369.21 | 0 | 0 | 0 | 0 |
| star_bgp | 1 | 100.00% | 2.46ms | 2.46ms | 406.67 | 406.67 | 0 | 0 | 0 | 0 |

## Query Summaries

| Benchmark ID | Suite | Variant | Shape | Completion | Raw Median | Adjusted Mean | Correctness | Classification | Raw QPS | Failures |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| wgpb-single-bgp-001 | wgpb | raw | single_bgp | 100.00% | 5.05ms | 5.05ms | match | n/a | 198.20 | 0 |
| wdbench-single-bgp-001 | wdbench | raw | single_bgp | 100.00% | 371.50us | 371.50us | match | n/a | 2691.79 | 0 |
| wdqs-people-occupation-001 | wdqs | raw | star_bgp | 100.00% | 2.46ms | 2.46ms | match | n/a | 406.67 | 0 |
| scholia-author-works-q42-raw | scholia | raw | path_bgp | 100.00% | 542.50us | 542.50us | match | n/a | 1843.32 | 0 |