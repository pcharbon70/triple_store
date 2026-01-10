# Section 2.2: N-Quads Format Support - Implementation Summary

## Overview

Section 2.2 implements full N-Quads format support for loading and exporting quads. This enables the triple store to handle named graphs correctly when working with N-Quads files, which is essential for RDF dataset support.

## Files Modified

### 1. `lib/triple_store/loader.ex`

Added N-Quads loading section with the following functions:

**Public API:**
- `load_nquads_file/4` - Load N-Quads from file path
- `load_nquads_string/4` - Load N-Quads from string

**Private functions:**
- `load_quads/5` - Main quad loading orchestration
- `load_quads_sequential/6` - Sequential quad loading with progress reporting
- `load_quads_parallel/9` - Parallel quad loading using Flow
- `process_quad_batch/4` - Process a batch of RDF quads
- `encode_quad_batch/3` - Encode RDF quads to internal format
- `write_encoded_quad_batch_with_progress/8` - Write encoded batch with progress
- `maybe_report_quad_progress/3` - Report progress and handle halting
- `parse_nquads_file_full/1` - Parse N-Quads file to complete Dataset
- `parse_nquads_string_full/2` - Parse N-Quads string to complete Dataset

### 2. `lib/triple_store/exporter.ex`

Added N-Quads export section with the following functions:

**Public API:**
- `export_nquads_file/3` - Export quads to N-Quads file
- `export_nquads_string/2` - Export quads to N-Quads string

**Private functions:**
- `extract_bound_values/2` - Extract bound values from opts based on pattern

**Types:**
- Added `quad_pattern` type definition

### 3. `test/triple_store/nquads_test.exs` (new file)

Created comprehensive test suite with 18 tests covering:
- File loading with named graphs
- File loading with default graph only
- File loading with mixed graphs
- Empty file handling
- File not found error handling
- String loading with named graphs
- String loading with default graph
- Invalid string handling
- Export to file
- Export with pattern filtering
- Export to string
- Roundtrip (load + export) for files
- Roundtrip for strings
- Progress callback invocation
- Progress callback halting
- Batch processing with large files
- Special characters (quotes, Unicode)

## Key Implementation Details

### Pattern Format

The quad lookup pattern format uses `:bound` or `:var` atoms (not `{:bound, value}` tuples):
- `{:var, :var, :var, :var}` - All quads
- `{:bound, :var, :var, :bound}` - Specific subject and graph
- Values map contains actual IDs for bound positions: `%{s: 1, g: 0}`

### Progress Callback

The progress callback receives a map with:
- `quads_loaded` - Total count loaded so far
- `batch_number` - Current batch number (1-indexed)
- `elapsed_ms` - Time elapsed since start
- `rate_per_second` - Loading rate

Callback returns `:continue` to keep loading or `:halt` to stop.

**Note:** With parallel loading (`parallel: true`, the default), multiple batches may complete before the halt takes effect due to async processing. Use `parallel: false` for deterministic halting behavior.

### Batch Size

The minimum batch size is 100. Smaller values are automatically clamped to this minimum.

## Test Results

```
18 tests, 0 failures
```

All tests pass successfully, covering:
- Loading N-Quads files with named graphs
- Loading N-Quads files with default graph only
- Loading N-Quads files with mixed graphs
- Exporting to N-Quads format
- Roundtrip preservation of data
- String loading and export
- Progress callback functionality
- Batch processing
- Error handling

## Integration Points

- Uses `TripleStore.Adapter.from_rdf_quads/2` for RDF.Quad to internal conversion
- Uses `TripleStore.Adapter.to_rdf_quads/2` for internal to RDF.Quad conversion
- Uses `TripleStore.QuadOperations.insert_quads/3` for quad insertion
- Uses `TripleStore.QuadOperations.lookup_quads/3` for quad lookup
- Uses `RDF.NQuads.read_file/1` and `RDF.NQuads.read_string/1` for parsing
- Uses `RDF.NQuads.write_file/2` and `RDF.NQuads.write_string/2` for serialization

## Breaking Changes

None. All new functions are additions to the existing API. Existing triple-loading functions remain unchanged.

## Future Work

- The stream-based export (`lookup_quads_stream/3`) had timeout issues and was replaced with list-based `lookup_quads/3`. This should be investigated for potential optimization with large datasets.
- Consider adding support for TriG format (Turtle with named graphs) in a future section.
