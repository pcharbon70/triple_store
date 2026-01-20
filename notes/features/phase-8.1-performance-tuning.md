# Phase 8.1: Performance Tuning for Quads

**Status:** Planning
**Priority:** High
**Owner:** Development Team
**Created:** 2025-01-20

---

## Executive Summary

Phase 8.1 optimizes the quad store specifically for quad operations, addressing the performance differences between triple and quad storage. Quad keys are 32 bytes (4 × 64-bit IDs) compared to 24-byte triple keys, and quad operations write to 4 indices instead of 3, resulting in different performance characteristics.

This phase focuses on three key areas:
1. **RocksDB Configuration** - Tune block sizes, bloom filters, and compaction for 32-byte keys
2. **Write Optimization** - Optimize WriteBatch operations and dictionary lookups for 4-index writes
3. **Read Optimization** - Optimize prefix scans and implement cache warming for graph-scoped queries

**Target Performance:**
- Insert throughput: >50k quads/sec (sync: false)
- Graph-scoped queries: <10ms for typical patterns
- Prefix scan throughput: >100K quads/sec

---

## Problem Statement

### Current State

The quad store implementation in Phase 7 uses the same RocksDB configuration as the triple store, which is suboptimal for quad operations:

1. **Larger Key Size**: Quad keys are 32 bytes (4 × 64-bit IDs) vs 24 bytes for triples
   - Increases memory footprint by 33%
   - Reduces number of keys per block
   - Affects block cache efficiency

2. **Higher Write Amplification**: Quad operations write to 4 indices (GSPO, GPOS, SPOG, POSG) vs 3 for triples
   - 4x write amplification per insert
   - More WriteBatch overhead
   - Higher compaction pressure

3. **Access Pattern Differences**: Quad queries have different access patterns
   - Graph-scoped queries (GSPO/GPOS) are more common than cross-graph queries
   - Graph enumeration requires full GSPO scans
   - Default graph (ID=0) is frequently accessed

### Performance Gap

Current configuration is optimized for 24-byte triple keys:
- Block size: 8KB for index CFs (holds ~341 triple keys, ~256 quad keys)
- Bloom filter: 12 bits/key (tuned for triple key distribution)
- Memtable: 64MB (sufficient for 3x write amplification)

**Impact:**
- Reduced block cache efficiency for quad indices
- Higher bloom filter memory overhead per key
- Potential memtable flush pressure during bulk loads

---

## Solution Overview

### Design Principles

1. **Proportional Tuning**: Scale RocksDB parameters proportionally to key size increase
   - Block size: 8KB → 16KB (maintain keys-per-block ratio)
   - Bloom filter bits: 12 → 10 (reduce memory overhead while maintaining FPR)

2. **Access Pattern Optimization**: Optimize for common quad query patterns
   - GSPO prefix scans (graph-scoped queries)
   - SPOG prefix scans (subject-scoped cross-graph queries)
   - Graph enumeration (full GSPO scans)

3. **Write Amplification Mitigation**: Reduce overhead of 4-index writes
   - Increase WriteBatch size to batch more operations per flush
   - Implement batch grouping by graph to improve locality
   - Optimize dictionary lookups for graph terms

4. **Cache Optimization**: Improve cache hit rates for frequently accessed graphs
   - Cache warming for hot graphs
   - Read-ahead for sequential graph scans
   - Graph-aware cache priorities

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   Quad Operations Layer                      │
│  (QuadOperations, GraphManager, Statistics)                  │
├──────────────────────────────────────────────────────────────┤
│              Performance Optimization Layer                   │
│  • Batch Grouping by Graph                                   │
│  • Cache Warming for Hot Graphs                              │
│  • Read-Ahead for Sequential Scans                           │
├──────────────────────────────────────────────────────────────┤
│             RocksDB Configuration (Quad-Tuned)               │
│  • 16KB Blocks for Quad Indices (vs 8KB triples)             │
│  • 10 bits/key Bloom Filter (vs 12 bits/key triples)         │
│  • Optimized Memtable Size for 4x Write Amplification        │
│  • Prefix Extractors for 4-Part Keys                         │
├──────────────────────────────────────────────────────────────┤
│                   erlang-rocksdb NIF                         │
│              (Column Families: GSPO, GPOS, SPOG, POSG)       │
└──────────────────────────────────────────────────────────────┘
```

---

## Technical Details

### File Locations

**Current Implementation:**
- `/lib/triple_store/backend/rocksdb/column_family_config.ex` - CF configuration
- `/lib/triple_store/quad_operations.ex` - Quad CRUD operations
- `/lib/triple_store/quad_index.ex` - Quad key encoding/decoding
- `/lib/triple_store/statistics.ex` - Statistics and caching

**New Files to Create:**
- `/lib/triple_store/quad/batch_optimizer.ex` - Batch grouping optimization
- `/lib/triple_store/quad/cache_warmer.ex` - Graph-specific cache warming
- `/test/triple_store/benchmark/phase_8_1_quad_performance_test.exs` - Performance benchmarks

**Files to Modify:**
- `/lib/triple_store/backend/rocksdb/column_family_config.ex` - Update quad CF options
- `/lib/triple_store/quad_operations.ex` - Add batch grouping support
- `/lib/triple_store/statistics.ex` - Add graph-specific cache warming

---

### Current Configuration

From `/lib/triple_store/backend/rocksdb/column_family_config.ex`:

```elixir
# Quad Index CF Options (lines 481-509)
defp quad_index_cf_options do
  base_options()
  |> Keyword.merge(index_compaction_options())
  |> Keyword.merge(
    block_based_table_options: [
      bloom_filter_policy: @bloom_index_bits,  # 12 bits/key
      block_size: @block_size_index,            # 8KB (8192 bytes)
      cache_index_and_filter_blocks: true,
      pin_l0_filter_and_index_blocks_in_cache: false,
      whole_key_filtering: false
    ],
    memtable_prefix_bloom_size_ratio: 0.1,
    compression: @compression_l1_l6,
    bottommost_compression: @compression_l1_l6
  )
end

# Constants (lines 100-114)
@bloom_index_bits 12
@block_size_index 8 * 1024  # 8KB

# Compaction (lines 351-386)
defp index_compaction_options do
  [
    write_buffer_size: 64 * 1024 * 1024,  # 64MB memtable
    max_write_buffer_number: 3,
    min_write_buffer_number_to_merge: 1,
    # ... other options
  ]
end
```

**Current WriteBatch Pattern:**
From `/lib/triple_store/quad_operations.ex` (lines 165-181):

```elixir
def insert_quads(db, quads, opts) when is_list(quads) do
  sync = Keyword.get(opts, :sync, true)

  operations =
    for {subject, predicate, object, graph} <- quads,
        op <- build_insert_operations(subject, predicate, object, graph) do
      op
    end

  NIF.write_batch(db, operations, sync)
end

# Each quad generates 4 operations (one per index)
defp build_insert_operations(s, p, o, g) do
  keys = QuadIndex.encode_quad_keys(s, p, o, g)
  [
    {:gspo, Map.get(keys, :gspo), @empty_value},
    {:gpos, Map.get(keys, :gpos), @empty_value},
    {:spog, Map.get(keys, :spog), @empty_value},
    {:posg, Map.get(keys, :posg), @empty_value}
  ]
end
```

---

## Proposed Changes

### 8.1.1 RocksDB Configuration for Quads

#### 8.1.1.1 Update Block Sizes for 32-byte Keys

**Current:** 8KB block size (optimized for 24-byte triple keys)
**Proposed:** 16KB block size for quad indices

**Rationale:**
- Triple keys: 24 bytes → ~341 keys per 8KB block
- Quad keys: 32 bytes → ~256 keys per 8KB block (25% reduction)
- 16KB blocks → ~512 quad keys per block (better locality)
- Larger blocks reduce block cache miss rate for sequential scans

**Implementation:**

```elixir
# Add new constant for quad index block size
@block_size_quad_index 16 * 1024  # 16KB

# Update quad_index_cf_options/0
defp quad_index_cf_options do
  base_options()
  |> Keyword.merge(index_compaction_options())
  |> Keyword.merge(
    block_based_table_options: [
      bloom_filter_policy: @bloom_quad_index_bits,  # 10 bits/key (new)
      block_size: @block_size_quad_index,            # 16KB (updated)
      cache_index_and_filter_blocks: true,
      pin_l0_filter_and_index_blocks_in_cache: false,
      whole_key_filtering: false
    ],
    memtable_prefix_bloom_size_ratio: 0.1,
    compression: @compression_l1_l6,
    bottommost_compression: @compression_l1_l6
  )
end
```

**Expected Impact:**
- 50% increase in keys per block (256 → 512)
- Reduced block cache miss rate for graph-scoped queries
- Better compression efficiency (larger blocks compress better)

---

#### 8.1.1.2 Adjust Bloom Filter Bits for Quad Indices

**Current:** 12 bits/key for all index CFs
**Proposed:** 10 bits/key for quad indices

**Rationale:**
- Bloom filter memory per key: `bits_per_key × key_size × ln(2)`
- Triple keys (24 bytes): 12 bits × 24 × 0.693 = ~200 bytes/key overhead
- Quad keys (32 bytes): 12 bits × 32 × 0.693 = ~266 bytes/key overhead (33% increase)
- Reducing to 10 bits/key: ~222 bytes/key (17% savings) with minimal FPR increase
- False positive rate at 10 bits/key: ~0.98% (vs 0.09% at 12 bits)
- Acceptable trade-off: Prefix scans have additional filtering via post-filter

**Implementation:**

```elixir
# Add new constant for quad index bloom filter
@bloom_quad_index_bits 10

# Update quad_index_cf_options/0 (as shown above)
```

**Expected Impact:**
- 17% reduction in bloom filter memory overhead
- Minimal impact on query performance (post-filtering handles false positives)
- More memory available for block cache

---

#### 8.1.1.3 Configure Prefix Extractors for Quad Indices

**Current:** Prefix extractors are commented out due to version uncertainty
**Proposed:** Enable and test prefix extractors for quad indices

**Rationale:**
- Quad indices use 4-part keys: {graph, subject, predicate, object}
- Prefix extractors enable RocksDB's native prefix seek optimization
- Reduces iterator overhead for graph-scoped queries
- Different prefix lengths per index:
  - GSPO: 8 bytes (graph ID) for graph enumeration
  - GPOS: 8 bytes (graph ID) for graph-scoped predicate queries
  - SPOG: 8 bytes (subject ID) for subject-scoped cross-graph queries
  - POSG: 8 bytes (predicate ID) for predicate-scoped cross-graph queries

**Implementation:**

```elixir
# Update quad_index_cf_options/0
defp quad_index_cf_options do
  base_options()
  |> Keyword.merge(index_compaction_options())
  |> Keyword.merge(
    block_based_table_options: [
      bloom_filter_policy: @bloom_quad_index_bits,
      block_size: @block_size_quad_index,
      cache_index_and_filter_blocks: true,
      pin_l0_filter_and_index_blocks_in_cache: false,
      whole_key_filtering: false
    ],
    # Enable prefix extractor for quad indices
    # Note: erlang-rocksdb format may vary - test during implementation
    prefix_extractor: {:fixed, 8},  # Extract first 64-bit ID as prefix
    memtable_prefix_bloom_size_ratio: 0.1,
    compression: @compression_l1_l6,
    bottommost_compression: @compression_l1_l6
  )
end
```

**Caveats:**
- erlang-rocksdb prefix extractor format is version-dependent
- May need to test multiple formats: `{:fixed, 8}`, `{"fixed.prefix", 8}`, or `{"rocksdb.FixedPrefix.8"}`
- Fallback: Keep manual prefix checking in iterator (current implementation)

---

#### 8.1.1.4 Optimize Memtable Size for 4x Write Amplification

**Current:** 64MB memtable for all index CFs
**Proposed:** Increase to 128MB for quad indices

**Rationale:**
- Triple store: 3 indices × 64MB = 192MB total memtable across indices
- Quad store: 4 indices × 64MB = 256MB total memtable (33% increase)
- However, each quad insert writes to 4 indices (vs 3 for triples)
- Larger memtable reduces flush frequency during bulk loads
- 128MB × 4 indices = 512MB total (acceptable on modern hardware)

**Implementation:**

```elixir
# Add quad-specific compaction options
defp quad_index_compaction_options do
  [
    compaction_style: :level,
    write_buffer_size: 128 * 1024 * 1024,  # 128MB (increased from 64MB)
    max_write_buffer_number: 3,
    min_write_buffer_number_to_merge: 1,
    target_file_size_base: 64 * 1024 * 1024,
    target_file_size_multiplier: 1,
    level0_file_num_compaction_trigger: 4,
    level0_slowdown_writes_trigger: 8,
    level0_stop_writes_trigger: 12,
    max_bytes_for_level_base: 256 * 1024 * 1024,
    max_bytes_for_level_multiplier: 10,
    num_levels: 7,
    compression: @compression_l1_l6,
    bottommost_compression: @compression_l1_l6
  ]
end

# Update quad_index_cf_options/0 to use quad-specific compaction
defp quad_index_cf_options do
  base_options()
  |> Keyword.merge(quad_index_compaction_options())  # Use quad-specific options
  |> Keyword.merge(
    # ... block_based_table_options as above
  )
end
```

**Expected Impact:**
- 50% reduction in memtable flush frequency during bulk loads
- Reduced write amplification from compaction
- Better throughput for large batch inserts

---

#### 8.1.1.5 Document Quad-Specific Tuning Rationale

**Create:** `/lib/triple_store/backend/rocksdb/column_family_config.ex`

Add comprehensive documentation explaining quad-specific tuning:

```elixir
@moduledoc """
## Quad Store Column Families (Schema v2)

The quad store uses four quad indices for named graph support:

### Performance Tuning for Quads

Quad keys are 32 bytes (4 × 64-bit IDs) vs 24 bytes for triple keys (3 × 64-bit IDs):
- **Key size increase**: 33% larger (32 bytes vs 24 bytes)
- **Write amplification**: 4x writes vs 3x for triples
- **Block cache efficiency**: Fewer keys per block at default block size

### Tuning Strategy

1. **Block Size**: 16KB for quad indices (vs 8KB for triples)
   - Maintains keys-per-block ratio despite larger keys
   - Reduces block cache miss rate for sequential scans
   - Improves compression efficiency

2. **Bloom Filter**: 10 bits/key for quad indices (vs 12 bits/key for triples)
   - Reduces memory overhead for larger keys
   - Acceptable FPR increase (0.98% vs 0.09%)
   - Post-filtering handles false positives efficiently

3. **Memtable Size**: 128MB for quad indices (vs 64MB for triple indices)
   - Compensates for 4x write amplification
   - Reduces flush frequency during bulk loads
   - Better throughput for large batches

4. **Prefix Extractor**: Fixed 8-byte prefix for quad indices
   - Enables native RocksDB prefix seek optimization
   - Reduces iterator overhead for graph-scoped queries
   - Different prefix lengths per index (graph, subject, predicate)

### Performance Targets

- **Insert throughput**: >50k quads/sec (sync: false)
- **Graph-scoped queries**: <10ms for typical patterns
- **Prefix scan throughput**: >100K quads/sec
"""
```

---

### 8.1.2 Write Optimization

#### 8.1.2.1 Increase WriteBatch Size for 4-Index Writes

**Current:** No explicit WriteBatch size limit
**Proposed:** Implement adaptive WriteBatch sizing with target of 10K-50K operations

**Rationale:**
- Each quad insert generates 4 operations (one per index)
- Default RocksDB WriteBatch has overhead per operation
- Larger batches amortize WriteBatch overhead
- Optimal batch size: 10K-50K operations (balance memory vs throughput)

**Implementation:**

Create `/lib/triple_store/quad/batch_optimizer.ex`:

```elixir
defmodule TripleStore.Quad.BatchOptimizer do
  @moduledoc """
  Optimizes WriteBatch operations for quad store.

  Quad operations write to 4 indices (GSPO, GPOS, SPOG, POSG), so
  each quad generates 4 WriteBatch operations. This module optimizes
  batching to improve throughput.
  """

  @max_batch_size 50_000  # Maximum operations per batch
  @target_batch_size 20_000  # Target operations per batch
  @min_batch_size 1_000  # Minimum operations per batch

  @doc """
  Groups quads into optimal batch sizes for WriteBatch operations.

  Each quad generates 4 operations (one per index), so we divide the
  target batch size by 4 to determine the number of quads per batch.

  ## Parameters

  - `quads` - List of quads to batch
  - `opts` - Keyword list of options:
    - `:target_size` - Target operations per batch (default: 20,000)
    - `:max_size` - Maximum operations per batch (default: 50,000)

  ## Returns

  List of batches, where each batch is a list of quads

  ## Examples

      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}]
      iex> BatchOptimizer.group_quads_for_batch(quads)
      [[{1, 2, 3, 0}, {4, 5, 6, 0}]]

  """
  @spec group_quads_for_batch([TripleStore.QuadOperations.quad()], keyword()) ::
          [[TripleStore.QuadOperations.quad()]]
  def group_quads_for_batch(quads, opts \\ []) do
    target_size = Keyword.get(opts, :target_size, @target_batch_size)
    max_size = Keyword.get(opts, :max_size, @max_batch_size)

    # Each quad generates 4 operations
    quads_per_batch = div(target_size, 4)
    max_quads_per_batch = div(max_size, 4)

    # Group quads into chunks
    quads
    |> Enum.chunk_every(quads_per_batch)
    |> Enum.map(fn chunk ->
      # Ensure no chunk exceeds max size
      if length(chunk) > max_quads_per_batch do
        Enum.chunk_every(chunk, max_quads_per_batch)
      else
        [chunk]
      end
    end)
    |> List.flatten()
  end

  @doc """
  Estimates the number of WriteBatch operations for a list of quads.

  Each quad generates 4 operations (GSPO, GPOS, SPOG, POSG).

  ## Examples

      iex> BatchOptimizer.estimate_operation_count([{1, 2, 3, 0}])
      4

  """
  @spec estimate_operation_count([TripleStore.QuadOperations.quad()]) :: non_neg_integer()
  def estimate_operation_count(quads) when is_list(quads) do
    length(quads) * 4
  end
end
```

**Update `/lib/triple_store/quad_operations.ex`:**

```elixir
def insert_quads(db, quads, opts) when is_list(quads) do
  sync = Keyword.get(opts, :sync, true)
  use_batching = Keyword.get(opts, :use_batching, true)

  if use_batching and length(quads) > 100 do
    # Use batch optimizer for large inserts
    batches = TripleStore.Quad.BatchOptimizer.group_quads_for_batch(quads)

    # Process each batch sequentially
    Enum.each(batches, fn batch ->
      operations =
        for {subject, predicate, object, graph} <- batch,
            op <- build_insert_operations(subject, predicate, object, graph) do
          op
        end

      NIF.write_batch(db, operations, sync)
    end)

    :ok
  else
    # Original implementation for small batches
    Telemetry.span(:quad, :insert, %{sync: sync}, fn ->
      operations =
        for {subject, predicate, object, graph} <- quads,
            op <- build_insert_operations(subject, predicate, object, graph) do
          op
        end

      result = NIF.write_batch(db, operations, sync)
      {result, %{count: length(quads)}}
    end)
  end
end
```

**Expected Impact:**
- 30-50% improvement in bulk insert throughput
- Reduced memory pressure from intermediate lists
- Better write amplification during large bulk loads

---

#### 8.1.2.2 Implement Batch Grouping by Graph

**Current:** Quads processed in insertion order
**Proposed:** Group quads by graph ID before batch insertion

**Rationale:**
- Graph-local inserts improve memtable locality
- Reduces cross-graph index page churn
- Better compaction efficiency (similar keys grouped together)
- Particularly effective for bulk loads with multiple graphs

**Implementation:**

Update `/lib/triple_store/quad/batch_optimizer.ex`:

```elixir
@doc """
  Groups quads by graph ID for better locality during bulk inserts.

  Graph-local grouping improves memtable locality and compaction efficiency.

  ## Parameters

  - `quads` - List of quads to group
  - `opts` - Keyword list of options:
    - `:batch_size` - Target quads per batch (default: 5,000)

  ## Returns

  Map where keys are graph IDs and values are lists of quads

  ## Examples

      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 1}, {7, 8, 9, 0}]
      iex> BatchOptimizer.group_quads_by_graph(quads)
      %{0 => [{1, 2, 3, 0}, {7, 8, 9, 0}], 1 => [{4, 5, 6, 1}]}

  """
  @spec group_quads_by_graph([TripleStore.QuadOperations.quad()], keyword()) ::
          %{non_neg_integer() => [TripleStore.QuadOperations.quad()]}
  def group_quads_by_graph(quads, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 5_000)

    quads
    |> Enum.group_by(fn {_s, _p, _o, g} -> g end)
    |> Enum.map(fn {graph_id, graph_quads} ->
      # Split large graphs into chunks
      chunks = Enum.chunk_every(graph_quads, batch_size)
      {graph_id, chunks}
    end)
    |> Map.new()
  end
```

**Update `/lib/triple_store/quad_operations.ex`:**

```elixir
def insert_quads(db, quads, opts) when is_list(quads) do
  sync = Keyword.get(opts, :sync, true)
  group_by_graph = Keyword.get(opts, :group_by_graph, true)

  if group_by_graph and length(quads) > 1000 do
    # Group by graph for better locality
    graph_groups = TripleStore.Quad.BatchOptimizer.group_quads_by_graph(quads)

    # Insert each graph group separately
    Enum.each(graph_groups, fn {_graph_id, graph_quads} ->
      # graph_quads is a list of chunks for large graphs
      Enum.each(graph_quads, fn chunk ->
        operations =
          for {subject, predicate, object, graph} <- chunk,
              op <- build_insert_operations(subject, predicate, object, graph) do
            op
          end

        NIF.write_batch(db, operations, sync)
      end)
    end)

    :ok
  else
    # Original implementation
    # ...
  end
end
```

**Expected Impact:**
- 10-20% improvement in bulk insert throughput for multi-graph datasets
- Reduced compaction overhead
- Better prefix scan performance for graph-scoped queries

---

#### 8.1.2.3 Optimize Dictionary Lookups for Graph Terms

**Current:** Graph terms looked up via `term_to_id` for each quad
**Proposed:** Batch graph term lookups and cache graph IDs

**Rationale:**
- Graph terms are frequently repeated across quads
- Current implementation does dictionary lookup per operation
- Caching graph IDs reduces dictionary pressure
- Particularly effective for named graph workloads

**Implementation:**

Update `/lib/triple_store/quad_operations.ex`:

```elixir
# Add new function for optimized bulk insert with graph term caching
@doc """
  Inserts multiple quads with graph term caching for better performance.

  This function is optimized for bulk inserts where the same graph terms
  appear many times. It looks up each unique graph term once and caches
  the ID, reducing dictionary pressure.

  ## Parameters

  - `db` - RocksDB database reference
  - `manager` - Dictionary manager process
  - `quad_terms` - List of `{subject, predicate, object, graph_term}` tuples
    where `graph_term` is an `RDF.IRI` or `RDF.BlankNode` (not an ID)
  - `opts` - Keyword list of options

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      # Insert quads with graph terms (automatic ID lookup with caching)
      quad_terms = [
        {1, 2, 3, RDF.iri("http://example.org/g1")},
        {4, 5, 6, RDF.iri("http://example.org/g1")},
        {7, 8, 9, RDF.iri("http://example.org/g2")}
      ]
      QuadOperations.insert_quads_with_terms(db, manager, quad_terms)

  """
  @spec insert_quads_with_terms(
          NIF.db_ref(),
          TripleStore.Dictionary.Manager.manager(),
          [{term_id(), term_id(), term_id(), RDF.IRI.t() | RDF.BlankNode.t()}],
          keyword()
        ) :: :ok | {:error, term()}
  def insert_quads_with_terms(db, manager, quad_terms, opts \\ []) do
    sync = Keyword.get(opts, :sync, true)

    Telemetry.span(:quad, :insert_with_terms, %{count: length(quad_terms)}, fn ->
      # Extract unique graph terms
      unique_graph_terms =
        quad_terms
        |> Enum.map(fn {_s, _p, _o, g} -> g end)
        |> Enum.uniq()

      # Lookup each unique graph term once
      graph_id_map =
        Enum.reduce(unique_graph_terms, %{}, fn graph_term, acc ->
          case TripleStore.Adapter.term_to_id(manager, graph_term) do
            {:ok, graph_id} -> Map.put(acc, graph_term, graph_id)
            {:error, _reason} -> acc
          end
        end)

      # Convert quad_terms to quads using cached graph IDs
      quads =
        Enum.map(quad_terms, fn {s, p, o, graph_term} ->
          graph_id = Map.get(graph_id_map, graph_term, 0)
          {s, p, o, graph_id}
        end)

      # Use optimized insert_quads
      result = insert_quads(db, quads, sync: sync)
      {result, %{count: length(quads)}}
    end)
  end
```

**Expected Impact:**
- 20-30% reduction in dictionary lookup overhead for named graph workloads
- Particularly effective for datasets with few graphs and many quads per graph

---

#### 8.1.2.4 Use Write-Ahead Logging for Durability

**Current:** WAL enabled by default in RocksDB
**Proposed:** Document WAL usage and ensure proper configuration

**Rationale:**
- WAL provides durability without requiring sync on every write
- Current implementation uses `sync: true` by default (synchronous fsync)
- For bulk loads, `sync: false` with periodic WAL flush is more efficient
- WAL provides crash recovery while maintaining high throughput

**Implementation:**

Add documentation to `/lib/triple_store/quad_operations.ex`:

```elixir
@moduledoc """
  ## Durability and Write-Ahead Logging

  All quad insert operations use RocksDB's Write-Ahead Log (WAL) for durability.

  ### Sync Modes

  - **sync: true** (default for single operations): Synchronously flushes WAL to disk
    on every write. Provides immediate durability at the cost of performance.
    Use for critical operations where durability is more important than throughput.

  - **sync: false** (recommended for bulk loads): Writes are buffered in the OS page
    cache and flushed periodically by RocksDB. The WAL still provides durability,
    but there's a small window of data loss on crash (typically <1 second).
    Use for bulk loading where throughput is critical.

  ### WAL Flush Strategy

  For bulk loads with `sync: false`, periodically flush the WAL to bound the
  potential data loss window:

      # Insert 100K quads with sync disabled
      QuadOperations.insert_quads(db, quads, sync: false)

      # Flush WAL to ensure durability
      NIF.flush_wal(db, true)

  ### Recommendation

  - **Single operations**: Use `sync: true` (default)
  - **Bulk loads**: Use `sync: false` and flush WAL periodically
  - **Batch operations**: Use `sync: true` for critical batches, `sync: false` for non-critical
"""
```

**No code changes required** - just documentation and best practices.

---

#### 8.1.2.5 Benchmark Insert Throughput (Target: >50k Quads/Sec)

**Create:** `/test/triple_store/benchmark/phase_8_1_quad_performance_test.exs`

```elixir
defmodule TripleStore.Benchmark.Phase81QuadPerformanceTest do
  @moduledoc """
  Performance benchmarks for Phase 8.1: Quad Performance Tuning.

  Run with: mix test test/triple_store/benchmark/phase_8_1_quad_performance_test.exs
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_quad_perf_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = NIF.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager}
  end

  describe "insert throughput benchmarks" do
    test "inserts 50K quads/sec with sync: false", %{db: db} do
      # Generate 100K quads
      quads =
        for i <- 1..100_000 do
          subject = rem(i, 10_000) + 1
          predicate = rem(i, 100) + 10
          object = i + 100_000
          graph = rem(i, 10)
          {subject, predicate, object, graph}
        end

      # Measure insert throughput
      {time_us, :ok} = :timer.tc(fn -> QuadOperations.insert_quads(db, quads, sync: false) end)

      throughput = Float.round(100_000_000_000.0 / time_us, 2)

      IO.puts("\n  [Benchmark] Inserted 100K quads in #{div(time_us, 1000)}ms")
      IO.puts("  [Benchmark] Throughput: #{throughput}K quads/sec")

      # Target: >50K quads/sec
      assert throughput > 50.0,
             "Insert throughput #{throughput}K quads/sec is below target 50K quads/sec"
    end

    test "inserts 10K quads/sec with sync: true", %{db: db} do
      # Generate 10K quads
      quads =
        for i <- 1..10_000 do
          subject = rem(i, 1_000) + 1
          predicate = rem(i, 50) + 10
          object = i + 10_000
          graph = rem(i, 5)
          {subject, predicate, object, graph}
        end

      # Measure insert throughput
      {time_us, :ok} = :timer.tc(fn -> QuadOperations.insert_quads(db, quads, sync: true) end)

      throughput = Float.round(10_000_000_000.0 / time_us, 2)

      IO.puts("\n  [Benchmark] Inserted 10K quads (sync: true) in #{div(time_us, 1000)}ms")
      IO.puts("  [Benchmark] Throughput: #{throughput}K quads/sec")

      # Target: >10K quads/sec for sync writes
      assert throughput > 10.0,
             "Insert throughput #{throughput}K quads/sec is below target 10K quads/sec"
    end

    test "graph grouping improves throughput", %{db: db} do
      # Generate quads across multiple graphs
      quads =
        for i <- 1..50_000 do
          subject = rem(i, 5_000) + 1
          predicate = rem(i, 50) + 10
          object = i + 50_000
          graph = rem(i, 20)  # 20 different graphs
          {subject, predicate, object, graph}
        end

      # Measure without graph grouping
      {time_no_group, :ok} =
        :timer.tc(fn -> QuadOperations.insert_quads(db, quads, sync: false, group_by_graph: false) end)

      # Clear database
      NIF.close(db)
      File.rm_rf(test_path)
      {:ok, db} = NIF.open(test_path, schema: :quad)

      # Measure with graph grouping
      {time_with_group, :ok} =
        :timer.tc(fn -> QuadOperations.insert_quads(db, quads, sync: false, group_by_graph: true) end)

      improvement = Float.round((time_no_group - time_with_group) / time_no_group * 100, 2)

      IO.puts("\n  [Benchmark] Without graph grouping: #{div(time_no_group, 1000)}ms")
      IO.puts("  [Benchmark] With graph grouping: #{div(time_with_group, 1000)}ms")
      IO.puts("  [Benchmark] Improvement: #{improvement}%")

      # Graph grouping should provide at least 10% improvement
      assert time_with_group < time_no_group * 0.9,
             "Graph grouping did not improve throughput as expected"
    end
  end
end
```

---

### 8.1.3 Read Optimization

#### 8.1.3.1 Optimize Prefix Scan for GSPO Index

**Current:** Generic prefix scan implementation
**Proposed:** Optimize specifically for GSPO (graph-scoped) queries

**Rationale:**
- GSPO is the most commonly accessed index (graph-scoped queries)
- Graph enumeration scans all quads in a graph
- Default graph (ID=0) is frequently accessed
- Optimization should focus on:
  - Read-ahead for sequential scans
  - Efficient bounds checking
  - Reduced iterator overhead

**Implementation:**

Update `/lib/triple_store/quad_operations.ex`:

```elixir
@doc """
  Optimized prefix scan for GSPO (graph-scoped) queries.

  This function is optimized for graph-scoped queries where most or all
  quads in a graph are accessed. It uses read-ahead and efficient bounds
  checking to improve performance.

  ## Parameters

  - `db` - RocksDB database reference
  - `graph_id` - Graph ID to scan
  - `opts` - Keyword list of options:
    - `:readahead_size` - Read-ahead size in bytes (default: 1MB)

  ## Returns

  - List of `{subject, predicate, object, graph}` tuples in the graph

  ## Examples

      # Get all quads in default graph
      quads = QuadOperations.scan_graph(db, 0)

      # Get all quads in specific graph
      quads = QuadOperations.scan_graph(db, graph_id)

  """
  @spec scan_graph(NIF.db_ref(), non_neg_integer(), keyword()) :: [TripleStore.QuadOperations.quad()]
  def scan_graph(db, graph_id, opts \\ []) do
    prefix = TripleStore.QuadIndex.gspo_prefix(graph_id)

    try do
      NIF.fold_keys(db, :gspo, prefix, [], fn key, acc ->
        # Extract graph ID from key (first 8 bytes)
        <<g::unsigned-big-integer-size(64), _rest::binary>> = key

        # Check if we're still within the graph
        if g == graph_id do
          # Decode key and add to accumulator
          {g, s, p, o} = TripleStore.QuadIndex.decode_gspo_key(key)
          [{s, p, o, g} | acc]
        else
          # Moved beyond the graph, stop iteration
          throw({:halt, acc})
        end
      end)
      |> Enum.reverse()
    catch
      {:halt, acc} -> Enum.reverse(acc)
    end
  end

@doc """
  Optimized prefix scan for GSPO with streaming.

  Returns a stream of quads for memory-efficient processing of large graphs.

  ## Parameters

  - `db` - RocksDB database reference
  - `graph_id` - Graph ID to scan

  ## Returns

  - Stream of `{subject, predicate, object, graph}` tuples

  ## Examples

      # Stream quads and process incrementally
      QuadOperations.scan_graph_stream(db, 0)
      |> Stream.each(fn {s, p, o, g} -> process_quad(s, p, o, g) end)
      |> Stream.run()

  """
  @spec scan_graph_stream(NIF.db_ref(), non_neg_integer()) :: Enumerable.t()
  def scan_graph_stream(db, graph_id) do
    prefix = TripleStore.QuadIndex.gspo_prefix(graph_id)

    Stream.resource(
      fn ->
        # Create iterator (this would need to be added to NIF module)
        {:ok, iter} = NIF.iterator(db, :gspo)
        NIF.iterator_seek(iter, prefix)
        iter
      end,
      fn iter ->
        case NIF.iterator_move(iter, :next) do
          {:ok, key, _value} ->
            <<g::unsigned-big-integer-size(64), _rest::binary>> = key

            if g == graph_id do
              {g, s, p, o} = TripleStore.QuadIndex.decode_gspo_key(key)
              {[{s, p, o, g}], iter}
            else
              {:halt, iter}
            end

          :done ->
            {:halt, iter}
        end
      end,
      fn iter ->
        NIF.iterator_close(iter)
      end
    )
  end
```

**Expected Impact:**
- 20-30% improvement in graph-scoped query performance
- Reduced memory usage for large graph scans
- Better cache locality for sequential reads

---

#### 8.1.3.2 Optimize Prefix Scan for SPOG Index

**Current:** Generic prefix scan implementation
**Proposed:** Optimize for SPOG (subject-scoped cross-graph) queries

**Rationale:**
- SPOG index is used for subject-scoped queries across graphs
- Common pattern: "Find all triples about subject X across all graphs"
- Optimization should focus on:
  - Efficient subject ID prefix extraction
  - Fast graph filtering
  - Cross-graph aggregation

**Implementation:**

```elixir
@doc """
  Optimized prefix scan for SPOG (subject-scoped) queries.

  This function finds all quads with a given subject across all graphs.

  ## Parameters

  - `db` - RocksDB database reference
  - `subject_id` - Subject ID to lookup

  ## Returns

  - List of `{subject, predicate, object, graph}` tuples

  ## Examples

      # Find all quads about subject 1 across all graphs
      quads = QuadOperations.scan_subject(db, 1)

  """
  @spec scan_subject(NIF.db_ref(), non_neg_integer()) :: [TripleStore.QuadOperations.quad()]
  def scan_subject(db, subject_id) do
    # SPOG key: subject (8 bytes) | predicate (8 bytes) | object (8 bytes) | graph (8 bytes)
    # Prefix: subject (8 bytes)
    prefix = <<subject_id::unsigned-big-integer-size(64)>>

    try do
      NIF.fold_keys(db, :spog, prefix, [], fn key, acc ->
        <<s::unsigned-big-integer-size(64), _rest::binary>> = key

        if s == subject_id do
          {s, p, o, g} = TripleStore.QuadIndex.decode_spog_key(key)
          [{s, p, o, g} | acc]
        else
          throw({:halt, acc})
        end
      end)
      |> Enum.reverse()
    catch
      {:halt, acc} -> Enum.reverse(acc)
    end
  end
```

---

#### 8.1.3.3 Implement Cache Warming for Frequently Accessed Graphs

**Current:** Statistics module has some caching, but not graph-specific
**Proposed:** Implement proactive cache warming for frequently accessed graphs

**Rationale:**
- Default graph and active named graphs are accessed frequently
- Cache warming reduces latency for graph-scoped queries
- Particularly effective for workloads with temporal locality

**Create:** `/lib/triple_store/quad/cache_warmer.ex`

```elixir
defmodule TripleStore.Quad.CacheWarmer do
  @moduledoc """
  Cache warming for frequently accessed graphs.

  This module proactively loads graph data into the RocksDB block cache
  to improve query latency for frequently accessed graphs.
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadOperations

  @doc """
  Warms the block cache for a specific graph by scanning all its quads.

  This function performs a full scan of the graph's quads, loading them
  into the RocksDB block cache. Subsequent queries for this graph will
  be faster as the data is already cached.

  ## Parameters

  - `db` - RocksDB database reference
  - `graph_id` - Graph ID to warm

  ## Returns

  - `{:ok, count}` - Number of quads loaded into cache
  - `{:error, reason}` - On error

  ## Examples

      {:ok, count} = CacheWarmer.warm_graph_cache(db, 0)

  """
  @spec warm_graph_cache(NIF.db_ref(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def warm_graph_cache(db, graph_id) do
    # Perform a full scan of the graph
    # This loads the graph's data into the block cache
    case count_quads_in_graph(db, graph_id) do
      {:ok, count} ->
        # Scan through all quads to populate cache
        _quads = QuadOperations.scan_graph(db, graph_id)
        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Warms the block cache for the default graph (ID=0).

  The default graph is frequently accessed in many workloads, so
  warming its cache can improve overall query performance.

  ## Parameters

  - `db` - RocksDB database reference

  ## Returns

  - `{:ok, count}` - Number of quads in default graph
  - `{:error, reason}` - On error

  """
  @spec warm_default_graph_cache(NIF.db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def warm_default_graph_cache(db) do
    warm_graph_cache(db, 0)
  end

  @doc """
  Warms the block cache for multiple graphs.

  ## Parameters

  - `db` - RocksDB database reference
  - `graph_ids` - List of graph IDs to warm

  ## Returns

  - `{:ok, map}` - Map of graph_id => quad_count
  - `{:error, reason}` - On error

  """
  @spec warm_multiple_graphs_cache(NIF.db_ref(), [non_neg_integer()]) ::
          {:ok, %{non_neg_integer() => non_neg_integer()}} | {:error, term()}
  def warm_multiple_graphs_cache(db, graph_ids) when is_list(graph_ids) do
    results =
      Enum.map(graph_ids, fn graph_id ->
        case warm_graph_cache(db, graph_id) do
          {:ok, count} -> {graph_id, count}
          {:error, _reason} -> {graph_id, 0}
        end
      end)

    {:ok, Map.new(results)}
  end

  # Private helper to count quads in a graph
  defp count_quads_in_graph(db, graph_id) do
    prefix = TripleStore.QuadIndex.gspo_prefix(graph_id)

    try do
      count =
        NIF.fold_keys(db, :gspo, prefix, 0, fn key, acc ->
          <<g::unsigned-big-integer-size(64), _rest::binary>> = key
          if g == graph_id, do: acc + 1, else: throw({:halt, acc})
        end)

      {:ok, count}
    catch
      {:halt, acc} -> {:ok, acc}
      :exit -> {:error, :scan_failed}
    end
  end
end
```

**Expected Impact:**
- 30-50% reduction in latency for frequently accessed graphs
- Particularly effective for default graph queries
- Reduces cache miss rate for graph-scoped workloads

---

#### 8.1.3.4 Use Read-Ahead for Sequential Graph Scans

**Current:** Default read-ahead settings
**Proposed:** Increase read-ahead for sequential graph scans

**Rationale:**
- Graph enumeration is a sequential access pattern
- Larger read-ahead reduces I/O wait time
- RocksDB supports read-ahead sizing via iterator options
- Optimal read-ahead: 1MB for SSD, 4MB for HDD

**Implementation:**

This may require adding read-ahead options to the NIF wrapper. Check if erlang-rocksdb supports this:

```elixir
# In /lib/triple_store/backend/rocksdb/erlang_adapter.ex

@doc """
  Creates an iterator with read-ahead optimization for sequential scans.

  ## Parameters

  - `adapter` - Adapter process
  - `cf` - Column family
  - `opts` - Keyword list of options:
    - `:readahead_size` - Read-ahead size in bytes (default: 1MB)

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - On error

  """
  @spec iterator_with_readahead(pid(), atom(), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def iterator_with_readahead(adapter, cf, opts \\ []) do
    readahead_size = Keyword.get(opts, :readahead_size, 1_048_576)  # 1MB default

    # Note: Check if erlang-rocksdb supports read-ahead options
    # This is a placeholder - actual implementation depends on NIF capabilities
    iterator(adapter, cf)
  end
```

**If read-ahead is not supported by erlang-rocksdb**, document this as a limitation and focus on other optimizations.

---

#### 8.1.3.5 Benchmark Graph-Scoped Queries (Target: <10ms)

Add to `/test/triple_store/benchmark/phase_8_1_quad_performance_test.exs`:

```elixir
describe "read performance benchmarks" do
  test "graph-scoped query <10ms", %{db: db} do
    # Insert 100K quads in default graph
    quads =
      for i <- 1..100_000 do
        {rem(i, 10_000) + 1, rem(i, 100) + 10, i + 100_000, 0}
      end

    :ok = QuadOperations.insert_quads(db, quads, sync: false)

    # Warm cache
    TripleStore.Quad.CacheWarmer.warm_default_graph_cache(db)

    # Measure query latency for graph-scoped pattern
    # Pattern: ?s ?p ?o (all quads in default graph)
    {time_us, result} =
      :timer.tc(fn ->
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      end)

    result_count = length(result)
    time_ms = div(time_us, 1000)

    IO.puts("\n  [Benchmark] Graph-scoped query returned #{result_count} quads")
    IO.puts("  [Benchmark] Query time: #{time_ms}ms")
    IO.puts("  [Benchmark] Throughput: #{div(result_count * 1_000_000, time_us)} quads/sec")

    # Target: <10ms for typical graph-scoped queries
    assert time_ms < 10,
           "Graph-scoped query took #{time_ms}ms, expected <10ms"
  end

  test "subject-scoped cross-graph query <5ms", %{db: db} do
    # Insert quads across multiple graphs
    quads =
      for i <- 1..10_000 do
        graph = rem(i, 10)
        {1, rem(i, 50) + 10, i + 10_000, graph}  # Subject 1 in all graphs
      end

    :ok = QuadOperations.insert_quads(db, quads, sync: false)

    # Measure query latency for subject-scoped pattern
    # Pattern: subject=1 ?p ?o ?g (subject 1 across all graphs)
    {time_us, result} =
      :timer.tc(fn ->
        QuadOperations.scan_subject(db, 1)
      end)

    result_count = length(result)
    time_ms = div(time_us, 1000)

    IO.puts("\n  [Benchmark] Subject-scoped query returned #{result_count} quads")
    IO.puts("  [Benchmark] Query time: #{time_ms}ms")

    # Target: <5ms for subject-scoped queries
    assert time_ms < 5,
           "Subject-scoped query took #{time_ms}ms, expected <5ms"
  end

  test "cache warming improves query latency", %{db: db} do
    # Insert 10K quads
    quads = for i <- 1..10_000, do: {rem(i, 1000) + 1, 10, i + 100, 0}
    :ok = QuadOperations.insert_quads(db, quads, sync: false)

    # Invalidate cache (close and reopen)
    NIF.close(db)
    {:ok, db} = NIF.open(test_path, schema: :quad)

    # Measure cold cache query
    {time_cold_us, _result} =
      :timer.tc(fn ->
        QuadOperations.scan_graph(db, 0)
      end)

    # Warm cache
    TripleStore.Quad.CacheWarmer.warm_default_graph_cache(db)

    # Measure warm cache query
    {time_warm_us, _result} =
      :timer.tc(fn ->
        QuadOperations.scan_graph(db, 0)
      end)

    speedup = Float.round(time_cold_us / time_warm_us, 2)

    IO.puts("\n  [Benchmark] Cold cache: #{div(time_cold_us, 1000)}ms")
    IO.puts("  [Benchmark] Warm cache: #{div(time_warm_us, 1000)}ms")
    IO.puts("  [Benchmark] Speedup: #{speedup}x")

    # Warm cache should be at least 2x faster
    assert time_warm_us * 2 < time_cold_us,
           "Cache warming did not improve query latency as expected"
  end
end
```

---

## Success Criteria

### Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Insert Throughput** (sync: false) | >50k quads/sec | Bulk insert benchmark |
| **Insert Throughput** (sync: true) | >10k quads/sec | Synchronous insert benchmark |
| **Graph-Scoped Query Latency** | <10ms | Graph enumeration query |
| **Subject-Scoped Query Latency** | <5ms | Cross-graph subject query |
| **Prefix Scan Throughput** | >100k quads/sec | Sequential graph scan |
| **Cache Hit Rate** (after warming) | >80% | RocksDB block cache stats |

### Functional Requirements

- [ ] 8.1.1.1: Quad indices use 16KB block size
- [ ] 8.1.1.2: Quad indices use 10 bits/key bloom filter
- [ ] 8.1.1.3: Prefix extractors enabled for quad indices
- [ ] 8.1.1.4: Quad indices use 128MB memtable
- [ ] 8.1.1.5: Quad tuning rationale documented
- [ ] 8.1.2.1: Adaptive WriteBatch sizing implemented
- [ ] 8.1.2.2: Batch grouping by graph implemented
- [ ] 8.1.2.3: Graph term lookup caching implemented
- [ ] 8.1.2.4: WAL usage documented
- [ ] 8.1.2.5: Insert throughput benchmark meets target
- [ ] 8.1.3.1: GSPO prefix scan optimized
- [ ] 8.1.3.2: SPOG prefix scan optimized
- [ ] 8.1.3.3: Cache warming module implemented
- [ ] 8.1.3.4: Read-ahead documented (if supported)
- [ ] 8.1.3.5: Read performance benchmarks meet targets

### Code Quality

- [ ] All new code has @moduledoc documentation
- [ ] All public functions have @spec typespecs
- [ ] All new functions have unit tests
- [ ] Benchmarks demonstrate performance improvements
- [ ] No regression in existing functionality (all tests pass)

---

## Implementation Plan

### Phase 1: RocksDB Configuration (Week 1)

**Objective:** Update RocksDB configuration for quad indices

**Tasks:**
1. Add quad-specific constants to `ColumnFamilyConfig`
   - `@block_size_quad_index` (16KB)
   - `@bloom_quad_index_bits` (10 bits/key)
   - `@write_buffer_size_quad` (128MB)

2. Create `quad_index_compaction_options()` function
   - 128MB memtable
   - Adjusted compaction triggers for larger memtable
   - Document quad-specific tuning rationale

3. Update `quad_index_cf_options()` to use new constants
   - 16KB block size
   - 10 bits/key bloom filter
   - Quad-specific compaction options

4. Enable prefix extractors for quad indices (if supported)
   - Test erlang-rocksdb prefix extractor format
   - Implement with fallback to manual checking

5. Add comprehensive documentation
   - Update @moduledoc with quad tuning rationale
   - Document trade-offs and targets

**Testing:**
- Unit tests for configuration values
- Integration tests with existing quad store
- Performance baseline benchmarks

**Success Criteria:**
- All tests pass
- Configuration values match specification
- Documentation is complete

---

### Phase 2: Write Optimization (Week 2)

**Objective:** Optimize write performance for quad operations

**Tasks:**
1. Create `TripleStore.Quad.BatchOptimizer` module
   - `group_quads_for_batch/2` - Adaptive batch sizing
   - `group_quads_by_graph/2` - Graph-local grouping
   - `estimate_operation_count/1` - Operation counting

2. Update `QuadOperations.insert_quads/3`
   - Add `use_batching` option
   - Add `group_by_graph` option
   - Integrate batch optimizer

3. Implement graph term lookup caching
   - Add `insert_quads_with_terms/4` function
   - Cache graph ID lookups for repeated terms
   - Benchmark improvement

4. Document WAL usage and best practices
   - Add @moduledoc section on durability
   - Document sync modes and recommendations
   - Provide WAL flush examples

5. Create write performance benchmarks
   - Bulk insert throughput (sync: false)
   - Synchronous insert throughput (sync: true)
   - Graph grouping improvement
   - Graph term caching improvement

**Testing:**
- Unit tests for batch optimizer
- Integration tests for optimized inserts
- Performance benchmarks meet targets
- Compare before/after metrics

**Success Criteria:**
- >50k quads/sec (sync: false)
- >10k quads/sec (sync: true)
- Graph grouping provides >10% improvement
- All tests pass

---

### Phase 3: Read Optimization (Week 3)

**Objective:** Optimize read performance for graph-scoped queries

**Tasks:**
1. Create `TripleStore.Quad.CacheWarmer` module
   - `warm_graph_cache/2` - Warm specific graph
   - `warm_default_graph_cache/1` - Warm default graph
   - `warm_multiple_graphs_cache/2` - Warm multiple graphs

2. Optimize GSPO prefix scan
   - Add `scan_graph/3` - Optimized graph scan
   - Add `scan_graph_stream/2` - Streaming graph scan
   - Use efficient bounds checking

3. Optimize SPOG prefix scan
   - Add `scan_subject/2` - Subject-scoped scan
   - Fast cross-graph aggregation
   - Efficient filtering

4. Investigate read-ahead support
   - Check erlang-rocksdb read-ahead options
   - Implement if supported
   - Document limitations if not supported

5. Create read performance benchmarks
   - Graph-scoped query latency (<10ms target)
   - Subject-scoped query latency (<5ms target)
   - Cache warming improvement
   - Prefix scan throughput (>100k quads/sec)

**Testing:**
- Unit tests for cache warmer
- Integration tests for optimized scans
- Performance benchmarks meet targets
- Cache effectiveness validation

**Success Criteria:**
- <10ms for graph-scoped queries
- <5ms for subject-scoped queries
- >100k quads/sec prefix scan throughput
- Cache warming provides >2x speedup

---

### Phase 4: Integration and Documentation (Week 4)

**Objective:** Integrate optimizations and complete documentation

**Tasks:**
1. Update existing documentation
   - CLAUDE.md - Add quad performance section
   - User guides - Document tuning options
   - API documentation - Add examples

2. Create performance tuning guide
   - `/guides/developer/05-quad-performance-tuning.md`
   - Document all configuration options
   - Provide tuning recommendations
   - Include benchmark results

3. Integration testing
   - Full test suite with optimizations enabled
   - Compare to Phase 7 baseline
   - Validate no regressions

4. Performance validation
   - Run all benchmarks
   - Verify targets are met
   - Document final performance numbers

5. Code review and cleanup
   - Review all new code
   - Ensure consistent style
   - Update type specs

**Testing:**
- Full test suite passes (all 4493+ tests)
- Performance benchmarks meet all targets
- Documentation is complete and accurate

**Success Criteria:**
- All tests pass
- All performance targets met
- Documentation complete
- Code review approved

---

## Testing Strategy

### Unit Tests

**Configuration Tests:**
- Verify quad index block size is 16KB
- Verify quad index bloom filter is 10 bits/key
- Verify quad memtable is 128MB
- Test prefix extractor configuration

**Batch Optimizer Tests:**
- Test batch grouping for various sizes
- Test graph grouping correctness
- Test operation count estimation
- Test edge cases (empty list, single quad)

**Cache Warmer Tests:**
- Test single graph warming
- Test multiple graph warming
- Test default graph warming
- Test error handling

**Optimized Scan Tests:**
- Test GSPO scan returns all quads in graph
- Test SPOG scan returns all quads for subject
- Test streaming scan correctness
- Test bounds checking

### Integration Tests

**Write Path:**
- Insert quads with batching enabled
- Insert quads with graph grouping
- Insert quads with graph term caching
- Verify all indices are updated correctly

**Read Path:**
- Perform graph-scoped queries
- Perform subject-scoped queries
- Verify cache warming effectiveness
- Compare optimized vs generic scans

**Mixed Workload:**
- Insert and query in same session
- Verify cache consistency
- Test concurrent operations
- Validate telemetry events

### Performance Benchmarks

**Baseline (Phase 7):**
- Establish current performance metrics
- Document as comparison point

**Target (Phase 8.1):**
- Insert throughput: >50k quads/sec (sync: false)
- Insert throughput: >10k quads/sec (sync: true)
- Graph-scoped query: <10ms
- Subject-scoped query: <5ms
- Prefix scan: >100k quads/sec

**Comparison:**
- Measure percentage improvement
- Document trade-offs
- Validate targets are met

### Regression Tests

- Ensure all existing tests pass
- No behavior changes to existing APIs
- Backward compatibility maintained
- Performance improvements are positive

---

## Notes and Considerations

### Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Larger block size increases memory** | Reduced cache capacity | Monitor memory usage; adjust if necessary |
| **Lower bloom filter bits increase FPR** | Slower point lookups | Measure FPR; adjust if queries degrade |
| **Larger memtable increases flush time** | Longer pause times | Monitor flush times; adjust if needed |
| **Prefix extractor format incompatibility** | Feature cannot be enabled | Document fallback to manual checking |
| **Graph grouping adds overhead** | Slower for small batches | Only enable for large batches (>1000 quads) |
| **Cache warming consumes I/O** | Slower startup | Make warming optional; use selectively |

### Future Improvements

1. **Adaptive Tuning**
   - Automatically adjust block size based on workload
   - Monitor access patterns and reconfigure
   - Implement ML-based optimization

2. **Partitioned Indices**
   - Split large graphs into partitions
   - Reduce individual index size
   - Improve parallelism

3. **Tiered Storage**
   - Hot graphs on SSD
   - Cold graphs on HDD
   - Automatic tier migration

4. **Compression Improvements**
   - Enable Zstd for better compression
   - Train compression dictionaries
   - Per-graph compression settings

### Dependencies

- **erlang-rocksdb**: Must support prefix extractors (verify version)
- **RocksDB C++ library**: Must be compiled with LZ4 support
- **System resources**: Sufficient RAM for larger memtables and block cache

### Monitoring

After deployment, monitor:

1. **Performance Metrics**
   - Insert throughput (quads/sec)
   - Query latency (p50, p95, p99)
   - Cache hit rate
   - Bloom filter false positive rate

2. **Resource Usage**
   - Memory usage (memtables, block cache)
   - Disk I/O (read/write throughput)
   - CPU usage (compression, compaction)

3. **Database Statistics**
   - Compaction efficiency
   - Write amplification factor
   - SSTable size distribution

### Rollback Plan

If performance targets are not met or issues arise:

1. **Revert Configuration Changes**
   - Restore original block size (8KB)
   - Restore original bloom filter (12 bits/key)
   - Restore original memtable (64MB)

2. **Disable New Features**
   - Remove batch grouping
   - Disable cache warming
   - Use generic scans instead of optimized ones

3. **Re-benchmark**
   - Verify performance returns to baseline
   - Document regression for future investigation

---

## References

### Internal Documentation

- `/notes/planning/quad/phase-08-production-hardening.md` - Overall Phase 8 plan
- `/lib/triple_store/backend/rocksdb/column_family_config.ex` - Current configuration
- `/lib/triple_store/quad_operations.ex` - Quad operations implementation
- `/notes/planning/performance/phase-04-storage-layer-tuning.md` - Triple store tuning (reference)

### External Resources

- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
- [RocksDB Bloom Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)
- [erlang-rocksdb Documentation](https://github.com/EnkiMultimedia/erlang-rocksdb)

### Related Work

- Phase 4: Storage Layer Tuning (triple store optimization)
- Phase 7: Reasoning with Named Graphs (quad store implementation)
- BSBM Benchmark Suite (query performance validation)

---

## Appendix

### A. Configuration Comparison

| Parameter | Triple Store | Quad Store (Current) | Quad Store (Proposed) |
|-----------|--------------|----------------------|----------------------|
| **Key Size** | 24 bytes | 32 bytes | 32 bytes |
| **Indices** | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) | 4 (GSPO, GPOS, SPOG, POSG) |
| **Block Size** | 8KB | 8KB | 16KB |
| **Bloom Filter** | 12 bits/key | 12 bits/key | 10 bits/key |
| **Memtable** | 64MB | 64MB | 128MB |
| **Write Amp** | 3x | 4x | 4x (optimized) |

### B. Performance Targets

| Metric | Current (Estimated) | Target | Improvement |
|--------|---------------------|--------|-------------|
| Insert Throughput (sync: false) | ~30k quads/sec | >50k quads/sec | +67% |
| Insert Throughput (sync: true) | ~5k quads/sec | >10k quads/sec | +100% |
| Graph-Scoped Query | ~15ms | <10ms | +50% |
| Subject-Scoped Query | ~8ms | <5ms | +60% |
| Prefix Scan Throughput | ~60k quads/sec | >100k quads/sec | +67% |

### C. Key Equations

**Bloom Filter Memory per Key:**
```
memory_per_key = bits_per_key × key_size × ln(2)
```

**False Positive Rate:**
```
FPR = (1 - e^(-bits_per_key × num_keys / cache_size))^bits_per_key
```

**Write Amplification:**
```
write_amp = num_indices × (1 + compaction_overhead)
```

**Keys Per Block:**
```
keys_per_block = block_size / (key_size + value_size)
```

---

**Document Status:** Ready for Review
**Last Updated:** 2025-01-20
**Version:** 1.0
