# Benchmark Performance Improvements

> **Analysis Date:** 2025-12-31
> **Target:** Achieve performance targets for all benchmark suites

## Executive Summary

This document identifies specific improvements to achieve the performance targets defined in `guides/benchmarks/performance-targets.md`. The analysis covers:

| Target | Current | Goal | Gap | Priority |
|--------|---------|------|-----|----------|
| Simple BGP | 9.4-11.6ms | <10ms | Borderline | Medium |
| Complex Join | 96.3ms | <100ms | Pass (borderline) | Low |
| Bulk Load | 42K tps | >100K tps | 58% below | **Critical** |
| BSBM Mix | 170ms | <50ms | 240% over | **Critical** |

---

## Part 1: Bulk Load Optimization (Critical)

### Current Bottleneck Analysis

The bulk load achieves only 42% of the target (42K vs 100K triples/sec). Root causes identified:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BULK LOAD PIPELINE (Current)                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  RDF Triples ──► Dictionary Encoding ──► Index Writes ──► Commit    │
│                        │                      │                      │
│                        ▼                      ▼                      │
│                  BOTTLENECK 1:          BOTTLENECK 2:               │
│              Single GenServer        Small Batch Size               │
│              (sequential)            (1000 triples)                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.1 Dictionary Manager Serialization (Critical)

**File:** `lib/triple_store/dictionary/manager.ex`

**Problem:** All dictionary operations serialize through a single GenServer, creating a bottleneck.

```elixir
# Current: Sequential processing in GenServer
def handle_call({:get_or_create_ids, terms}, _from, state) do
  # Processes terms one-by-one via Batch.map_collect_success
  result = do_get_or_create_ids(state.db, state.counter, terms)
  {:reply, result, state}
end
```

**Impact:**
- 100K triples/sec = 300K term encodings/sec needed
- GenServer can achieve ~10K ops/sec (estimate)
- Theoretical limit: ~33K triples/sec from dictionary alone

**Improvements:**

| Improvement | Implementation | Expected Gain |
|-------------|----------------|---------------|
| **1.1.1 Sharded Dictionary Manager** | Partition terms by hash across N GenServers | +200% |
| **1.1.2 Lock-Free Read Cache** | ETS table for read-only lookups | +50% |
| **1.1.3 Batch Sequence Allocation** | Pre-allocate ID ranges per batch | +30% |

**Implementation Details:**

```elixir
# 1.1.1 Sharded Manager (conceptual)
defmodule TripleStore.Dictionary.ShardedManager do
  @shards 8  # Configurable based on CPU cores

  def get_or_create_id(term) do
    shard = :erlang.phash2(term, @shards)
    GenServer.call(:"dictionary_shard_#{shard}", {:get_or_create_id, term})
  end
end
```

```elixir
# 1.1.2 Lock-Free Read Cache
# In Manager init:
:ets.new(:dictionary_cache, [:named_table, :public, :set, {:read_concurrency, true}])

# In get_id (read path):
def get_id(term) do
  case :ets.lookup(:dictionary_cache, term) do
    [{^term, id}] -> {:ok, id}
    [] -> GenServer.call(__MODULE__, {:get_id, term})
  end
end
```

---

### 1.2 Batch Size Configuration (Critical)

**File:** `lib/triple_store/loader.ex:89`

**Problem:** Default batch size of 1,000 creates excessive NIF round-trips.

```elixir
# Current
@default_batch_size 1000  # Too small
```

**Improvements:**

| Batch Size | NIF Calls per 1M | Overhead | Status |
|------------|------------------|----------|--------|
| 1,000 | 1,000 | High | Current |
| 5,000 | 200 | Medium | Recommended minimum |
| 10,000 | 100 | Low | Optimal |
| 50,000 | 20 | Very Low | For bulk imports |

**Implementation:**

```elixir
# Proposed change to loader.ex
@default_batch_size 10_000  # 10x improvement

# Or dynamic based on available memory
def optimal_batch_size(opts) do
  memory_budget = Keyword.get(opts, :memory_budget, :auto)
  case memory_budget do
    :auto -> detect_optimal_batch_size()
    :low -> 5_000
    :high -> 50_000
    bytes when is_integer(bytes) -> trunc(bytes / @bytes_per_triple)
  end
end
```

---

### 1.3 Parallel Pipeline Loading (High Priority)

**File:** `lib/triple_store/loader.ex:469-493`

**Problem:** No pipelining between dictionary encoding and index writes.

```elixir
# Current: Sequential batch processing
defp load_triples(db, manager, triples, batch_size) do
  triples
  |> Stream.chunk_every(batch_size)
  |> Enum.reduce_while({:ok, 0}, fn batch, {:ok, total} ->
    # Blocks on each batch completely
    case process_batch(db, manager, batch) do
      :ok -> {:cont, {:ok, total + length(batch)}}
    end
  end)
end
```

**Improvements:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                PROPOSED PIPELINE ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Stage 1: Chunking     Stage 2: Encoding     Stage 3: Writing       │
│  ────────────────      ──────────────────    ─────────────────      │
│                                                                      │
│  [Batch 1] ──────────► [Encode 1] ─────────► [Write 1]              │
│  [Batch 2] ──────────► [Encode 2] ─────────► [Write 2]              │
│  [Batch 3] ──────────► [Encode 3] ─────────► [Write 3]              │
│       ▲                     ▲                    ▲                   │
│       │                     │                    │                   │
│    Parallel              Parallel             Sequential             │
│    OK                    4 workers            (atomic)               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Implementation using Flow:**

```elixir
defp load_triples_parallel(db, manager, triples, opts) do
  stages = Keyword.get(opts, :stages, System.schedulers_online())
  batch_size = Keyword.get(opts, :batch_size, 10_000)

  triples
  |> Stream.chunk_every(batch_size)
  |> Flow.from_enumerable(stages: stages, max_demand: 2)
  |> Flow.map(fn batch ->
    # Stage 2: Parallel encoding (CPU-bound)
    {:ok, encoded} = Adapter.from_rdf_triples(manager, batch)
    encoded
  end)
  |> Flow.partition(stages: 1)  # Single writer for atomicity
  |> Flow.reduce(fn -> 0 end, fn encoded, acc ->
    # Stage 3: Sequential writing (I/O-bound)
    :ok = Index.insert_triples(db, encoded)
    acc + length(encoded)
  end)
  |> Enum.to_list()
  |> List.first()
end
```

**Expected Throughput:**

| Configuration | Throughput | Notes |
|---------------|------------|-------|
| Current (sequential) | 42K tps | Baseline |
| 4-stage pipeline | ~80K tps | 2x improvement |
| 8-stage + large batches | ~120K tps | Exceeds target |

---

### 1.4 RocksDB Write Options (Medium Priority)

**File:** `native/rocksdb_nif/src/lib.rs:481-569`

**Problem:** Using default WriteOptions, which fsync each write.

```rust
// Current
fn write_batch<'a>(...) -> NifResult<Term<'a>> {
    let mut batch = WriteBatch::default();
    // ...
    match shared_db.db.write(batch) {  // Default options
        Ok(()) => Ok(atoms::ok().encode(env)),
        // ...
    }
}
```

**Improvements:**

```rust
// Proposed
fn write_batch<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    operations: Vec<(Term, Term, Term)>,
    sync: bool  // New parameter
) -> NifResult<Term<'a>> {
    let mut batch = WriteBatch::default();
    // ...

    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(sync);  // false for bulk loading
    write_opts.disable_wal(false);  // Keep WAL for crash recovery

    match shared_db.db.write_opt(batch, &write_opts) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        // ...
    }
}
```

**Expected Impact:**

| Option | Description | Throughput Gain |
|--------|-------------|-----------------|
| `sync=false` | Defer fsync to OS | +20-30% |
| `disable_wal=true` | Skip write-ahead log | +50% (risky) |
| Larger write buffers | More in-memory batching | +10% |

---

### 1.5 Write Buffer Sizing (Low Priority)

**File:** `lib/triple_store/config/rocksdb.ex:91-123`

**Problem:** Write buffers are sized for general use, not bulk loading.

```elixir
# Current presets
write_heavy: %{
  write_buffer_size: 256 * 1024 * 1024,  # 256 MB
  max_write_buffer_number: 4,
}
```

**Improvements:**

```elixir
# Add bulk_load preset
bulk_load: %{
  write_buffer_size: 512 * 1024 * 1024,  # 512 MB per CF
  max_write_buffer_number: 6,             # More buffers before stall
  level0_file_num_compaction_trigger: 8,  # Delay L0 compaction
  level0_slowdown_writes_trigger: 20,     # Higher thresholds
  level0_stop_writes_trigger: 36,
}
```

---

## Part 2: BSBM Query Optimization (Critical)

### Current Performance Profile

```
┌───────────────────────────────────────────────────────────────┐
│                  BSBM QUERY LATENCY BREAKDOWN                  │
├───────┬──────────┬──────────────────────────────────────────┤
│ Query │ Latency  │ Issue                                     │
├───────┼──────────┼──────────────────────────────────────────┤
│ Q1-Q4 │ 4-20ms   │ OK - Search queries                       │
│ Q5    │ error    │ Typed literal mismatch                    │
│ Q6    │ 175ms    │ CRITICAL - Single lookup too slow         │
│ Q7    │ 1393ms   │ CRITICAL - Offer join explosion           │
│ Q8-Q10│ 0.2-1ms  │ OK - Simple lookups                       │
│ Q11   │ error    │ URI escaping issue                        │
│ Q12   │ 28ms     │ OK - CONSTRUCT                            │
└───────┴──────────┴──────────────────────────────────────────┘
```

### 2.1 Q7 Optimization: Product-Offer Join (Critical)

**Query:**
```sparql
SELECT ?product ?offer ?price ?vendor
WHERE {
  ?product rdf:type bsbm:Product .
  ?offer bsbm:product ?product .
  ?offer bsbm:price ?price .
  ?offer bsbm:vendor ?vendor .
  FILTER (?price >= 50 && ?price <= 500)
}
ORDER BY ?price
LIMIT 20
```

**Problem:** Query scans all products (~1000) and all offers (~13K), then filters by price.

**Root Cause Analysis:**

```
Current Execution Plan:
1. Scan all bsbm:Product types (~1000 products)     O(n)
2. For each product, lookup offers (~13 per)        O(n*m)
3. For each offer, get price and vendor             O(n*m)
4. Apply price filter                               Post-filter
5. Sort by price                                    O(k log k)
6. Take top 20                                      O(1)

Total: O(n*m) = 1000 * 13 = 13,000 index lookups
```

**Improvements:**

| Improvement | Implementation | Expected Latency |
|-------------|----------------|------------------|
| **2.1.1 Price Index** | Secondary index on `bsbm:price` | 50-100ms |
| **2.1.2 Join Reordering** | Start from price filter, not product type | 100-200ms |
| **2.1.3 Materialized View** | Pre-computed product-offer-price tuples | <20ms |

**2.1.1 Price Index Implementation:**

```elixir
# Add numeric range index for price predicates
defmodule TripleStore.Index.NumericRange do
  @doc """
  Maintains a B-tree index for numeric predicates enabling range queries.

  Key format: <<predicate_id::64, value::64-float, subject_id::64>>
  """

  def create_range_index(db, predicate_id) do
    # Create column family: price_range
    # Keys: price value (as sortable float) + offer_id
  end

  def range_query(db, predicate_id, min, max) do
    # Efficient range scan using RocksDB prefix iterator
    prefix = <<predicate_id::64-big>>
    min_key = <<predicate_id::64-big, float_to_sortable(min)::64-big>>
    max_key = <<predicate_id::64-big, float_to_sortable(max)::64-big>>

    NIF.range_iterator(db, :price_range, min_key, max_key)
  end
end
```

**2.1.2 Join Reordering:**

```elixir
# In optimizer.ex, add price predicate selectivity
defp selectivity_for_pattern({:triple, _s, predicate, _o}, stats) when is_filter_applied do
  case predicate do
    # Price with FILTER is highly selective
    "bsbm:price" -> 0.1  # Only 10% of offers match price range
    _ -> estimate_from_stats(predicate, stats)
  end
end

# Reordered execution plan:
# 1. Get offers matching price range       O(k) where k = filtered offers
# 2. For each offer, get product           O(k)
# 3. Verify product type                   O(k)
# 4. Get vendor                            O(k)
# Total: O(k) where k << n*m
```

**2.1.3 Materialized View (Most Effective):**

```elixir
defmodule TripleStore.MaterializedView do
  @doc """
  Pre-computed join for common BSBM patterns.
  """

  def create_product_offer_view(store) do
    # Column family: mv_product_offer
    # Key: <<product_id::64, offer_id::64>>
    # Value: <<price::64-float, vendor_id::64, valid_from::64, valid_to::64>>
  end

  def query_by_price_range(store, min, max, limit) do
    # Direct range scan on materialized view
    # O(log n + k) instead of O(n * m)
  end
end
```

---

### 2.2 Q6 Optimization: Single Product Lookup (High Priority)

**Query:**
```sparql
SELECT ?product ?label ?comment ?producer ...
WHERE {
  BIND(<.../Product1> AS ?product)
  ?product rdfs:label ?label .
  ?product rdfs:comment ?comment .
  ?product bsbm:producer ?producer .
  ?product bsbm:productPropertyTextual1 ?propertyTextual1 .
  ...
}
```

**Problem:** Single-product lookup takes 175ms (should be <1ms).

**Root Cause Investigation:**

```
Suspected Issues:
1. BIND not being pushed down - full pattern evaluation first
2. 7 separate index lookups for one subject
3. No subject-based caching
```

**Improvements:**

| Improvement | Implementation | Expected Latency |
|-------------|----------------|------------------|
| **2.2.1 BIND Push-Down** | Treat BIND as bound constant | <5ms |
| **2.2.2 Multi-Property Fetch** | Batch property lookups | <2ms |
| **2.2.3 Subject Cache** | Cache all properties for recently accessed subjects | <1ms |

**2.2.1 BIND Push-Down:**

```elixir
# In executor.ex, handle BIND specially
defp execute_pattern(ctx, {:extend, pattern, var, value}) do
  # If value is a constant IRI, treat as bound in inner pattern
  case value do
    {:iri, _} ->
      binding = %{var => value}
      execute_pattern(%{ctx | bindings: binding}, pattern)
    _ ->
      # Standard EXTEND behavior
      execute_extend(ctx, pattern, var, value)
  end
end
```

**2.2.2 Multi-Property Fetch:**

```elixir
# In index.ex, add batch lookup for same subject
def lookup_all_properties(db, subject_id) do
  # Single prefix scan on SPO index for subject
  prefix = <<subject_id::64-big>>

  NIF.prefix_iterator(db, :spo, prefix)
  |> Stream.map(fn <<_s::64, p::64, o::64>> -> {p, o} end)
  |> Enum.to_list()
end
```

---

### 2.3 Q5/Q11 Bug Fixes (Medium Priority)

**Q5 Issue:** Typed literal syntax mismatch

```sparql
# Query uses:
?product rdfs:label "Product1"^^xsd:string .

# Data has:
?product rdfs:label "Product1" .  # Plain literal
```

**Fix:** Normalize literal comparison in FILTER evaluation.

**Q11 Issue:** URI fragment escaping

```sparql
# Query has:
?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries\#US> .

# Should be:
?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
```

**Fix:** Update `bsbm_queries.ex:433` to remove backslash.

---

## Part 3: LUBM Query Optimization (Medium Priority)

### Current Performance Profile

| Query | Latency | Issue | Priority |
|-------|---------|-------|----------|
| Q1 | 13.3ms | Slightly over target | Medium |
| Q2 | 96.3ms | Complex 3-way join, borderline | Low |
| Q3 | 36.5ms | Publications lookup | Medium |
| Q14 | 11.5ms | Large result set (2329 results) | Low |

### 3.1 Q2 Join Optimization

**Query:**
```sparql
SELECT ?x ?y ?z
WHERE {
  ?x rdf:type ub:GraduateStudent .
  ?y rdf:type ub:University .
  ?z rdf:type ub:Department .
  ?x ub:memberOf ?z .
  ?z ub:subOrganizationOf ?y .
  ?x ub:undergraduateDegreeFrom ?y .
}
```

**Analysis:** Three type scans + three joins = O(students * universities * departments).

**Improvements:**

```
Optimal Join Order:
1. ?z ub:subOrganizationOf ?y       (depts → universities, small)
2. ?x ub:memberOf ?z                 (students in dept, medium)
3. ?x ub:undergraduateDegreeFrom ?y  (filter: same university)
4. Verify types                       (post-filter)
```

```elixir
# Verify optimizer selects correct join order
# File: lib/triple_store/sparql/optimizer.ex

defp pattern_selectivity({:triple, s, p, o}) do
  case {bound?(s), bound?(o)} do
    {false, true} -> 5.0   # ?x predicate <uri> - most selective
    {true, false} -> 10.0  # <uri> predicate ?x
    {false, false} -> 100.0 # ?x predicate ?y - least selective
    {true, true} -> 1.0    # Both bound - existence check
  end
end
```

---

## Part 4: Query Engine Improvements (General)

### 4.1 Statistics Collection

**Problem:** Cardinality estimation uses hardcoded factors.

**File:** `lib/triple_store/sparql/cardinality.ex`

**Improvements:**

```elixir
defmodule TripleStore.Statistics do
  @doc """
  Collect and maintain statistics for query optimization.
  """

  defstruct [
    :total_triples,
    :distinct_subjects,
    :distinct_predicates,
    :distinct_objects,
    :predicate_cardinalities,  # Map of predicate → count
    :predicate_histograms      # For numeric predicates
  ]

  def collect(store) do
    %__MODULE__{
      total_triples: count_triples(store),
      distinct_subjects: count_distinct(store, :subject),
      distinct_predicates: count_distinct(store, :predicate),
      distinct_objects: count_distinct(store, :object),
      predicate_cardinalities: build_predicate_stats(store),
      predicate_histograms: build_numeric_histograms(store)
    }
  end
end
```

### 4.2 Query Result Caching

**Problem:** Repeated patterns hit storage every time.

**Improvements:**

```elixir
defmodule TripleStore.QueryCache do
  @doc """
  LRU cache for query results with TTL.
  """
  use GenServer

  @default_ttl :timer.minutes(5)
  @max_entries 1000

  def get_or_execute(cache, query, execute_fn) do
    key = hash_query(query)

    case lookup(cache, key) do
      {:ok, result} -> result
      :miss ->
        result = execute_fn.()
        put(cache, key, result, @default_ttl)
        result
    end
  end
end
```

### 4.3 Lazy Property Path Evaluation

**Problem:** Property paths materialize all intermediate results.

**File:** `lib/triple_store/sparql/executor.ex:1097`

**Current:**
```elixir
path_stream |> Enum.to_list()  # Forces materialization
```

**Improvements:**

```elixir
# Use lazy iteration with depth limit
def evaluate_property_path(ctx, start, path, end_var) do
  Stream.resource(
    fn -> {[{start, 0}], MapSet.new()} end,  # BFS queue + visited
    fn {queue, visited} ->
      case explore_next(queue, visited, path, ctx) do
        {:found, binding, new_queue, new_visited} ->
          {[binding], {new_queue, new_visited}}
        {:continue, new_queue, new_visited} ->
          explore_next(new_queue, new_visited, path, ctx)
        :done ->
          {:halt, nil}
      end
    end,
    fn _ -> :ok end
  )
end
```

---

## Part 5: Storage Layer Improvements

### 5.1 Prefix Extractor Optimization

**File:** `native/rocksdb_nif/src/lib.rs:844-845`

**Problem:** Iterator bounds checking on every `next()` call.

```rust
// Current
if !key.starts_with(&iter_ref.prefix) {
    return Ok(atoms::iterator_end().encode(env));
}
```

**Improvement:** Let RocksDB handle bounds via SliceTransform.

```rust
// In open(), configure prefix extractor
let mut cf_opts = Options::default();
cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
// 8 bytes = first component of triple key
```

### 5.2 Snapshot Management

**Problem:** Long-lived snapshots block compaction.

**Improvements:**

```elixir
defmodule TripleStore.Snapshot do
  @max_lifetime :timer.minutes(5)

  def with_snapshot(store, fun) do
    snapshot = create_snapshot(store)
    try do
      fun.(snapshot)
    after
      release_snapshot(snapshot)
    end
  end

  # Automatic cleanup via process monitoring
  def create_snapshot(store) do
    ref = make_ref()
    snapshot = NIF.create_snapshot(store.db)

    # Auto-release after timeout
    Process.send_after(self(), {:release_snapshot, ref}, @max_lifetime)

    {ref, snapshot}
  end
end
```

### 5.3 Column Family Tuning

**File:** `lib/triple_store/config/column_family.ex`

**Improvements for Index CFs:**

```elixir
# Increase bloom filter bits for lower FPR
def bloom_filter_config(:spo), do: %{bits_per_key: 12, block_based: true}
def bloom_filter_config(:pos), do: %{bits_per_key: 12, block_based: true}
def bloom_filter_config(:osp), do: %{bits_per_key: 12, block_based: true}

# Use larger blocks for sequential scans
def block_size(:spo), do: 8 * 1024  # 8KB instead of 4KB
def block_size(:derived), do: 32 * 1024  # 32KB for reasoning results
```

---

## Part 6: Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)

| Task | Impact | Effort |
|------|--------|--------|
| Increase batch size to 10K | +30% bulk load | Low |
| Fix Q5/Q11 query bugs | 2 queries working | Low |
| Add `sync=false` for bulk loads | +20% bulk load | Low |

### Phase 2: Medium-Term (1-2 weeks)

| Task | Impact | Effort |
|------|--------|--------|
| Flow-based parallel loading | +100% bulk load | Medium |
| BIND push-down in executor | Fix Q6 (175ms → <5ms) | Medium |
| Price range index | Fix Q7 (1393ms → <100ms) | Medium |
| Statistics collection | Better query plans | Medium |

### Phase 3: Long-Term (2-4 weeks)

| Task | Impact | Effort |
|------|--------|--------|
| Sharded dictionary manager | +200% bulk load | High |
| Materialized views | Sub-ms joins | High |
| Query result caching | Repeated query speedup | Medium |
| Leapfrog Triejoin tuning | Complex join optimization | High |

---

## Part 7: Expected Results

### After All Optimizations

| Metric | Current | Expected | Target | Status |
|--------|---------|----------|--------|--------|
| Simple BGP (p95) | 11.6ms | <5ms | <10ms | Pass |
| Complex Join (p95) | 96.3ms | <50ms | <100ms | Pass |
| Bulk Load | 42K tps | 150K+ tps | >100K tps | Pass |
| BSBM Mix (p95) | 170ms | <30ms | <50ms | Pass |

### Throughput Projections

```
Bulk Load Throughput Improvement Stack:

Current:                    42,000 tps
+ Batch size (10K):        +12,600 tps  →  54,600 tps
+ sync=false:               +8,190 tps  →  62,790 tps
+ Parallel pipeline (4x):  +47,093 tps  → 109,883 tps  ✓ TARGET MET
+ Sharded dictionary:      +65,930 tps  → 175,813 tps
```

---

## References

- [Performance Targets](../guides/benchmarks/performance-targets.md)
- [LUBM Benchmark Guide](../guides/benchmarks/lubm.md)
- [BSBM Benchmark Guide](../guides/benchmarks/bsbm.md)
- [Storage Layer: Index Module](../lib/triple_store/index.ex)
- [Storage Layer: Dictionary Module](../lib/triple_store/dictionary.ex)
- [Query Engine: Executor](../lib/triple_store/sparql/executor.ex)
- [Query Engine: Optimizer](../lib/triple_store/sparql/optimizer.ex)
- [RocksDB NIF](../native/rocksdb_nif/src/lib.rs)
