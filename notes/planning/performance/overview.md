# Performance Optimization Planning

## Overview

This planning directory contains the implementation roadmap for achieving the performance targets defined in `guides/benchmarks/performance-targets.md`. The optimization work is organized into four phases, each targeting specific performance bottlenecks identified through benchmark analysis.

The detailed analysis supporting these plans is documented in `notes/reviews/benchmark-performance-improvements.md`.

---

## Current Performance Status

| Target | Metric | Current | Goal | Gap |
|--------|--------|---------|------|-----|
| Simple BGP | p95 latency | 9.4-11.6ms | <10ms | Borderline |
| Complex Join | p95 latency | 96.3ms | <100ms | Pass |
| Bulk Load | throughput | 42K tps | >100K tps | **58% below** |
| BSBM Mix | p95 latency | 170ms | <50ms | **240% over** |

---

## Phase Documents

| Phase | Focus | Target Improvement | Priority |
|-------|-------|-------------------|----------|
| [Phase 1](./phase-01-bulk-load-optimization.md) | Bulk Load Optimization | 42K → 100K+ tps | Critical |
| [Phase 2](./phase-02-bsbm-query-optimization.md) | BSBM Query Optimization | 170ms → <50ms p95 | Critical |
| [Phase 3](./phase-03-query-engine-improvements.md) | Query Engine Improvements | General performance | Medium |
| [Phase 4](./phase-04-storage-layer-tuning.md) | Storage Layer Tuning | Infrastructure | Low |

---

## Success Criteria

### Phase 1: Bulk Load Optimization
- [ ] Bulk load throughput exceeds 100,000 triples/second
- [ ] Dictionary encoding supports parallel operations
- [ ] Flow-based pipeline achieves near-linear scaling with CPU cores
- [ ] Memory usage remains bounded during large imports

### Phase 2: BSBM Query Optimization
- [ ] BSBM Q7 latency reduced from 1393ms to <100ms
- [ ] BSBM Q6 latency reduced from 175ms to <5ms
- [ ] Q5 and Q11 execute correctly (bug fixes)
- [ ] Overall BSBM mix p95 below 50ms target

### Phase 3: Query Engine Improvements
- [ ] Statistics collection provides accurate cardinality estimates
- [ ] Query cache reduces repeated query latency by >80%
- [ ] Cost model improvements measurable in query plans

### Phase 4: Storage Layer Tuning
- [ ] RocksDB prefix extractor eliminates per-iteration bounds checks
- [ ] Column family bloom filters achieve <0.1% false positive rate
- [ ] Snapshot lifecycle properly managed with TTL

---

## Implementation Order

```
Phase 1 ─────────────────────────────────────────────────────────►
         Bulk Load (Critical - enables efficient testing)

         Phase 2 ────────────────────────────────────────────────►
                  BSBM Queries (Critical - largest user impact)

                  Phase 3 ───────────────────────────────────────►
                           Query Engine (Enables further optimization)

                           Phase 4 ──────────────────────────────►
                                    Storage Tuning (Foundation)
```

Phases can proceed in parallel where dependencies allow. Phase 1 should be prioritized first as it enables efficient testing and benchmarking of subsequent phases.

---

## Key Files Modified

### Phase 1
- `lib/triple_store/dictionary/manager.ex` - Sharded manager, ETS cache
- `lib/triple_store/loader.ex` - Batch size, Flow pipeline
- `native/rocksdb_nif/src/lib.rs` - Write options
- `lib/triple_store/config/rocksdb.ex` - Write buffer sizing

### Phase 2
- `lib/triple_store/sparql/executor.ex` - BIND push-down
- `lib/triple_store/sparql/optimizer.ex` - Join reordering
- `lib/triple_store/index.ex` - Price range index (new)
- `lib/triple_store/benchmark/bsbm_queries.ex` - Bug fixes

### Phase 3
- `lib/triple_store/sparql/statistics.ex` - New module
- `lib/triple_store/sparql/cache.ex` - New module
- `lib/triple_store/sparql/cost_model.ex` - Refinements

### Phase 4
- `native/rocksdb_nif/src/lib.rs` - Prefix extractor
- `lib/triple_store/config/column_family.ex` - Bloom filter tuning
- `lib/triple_store/snapshot.ex` - TTL management

---

## References

- [Performance Targets](../../guides/benchmarks/performance-targets.md)
- [LUBM Benchmark](../../guides/benchmarks/lubm.md)
- [BSBM Benchmark](../../guides/benchmarks/bsbm.md)
- [Performance Improvement Analysis](../reviews/benchmark-performance-improvements.md)
