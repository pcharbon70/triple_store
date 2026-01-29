# WatDiv Benchmark Implementation Summary

## Overview

Implemented the Waterloo SPARQL Diversity Test (WatDiv) benchmark for the TripleStore project. WatDiv complements the existing BSBM and LUBM benchmarks by providing diverse query patterns over heterogeneous RDF data.

## Implementation Details

### Files Created

1. **`lib/triple_store/benchmark/watdiv.ex`** (~750 lines)
   - Complete WatDiv data generator
   - Heterogeneous structure with probabilistic attributes
   - Scale factor support (1 = ~40K triples)
   - Stream generation for memory efficiency
   - ETS tables for tracking friendships and likes

2. **`lib/triple_store/benchmark/watdiv_queries.ex`** (~520 lines)
   - All 20 WatDiv query templates
   - Linear queries (L1-L5)
   - Star queries (S1-S7)
   - Snowflake queries (F1-F5)
   - Complex queries (C1-C3)
   - Parameter substitution with `%v1%` placeholder format

3. **`test/triple_store/benchmark/watdiv_test.exs`** (~380 lines)
   - 38 tests for data generator
   - Tests for RDF validity
   - Tests for WatDiv-specific features

4. **`test/triple_store/benchmark/watdiv_queries_test.exs`** (~460 lines)
   - 38 tests for query templates
   - Tests for parameter substitution
   - Tests for query categorization

### Files Modified

1. **`scripts/run_benchmarks.exs`**
   - Added WatDiv alias
   - Added WatDiv benchmark section
   - Added WatDiv query performance table
   - Added WatDiv to bulk load throughput table

## WatDiv Characteristics

### Data Model

- **Heterogeneous structure**: Same entity types don't always have the same attributes
- **Probabilistic attributes**: Attributes appear with specific probabilities
- **Social relationships**: Friendships and likes between users
- **E-commerce entities**: Products, offers, purchases, retailers

### Entity Types

- User (social network users)
- Product (goods for sale)
- Offer (seller offers)
- Purchase (purchase transactions)
- Retailer (product sellers)
- Website (retailer websites)
- Genre, SubGenre (product categorization)
- City, Country, Language (metadata)

### Query Categories

| Category | Queries | Pattern Type |
|----------|---------|--------------|
| Linear | L1-L5 | Linear graph paths |
| Star | S1-S7 | Centered on single entity |
| Snowflake | F1-F5 | Branching patterns |
| Complex | C1-C3 | Combined features |

## Key Implementation Decisions

1. **Namespace isolation**: WatDiv uses its own namespace (`wsdbm:`)
2. **ETS for deduplication**: Friendship and like relationships tracked via ETS
3. **Probabilistic attributes**: `maybe_add/4` helper for conditional attributes
4. **Stream support**: Memory-efficient generation for large datasets
5. **Scale factor**: Base of ~40K triples per scale factor

## Test Results

All 76 tests pass:
- 38 tests for data generator
- 38 tests for query templates

## Integration with Existing Benchmarks

WatDiv now runs alongside BSBM and LUBM in `run_benchmarks.exs`:
```bash
mix run scripts/run_benchmarks.exs
```

## Notes

- The WatDiv basic test set includes 20 queries (not the full 100+ query suite)
- Query templates use `%v1%` placeholder format (different from BSBM's `{param}` format)
- Some queries use GoodRelations (`gr:`) and Open Graph (`og:`) vocabularies
- Generated data matches WatDiv's e-commerce domain theme
