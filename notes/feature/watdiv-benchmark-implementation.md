# WatDiv Benchmark Implementation

## Overview

This document describes the implementation of the Waterloo SPARQL Diversity Test (WatDiv) benchmark for the TripleStore project. WatDiv complements the existing BSBM and LUBM benchmarks by providing a more diverse set of query patterns and data characteristics.

## Goals

1. Implement WatDiv data generator that creates synthetic heterogeneous RDF datasets
2. Implement the 20 standard WatDiv query templates (L1-L5, S1-S7, F1-F5, C1-C5)
3. Integrate WatDiv into the benchmark execution script
4. Provide comprehensive tests

## Background

WatDiv (Waterloo SPARQL Diversity Test) is designed to test RDF stores with diverse query patterns:

- **Linear (L1-L5)**: Queries following linear paths
- **Star (S1-S7)**: Queries centered on a single entity with many relationships
- **Snowflake (F1-F5)**: Queries with branching patterns
- **Complex (C1-C5)**: Queries with complex combinations

### WatDiv vs Other Benchmarks

| Feature | WatDiv | BSBM | LUBM |
|---------|--------|------|------|
| Domain | E-commerce | E-commerce | University |
| Data Structure | Heterogeneous | Homogeneous | Homogeneous |
| Query Types | 4 categories (20 queries) | 12 queries | 14 queries |
| Scale Factor | ~100K triples per factor | ~500 triples per product | ~100K triples per university |
| Attributes | Probabilistic, correlated | Fixed | Fixed |

## Implementation Tasks

### Task 1: WatDiv Data Generator ✅
**Status**: Pending

**File**: `lib/triple_store/benchmark/watdiv.ex`

**Requirements**:
- Generate entities: Users, Goods, Reviews, Sellers, Offer, Friendship, Likes
- Support scale factors (1 = ~100K triples)
- Probabilistic attributes (same entity types don't always have same attributes)
- Correlated attributes between entities
- Stream generation for large datasets

**Sub-tasks**:
- [ ] Define WatDiv namespaces and URIs
- [ ] Implement entity generators (User, Good, Review, Seller, Offer)
- [ ] Implement relationship generators (Friendship, Likes)
- [ ] Add probabilistic attribute generation
- [ ] Add `generate/1` and `stream/1` functions
- [ ] Add `estimate_triple_count/1` function

### Task 2: WatDiv Query Templates ✅
**Status**: Pending

**File**: `lib/triple_store/benchmark/watdiv_queries.ex`

**Requirements**:
- Implement 20 query templates (L1-L5, S1-S7, F1-F5, C1-C5)
- Support parameter substitution
- Provide query metadata (complexity, category, description)

**Sub-tasks**:
- [ ] Define query template struct
- [ ] Implement Linear queries (L1-L5)
- [ ] Implement Star queries (S1-S7)
- [ ] Implement Snowflake queries (F1-F5)
- [ ] Implement Complex queries (C1-C5)
- [ ] Add parameter substitution helper

### Task 3: Benchmark Script Integration ✅
**Status**: Pending

**File**: `scripts/run_benchmarks.exs`

**Requirements**:
- Add WatDiv section alongside BSBM and LUBM
- Use scale factor 1 (~100K triples)
- Report same metrics as other benchmarks

**Sub-tasks**:
- [ ] Add WatDiv alias
- [ ] Add WatDiv data generation section
- [ ] Add WatDiv query execution section
- [ ] Add WatDiv results table

### Task 4: Tests ✅
**Status**: Pending

**File**: `test/triple_store/benchmark/watdiv_test.exs`

**Requirements**:
- Test data generation
- Test query templates
- Test parameter substitution
- Test triple count estimation

**Sub-tasks**:
- [ ] Test basic data generation
- [ ] Test stream generation
- [ ] Test query retrieval
- [ ] Test parameter substitution

## File Structure

```
lib/triple_store/benchmark/
  ├── watdiv.ex          # Data generator
  └── watdiv_queries.ex  # Query templates

test/triple_store/benchmark/
  └── watdiv_test.exs    # Tests

scripts/
  └── run_benchmarks.exs # Updated to include WatDiv
```

## WatDiv Ontology

### Classes
- `User`: Social network users
- `Good`: Products for sale
- `Review`: Product reviews
- `Seller`: Product sellers

### Properties
- `friendship`: User-to-user friendship
- `likes`: User likes a good
- `reviews`: User reviews a good
- `offers`: Seller offers a good
- `purchaseDate`: Date of purchase
- `reviewDate`: Date of review
- `reviewerName`: Name of reviewer
- `reviewTitle`: Title of review
- `reviewText`: Text of review
- `rating`: Numeric rating (1-5)
- `publisher`: Publisher of review/offer
- `price`: Price of offer
- `validFrom`: Offer validity start
- `validTo`: Offer validity end
- `deliveryDate`: Delivery date
- `homepage`: Homepage URL
- `country`: Country code
- `hasGenre`: Genre classification

## Scale Factor

| Scale | Triples (approx) | Description |
|-------|------------------|-------------|
| 1     | ~100K            | Basic testing |
| 10    | ~1M              | Medium dataset |
| 100   | ~10M             | Large dataset |

## Query Categories

### Linear (L1-L5)
Query patterns following linear paths through the graph.

### Star (S1-S7)
Query patterns centered on a single entity with many outgoing edges.

### Snowflake (F1-F5)
Query patterns with branching from multiple entities.

### Complex (C1-C5)
Query patterns combining multiple features.

## Progress

- [x] Create feature branch
- [x] Write working plan
- [x] Implement WatDiv data generator
- [x] Implement WatDiv query templates
- [x] Update benchmark script
- [x] Create tests
- [x] Write summary
- [ ] Commit and merge

## Notes

- WatDiv uses heterogeneous structure: same entity types don't always have same attributes
- Attributes are probabilistic and correlated
- This creates more realistic and challenging query patterns
- Scale factor 1 generates ~100K triples (similar to LUBM scale 1)
