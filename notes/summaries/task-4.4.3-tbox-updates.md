# Task 4.4.3: TBox Updates - Summary

**Date:** 2025-12-26
**Branch:** feature/4.4.3-tbox-updates

## Overview

Implemented TBox update detection and cache invalidation/recomputation for handling schema changes. The system can now detect when triples affecting class hierarchies, property hierarchies, or property characteristics are added or removed, and automatically invalidate and recompute cached hierarchies as needed.

## Implementation

### Extended Module: `TripleStore.Reasoner.TBoxCache`

**Location:** `lib/triple_store/reasoner/tbox_cache.ex`

#### New TBox Update Detection Functions

| Function | Description |
|----------|-------------|
| `tbox_predicates/0` | Returns set of TBox-modifying predicates |
| `property_characteristic_types/0` | Returns set of property characteristic OWL types |
| `tbox_triple?/1` | Checks if a single triple is TBox-modifying |
| `contains_tbox_triples?/1` | Checks if any triples in collection are TBox-modifying |
| `filter_tbox_triples/1` | Filters TBox-modifying triples from collection |
| `categorize_tbox_triples/1` | Categorizes triples by what they affect |

#### New Cache Invalidation Functions

| Function | Description |
|----------|-------------|
| `invalidate_affected/2` | Invalidates caches affected by given triples |
| `recompute_hierarchies/2` | Recomputes both class and property hierarchies |
| `handle_tbox_update/4` | Main entry point for handling TBox modifications |
| `needs_recomputation?/1` | Lightweight check for what would need recomputation |

### TBox-Modifying Predicates Detected

| Predicate | Affects |
|-----------|---------|
| `rdfs:subClassOf` | Class hierarchy |
| `rdfs:subPropertyOf` | Property hierarchy |
| `rdf:type` (with OWL property type) | Property characteristics |
| `owl:inverseOf` | Property hierarchy (inverse pairs) |
| `rdfs:domain` | Domain constraints |
| `rdfs:range` | Range constraints |

### Property Characteristic Types Detected

| Type | Effect |
|------|--------|
| `owl:TransitiveProperty` | Transitive property characteristic |
| `owl:SymmetricProperty` | Symmetric property characteristic |
| `owl:FunctionalProperty` | Functional property characteristic |
| `owl:InverseFunctionalProperty` | Inverse functional property characteristic |

### Categorization Output

The `categorize_tbox_triples/1` function returns a map with:
- `:class_hierarchy` - rdfs:subClassOf triples
- `:property_hierarchy` - rdfs:subPropertyOf triples
- `:property_characteristics` - rdf:type with OWL property types
- `:inverse_properties` - owl:inverseOf triples
- `:domain_range` - rdfs:domain and rdfs:range triples

### Key Design Decisions

- **Selective Invalidation**: Only invalidates caches that are actually affected
- **Optional Recomputation**: Can invalidate without recomputing (recompute: false)
- **Lightweight Check**: `needs_recomputation?/1` for checking without side effects
- **Main Entry Point**: `handle_tbox_update/4` coordinates detection, invalidation, and recomputation

## Test Coverage

**Location:** `test/triple_store/reasoner/tbox_cache_test.exs`

| Test Category | Test Count |
|--------------|------------|
| Class Hierarchy (existing) | 30 |
| Property Hierarchy (existing) | 32 |
| TBox Update Detection | 19 |
| Cache Invalidation | 4 |
| Recomputation | 5 |
| Needs Recomputation | 5 |
| **Total** | **95** |

### New Scenarios Tested

- Detecting rdfs:subClassOf as TBox triple
- Detecting rdfs:subPropertyOf as TBox triple
- Detecting owl:inverseOf as TBox triple
- Detecting property characteristic declarations
- Distinguishing instance data from TBox data
- Filtering TBox triples from mixed collections
- Categorizing triples by type
- Invalidating class hierarchy on subClassOf changes
- Invalidating property hierarchy on characteristic changes
- Recomputing hierarchies after invalidation
- Skipping recomputation with option
- Handling mixed TBox and instance updates

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/tbox_cache.ex` | Added TBox update detection (+375 lines) |
| `test/triple_store/reasoner/tbox_cache_test.exs` | Added 33 TBox update tests (+400 lines) |

## Test Results

```
95 tests, 0 failures
```

## Usage Example

```elixir
# Check if triples would affect TBox
triples = [
  {{:iri, "Student"}, {:iri, "rdfs:subClassOf"}, {:iri, "Person"}},
  {{:iri, "alice"}, {:iri, "rdf:type"}, {:iri, "Student"}}
]

# Quick check
needs = TBoxCache.needs_recomputation?(triples)
# => %{class_hierarchy: true, property_hierarchy: false, any: true}

# Filter to just TBox triples
tbox_only = TBoxCache.filter_tbox_triples(triples)
# => [{{:iri, "Student"}, {:iri, "rdfs:subClassOf"}, {:iri, "Person"}}]

# Handle update with automatic recomputation
current_facts = MapSet.new(triples)
{:ok, result} = TBoxCache.handle_tbox_update(triples, current_facts, :my_ontology)
# => %{
#      tbox_modified: true,
#      invalidated: %{class_hierarchy: true, property_hierarchy: false},
#      recomputed: %{class: %{class_count: 2}, property: %{...}}
#    }

# Handle update without recomputation
{:ok, result} = TBoxCache.handle_tbox_update(triples, current_facts, :my_ontology, recompute: false)
# => %{tbox_modified: true, invalidated: %{...}, recomputed: nil}
```

## Next Steps

- **Task 4.4.4**: Unit tests for the complete TBox caching system (already substantially covered)
- **Section 4.5**: Reasoning Configuration (profile selection, reasoning mode)
