# Task 4.4.2: Property Hierarchy - Summary

**Date:** 2025-12-26
**Branch:** feature/4.4.2-property-hierarchy

## Overview

Extended the TBox caching system to support property hierarchies, enabling O(1) lookup of superproperty and subproperty relationships. The implementation also extracts and caches property characteristics (transitive, symmetric, functional, inverse functional) and inverse property pairs.

## Implementation

### Extended Module: `TripleStore.Reasoner.TBoxCache`

**Location:** `lib/triple_store/reasoner/tbox_cache.ex`

#### New Types

| Type | Description |
|------|-------------|
| `property_characteristics` | Map containing sets of transitive, symmetric, functional, inverse functional properties and inverse pairs |
| `property_hierarchy` | Cache structure with superproperty/subproperty maps, characteristics, property count, version, and stats |

#### New In-Memory API Functions

| Function | Description |
|----------|-------------|
| `compute_property_hierarchy_in_memory/1` | Computes hierarchy and characteristics from a fact set |
| `superproperties_from/2` | Returns all superproperties from a precomputed hierarchy |
| `subproperties_from/2` | Returns all subproperties from a precomputed hierarchy |
| `transitive_property?/2` | Checks if property is owl:TransitiveProperty |
| `symmetric_property?/2` | Checks if property is owl:SymmetricProperty |
| `functional_property?/2` | Checks if property is owl:FunctionalProperty |
| `inverse_functional_property?/2` | Checks if property is owl:InverseFunctionalProperty |
| `inverse_of/2` | Returns inverse property if declared |
| `transitive_properties/1` | Returns all transitive properties |
| `symmetric_properties/1` | Returns all symmetric properties |
| `functional_properties/1` | Returns all functional properties |
| `inverse_functional_properties/1` | Returns all inverse functional properties |
| `inverse_pairs/1` | Returns all inverse property pairs as a map |

#### New Persistent Term API Functions

| Function | Description |
|----------|-------------|
| `compute_and_store_property_hierarchy/2` | Computes and stores in `:persistent_term` |
| `superproperties/2` | Returns superproperties from cached hierarchy |
| `subproperties/2` | Returns subproperties from cached hierarchy |

### Algorithm

1. **Extract Relationships**: Filter facts for `rdfs:subPropertyOf` triples
2. **Build Direct Map**: Create map from property to its direct superproperties
3. **Compute Transitive Closure**: Iteratively extend superproperty sets until fixpoint
4. **Invert Hierarchy**: Derive subproperty map from superproperty map
5. **Extract Characteristics**: Find properties declared as transitive, symmetric, functional, inverse functional
6. **Extract Inverse Pairs**: Find owl:inverseOf declarations and store bidirectionally
7. **Store**: Save in `:persistent_term` with version and stats

### Property Characteristics Extracted

| Characteristic | OWL Type |
|---------------|----------|
| Transitive | `owl:TransitiveProperty` |
| Symmetric | `owl:SymmetricProperty` |
| Functional | `owl:FunctionalProperty` |
| Inverse Functional | `owl:InverseFunctionalProperty` |
| Inverse Pairs | `owl:inverseOf` (bidirectional) |

### Key Design Decisions

- **Reuses Class Hierarchy Algorithm**: Same transitive closure algorithm for property hierarchy
- **Bidirectional Inverse Pairs**: If p1 inverseOf p2, both p1->p2 and p2->p1 are stored
- **Combined Statistics**: Stats include property count, relationship count, and counts per characteristic type
- **Zero-Copy Access**: Uses `:persistent_term` for efficient cross-process access

## Test Coverage

**Location:** `test/triple_store/reasoner/tbox_cache_test.exs`

| Test Category | Test Count |
|--------------|------------|
| Class Hierarchy (existing) | 30 |
| Property Hierarchy Basic Computation | 7 |
| Property Characteristics | 8 |
| Property Query Functions | 4 |
| Property Persistent Term Storage | 6 |
| Property Cache Management | 4 |
| Property Edge Cases | 5 |
| **Total** | **64** |

### New Scenarios Tested

- Empty property facts
- Direct superproperty relationships
- Transitive superproperty closure
- Multiple superproperties (diamond inheritance)
- Transitive property extraction
- Symmetric property extraction
- Functional property extraction
- Inverse functional property extraction
- Inverse property pairs (bidirectional)
- Multiple inverse pairs
- Properties with multiple characteristics
- Characteristic counts in stats
- Large linear property hierarchies (100 properties)
- Wide property hierarchies (50 subproperties)
- Cycles in property hierarchy
- Blank node properties
- Combined hierarchy and characteristics

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/tbox_cache.ex` | Extended with property hierarchy support (+270 lines) |
| `test/triple_store/reasoner/tbox_cache_test.exs` | Added 32 property hierarchy tests (+560 lines) |

## Test Results

```
62 tests, 0 failures
```

## Usage Example

```elixir
# In-memory computation
facts = MapSet.new([
  {{:iri, "hasChild"}, {:iri, "rdfs:subPropertyOf"}, {:iri, "hasDescendant"}},
  {{:iri, "hasDescendant"}, {:iri, "rdfs:subPropertyOf"}, {:iri, "hasRelative"}},
  {{:iri, "hasChild"}, {:iri, "rdf:type"}, {:iri, "owl:TransitiveProperty"}},
  {{:iri, "hasChild"}, {:iri, "owl:inverseOf"}, {:iri, "hasParent"}}
])

{:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

# Query hierarchy
TBoxCache.superproperties_from(hierarchy, {:iri, "hasChild"})
# => MapSet<[{:iri, "hasDescendant"}, {:iri, "hasRelative"}]>

# Check characteristics
TBoxCache.transitive_property?(hierarchy, {:iri, "hasChild"})
# => true

TBoxCache.inverse_of(hierarchy, {:iri, "hasChild"})
# => {:iri, "hasParent"}

# With persistent storage
{:ok, _stats} = TBoxCache.compute_and_store_property_hierarchy(facts, :my_ontology)
TBoxCache.superproperties({:iri, "hasChild"}, :my_ontology)
# => MapSet<[{:iri, "hasDescendant"}, {:iri, "hasRelative"}]>
```

## Next Steps

- **Task 4.4.3**: Handle TBox updates requiring hierarchy recomputation
- **Task 4.4.4**: Unit tests for the complete TBox caching system
