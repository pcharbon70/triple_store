# ADR-002: Triple vs Quad API Coexistence

**Status**: Accepted
**Date**: 2026-01-20
**Deciders**: Engineering Team
**Related**: Phase 7 (Reasoning with Named Graphs), DerivedStore module

---

## Context and Problem Statement

The triple store was originally designed for RDF triples (subject, predicate, object). With Phase 7 (Reasoning with Named Graphs), we extended to support quads (graph, subject, predicate, object).

This creates an API design challenge:

1. **Backward compatibility**: Existing code uses triple-based APIs
2. **Feature parity**: New quad-aware code needs same capabilities
3. **Type safety**: Mixing triples and quads can cause bugs
4. **Documentation**: Users need clear guidance on which API to use

### The Problem

```elixir
# Triple API (legacy)
DerivedStore.insert_derived(db, {subject, predicate, object})
DerivedStore.lookup_derived(db, pattern)

# Quad API (new)
DerivedStore.insert_derived_quads(db, [{graph, subject, predicate, object}])
DerivedStore.lookup_derived_quads(db, graph_id, pattern)

# Problem: Which one should I use?
# Problem: Can I mix them?
# Problem: What happens if I insert triples and query as quads?
```

## Decision

We maintain **parallel triple and quad APIs** with clear separation:

### API Naming Convention

- **Triple functions**: Operate on `{s, p, O}` triples, no graph parameter
  - `insert_derived/2`
  - `lookup_derived/2`
  - `delete_derived/2`
  - `clear_derived/1`

- **Quad functions**: Operate on `{g, s, p, o}` quads, require graph_id
  - `insert_derived_quads/2`
  - `insert_derived_quad_single/2`
  - `lookup_derived_quads/3`
  - `delete_derived_quads/2`
  - `clear_derived_quads/2`

### Module Documentation

The `DerivedStore` module documentation clearly explains when to use each API:

```elixir
@moduledoc """
Storage layer for derived (inferred) facts.

## Choosing the Right API

### Triple Store Mode (Legacy)
Use these functions for backward compatibility with triple-only code:
- `insert_derived/2` - Insert a single derived triple
- `lookup_derived/2` - Find derived triples matching a pattern
- `delete_derived/2` - Delete specific derived triples
- `clear_derived/1` - Clear all derived triples

**Use when**: Migrating from triple store, not using named graphs

### Quad Store Mode (Graph-Aware)
Use these functions for quad-aware reasoning with named graphs:
- `insert_derived_quads/2` - Insert multiple derived quads
- `insert_derived_quad_single/2` - Insert a single derived quad
- `lookup_derived_quads/3` - Find derived quads in a specific graph
- `delete_derived_quads/2` - Delete derived quads
- `clear_derived_quads/2` - Clear derived quads for a graph

**Use when**: Using named graphs, multi-tenant datasets, graph-scoped reasoning

## Important

Do not mix triple and quad APIs in the same application unless you have
specific migration requirements. The two APIs operate on different
column families and have no cross-awareness.
"""
```

### Implementation Pattern

Both APIs delegate to the same underlying storage but with different graph handling:

```elixir
# Triple API: Uses default graph (0)
def insert_derived(db, {s, p, o}) do
  insert_derived_quads(db, [{0, s, p, o}])
end

# Quad API: Explicit graph
def insert_derived_quads(db, quads) when is_list(quads) do
  # Batch insert with explicit graph IDs
end

def insert_derived_quad_single(db, {g, s, p, o}) do
  # Single insert with explicit graph ID
end
```

## Rationale

### Why Keep Both APIs

1. **Backward compatibility**: Existing code continues to work without changes
2. **Incremental migration**: Users can migrate gradually from triple to quad
3. **Mental model matching**: Triple API matches RDF triple concepts; quad API matches named graphs
4. **Performance optimization**: Triple API can skip graph parameter passing

### Why Separate Functions (Not Polymorphism)

1. **Type safety**: Separate functions prevent accidentally passing triples to quad code
2. **Documentation clarity**: Function names make the data type explicit
3. **Dialyzer compatibility**: Separate specs are easier for type checking
4. **No runtime dispatch**: Direct function calls are faster than pattern matching on arity

### Why Not Use a Single Polymorphic API

```elixir
# Rejected: Single function with pattern matching
def insert_derived(db, triple_or_quad)

# Problems:
# 1. Not obvious from call site what type is being used
# 2. Easy to accidentally pass wrong type
# 3. Documentation is less clear
# 4. Dialyzer specs become complex
```

## Implementation Details

### Type Specifications

```elixir
# Triple API types
@type triple :: {subject_id(), predicate_id(), object_id()}
@type triple_pattern :: {triple_term(), triple_term(), triple_term()}

# Quad API types
@type id_quad :: {graph_id(), subject_id(), predicate_id(), object_id()}
@type quad_pattern :: {quad_term(), quad_term(), quad_term(), quad_term()}

# Separate specs for each function
@spec insert_derived(db_ref(), triple) :: :ok | {:error, term()}
@spec insert_derived_quads(db_ref(), [id_quad()]) :: :ok | {:error, term()}
```

### Storage Mapping

Both APIs use the same RocksDB column family (`:derived`) with consistent key encoding:

```
Key format: <<graph_id::64-big, subject_id::64-big, predicate_id::64-big, object_id::64-big>>

Triple API insert: Always uses graph_id = 0
Quad API insert: Uses explicit graph_id
```

This means:
- Triple inserts are queryable via Quad API (as graph 0)
- Quad inserts are NOT queryable via Triple API (except graph 0)

## Migration Path

For users migrating from triple to quad API:

### Phase 1: Audit Usage

```bash
# Find all triple API usage
grep -r "insert_derived\|lookup_derived\|delete_derived" lib/
```

### Phase 2: Add Graph Parameter

```elixir
# Before
{:ok, stats} = Reasoner.materialize(db, rules)

# After (if using named graphs)
{:ok, config} = ReasoningConfig.new(profile: :owl2rl, scope: :local)
{:ok, stats} = GraphScopedReasoner.materialize_graph(db, graph_id: 1, config: config)
```

### Phase 3: Switch API Calls

```elixir
# Before (triple API)
DerivedStore.insert_derived(db, {s, p, o})
results = DerivedStore.lookup_derived(db, pattern)

# After (quad API)
DerivedStore.insert_derived_quads(db, [{g, s, p, o}])
results = DerivedStore.lookup_derived_quads(db, g, pattern)
```

### Phase 4: Remove Triple API

Once migration is complete, triple API usage can be removed from your codebase.

## Alternatives Considered

### Alternative 1: Single Unified API

```elixir
def insert_derived(db, data) when is_tuple(data) do
  case tuple_size(data) do
    3 -> insert_as_triple(db, data)
    4 -> insert_as_quad(db, data)
  end
end
```

**Rejected because**:
- Type safety is reduced (runtime vs compile-time)
- Function calls don't indicate data type
- Harder to document clearly
- Dialyzer specs become complex

### Alternative 2: Use Protocol for Polymorphism

```elixir
defprotocol DerivedStore.Operations do
  def insert(db, data)
  def lookup(db, pattern)
end

defimpl DerivedStore.Operations, for: Triple do ...
defimpl DerivedStore.Operations, for: Quad do ...
```

**Rejected because**:
- Over-engineering for simple use case
- Protocols have overhead
- Still requires explicit type wrappers
- More complex than separate functions

### Alternative 3: Break Compatibility, Quad Only

Remove triple API entirely and require all users to migrate.

**Rejected because**:
- Breaking change for existing users
- Large migration burden
- Triple API is still useful for simple use cases
- No technical benefit to forcing migration

## Consequences

### Positive

1. **Clear separation**: Function names indicate data type
2. **Type safety**: Dialyzer can catch type mismatches
3. **Documentation**: Easy to document when to use each API
4. **Backward compatibility**: Existing code works unchanged
5. **Performance**: Direct function calls, no runtime dispatch

### Negative

1. **API surface area**: More functions to learn and document
2. **Potential confusion**: Users must understand which API to use
3. **Implementation maintenance**: Changes must be mirrored across both APIs
4. **Testing burden**: More functions to test

### Mitigation Strategies

1. **Clear module documentation**: Section explaining when to use each API
2. **Deprecation warnings**: Triple API functions emit warnings in non-legacy contexts
3. **Migration guide**: Documented step-by-step migration process
4. **Consistent naming**: `{function}_quads` suffix makes quad API obvious

## Future Considerations

1. **Triple API deprecation**: May add deprecation warnings in future versions
2. **Type aliases**: Could use `%Triple{}` and `%Quad{}` structs for better type safety
3. **API consolidation**: If usage patterns emerge, may add convenience functions
4. **Performance monitoring**: Track which API is used more to guide decisions

## References

- DerivedStore module: `lib/triple_store/reasoner/derived_store.ex`
- Phase 7 planning document: `notes/features/phase-7-review-fixes.md`
- RDF 1.1 Named Graphs spec: https://www.w3.org/TR/rdf11-datasets/

## Revisions

| Date | Change | Author |
|------|--------|--------|
| 2026-01-20 | Initial ADR | Engineering Team |
