# Working Plan: Section 2.6 - Loader Module Refactoring

## Branch: `feature/section-2.6-loader-refactoring`

## Status: COMPLETED

## Overview

Section 2.6 refactors the Loader module to provide a unified API for both triples and quads. The goal is to make the quad store functionality first-class while maintaining backward compatibility with existing triple-loading code.

**Note**: Many quad features have already been implemented in sections 2.1-2.5. This section focuses on API unification and cleanup.

---

## Part 1: Analysis (COMPLETED)

- [x] Review current Loader module public API
- [x] Identify functions that need graph parameter support
- [x] Review existing quad loading implementations
- [x] Determine backward compatibility requirements

---

## Part 2: Implementation Tasks (COMPLETED)

### 2.6.1 API Updates for Quads

**Module:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.6.1.1 Add optional `graph:` parameter to existing load functions
- [x] 2.6.1.2 Update function specifications for quad types
- [x] 2.6.1.3 Update documentation with graph parameter examples
- [x] 2.6.1.4 Ensure backward compatibility (default to :default graph)

**Functions to update:**
- `load_graph/3` - Add graph parameter ✅
- `load_file/4` - Add graph parameter option ✅
- `load_string/5` - Add graph parameter option ✅
- `load_stream/4` - Add graph parameter option ✅

**API Design:**
```elixir
# Existing functions get graph option
@spec load_file(db_ref(), manager(), Path.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
# New option: :graph (defaults to :default)

@spec load_graph(db_ref(), manager(), RDF.Graph.t() | RDF.Dataset.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
# New option: :target_graph (defaults to :default)
```

### 2.6.2 Documentation Updates

**Module:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.6.2.1 Update moduledoc to mention quad support
- [x] 2.6.2.2 Add examples for loading with named graphs
- [x] 2.6.2.3 Document the graph parameter behavior
- [x] 2.6.2.4 Add migration guide from triples to quads

### 2.6.3 Error Handling

**Module:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.6.3.1 Add `{:error, :invalid_graph_term}` for invalid graph names
- [x] 2.6.3.2 Add `{:error, :graph_not_found}` for operations on missing graphs
- [x] 2.6.3.3 Document error conditions in @moduledoc

---

## Part 3: Tests (COMPLETED)

### Test File: `test/triple_store/loader_refactoring_test.exs` (new file)

- [x] 2.6.4.1 Test load_file with graph option
- [x] 2.6.4.2 Test load_string with graph option
- [x] 2.6.4.3 Test load_graph accepts RDF.Dataset
- [x] 2.6.4.4 Test load_graph with RDF.Graph uses default graph
- [x] 2.6.4.5 Test backward compatibility (no graph option)
- [x] 2.6.4.6 Test invalid_graph_term error
- [x] 2.6.4.7 Test error messages are descriptive

**Test Results: 17 tests, 0 failures**

---

## Dependencies

- `TripleStore.Loader` - Module being refactored
- `TripleStore.Adapter` - For quad conversion
- `TripleStore.QuadOperations` - For quad storage
- `RDF.Dataset` - For quad containers
- `RDF.Graph` - For triple containers (backward compatibility)

---

## Success Criteria

1. ✅ Existing load functions accept graph parameter
2. ✅ Backward compatibility maintained (no graph option = default graph)
3. ✅ load_graph accepts both RDF.Graph and RDF.Dataset
4. ✅ Documentation updated with examples
5. ✅ All tests pass (17 new tests + 123 existing tests)

---

## Notes

- This is a refactoring section, not new features
- Most quad functionality already exists from sections 2.1-2.5
- Focus is on API consistency and developer experience
- Existing tests should continue to pass without modification
- New tests verify the enhanced API
