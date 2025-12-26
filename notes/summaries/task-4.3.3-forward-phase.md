# Task 4.3.3: Forward Phase - Summary

**Date:** 2025-12-26
**Branch:** feature/4.3.3-forward-phase

## Overview

Implemented the forward phase of the Backward/Forward deletion algorithm for incremental reasoning. After the backward phase identifies potentially invalid derived facts, this module attempts to re-derive each fact using alternative justifications. Facts that can be re-derived are kept; those that cannot are deleted.

## Key Implementation Details

### New Module: `TripleStore.Reasoner.ForwardRederive`

Location: `lib/triple_store/reasoner/forward_rederive.ex`

#### Main Functions

1. **`rederive_in_memory/4`** - Attempts to re-derive potentially invalid facts
   - Takes potentially invalid facts, all facts, deleted facts, and rules
   - Returns partition of facts into keep/delete sets with statistics
   - Handles incremental processing with proper binding management

2. **`can_rederive?/3`** - Checks if a single fact can be re-derived
   - Tests all rules for potential derivation
   - Verifies body patterns can be satisfied by valid facts
   - Evaluates conditions on bindings

3. **`partition_invalid/4`** - Convenience wrapper returning `{keep, delete}` tuple
   - Simplifies integration with deletion coordinator

### Algorithm

The forward re-derivation algorithm works by:

1. **Compute Valid Facts**: Start with all facts minus deleted facts
2. **Check Each Fact**: For each potentially invalid fact:
   - Find rules where the fact matches the head pattern
   - Check if body patterns can be satisfied by valid facts
   - Verify all conditions are satisfied
3. **Incremental Update**: As facts are confirmed re-derivable, add them to valid set
4. **Partition Result**: Return keep set (re-derivable) and delete set (not re-derivable)

### Key Design Decisions

1. **Conservative Validity**: A fact is only kept if it can be re-derived from definitely valid facts
2. **Incremental Keep Set**: Facts confirmed as re-derivable become available for subsequent checks
3. **Self-Justification Prevention**: A fact cannot justify itself during re-derivation
4. **Full Pattern Matching**: Complete unification of head and body patterns with bindings

### Statistics Returned

```elixir
%{
  keep: MapSet.t(),           # Facts that can be re-derived
  delete: MapSet.t(),         # Facts that cannot be re-derived
  rederivation_count: non_neg_integer(),  # Number of facts kept
  facts_checked: non_neg_integer()         # Total facts checked
}
```

### Re-derivation Check

A fact `F` can be re-derived if there exists:
- A rule `R` where `F` matches `R`'s head pattern
- Bindings that satisfy all body patterns using only valid facts
- All conditions are satisfied by the bindings

## Test Coverage

**New test file:** `test/triple_store/reasoner/forward_rederive_test.exs`

20 new tests covering:

- Empty inputs and statistics
- Simple re-derivation (alternative paths exist)
- No re-derivation (no alternative paths)
- Mixed results (some facts kept, some deleted)
- Multiple alternative paths
- sameAs symmetry and transitivity re-derivation
- Explicit facts in valid set
- Subclass chain re-derivation
- Multiple rules for same fact
- Edge cases (deleted explicit facts, all facts invalid)

## Example Scenario

Given:
- `alice rdf:type Student` (explicit, being deleted)
- `alice rdf:type GradStudent` (explicit, not deleted)
- `Student rdfs:subClassOf Person` (explicit)
- `GradStudent rdfs:subClassOf Person` (explicit)
- `alice rdf:type Person` (derived, potentially invalid)

The forward phase finds that `alice rdf:type Person` can be re-derived via `GradStudent rdfs:subClassOf Person`, so it should be kept.

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/forward_rederive.ex` | New module |
| `test/triple_store/reasoner/forward_rederive_test.exs` | New test file |

## Test Results

```
20 tests, 0 failures
Total project: 2778 tests, 0 failures
```

## Integration with Backward Phase

The forward phase complements the backward phase (Task 4.3.2):

1. **Backward Phase**: Identifies all potentially invalid derived facts
2. **Forward Phase**: Determines which can be re-derived

Together, they implement sound incremental deletion that:
- Never loses facts that have alternative derivations
- Only deletes facts with no remaining justification
- Handles complex dependency chains correctly

## Next Steps

Task 4.3.3 is complete. The next tasks in Section 4.3 are:

- **4.3.4**: Delete with Reasoning - Coordinate backward and forward phases
  - Integrate BackwardTrace and ForwardRederive
  - Implement `delete_with_reasoning/2` public API
  - Handle explicit vs derived fact deletion

- **4.3.5**: Unit Tests - Additional tests for deletion with reasoning
