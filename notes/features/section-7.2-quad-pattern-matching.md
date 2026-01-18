# Section 7.2: Quad Pattern Matching for Rules

**Status:** PLANNING
**Phase:** 7 - Reasoning with Named Graphs
**Created:** 2025-01-18
**Feature Branch:** `feature/section-7.2-quad-pattern-matching`

## 1. Problem Statement

The existing reasoning rule system operates on triple patterns without awareness of named graphs. Section 7.1 introduced graph-aware reasoning infrastructure (GraphReasoningConfig, GraphScopedReasoner) and basic quad pattern types, but the rule system itself needs to be extended to:

1. **Support quad patterns in rule heads** - Currently, only rule body patterns support quad format
2. **Enable graph-specific rule compilation** - Rules need to be compiled with graph context awareness
3. **Maintain backward compatibility** - Existing triple-based rules must continue to work

### 1.1 Current Limitations

- Rule heads can only be triple patterns (`{:pattern, [s, p, o]}`)
- RuleCompiler doesn't consider graph context when compiling
- No mechanism for TBox axioms to be compiled to specific graphs
- Graph variable bindings in rule bodies are not fully integrated with rule evaluation

### 1.2 Why This Matters

- Enables TBox to be stored in a separate graph from ABox data
- Allows per-graph reasoning profiles where different graphs use different rules
- Supports hybrid reasoning where some graphs participate in global inference
- Foundation for Section 7.3 (Graph-Local Materialization)

## 2. Solution Overview

### 2.1 Design Decision: Extend, Don't Replace

Rather than creating separate "QuadRule" structures, we will extend the existing `Rule` module to support both triple and quad patterns seamlessly:

**Key Principle:** A rule is a rule - whether it operates on triples or quads is a matter of the pattern format, not the rule structure itself.

### 2.2 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Rule Compilation (Enhanced)             │
│  - Takes graph context into account                        │
│  - Compiles TBox rules to designated TBox graph            │
│  - Marks rules with their applicable graph scope           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rule Representation                       │
│  - Body patterns can be triple OR quad                     │
│  - Head patterns can be triple OR quad (NEW)              │
│  - Graph variable bindings supported throughout            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   Pattern Matching (Extended)               │
│  - matches_quad?/2 already exists                          │
│  - unification with graph bindings (ENHANCE)               │
│  - Index pattern conversion for quads (already exists)     │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Backward Compatibility

- Triple-only rules remain the default
- Existing rule definitions continue to work unchanged
- Quad patterns are opt-in via explicit constructors

## 3. Technical Details

### 3.1 File Locations

| File | Current State | Changes Needed |
|------|---------------|----------------|
| `lib/triple_store/reasoner/rule.ex` | Has quad_pattern type, but head limited to triple | Add quad head support, graph context metadata |
| `lib/triple_store/reasoner/pattern_matcher.ex` | Has basic quad matching functions | Enhance unification, graph binding propagation |
| `lib/triple_store/reasoner/rule_compiler.ex` | Triple-only compilation | Add graph context, TBox graph handling |
| `lib/triple_store/reasoner/rules.ex` | Triple-only rule definitions | Add quad-aware rule variants |
| `lib/triple_store/reasoner/semi_naive.ex` | Triple pattern evaluation | Handle quad patterns in rule evaluation |
| `lib/triple_store/reasoner/delta_computation.ex` | Some quad support | Full quad pattern support in unification |

### 3.2 Data Structure Changes

#### Rule Struct Enhancement

```elixir
# Current
defstruct [:name, :body, :head, :description, :profile, :metadata]

# Enhanced (metadata field can carry graph info)
# No struct change needed - use metadata map for:
# - :graph_id - Which graph this rule applies to
# - :scope - :local (single graph) or :global (all graphs)
# - :tbox_rule - Whether this is a TBox axiom rule
```

#### Head Pattern Support

```elixir
# Current: Head is always a triple pattern
head: {:pattern, [subject, predicate, object]}

# New: Head can also be a quad pattern
head: {:quad_pattern, [graph, subject, predicate, object]}
```

### 3.3 Dependencies

- **Elixir 1.18+** - Pattern matching and struct updates
- **Existing modules**:
  - `TripleStore.Reasoner.Rule` - Core rule structure
  - `TripleStore.Reasoner.PatternMatcher` - Pattern matching utilities
  - `TripleStore.Reasoner.RuleCompiler` - Rule compilation
  - `TripleStore.QuadIndex` - Quad index operations (for lookups)

## 4. Success Criteria

### 4.1 Functional Requirements

- [ ] Rules can have quad patterns in both body and head
- [ ] RuleCompiler accepts `graph_id` option
- [ ] TBox axioms can be compiled to a designated graph
- [ ] Graph variables in rule patterns properly bind during evaluation
- [ ] Existing triple-only rules continue to work without modification

### 4.2 Non-Functional Requirements

- No performance regression for triple-only rule evaluation
- Quad pattern matching performance comparable to triple matching
- Memory usage scales linearly with number of graphs

### 4.3 Measurable Outcomes

- All existing tests pass without modification
- New tests for quad pattern rules pass
- Benchmark shows <5% overhead for triple rules with quad extensions present

## 5. Implementation Plan

### 5.1 Task 7.2.1: Rule Pattern Extension

**Goal:** Enable quad patterns in rule heads and add graph context to rules.

#### Subtasks

1. **7.2.1.1** Add `head_quad_pattern/4` constructor to Rule module
2. **7.2.1.2** Extend `instantiate_head/2` to handle quad patterns
3. **7.2.1.3** Add `:graph_id` metadata field support
4. **7.2.1.4** Add `:scope` metadata field support (:local/:global)
5. **7.2.1.5** Default triple rules to `scope: :local, graph_id: :default`

#### Files to Modify
- `lib/triple_store/reasoner/rule.ex`

#### Testing
- Unit tests for quad head pattern construction
- Tests for head instantiation with graph variable
- Tests for default metadata values

### 5.2 Task 7.2.2: Quad Pattern Matching

**Goal:** Ensure pattern matching fully supports quad patterns with graph bindings.

#### Subtasks

1. **7.2.2.1** Verify `matches_quad?/2` handles all graph term types
2. **7.2.2.2** Add `unify_quad_pattern/3` for binding propagation
3. **7.2.2.3** Extend `unify_pattern_with_fact/3` for quad facts
4. **7.2.2.4** Add graph variable tracking in binding maps
5. **7.2.2.5** Handle `:default`, `:all`, and specific graph_id matching

#### Files to Modify
- `lib/triple_store/reasoner/pattern_matcher.ex`
- `lib/triple_store/reasoner/delta_computation.ex`

#### Testing
- Tests for unifying quad patterns with ground quads
- Tests for graph variable binding propagation
- Tests for special graph terms (:default, :all)

### 5.3 Task 7.2.3: Rule Compilation for Quads

**Goal:** Make RuleCompiler graph-aware for TBox/ABox separation.

#### Subtasks

1. **7.2.3.1** Add `graph_id` option to `RuleCompiler.compile/2`
2. **7.2.3.2** Add `tbox_graph` option to `RuleCompiler.compile/2`
3. **7.2.3.3** Mark TBox rules with `tbox_rule: true` metadata
4. **7.2.3.4** Support graph-specific rule specialization
5. **7.2.3.5** Store compiled rules with graph context in persistent_term

#### Files to Modify
- `lib/triple_store/reasoner/rule_compiler.ex`

#### Testing
- Tests for compiling rules to specific graphs
- Tests for TBox vs ABox rule separation
- Tests for graph-specific rule specialization
- Integration tests with GraphScopedReasoner

## 6. Testing Strategy

### 6.1 Unit Tests

**Rule Module Tests**
- Quad head pattern construction
- Head instantiation with graph variables
- Metadata field defaults and updates

**PatternMatcher Tests**
- Quad pattern matching with various graph terms
- Unification with graph variable propagation
- Index pattern conversion for quads

**RuleCompiler Tests**
- Compilation with graph_id option
- TBox graph handling
- Rule specialization with graph context

### 6.2 Integration Tests

- End-to-end reasoning with quad-aware rules
- TBox in separate graph from ABox
- Multi-graph reasoning with graph-specific rules

### 6.3 Property-Based Tests

- Property: Instantiating a quad head with a binding always produces a valid quad
- Property: Unifying two compatible quad patterns produces a consistent binding
- Property: Graph variables maintain consistency across rule evaluation

## 7. Notes and Considerations

### 7.1 Edge Cases

1. **Graph variable in head but not body** - What does this mean?
   - Resolution: Treat as "derive to any matching graph" or error

2. **Mixed triple and quad patterns in same rule** - Should we allow?
   - Resolution: Allow for compatibility, but document as edge case

3. **:all graph in rule head** - Cannot materialize to "all graphs"
   - Resolution: :all only valid in body patterns, not head

### 7.2 Performance Considerations

- Quad pattern matching adds 4th position to every match
- Index lookups must include graph component
- Consider separate rule sets for triple-only vs quad-aware reasoning

### 7.3 Future Work

- **Section 7.3**: Graph-Local Materialization (depends on this)
- **Section 7.4**: Global Materialization (depends on this)
- Rule optimization for quad patterns
- Parallel rule evaluation across graphs

## 8. Implementation Status

**Status:** COMPLETE
**Completed:** 2025-01-18

### Task 7.2.1: Rule Pattern Extension
- [x] 7.2.1.1 Add head_quad_pattern/4 constructor
- [x] 7.2.1.2 Extend instantiate_head/2 for quad patterns
- [x] 7.2.1.3 Add :graph_id metadata support
- [x] 7.2.1.4 Add :scope metadata support
- [x] 7.2.1.5 Default triple rules to local scope

### Task 7.2.2: Quad Pattern Matching
- [x] 7.2.2.1 Verify matches_quad?/2 completeness
- [x] 7.2.2.2 Add unify_quad_pattern/3
- [x] 7.2.2.3 Extend unify_pattern_with_fact/3
- [x] 7.2.2.4 Add graph variable tracking
- [x] 7.2.2.5 Handle special graph terms

### Task 7.2.3: Rule Compilation for Quads
- [x] 7.2.3.1 Add graph_id option to compile/2
- [x] 7.2.3.2 Add tbox_graph option to compile/2
- [x] 7.2.3.3 Mark TBox rules with metadata
- [x] 7.2.3.4 Graph-specific rule specialization
- [x] 7.2.3.5 Store rules with graph context

### Tests
- [x] Unit tests (38 tests, all passing)
- [ ] Integration tests (deferred to Section 7.3)

---

**Last Updated:** 2025-01-18
**Status:** COMPLETE
