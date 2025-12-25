# Task 4.1.4 Rule Optimization Summary

**Date:** 2025-12-25
**Branch:** feature/4.1.4-rule-optimization

## Overview

Implemented rule optimization for OWL 2 RL reasoning. The `TripleStore.Reasoner.RuleOptimizer` module provides three key optimizations:

1. **Pattern Reordering** - Reorder body patterns by selectivity to minimize intermediate results
2. **Rule Batching** - Group rules that can share intermediate results
3. **Dead Rule Detection** - Identify rules that cannot fire given the current schema

## Key Components

### Pattern Reordering

Patterns are reordered based on selectivity estimates:
- Patterns with bound constants come before all-variable patterns
- Predicate position is weighted as most selective (typical RDF data characteristic)
- Variables bound by earlier patterns increase selectivity of subsequent patterns
- Conditions are interleaved optimally (placed after their variables are bound)

```elixir
# Example: Before optimization
body = [
  {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},         # Low selectivity
  {:pattern, [{:var, "y"}, {:iri, "rdf:type"}, {:var, "c"}]}  # High selectivity
]

# After optimization - high selectivity pattern first
body = [
  {:pattern, [{:var, "y"}, {:iri, "rdf:type"}, {:var, "c"}]},
  {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]}
]
```

### Rule Batching

Rules are grouped by head predicate and shared patterns:

| Batch Type | Description |
|------------|-------------|
| `:same_predicate` | Rules with shared body patterns can reuse intermediate results |
| `:same_head` | Rules with same head structure can use bulk insert |
| `:independent` | Rules without sharing |

### Dead Rule Detection

Rules are identified as "dead" when their required schema features are missing:

| Rule | Required Feature |
|------|------------------|
| prp_trp | transitive_properties not empty |
| prp_symp | symmetric_properties not empty |
| prp_inv1/inv2 | inverse_properties not empty |
| prp_fp | functional_properties not empty |
| prp_ifp | inverse_functional_properties not empty |
| scm_sco, cax_sco | has_subclass = true |
| scm_spo, prp_spo1 | has_subproperty = true |
| prp_dom | has_domain = true |
| prp_rng | has_range = true |
| eq_sym, eq_trans, eq_rep_* | has_sameas = true |
| cls_hv*, cls_svf*, cls_avf | has_restrictions = true |

Specialized rules are also checked against the properties they were specialized for.

## API Functions

```elixir
# Pattern reordering
RuleOptimizer.optimize_rule(rule, opts \\ [])
RuleOptimizer.optimize_rules(rules, opts \\ [])

# Options:
#   data_stats: %{predicate_counts: %{}, total_triples: n}
#   preserve_conditions: boolean (default: false)

# Rule batching
RuleOptimizer.batch_rules(rules)
RuleOptimizer.find_shareable_rules(rules)

# Dead rule detection
RuleOptimizer.find_dead_rules(rules, schema_info)
RuleOptimizer.filter_active_rules(rules, schema_info)
RuleOptimizer.rule_dead?(rule, schema_info)

# Selectivity estimation
RuleOptimizer.estimate_selectivity(pattern, bound_vars, data_stats)
```

## Selectivity Estimation

Selectivity is estimated as follows:

| Term Type | Position | Selectivity |
|-----------|----------|-------------|
| Bound variable | Any | 0.01 (very selective) |
| Constant IRI | Predicate | Uses data stats or 0.001 |
| Constant IRI | Subject/Object | 0.01 |
| Literal | Any | 0.001 (very selective) |
| Unbound variable | Subject | 0.1 |
| Unbound variable | Predicate | 0.01 |
| Unbound variable | Object | 0.1 |

Pattern selectivity is the product of term selectivities.

## Data Statistics Support

The optimizer can use runtime data statistics for better estimates:

```elixir
data_stats = %{
  predicate_counts: %{
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" => 10_000,
    "http://www.w3.org/2000/01/rdf-schema#label" => 100
  },
  total_triples: 100_000
}

optimized = RuleOptimizer.optimize_rule(rule, data_stats: data_stats)
```

## Files Created

- `lib/triple_store/reasoner/rule_optimizer.ex` - RuleOptimizer module (~450 lines)
- `test/triple_store/reasoner/rule_optimizer_test.exs` - 28 unit tests

## Test Results

All 28 tests pass covering:
- Pattern reordering (6 tests)
- Selectivity estimation (4 tests)
- Rule batching (4 tests)
- Dead rule detection (7 tests)
- Edge cases (5 tests)
- Filter and helper functions (2 tests)

## Design Decisions

1. **Multiplicative selectivity** - Term selectivities are multiplied assuming independence
2. **Predicate-first heuristic** - Predicate position given lowest base selectivity (most common in RDF)
3. **Condition interleaving** - Conditions placed as early as possible once variables are bound
4. **Conservative dead detection** - Unknown rules assumed active by default
5. **Specialized rule tracking** - Dead rule detection also works for specialized rules

## Performance Considerations

- Pattern reordering is O(n²) where n = number of patterns (typically small)
- Batch grouping is O(r × p) where r = rules, p = patterns per rule
- Dead rule detection is O(r) where r = number of rules
- All operations are pure and don't require database access

## Integration with RuleCompiler

The optimizer can be used after compilation:

```elixir
{:ok, compiled} = RuleCompiler.compile(ctx, profile: :owl2rl)

# Get and optimize rules
rules = RuleCompiler.get_rules(compiled)
optimized_rules = RuleOptimizer.optimize_rules(rules)

# Remove dead rules
active_rules = RuleOptimizer.filter_active_rules(optimized_rules, compiled.schema_info)

# Batch for efficient evaluation
batches = RuleOptimizer.batch_rules(active_rules)
```

## Next Steps

Task 4.1.5 will implement unit tests for the complete rule compiler subsystem:
- Test rule representation captures patterns correctly
- Test subClassOf rule produces correct inferences
- Test domain/range rules produce type inferences
- Test transitive property rule chains correctly
- Test symmetric property rule generates inverse
- Test sameAs rules propagate equality
- Test rule compilation filters inapplicable rules
- Test rule optimization reorders patterns
