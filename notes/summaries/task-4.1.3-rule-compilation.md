# Task 4.1.3 Rule Compilation Summary

**Date:** 2025-12-25
**Branch:** feature/4.1.3-rule-compilation

## Overview

Implemented rule compilation for OWL 2 RL reasoning. The `TripleStore.Reasoner.RuleCompiler` module compiles generic rules from the Rules module into optimized, ontology-specific rules by:
1. Extracting schema information from the ontology
2. Filtering rules to those applicable given the schema
3. Specializing rules by binding ontology constants
4. Storing compiled rules in `:persistent_term` for zero-copy access

## Key Components

### Schema Information Extraction

The compiler extracts relevant schema features from the ontology context:

```elixir
%{
  has_subclass: boolean(),        # rdfs:subClassOf assertions exist
  has_subproperty: boolean(),     # rdfs:subPropertyOf assertions exist
  has_domain: boolean(),          # rdfs:domain assertions exist
  has_range: boolean(),           # rdfs:range assertions exist
  transitive_properties: [iri],   # owl:TransitiveProperty instances
  symmetric_properties: [iri],    # owl:SymmetricProperty instances
  inverse_properties: [{p1, p2}], # owl:inverseOf pairs
  functional_properties: [iri],   # owl:FunctionalProperty instances
  inverse_functional: [iri],      # owl:InverseFunctionalProperty instances
  has_same_as: boolean(),         # owl:sameAs assertions exist
  has_restrictions: boolean()     # owl:Restriction instances exist
}
```

### Rule Filtering

Rules are filtered based on schema presence:

| Rule | Required Schema Feature |
|------|------------------------|
| eq_ref | Always included (reflexivity) |
| scm_sco, cax_sco | has_subclass |
| scm_spo, prp_spo1 | has_subproperty |
| prp_dom | has_domain |
| prp_rng | has_range |
| prp_trp | transitive_properties not empty |
| prp_symp | symmetric_properties not empty |
| prp_inv1, prp_inv2 | inverse_properties not empty |
| prp_fp | functional_properties not empty |
| prp_ifp | inverse_functional not empty |
| eq_sym, eq_trans, eq_rep_* | has_same_as |
| cls_hv1, cls_hv2, cls_svf1, cls_svf2, cls_avf | has_restrictions |

### Rule Specialization

Generic rules are specialized by binding ontology constants. For example, a transitive property rule:

**Generic (prp_trp):**
```
(?p rdf:type owl:TransitiveProperty), (?x ?p ?y), (?y ?p ?z) -> (?x ?p ?z)
```

**Specialized for :ancestorOf:**
```
(?x :ancestorOf ?y), (?y :ancestorOf ?z) -> (?x :ancestorOf ?z)
```

Specialization reduces the number of body patterns and eliminates the need to check property types at runtime.

### Persistent Term Storage

Compiled rules are stored in `:persistent_term` for zero-copy access:

```elixir
RuleCompiler.store(:my_ontology, compiled)
{:ok, compiled} = RuleCompiler.load(:my_ontology)
RuleCompiler.remove(:my_ontology)
RuleCompiler.exists?(:my_ontology)
```

## API Functions

```elixir
# Main compilation entry points
RuleCompiler.compile(ctx, opts \\ [])
RuleCompiler.compile_with_schema(schema_info, opts \\ [])

# Options
#   profile: :rdfs | :owl2rl (default :owl2rl)
#   specialize: boolean (default true)

# Rule access
RuleCompiler.get_rules(compiled)           # All rules (specialized + generic)
RuleCompiler.get_specialized_rules(compiled)  # Specialized rules only
RuleCompiler.get_generic_rules(compiled)      # Generic rules only

# Persistent term operations
RuleCompiler.store(key, compiled)
RuleCompiler.load(key)
RuleCompiler.remove(key)
RuleCompiler.exists?(key)

# Utility
RuleCompiler.empty_schema_info()
RuleCompiler.filter_applicable_rules(rules, schema_info)
RuleCompiler.specialize_rules(rules, schema_info)
```

## Compiled Structure

```elixir
%{
  rules: [%Rule{}, ...],           # Applicable generic rules
  specialized_rules: [%Rule{}, ...], # Specialized rules with bound constants
  schema_info: %{...},             # Extracted schema information
  profile: :owl2rl,                # Reasoning profile used
  compiled_at: ~U[2025-12-25 ...]  # Compilation timestamp
}
```

## Files Created/Modified

- `lib/triple_store/reasoner/rule_compiler.ex` - New RuleCompiler module (350 lines)
- `test/triple_store/reasoner/rule_compiler_test.exs` - 32 unit tests

## Test Results

All 32 tests pass covering:
- Empty schema compilation (1 test)
- Schema feature compilation (11 tests)
- Rule specialization (8 tests)
- Rule access functions (4 tests)
- Persistent term storage (4 tests)
- Filter and structure tests (4 tests)

## Design Decisions

1. **Dual rule sets** - Both generic and specialized rules are retained; the evaluator can choose which to use based on workload characteristics

2. **Schema-driven filtering** - Rules are filtered at compile time based on schema presence, avoiding runtime checks for inapplicable rules

3. **Property-level specialization** - Rules are specialized per-property (e.g., one specialized prp_trp for each transitive property)

4. **Condition handling** - Rule conditions (not_equal, is_iri, etc.) are properly substituted during specialization

5. **eq_ref always included** - The reflexivity rule is always applicable regardless of schema content

## Performance Considerations

- Schema extraction is O(n) where n is triple count
- Rule filtering is O(r) where r is number of rules
- Specialization creates at most O(r × p) rules where p is max properties per type
- `:persistent_term` provides O(1) access with no copying

## Next Steps

Task 4.1.4 will implement rule optimization:
- Reorder body patterns by selectivity
- Identify rules that can be batched together
- Detect rules that cannot fire given current schema
