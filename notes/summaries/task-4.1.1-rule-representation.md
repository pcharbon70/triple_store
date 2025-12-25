# Task 4.1.1 Rule Representation Summary

**Date:** 2025-12-25
**Branch:** feature/4.1.1-rule-representation

## Overview

Implemented the rule representation structure for OWL 2 RL forward-chaining inference. The `TripleStore.Reasoner.Rule` module provides a Datalog-style representation of reasoning rules with pattern matching and condition support.

## Implementation Details

### Rule Struct

```elixir
%Rule{
  name: atom(),           # Unique identifier (e.g., :cax_sco, :prp_trp)
  body: [body_element()], # List of patterns and conditions
  head: pattern(),        # Triple pattern to derive
  description: String.t() | nil,
  profile: :rdfs | :owl2rl | :custom | nil
}
```

### Term Types

- `{:var, name}` - Variable reference
- `{:iri, uri}` - IRI constant
- `{:literal, :simple, value}` - Simple literal
- `{:literal, :typed, value, datatype}` - Typed literal
- `{:literal, :lang, value, lang}` - Language-tagged literal

### Pattern Structure

```elixir
{:pattern, [subject_term, predicate_term, object_term]}
```

### Condition Types

- `{:not_equal, term1, term2}` - Terms must differ
- `{:is_iri, term}` - Term must be an IRI
- `{:is_blank, term}` - Term must be a blank node
- `{:is_literal, term}` - Term must be a literal
- `{:bound, term}` - Variable must be bound

## Key Features

### Constructor Functions

- `Rule.new/4` - Create a new rule with optional description/profile
- `Rule.var/1`, `Rule.iri/1`, `Rule.literal/1-2`, `Rule.lang_literal/2` - Term constructors
- `Rule.pattern/3` - Pattern constructor
- `Rule.not_equal/2`, `Rule.is_iri/1`, etc. - Condition constructors

### Common IRI Helpers

Predefined IRI helpers for frequently used RDF/RDFS/OWL terms:
- `rdf_type()`, `rdfs_subClassOf()`, `rdfs_subPropertyOf()`
- `rdfs_domain()`, `rdfs_range()`
- `owl_sameAs()`, `owl_TransitiveProperty()`, `owl_SymmetricProperty()`
- `owl_inverseOf()`, `owl_hasValue()`, `owl_onProperty()`
- `owl_someValuesFrom()`, `owl_allValuesFrom()`

### Analysis Functions

- `variables/1` - Extract all variables from rule
- `body_variables/1`, `head_variables/1` - Extract from specific parts
- `pattern_count/1`, `condition_count/1` - Count body elements
- `safe?/1` - Check if all head variables appear in body
- `body_patterns/1`, `body_conditions/1` - Filter body elements

### Binding Operations

- `substitute/2` - Apply binding to a term
- `substitute_pattern/2` - Apply binding to a pattern
- `ground?/1` - Check if pattern has no variables
- `evaluate_condition/2` - Evaluate a condition against binding
- `evaluate_conditions/2` - Evaluate all rule conditions

## Example: OWL 2 RL Rules

```elixir
# cax-sco: Class membership through subclass
Rule.new(:cax_sco,
  [
    Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c1")),
    Rule.pattern(Rule.var("c1"), Rule.rdfs_subClassOf(), Rule.var("c2"))
  ],
  Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c2")),
  description: "Class membership through subclass",
  profile: :owl2rl
)

# prp-trp: Transitive property
Rule.new(:prp_trp,
  [
    Rule.pattern(Rule.var("p"), Rule.rdf_type(), Rule.owl_TransitiveProperty()),
    Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
    Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("z"))
  ],
  Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("z")),
  description: "Transitive property inference",
  profile: :owl2rl
)
```

## Files Created

- `lib/triple_store/reasoner/rule.ex` - Rule representation module
- `test/triple_store/reasoner/rule_test.exs` - 68 unit tests

## Test Results

All 68 tests pass covering:
- Term constructors (5 tests)
- Pattern constructors (2 tests)
- Condition constructors (5 tests)
- Common IRI helpers (9 tests)
- Rule constructor (2 tests)
- Variable extraction (5 tests)
- Analysis functions (6 tests)
- Substitution (4 tests)
- Ground checking (2 tests)
- Condition evaluation (16 tests)
- Real OWL 2 RL rule examples (12 tests)

## Design Decisions

1. **Tuple-based representation** - Consistent with existing SPARQL algebra patterns
2. **Explicit type tags** - `{:var, ...}`, `{:iri, ...}`, `{:literal, ...}` for easy pattern matching
3. **Separated conditions from patterns** - Conditions filter bindings without database access
4. **Profile tagging** - Allows filtering rules by reasoning profile (RDFS, OWL 2 RL)
5. **Safety checking** - `safe?/1` ensures all head variables are bound by body

## Next Steps

Task 4.1.2 will implement the standard OWL 2 RL rule set using this representation:
- rdfs:subClassOf transitivity (scm-sco)
- Class membership through subclass (cax-sco)
- Property domain/range (prp-dom, prp-rng)
- Transitive, symmetric, inverse properties
- owl:sameAs transitivity and symmetry
- Restriction classes (hasValue, someValuesFrom, allValuesFrom)
