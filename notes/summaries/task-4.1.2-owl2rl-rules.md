# Task 4.1.2 OWL 2 RL Rules Summary

**Date:** 2025-12-25
**Branch:** feature/4.1.2-owl2rl-rules

## Overview

Implemented the complete set of OWL 2 RL rules for forward-chaining inference. The `TripleStore.Reasoner.Rules` module provides 23 rules organized by category: RDFS schema rules, property characteristic rules, equality rules, and class restriction rules.

## Rules Implemented

### RDFS Schema Rules (6 rules, profile: :rdfs)

| Rule | Name | Description |
|------|------|-------------|
| scm-sco | `scm_sco` | rdfs:subClassOf transitivity |
| scm-spo | `scm_spo` | rdfs:subPropertyOf transitivity |
| cax-sco | `cax_sco` | Class membership through subclass |
| prp-spo1 | `prp_spo1` | Property inheritance through subproperty |
| prp-dom | `prp_dom` | Property domain inference |
| prp-rng | `prp_rng` | Property range inference |

### OWL 2 RL Property Rules (6 rules, profile: :owl2rl)

| Rule | Name | Description |
|------|------|-------------|
| prp-trp | `prp_trp` | Transitive property inference |
| prp-symp | `prp_symp` | Symmetric property inference |
| prp-inv1 | `prp_inv1` | Inverse property (forward direction) |
| prp-inv2 | `prp_inv2` | Inverse property (backward direction) |
| prp-fp | `prp_fp` | Functional property derives equality |
| prp-ifp | `prp_ifp` | Inverse functional property derives equality |

### OWL 2 RL Equality Rules (6 rules, profile: :owl2rl)

| Rule | Name | Description |
|------|------|-------------|
| eq-ref | `eq_ref` | owl:sameAs reflexivity |
| eq-sym | `eq_sym` | owl:sameAs symmetry |
| eq-trans | `eq_trans` | owl:sameAs transitivity |
| eq-rep-s | `eq_rep_s` | Equality replacement in subject position |
| eq-rep-p | `eq_rep_p` | Equality replacement in predicate position |
| eq-rep-o | `eq_rep_o` | Equality replacement in object position |

### OWL 2 RL Class Restriction Rules (5 rules, profile: :owl2rl)

| Rule | Name | Description |
|------|------|-------------|
| cls-hv1 | `cls_hv1` | hasValue restriction infers property value |
| cls-hv2 | `cls_hv2` | hasValue restriction infers class membership |
| cls-svf1 | `cls_svf1` | someValuesFrom restriction from typed value |
| cls-svf2 | `cls_svf2` | someValuesFrom owl:Thing restriction |
| cls-avf | `cls_avf` | allValuesFrom restriction infers value type |

## API Functions

```elixir
# Get rules by profile
Rules.rdfs_rules()           # 6 RDFS rules
Rules.owl2rl_rules()         # 17 OWL 2 RL rules (excludes RDFS)
Rules.all_rules()            # 23 total rules

# Get rules for a reasoning profile
Rules.rules_for_profile(:rdfs)    # RDFS only
Rules.rules_for_profile(:owl2rl)  # RDFS + OWL 2 RL
Rules.rules_for_profile(:all)     # All rules

# Lookup individual rules
Rules.get_rule(:cax_sco)     # Get rule by name
Rules.rule_names()           # List all rule names
```

## Example Rules

### cax-sco: Class membership through subclass
```
(?x rdf:type ?c1), (?c1 rdfs:subClassOf ?c2) -> (?x rdf:type ?c2)
```

### prp-trp: Transitive property
```
(?p rdf:type owl:TransitiveProperty), (?x ?p ?y), (?y ?p ?z) -> (?x ?p ?z)
```

### prp-fp: Functional property derives equality
```
(?p rdf:type owl:FunctionalProperty), (?x ?p ?y1), (?x ?p ?y2), ?y1 != ?y2
  -> (?y1 owl:sameAs ?y2)
```

### cls-avf: allValuesFrom restriction
```
(?x rdf:type ?r), (?x ?p ?y), (?r owl:allValuesFrom ?c), (?r owl:onProperty ?p)
  -> (?y rdf:type ?c)
```

## Files Created

- `lib/triple_store/reasoner/rules.ex` - OWL 2 RL rule definitions
- `test/triple_store/reasoner/rules_test.exs` - 64 unit tests

## Test Results

All 64 tests pass covering:
- API functions (7 tests)
- RDFS schema rules (12 tests)
- Property characteristic rules (19 tests)
- Equality rules (11 tests)
- Class restriction rules (10 tests)
- Rule validation (5 tests)
- Rule instantiation (3 tests)

## Design Decisions

1. **Profile tagging** - Rules tagged with `:rdfs` or `:owl2rl` for selective application
2. **Conditions for equality** - prp-fp and prp-ifp use `not_equal` condition to avoid trivial inferences
3. **Comprehensive equality** - Full eq-rep-* rules for subject, predicate, and object positions
4. **owl:Thing handling** - cls-svf2 specifically handles someValuesFrom with owl:Thing filler

## Rule Statistics

- Total rules: 23
- With conditions: 2 (prp-fp, prp-ifp)
- RDFS profile: 6 rules
- OWL 2 RL profile: 17 rules
- All rules are safe (head variables in body)
- All rules have descriptions and profiles

## Next Steps

Task 4.1.3 will implement rule compilation:
- Compile ontology axioms to applicable rules
- Filter rules based on ontology content
- Specialize rules with ontology constants
- Store compiled rules in :persistent_term
