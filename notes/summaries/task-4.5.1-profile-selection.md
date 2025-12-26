# Task 4.5.1: Profile Selection - Summary

**Date:** 2025-12-26
**Branch:** feature/4.5.1-profile-selection

## Overview

Implemented reasoning profile selection for OWL 2 RL inference. Profiles determine which inference rules are active during materialization, allowing users to choose appropriate reasoning levels based on their ontology requirements and performance needs.

## Implementation

### New Module: `TripleStore.Reasoner.ReasoningProfile`

**Location:** `lib/triple_store/reasoner/reasoning_profile.ex`

A comprehensive module for managing reasoning profiles with the following features:

#### Built-in Profiles

| Profile | Description | Rule Count |
|---------|-------------|------------|
| `:none` | No inference (empty rule set) | 0 |
| `:rdfs` | RDFS semantics (subclass/property hierarchies, domain/range) | 6 |
| `:owl2rl` | Full OWL 2 RL reasoning (all rules) | 23 |
| `:custom` | User-selected rules | Variable |

#### RDFS Profile Rules

| Rule | Description |
|------|-------------|
| `scm_sco` | rdfs:subClassOf transitivity |
| `scm_spo` | rdfs:subPropertyOf transitivity |
| `cax_sco` | Class membership through subclass |
| `prp_spo1` | Property inheritance through subproperty |
| `prp_dom` | Property domain inference |
| `prp_rng` | Property range inference |

#### OWL 2 RL Additional Rules (on top of RDFS)

| Category | Rules |
|----------|-------|
| Property Characteristics | `prp_trp`, `prp_symp`, `prp_inv1`, `prp_inv2`, `prp_fp`, `prp_ifp` |
| Equality | `eq_ref`, `eq_sym`, `eq_trans`, `eq_rep_s`, `eq_rep_p`, `eq_rep_o` |
| Restrictions | `cls_hv1`, `cls_hv2`, `cls_svf1`, `cls_svf2`, `cls_avf` |

### Public API

| Function | Description |
|----------|-------------|
| `rules_for/2` | Get rules for a profile with options |
| `rules_for!/2` | Same as above, raises on error |
| `info/1` | Get information about a profile |
| `available_rules/0` | List all available rule names |
| `rules_by_category/0` | Get rules grouped by category |
| `rules_for_category/1` | Get rules for a specific category |
| `from_categories/2` | Build custom profile from categories |
| `suggest_profile/1` | Suggest appropriate profile for schema |
| `valid_profile?/1` | Check if profile name is valid |
| `profile_names/0` | List all valid profile names |

### Profile Options

| Option | Description |
|--------|-------------|
| `:rules` | List of rule names (required for `:custom`) |
| `:exclude` | List of rule names to exclude from profile |

### Updated Module: `TripleStore.Reasoner.RuleCompiler`

**Location:** `lib/triple_store/reasoner/rule_compiler.ex`

Updated to use `ReasoningProfile` for profile-based rule selection:

```elixir
# RDFS profile
{:ok, compiled} = RuleCompiler.compile(ctx, profile: :rdfs)

# OWL 2 RL profile (default)
{:ok, compiled} = RuleCompiler.compile(ctx, profile: :owl2rl)

# Custom profile with specific rules
{:ok, compiled} = RuleCompiler.compile(ctx,
  profile: :custom,
  rules: [:scm_sco, :cax_sco, :prp_trp]
)

# OWL 2 RL minus equality rules
{:ok, compiled} = RuleCompiler.compile(ctx,
  profile: :owl2rl,
  exclude: [:eq_ref, :eq_sym, :eq_trans]
)
```

### Key Design Decisions

- **Profile as Configuration**: Profiles are configuration, not runtime state
- **Exclusion Support**: Any profile can have rules excluded via `:exclude` option
- **Category-Based Composition**: Custom profiles can be built from rule categories
- **Profile Suggestion**: `suggest_profile/1` analyzes schema to recommend appropriate profile
- **Integration**: Works with existing `RuleCompiler` and `SemiNaive` modules

## Test Coverage

**Location:** `test/triple_store/reasoner/reasoning_profile_test.exs`

| Test Category | Test Count |
|--------------|------------|
| Profile Names | 2 |
| RDFS Profile | 4 |
| OWL 2 RL Profile | 2 |
| None Profile | 1 |
| Custom Profile | 5 |
| Exclude Option | 3 |
| rules_for!/2 | 2 |
| Profile Info | 4 |
| Available Rules | 1 |
| Rules by Category | 3 |
| From Categories | 5 |
| Profile Suggestion | 6 |
| Invalid Profile | 1 |
| **Total** | **42** |

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/reasoning_profile.ex` | New module (+320 lines) |
| `lib/triple_store/reasoner/rule_compiler.ex` | Updated to use ReasoningProfile |
| `test/triple_store/reasoner/reasoning_profile_test.exs` | New tests (+280 lines) |

## Test Results

```
637 tests, 0 failures
```

All existing tests continue to pass with the new profile system.

## Usage Examples

```elixir
# Get profile information
info = ReasoningProfile.info(:rdfs)
# => %{name: :rdfs, rule_count: 6, ...}

# Get rules for a profile
{:ok, rules} = ReasoningProfile.rules_for(:rdfs)

# Build custom profile from categories
{:ok, rules} = ReasoningProfile.from_categories([:rdfs, :property_characteristics])

# Suggest profile based on schema
profile = ReasoningProfile.suggest_profile(schema_info)
# => :rdfs or :owl2rl

# Compile with profile
{:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)
```

## Next Steps

- **Task 4.5.2**: Reasoning Mode (materialized, hybrid, query-time)
- **Task 4.5.3**: Reasoning Status (derived count, last materialization time)
- **Task 4.5.4**: Unit tests for reasoning configuration
