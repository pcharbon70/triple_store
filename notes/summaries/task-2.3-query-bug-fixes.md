# Task 2.3: Query Bug Fixes - Summary

**Date:** 2026-01-02

## Overview

Fixed two BSBM query bugs that caused Q5 and Q11 to fail:

1. **Q5**: Typed literal mismatch (`"Product1"^^xsd:string` vs plain `"Product1"`)
2. **Q11**: URI fragment escaping issue (`countries\#` vs `countries#`)

## 2.3.1 Q5 Literal Matching Fix

### Problem

Q5 query used a typed literal `"Product{product}"^^xsd:string` to search for products by label, but the BSBM data generator creates labels as plain literals using `RDF.literal("Product#{product_id}")`.

**Before:**
```sparql
?product rdfs:label "Product1"^^xsd:string .
```

**Data generated:**
```elixir
{product_uri, rdfs("label"), RDF.literal("Product#{product_id}")}
# Creates plain literal without xsd:string type
```

### Solution

Changed Q5 query to use plain literal matching:

**After:**
```sparql
?product rdfs:label "Product1" .
```

### Files Modified

| File | Change |
|------|--------|
| `lib/triple_store/benchmark/bsbm_queries.ex:285` | Removed `^^xsd:string` type annotation |

## 2.3.2 Q11 URI Escaping Fix

### Problem

Q11 query had an escaped hash character `\#` in the country URI, which produced an invalid URI pattern.

**Before:**
```sparql
?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries\#US> .
```

The default country parameter was also `"#US"` which would have created a double-hash issue once fixed.

### Solution

1. Added `@hash "#"` module attribute to handle Elixir string interpolation
2. Used `#{@hash}` in the URI template
3. Changed default country from `"#US"` to `"US"`

**After:**
```sparql
?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
```

### Files Modified

| File | Lines | Change |
|------|-------|--------|
| `lib/triple_store/benchmark/bsbm_queries.ex:50` | Added `@hash "#"` module attribute |
| `lib/triple_store/benchmark/bsbm_queries.ex:438` | Changed URI to use `#{@hash}` |
| `lib/triple_store/benchmark/bsbm_queries.ex:498` | Changed default country from `"#US"` to `"US"` |

## Test Coverage

Added 7 new tests to `bsbm_queries_test.exs`:

### Q5 Literal Fix Tests
| Test | Description |
|------|-------------|
| Q5 uses plain literal without type annotation | Verifies no `^^xsd:string` |
| Q5 substitutes product number correctly | Verifies `"Product42"` format |
| Q5 matches data generation format | Verifies SPARQL matches RDF.literal format |

### Q11 URI Fix Tests
| Test | Description |
|------|-------------|
| Q11 country URI has proper hash fragment | Verifies `countries#US` format |
| Q11 substitutes country code correctly | Verifies country substitution |
| Q11 default country is US without hash prefix | Verifies no double-hash |
| Q11 URI is well-formed | Verifies complete URI format |

## Test Results

```
37 tests, 0 failures
```

All BSBM query tests pass with the fixes applied.

## Impact

With these fixes:
- **Q5** can now find products by label (was returning no results)
- **Q11** can now filter offers by country (was failing with malformed URI)

Both queries should now execute correctly and return expected results.

## Next Steps

**Section 2.4: Integration Tests** is the next upcoming task:
- 2.4.1: Query Correctness Tests (verify all BSBM queries return correct results)
- 2.4.2: Result Validation Tests (validate counts and ordering)
- 2.4.3: Performance Regression Tests (ensure queries meet latency targets)
