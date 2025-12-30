# Codebase Insight Queries

This directory contains example scripts demonstrating how to use TripleStore to analyze codebases represented as RDF knowledge graphs.

## Prerequisites

Before running these scripts, load the sample data:

```bash
mix run -e '
{:ok, store} = TripleStore.open("./tmp/ash_data")
{:ok, count} = TripleStore.load(store, "examples/ash.ttl")
IO.puts("Loaded #{count} triples")
TripleStore.close(store)
'
```

The `ash.ttl` file contains an RDF representation of the [Ash Framework](https://ash-hq.org/) codebase, including modules, functions, types, and call relationships.

## Available Scripts

### Call Graph Analysis

#### `call_graph_query.exs`
Shows incoming and outgoing function calls for a specific module.

```bash
mix run examples/call_graph_query.exs              # Default: Ash.Changeset
mix run examples/call_graph_query.exs Ash.Query    # Specify module
```

**Use when:** You want to understand how a specific module interacts with the rest of the codebase.

---

### Architectural Analysis

#### `hub_modules.exs`
Identifies the most connected modules in the codebase - modules that are called by many others and call many others.

```bash
mix run examples/hub_modules.exs
```

**Use when:** You need to identify core architectural components, prioritize testing, or assess refactoring risk.

**Key insight:** Hub modules are critical to system stability. Changes ripple outward to many dependents.

---

#### `entry_points.exs`
Finds modules with few or no incoming dependencies - the "edges" of your dependency graph.

```bash
mix run examples/entry_points.exs
```

**Use when:** Onboarding to a new codebase, looking for dead code, or identifying application boundaries.

**Key insight:** Entry points are good starting points for learning a codebase since they have fewer prerequisites.

---

#### `module_clusters.exs`
Groups modules by namespace to reveal domain organization.

```bash
mix run examples/module_clusters.exs
```

**Use when:** Understanding how a codebase is organized, identifying bounded contexts, or planning team ownership.

**Key insight:** Namespace patterns reveal architectural layers and domain boundaries.

---

### API & Type Analysis

#### `api_surface.exs`
Counts public functions per module to measure API complexity.

```bash
mix run examples/api_surface.exs
```

**Use when:** Identifying modules that might be doing too much, prioritizing documentation efforts, or assessing learning curve.

**Key insight:** Modules with 50+ public functions may need refactoring into sub-modules.

---

#### `type_usage.exs`
Shows which type definitions appear across the codebase.

```bash
mix run examples/type_usage.exs
```

**Use when:** Understanding domain modeling, checking type conventions, or identifying shared types.

**Key insight:** The `t` type convention (main struct type) should appear in most modules.

---

### Error Handling

#### `error_patterns.exs`
Maps the error module hierarchy to show how errors are organized.

```bash
mix run examples/error_patterns.exs
```

**Use when:** Debugging, implementing error handling, or understanding what can go wrong.

**Key insight:** Well-organized error hierarchies enable broad rescue clauses (e.g., `rescue Error.Query`).

---

### Change Impact

#### `impact_analysis.exs`
Shows what would be affected if you change a specific module.

```bash
mix run examples/impact_analysis.exs                    # Default: Ash.Changeset
mix run examples/impact_analysis.exs Ash.Resource.Info  # Specify module
```

**Use when:** Planning refactoring, assessing change risk, or determining test scope.

**Key insight:** Modules with many dependents using many functions are risky to change.

---

#### `complexity.exs`
Identifies modules with the most outgoing dependencies.

```bash
mix run examples/complexity.exs
```

**Use when:** Finding modules that might be doing too much, identifying testing challenges, or spotting coupling issues.

**Key insight:** Modules with 100+ outgoing calls often need breaking up or better abstraction.

---

## Understanding the RDF Data Model

The `ash.ttl` file uses an Elixir code ontology with these key concepts:

### Node Types
- `s:Module` - Elixir module
- `s:Function` / `s:PublicFunction` / `s:PrivateFunction` - Functions
- `s:PublicType` - Type definitions (`@type`)
- `core:RemoteCall` / `core:LocalCall` - Call sites

### Key Predicates
- `s:moduleName` - Module's string name
- `s:containsFunction` - Module → Function relationship
- `s:functionName` - Function's string name
- `s:callsFunction` - Call site → Called function
- `s:belongsTo` - Call site → Containing function
- `s:usesModule` / `s:requiresModule` / `s:aliasesModule` - Module dependencies
- `s:containsType` / `s:typeName` - Type definitions
- `s:docstring` - Documentation strings

### Namespace Prefixes
```sparql
PREFIX s: <https://w3id.org/elixir-code/structure#>
PREFIX core: <https://w3id.org/elixir-code/core#>
```

---

## Writing Custom Queries

Use `query_helpers.ex` for common utilities:

```elixir
Code.require_file("query_helpers.ex", __DIR__)

import QueryHelpers

with_store(fn store ->
  {:ok, results} = TripleStore.query(store, """
    PREFIX s: <https://w3id.org/elixir-code/structure#>
    SELECT ?mod ?func WHERE {
      ?m a s:Module .
      ?m s:moduleName ?mod .
      ?m s:containsFunction ?f .
      ?f s:functionName ?func .
    } LIMIT 10
  """)

  Enum.each(results, fn row ->
    IO.puts("#{extract(row["mod"])}.#{extract(row["func"])}")
  end)
end)
```

### Helper Functions

| Function | Purpose |
|----------|---------|
| `extract/1` | Convert RDF term tuples to readable values |
| `short_name/1` | Get last part of dotted module name |
| `extract_caller_module/1` | Parse module from call site URI |
| `with_store/2` | Open store, run function, close store |
| `header/2` | Print formatted section header |
| `separator/0` | Print divider line |
| `pad_num/2` | Right-align numbers |
| `bar/3` | Create ASCII bar chart |

---

## Example SPARQL Queries

### Find all modules
```sparql
PREFIX s: <https://w3id.org/elixir-code/structure#>
SELECT ?name WHERE {
  ?mod a s:Module .
  ?mod s:moduleName ?name .
}
```

### Find functions with docstrings
```sparql
PREFIX s: <https://w3id.org/elixir-code/structure#>
SELECT ?mod ?func ?doc WHERE {
  ?m s:moduleName ?mod .
  ?m s:containsFunction ?f .
  ?f s:functionName ?func .
  ?f s:docstring ?doc .
} LIMIT 20
```

### Find module dependencies
```sparql
PREFIX s: <https://w3id.org/elixir-code/structure#>
SELECT ?from ?to WHERE {
  ?m s:moduleName ?from .
  ?m s:usesModule ?dep .
  ?dep s:moduleName ?to .
}
```

### Find call relationships
```sparql
PREFIX s: <https://w3id.org/elixir-code/structure#>
SELECT ?called_mod ?called_func WHERE {
  ?callsite s:callsFunction ?callee .
  ?callsite s:moduleName ?called_mod .
  ?callsite s:functionName ?called_func .
  FILTER(CONTAINS(STR(?callsite), "Ash.Changeset/"))
}
```

---

## Creating Your Own Code Graph

To analyze your own Elixir project, you'll need to generate an RDF representation. The `ash.ttl` file was created using code analysis tools that extract:

1. Module definitions and their relationships
2. Function signatures and visibility
3. Type specifications
4. Call site analysis (which functions call which)
5. Documentation strings

This data can be generated by walking the AST of compiled modules or using tools like `ex_doc` internals.
