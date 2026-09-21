# Reasoning Engine

The reasoner implements forward-chaining RDFS and OWL 2 RL profiles. Rule
definitions, compilation, semi-naive evaluation, derived storage, provenance,
incremental maintenance, rederivation, and graph scope are separate modules
under `TripleStore.Reasoner`.

## Compilation and evaluation

`RuleCompiler.compile/2` extracts schema information from a lookup context and
builds applicable generic and specialized rules. `compile_with_schema/2` accepts
precomputed schema information. Compiled rule sets can be stored in
`:persistent_term` through `store/2` and removed through `remove/1`.

`SemiNaive.materialize/5` accepts lookup and store callbacks.
`materialize_in_memory/3` returns the closure and statistics without persistence.
`materialize_parallel/5` evaluates eligible work concurrently. Fully ground
premises still use the configured lookup provider; lookup failures abort with a
tagged error.

## Facade behavior

~~~elixir
{:ok, stats} = TripleStore.materialize(store, profile: :rdfs)
{:ok, status} = TripleStore.reasoning_status(store)
~~~

The default facade path uses local scope, reads triple indices, calls the
in-memory evaluator, returns statistics, and discards the returned fact set. It
does not persist inferred triples and is not schema-neutral. Do not infer
persistence from a successful statistics result.

## Persistent and incremental reasoning

`DerivedStore` persists inferred facts separately from explicit indices.
`Incremental.add_with_reasoning/4` derives consequences of additions, while
deletion support uses provenance and rederivation modules to decide what remains
supported. Direct loading does not automatically invoke incremental reasoning.

Persistent derivation records retain their existing unversioned map encoding.
Rule identifiers may be legacy atoms or nonempty binaries. Provenance readers
safe-decode and validate the entire key and record, returning
`{:error, {:corrupt_provenance, reason}}` for malformed, unsafe, or unsupported
data. Explanation and delete-with-reasoning propagate that result before
changing explicit or derived facts. Valid existing records require no migration.

Triple derived APIs use `{s, p, o}`. Quad derived APIs use graph-first
`{g, s, p, o}`. This differs from `QuadOperations`, which uses
`{s, p, o, g}`.

## Graph scope

`GraphScopedReasoner` supplies `materialize_graph/2`,
`materialize_graphs/2`, `materialize_all/2`, and `materialize_hybrid/2` for
quad stores. Local scope reasons within graphs. Global and hybrid modes may make
schema data visible across graph boundaries according to their configuration.
For `:per_graph_cf` global materialization, derived keys are canonical GSPO keys
stored under graph ID `0`.

Use `ReasoningConfig` presets only as configuration inputs; confirm the selected
scope and derived-storage strategy at each call site.
