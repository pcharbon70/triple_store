# Reasoning: Triple Schema

`TripleStore.materialize/2` defaults to `profile: :owl2rl` and
`scope: :local`. The current local facade path reads explicit triple indices,
computes an in-memory semi-naive fixpoint, returns statistics, and discards the
returned fact set. It does not persist inferred triples in the `derived` column
family.

~~~elixir
{:ok, stats} = TripleStore.materialize(store, profile: :rdfs)
~~~

Facade profile names accepted without additional rule options are `:rdfs`,
`:owl2rl`, and `:none`. A successful call demonstrates completion of the
implemented rule set, not complete W3C standards conformance.

Fully ground premises are checked through the configured lookup provider.
Missing premises produce no derivation; lookup errors return
`{:error, {:lookup_failed, reason}}` rather than a successful fixpoint.

## Status

~~~elixir
{:ok, status} = TripleStore.reasoning_status(store)
~~~

With no saved status, this returns an initialized default. Do not infer
persisted materialization merely from local `materialize/2` statistics.

For persistent quad workflows, `materialize_graph/3`, `materialize_graphs/3`,
and `materialize_all/2` route through `GraphScopedReasoner`. Use the
[quad reasoning guide](../quads/05-reasoning.md).

Direct `insert/2`, `delete/2`, and `update/2` do not automatically perform a
complete reasoning-maintenance cycle.
