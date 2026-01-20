defmodule TripleStore.Reasoner.IncrementalQuad do
  @moduledoc """
  Incremental maintenance of materialized inferences for quad store.

  This module provides functions for adding and removing quads while
  maintaining the correctness of derived inferences. When quads are added,
  new inferences are computed incrementally. When quads are removed, the
  Backward/Forward algorithm ensures correct retraction without over-deletion.

  ## Graph-Scoped Incremental Addition

  When new quads are added with `add_quads_with_reasoning/5`, the system:
  1. Filters out quads that already exist in the specified graph
  2. Uses semi-naive evaluation with the new quads as the initial delta
  3. Returns per-graph derivation counts
  4. Respects graph scope (local vs global) and TBox sharing

  ## Two APIs

  This module provides two APIs:

  1. **In-Memory API** (`add_quads_in_memory/5`): Works with
     term-based quads (IRI, literal terms) entirely in memory. Suitable for
     testing and small datasets.

  2. **Database API** (`add_quads_with_reasoning/5`): Works with
     dictionary-encoded ID quads and integrates with the database storage layer.
     Suitable for production use with persistent storage.

  ## Usage

      # Database-backed incremental addition for graph 1
      quads = [{1, alice_id, rdf_type_id, person_id}]
      {:ok, stats} = IncrementalQuad.add_quads_with_reasoning(db, quads, rules, graph_id: 1)

      # With TBox sharing from graph 0
      {:ok, stats} = IncrementalQuad.add_quads_with_reasoning(
        db, quads, rules,
        graph_id: 1,
        tbox_graph_id: 0,
        scope: :local
      )

  ## Performance Considerations

  - Incremental addition is O(|new_derivations|) rather than O(|all_derivations|)
  - For small additions to large graphs, this is dramatically faster
  - For bulk loading, full materialization may be more efficient
  - Graph-local reasoning isolates computations to specific graphs
  """
  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule
  alias TripleStore.Reasoner.SemiNaive

  # Database-related imports for the database API
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.TBoxExtractor

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A quad as RDF terms (IRI, literal, blank node) with graph"
  @type term_quad :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A quad as dictionary-encoded IDs {graph, subject, predicate, object}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "A triple as dictionary-encoded IDs (without graph)"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "A set of term-based quads"
  @type quad_set :: MapSet.t(term_quad())

  @typedoc "A set of term-based triples (for TBox)"
  @type triple_set :: MapSet.t(Rule.term_triple())

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "Statistics from incremental quad addition"
  @type add_stats :: %{
          explicit_added: non_neg_integer(),
          derived_count: non_neg_integer(),
          iterations: non_neg_integer(),
          duration_ms: non_neg_integer()
        }

  @typedoc "Options for incremental quad addition"
  @type add_opts :: [
          graph_id: non_neg_integer(),
          tbox_graph_id: non_neg_integer() | nil,
          scope: :local | :global,
          parallel: boolean(),
          max_concurrency: pos_integer(),
          max_iterations: non_neg_integer(),
          max_facts: non_neg_integer(),
          emit_telemetry: boolean()
        ]

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Adds quads to an in-memory fact set and derives consequences using reasoning rules.

  This function performs incremental materialization in memory by:
  1. Filtering out quads that already exist in the target graph
  2. Loading TBox facts from the configured TBox graph (if any)
  3. Creating a lookup function combining TBox + graph facts
  4. Running semi-naive evaluation with the new quads as the initial delta
  5. Storing derived quads in the target graph

  ## Parameters

  - `new_quads` - List of quads to add (as `{graph, subject, predicate, object}` tuples)
  - `existing` - MapSet of existing facts (may include multiple graphs)
  - `rules` - List of reasoning rules to apply
  - `opts` - Options (see below)

  ## Options

  - `:graph_id` - Target graph ID for new quads (required)
  - `:tbox_graph_id` - Graph ID containing shared TBox (nil = no TBox sharing)
  - `:scope` - Reasoning scope: `:local` (default) or `:global`
  - `:parallel` - Enable parallel rule evaluation. Default: `false`
  - `:max_concurrency` - Maximum parallel tasks. Default: `System.schedulers_online()`
  - `:max_iterations` - Maximum fixpoint iterations. Default: `1000`
  - `:max_facts` - Maximum total facts before stopping. Default: `10_000_000`
  - `:emit_telemetry` - Emit telemetry events. Default: `true`

  ## Returns

  - `{:ok, all_facts, stats}` - All facts (existing + new + derived) and statistics
  - `{:error, reason}` - On failure

  ## Examples

      existing = MapSet.new([
        {1, student_iri, subClassOf, person_iri}
      ])
      new_quads = [{1, alice_iri, rdf_type, student_iri}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = IncrementalQuad.add_quads_in_memory(
        new_quads, existing, rules,
        graph_id: 1
      )
      # all_facts now contains {1, alice, rdf:type, person} (derived)
  """
  @spec add_quads_in_memory([term_quad()], quad_set(), [Rule.t()], add_opts()) ::
          {:ok, quad_set(), add_stats()} | {:error, term()}
  def add_quads_in_memory(new_quads, existing, rules, opts \\ [])

  def add_quads_in_memory([], existing, _rules, _opts) do
    {:ok, existing,
     %{
       explicit_added: 0,
       derived_count: 0,
       iterations: 0,
       duration_ms: 0
     }}
  end

  def add_quads_in_memory(new_quads, existing, rules, opts) when is_list(new_quads) do
    start_time = System.monotonic_time(:millisecond)

    graph_id = Keyword.fetch!(opts, :graph_id)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    scope = Keyword.get(opts, :scope, :local)

    # Build semi-naive options
    semi_naive_opts = [
      parallel: Keyword.get(opts, :parallel, false),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_iterations: Keyword.get(opts, :max_iterations, 1000),
      max_facts: Keyword.get(opts, :max_facts, 10_000_000),
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true),
      scope: scope
    ]

    # Normalize quads to ensure graph_id consistency
    normalized_quads = Enum.map(new_quads, fn
      {g, s, p, o} -> {g || graph_id, s, p, o}
      {s, p, o} -> {graph_id, s, p, o}
    end)

    # Filter out quads that already exist in the target graph
    novel_quads = filter_existing_quads(normalized_quads, existing, graph_id)

    # Combine existing facts with new explicit facts
    combined = Enum.reduce(novel_quads, existing, &MapSet.put(&2, &1))

    # Load TBox if configured
    tbox_facts = load_tbox_facts_in_memory(existing, tbox_graph_id, graph_id)

    # Initial delta is just the new quads (as triples for SemiNaive)
    initial_delta =
      novel_quads
      |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
      |> MapSet.new()

    # Create in-memory lookup and store functions
    {:ok, agent} = Agent.start_link(fn -> combined end)

    # TBox-aware lookup function
    lookup_fn = fn pattern ->
      all_facts = Agent.get(agent, & &1)
      graph_facts = filter_facts_by_graph(all_facts, graph_id)
      facts_with_tbox = MapSet.union(graph_facts, tbox_facts)
      {:ok, match_quad_pattern(pattern, facts_with_tbox)}
    end

    # Store function that adds derived quads with graph_id
    store_fn = fn new_facts ->
      Agent.update(agent, fn current ->
        # Add graph_id to derived triples
        new_quads_with_graph =
          new_facts
          |> MapSet.to_list()
          |> Enum.map(fn {s, p, o} -> {graph_id, s, p, o} end)
          |> MapSet.new()

        MapSet.union(current, new_quads_with_graph)
      end)
      :ok
    end

    try do
      case SemiNaive.materialize(lookup_fn, store_fn, rules, initial_delta, semi_naive_opts) do
        {:ok, semi_naive_stats} ->
          all_facts = Agent.get(agent, & &1)
          duration_ms = System.monotonic_time(:millisecond) - start_time

          # Calculate truly new derived quads by comparing with combined
          truly_derived_count = MapSet.size(all_facts) - MapSet.size(combined)

          stats = %{
            explicit_added: length(novel_quads),
            derived_count: truly_derived_count,
            iterations: semi_naive_stats.iterations,
            duration_ms: duration_ms
          }

          {:ok, all_facts, stats}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(agent)
    end
  end

  # ============================================================================
  # Database API
  # ============================================================================

  @doc """
  Adds quads to the database and derives consequences using reasoning rules.

  This function performs incremental materialization with persistent storage by:
  1. Inserting the new explicit quads into the database
  2. Loading TBox facts from the configured TBox graph (if any)
  3. Creating a TBox-aware lookup function for the target graph
  4. Running semi-naive evaluation with the new quads as the initial delta
  5. Storing any new derivations in the derived column family with graph_id

  ## Parameters

  - `db` - Database reference
  - `quads` - List of `{graph_id, subject_id, predicate_id, object_id}` tuples to add
  - `rules` - List of reasoning rules to apply
  - `opts` - Options (see `add_opts()`)

  ## Options

  - `:graph_id` - Target graph ID for new quads (required for triple inputs)
  - `:tbox_graph_id` - Graph ID containing shared TBox (nil = no TBox sharing)
  - `:scope` - Reasoning scope: `:local` (default) or `:global`
  - `:parallel` - Enable parallel rule evaluation. Default: `false`
  - `:max_concurrency` - Maximum parallel tasks. Default: `System.schedulers_online()`
  - `:max_iterations` - Maximum iterations before stopping. Default: `1000`
  - `:max_facts` - Maximum total facts before stopping. Default: `10_000_000`
  - `:emit_telemetry` - Emit telemetry events. Default: `true`

  ## Returns

  - `{:ok, stats}` - Addition completed with statistics
  - `{:error, reason}` - On failure

  ## Note

  This function requires the rules to work with dictionary-encoded ID triples.
  For typical OWL 2 RL rules that use IRI terms, use the dictionary to convert
  terms to IDs before calling this function.
  """
  @spec add_quads_with_reasoning(db_ref(), [id_quad()], [Rule.t()], add_opts()) ::
          {:ok, add_stats()} | {:error, term()}
  def add_quads_with_reasoning(db, quads, rules, opts \\ [])

  def add_quads_with_reasoning(_db, [], _rules, _opts) do
    {:ok,
     %{
       explicit_added: 0,
       derived_count: 0,
       iterations: 0,
       duration_ms: 0
     }}
  end

  def add_quads_with_reasoning(db, quads, rules, opts) when is_list(quads) do
    start_time = System.monotonic_time(:millisecond)

    # Get target graph_id from first quad or options
    graph_id = determine_graph_id(quads, opts)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    scope = Keyword.get(opts, :scope, :local)

    semi_naive_opts = [
      parallel: Keyword.get(opts, :parallel, false),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_iterations: Keyword.get(opts, :max_iterations, 1000),
      max_facts: Keyword.get(opts, :max_facts, 10_000_000),
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true),
      scope: scope
    ]

    with {:ok, novel_quads} <- filter_existing_db_quads(db, quads, graph_id),
         :ok <- insert_explicit_quads(db, novel_quads),
         {:ok, stats} <-
           run_db_reasoning(db, novel_quads, rules, graph_id, tbox_graph_id, scope, semi_naive_opts) do
      duration_ms = System.monotonic_time(:millisecond) - start_time

      {:ok,
       %{
         explicit_added: length(novel_quads),
         derived_count: stats.total_derived,
         iterations: stats.iterations,
         duration_ms: duration_ms
       }}
    end
  end

  @doc """
  Checks if adding quads would derive any new facts (database version).

  This is a dry-run version that computes what would be derived without
  actually inserting anything into the database.
  """
  @spec preview_quad_additions(db_ref(), [id_quad()], [Rule.t()], keyword()) ::
          {:ok, MapSet.t(id_triple())} | {:error, term()}
  def preview_quad_additions(db, quads, rules, opts) do
    graph_id = determine_graph_id(quads, opts)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)

    # Convert quads to prospective triples
    prospective_triples = MapSet.new(quads, fn {_g, s, p, o} -> {s, p, o} end)

    # Load TBox facts
    tbox_facts = load_tbox_facts_from_db(db, tbox_graph_id, graph_id)

    # Create lookup function combining TBox with graph facts
    lookup_fn = fn pattern ->
      with {:ok, graph_facts} <- QuadIndex.lookup_all_fold(db, graph_id, convert_rule_pattern(pattern)) do
        all_facts = MapSet.union(MapSet.new(graph_facts), tbox_facts)
        {:ok, match_quad_pattern(pattern, all_facts)}
      end
    end

    {:ok, agent} = Agent.start_link(fn -> MapSet.new() end)

    store_fn = fn new_facts ->
      Agent.update(agent, fn existing -> MapSet.union(existing, new_facts) end)
      :ok
    end

    try do
      case SemiNaive.materialize(lookup_fn, store_fn, rules, prospective_triples,
             emit_telemetry: false
           ) do
        {:ok, _stats} ->
          all_derived = Agent.get(agent, & &1)
          new_derivations = MapSet.difference(all_derived, prospective_triples)
          {:ok, new_derivations}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(agent)
    end
  end

  # ============================================================================
  # Private Functions - In-Memory
  # ============================================================================

  defp filter_existing_quads(quads, existing, graph_id) do
    Enum.reject(quads, fn quad ->
      MapSet.member?(existing, normalize_quad(quad, graph_id))
    end)
  end

  defp normalize_quad({g, s, p, o}, _default_g) when is_integer(g), do: {g, s, p, o}
  defp normalize_quad({s, p, o}, default_g), do: {default_g, s, p, o}

  defp filter_facts_by_graph(facts, graph_id) do
    facts
    |> Enum.filter(fn
      {g, _s, _p, _o} when is_integer(g) -> g == graph_id
      {_s, _p, _o} -> true
    end)
    |> Enum.map(fn
      {_g, s, p, o} -> {s, p, o}
      triple -> triple
    end)
    |> MapSet.new()
  end

  defp load_tbox_facts_in_memory(_existing, nil, _graph_id), do: MapSet.new()

  defp load_tbox_facts_in_memory(existing, tbox_graph_id, _graph_id) do
    existing
    |> Enum.filter(fn
      {g, _s, _p, _o} when is_integer(g) -> g == tbox_graph_id
      {_s, _p, _o} -> false
    end)
    |> Enum.map(fn
      {_g, s, p, o} -> {s, p, o}
    end)
    |> MapSet.new()
  end

  defp match_quad_pattern(pattern, facts) do
    PatternMatcher.filter_matching(facts, pattern)
  end

  # ============================================================================
  # Private Functions - Database
  # ============================================================================

  defp determine_graph_id([], opts), do: Keyword.get(opts, :graph_id, 0)

  defp determine_graph_id([{g, _s, _p, _o} | _], opts) when is_integer(g) do
    # Use graph_id from first quad, options can override
    Keyword.get(opts, :graph_id, g)
  end

  defp determine_graph_id(_quads, opts) do
    Keyword.fetch!(opts, :graph_id)
  end

  defp filter_existing_db_quads(db, quads, graph_id) do
    novel =
      Enum.filter(quads, fn quad ->
        case quad_is_novel_db?(db, normalize_quad(quad, graph_id)) do
          {:ok, true} -> true
          {:ok, false} -> false
          {:error, _} -> false
        end
      end)

    {:ok, novel}
  end

  defp quad_is_novel_db?(db, {g, s, p, o}) do
    case QuadOperations.quad_exists?(db, {s, p, o, g}) do
      true ->
        {:ok, false}

      false ->
        case DerivedStore.derived_quad_exists?(db, {g, s, p, o}) do
          {:ok, exists} -> {:ok, not exists}
          error -> error
        end
    end
  end

  defp insert_explicit_quads(_db, []), do: :ok

  defp insert_explicit_quads(db, quads) do
    # Convert from {g, s, p, o} to {s, p, o, g} format for QuadOperations
    normalized_quads = Enum.map(quads, fn {g, s, p, o} -> {s, p, o, g} end)
    QuadOperations.insert_quads(db, normalized_quads, sync: true)
  end

  defp run_db_reasoning(_db, [], _rules, _graph_id, _tbox_graph_id, _scope, _opts) do
    {:ok,
     %{
       iterations: 0,
       total_derived: 0,
       derivations_per_iteration: [],
       duration_ms: 0,
       rules_applied: 0
     }}
  end

  defp run_db_reasoning(db, new_quads, rules, graph_id, tbox_graph_id, _scope, opts) do
    # Load TBox facts if configured
    tbox_facts = load_tbox_facts_from_db(db, tbox_graph_id, graph_id)

    # Create TBox-aware lookup function
    lookup_fn = make_tbox_aware_lookup_fn(db, graph_id, tbox_facts)

    # Create store function that adds graph_id to derived triples
    store_fn = DerivedStore.make_graph_store_fn(db, graph_id)

    # Convert new quads to triples (remove graph_id for SemiNaive)
    new_triples = MapSet.new(new_quads, fn {_g, s, p, o} -> {s, p, o} end)

    SemiNaive.materialize(lookup_fn, store_fn, rules, new_triples, opts)
  end

  defp load_tbox_facts_from_db(_db, nil, _graph_id), do: MapSet.new()

  defp load_tbox_facts_from_db(db, tbox_graph_id, _graph_id) do
    case TBoxExtractor.extract_tbox(db, tbox_graph_id) do
      {:ok, tbox_quads} ->
        tbox_quads
        |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
        |> MapSet.new()

      {:error, _reason} ->
        MapSet.new()
    end
  end

  defp make_tbox_aware_lookup_fn(db, graph_id, tbox_facts) do
    fn pattern ->
      lookup_pattern = convert_rule_pattern(pattern)

      with {:ok, graph_facts} <- QuadIndex.lookup_all_fold(db, graph_id, lookup_pattern) do
        all_facts = MapSet.union(MapSet.new(graph_facts), tbox_facts)
        {:ok, all_facts}
      end
    end
  end

  defp convert_rule_pattern({:pattern, [s, p, o]}) do
    {convert_term(s), convert_term(p), convert_term(o)}
  end

  defp convert_term({:var, _name}), do: :var
  defp convert_term({:const, value}), do: {:bound, value}
  defp convert_term(:var), do: :var
  defp convert_term({:bound, _} = bound), do: bound
  defp convert_term(value), do: {:bound, value}
end
