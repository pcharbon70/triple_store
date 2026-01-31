defmodule TripleStore.Reasoner.GraphScopedReasoner do
  @moduledoc """
  Graph-aware reasoning operations for quad store.

  This module provides reasoning functionality that is aware of named graphs,
  supporting three reasoning scopes:
  - **Local**: Each graph reasons independently (default)
  - **Global**: All quads participate in a single inference closure
  - **Hybrid**: Per-graph configuration with mixed local/global reasoning

  ## Graph-Local Reasoning

  In graph-local mode (the default), each graph is materialized independently.
  Inferences stay in the source graph, and there is no cross-graph inference.

      {:ok, stats} = GraphScopedReasoner.materialize_graph(db,
        graph_id: 1,
        config: config
      )

  ## Global Reasoning

  In global mode, all graphs participate in a single inference closure.
  Derived quads are stored either in the same graph as premises or in a
  designated inference graph.

      {:ok, stats} = GraphScopedReasoner.materialize_all(db,
        config: global_config
      )

  ## Hybrid Reasoning

  Hybrid mode allows per-graph configuration, where some graphs use local
  reasoning and others participate in global reasoning.

      {:ok, per_graph_stats} = GraphScopedReasoner.materialize_hybrid(db,
        config: hybrid_config
      )

  ## Storage Strategy

  Derived quads can be stored in two ways:
  - `:self` - Store in same graph as explicit facts (default)
  - `:separate` - Store in a separate inference graph

  ## TBox Sharing

  For efficiency, multiple graphs can share a single TBox (schema graph).
  This avoids redundant schema processing while maintaining data isolation.

  ## Usage Example

      # Create reasoning configuration with local scope
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        mode: :materialized,
        scope: :local
      )

      # Materialize a specific graph
      {:ok, stats} = GraphScopedReasoner.materialize_graph(db,
        graph_id: 1,
        config: config
      )

      # Check graph reasoning status
      {:ok, status} = GraphReasoningStatus.load(:graph_1)
      count = status.derived_count
      gid = status.graph_id
      IO.puts("Derived \#{count} facts for graph \#{gid}")
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadIndex

  alias TripleStore.Reasoner.{
    GraphReasoningConfig,
    GraphReasoningStatus,
    ReasoningConfig,
    SemiNaive,
    TBoxExtractor,
    Telemetry
  }

  require Logger

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Database reference"
  @type db_ref :: ErlangAdapter.db_ref()

  @typedoc "Graph ID"
  @type graph_id :: non_neg_integer()

  @typedoc "ID triple {subject, predicate, object}"
  @type id_triple :: {integer(), integer(), integer()}

  @typedoc "ID quad {graph, subject, predicate, object}"
  @type id_quad :: {integer(), integer(), integer(), integer()}

  @typedoc "Pattern element for matching"
  @type pattern_element :: :var | {:var, atom()} | :bound | {:bound, non_neg_integer()} | non_neg_integer()

  @typedoc "Triple pattern for SPO matching"
  @type pattern :: {:pattern, [pattern_element()]}

  @typedoc "Quad pattern including graph position"
  @type quad_pattern :: {:quad_pattern, [pattern_element()]}

  @typedoc "Union type for all pattern types"
  @type pattern_union :: pattern() | quad_pattern()

  @typedoc "Materialization statistics"
  @type materialization_stats :: %{
          iterations: non_neg_integer(),
          total_derived: non_neg_integer(),
          duration_ms: non_neg_integer(),
          rules_applied: non_neg_integer(),
          graph_id: graph_id()
        }

  # ============================================================================
  # Public API - Graph-Local Reasoning
  # ============================================================================

  @doc """
  Materializes inferences for a single graph (graph-local reasoning).

  In graph-local mode, only facts within the specified graph participate
  in reasoning. All derived facts are stored in the same graph.

  ## Parameters

  - `db` - Database reference
  - `opts` - Options:
    - `:graph_id` - Graph ID to materialize (required)
    - `:config` - Global reasoning configuration (required)
    - `:rules` - Custom rules (optional, defaults to config rules)
    - `:emit_telemetry` - Emit telemetry events (default: true)
    - `:parallel` - Enable parallel rule evaluation (default: false)

  ## Returns

  - `{:ok, stats}` - Materialization completed with statistics
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = GraphScopedReasoner.materialize_graph(db,
        graph_id: 1,
        config: config
      )

      {:ok, stats} = GraphScopedReasoner.materialize_graph(db,
        graph_id: 1,
        config: config,
        parallel: true
      )
  """
  @spec materialize_graph(db_ref(), keyword()) ::
          {:ok, materialization_stats()} | {:error, term()}
  def materialize_graph(db, opts) do
    with {:ok, graph_id} <- validate_graph_id(opts),
         {:ok, config} <- validate_config(opts),
         {:ok, graph_config} <- resolve_graph_config(config, graph_id),
         :ok <- check_graph_participation(graph_config) do
      do_materialize_graph(db, graph_id, config, graph_config, opts)
    end
  end

  @doc """
  Materializes inferences for multiple graphs independently.

  Each graph is materialized separately, optionally in parallel.
  This is more efficient than calling `materialize_graph/2` multiple times
  when parallel execution is enabled.

  ## Parameters

  - `db` - Database reference
  - `opts` - Options:
    - `:graph_ids` - List of graph IDs to materialize (required)
    - `:config` - Global reasoning configuration (required)
    - `:parallel` - Enable parallel graph materialization (default: true)

  ## Returns

  - `{:ok, stats_map}` - Map of graph_id to statistics
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = GraphScopedReasoner.materialize_graphs(db,
        graph_ids: [1, 2, 3],
        config: config,
        parallel: true
      )
  """
  @spec materialize_graphs(db_ref(), keyword()) ::
          {:ok, %{graph_id() => materialization_stats()}} | {:error, term()}
  def materialize_graphs(db, opts) do
    with {:ok, graph_ids} <- validate_graph_ids(opts),
         {:ok, config} <- validate_config(opts) do
      parallel = Keyword.get(opts, :parallel, true)

      if parallel and length(graph_ids) > 1 do
        materialize_graphs_parallel(db, graph_ids, config, opts)
      else
        materialize_graphs_sequential(db, graph_ids, config, opts)
      end
    end
  end

  # ============================================================================
  # Public API - Global Reasoning
  # ============================================================================

  @doc """
  Materializes inferences across all graphs (global reasoning).

  In global mode, all quads from all graphs participate in a single
  inference closure. Derived quads are stored according to the configured
  strategy (same graph as premises or separate inference graph).

  ## Parameters

  - `db` - Database reference
  - `opts` - Options:
    - `:config` - Global reasoning configuration (required)
    - `:emit_telemetry` - Emit telemetry events (default: true)
    - `:parallel` - Enable parallel rule evaluation (default: false)

  ## Returns

  - `{:ok, stats}` - Materialization completed with statistics
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = GraphScopedReasoner.materialize_all(db,
        config: global_config
      )
  """
  @spec materialize_all(db_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  def materialize_all(db, opts) do
    with {:ok, config} <- validate_config(opts),
         :ok <- verify_global_scope(config) do
      do_materialize_all(db, config, opts)
    end
  end

  # ============================================================================
  # Public API - Hybrid Reasoning
  # ============================================================================

  @doc """
  Materializes inferences with hybrid (per-graph) reasoning configuration.

  Each graph uses its own configuration from the global config's graph_configs map.
  Graphs with local scope are materialized independently, while graphs with
  global scope participate in a joint materialization.

  ## Parameters

  - `db` - Database reference
  - `opts` - Options:
    - `:config` - Global reasoning configuration with graph_configs (required)
    - `:emit_telemetry` - Emit telemetry events (default: true)

  ## Returns

  - `{:ok, stats_map}` - Map with :local and :global statistics
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = GraphScopedReasoner.materialize_hybrid(db,
        config: hybrid_config
      )
  """
  @spec materialize_hybrid(db_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  def materialize_hybrid(db, opts) do
    with {:ok, config} <- validate_config(opts),
         :ok <- verify_hybrid_scope(config) do
      do_materialize_hybrid(db, config, opts)
    end
  end

  # ============================================================================
  # Private Functions - Graph-Local Implementation
  # ============================================================================

  defp do_materialize_graph(db, graph_id, config, graph_config, opts) do
    start_time = System.monotonic_time(:millisecond)

    # Get initial facts from this graph only
    {:ok, initial_facts} = load_graph_facts(db, graph_id)

    # Get rules to apply
    rules = Keyword.get(opts, :rules, get_rules_for_graph(config, graph_config))

    # Check if we should load TBox from a separate graph
    tbox_graph_id = ReasoningConfig.tbox_graph(config)
    tbox_facts = load_tbox_facts(db, tbox_graph_id, graph_id)

    # Create TBox-aware lookup and graph-scoped store functions
    lookup_fn = make_tbox_aware_lookup_fn(db, graph_id, tbox_facts)
    store_fn = make_graph_store_fn(db, graph_id)

    # Run semi-naive materialization
    semi_naive_opts = [
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true),
      parallel: Keyword.get(opts, :parallel, false),
      validate_rules: false
    ]

    case SemiNaive.materialize(lookup_fn, store_fn, rules, initial_facts, semi_naive_opts) do
      {:ok, semi_naive_stats} ->
        # Update graph reasoning status
        duration_ms = System.monotonic_time(:millisecond) - start_time

        stats = %{
          iterations: semi_naive_stats.iterations,
          total_derived: semi_naive_stats.total_derived,
          duration_ms: duration_ms,
          rules_applied: semi_naive_stats.rules_applied,
          graph_id: graph_id,
          tbox_graph: tbox_graph_id
        }

        update_graph_status_after_materialization(db, graph_id, graph_config, stats)

        {:ok, stats}

      {:error, _reason} = error ->
        # Record error in status
        record_graph_error(db, graph_id, error)
        error
    end
  end

  # Load TBox facts from the designated TBox graph
  # If TBox graph is the same as target graph, don't duplicate facts
  defp load_tbox_facts(_db, tbox_graph_id, target_graph_id)
       when tbox_graph_id == target_graph_id do
    # TBox is in the same graph, facts already loaded as initial_facts
    MapSet.new()
  end

  defp load_tbox_facts(db, tbox_graph_id, target_graph_id) do
    # Emit telemetry event for TBox extraction start
    Telemetry.emit_tbox_extract_start(target_graph_id, tbox_graph_id)
    start_time = System.monotonic_time()

    case TBoxExtractor.extract_tbox(db, tbox_graph_id) do
      {:ok, tbox_quads} ->
        duration = System.monotonic_time() - start_time
        tbox_fact_count = MapSet.size(tbox_quads)

        # Emit success telemetry event
        Telemetry.emit_tbox_extract_stop(
          target_graph_id,
          tbox_graph_id,
          tbox_fact_count,
          duration
        )

        Logger.debug(
          "TBox extraction succeeded: graph=#{target_graph_id}, tbox_graph=#{tbox_graph_id}, facts=#{tbox_fact_count}"
        )

        # Convert TBox quads to triples for reasoning
        Enum.into(tbox_quads, MapSet.new(), fn {_g, s, p, o} -> {s, p, o} end)

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        # Emit error telemetry event
        Telemetry.emit_tbox_extract_error(target_graph_id, tbox_graph_id, reason, duration)

        # Log warning (not error, since we continue without TBox)
        Logger.warning(
          "TBox extraction failed: graph=#{target_graph_id}, tbox_graph=#{tbox_graph_id}, reason=#{inspect(reason)}. Continuing without TBox."
        )

        # If TBox extraction fails, continue without TBox
        MapSet.new()
    end
  end

  defp materialize_graphs_sequential(db, graph_ids, _config, opts) do
    Enum.reduce_while(graph_ids, {:ok, %{}}, fn graph_id, {:ok, acc} ->
      case materialize_graph(db, Keyword.put(opts, :graph_id, graph_id)) do
        {:ok, stats} -> {:cont, {:ok, Map.put(acc, graph_id, stats)}}
        {:error, reason} -> {:halt, {:error, {graph_id, reason}}}
      end
    end)
  end

  defp materialize_graphs_parallel(db, graph_ids, _config, opts) do
    # Materialize graphs in parallel using Task.async_stream
    max_concurrency = min(length(graph_ids), System.schedulers_online())

    graph_ids
    |> Task.async_stream(
      fn graph_id ->
        case materialize_graph(db, Keyword.put(opts, :graph_id, graph_id)) do
          {:ok, stats} -> {:ok, graph_id, stats}
          {:error, reason} -> {:error, graph_id, reason}
        end
      end,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, %{}}, fn
      {:ok, {:ok, graph_id, stats}}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, graph_id, stats)}}

      {:ok, {:error, graph_id, reason}}, _acc ->
        {:halt, {:error, {graph_id, reason}}}

      {:exit, reason}, _acc ->
        {:halt, {:error, {:task_crashed, reason}}}
    end)
  end

  # ============================================================================
  # Private Functions - Global Implementation
  # ============================================================================

  defp do_materialize_all(db, config, opts) do
    start_time = System.monotonic_time(:millisecond)

    # Load TBox from designated graph (if configured)
    tbox_graph_id = ReasoningConfig.tbox_graph(config)
    tbox_facts = load_tbox_facts_for_global(db, tbox_graph_id)

    # Load explicit facts from all graphs (excluding derived quads)
    {:ok, explicit_facts} = load_all_explicit_quads(db)

    # Combine TBox and explicit facts for initial fact set
    initial_facts = MapSet.union(tbox_facts, explicit_facts)

    # Get rules to apply
    rules = ReasoningConfig.materialization_rules(config)

    # Get storage strategy
    storage_strategy = ReasoningConfig.storage_strategy(config)
    inferred_graph = ReasoningConfig.inferred_graph(config)

    # Create TBox-aware global lookup function
    lookup_fn = make_global_lookup_fn(db, tbox_facts)

    # Create store function based on storage strategy
    store_fn = make_global_store_fn(db, config, storage_strategy, inferred_graph)

    # Run semi-naive materialization
    semi_naive_opts = [
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true),
      parallel: Keyword.get(opts, :parallel, false),
      validate_rules: false
    ]

    case SemiNaive.materialize(lookup_fn, store_fn, rules, initial_facts, semi_naive_opts) do
      {:ok, semi_naive_stats} ->
        duration_ms = System.monotonic_time(:millisecond) - start_time

        stats = %{
          iterations: semi_naive_stats.iterations,
          total_derived: semi_naive_stats.total_derived,
          duration_ms: duration_ms,
          rules_applied: semi_naive_stats.rules_applied,
          scope: :global,
          storage_strategy: storage_strategy,
          inferred_graph: inferred_graph,
          tbox_graph: tbox_graph_id
        }

        {:ok, stats}

      {:error, _reason} = error ->
        error
    end
  end

  # ============================================================================
  # Private Functions - Hybrid Implementation
  # ============================================================================

  defp do_materialize_hybrid(db, config, opts) do
    # Separate graphs by scope
    {local_graphs, global_graph_ids} = partition_graphs_by_scope(config, db)

    # Materialize local graphs
    local_results =
      if map_size(local_graphs) > 0 do
        Enum.map(local_graphs, fn {graph_id, graph_config} ->
          materialize_graph(db,
            graph_id: graph_id,
            config: config,
            rules: get_rules_for_graph(config, graph_config),
            emit_telemetry: Keyword.get(opts, :emit_telemetry, true)
          )
        end)
      else
        []
      end

    # Materialize global graphs (if any)
    global_results =
      if map_size(global_graph_ids) > 0 do
        # Create a temporary config with only global graphs
        global_config = put_global_graphs(config, Map.keys(global_graph_ids))

        [materialize_all(db, [config: global_config] ++ opts)]
      else
        []
      end

    # Combine results
    all_results = local_results ++ global_results

    case collect_results(all_results) do
      {:ok, stats_map} ->
        final_stats = %{
          local: stats_map,
          global: Map.get(stats_map, :global, %{}),
          total_graphs: map_size(local_graphs) + map_size(global_graph_ids)
        }

        {:ok, final_stats}

      {:error, _} = error ->
        error
    end
  end

  # ============================================================================
  # Private Functions - Graph Fact Loading
  # ============================================================================

  @doc """
  Loads all explicit quads from all graphs, excluding derived quads.

  This function scans the GSPO index and filters out any quads that exist
  in the derived column family, returning only explicit (user-provided) quads.

  ## Parameters

  - `db` - Database reference

  ## Returns

  - `{:ok, explicit_quads}` - MapSet of explicit quads as {s, p, o} triples
  - `{:error, reason}` - On failure
  """
  @spec load_all_explicit_quads(db_ref()) :: {:ok, MapSet.t(id_triple())} | {:error, term()}
  def load_all_explicit_quads(db) do
    # First, collect all keys from the derived CF for filtering
    derived_keys =
      ErlangAdapter.fold_keys(db, :derived, <<>>, MapSet.new(), fn key, acc ->
        MapSet.put(acc, key)
      end)

    facts =
      ErlangAdapter.fold(db, :gspo, <<>>, MapSet.new(), fn {key, _value}, acc ->
        # Check if this quad is in the derived CF
        if MapSet.member?(derived_keys, key) do
          # Skip derived quads
          acc
        else
          case QuadIndex.key_to_quad(:gspo, key) do
            {_g, _s, _p, _o} = quad ->
              MapSet.put(acc, quad_to_triple(quad))

            _error ->
              acc
          end
        end
      end)

    {:ok, facts}
  rescue
    e -> {:error, {:explicit_quad_loading_failed, e}}
  end

  defp load_graph_facts(db, graph_id) do
    # Load all explicit quads from the specified graph
    # Use GSPO index to iterate over the graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    try do
      facts =
        ErlangAdapter.fold(db, :gspo, prefix, MapSet.new(), fn {key, _value}, acc ->
          case QuadIndex.key_to_quad(:gspo, key) do
            {_g, _s, _p, _o} = quad ->
              MapSet.put(acc, quad_to_triple(quad))

            _error ->
              acc
          end
        end)

      {:ok, facts}
    rescue
      e -> {:error, {:fact_loading_failed, e}}
    end
  end

  # Convert quad {g, s, p, o} to triple {s, p, o} for reasoning
  defp quad_to_triple({_g, s, p, o}), do: {s, p, o}

  # ============================================================================
  # Private Functions - TBox-Aware Lookup
  # ============================================================================

  defp make_tbox_aware_lookup_fn(db, graph_id, tbox_facts) do
    fn pattern ->
      # First check TBox facts (these don't require database lookup)
      {:ok, tbox_results} = lookup_in_tbox_facts(pattern, tbox_facts)

      # Then check graph facts
      {:ok, graph_results} = lookup_in_graph_facts(db, graph_id, pattern)

      # Union both result sets
      {:ok, MapSet.union(tbox_results, graph_results)}
    end
  end

  # Lookup facts in TBox (in-memory MapSet)
  @spec lookup_in_tbox_facts(pattern_union(), MapSet.t(id_triple())) :: {:ok, MapSet.t(id_triple())}
  defp lookup_in_tbox_facts(_pattern, tbox_facts) when map_size(tbox_facts) == 0 do
    {:ok, MapSet.new()}
  end

  defp lookup_in_tbox_facts({:pattern, [s, p, o]}, tbox_facts) do
    # Pattern match against TBox triples
    matches =
      Enum.filter(tbox_facts, fn {fact_s, fact_p, fact_o} ->
        matches_term?(s, fact_s) and matches_term?(p, fact_p) and matches_term?(o, fact_o)
      end)

    {:ok, MapSet.new(matches)}
  end

  defp lookup_in_tbox_facts({:quad_pattern, [_g, s, p, o]}, tbox_facts) do
    # Quad pattern with variable/ignored graph - match on SPO
    matches =
      Enum.filter(tbox_facts, fn {fact_s, fact_p, fact_o} ->
        matches_term?(s, fact_s) and matches_term?(p, fact_p) and matches_term?(o, fact_o)
      end)

    {:ok, MapSet.new(matches)}
  end

  defp lookup_in_tbox_facts(_pattern, _tbox_facts) do
    # Unsupported pattern, return empty
    {:ok, MapSet.new()}
  end

  defp matches_term?(:var, _fact), do: true
  defp matches_term?({:var, _name}, _fact), do: true
  defp matches_term?(:bound, _fact), do: true
  defp matches_term?({:bound, term}, fact), do: term == fact
  defp matches_term?(term, fact) when is_integer(term), do: term == fact
  defp matches_term?(_, _), do: false

  # ============================================================================
  # Private Functions - Graph-Scoped Lookup/Store
  # ============================================================================

  @spec lookup_in_graph_facts(db_ref(), graph_id(), pattern_union()) :: {:ok, MapSet.t(id_triple())}
  defp lookup_in_graph_facts(db, graph_id, pattern) do
    # Build a quad pattern bound to the specific graph
    quad_pattern = bind_graph_to_pattern(pattern, graph_id)
    lookup_quads_as_triples_in_graph(db, quad_pattern, graph_id)
  end

  @spec lookup_quads_as_triples_in_graph(db_ref(), quad_pattern(), graph_id()) :: {:ok, MapSet.t(id_triple())}
  defp lookup_quads_as_triples_in_graph(
         db,
         {:quad_pattern, [{:bound, graph_id}, s, p, o]},
         graph_id
       ) do
    # Build quad index lookup pattern
    pattern = build_quad_lookup_pattern(graph_id, s, p, o)

    {:ok, quads} = lookup_quads_with_pattern(db, pattern, graph_id)

    triples = Enum.map(quads, fn {_g, s, p, o} -> {s, p, o} end)
    {:ok, MapSet.new(triples)}
  end

  defp lookup_quads_as_triples_in_graph(_db, _pattern, _graph_id) do
    {:ok, MapSet.new()}
  end

  defp make_graph_store_fn(db, graph_id) do
    fn fact_set ->
      # Convert triples back to quads with graph_id
      quads = Enum.map(fact_set, fn {s, p, o} -> {s, p, o, graph_id} end)

      # Store as derived quads
      case store_derived_quads(db, quads, graph_id) do
        :ok -> :ok
        {:error, _} = error -> error
      end
    end
  end

  # ============================================================================
  # Private Functions - Global Lookup and Storage
  # ============================================================================

  # Loads TBox facts for global reasoning.
  # If a TBox graph is configured, extracts TBox facts from it.
  # Otherwise returns an empty set.
  defp load_tbox_facts_for_global(_db, nil), do: MapSet.new()

  defp load_tbox_facts_for_global(db, tbox_graph_id) do
    # Emit telemetry event for TBox extraction start (global reasoning uses graph_id = :global)
    Telemetry.emit_tbox_extract_start(:global, tbox_graph_id)
    start_time = System.monotonic_time()

    case TBoxExtractor.extract_tbox(db, tbox_graph_id) do
      {:ok, tbox_quads} ->
        duration = System.monotonic_time() - start_time
        tbox_fact_count = MapSet.size(tbox_quads)

        # Emit success telemetry event
        Telemetry.emit_tbox_extract_stop(:global, tbox_graph_id, tbox_fact_count, duration)

        Logger.debug(
          "Global TBox extraction succeeded: tbox_graph=#{tbox_graph_id}, facts=#{tbox_fact_count}"
        )

        # Convert TBox quads to triples for reasoning
        Enum.into(tbox_quads, MapSet.new(), fn {_g, s, p, o} -> {s, p, o} end)

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        # Emit error telemetry event
        Telemetry.emit_tbox_extract_error(:global, tbox_graph_id, reason, duration)

        # Log warning (not error, since we continue without TBox)
        Logger.warning(
          "Global TBox extraction failed: tbox_graph=#{tbox_graph_id}, reason=#{inspect(reason)}. Continuing without TBox."
        )

        # If TBox extraction fails, continue without TBox
        MapSet.new()
    end
  end

  # Creates a TBox-aware global lookup function.
  # The lookup function combines in-memory TBox facts with database lookups
  # across all graphs for explicit facts.
  defp make_global_lookup_fn(db, tbox_facts) do
    fn pattern ->
      # First check TBox facts (fast, in-memory)
      tbox_matches = lookup_in_tbox_facts(pattern, tbox_facts)

      # Then check all graphs in the database
      graph_matches = lookup_all_graphs_facts(db, pattern)

      # Union both result sets
      case {tbox_matches, graph_matches} do
        {{:ok, tbox_results}, {:ok, graph_results}} ->
          {:ok, MapSet.union(tbox_results, graph_results)}

        {{:ok, _tbox_results}, {:error, _reason}} ->
          # If graph lookup fails, the error is fatal since it's the primary source
          :error
      end
    end
  end

  # Looks up facts across all graphs in the database.
  # For global reasoning, this uses index selection to optimize the scan
  # and returns matching triples (quads converted to triples).
  #
  # ## Index Selection
  #
  # The function selects the optimal quad index based on which pattern
  # positions are bound:
  # - Subject bound → SPOG index (cross-graph subject lookup)
  # - Predicate bound → POSG index (cross-graph predicate lookup)
  # - All variables → GSPO index (default full scan)
  #
  # This avoids scanning the entire database when possible.
  defp lookup_all_graphs_facts(db, {:pattern, [s, p, o]}) do
    # Convert to quad pattern with variable graph (:var for all graphs)
    s_pat = if s == :var or match?({:var, _}, s), do: :var, else: :bound
    p_pat = if p == :var or match?({:var, _}, p), do: :var, else: :bound
    o_pat = if o == :var or match?({:var, _}, o), do: :var, else: :bound

    # Quad pattern: {s, p, o, g} - g is :var for all graphs
    quad_pattern = {s_pat, p_pat, o_pat, :var}

    # Extract bound values for prefix building
    bound_values =
      %{}
      |> maybe_add_bound_value(:s, s)
      |> maybe_add_bound_value(:p, p)
      |> maybe_add_bound_value(:o, o)

    # Use QuadIndex to select optimal index and build prefix
    try do
      case QuadIndex.build_quad_prefix(quad_pattern, bound_values) do
        %{
          index: index,
          prefix: prefix,
          needs_filter: needs_filter,
          filter_positions: filter_positions
        } ->
          lookup_facts_with_index(
            db,
            index,
            prefix,
            quad_pattern,
            bound_values,
            needs_filter,
            filter_positions
          )
      end
    rescue
      e -> {:error, {:lookup_failed, e}}
    end
  end

  # Catch-all clause for non-pattern lookups
  defp lookup_all_graphs_facts(_db, _pattern) do
    {:ok, MapSet.new()}
  end

  # Helper to extract bound values from pattern elements
  defp maybe_add_bound_value(values, _key, :var), do: values
  defp maybe_add_bound_value(values, _key, {:var, _}), do: values
  defp maybe_add_bound_value(values, key, {:bound, value}), do: Map.put(values, key, value)

  defp maybe_add_bound_value(values, key, value) when is_integer(value),
    do: Map.put(values, key, value)

  # Optimized fact lookup using selected index
  defp lookup_facts_with_index(
         db,
         index,
         prefix,
         quad_pattern,
         bound_values,
         needs_filter,
         filter_positions
       ) do
    results =
      ErlangAdapter.fold(db, index, prefix, [], fn {key, _value}, acc ->
        case QuadIndex.key_to_quad(index, key) do
          {_g, s, p, o} ->
            if needs_filter do
              if matches_quad_pattern?(
                   {s, p, o, :ignored},
                   quad_pattern,
                   bound_values,
                   filter_positions
                 ) do
                [{s, p, o} | acc]
              else
                acc
              end
            else
              [{s, p, o} | acc]
            end

          _error ->
            acc
        end
      end)

    {:ok, MapSet.new(results)}
  end

  # Creates a global store function based on the storage strategy.
  #
  # The store function handles three strategies:
  # - `:separate_graph` - Store in designated inference graph (recommended)
  # - `:per_graph_cf` - Store only in derived CF (no graph context)
  # - `:same_as_premises` - **DEPRECATED**: Falls back to `:separate_graph`
  #
  # ## Deprecation Notice
  #
  # The `:same_as_premises` storage strategy is **not fully implemented**.
  # This strategy would store derived facts in the same graph as their premises,
  # but requires full provenance tracking infrastructure.
  #
  # Currently, using `:same_as_premises` will fall back to `:separate_graph` behavior
  # with a deprecation warning. Use `:separate_graph` explicitly instead.
  #
  # ## Future Implementation
  #
  # Full `:same_as_premises` support requires:
  # - Provenance tracking for each derived fact (which rule produced it)
  # - Source graph identification for each premise
  # - Incremental maintenance with cross-graph dependencies
  #
  # Use `:separate_graph` for now, which stores all derived facts in a
  # designated inference graph (default: graph 9999).
  defp make_global_store_fn(db, config, :same_as_premises, inferred_graph) do
    # Emit deprecation warning
    Logger.warning("""
    The storage strategy :same_as_premises is deprecated and not fully implemented.
    Falling back to :separate_graph behavior.

    To store derived facts in the same graph as premises, use :separate_graph
    with a specific inferred_graph ID, or wait for full provenance tracking support.
    """)

    # For same_as_premises, we need to track source graphs
    # This requires provenance tracking - for now, use inferred_graph
    make_global_store_fn(db, config, :separate_graph, inferred_graph)
  end

  defp make_global_store_fn(db, _config, :separate_graph, inferred_graph) do
    target_graph =
      case inferred_graph do
        :separate -> get_inference_graph_id()
        graph_id when is_integer(graph_id) -> graph_id
        # Default to graph 0
        nil -> 0
      end

    fn fact_set ->
      quads = Enum.map(fact_set, fn {s, p, o} -> {s, p, o, target_graph} end)
      store_derived_quads(db, quads, target_graph)
    end
  end

  defp make_global_store_fn(db, _config, :per_graph_cf, _inferred_graph) do
    fn fact_set ->
      # Store in derived CF only, without graph context
      # Convert triples to quads with default graph 0
      quads = Enum.map(fact_set, fn {s, p, o} -> {s, p, o, 0} end)

      # Store in derived CF
      Enum.each(quads, fn {s, p, o, _g} ->
        # Use spog_key for derived storage (subject-predicate-object-graph order)
        key = QuadIndex.spog_key(s, p, o, 0)
        ErlangAdapter.put(db, :derived, key, <<>>)
      end)

      :ok
    end
  end

  # Gets the inference graph ID for storing derived quads.
  # Returns a predefined graph ID (e.g., 9999) for storing inferences
  # when :separate is specified.
  defp get_inference_graph_id, do: 9999

  # ============================================================================
  # Private Functions - Pattern Conversion
  # ============================================================================

  defp bind_graph_to_pattern({:pattern, [s, p, o]}, graph_id) do
    {:quad_pattern, [{:bound, graph_id}, s, p, o]}
  end

  defp bind_graph_to_pattern({:quad_pattern, [_g, s, p, o]}, graph_id) do
    {:quad_pattern, [{:bound, graph_id}, s, p, o]}
  end

  # ============================================================================
  # Private Functions - Quad Lookup
  # ============================================================================

  defp build_quad_lookup_pattern(graph_id, s, p, o) do
    s_elem = if s == :var or match?({:var, _}, s), do: :var, else: {:bound, s}
    p_elem = if p == :var or match?({:var, _}, p), do: :var, else: {:bound, p}
    o_elem = if o == :var or match?({:var, _}, o), do: :var, else: {:bound, o}

    {{:bound, graph_id}, s_elem, p_elem, o_elem}
  end

  defp matches_quad_pattern?(
         {s, p, o, _g},
         {s_pat, p_pat, o_pat, _g_pat},
         bound_values,
         filter_positions
       ) do
    (:s not in filter_positions or matches_bound?(:s, s, s_pat, bound_values)) and
      (:p not in filter_positions or matches_bound?(:p, p, p_pat, bound_values)) and
      (:o not in filter_positions or matches_bound?(:o, o, o_pat, bound_values))
  end

  defp matches_bound?(:s, value, :bound, values), do: Map.get(values, :s) == value
  defp matches_bound?(:p, value, :bound, values), do: Map.get(values, :p) == value
  defp matches_bound?(:o, value, :bound, values), do: Map.get(values, :o) == value
  defp matches_bound?(_pos, _value, _pat, _values), do: true

  @spec lookup_quads_with_pattern(db_ref(), term(), graph_id()) :: {:ok, list(id_quad())}
  defp lookup_quads_with_pattern(db, _pattern, graph_id) do
    # Use GSPO index to look up quads for this graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    stream = ErlangAdapter.prefix_stream(db, :gspo, prefix)

    quads =
      stream
      |> Stream.map(fn {key, _value} ->
        {g, s, p, o} = QuadIndex.decode_gspo_key(key)
        {g, s, p, o}
      end)
      |> Enum.to_list()

    {:ok, quads}
  end

  # ============================================================================
  # Private Functions - Derived Quad Storage
  # ============================================================================

  @derived_cf :derived

  defp store_derived_quads(db, quads, _graph_context) do
    # Store derived quads using the derived column family
    # Key encoding includes graph ID for graph-aware derived storage
    operations =
      for {s, p, o, g} <- quads do
        key = QuadIndex.gspo_key(g, s, p, o)
        {@derived_cf, key, <<>>}
      end

    ErlangAdapter.write_batch(db, operations, true)
  end

  # ============================================================================
  # Private Functions - Validation
  # ============================================================================

  defp validate_graph_id(opts) do
    case Keyword.get(opts, :graph_id) do
      nil -> {:error, :graph_id_required}
      graph_id when is_integer(graph_id) and graph_id >= 0 -> {:ok, graph_id}
      _ -> {:error, :invalid_graph_id}
    end
  end

  defp validate_graph_ids(opts) do
    case Keyword.get(opts, :graph_ids) do
      nil -> {:error, :graph_ids_required}
      ids when is_list(ids) -> {:ok, ids}
      _ -> {:error, :invalid_graph_ids}
    end
  end

  defp validate_config(opts) do
    case Keyword.get(opts, :config) do
      nil -> {:error, :config_required}
      %ReasoningConfig{} = config -> {:ok, config}
      _ -> {:error, :invalid_config}
    end
  end

  defp resolve_graph_config(config, graph_id) do
    case ReasoningConfig.graph_config(config, graph_id) do
      {:ok, graph_config} -> {:ok, graph_config}
      :error -> {:ok, GraphReasoningConfig.default(graph_id)}
    end
  end

  defp check_graph_participation(%GraphReasoningConfig{} = graph_config) do
    if GraphReasoningConfig.participates?(graph_config) do
      :ok
    else
      {:error, :graph_does_not_participate}
    end
  end

  defp verify_global_scope(%ReasoningConfig{scope: :global}), do: :ok
  defp verify_global_scope(_config), do: {:error, :not_global_scope}

  defp verify_hybrid_scope(%ReasoningConfig{scope: :hybrid}), do: :ok
  defp verify_hybrid_scope(_config), do: {:error, :not_hybrid_scope}

  # ============================================================================
  # Private Functions - Rules
  # ============================================================================

  defp get_rules_for_graph(config, graph_config) do
    case GraphReasoningConfig.resolve_rules(graph_config, config) do
      {:ok, rule_names} -> rule_names
      {:error, _} -> []
    end
  end

  # ============================================================================
  # Private Functions - Status Management
  # ============================================================================

  defp update_graph_status_after_materialization(_db, graph_id, graph_config, stats) do
    alias TripleStore.Reasoner.GraphReasoningStatus

    case GraphReasoningStatus.load(status_key(graph_id)) do
      {:ok, status} ->
        updated =
          status
          |> GraphReasoningStatus.record_materialization(%{
            derived_count: stats.total_derived,
            iterations: stats.iterations,
            duration_ms: stats.duration_ms
          })
          |> GraphReasoningStatus.update_config(graph_config)

        GraphReasoningStatus.store(updated, status_key(graph_id))

      {:error, :not_found} ->
        # Create new status
        {:ok, status} =
          GraphReasoningStatus.new(
            graph_id: graph_id,
            config: graph_config,
            explicit_count: 0
          )

        updated =
          status
          |> GraphReasoningStatus.record_materialization(%{
            derived_count: stats.total_derived,
            iterations: stats.iterations,
            duration_ms: stats.duration_ms
          })

        GraphReasoningStatus.store(updated, status_key(graph_id))
    end
  end

  defp record_graph_error(_db, graph_id, error) do
    alias TripleStore.Reasoner.GraphReasoningStatus

    case GraphReasoningStatus.load(status_key(graph_id)) do
      {:ok, status} ->
        updated = GraphReasoningStatus.record_error(status, error)
        GraphReasoningStatus.store(updated, status_key(graph_id))

      {:error, :not_found} ->
        {:ok, status} = GraphReasoningStatus.new(graph_id: graph_id)
        updated = GraphReasoningStatus.record_error(status, error)
        GraphReasoningStatus.store(updated, status_key(graph_id))
    end
  end

  defp status_key(graph_id), do: :"graph_#{graph_id}"

  # ============================================================================
  # Private Functions - Hybrid Support
  # ============================================================================

  defp partition_graphs_by_scope(config, db) do
    # Get all graphs in the database
    all_graph_ids = get_all_graph_ids(db)

    # Track whether we're in hybrid mode for warning
    hybrid_mode? = ReasoningConfig.scope(config) == :hybrid

    # First pass: partition graphs and collect unconfigured ones
    {local, global, unconfigured_graphs} =
      Enum.reduce(all_graph_ids, {%{}, %{}, []}, fn graph_id,
                                                    {local_acc, global_acc, unconf_acc} ->
        case ReasoningConfig.graph_config(config, graph_id) do
          {:ok, %GraphReasoningConfig{scope: :local} = gc} ->
            {Map.put(local_acc, graph_id, gc), global_acc, unconf_acc}

          {:ok, %GraphReasoningConfig{scope: :global} = gc} ->
            {local_acc, Map.put(global_acc, graph_id, gc), unconf_acc}

          {:ok, %GraphReasoningConfig{scope: :none}} ->
            # Graph doesn't participate in reasoning
            {local_acc, global_acc, unconf_acc}

          :error ->
            # Graph has no explicit configuration - use default
            default_gc = GraphReasoningConfig.default(graph_id)

            if hybrid_mode? do
              # Track unconfigured graphs for warning
              {Map.put(local_acc, graph_id, default_gc), global_acc, [graph_id | unconf_acc]}
            else
              {Map.put(local_acc, graph_id, default_gc), global_acc, unconf_acc}
            end
        end
      end)

    # Emit warning if hybrid mode has unconfigured graphs
    if hybrid_mode? and not Enum.empty?(unconfigured_graphs) do
      Logger.warning("""
      Hybrid reasoning mode: #{length(unconfigured_graphs)} graphs have no explicit configuration.
      Using local scope as default for graphs: #{inspect(Enum.reverse(unconfigured_graphs))}

      To silence this warning, configure each graph explicitly using ReasoningConfig.set_graph_config/3.
      """)
    end

    {local, global}
  end

  defp get_all_graph_ids(db) do
    # Check cache first
    cache_key = {:graph_discovery, db}
    # 5 minutes
    cache_ttl_ms = 5 * 60 * 1000

    case :persistent_term.get(cache_key, :cache_miss) do
      {:cached, graph_ids, timestamp} ->
        # Check if cache is still valid
        if System.system_time(:millisecond) - timestamp < cache_ttl_ms do
          graph_ids
        else
          # Cache expired, refresh
          discover_and_cache_graphs(db, cache_key)
        end

      :cache_miss ->
        discover_and_cache_graphs(db, cache_key)
    end
  end

  defp discover_and_cache_graphs(db, cache_key) do
    # Use fold_keys to scan GSPO index keys (efficient - only keys, not values)
    # Extract graph ID from first 8 bytes of each GSPO key
    graph_ids =
      ErlangAdapter.fold_keys(db, :gspo, <<>>, MapSet.new(), fn key, acc ->
        # GSPO key format: <<graph::64-big, subject::64-big, predicate::64-big, object::64-big>>
        case key do
          <<graph_id::64-big, _rest::binary>> ->
            MapSet.put(acc, graph_id)

          _ ->
            acc
        end
      end)
      |> MapSet.to_list()
      |> Enum.sort()

    # Cache the result with timestamp
    :persistent_term.put(cache_key, {:cached, graph_ids, System.system_time(:millisecond)})

    graph_ids
  end

  defp put_global_graphs(config, _graph_ids) do
    # Create a config focused on global graphs
    # For now, just return the original config
    config
  end

  defp collect_results(results) do
    Enum.reduce_while(results, {:ok, %{}}, fn result, acc ->
      case collect_single_result(result, acc) do
        {:cont, new_acc} -> {:cont, new_acc}
        {:halt, reason} -> {:halt, reason}
      end
    end)
  end

  defp collect_single_result({:ok, stats}, {:ok, acc}) do
    graph_id = Map.get(stats, :graph_id, :global)
    {:cont, {:ok, Map.put(acc, graph_id, stats)}}
  end

  defp collect_single_result({:error, _reason} = error, _acc), do: {:halt, error}
end
