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

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.{
    GraphReasoningConfig,
    GraphReasoningStatus,
    ReasoningConfig,
    SemiNaive,
    TBoxExtractor
  }

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "Graph ID"
  @type graph_id :: non_neg_integer()

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
  @spec materialize_graph(db_ref(), keyword()) :: {:ok, materialization_stats()} | {:error, term()}
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
  defp load_tbox_facts(_db, tbox_graph_id, target_graph_id) when tbox_graph_id == target_graph_id do
    # TBox is in the same graph, facts already loaded as initial_facts
    MapSet.new()
  end

  defp load_tbox_facts(db, tbox_graph_id, _target_graph_id) do
    case TBoxExtractor.extract_tbox(db, tbox_graph_id) do
      {:ok, tbox_quads} ->
        # Convert TBox quads to triples for reasoning
        Enum.into(tbox_quads, MapSet.new(), fn {_g, s, p, o} -> {s, p, o} end)

      {:error, _reason} ->
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

    # Load initial facts from all graphs
    {:ok, initial_facts} = load_all_facts(db)

    # Get rules to apply
    rules = ReasoningConfig.materialization_rules(config)
    {:ok, compiled_rules} = compile_rules(rules, config)

    # Determine where to store derived facts
    inferred_graph = ReasoningConfig.inferred_graph(config)

    lookup_fn = make_all_graphs_lookup_fn(db)
    store_fn = make_inferred_store_fn(db, inferred_graph)

    # Run semi-naive materialization
    semi_naive_opts = [
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true),
      parallel: Keyword.get(opts, :parallel, false),
      validate_rules: false
    ]

    case SemiNaive.materialize(lookup_fn, store_fn, compiled_rules, initial_facts, semi_naive_opts) do
      {:ok, semi_naive_stats} ->
        duration_ms = System.monotonic_time(:millisecond) - start_time

        stats = %{
          iterations: semi_naive_stats.iterations,
          total_derived: semi_naive_stats.total_derived,
          duration_ms: duration_ms,
          rules_applied: semi_naive_stats.rules_applied,
          scope: :global,
          inferred_graph: inferred_graph
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

  defp load_graph_facts(db, graph_id) do
    # Load all explicit quads from the specified graph
    # Use GSPO index to iterate over the graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    try do
      facts =
        NIF.fold(db, :gspo, prefix, MapSet.new(), fn {key, _value}, acc ->
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

  defp load_all_facts(db) do
    # Load all quads from all graphs as triples
    # This requires scanning the entire GSPO index
    try do
      facts =
        NIF.fold(db, :gspo, <<>>, MapSet.new(), fn {key, _value}, acc ->
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

  # Convert quad {s, p, o, g} to triple {s, p, o} for reasoning
  defp quad_to_triple({_g, s, p, o}), do: {s, p, o}

  # ============================================================================
  # Private Functions - TBox-Aware Lookup
  # ============================================================================

  defp make_tbox_aware_lookup_fn(db, graph_id, tbox_facts) do
    fn pattern ->
      # First check TBox facts (these don't require database lookup)
      tbox_matches = lookup_in_tbox_facts(pattern, tbox_facts)

      # Then check graph facts
      graph_matches = lookup_in_graph_facts(db, graph_id, pattern)

      # Union both result sets
      case {tbox_matches, graph_matches} do
        {{:ok, tbox_results}, {:ok, graph_results}} ->
          {:ok, MapSet.union(tbox_results, graph_results)}

        {{:ok, tbox_results}, :error} ->
          {:ok, tbox_results}

        {:error, {:ok, graph_results}} ->
          {:ok, graph_results}

        {:error, :error} ->
          :error
      end
    end
  end

  # Lookup facts in TBox (in-memory MapSet)
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

  defp lookup_in_graph_facts(db, graph_id, pattern) do
    # Build a quad pattern bound to the specific graph
    quad_pattern = bind_graph_to_pattern(pattern, graph_id)
    lookup_quads_as_triples_in_graph(db, quad_pattern, graph_id)
  end

  defp lookup_quads_as_triples_in_graph(db, {:quad_pattern, [{:bound, graph_id}, s, p, o]}, graph_id) do
    # Build quad index lookup pattern
    pattern = build_quad_lookup_pattern(graph_id, s, p, o)

    case lookup_quads_with_pattern(db, pattern, graph_id) do
      {:ok, quads} ->
        triples = Enum.map(quads, fn {_g, s, p, o} -> {s, p, o} end)
        {:ok, MapSet.new(triples)}

      {:error, _} = error ->
        error
    end
  end

  defp lookup_quads_as_triples_in_graph(_db, _pattern, _graph_id) do
    {:ok, MapSet.new()}
  end

  defp make_graph_lookup_fn(db, graph_id) do
    fn pattern ->
      # Convert rule pattern to quad pattern with bound graph
      quad_pattern = bind_graph_to_pattern(pattern, graph_id)
      lookup_quads_as_triples(db, quad_pattern)
    end
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

  defp make_all_graphs_lookup_fn(db) do
    fn pattern ->
      # Lookup across all graphs, return as triples
      lookup_quads_all_graphs_as_triples(db, pattern)
    end
  end

  defp make_inferred_store_fn(db, inferred_graph) do
    fn fact_set ->
      quads =
        case inferred_graph do
          :separate ->
            # Store in separate inference graph
            Enum.map(fact_set, fn {s, p, o} -> {s, p, o, get_inference_graph_id()} end)

          graph_id when is_integer(graph_id) ->
            # Store in specified graph
            Enum.map(fact_set, fn {s, p, o} -> {s, p, o, graph_id} end)

          nil ->
            # Store in same graph as source (need graph context)
            # For now, use default graph
            Enum.map(fact_set, fn {s, p, o} -> {s, p, o, 0} end)
        end

      store_derived_quads(db, quads, inferred_graph)
    end
  end

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

  defp lookup_quads_as_triples(db, {:quad_pattern, [{:bound, graph_id}, s, p, o]}) do
    # Build quad index lookup pattern
    pattern = build_quad_lookup_pattern(graph_id, s, p, o)

    case lookup_quads_with_pattern(db, pattern, graph_id) do
      {:ok, quads} ->
        triples = Enum.map(quads, fn {_g, s, p, o} -> {s, p, o} end)
        {:ok, triples}

      {:error, _} = error ->
        error
    end
  end

  defp lookup_quads_all_graphs_as_triples(db, pattern) do
    # For global reasoning, look up across all graphs
    # Convert to triple pattern for lookup
    case pattern do
      {:pattern, [s, p, o]} ->
        # Use triple index lookup
        TripleStore.Index.lookup_all_fold(db, {s, p, o})

      {:quad_pattern, [{:var, _g}, s, p, o]} ->
        # Variable graph - look up all graphs
        lookup_quads_all_graphs_var(db, s, p, o)
    end
  end

  defp lookup_quads_all_graphs_var(_db, _s, _p, _o) do
    # This requires scanning all quad indices
    # For now, return empty and implement fully in next phase
    {:ok, []}
  end

  defp build_quad_lookup_pattern(graph_id, s, p, o) do
    s_elem = if s == :var or match?({:var, _}, s), do: :var, else: {:bound, s}
    p_elem = if p == :var or match?({:var, _}, p), do: :var, else: {:bound, p}
    o_elem = if o == :var or match?({:var, _}, o), do: :var, else: {:bound, o}

    {{:bound, graph_id}, s_elem, p_elem, o_elem}
  end

  defp lookup_quads_with_pattern(db, _pattern, graph_id) do
    # Use GSPO index to look up quads for this graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    case NIF.prefix_stream(db, :gspo, prefix) do
      stream when is_function(stream) ->
        quads =
          stream
          |> Stream.map(fn {key, _value} ->
            {g, s, p, o} = QuadIndex.decode_gspo_key(key)
            {g, s, p, o}
          end)
          |> Enum.to_list()

        {:ok, quads}

      {:error, _} = error ->
        error
    end
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

    NIF.write_batch(db, operations, true)
  end

  defp get_inference_graph_id, do: 0

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

  defp compile_rules(_rule_names, _config) do
    # This would use RuleCompiler to compile rule names to Rule structs
    # For now, return empty list
    []
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

    {local, global} =
      Enum.reduce(all_graph_ids, {%{}, %{}}, fn graph_id, {local_acc, global_acc} ->
        case ReasoningConfig.graph_config(config, graph_id) do
          {:ok, %GraphReasoningConfig{scope: :local} = gc} ->
            {Map.put(local_acc, graph_id, gc), global_acc}

          {:ok, %GraphReasoningConfig{scope: :global} = gc} ->
            {local_acc, Map.put(global_acc, graph_id, gc)}

          {:ok, %GraphReasoningConfig{scope: :none}} ->
            # Graph doesn't participate in reasoning
            {local_acc, global_acc}

          :error ->
            # Use default scope from config
            case ReasoningConfig.scope(config) do
              :local -> {Map.put(local_acc, graph_id, GraphReasoningConfig.default(graph_id)), global_acc}
              :global -> {local_acc, Map.put(global_acc, graph_id, GraphReasoningConfig.default(graph_id))}
              :hybrid -> {Map.put(local_acc, graph_id, GraphReasoningConfig.default(graph_id)), global_acc}
            end
        end
      end)

    {local, global}
  end

  defp get_all_graph_ids(_db) do
    # Get all graph IDs from the database
    # For now, return default graph
    [0]
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
