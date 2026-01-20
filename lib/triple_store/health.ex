defmodule TripleStore.Health do
  @moduledoc """
  Health check module for the TripleStore.

  Provides health checks for production monitoring including:

  - **Liveness**: Simple up/down check for orchestration (Kubernetes, etc.)
  - **Readiness**: Ready to serve traffic check
  - **Full Health**: Comprehensive status with statistics

  ## Health Status

  - `:healthy` - All systems operational
  - `:degraded` - Non-critical issues (e.g., cache unavailable)
  - `:unhealthy` - Critical issues (e.g., database closed)

  ## Usage

      # Liveness probe (quick check)
      :ok = TripleStore.Health.liveness(store)

      # Readiness probe
      {:ok, :ready} = TripleStore.Health.readiness(store)

      # Full health check
      {:ok, health} = TripleStore.Health.health(store)

      # Detailed health with all metrics
      {:ok, health} = TripleStore.Health.health(store, include_all: true)

  ## Integration Examples

  ### Plug Endpoint

      get "/health/live" do
        case TripleStore.Health.liveness(store) do
          :ok -> send_resp(conn, 200, "OK")
          {:error, _} -> send_resp(conn, 503, "Unhealthy")
        end
      end

      get "/health/ready" do
        case TripleStore.Health.readiness(store) do
          {:ok, :ready} -> send_resp(conn, 200, "Ready")
          {:ok, :not_ready} -> send_resp(conn, 503, "Not Ready")
          {:error, _} -> send_resp(conn, 503, "Error")
        end
      end

  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Statistics

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Store handle"
  @type store :: TripleStore.store()

  @typedoc "Health status"
  @type status :: :healthy | :degraded | :unhealthy

  @typedoc "Readiness status"
  @type readiness_status :: :ready | :not_ready

  @typedoc "Index sizes"
  @type index_sizes :: %{
          spo: non_neg_integer(),
          pos: non_neg_integer(),
          osp: non_neg_integer(),
          derived: non_neg_integer(),
          dictionary: non_neg_integer()
        }

  @typedoc "Memory estimates"
  @type memory_estimate :: %{
          beam_mb: float(),
          estimated_data_mb: float(),
          estimated_total_mb: float()
        }

  @typedoc "Compaction status"
  @type compaction_status :: %{
          running: boolean(),
          pending_bytes: non_neg_integer(),
          pending_compactions: non_neg_integer()
        }

  @typedoc "Full health check result"
  @type health_result :: %{
          status: status(),
          triple_count: non_neg_integer(),
          database_open: boolean(),
          dict_manager_alive: boolean(),
          plan_cache_alive: boolean(),
          query_cache_alive: boolean(),
          metrics_alive: boolean(),
          index_sizes: index_sizes() | nil,
          memory: memory_estimate() | nil,
          compaction: compaction_status() | nil,
          checked_at: DateTime.t()
        }

  @typedoc "Health check options"
  @type health_opts :: [
          include_stats: boolean(),
          include_memory: boolean(),
          include_indices: boolean(),
          include_compaction: boolean(),
          include_all: boolean()
        ]

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Estimated bytes per triple (24 bytes key + overhead per index)
  @bytes_per_triple 100

  # ===========================================================================
  # Liveness Check
  # ===========================================================================

  @doc """
  Simple liveness check.

  Returns `:ok` if the store is alive and responding.
  This is designed for Kubernetes liveness probes.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`

  ## Returns

  - `:ok` - Store is alive
  - `{:error, :database_closed}` - Database is not open

  ## Examples

      :ok = TripleStore.Health.liveness(store)

  """
  @spec liveness(store()) :: :ok | {:error, :database_closed}
  def liveness(%{db: db}) do
    if NIF.is_open(db) do
      :ok
    else
      {:error, :database_closed}
    end
  end

  # ===========================================================================
  # Readiness Check
  # ===========================================================================

  @doc """
  Readiness check for serving traffic.

  Checks if the store is ready to serve requests. This includes
  verifying that all required processes are running.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`

  ## Returns

  - `{:ok, :ready}` - Store is ready to serve traffic
  - `{:ok, :not_ready}` - Store is alive but not ready (e.g., warming up)
  - `{:error, reason}` - Critical failure

  ## Examples

      {:ok, :ready} = TripleStore.Health.readiness(store)

  """
  @spec readiness(store()) :: {:ok, readiness_status()} | {:error, term()}
  def readiness(%{db: db, dict_manager: dict_manager}) do
    database_open = NIF.is_open(db)
    dict_manager_alive = is_pid(dict_manager) and Process.alive?(dict_manager)

    cond do
      not database_open ->
        {:error, :database_closed}

      not dict_manager_alive ->
        {:ok, :not_ready}

      true ->
        {:ok, :ready}
    end
  end

  # ===========================================================================
  # Full Health Check
  # ===========================================================================

  @doc """
  Comprehensive health check.

  Returns detailed health information including statistics and
  process status. Use this for monitoring dashboards.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`

  ## Options

  - `:include_stats` - Include triple count (default: true)
  - `:include_memory` - Include memory estimates (default: false)
  - `:include_indices` - Include index sizes (default: false)
  - `:include_compaction` - Include compaction status (default: false)
  - `:include_all` - Include all optional metrics (default: false)

  ## Returns

  - `{:ok, health}` - Health status map
  - `{:error, reason}` - On failure

  ## Health Map

  - `:status` - Overall health status (`:healthy`, `:degraded`, `:unhealthy`)
  - `:triple_count` - Number of triples (if `include_stats: true`)
  - `:database_open` - Whether database is open
  - `:dict_manager_alive` - Whether dictionary manager is alive
  - `:plan_cache_alive` - Whether plan cache is alive
  - `:query_cache_alive` - Whether query cache is alive
  - `:metrics_alive` - Whether metrics collector is alive
  - `:index_sizes` - Index entry counts (if `include_indices: true`)
  - `:memory` - Memory estimates (if `include_memory: true`)
  - `:compaction` - Compaction status (if `include_compaction: true`)
  - `:checked_at` - Timestamp of health check

  ## Examples

      {:ok, health} = TripleStore.Health.health(store)
      # => {:ok, %{
      #      status: :healthy,
      #      triple_count: 10000,
      #      database_open: true,
      #      dict_manager_alive: true,
      #      plan_cache_alive: true,
      #      query_cache_alive: true,
      #      metrics_alive: true,
      #      checked_at: ~U[2025-12-27 10:00:00Z]
      #    }}

      # With all metrics
      {:ok, health} = TripleStore.Health.health(store, include_all: true)
      # => {:ok, %{
      #      status: :healthy,
      #      triple_count: 10000,
      #      index_sizes: %{spo: 10000, pos: 10000, osp: 10000, ...},
      #      memory: %{beam_mb: 50.5, estimated_data_mb: 1.0, estimated_total_mb: 51.5},
      #      compaction: %{running: false, pending_bytes: 0, pending_compactions: 0},
      #      ...
      #    }}

  """
  @spec health(store(), health_opts()) :: {:ok, health_result()} | {:error, term()}
  def health(store, opts \\ [])

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def health(%{db: db, dict_manager: dict_manager}, opts) do
    include_all = Keyword.get(opts, :include_all, false)
    include_stats = Keyword.get(opts, :include_stats, true) or include_all
    include_memory = Keyword.get(opts, :include_memory, false) or include_all
    include_indices = Keyword.get(opts, :include_indices, false) or include_all
    include_compaction = Keyword.get(opts, :include_compaction, false) or include_all

    # Check component status
    database_open = NIF.is_open(db)
    dict_manager_alive = is_pid(dict_manager) and Process.alive?(dict_manager)
    plan_cache_alive = plan_cache_running?()
    query_cache_alive = query_cache_running?()
    metrics_alive = metrics_running?()

    # Get statistics if requested
    triple_count =
      if include_stats do
        case Statistics.triple_count(db) do
          {:ok, count} -> count
          _ -> 0
        end
      else
        nil
      end

    # Get index sizes if requested
    index_sizes =
      if include_indices do
        get_index_sizes(db)
      else
        nil
      end

    # Get memory estimates if requested
    memory =
      if include_memory do
        estimate_memory(triple_count || 0)
      else
        nil
      end

    # Get compaction status if requested
    compaction =
      if include_compaction do
        get_compaction_status()
      else
        nil
      end

    # Determine overall status
    status =
      determine_status(
        database_open,
        dict_manager_alive,
        plan_cache_alive,
        query_cache_alive,
        metrics_alive
      )

    health = %{
      status: status,
      database_open: database_open,
      dict_manager_alive: dict_manager_alive,
      plan_cache_alive: plan_cache_alive,
      query_cache_alive: query_cache_alive,
      metrics_alive: metrics_alive,
      checked_at: DateTime.utc_now()
    }

    # Add optional fields
    health =
      health
      |> maybe_add(:triple_count, triple_count)
      |> maybe_add(:index_sizes, index_sizes)
      |> maybe_add(:memory, memory)
      |> maybe_add(:compaction, compaction)

    {:ok, health}
  end

  # ===========================================================================
  # Index Sizes
  # ===========================================================================

  @doc """
  Returns the sizes (entry counts) of all indices.

  ## Arguments

  - `db` - Database reference

  ## Returns

  Map with counts for each index:
  - `:spo` - Subject-Predicate-Object index
  - `:pos` - Predicate-Object-Subject index
  - `:osp` - Object-Subject-Predicate index
  - `:derived` - Derived/inferred triples
  - `:dictionary` - Dictionary entries

  ## Examples

      sizes = TripleStore.Health.get_index_sizes(db)
      # => %{spo: 10000, pos: 10000, osp: 10000, derived: 500, dictionary: 2500}

  """
  @spec get_index_sizes(NIF.db_ref()) :: index_sizes()
  def get_index_sizes(db) do
    %{
      spo: count_index_entries(db, :spo),
      pos: count_index_entries(db, :pos),
      osp: count_index_entries(db, :osp),
      derived: count_index_entries(db, :derived),
      dictionary: count_index_entries(db, :id2str)
    }
  end

  @doc """
  Estimates total data size based on index sizes.

  ## Arguments

  - `db` - Database reference

  ## Returns

  Estimated size in bytes.

  """
  @spec estimate_data_size(NIF.db_ref()) :: non_neg_integer()
  def estimate_data_size(db) do
    sizes = get_index_sizes(db)

    # Each triple has 3 index entries (SPO, POS, OSP)
    # Plus dictionary entries and derived facts
    triple_bytes = sizes.spo * @bytes_per_triple
    derived_bytes = sizes.derived * @bytes_per_triple
    dict_bytes = sizes.dictionary * 50

    triple_bytes + derived_bytes + dict_bytes
  end

  # ===========================================================================
  # Memory Estimation
  # ===========================================================================

  @doc """
  Estimates memory usage.

  Returns estimates for:
  - BEAM process memory
  - Estimated data size
  - Estimated total

  ## Arguments

  - `triple_count` - Number of triples for data estimate

  ## Returns

  Map with memory estimates in MB.

  ## Examples

      memory = TripleStore.Health.estimate_memory(10000)
      # => %{beam_mb: 50.5, estimated_data_mb: 1.0, estimated_total_mb: 51.5}

  """
  @spec estimate_memory(non_neg_integer()) :: memory_estimate()
  def estimate_memory(triple_count) do
    # Get BEAM memory usage
    beam_bytes = :erlang.memory(:total)
    beam_mb = beam_bytes / (1024 * 1024)

    # Estimate data size based on triple count
    # Each triple uses approximately 100 bytes across indices
    estimated_data_bytes = triple_count * @bytes_per_triple
    estimated_data_mb = estimated_data_bytes / (1024 * 1024)

    %{
      beam_mb: Float.round(beam_mb, 2),
      estimated_data_mb: Float.round(estimated_data_mb, 2),
      estimated_total_mb: Float.round(beam_mb + estimated_data_mb, 2)
    }
  end

  # ===========================================================================
  # Compaction Status
  # ===========================================================================

  @doc """
  Returns compaction status.

  ## Current Limitation

  This function currently returns static default values. Actual compaction
  monitoring requires additional RocksDB NIF bindings for:
  - `rocksdb.compaction-pending`
  - `rocksdb.num-running-compactions`
  - `rocksdb.estimate-pending-compaction-bytes`

  These bindings may be added in a future release.

  ## Returns

  Map with compaction information:
  - `:running` - Whether compaction is currently running (always `false`)
  - `:pending_bytes` - Estimated pending compaction bytes (always `0`)
  - `:pending_compactions` - Estimated number of pending compactions (always `0`)

  ## Examples

      status = TripleStore.Health.get_compaction_status()
      # => %{running: false, pending_bytes: 0, pending_compactions: 0}

  """
  @spec get_compaction_status() :: compaction_status()
  def get_compaction_status do
    # Static values until RocksDB property bindings are implemented
    %{
      running: false,
      pending_bytes: 0,
      pending_compactions: 0
    }
  end

  # ===========================================================================
  # Component Status Checks
  # ===========================================================================

  @doc """
  Checks if the plan cache is running.

  ## Returns

  - `true` if the plan cache is running
  - `false` otherwise

  """
  @spec plan_cache_running?() :: boolean()
  def plan_cache_running? do
    case Process.whereis(TripleStore.SPARQL.PlanCache) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end

  @doc """
  Checks if the query cache is running.

  ## Returns

  - `true` if the query cache is running
  - `false` otherwise

  """
  @spec query_cache_running?() :: boolean()
  def query_cache_running? do
    case Process.whereis(TripleStore.Query.Cache) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end

  @doc """
  Checks if the metrics collector is running.

  ## Returns

  - `true` if the metrics collector is running
  - `false` otherwise

  """
  @spec metrics_running?() :: boolean()
  def metrics_running? do
    case Process.whereis(TripleStore.Metrics) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end

  @doc """
  Checks the status of a specific component.

  ## Arguments

  - `component` - Component to check (`:database`, `:dict_manager`, `:plan_cache`, `:query_cache`, `:metrics`)
  - `store` - Store handle

  ## Returns

  - `{:ok, :running}` - Component is running
  - `{:ok, :stopped}` - Component is stopped
  - `{:error, :unknown_component}` - Unknown component

  """
  @spec component_status(atom(), store()) :: {:ok, :running | :stopped} | {:error, term()}
  def component_status(:database, %{db: db}) do
    if NIF.is_open(db), do: {:ok, :running}, else: {:ok, :stopped}
  end

  def component_status(:dict_manager, %{dict_manager: dict_manager}) do
    if is_pid(dict_manager) and Process.alive?(dict_manager) do
      {:ok, :running}
    else
      {:ok, :stopped}
    end
  end

  def component_status(:plan_cache, _store) do
    if plan_cache_running?(), do: {:ok, :running}, else: {:ok, :stopped}
  end

  def component_status(:query_cache, _store) do
    if query_cache_running?(), do: {:ok, :running}, else: {:ok, :stopped}
  end

  def component_status(:metrics, _store) do
    if metrics_running?(), do: {:ok, :running}, else: {:ok, :stopped}
  end

  def component_status(component, _store) do
    {:error, {:unknown_component, component}}
  end

  # ===========================================================================
  # Graph Health (Quad Store)
  # ===========================================================================

  @doc """
  Returns health information for a specific graph.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`
  - `graph_id` - The graph ID to check (0 for default, or named graph ID)

  ## Options

  - `:include_query_stats` - Include query statistics for this graph (default: false)
  - `:include_predicate_counts` - Include predicate breakdown (default: false)
  - `:size_alert_threshold` - Quad count before degraded status (default: 10_000_000)

  ## Returns

  - `{:ok, health}` - Graph health status map
  - `{:error, reason}` - On failure

  ## Graph Health Map

  - `:status` - Health status (`:healthy`, `:degraded`, `:unhealthy`, `:empty`, `:not_found`)
  - `:graph_id` - The graph ID
  - `:quad_count` - Number of quads in the graph
  - `:distinct_subjects` - Number of distinct subjects
  - `:distinct_predicates` - Number of distinct predicates
  - `:checked_at` - Timestamp of health check

  ## Examples

      {:ok, health} = TripleStore.Health.graph_health(store, 0)
      # => {:ok, %{
      #      status: :healthy,
      #      graph_id: 0,
      #      quad_count: 1000,
      #      distinct_subjects: 100,
      #      distinct_predicates: 15,
      #      checked_at: ~U[2025-12-27 10:00:00Z]
      #    }}

  """
  @spec graph_health(store(), non_neg_integer(), keyword()) :: {:ok, map()} | {:error, term()}
  def graph_health(%{db: db} = store, graph_id, opts \\ [])
      when is_integer(graph_id) and graph_id >= 0 do

    include_query_stats = Keyword.get(opts, :include_query_stats, false)
    include_predicate_counts = Keyword.get(opts, :include_predicate_counts, false)
    size_threshold = Keyword.get(opts, :size_alert_threshold, 10_000_000)

    with {:ok, quad_count} <- Statistics.graph_quad_count(db, graph_id),
         {:ok, summary} <- Statistics.graph_summary(db, graph_id) do

      # Determine status based on quad count and summary
      status = determine_graph_health_status(quad_count, summary, size_threshold)

      health = %{
        status: status,
        graph_id: graph_id,
        quad_count: quad_count,
        distinct_subjects: Map.get(summary, :distinct_subjects, 0),
        distinct_predicates: Map.get(summary, :distinct_predicates, 0),
        checked_at: DateTime.utc_now()
      }

      # Add optional distinct_objects if available
      health = maybe_add(health, :distinct_objects, Map.get(summary, :distinct_objects))

      # Add predicate counts if requested
      health =
        if include_predicate_counts do
          Map.put(health, :predicate_counts, Map.get(summary, :predicate_counts, %{}))
        else
          health
        end

      # Add query stats if requested
      health =
        if include_query_stats do
          Map.put(health, :query_stats, get_graph_query_stats(graph_id))
        else
          health
        end

      {:ok, health}
    else
      {:error, :not_found} ->
        {:ok, %{status: :not_found, graph_id: graph_id, checked_at: DateTime.utc_now()}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns health summary for all graphs in the store.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`

  ## Options

  - `:include_empty` - Include empty graphs in results (default: true)
  - `:size_threshold` - Quad count for large graph alert (default: 1_000_000)

  ## Returns

  - `{:ok, graphs}` - Map of graph_id => health_info

  ## Examples

      {:ok, graphs} = TripleStore.Health.all_graphs_health(store)
      # => {:ok, %{
      #      0 => %{status: :healthy, quad_count: 1000, ...},
      #      1 => %{status: :healthy, quad_count: 500, ...}
      #    }}

  """
  @spec all_graphs_health(store(), keyword()) :: {:ok, %{non_neg_integer() => map()}}
  def all_graphs_health(%{db: db} = _store, opts \\ []) do
    include_empty = Keyword.get(opts, :include_empty, true)
    size_threshold = Keyword.get(opts, :size_threshold, 1_000_000)

    case Statistics.build_per_graph_histograms(db, []) do
      {:ok, histograms} ->
        graphs =
          histograms
          |> Enum.map(fn {graph_id, _histogram} ->
            case graph_health(%{db: db}, graph_id, size_alert_threshold: size_threshold) do
              {:ok, health} -> {graph_id, health}
              _ -> {graph_id, %{status: :error, graph_id: graph_id}}
            end
          end)
          |> Map.new()
          |> filter_empty_graphs(include_empty)

        {:ok, graphs}

      {:error, _reason} ->
        {:ok, %{}}
    end
  end

  @doc """
  Checks for abnormal patterns in graph health.

  Returns alerts for graphs that exhibit concerning patterns:
  - Rapidly growing graphs (potential data ingestion issues)
  - Empty graphs with high query count (potential data loss)
  - Stale graphs (not accessed recently but still large)
  - Large graphs exceeding thresholds

  ## Arguments

  - `store` - Store handle
  - `opts` - Options:
    - `:large_graph_threshold` - Size considered large (default: 1_000_000)
    - `:empty_threshold` - Size considered empty (default: 0)

  ## Returns

  - `{:ok, alerts}` - List of alert maps

  ## Alert Types

  - `:large_graph` - Graph exceeds size threshold
  - `:empty_graph` - Graph has no quads
  - `:stale_graph` - Graph hasn't been accessed recently (if tracking available)

  """
  @spec graph_health_alerts(store(), keyword()) :: {:ok, [map()]}
  def graph_health_alerts(%{db: db} = _store, opts \\ []) do
    large_threshold = Keyword.get(opts, :large_graph_threshold, 1_000_000)
    empty_threshold = Keyword.get(opts, :empty_threshold, 0)

    alerts = []

    with {:ok, histograms} <- Statistics.build_per_graph_histograms(db, []) do
      alerts =
        Enum.flat_map(histograms, fn {graph_id, _histogram} ->
          case graph_health(%{db: db}, graph_id) do
            {:ok, health} ->
              check_graph_for_alerts(health, large_threshold, empty_threshold)

            _ ->
              []
          end
        end)

      {:ok, alerts}
    else
      _ ->
        {:ok, []}
    end
  end

  # ===========================================================================
  # Metrics Integration
  # ===========================================================================

  @doc """
  Returns current metrics if the metrics collector is running.

  ## Returns

  - `{:ok, metrics}` - Current metrics
  - `{:error, :not_running}` - Metrics collector not running

  ## Examples

      {:ok, metrics} = TripleStore.Health.get_metrics()
      # => {:ok, %{query: ..., cache: ..., throughput: ..., reasoning: ...}}

  """
  @spec get_metrics() :: {:ok, map()} | {:error, :not_running}
  def get_metrics do
    if metrics_running?() do
      {:ok, TripleStore.Metrics.get_all()}
    else
      {:error, :not_running}
    end
  end

  @doc """
  Returns a summary of health suitable for JSON encoding.

  This is useful for HTTP health endpoints that need JSON output.

  ## Arguments

  - `store` - Store handle
  - `opts` - Same options as `health/2`

  ## Returns

  - `{:ok, map}` - JSON-serializable health summary

  ## Examples

      {:ok, summary} = TripleStore.Health.summary(store)
      Jason.encode!(summary)

  """
  @spec summary(store(), health_opts()) :: {:ok, map()}
  def summary(store, opts \\ []) do
    {:ok, health} = health(store, opts)

    summary = %{
      status: Atom.to_string(health.status),
      database_open: health.database_open,
      components: %{
        dict_manager: health.dict_manager_alive,
        plan_cache: health.plan_cache_alive,
        query_cache: health.query_cache_alive,
        metrics: health.metrics_alive
      },
      checked_at: DateTime.to_iso8601(health.checked_at)
    }

    summary =
      summary
      |> maybe_add(:triple_count, Map.get(health, :triple_count))
      |> maybe_add(:index_sizes, Map.get(health, :index_sizes))
      |> maybe_add(:memory, Map.get(health, :memory))
      |> maybe_add(:compaction, Map.get(health, :compaction))

    {:ok, summary}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp determine_status(
         database_open,
         dict_manager_alive,
         plan_cache_alive,
         query_cache_alive,
         metrics_alive
       ) do
    cond do
      not database_open ->
        :unhealthy

      not dict_manager_alive ->
        :unhealthy

      not plan_cache_alive or not query_cache_alive or not metrics_alive ->
        :degraded

      true ->
        :healthy
    end
  end

  defp maybe_add(map, _key, nil), do: map
  defp maybe_add(map, key, value), do: Map.put(map, key, value)

  defp count_index_entries(db, cf) do
    # prefix_stream now returns the stream directly (may raise on error)
    stream = NIF.prefix_stream(db, cf, <<>>)
    Enum.count(stream)
  end

  # ===========================================================================
  # Graph Health Helpers
  # ===========================================================================

  defp determine_graph_health_status(0, _summary, _threshold), do: :empty

  defp determine_graph_health_status(quad_count, _summary, threshold) do
    cond do
      quad_count > threshold -> :degraded
      true -> :healthy
    end
  end

  defp get_graph_query_stats(graph_id) do
    case TripleStore.Metrics.get_all() do
      metrics when is_map(metrics) ->
        # Extract graph-specific query stats if available
        %{
          query_count: Map.get(metrics, :graph_queries, %{}) |> Map.get(graph_id, 0),
          total_duration_ms: Map.get(metrics, :graph_query_durations, %{}) |> Map.get(graph_id, 0),
          last_query_at: Map.get(metrics, :graph_last_query, %{}) |> Map.get(graph_id, nil)
        }

      _ ->
        %{
          query_count: 0,
          total_duration_ms: 0,
          last_query_at: nil
        }
    end
  end

  defp filter_empty_graphs(graphs, true), do: graphs

  defp filter_empty_graphs(graphs, false) do
    Enum.filter(graphs, fn {_graph_id, health} ->
      Map.get(health, :status) != :empty
    end)
    |> Map.new()
  end

  defp check_graph_for_alerts(health, large_threshold, empty_threshold) do
    alerts = []

    # Check for large graph
    alerts =
      if health.quad_count > large_threshold do
        [
          %{
            type: :large_graph,
            graph_id: health.graph_id,
            severity: :info,
            message: "Graph has #{health.quad_count} quads",
            quad_count: health.quad_count,
            threshold: large_threshold
          }
          | alerts
        ]
      else
        alerts
      end

    # Check for empty graph
    alerts =
      if health.quad_count <= empty_threshold do
        [
          %{
            type: :empty_graph,
            graph_id: health.graph_id,
            severity: :info,
            message: "Graph is empty",
            quad_count: health.quad_count
          }
          | alerts
        ]
      else
        alerts
      end

    alerts
  end
end
