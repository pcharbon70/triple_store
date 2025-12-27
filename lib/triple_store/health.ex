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

  @typedoc "Full health check result"
  @type health_result :: %{
          status: status(),
          triple_count: non_neg_integer(),
          database_open: boolean(),
          dict_manager_alive: boolean(),
          plan_cache_alive: boolean(),
          memory_estimate_mb: float() | nil,
          checked_at: DateTime.t()
        }

  @typedoc "Health check options"
  @type health_opts :: [
          include_stats: boolean(),
          include_memory: boolean()
        ]

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
  - `:include_memory` - Include memory estimate (default: false, can be slow)

  ## Returns

  - `{:ok, health}` - Health status map
  - `{:error, reason}` - On failure

  ## Health Map

  - `:status` - Overall health status (`:healthy`, `:degraded`, `:unhealthy`)
  - `:triple_count` - Number of triples (if `include_stats: true`)
  - `:database_open` - Whether database is open
  - `:dict_manager_alive` - Whether dictionary manager is alive
  - `:plan_cache_alive` - Whether plan cache is alive
  - `:memory_estimate_mb` - Estimated memory usage (if `include_memory: true`)
  - `:checked_at` - Timestamp of health check

  ## Examples

      {:ok, health} = TripleStore.Health.health(store)
      # => {:ok, %{
      #      status: :healthy,
      #      triple_count: 10000,
      #      database_open: true,
      #      dict_manager_alive: true,
      #      plan_cache_alive: true,
      #      checked_at: ~U[2025-12-27 10:00:00Z]
      #    }}

  """
  @spec health(store(), health_opts()) :: {:ok, health_result()} | {:error, term()}
  def health(store, opts \\ [])

  def health(%{db: db, dict_manager: dict_manager}, opts) do
    include_stats = Keyword.get(opts, :include_stats, true)
    include_memory = Keyword.get(opts, :include_memory, false)

    # Check component status
    database_open = NIF.is_open(db)
    dict_manager_alive = is_pid(dict_manager) and Process.alive?(dict_manager)
    plan_cache_alive = plan_cache_running?()

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

    # Get memory estimate if requested
    memory_estimate =
      if include_memory do
        estimate_memory_mb(db)
      else
        nil
      end

    # Determine overall status
    status = determine_status(database_open, dict_manager_alive, plan_cache_alive)

    health = %{
      status: status,
      database_open: database_open,
      dict_manager_alive: dict_manager_alive,
      plan_cache_alive: plan_cache_alive,
      checked_at: DateTime.utc_now()
    }

    # Add optional fields
    health =
      health
      |> maybe_add(:triple_count, triple_count)
      |> maybe_add(:memory_estimate_mb, memory_estimate)

    {:ok, health}
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
  Checks the status of a specific component.

  ## Arguments

  - `component` - Component to check (`:database`, `:dict_manager`, `:plan_cache`)
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

  def component_status(component, _store) do
    {:error, {:unknown_component, component}}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp determine_status(database_open, dict_manager_alive, plan_cache_alive) do
    cond do
      not database_open ->
        :unhealthy

      not dict_manager_alive ->
        :unhealthy

      not plan_cache_alive ->
        :degraded

      true ->
        :healthy
    end
  end

  defp maybe_add(map, _key, nil), do: map
  defp maybe_add(map, key, value), do: Map.put(map, key, value)

  defp estimate_memory_mb(_db) do
    # This would use RocksDB's memory usage estimation
    # For now, return nil as it requires additional NIF support
    nil
  end
end
