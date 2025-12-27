defmodule TripleStore.Application do
  @moduledoc """
  OTP Application for the TripleStore.

  Manages the supervision tree for:
  - Query plan caching
  - Configuration registry

  ## Supervision Strategy

  Uses `:one_for_one` strategy - if a child process crashes, only that
  process is restarted. This is appropriate because our services are
  independent of each other.

  ## Services

  The following services are started automatically:

  - `TripleStore.SPARQL.PlanCache` - LRU cache for query execution plans

  Database-dependent services (Statistics.Cache) are started dynamically
  when a database is opened via `TripleStore.open/2`.

  ## Dynamic Children

  Some services require a database reference and are started dynamically:

      # Start statistics cache for a specific database
      TripleStore.Application.start_stats_cache(db, name: MyStats)

  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      # Plan cache for SPARQL query optimization (no db dependency)
      {TripleStore.SPARQL.PlanCache, name: TripleStore.SPARQL.PlanCache}
    ]

    opts = [strategy: :one_for_one, name: TripleStore.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.debug("TripleStore application started")
        {:ok, pid}

      {:error, reason} = error ->
        Logger.error("Failed to start TripleStore application: #{inspect(reason)}")
        error
    end
  end

  @impl true
  def stop(_state) do
    Logger.debug("TripleStore application stopping")
    :ok
  end

  @doc """
  Starts a statistics cache for the given database.

  This is called dynamically when a database is opened to provide
  cached access to statistics.

  ## Options

  - `:name` - Process name for the cache (required)
  - `:refresh_interval` - Milliseconds between automatic refreshes (default: 60_000)

  ## Examples

      {:ok, cache} = TripleStore.Application.start_stats_cache(db, name: MyStats)

  """
  @spec start_stats_cache(reference(), keyword()) :: Supervisor.on_start_child()
  def start_stats_cache(db, opts) do
    name = Keyword.fetch!(opts, :name)
    child_spec = {TripleStore.Statistics.Cache, Keyword.merge(opts, db: db, name: name)}

    case Supervisor.start_child(TripleStore.Supervisor, child_spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} = error ->
        Logger.error("Failed to start stats cache: #{inspect(reason)}")
        error
    end
  end

  @doc """
  Stops a statistics cache.

  ## Examples

      :ok = TripleStore.Application.stop_stats_cache(MyStats)

  """
  @spec stop_stats_cache(atom()) :: :ok | {:error, term()}
  def stop_stats_cache(name) do
    case Supervisor.terminate_child(TripleStore.Supervisor, name) do
      :ok ->
        Supervisor.delete_child(TripleStore.Supervisor, name)
        :ok

      {:error, :not_found} ->
        :ok

      error ->
        error
    end
  end
end
