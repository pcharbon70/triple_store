defmodule TripleStore.Reasoner.GraphReasoningStatus do
  @moduledoc """
  Per-graph reasoning status tracking for quad store reasoning.

  This module tracks reasoning state for each graph independently,
  enabling granular status reporting and rematerialization.

  ## Status States

  - `:initialized` - Graph has been configured but not yet materialized
  - `:materialized` - Graph has been successfully materialized
  - `:stale` - Graph needs rematerialization (TBox or data changed)
  - `:error` - Last materialization failed

  ## Usage

      # Create a new status for a graph
      {:ok, status} = GraphReasoningStatus.new(graph_id: 1)

      # Record materialization
      status = GraphReasoningStatus.record_materialization(status, %{
        derived_count: 500,
        iterations: 3,
        duration_ms: 100
      })

      # Mark as stale
      status = GraphReasoningStatus.mark_stale(status)

      # Check if needs rematerialization
      GraphReasoningStatus.needs_rematerialization?(status)

  ## Storage

  Status can be stored in `:persistent_term` for fast access:

      GraphReasoningStatus.store(status, :graph_1)
      {:ok, status} = GraphReasoningStatus.load(:graph_1)

  Multiple graph statuses can be managed together:

      statuses = %{
        1 => status1,
        2 => status2,
        3 => status3
      }

      GraphReasoningStatus.store_all(statuses, :my_quad_store)
  """

  alias TripleStore.Reasoner.GraphReasoningConfig

  # ETS table for registry
  @registry_table :graph_reasoning_status_registry

  # ============================================================================
  # Types
  # ============================================================================

  @type state :: :initialized | :materialized | :stale | :error

  @type t :: %__MODULE__{
          graph_id: non_neg_integer(),
          config: GraphReasoningConfig.t() | nil,
          state: state(),
          derived_count: non_neg_integer(),
          explicit_count: non_neg_integer(),
          last_materialization: DateTime.t() | nil,
          materialization_count: non_neg_integer(),
          error: term() | nil,
          created_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  defstruct [
    :graph_id,
    :config,
    :state,
    :derived_count,
    :explicit_count,
    :last_materialization,
    :materialization_count,
    :error,
    :created_at,
    :updated_at
  ]

  @typedoc "Materialization statistics"
  @type materialization_stats :: %{
          derived_count: non_neg_integer(),
          iterations: non_neg_integer(),
          duration_ms: non_neg_integer(),
          rules_applied: non_neg_integer() | nil
        }

  # ============================================================================
  # Public API - Creation
  # ============================================================================

  @doc """
  Creates a new graph reasoning status.

  ## Options

  - `:graph_id` - Graph identifier (required)
  - `:config` - Graph reasoning configuration (optional)
  - `:explicit_count` - Number of explicit quads in graph (default: 0)

  ## Examples

      {:ok, status} = GraphReasoningStatus.new(graph_id: 1)

      {:ok, status} = GraphReasoningStatus.new(
        graph_id: 1,
        config: graph_config,
        explicit_count: 1000
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_graph_id(opts) do
      now = DateTime.utc_now()

      status = %__MODULE__{
        graph_id: Keyword.get(opts, :graph_id),
        config: Keyword.get(opts, :config),
        state: :initialized,
        derived_count: 0,
        explicit_count: Keyword.get(opts, :explicit_count, 0),
        last_materialization: nil,
        materialization_count: 0,
        error: nil,
        created_at: now,
        updated_at: now
      }

      {:ok, status}
    end
  end

  @doc """
  Creates a new status, raising on error.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    case new(opts) do
      {:ok, status} ->
        status

      {:error, reason} ->
        raise ArgumentError, "Invalid graph reasoning status: #{inspect(reason)}"
    end
  end

  @doc """
  Creates a default status for the given graph ID.
  """
  @spec default(non_neg_integer()) :: t()
  def default(graph_id) when is_integer(graph_id) and graph_id >= 0 do
    now = DateTime.utc_now()

    %__MODULE__{
      graph_id: graph_id,
      config: nil,
      state: :initialized,
      derived_count: 0,
      explicit_count: 0,
      last_materialization: nil,
      materialization_count: 0,
      error: nil,
      created_at: now,
      updated_at: now
    }
  end

  # ============================================================================
  # Public API - Status Updates
  # ============================================================================

  @doc """
  Records a materialization event with statistics.

  ## Examples

      status = GraphReasoningStatus.record_materialization(status, %{
        derived_count: 500,
        iterations: 3,
        duration_ms: 100
      })
  """
  @spec record_materialization(t(), materialization_stats()) :: t()
  def record_materialization(%__MODULE__{} = status, stats) do
    now = DateTime.utc_now()

    %{
      status
      | derived_count: Map.get(stats, :derived_count, status.derived_count),
        last_materialization: now,
        materialization_count: status.materialization_count + 1,
        state: :materialized,
        error: nil,
        updated_at: now
    }
  end

  @doc """
  Updates the explicit quad count for this graph.
  """
  @spec update_explicit_count(t(), non_neg_integer()) :: t()
  def update_explicit_count(%__MODULE__{} = status, count) do
    %{status | explicit_count: count, updated_at: DateTime.utc_now()}
  end

  @doc """
  Updates the derived quad count for this graph.
  """
  @spec update_derived_count(t(), non_neg_integer()) :: t()
  def update_derived_count(%__MODULE__{} = status, count) do
    %{status | derived_count: count, updated_at: DateTime.utc_now()}
  end

  @doc """
  Marks the status as stale, indicating rematerialization is needed.

  This should be called when:
  - TBox changes occur that invalidate current materialization
  - New explicit quads are added to the graph
  - Explicit quads are deleted from the graph
  """
  @spec mark_stale(t()) :: t()
  def mark_stale(%__MODULE__{} = status) do
    %{status | state: :stale, updated_at: DateTime.utc_now()}
  end

  @doc """
  Records an error in the reasoning subsystem for this graph.

  ## Examples

      status = GraphReasoningStatus.record_error(status, {:max_iterations_exceeded, 1000})
  """
  @spec record_error(t(), term()) :: t()
  def record_error(%__MODULE__{} = status, error) do
    %{status | state: :error, error: error, updated_at: DateTime.utc_now()}
  end

  @doc """
  Updates the configuration for this graph.
  """
  @spec update_config(t(), GraphReasoningConfig.t()) :: t()
  def update_config(%__MODULE__{} = status, config) do
    %{status | config: config, updated_at: DateTime.utc_now()}
  end

  @doc """
  Resets the status to initialized state.
  """
  @spec reset(t()) :: t()
  def reset(%__MODULE__{} = status) do
    now = DateTime.utc_now()

    %{
      status
      | state: :initialized,
        derived_count: 0,
        last_materialization: nil,
        materialization_count: 0,
        error: nil,
        updated_at: now
    }
  end

  # ============================================================================
  # Public API - Status Queries
  # ============================================================================

  @doc """
  Returns a summary of the graph reasoning status.
  """
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = status) do
    %{
      graph_id: status.graph_id,
      state: status.state,
      scope: scope(status),
      enabled: enabled(status),
      derived_count: status.derived_count,
      explicit_count: status.explicit_count,
      total_count: status.derived_count + status.explicit_count,
      last_materialization: status.last_materialization,
      materialization_count: status.materialization_count,
      needs_rematerialization: needs_rematerialization?(status),
      error: status.error,
      created_at: status.created_at,
      updated_at: status.updated_at
    }
  end

  @doc """
  Returns the scope from the configuration, or nil if no config.
  """
  @spec scope(t()) :: atom() | nil
  def scope(%__MODULE__{config: nil}), do: nil
  def scope(%__MODULE__{config: %GraphReasoningConfig{scope: scope}}), do: scope

  @doc """
  Returns whether reasoning is enabled for this graph.
  """
  @spec enabled(t()) :: boolean()
  def enabled(%__MODULE__{config: nil}), do: true
  def enabled(%__MODULE__{config: %GraphReasoningConfig{enabled: enabled}}), do: enabled

  @doc """
  Returns the current state.
  """
  @spec state(t()) :: state()
  def state(%__MODULE__{state: state}), do: state

  @doc """
  Returns true if the status indicates an error.
  """
  @spec error?(t()) :: boolean()
  def error?(%__MODULE__{state: :error}), do: true
  def error?(%__MODULE__{}), do: false

  @doc """
  Returns the error if present.
  """
  @spec error(t()) :: term() | nil
  def error(%__MODULE__{error: error}), do: error

  @doc """
  Returns the derived quad count.
  """
  @spec derived_count(t()) :: non_neg_integer()
  def derived_count(%__MODULE__{derived_count: count}), do: count

  @doc """
  Returns the explicit quad count.
  """
  @spec explicit_count(t()) :: non_neg_integer()
  def explicit_count(%__MODULE__{explicit_count: count}), do: count

  @doc """
  Returns the total quad count (explicit + derived).
  """
  @spec total_count(t()) :: non_neg_integer()
  def total_count(%__MODULE__{} = status) do
    status.derived_count + status.explicit_count
  end

  @doc """
  Returns the last materialization timestamp.
  """
  @spec last_materialization(t()) :: DateTime.t() | nil
  def last_materialization(%__MODULE__{last_materialization: time}), do: time

  @doc """
  Returns the number of times this graph has been materialized.
  """
  @spec materialization_count(t()) :: non_neg_integer()
  def materialization_count(%__MODULE__{materialization_count: count}), do: count

  @doc """
  Returns true if rematerialization is needed.

  Rematerialization is needed when:
  - State is `:stale`
  - State is `:initialized` and graph requires materialization
  """
  @spec needs_rematerialization?(t()) :: boolean()
  def needs_rematerialization?(%__MODULE__{state: :stale}), do: true

  def needs_rematerialization?(%__MODULE__{state: :initialized, config: config})
      when not is_nil(config) do
    GraphReasoningConfig.participates?(config)
  end

  def needs_rematerialization?(%__MODULE__{}), do: false

  @doc """
  Returns true if this graph participates in reasoning.
  """
  @spec participates?(t()) :: boolean()
  def participates?(%__MODULE__{config: nil}), do: true
  def participates?(%__MODULE__{config: config}), do: GraphReasoningConfig.participates?(config)

  @doc """
  Returns the time since last materialization in seconds.

  Returns nil if no materialization has occurred.
  """
  @spec time_since_materialization(t()) :: non_neg_integer() | nil
  def time_since_materialization(%__MODULE__{last_materialization: nil}), do: nil

  def time_since_materialization(%__MODULE__{last_materialization: time}) do
    DateTime.diff(DateTime.utc_now(), time, :second)
  end

  # ============================================================================
  # Public API - Persistent Term Storage
  # ============================================================================

  @doc """
  Stores a status in `:persistent_term` for fast access.

  ## Examples

      :ok = GraphReasoningStatus.store(status, :graph_1)
  """
  @spec store(t(), atom()) :: :ok
  def store(%__MODULE__{} = status, key) when is_atom(key) do
    register_key(key)
    :persistent_term.put({__MODULE__, key}, status)
    :ok
  end

  @doc """
  Loads a status from `:persistent_term`.
  """
  @spec load(atom()) :: {:ok, t()} | {:error, :not_found}
  def load(key) when is_atom(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> {:error, :not_found}
      status -> {:ok, status}
    end
  end

  @doc """
  Removes a status from `:persistent_term`.
  """
  @spec remove(atom()) :: :ok
  def remove(key) when is_atom(key) do
    unregister_key(key)
    :persistent_term.erase({__MODULE__, key})
    :ok
  end

  @doc """
  Deletes a status from `:persistent_term` (alias for remove/1).
  """
  @spec delete(atom()) :: :ok
  def delete(key) when is_atom(key), do: remove(key)

  @doc """
  Checks if a status exists in `:persistent_term`.
  """
  @spec exists?(atom()) :: boolean()
  def exists?(key) when is_atom(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> false
      _ -> true
    end
  end

  @doc """
  Lists all stored status keys.
  """
  @spec list_stored() :: [atom()]
  def list_stored do
    list_registry_keys()
  end

  @doc """
  Stores a map of graph statuses.
  """
  @spec store_all(%{non_neg_integer() => t()}, atom()) :: :ok
  def store_all(statuses, key) when is_atom(key) do
    :persistent_term.put({__MODULE__, key}, statuses)
    :ok
  end

  @doc """
  Loads a map of graph statuses.
  """
  @spec load_all(atom()) :: {:ok, %{non_neg_integer() => t()}} | {:error, :not_found}
  def load_all(key) when is_atom(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> {:error, :not_found}
      statuses -> {:ok, statuses}
    end
  end

  @doc """
  Removes all stored statuses for a given key.
  """
  @spec remove_all(atom()) :: :ok
  def remove_all(key) when is_atom(key) do
    :persistent_term.erase({__MODULE__, key})
    :ok
  end

  @doc """
  Returns aggregate statistics for a map of statuses.
  """
  @spec aggregate(%{non_neg_integer() => t()}) :: map()
  def aggregate(statuses) when is_map(statuses) do
    graph_count = map_size(statuses)

    materialized =
      statuses
      |> Enum.count(fn {_, s} -> s.state == :materialized end)

    stale =
      statuses
      |> Enum.count(fn {_, s} -> s.state == :stale end)

    error_count =
      statuses
      |> Enum.count(fn {_, s} -> s.state == :error end)

    total_derived =
      statuses
      |> Enum.map(fn {_, s} -> s.derived_count end)
      |> Enum.sum()

    total_explicit =
      statuses
      |> Enum.map(fn {_, s} -> s.explicit_count end)
      |> Enum.sum()

    %{
      total_graphs: graph_count,
      materialized: materialized,
      stale: stale,
      error: error_count,
      initialized: graph_count - materialized - stale - error_count,
      total_derived: total_derived,
      total_explicit: total_explicit,
      total_quads: total_derived + total_explicit
    }
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp validate_graph_id(opts) do
    graph_id = Keyword.get(opts, :graph_id)

    if is_integer(graph_id) and graph_id >= 0 do
      :ok
    else
      {:error, :invalid_graph_id}
    end
  end

  defp register_key(key) do
    ensure_registry_exists()
    :ets.insert(@registry_table, {key, true})
  end

  defp unregister_key(key) do
    if registry_exists?() do
      :ets.delete(@registry_table, key)
    end
  end

  defp ensure_registry_exists do
    unless registry_exists?() do
      try do
        :ets.new(@registry_table, [:set, :public, :named_table])
      rescue
        ArgumentError ->
          # Table already exists (race condition), that's fine
          :ok
      end
    end
  end

  defp registry_exists? do
    :ets.whereis(@registry_table) != :undefined
  end

  defp list_registry_keys do
    if registry_exists?() do
      :ets.tab2list(@registry_table) |> Enum.map(fn {key, _} -> key end)
    else
      []
    end
  end
end
