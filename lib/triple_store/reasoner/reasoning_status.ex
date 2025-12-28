defmodule TripleStore.Reasoner.ReasoningStatus do
  @moduledoc """
  Provides reasoning status information for monitoring and debugging.

  This module tracks and reports the current state of the reasoning subsystem,
  including:
  - Active profile and mode configuration
  - Derived triple counts
  - Materialization history and timing
  - Rule application statistics

  ## Usage

      # Create a new status tracker
      {:ok, status} = ReasoningStatus.new(config)

      # Record a materialization event
      status = ReasoningStatus.record_materialization(status, %{
        derived_count: 1500,
        iterations: 5,
        duration_ms: 250
      })

      # Get current status summary
      summary = ReasoningStatus.summary(status)

      # Check if rematerialization is needed
      ReasoningStatus.needs_rematerialization?(status)

  ## Storage

  Status can be stored in `:persistent_term` for fast access:

      ReasoningStatus.store(status, :my_ontology)
      {:ok, status} = ReasoningStatus.load(:my_ontology)
  """

  alias TripleStore.Reasoner.ReasoningConfig

  # ETS table for registry (provides atomic concurrent access)
  @registry_table :reasoning_status_registry

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Reasoning status structure"
  @type t :: %__MODULE__{
          config: ReasoningConfig.t() | nil,
          derived_count: non_neg_integer(),
          explicit_count: non_neg_integer(),
          last_materialization: DateTime.t() | nil,
          materialization_count: non_neg_integer(),
          last_materialization_stats: map() | nil,
          created_at: DateTime.t(),
          updated_at: DateTime.t(),
          state: :initialized | :materialized | :stale | :error,
          error: term() | nil
        }

  defstruct [
    :config,
    :derived_count,
    :explicit_count,
    :last_materialization,
    :materialization_count,
    :last_materialization_stats,
    :created_at,
    :updated_at,
    :state,
    :error
  ]

  @typedoc "Materialization statistics"
  @type materialization_stats :: %{
          derived_count: non_neg_integer(),
          iterations: non_neg_integer(),
          duration_ms: non_neg_integer(),
          rules_applied: non_neg_integer()
        }

  # ============================================================================
  # Public API - Creation
  # ============================================================================

  @doc """
  Creates a new reasoning status with the given configuration.

  ## Examples

      {:ok, status} = ReasoningStatus.new(config)
  """
  @spec new(ReasoningConfig.t() | nil) :: {:ok, t()}
  def new(config \\ nil) do
    now = DateTime.utc_now()

    status = %__MODULE__{
      config: config,
      derived_count: 0,
      explicit_count: 0,
      last_materialization: nil,
      materialization_count: 0,
      last_materialization_stats: nil,
      created_at: now,
      updated_at: now,
      state: :initialized,
      error: nil
    }

    {:ok, status}
  end

  @doc """
  Creates a new status, raising on error.
  """
  @spec new!(ReasoningConfig.t() | nil) :: t()
  def new!(config \\ nil) do
    {:ok, status} = new(config)
    status
  end

  # ============================================================================
  # Public API - Status Updates
  # ============================================================================

  @doc """
  Records a materialization event with statistics.

  ## Parameters

  - `status` - Current status
  - `stats` - Materialization statistics map with:
    - `:derived_count` - Number of derived triples
    - `:iterations` - Number of fixpoint iterations
    - `:duration_ms` - Time taken in milliseconds
    - `:rules_applied` - Number of rule applications (optional)

  ## Examples

      status = ReasoningStatus.record_materialization(status, %{
        derived_count: 1500,
        iterations: 5,
        duration_ms: 250
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
        last_materialization_stats: stats,
        updated_at: now,
        state: :materialized,
        error: nil
    }
  end

  @doc """
  Updates the explicit triple count.

  ## Examples

      status = ReasoningStatus.update_explicit_count(status, 5000)
  """
  @spec update_explicit_count(t(), non_neg_integer()) :: t()
  def update_explicit_count(%__MODULE__{} = status, count) do
    %{status | explicit_count: count, updated_at: DateTime.utc_now()}
  end

  @doc """
  Updates the derived triple count.

  ## Examples

      status = ReasoningStatus.update_derived_count(status, 1500)
  """
  @spec update_derived_count(t(), non_neg_integer()) :: t()
  def update_derived_count(%__MODULE__{} = status, count) do
    %{status | derived_count: count, updated_at: DateTime.utc_now()}
  end

  @doc """
  Marks the status as stale, indicating rematerialization is needed.

  This should be called when TBox changes occur that invalidate
  the current materialization.
  """
  @spec mark_stale(t()) :: t()
  def mark_stale(%__MODULE__{} = status) do
    %{status | state: :stale, updated_at: DateTime.utc_now()}
  end

  @doc """
  Records an error in the reasoning subsystem.

  ## Examples

      status = ReasoningStatus.record_error(status, {:max_iterations_exceeded, 1000})
  """
  @spec record_error(t(), term()) :: t()
  def record_error(%__MODULE__{} = status, error) do
    %{status | state: :error, error: error, updated_at: DateTime.utc_now()}
  end

  @doc """
  Updates the configuration.
  """
  @spec update_config(t(), ReasoningConfig.t()) :: t()
  def update_config(%__MODULE__{} = status, config) do
    %{status | config: config, updated_at: DateTime.utc_now()}
  end

  # ============================================================================
  # Public API - Status Queries
  # ============================================================================

  @doc """
  Returns a summary of the current reasoning status.

  ## Examples

      summary = ReasoningStatus.summary(status)
      # => %{
      #      state: :materialized,
      #      profile: :owl2rl,
      #      mode: :materialized,
      #      derived_count: 1500,
      #      explicit_count: 5000,
      #      total_count: 6500,
      #      ...
      #    }
  """
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = status) do
    %{
      state: status.state,
      profile: profile(status),
      mode: mode(status),
      derived_count: status.derived_count,
      explicit_count: status.explicit_count,
      total_count: status.derived_count + status.explicit_count,
      last_materialization: status.last_materialization,
      materialization_count: status.materialization_count,
      last_materialization_stats: status.last_materialization_stats,
      created_at: status.created_at,
      updated_at: status.updated_at,
      error: status.error
    }
  end

  @doc """
  Returns the active reasoning profile.
  """
  @spec profile(t()) :: atom() | nil
  def profile(%__MODULE__{config: nil}), do: nil
  def profile(%__MODULE__{config: config}), do: config.profile

  @doc """
  Returns the active reasoning mode.
  """
  @spec mode(t()) :: atom() | nil
  def mode(%__MODULE__{config: nil}), do: nil
  def mode(%__MODULE__{config: config}), do: config.mode

  @doc """
  Returns the derived triple count.
  """
  @spec derived_count(t()) :: non_neg_integer()
  def derived_count(%__MODULE__{derived_count: count}), do: count

  @doc """
  Returns the explicit triple count.
  """
  @spec explicit_count(t()) :: non_neg_integer()
  def explicit_count(%__MODULE__{explicit_count: count}), do: count

  @doc """
  Returns the total triple count (explicit + derived).
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
  Returns the last materialization statistics.
  """
  @spec last_materialization_stats(t()) :: map() | nil
  def last_materialization_stats(%__MODULE__{last_materialization_stats: stats}), do: stats

  @doc """
  Returns the current state.
  """
  @spec state(t()) :: :initialized | :materialized | :stale | :error
  def state(%__MODULE__{state: state}), do: state

  @doc """
  Returns true if rematerialization is needed.

  Rematerialization is needed when:
  - State is `:stale`
  - State is `:initialized` and mode requires materialization
  """
  @spec needs_rematerialization?(t()) :: boolean()
  def needs_rematerialization?(%__MODULE__{state: :stale}), do: true

  def needs_rematerialization?(%__MODULE__{state: :initialized, config: config})
      when not is_nil(config) do
    ReasoningConfig.requires_materialization?(config)
  end

  def needs_rematerialization?(_status), do: false

  @doc """
  Returns true if the status indicates an error.
  """
  @spec error?(t()) :: boolean()
  def error?(%__MODULE__{state: :error}), do: true
  def error?(_status), do: false

  @doc """
  Returns the error if present.
  """
  @spec error(t()) :: term() | nil
  def error(%__MODULE__{error: error}), do: error

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

      :ok = ReasoningStatus.store(status, :my_ontology)
  """
  @spec store(t(), atom()) :: :ok
  def store(%__MODULE__{} = status, key) when is_atom(key) do
    register_key(key)
    :persistent_term.put({__MODULE__, key}, status)
    :ok
  end

  @doc """
  Loads a status from `:persistent_term`.

  ## Examples

      {:ok, status} = ReasoningStatus.load(:my_ontology)
      {:ok, status} = ReasoningStatus.load("store_status_12345")
  """
  @spec load(atom() | String.t()) :: {:ok, t()} | {:error, :not_found}
  def load(key) when is_atom(key) or is_binary(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> {:error, :not_found}
      status -> {:ok, status}
    end
  end

  @doc """
  Removes a status from `:persistent_term`.
  """
  @spec remove(atom() | String.t()) :: :ok
  def remove(key) when is_atom(key) or is_binary(key) do
    unregister_key(key)
    :persistent_term.erase({__MODULE__, key})
    :ok
  end

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
  Removes all stored statuses.
  """
  @spec clear_all() :: :ok
  def clear_all do
    keys = list_stored()

    Enum.each(keys, fn key ->
      :persistent_term.erase({__MODULE__, key})
    end)

    # Clear the ETS registry
    if registry_exists?() do
      :ets.delete_all_objects(@registry_table)
    end

    :ok
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

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
