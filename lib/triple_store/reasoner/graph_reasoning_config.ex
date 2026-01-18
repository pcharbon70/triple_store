defmodule TripleStore.Reasoner.GraphReasoningConfig do
  @moduledoc """
  Per-graph reasoning configuration for quad store reasoning.

  Each graph can have its own reasoning profile and scope,
  enabling fine-grained control over multi-tenant datasets.

  ## Reasoning Scope

  - `:local` - Graph reasons independently, inferences stay in source graph (default)
  - `:global` - Graph participates in global reasoning (cross-graph inference)
  - `:none` - No reasoning for this graph

  ## TBox Source

  - `:self` - Graph uses its own TBox (schema triples)
  - `:shared` - Graph uses shared TBox from configured graph
  - `graph_id` - Graph uses TBox from specific graph ID

  ## Storage Strategy

  - `:self` - Derived quads stored in same graph as explicit facts
  - `:separate` - Derived quads stored in separate inference graph

  ## Examples

      # Default configuration (graph-local reasoning)
      config = GraphReasoningConfig.new(graph_id: 1)

      # Global reasoning participant
      config = GraphReasoningConfig.new(graph_id: 1, scope: :global)

      # No reasoning for this graph
      config = GraphReasoningConfig.new(graph_id: 1, scope: :none)

      # Custom profile with shared TBox
      config = GraphReasoningConfig.new(
        graph_id: 1,
        scope: :local,
        profile: :rdfs,
        tbox_source: :shared
      )

      # Override with custom rules
      config = GraphReasoningConfig.new(
        graph_id: 1,
        scope: :local,
        rules: [:scm_sco, :cax_sco]
      )
  """

  alias TripleStore.Reasoner.{ReasoningConfig, ReasoningProfile}

  # ============================================================================
  # Types
  # ============================================================================

  @type scope :: :local | :global | :none

  @type t :: %__MODULE__{
          graph_id: non_neg_integer(),
          scope: scope(),
          profile: atom() | nil,
          rules: [atom()] | nil,
          exclude: [atom()] | nil,
          enabled: boolean(),
          tbox_source: :self | :shared | non_neg_integer(),
          store_inferred: :self | :separate,
          metadata: map()
        }

  defstruct [
    :graph_id,
    :scope,
    :profile,
    :rules,
    :exclude,
    :enabled,
    :tbox_source,
    :store_inferred,
    :metadata
  ]

  # ============================================================================
  # Public API - Creation
  # ============================================================================

  @doc """
  Creates a new graph reasoning configuration.

  ## Options

  - `:graph_id` - Graph identifier (required, term_id from dictionary)
  - `:scope` - Reasoning scope: `:local` (default), `:global`, or `:none`
  - `:profile` - Override reasoning profile for this graph (nil = use global default)
  - `:rules` - Custom rules for this graph (nil = use profile rules)
  - `:exclude` - Rules to exclude for this graph
  - `:enabled` - Whether reasoning is enabled (default: true)
  - `:tbox_source` - Where to get TBox: `:self` (default), `:shared`, or graph_id
  - `:store_inferred` - Where to store inferred quads: `:self` (default) or `:separate`
  - `:metadata` - Additional graph-specific metadata

  ## Examples

      # Default configuration for graph with ID 5
      config = GraphReasoningConfig.new(graph_id: 5)

      # Graph that participates in global reasoning
      config = GraphReasoningConfig.new(
        graph_id: 5,
        scope: :global,
        tbox_source: :shared
      )

      # Graph with RDFS-only reasoning
      config = GraphReasoningConfig.new(
        graph_id: 5,
        scope: :local,
        profile: :rdfs
      )

      # Graph with no reasoning
      config = GraphReasoningConfig.new(
        graph_id: 5,
        scope: :none
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_required(opts) do
      config = %__MODULE__{
        graph_id: Keyword.get(opts, :graph_id),
        scope: Keyword.get(opts, :scope, :local),
        profile: Keyword.get(opts, :profile),
        rules: Keyword.get(opts, :rules),
        exclude: Keyword.get(opts, :exclude, []),
        enabled: Keyword.get(opts, :enabled, true),
        tbox_source: Keyword.get(opts, :tbox_source, :self),
        store_inferred: Keyword.get(opts, :store_inferred, :self),
        metadata: Keyword.get(opts, :metadata, %{})
      }

      validate_config(config)
    end
  end

  @doc """
  Creates a new configuration, raising on error.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    case new(opts) do
      {:ok, config} -> config
      {:error, reason} -> raise ArgumentError, "Invalid graph reasoning config: #{inspect(reason)}"
    end
  end

  @doc """
  Creates a default configuration for the given graph ID.
  """
  @spec default(non_neg_integer()) :: t()
  def default(graph_id) when is_integer(graph_id) and graph_id >= 0 do
    %__MODULE__{
      graph_id: graph_id,
      scope: :local,
      profile: nil,
      rules: nil,
      exclude: [],
      enabled: true,
      tbox_source: :self,
      store_inferred: :self,
      metadata: %{}
    }
  end

  # ============================================================================
  # Public API - Resolution
  # ============================================================================

  @doc """
  Resolves the effective reasoning profile for this graph.

  Returns the graph-specific profile if set, or the global default.
  """
  @spec resolve_profile(t(), ReasoningConfig.t()) :: atom()
  def resolve_profile(%__MODULE__{profile: nil}, %ReasoningConfig{profile: profile}) do
    profile
  end

  def resolve_profile(%__MODULE__{profile: profile}, _global_config) when is_atom(profile) do
    profile
  end

  @doc """
  Resolves the effective rules for this graph.

  Returns graph-specific rules if set, or profile rules otherwise.
  """
  @spec resolve_rules(t(), ReasoningConfig.t()) :: {:ok, [atom()]} | {:error, term()}
  def resolve_rules(%__MODULE__{rules: rules} = config, _global_config)
      when is_list(rules) do
    # Graph has custom rules
    {:ok, Enum.reject(rules, &(&1 in config.exclude || []))}
  end

  def resolve_rules(%__MODULE__{profile: nil, rules: nil}, %ReasoningConfig{} = global_config) do
    # Use global profile
    ReasoningProfile.rules_for(global_config.profile, global_config.profile_opts)
  end

  def resolve_rules(%__MODULE__{profile: profile, rules: nil}, _global_config)
      when is_atom(profile) do
    # Use graph-specific profile
    ReasoningProfile.rules_for(profile, [])
  end

  @doc """
  Returns true if this graph should participate in reasoning.
  """
  @spec participates?(t()) :: boolean()
  def participates?(%__MODULE__{enabled: false}), do: false
  def participates?(%__MODULE__{scope: :none}), do: false
  def participates?(%__MODULE__{}), do: true

  @doc """
  Returns true if this graph uses local (graph-isolated) reasoning.
  """
  @spec local?(t()) :: boolean()
  def local?(%__MODULE__{scope: :local}), do: true
  def local?(%__MODULE__{}), do: false

  @doc """
  Returns true if this graph participates in global reasoning.
  """
  @spec global?(t()) :: boolean()
  def global?(%__MODULE__{scope: :global}), do: true
  def global?(%__MODULE__{}), do: false

  @doc """
  Returns true if this graph shares TBox with other graphs.
  """
  @spec shared_tbox?(t()) :: boolean()
  def shared_tbox?(%__MODULE__{tbox_source: :shared}), do: true
  def shared_tbox?(%__MODULE__{tbox_source: source})
      when is_integer(source) and source >= 0,
      do: true
  def shared_tbox?(%__MODULE__{}), do: false

  @doc """
  Returns the TBox graph ID for this graph.

  Returns `:self` if the graph uses its own TBox, or the graph ID
  of the TBox source if shared.
  """
  @spec tbox_graph_id(t()) :: :self | non_neg_integer()
  def tbox_graph_id(%__MODULE__{tbox_source: :self}), do: :self
  def tbox_graph_id(%__MODULE__{tbox_source: :shared}), do: 0  # Default to default graph
  def tbox_graph_id(%__MODULE__{tbox_source: graph_id}) when is_integer(graph_id), do: graph_id

  @doc """
  Returns true if derived quads should be stored separately from explicit.
  """
  @spec separate_inferred_graph?(t()) :: boolean()
  def separate_inferred_graph?(%__MODULE__{store_inferred: :separate}), do: true
  def separate_inferred_graph?(%__MODULE__{}), do: false

  # ============================================================================
  # Public API - Updates
  # ============================================================================

  @doc """
  Sets the reasoning scope for this graph.
  """
  @spec put_scope(t(), scope()) :: t()
  def put_scope(%__MODULE__{} = config, scope) when scope in [:local, :global, :none] do
    %{config | scope: scope}
  end

  @doc """
  Enables reasoning for this graph.
  """
  @spec enable(t()) :: t()
  def enable(%__MODULE__{} = config) do
    %{config | enabled: true}
  end

  @doc """
  Disables reasoning for this graph.
  """
  @spec disable(t()) :: t()
  def disable(%__MODULE__{} = config) do
    %{config | enabled: false}
  end

  @doc """
  Sets the profile for this graph.
  """
  @spec put_profile(t(), atom()) :: t()
  def put_profile(%__MODULE__{} = config, profile) when is_atom(profile) do
    %{config | profile: profile}
  end

  @doc """
  Sets custom rules for this graph.
  """
  @spec put_rules(t(), [atom()]) :: t()
  def put_rules(%__MODULE__{} = config, rules) when is_list(rules) do
    %{config | rules: rules}
  end

  @doc """
  Adds a rule exclusion to this graph.
  """
  @spec add_exclusion(t(), atom()) :: t()
  def add_exclusion(%__MODULE__{exclude: exclusions} = config, rule) when is_atom(rule) do
    %{config | exclude: [rule | exclusions] |> Enum.uniq()}
  end

  @doc """
  Sets the TBox source for this graph.
  """
  @spec put_tbox_source(t(), :self | :shared | non_neg_integer()) :: t()
  def put_tbox_source(%__MODULE__{} = config, source)
      when source == :self or source == :shared or (is_integer(source) and source >= 0) do
    %{config | tbox_source: source}
  end

  @doc """
  Sets the inferred quad storage strategy.
  """
  @spec put_store_inferred(t(), :self | :separate) :: t()
  def put_store_inferred(%__MODULE__{} = config, strategy)
      when strategy in [:self, :separate] do
    %{config | store_inferred: strategy}
  end

  # ============================================================================
  # Public API - Queries
  # ============================================================================

  @doc """
  Returns a summary of the graph configuration.
  """
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = config) do
    %{
      graph_id: config.graph_id,
      scope: config.scope,
      enabled: config.enabled,
      profile: config.profile,
      custom_rules: config.rules != nil,
      exclusions: config.exclude,
      tbox_source: config.tbox_source,
      store_inferred: config.store_inferred,
      participates: participates?(config)
    }
  end

  # ============================================================================
  # Persistent Term Storage
  # ============================================================================

  @doc """
  Stores a graph reasoning configuration in `:persistent_term` for fast access.

  Configuration is stored with a compound key allowing O(1) access from all processes.

  ## Parameters

  - `config` - The graph reasoning configuration to store
  - `store_key` - Unique identifier for the store (e.g., store path)

  ## Returns

  - `:ok` on success

  ## Examples

      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      :ok = GraphReasoningConfig.store(config, "/path/to/store")
  """
  @spec store(t(), term()) :: :ok
  def store(%__MODULE__{graph_id: graph_id} = config, store_key) do
    :persistent_term.put({__MODULE__, store_key, graph_id}, config)
    :ok
  end

  @doc """
  Loads a graph reasoning configuration from `:persistent_term`.

  ## Parameters

  - `store_key` - The unique identifier for the store
  - `graph_id` - The graph ID to load configuration for

  ## Returns

  - `{:ok, config}` if found
  - `{:error, :not_found}` if not stored

  ## Examples

      {:ok, config} = GraphReasoningConfig.load("/path/to/store", 1)
  """
  @spec load(term(), non_neg_integer()) :: {:ok, t()} | {:error, :not_found}
  def load(store_key, graph_id) do
    case :persistent_term.get({__MODULE__, store_key, graph_id}, nil) do
      nil -> {:error, :not_found}
      config -> {:ok, config}
    end
  end

  @doc """
  Loads a graph reasoning configuration from `:persistent_term`, returning default if not found.

  ## Parameters

  - `store_key` - The unique identifier for the store
  - `graph_id` - The graph ID to load configuration for

  ## Returns

  - The stored configuration or a default configuration for the graph

  ## Examples

      config = GraphReasoningConfig.load!("/path/to/store", 1)
  """
  @spec load!(term(), non_neg_integer()) :: t()
  def load!(store_key, graph_id) do
    case load(store_key, graph_id) do
      {:ok, config} -> config
      {:error, :not_found} -> default(graph_id)
    end
  end

  @doc """
  Removes a graph reasoning configuration from `:persistent_term`.

  ## Parameters

  - `store_key` - The unique identifier for the store
  - `graph_id` - The graph ID to remove configuration for

  ## Returns

  - `:ok`

  ## Examples

      :ok = GraphReasoningConfig.delete("/path/to/store", 1)
  """
  @spec delete(term(), non_neg_integer()) :: :ok
  def delete(store_key, graph_id) do
    :persistent_term.erase({__MODULE__, store_key, graph_id})
    :ok
  end

  @doc """
  Checks if a graph reasoning configuration exists in `:persistent_term`.

  ## Parameters

  - `store_key` - The unique identifier for the store
  - `graph_id` - The graph ID to check

  ## Returns

  - `true` if stored, `false` otherwise

  ## Examples

      exists? = GraphReasoningConfig.stored?("/path/to/store", 1)
  """
  @spec stored?(term(), non_neg_integer()) :: boolean()
  def stored?(store_key, graph_id) do
    case :persistent_term.get({__MODULE__, store_key, graph_id}, nil) do
      nil -> false
      _config -> true
    end
  end

  @doc """
  Lists all graph IDs for stored graph configurations for a given store.

  ## Parameters

  - `store_key` - The unique identifier for the store

  ## Returns

  - List of graph IDs for which configurations are stored

  ## Examples

      graph_ids = GraphReasoningConfig.list_stored_for_store("/path/to/store")
  """
  @spec list_stored_for_store(term()) :: [non_neg_integer()]
  def list_stored_for_store(store_key) do
    # Get all persistent_term keys and filter for this store
    :persistent_term.get()
    |> Enum.filter(fn
      {{__MODULE__, ^store_key, _graph_id}, _val} -> true
      _ -> false
    end)
    |> Enum.map(fn {{__MODULE__, _store_key, graph_id}, _val} -> graph_id end)
  end

  @doc """
  Clears all stored graph configurations for a given store.

  ## Parameters

  - `store_key` - The unique identifier for the store

  ## Returns

  - `:ok`

  ## Examples

      :ok = GraphReasoningConfig.clear_store("/path/to/store")
  """
  @spec clear_store(term()) :: :ok
  def clear_store(store_key) do
    Enum.each(list_stored_for_store(store_key), fn graph_id ->
      :persistent_term.erase({__MODULE__, store_key, graph_id})
    end)

    :ok
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp validate_required(opts) do
    if Keyword.has_key?(opts, :graph_id) do
      :ok
    else
      {:error, :graph_id_required}
    end
  end

  defp validate_config(%__MODULE__{graph_id: graph_id} = config)
      when is_integer(graph_id) and graph_id >= 0 do
    with :ok <- validate_scope(config.scope),
         :ok <- validate_tbox_source(config.tbox_source, graph_id),
         :ok <- validate_store_inferred(config.store_inferred) do
      {:ok, config}
    end
  end

  defp validate_config(_config), do: {:error, :invalid_graph_id}

  defp validate_scope(scope) when scope in [:local, :global, :none], do: :ok
  defp validate_scope(_scope), do: {:error, :invalid_scope}

  defp validate_tbox_source(:self, _graph_id), do: :ok
  defp validate_tbox_source(:shared, _graph_id), do: :ok
  defp validate_tbox_source(tbox_graph_id, graph_id)
      when is_integer(tbox_graph_id) and tbox_graph_id >= 0 and tbox_graph_id != graph_id,
      do: :ok
  defp validate_tbox_source(_tbox_source, _graph_id), do: {:error, :invalid_tbox_source}

  defp validate_store_inferred(strategy) when strategy in [:self, :separate], do: :ok
  defp validate_store_inferred(_strategy), do: {:error, :invalid_store_inferred}
end
