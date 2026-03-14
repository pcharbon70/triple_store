defmodule TripleStore.Reasoner.GraphHelpers do
  @moduledoc """
  Helper functions for consistent graph ID and option handling.

  This module provides utilities for extracting and validating graph-related
  options across the reasoning subsystem. It consolidates the various patterns
  used throughout the codebase for:

  - Extracting `:graph_id` from options
  - Extracting `:tbox_graph_id` from options
  - Validating graph ID values
  - Handling default values

  ## Usage

  Instead of using `Keyword.get/3` or `Keyword.fetch!/2` directly, use these
  helpers for consistent behavior:

      # Get graph_id with default
      {:ok, graph_id} = GraphHelpers.graph_id(opts, default: 0)

      # Get graph_id with validation (returns :error if missing)
      case GraphHelpers.graph_id(opts) do
        {:ok, graph_id} -> ...
        :error -> ...
      end

      # Get TBox graph ID (defaults to :shared if not specified)
      {:ok, tbox_graph} = GraphHelpers.tbox_graph_id(opts, graph_id)

  ## Graph ID Values

  - **Non-negative integers**: Specific graph IDs (0, 1, 2, ...)
  - **`:global`**: Special value for global reasoning across all graphs
  - **`:shared`**: Special value for shared TBox across graphs

  """

  @type opts :: Keyword.t()
  @type graph_id :: non_neg_integer() | :global | :shared
  @type result :: {:ok, graph_id()} | :error

  # ============================================================================
  # Graph ID Extraction
  # ============================================================================

  @doc """
  Extracts and validates the `:graph_id` option.

  Returns `{:ok, graph_id}` if found and valid, or `:error` if missing or invalid.

  ## Examples

      iex> GraphHelpers.graph_id(graph_id: 1)
      {:ok, 1}

      iex> GraphHelpers.graph_id([])
      :error

      iex> GraphHelpers.graph_id(graph_id: -1)
      :error

  """
  @spec graph_id(opts()) :: result()
  def graph_id(opts) do
    case Keyword.get(opts, :graph_id) do
      nil -> :error
      graph_id when is_integer(graph_id) and graph_id >= 0 -> {:ok, graph_id}
      _graph_id -> :error
    end
  end

  @doc """
  Extracts the `:graph_id` option with a default value.

  ## Examples

      iex> GraphHelpers.graph_id([], default: 0)
      {:ok, 0}

      iex> GraphHelpers.graph_id(graph_id: 5, default: 0)
      {:ok, 5}

  """
  @spec graph_id(opts(), default: non_neg_integer()) :: result()
  def graph_id(opts, default: default) when is_integer(default) and default >= 0 do
    case Keyword.get(opts, :graph_id, default) do
      graph_id when is_integer(graph_id) and graph_id >= 0 -> {:ok, graph_id}
      _graph_id -> :error
    end
  end

  @doc """
  Extracts `:graph_id` option, raising if missing.

  Similar to `Keyword.fetch!/2` but validates the value.

  ## Examples

      iex> GraphHelpers.graph_id!(graph_id: 1)
      1

      iex> try do
      ...>   GraphHelpers.graph_id!([])
      ...> rescue
      ...>   error in KeyError -> error.key
      ...> end
      :graph_id

  """
  @spec graph_id!(opts()) :: graph_id()
  def graph_id!(opts) do
    case graph_id(opts) do
      {:ok, graph_id} -> graph_id
      :error -> raise KeyError, key: :graph_id, term: opts
    end
  end

  # ============================================================================
  # TBox Graph ID Extraction
  # ============================================================================

  @doc """
  Extracts the `:tbox_graph_id` option with intelligent defaults.

  ## Default Behavior

  - If `:tbox_graph_id` is explicitly set, use that value
  - If `:tbox_graph_id` is `nil` and no default graph, use `:shared`
  - If a `default_graph` is provided, use that instead of `:shared`

  ## Examples

      iex> GraphHelpers.tbox_graph_id([tbox_graph_id: 0], 1)
      {:ok, 0}

      iex> GraphHelpers.tbox_graph_id([], 1)
      {:ok, 1}

      iex> GraphHelpers.tbox_graph_id([], nil)
      {:ok, :shared}

  """
  @spec tbox_graph_id(opts(), default_graph :: non_neg_integer() | nil) :: result()
  def tbox_graph_id(opts, default_graph) do
    case Keyword.get(opts, :tbox_graph_id) do
      nil ->
        # No TBox graph specified - use shared or default graph
        if default_graph do
          {:ok, default_graph}
        else
          {:ok, :shared}
        end

      tbox_graph when is_integer(tbox_graph) and tbox_graph >= 0 ->
        {:ok, tbox_graph}

      :shared ->
        {:ok, :shared}

      _invalid ->
        :error
    end
  end

  # ============================================================================
  # Multiple Graph IDs
  # ============================================================================

  @doc """
  Extracts and validates the `:graph_ids` option (list of graph IDs).

  ## Examples

      iex> GraphHelpers.graph_ids(graph_ids: [0, 1, 2])
      {:ok, [0, 1, 2]}

      iex> GraphHelpers.graph_ids([])
      :error

  """
  @spec graph_ids(opts()) :: {:ok, [non_neg_integer()]} | :error
  def graph_ids(opts) do
    case Keyword.get(opts, :graph_ids) do
      ids when is_list(ids) ->
        if Enum.all?(ids, fn i -> is_integer(i) and i >= 0 end) do
          {:ok, ids}
        else
          :error
        end

      _ ->
        :error
    end
  end

  # ============================================================================
  # Validation
  # ============================================================================

  @doc """
  Validates a graph ID value.

  Returns `:ok` if valid, `:error` otherwise.

  ## Examples

      iex> GraphHelpers.valid_graph_id?(0)
      :ok

      iex> GraphHelpers.valid_graph_id?(:global)
      :ok

      iex> GraphHelpers.valid_graph_id?(-1)
      :error

  """
  @spec valid_graph_id?(any()) :: :ok | :error
  def valid_graph_id?(graph_id) when is_integer(graph_id) and graph_id >= 0, do: :ok
  def valid_graph_id?(:global), do: :ok
  def valid_graph_id?(:shared), do: :ok
  def valid_graph_id?(_), do: :error

  @doc """
  Checks if a value represents a valid graph reference.

  Valid references include non-negative integers and special atoms like
  `:global` and `:shared`.

  ## Examples

      iex> GraphHelpers.graph_ref?(0)
      true

      iex> GraphHelpers.graph_ref?(:global)
      true

      iex> GraphHelpers.graph_ref?(-1)
      false

      iex> GraphHelpers.graph_ref?(:invalid)
      false

  """
  @spec graph_ref?(any()) :: boolean()
  def graph_ref?(graph_id) when is_integer(graph_id), do: graph_id >= 0
  def graph_ref?(:global), do: true
  def graph_ref?(:shared), do: true
  def graph_ref?(_), do: false

  # ============================================================================
  # Scope Helpers
  # ============================================================================

  @doc """
  Extracts the `:scope` option with default validation.

  ## Valid Scopes

  - `:local` - Each graph reasons independently
  - `:global` - All graphs participate in single inference
  - `:hybrid` - Per-graph configuration

  ## Examples

      iex> GraphHelpers.scope([], default: :local)
      {:ok, :local}

      iex> GraphHelpers.scope(scope: :global)
      {:ok, :global}

      iex> GraphHelpers.scope(scope: :invalid)
      :error

  """
  @spec scope(opts(), keyword()) :: {:ok, :local | :global | :hybrid} | :error
  def scope(opts, options \\ []) do
    default = Keyword.get(options, :default, :local)
    validate_scope(Keyword.get(opts, :scope, default))
  end

  defp validate_scope(:local), do: {:ok, :local}
  defp validate_scope(:global), do: {:ok, :global}
  defp validate_scope(:hybrid), do: {:ok, :hybrid}
  defp validate_scope(_), do: :error

  # ============================================================================
  # Inference Graph Extraction
  # ============================================================================

  @doc """
  Extracts the `:inferred_graph` option.

  ## Values

  - `:separate` - Use separate inference graph (9999)
  - `nil` or `:self` - Store in same graph as premises (when implemented)
  - Integer - Specific graph ID for inferred facts

  ## Examples

      iex> GraphHelpers.inferred_graph(inferred_graph: 100)
      {:ok, 100}

      iex> GraphHelpers.inferred_graph(inferred_graph: :separate)
      {:ok, :separate}

      iex> GraphHelpers.inferred_graph([])
      {:ok, nil}

  """
  @spec inferred_graph(opts()) :: {:ok, non_neg_integer() | :separate | nil} | :error
  def inferred_graph(opts) do
    case Keyword.get(opts, :inferred_graph) do
      nil -> {:ok, nil}
      :separate -> {:ok, :separate}
      :self -> {:ok, nil}
      graph_id when is_integer(graph_id) and graph_id >= 0 -> {:ok, graph_id}
      _ -> :error
    end
  end
end
