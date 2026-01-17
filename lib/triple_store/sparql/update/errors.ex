defmodule TripleStore.SPARQL.Update.Errors do
  @moduledoc """
  Specific error types for SPARQL UPDATE operations.

  This module provides structured error types that distinguish between
  different failure modes in UPDATE operations, making it easier for
  clients to handle errors appropriately.

  ## Error Types

  - `UnauthorizedError` - Authorization failure
  - `GraphNotFoundError` - Graph doesn't exist
  - `InvalidOperationError` - Operation validation failed
  - `QuotaExceededError` - Rate limit or quota exceeded
  - `ConflictError` - Graph conflict (COPY/MOVE to existing graph)
  - `SourceGraphNotFoundError` - Source graph doesn't exist (for COPY/MOVE/ADD)

  ## Usage

      case UpdateExecutor.execute_insert_data(ctx, quads) do
        {:ok, count} -> # Success
        {:error, %UnauthorizedError{operation: :insert_data, graph: graph}} ->
          # Handle authorization failure
        {:error, %GraphNotFoundError{graph: graph}} ->
          # Handle missing graph
        {:error, %InvalidOperationError{reason: reason}} ->
          # Handle validation error
      end

  """

  # ===========================================================================
  # Error Structs
  # ===========================================================================

  defmodule UnauthorizedError do
    @moduledoc """
    Authorization failure for an UPDATE operation.

    Raised when a user doesn't have the required permission to perform
    an operation on a graph.
    """
    defexception [:operation, :graph, :user, :required_permission]

    @impl true
    def exception(opts) do
      operation = Keyword.get(opts, :operation)
      graph = Keyword.get(opts, :graph)
      user = Keyword.get(opts, :user)
      required_permission = Keyword.get(opts, :required_permission)

      %__MODULE__{
        operation: operation,
        graph: graph,
        user: user,
        required_permission: required_permission
      }
    end

    @impl true
    def message(%__MODULE__{operation: op, graph: graph, required_permission: perm}) do
      "Not authorized: #{op} requires #{perm} permission on graph #{inspect(graph)}"
    end
  end

  defmodule GraphNotFoundError do
    @moduledoc """
    Error raised when an operation references a graph that doesn't exist.

    This is distinct from a general not found error because it specifically
    indicates the graph is missing, which may require different handling.
    """
    defexception [:graph, :operation]

    @impl true
    def exception(opts) do
      graph = Keyword.get(opts, :graph)
      operation = Keyword.get(opts, :operation)
      %__MODULE__{graph: graph, operation: operation}
    end

    @impl true
    def message(%__MODULE__{graph: graph, operation: op}) do
      "Graph not found: #{inspect(graph)} for operation #{op}"
    end
  end

  defmodule InvalidOperationError do
    @moduledoc """
    Error raised when an UPDATE operation fails validation.

    This covers various validation failures such as:
    - Invalid syntax
    - Invalid graph identifiers
    - Conflicting operations
    """
    defexception [:operation, :reason, :details]

    @impl true
    def exception(opts) do
      operation = Keyword.get(opts, :operation)
      reason = Keyword.get(opts, :reason)
      details = Keyword.get(opts, :details)

      %__MODULE__{
        operation: operation,
        reason: reason,
        details: details
      }
    end

    @impl true
    def message(%__MODULE__{operation: op, reason: reason}) do
      "Invalid operation: #{op} - #{reason}"
    end
  end

  defmodule QuotaExceededError do
    @moduledoc """
    Error raised when an operation exceeds rate limits or quotas.

    This indicates the user has performed too many operations in a given
    time period or exceeded their allowed quota.
    """
    defexception [:operation, :user, :quota_type, :limit]

    @impl true
    def exception(opts) do
      operation = Keyword.get(opts, :operation)
      user = Keyword.get(opts, :user)
      quota_type = Keyword.get(opts, :quota_type, :rate_limit)
      limit = Keyword.get(opts, :limit)

      %__MODULE__{
        operation: operation,
        user: user,
        quota_type: quota_type,
        limit: limit
      }
    end

    @impl true
    def message(%__MODULE__{quota_type: type, limit: limit}) do
      "Quota exceeded: #{type} - limit: #{limit}"
    end
  end

  defmodule ConflictError do
    @moduledoc """
    Error raised when a COPY or MOVE operation conflicts with an existing graph.

    This occurs when trying to COPY or MOVE to a graph that already exists
    and the `:error` conflict mode is specified.
    """
    defexception [:operation, :target_graph, :source_graph]

    @impl true
    def exception(opts) do
      operation = Keyword.get(opts, :operation)
      target_graph = Keyword.get(opts, :target_graph)
      source_graph = Keyword.get(opts, :source_graph)

      %__MODULE__{
        operation: operation,
        target_graph: target_graph,
        source_graph: source_graph
      }
    end

    @impl true
    def message(%__MODULE__{operation: op, target_graph: target}) do
      "Conflict: #{op} - target graph #{inspect(target)} already exists"
    end
  end

  defmodule SourceGraphNotFoundError do
    @moduledoc """
    Error raised when a COPY, MOVE, or ADD operation references a non-existent source graph.
    """
    defexception [:operation, :source_graph]

    @impl true
    def exception(opts) do
      operation = Keyword.get(opts, :operation)
      source_graph = Keyword.get(opts, :source_graph)

      %__MODULE__{
        operation: operation,
        source_graph: source_graph
      }
    end

    @impl true
    def message(%__MODULE__{operation: op, source_graph: source}) do
      "Source graph not found: #{inspect(source)} for operation #{op}"
    end
  end

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Converts an error reason tuple to a specific error struct.

  ## Examples

      iex> to_error({:unauthorized, :insert_data, graph, user, :write})
      %UnauthorizedError{operation: :insert_data, ...}

  """
  @spec to_error(term()) :: Exception.t()
  def to_error({:unauthorized, operation, graph, user, permission}) do
    UnauthorizedError.exception(
      operation: operation,
      graph: graph,
      user: user,
      required_permission: permission
    )
  end

  def to_error({:graph_not_found, graph}) do
    GraphNotFoundError.exception(graph: graph)
  end

  def to_error({:source_graph_not_found, source_graph}) do
    SourceGraphNotFoundError.exception(source_graph: source_graph)
  end

  def to_error({:invalid_operation, operation, reason}) do
    InvalidOperationError.exception(operation: operation, reason: reason)
  end

  def to_error({:invalid_operation, operation, reason, details}) do
    InvalidOperationError.exception(operation: operation, reason: reason, details: details)
  end

  def to_error({:quota_exceeded, operation, user, quota_type, limit}) do
    QuotaExceededError.exception(
      operation: operation,
      user: user,
      quota_type: quota_type,
      limit: limit
    )
  end

  def to_error({:conflict, operation, target_graph, source_graph}) do
    ConflictError.exception(
      operation: operation,
      target_graph: target_graph,
      source_graph: source_graph
    )
  end

  def to_error(other), do: other

  @doc """
  Checks if an error is of a specific type.

  ## Examples

      iex> error_type?(%UnauthorizedError{}, UnauthorizedError)
      true

      iex> error_type?(%GraphNotFoundError{}, UnauthorizedError)
      false

  """
  @spec error_type?(term(), module()) :: boolean()
  def error_type?(error, module) when is_exception(error) do
    error.__struct__ == module
  end

  def error_type?(_error, _module), do: false

  @doc """
  Returns a human-readable error message for any error.
  """
  @spec message(Exception.t() | term()) :: String.t()
  def message(%module{} = error) when module in [UnauthorizedError, GraphNotFoundError, InvalidOperationError, QuotaExceededError, ConflictError, SourceGraphNotFoundError] do
    Exception.message(error)
  end

  def message(other), do: inspect(other)
end
