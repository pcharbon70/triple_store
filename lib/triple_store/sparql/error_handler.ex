defmodule TripleStore.SPARQL.ErrorHandler do
  @moduledoc """
  Structured error handling for SPARQL operations (S10).

  Provides consistent error types and handling patterns:
  - Query syntax errors
  - Execution errors
  - Timeout errors
  - Resource errors
  - Validation errors
  """

  @type error_kind :: :syntax | :execution | :timeout | :resource | :validation | :unknown

  # Exception Definitions

  defmodule SyntaxError do
    defexception [:message, :position, :query]

    def exception(message) when is_binary(message) do
      %__MODULE__{message: message}
    end

    def exception(args) when is_list(args) do
      message = Keyword.get(args, :message)
      position = Keyword.get(args, :position)
      query = Keyword.get(args, :query)

      msg = message || "SPARQL syntax error"

      %__MODULE__{
        message: msg,
        position: position,
        query: query
      }
    end

    def exception(message, args) when is_binary(message) and is_list(args) do
      position = Keyword.get(args, :position)
      query = Keyword.get(args, :query)

      %__MODULE__{
        message: message,
        position: position,
        query: query
      }
    end
  end

  defmodule ExecutionError do
    defexception [:message, :step, :algebra]

    def exception(message) when is_binary(message) do
      %__MODULE__{message: message}
    end

    def exception(args) when is_list(args) do
      message = Keyword.get(args, :message)
      step = Keyword.get(args, :step)
      algebra = Keyword.get(args, :algebra)

      msg = message || "Query execution error"

      %__MODULE__{
        message: msg,
        step: step,
        algebra: algebra
      }
    end

    def exception(message, args) when is_binary(message) and is_list(args) do
      step = Keyword.get(args, :step)
      algebra = Keyword.get(args, :algebra)

      %__MODULE__{
        message: message,
        step: step,
        algebra: algebra
      }
    end
  end

  defmodule TimeoutError do
    defexception [:message, :timeout_ms, :query]

    def exception(message) when is_binary(message) do
      %__MODULE__{message: message}
    end

    def exception(args) when is_list(args) do
      message = Keyword.get(args, :message)
      timeout_ms = Keyword.get(args, :timeout_ms)
      query = Keyword.get(args, :query)

      msg = message || "Query execution timeout"

      %__MODULE__{
        message: msg,
        timeout_ms: timeout_ms,
        query: query
      }
    end

    @impl true
    def exception(message, args) when is_binary(message) and is_list(args) do
      timeout_ms = Keyword.get(args, :timeout_ms)
      query = Keyword.get(args, :query)

      %__MODULE__{
        message: message,
        timeout_ms: timeout_ms,
        query: query
      }
    end
  end

  defmodule ValidationError do
    defexception [:message, :field, :value]

    def exception(message) when is_binary(message) do
      %__MODULE__{message: message}
    end

    def exception(args) when is_list(args) do
      message = Keyword.get(args, :message)
      field = Keyword.get(args, :field)
      value = Keyword.get(args, :value)

      msg = message || "Validation error"

      %__MODULE__{
        message: msg,
        field: field,
        value: value
      }
    end

    @impl true
    def exception(message, args) when is_binary(message) and is_list(args) do
      field = Keyword.get(args, :field)
      value = Keyword.get(args, :value)

      %__MODULE__{
        message: message,
        field: field,
        value: value
      }
    end
  end

  defmodule ResourceError do
    defexception [:message, :resource, :operation]

    def exception(message) when is_binary(message) do
      %__MODULE__{message: message}
    end

    def exception(args) when is_list(args) do
      message = Keyword.get(args, :message)
      resource = Keyword.get(args, :resource)
      operation = Keyword.get(args, :operation)

      msg = message || "Resource error"

      %__MODULE__{
        message: msg,
        resource: resource,
        operation: operation
      }
    end

    @impl true
    def exception(message, args) when is_binary(message) and is_list(args) do
      resource = Keyword.get(args, :resource)
      operation = Keyword.get(args, :operation)

      %__MODULE__{
        message: message,
        resource: resource,
        operation: operation
      }
    end
  end

  # Error Builder

  @doc """
  Builds a standardized error map.
  """
  def build_error(kind, message, context \\ %{}) do
    %{
      kind: kind,
      message: message,
      context: context,
      timestamp: System.system_time(:millisecond)
    }
  end

  @doc """
  Wraps an error in a tuple.
  """
  @spec wrap(term()) :: {:error, term()}
  def wrap(error), do: {:error, error}

  @doc """
  Wraps an error with additional context.
  """
  @spec wrap(term(), keyword()) :: {:error, term()}
  def wrap(error, context) when is_list(context) do
    {:error, Map.new(context) |> Map.put(:error, error)}
  end

  @doc """
  Checks if a value is an error tuple.
  """
  @spec error?(term()) :: boolean()
  def error?({:error, _}), do: true
  def error?(_), do: false

  @doc """
  Handles a result with a fallback.
  """
  @spec handle_or({:ok, any()} | {:error, term()}, any()) :: any() | no_return()
  def handle_or({:ok, value}, _default), do: value
  def handle_or({:error, _error}, default), do: default

  @doc """
  Handles a result with a function for errors.
  """
  @spec handle_or({:ok, any()} | {:error, term()}, any(), (term() -> any())) :: any()
  def handle_or({:ok, value}, _default, _fun), do: value
  def handle_or({:error, error}, _default, fun), do: fun.(error)

  @doc """
  Converts an exception to an error map.
  """
  @spec from_exception(Exception.t()) :: map()
  def from_exception(exception) do
    kind = exception_kind(exception)

    {:current_stacktrace, stacktrace} = Process.info(self(), :current_stacktrace)

    %{
      kind: kind,
      message: Exception.message(exception),
      type: exception.__struct__,
      stacktrace: stacktrace
    }
  end

  @doc """
  Formats an error for display.
  """
  @spec format(term()) :: String.t()
  def format({:error, error}) when is_exception(error) do
    "#{format_module_name(error.__struct__)}: #{Exception.message(error)}"
  end

  def format({:error, error}) when is_binary(error) do
    error
  end

  def format({:error, error}) when is_map(error) do
    error[:message] || inspect(error)
  end

  def format(error) when is_exception(error) do
    "#{format_module_name(error.__struct__)}: #{Exception.message(error)}"
  end

  def format(error) when is_binary(error) do
    error
  end

  def format(error), do: inspect(error)

  # Private Helpers

  defp format_module_name(module) do
    module
    |> Module.split()
    |> Enum.join(".")
  end

  defp exception_kind(%SyntaxError{}), do: :syntax
  defp exception_kind(%ExecutionError{}), do: :execution
  defp exception_kind(%TimeoutError{}), do: :timeout
  defp exception_kind(%ValidationError{}), do: :validation
  defp exception_kind(%ResourceError{}), do: :resource
  defp exception_kind(_), do: :unknown
end
