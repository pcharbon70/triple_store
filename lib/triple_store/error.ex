defmodule TripleStore.Error do
  @moduledoc """
  Structured error types for the TripleStore.

  This module defines error types with consistent structure for:

  - **User-facing errors**: Sanitized messages safe for production
  - **Debug errors**: Detailed information for development
  - **Error codes**: Numeric codes for programmatic handling

  ## Error Categories

  - `1xxx` - Query errors (parse, timeout, limit exceeded)
  - `2xxx` - Database errors (open, close, IO)
  - `3xxx` - Reasoning errors (rule, iteration, consistency)
  - `4xxx` - Validation errors (input, configuration)
  - `5xxx` - System errors (internal, resource)

  ## Usage

      case TripleStore.query(store, sparql) do
        {:ok, results} ->
          results

        {:error, %TripleStore.Error{} = error} ->
          Logger.error("Query failed: \#{error.message}")
          error.code  # => 1001

        {:error, reason} ->
          # Legacy error handling
          Logger.error("Query failed: \#{inspect(reason)}")
      end

  ## Production Safety

  Use `safe_message/1` to get sanitized error messages:

      # Full message (for logging)
      error.message  # => "Parse error at line 5: unexpected token 'WHERE'"

      # Safe message (for user display)
      TripleStore.Error.safe_message(error)  # => "Invalid SPARQL syntax"

  """

  # ===========================================================================
  # Error Struct
  # ===========================================================================

  @type t :: %__MODULE__{
          code: pos_integer(),
          category: atom(),
          message: String.t(),
          safe_message: String.t(),
          details: map(),
          stacktrace: Exception.stacktrace() | nil
        }

  defexception [:code, :category, :message, :safe_message, :details, :stacktrace]

  @impl true
  def message(%__MODULE__{message: msg}), do: msg

  # ===========================================================================
  # Error Codes
  # ===========================================================================

  @error_codes %{
    # Query errors (1xxx)
    query_parse_error: 1001,
    query_timeout: 1002,
    query_limit_exceeded: 1003,
    query_unsupported: 1004,
    query_invalid_option: 1005,

    # Database errors (2xxx)
    database_open_failed: 2001,
    database_closed: 2002,
    database_io_error: 2003,
    database_corruption: 2004,

    # Reasoning errors (3xxx)
    reasoning_max_iterations: 3001,
    reasoning_rule_error: 3002,
    reasoning_inconsistency: 3003,

    # Validation errors (4xxx)
    validation_invalid_input: 4001,
    validation_invalid_config: 4002,
    validation_invalid_path: 4003,
    validation_file_not_found: 4004,
    validation_file_too_large: 4005,

    # System errors (5xxx)
    system_internal_error: 5001,
    system_resource_exhausted: 5002,
    system_not_implemented: 5003
  }

  @safe_messages %{
    query_parse_error: "Invalid SPARQL syntax",
    query_timeout: "Query execution timed out",
    query_limit_exceeded: "Query result limit exceeded",
    query_unsupported: "Unsupported query feature",
    query_invalid_option: "Invalid query option",
    database_open_failed: "Failed to open database",
    database_closed: "Database is closed",
    database_io_error: "Database I/O error",
    database_corruption: "Database corruption detected",
    reasoning_max_iterations: "Reasoning iteration limit reached",
    reasoning_rule_error: "Reasoning rule error",
    reasoning_inconsistency: "Ontology inconsistency detected",
    validation_invalid_input: "Invalid input",
    validation_invalid_config: "Invalid configuration",
    validation_invalid_path: "Invalid file path",
    validation_file_not_found: "File not found",
    validation_file_too_large: "File too large",
    system_internal_error: "Internal error",
    system_resource_exhausted: "Resource limit exceeded",
    system_not_implemented: "Feature not implemented"
  }

  # ===========================================================================
  # Error Constructors
  # ===========================================================================

  @doc """
  Creates a new error struct.

  ## Arguments

  - `category` - Error category atom (e.g., `:query_parse_error`)
  - `message` - Detailed error message
  - `opts` - Additional options:
    - `:details` - Map of additional details
    - `:stacktrace` - Exception stacktrace

  ## Examples

      error = TripleStore.Error.new(:query_parse_error, "Unexpected token at line 5")

  """
  @spec new(atom(), String.t(), keyword()) :: t()
  def new(category, message, opts \\ []) when is_atom(category) and is_binary(message) do
    code = Map.get(@error_codes, category, 5001)
    safe_msg = Map.get(@safe_messages, category, "An error occurred")

    %__MODULE__{
      code: code,
      category: category,
      message: message,
      safe_message: safe_msg,
      details: Keyword.get(opts, :details, %{}),
      stacktrace: Keyword.get(opts, :stacktrace)
    }
  end

  @doc """
  Creates a query parse error.
  """
  @spec query_parse_error(String.t(), map()) :: t()
  def query_parse_error(message, details \\ %{}) do
    new(:query_parse_error, message, details: details)
  end

  @doc """
  Creates a query timeout error.
  """
  @spec query_timeout(non_neg_integer()) :: t()
  def query_timeout(timeout_ms) do
    new(:query_timeout, "Query timed out after #{timeout_ms}ms",
      details: %{timeout_ms: timeout_ms}
    )
  end

  @doc """
  Creates a query limit exceeded error.
  """
  @spec query_limit_exceeded(String.t(), non_neg_integer(), non_neg_integer()) :: t()
  def query_limit_exceeded(limit_type, actual, limit) do
    new(:query_limit_exceeded, "#{limit_type} exceeded: #{actual} > #{limit}",
      details: %{limit_type: limit_type, actual: actual, limit: limit}
    )
  end

  @doc """
  Creates a database closed error.
  """
  @spec database_closed() :: t()
  def database_closed do
    new(:database_closed, "Database is closed")
  end

  @doc """
  Creates a database open failed error.
  """
  @spec database_open_failed(String.t(), term()) :: t()
  def database_open_failed(path, reason) do
    new(:database_open_failed, "Failed to open database at #{path}: #{inspect(reason)}",
      details: %{path: path, reason: reason}
    )
  end

  @doc """
  Creates a file not found error.
  """
  @spec file_not_found(String.t()) :: t()
  def file_not_found(path) do
    new(:validation_file_not_found, "File not found: #{path}", details: %{path: path})
  end

  @doc """
  Creates an internal error.
  """
  @spec internal_error(String.t(), Exception.stacktrace() | nil) :: t()
  def internal_error(message, stacktrace \\ nil) do
    new(:system_internal_error, message, stacktrace: stacktrace)
  end

  # ===========================================================================
  # Error Utilities
  # ===========================================================================

  @doc """
  Returns a safe message suitable for user display.

  Safe messages do not contain sensitive information like:
  - File paths
  - Internal structure details
  - Stacktraces

  ## Examples

      error = TripleStore.Error.query_parse_error("Syntax error at line 5, column 10")
      TripleStore.Error.safe_message(error)
      # => "Invalid SPARQL syntax"

  """
  @spec safe_message(t()) :: String.t()
  def safe_message(%__MODULE__{safe_message: msg}), do: msg

  @doc """
  Converts a legacy error tuple to a structured error.

  ## Examples

      {:error, :timeout} |> TripleStore.Error.from_legacy()
      # => {:error, %TripleStore.Error{category: :query_timeout, ...}}

  """
  @spec from_legacy({:error, term()}) :: {:error, t()}
  def from_legacy({:error, reason}) do
    error = reason_to_error(reason)
    {:error, error}
  end

  @doc """
  Returns the error code for a category.

  ## Examples

      TripleStore.Error.code_for(:query_timeout)
      # => 1002

  """
  @spec code_for(atom()) :: pos_integer()
  def code_for(category) when is_atom(category) do
    Map.get(@error_codes, category, 5001)
  end

  @doc """
  Returns all error codes.
  """
  @spec error_codes() :: %{atom() => pos_integer()}
  def error_codes, do: @error_codes

  @doc """
  Checks if an error is retriable.

  Retriable errors are transient and may succeed on retry.
  """
  @spec retriable?(t()) :: boolean()
  def retriable?(%__MODULE__{category: category}) do
    category in [:query_timeout, :database_io_error, :system_resource_exhausted]
  end

  # ===========================================================================
  # Error Conversion
  # ===========================================================================

  @doc """
  Converts a raw error reason to a structured `TripleStore.Error`.

  This is the canonical function for converting error reasons to errors.
  Use this instead of manually creating errors for common cases.

  ## Supported Reasons

  - `:timeout` - Query timeout
  - `:database_closed` - Database is closed
  - `:file_not_found` - File not found
  - `:path_traversal_attempt` - Path traversal security error
  - `:database_not_found` - Database doesn't exist
  - `:max_iterations_exceeded` - Reasoning max iterations
  - `{:parse_error, details}` - Parse error with details
  - `{:file_not_found, path}` - File not found with path
  - `{:invalid_format, details}` - Invalid format
  - `{:io_error, reason}` - IO error
  - Any other atom/term - Internal error

  ## Options

  - `:category` - Override the error category
  - `:details` - Additional details map

  ## Examples

      TripleStore.Error.from_reason(:timeout)
      # => %TripleStore.Error{category: :query_timeout, ...}

      TripleStore.Error.from_reason({:parse_error, "line 5"})
      # => %TripleStore.Error{category: :query_parse_error, ...}

  """
  @spec from_reason(term(), keyword()) :: t()
  def from_reason(reason, opts \\ [])

  def from_reason(:timeout, opts) do
    category = Keyword.get(opts, :category, :query_timeout)
    new(category, "Query timed out", opts)
  end

  def from_reason(:database_closed, opts) do
    category = Keyword.get(opts, :category, :database_closed)
    new(category, "Database is closed", opts)
  end

  def from_reason(:file_not_found, opts) do
    category = Keyword.get(opts, :category, :validation_file_not_found)
    new(category, "File not found", opts)
  end

  def from_reason(:path_traversal_attempt, opts) do
    category = Keyword.get(opts, :category, :validation_invalid_input)
    new(category, "Path traversal not allowed", opts)
  end

  def from_reason(:database_not_found, opts) do
    category = Keyword.get(opts, :category, :database_not_found)
    new(category, "Database does not exist", opts)
  end

  def from_reason(:max_iterations_exceeded, opts) do
    category = Keyword.get(opts, :category, :reasoning_max_iterations)
    new(category, "Reasoning exceeded maximum iterations", opts)
  end

  def from_reason({:parse_error, details}, opts) do
    category = Keyword.get(opts, :category, :query_parse_error)
    merged_opts = Keyword.update(opts, :details, %{raw: details}, &Map.put(&1, :raw, details))
    new(category, "Parse error: #{inspect(details)}", merged_opts)
  end

  def from_reason({:file_not_found, path}, opts) do
    category = Keyword.get(opts, :category, :validation_file_not_found)
    new(category, "File not found: #{path}", Keyword.put(opts, :details, %{path: path}))
  end

  def from_reason({:invalid_format, details}, opts) do
    category = Keyword.get(opts, :category, :data_parse_error)
    new(category, "Invalid format: #{inspect(details)}", opts)
  end

  def from_reason({:io_error, reason}, opts) do
    category = Keyword.get(opts, :category, :database_io_error)
    new(category, "IO error: #{inspect(reason)}", opts)
  end

  def from_reason(reason, opts) when is_atom(reason) do
    category = Keyword.get(opts, :category, :system_internal_error)
    new(category, "Error: #{reason}", opts)
  end

  def from_reason(reason, opts) do
    category = Keyword.get(opts, :category, :system_internal_error)
    new(category, "Error: #{inspect(reason)}", opts)
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp reason_to_error(reason), do: from_reason(reason)
end
