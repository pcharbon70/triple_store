defmodule TripleStore.SPARQL.Query do
  @moduledoc """
  Public SPARQL query interface.

  This module provides the main entry point for executing SPARQL queries against
  a triple store. It integrates the parser, optimizer, and executor into a
  unified query pipeline.

  ## Query Types

  Four SPARQL query forms are supported:

  - **SELECT**: Returns variable bindings as a list of maps
  - **ASK**: Returns a boolean indicating if solutions exist
  - **CONSTRUCT**: Returns an `RDF.Graph` built from a template
  - **DESCRIBE**: Returns an `RDF.Graph` with Concise Bounded Descriptions

  ## Examples

      # Execute a SELECT query
      {:ok, results} = Query.query(ctx, "SELECT ?name WHERE { ?s foaf:name ?name }")
      # => {:ok, [%{"name" => {:literal, :simple, "Alice"}}, ...]}

      # Execute an ASK query
      {:ok, exists} = Query.query(ctx, "ASK { ?s a foaf:Person }")
      # => {:ok, true}

      # Execute with options
      {:ok, results} = Query.query(ctx, sparql, timeout: 5000)

      # Get query explanation without executing
      {:ok, {:explain, info}} = Query.query(ctx, sparql, explain: true)

  ## Options

  - `:timeout` - Maximum execution time in milliseconds (default: 30000)
  - `:explain` - Return query plan instead of executing (default: false)
  - `:optimize` - Enable query optimization (default: true)
  - `:stats` - Statistics for optimizer (predicate cardinalities, etc.)
  - `:log` - Enable debug logging (default: false)

  ## Performance Characteristics

  - **SELECT**: O(n) where n is result set size
  - **ASK**: O(1) after finding first solution (short-circuits)
  - **CONSTRUCT/DESCRIBE**: O(n) result set + O(m) triple construction
  - **Prepared Queries**: Amortizes parsing/optimization over multiple executions

  ## Security Notes

  - Prepared queries should not be deserialized from untrusted sources
  - Parameter values are substituted at AST level (no injection risk)
  - Timeout protection prevents unbounded query execution

  ## Architecture Notes

  Query execution uses `Task.async/1` for timeout isolation. We intentionally
  do not use `Task.Supervisor` because:

  1. Queries are short-lived and self-contained
  2. The calling process handles task cleanup via `Task.shutdown/2`
  3. Error propagation is handled explicitly via return values
  4. Adding supervision would add complexity without clear benefit

  For long-running or high-concurrency scenarios, consider wrapping query
  calls in a dedicated `Task.Supervisor` at the application level.

  """

  alias TripleStore.SPARQL.{Parser, Optimizer, Executor, PropertyPath}
  require Logger

  # ===========================================================================
  # Types
  # Task 2.5.1: Query Execution
  # ===========================================================================

  @typedoc "Query execution context with db and dict_manager"
  @type context :: %{db: reference(), dict_manager: GenServer.server()}

  @typedoc "SELECT query result - list of variable bindings"
  @type select_result :: [%{String.t() => term()}]

  @typedoc "ASK query result - boolean"
  @type ask_result :: boolean()

  @typedoc "CONSTRUCT or DESCRIBE result - RDF graph"
  @type graph_result :: RDF.Graph.t()

  @typedoc "Query result based on query type"
  @type query_result :: select_result() | ask_result() | graph_result()

  @typedoc "Query explanation"
  @type explanation :: %{
          query_type: :select | :ask | :construct | :describe,
          original_pattern: term(),
          optimized_pattern: term(),
          variables: [String.t()],
          optimizations: map()
        }

  @typedoc "Error reasons returned by query functions"
  @type error_reason ::
          {:parse_error, term()}
          | {:exception, module(), String.t()}
          | :timeout
          | {:task_exit, term()}
          | {:missing_parameters, [String.t()]}
          | {:unsupported_pattern, term()}
          | {:unsupported_stream_type, atom()}
          | {:invalid_option, atom()}
          | :unsupported_query_type
          | :invalid_query

  @typedoc "Query options"
  @type query_opts :: [
          timeout: pos_integer(),
          explain: boolean(),
          optimize: boolean(),
          stats: map()
        ]

  # Valid query options for validation (S6: added :log option)
  @valid_query_opts [:timeout, :explain, :optimize, :stats, :log]
  @valid_prepare_opts [:optimize, :stats, :log]
  @valid_stream_opts [:optimize, :stats, :variables, :log]
  @valid_execute_opts [:timeout, :log]

  # Default timeout: 30 seconds
  @default_timeout 30_000

  # Maximum results from bind-join before triggering limit (DoS protection)
  @max_bind_join_results 1_000_000

  # Maximum pattern recursion depth to prevent stack overflow (DoS protection)
  @max_pattern_depth 100

  # ===========================================================================
  # Prepared Query Struct
  # Task 2.5.3: Prepared Queries
  # ===========================================================================

  @typedoc "Prepared query struct containing pre-compiled query components"
  @type prepared_query :: %__MODULE__.Prepared{
          sparql: String.t(),
          query_type: :select | :ask | :construct | :describe,
          pattern: term(),
          optimized_pattern: term(),
          metadata: prepared_metadata(),
          parameters: [String.t()]
        }

  @typedoc "Metadata stored in prepared queries"
  @type prepared_metadata :: %{
          variables: term() | nil,
          template: term() | nil,
          modifiers: modifier_map(),
          props: keyword()
        }

  @typedoc "Solution modifiers extracted from query"
  @type modifier_map :: %{
          distinct: boolean(),
          reduced: boolean(),
          order_by: term() | nil,
          limit: non_neg_integer() | nil,
          offset: non_neg_integer() | nil
        }

  defmodule Prepared do
    @moduledoc """
    Represents a prepared SPARQL query.

    Prepared queries cache the parsed and optimized algebra, allowing repeated
    execution with different parameter bindings without re-parsing or re-optimizing.

    ## Parameters

    Parameters are SPARQL variables that will be bound at execution time.
    They are identified by the `$` prefix in the query string (e.g., `$person`).

    ## Security Warning

    Prepared queries contain executable AST and should NOT be deserialized from
    untrusted sources. Only use prepared queries that were created within your
    application's trusted context.

    ## Example

        # Prepare a query with parameters
        {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person foaf:name ?name }")

        # Execute with different bindings
        {:ok, results1} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})
        {:ok, results2} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Bob"})

    """

    @enforce_keys [:sparql, :query_type, :pattern, :optimized_pattern, :metadata]
    defstruct [
      :sparql,
      :query_type,
      :pattern,
      :optimized_pattern,
      :metadata,
      parameters: []
    ]
  end

  # ===========================================================================
  # Public API
  # Task 2.5.1: Query Execution
  # ===========================================================================

  @doc """
  Executes a SPARQL query against the triple store.

  Parses the query string, optimizes the algebra, executes against the database,
  and returns results appropriate to the query type.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `sparql` - SPARQL query string

  ## Returns

  - `{:ok, results}` - Query results (type depends on query form)
  - `{:error, reason}` - Error with reason

  ## Examples

      {:ok, results} = Query.query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, true} = Query.query(ctx, "ASK { <http://ex.org/Alice> ?p ?o }")

  """
  @spec query(context(), String.t()) :: {:ok, query_result()} | {:error, error_reason()}
  def query(ctx, sparql) do
    query(ctx, sparql, [])
  end

  @doc """
  Executes a SPARQL query with options.

  ## Options

  - `:timeout` - Maximum execution time in milliseconds (default: 30000)
  - `:explain` - If true, returns query plan without executing (default: false)
  - `:optimize` - If false, skips optimization (default: true)
  - `:stats` - Statistics map for optimizer (predicate cardinalities)

  ## Examples

      # With timeout
      {:ok, results} = Query.query(ctx, sparql, timeout: 5000)

      # Get explanation
      {:ok, {:explain, info}} = Query.query(ctx, sparql, explain: true)

      # Disable optimization
      {:ok, results} = Query.query(ctx, sparql, optimize: false)

  """
  @spec query(context(), String.t(), query_opts()) ::
          {:ok, query_result() | {:explain, explanation()}} | {:error, error_reason()}
  def query(ctx, sparql, opts) when is_binary(sparql) and is_list(opts) do
    case validate_opts(opts, @valid_query_opts) do
      :ok ->
        with_timeout(opts, fn -> execute_query(ctx, sparql, opts) end)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Executes a SPARQL query, raising on error.

  Same as `query/3` but raises `RuntimeError` on failure.

  ## Examples

      results = Query.query!(ctx, "SELECT ?s WHERE { ?s ?p ?o }")

  """
  @spec query!(context(), String.t(), query_opts()) :: query_result() | {:explain, explanation()}
  def query!(ctx, sparql, opts \\ []) do
    case query(ctx, sparql, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end

  # ===========================================================================
  # Prepared Query API
  # Task 2.5.3: Prepared Queries
  # ===========================================================================

  @typedoc "Prepare options"
  @type prepare_opts :: [
          optimize: boolean(),
          stats: map()
        ]

  @typedoc "Parameter bindings for prepared query execution"
  @type params :: %{String.t() => term()}

  @doc """
  Prepares a SPARQL query for repeated execution.

  Parses and optimizes the query once, caching the compiled algebra for
  efficient repeated execution with different parameter bindings.

  ## Parameters

  Parameters are SPARQL variables with a `$` prefix in the query string.
  When executed, these are replaced with bound values from the params map.

  ## Arguments

  - `sparql` - SPARQL query string (may contain $param placeholders)

  ## Returns

  - `{:ok, prepared}` - Prepared query struct
  - `{:error, reason}` - Error with reason

  ## Examples

      # Prepare a parameterized query
      {:ok, prepared} = Query.prepare(\"\"\"
        SELECT ?name ?age WHERE {
          $person foaf:name ?name .
          $person foaf:age ?age
        }
      \"\"\")

      # Execute multiple times with different parameters
      {:ok, results} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})

  """
  @spec prepare(String.t()) :: {:ok, prepared_query()} | {:error, term()}
  def prepare(sparql) do
    prepare(sparql, [])
  end

  @doc """
  Prepares a SPARQL query with options.

  ## Options

  - `:optimize` - Enable query optimization (default: true)
  - `:stats` - Statistics map for optimizer

  ## Examples

      # Prepare without optimization
      {:ok, prepared} = Query.prepare(sparql, optimize: false)

      # Prepare with statistics for better optimization
      {:ok, prepared} = Query.prepare(sparql, stats: %{rdf_type: 10000})

  """
  @spec prepare(String.t(), prepare_opts()) :: {:ok, prepared_query()} | {:error, term()}
  def prepare(sparql, opts) when is_binary(sparql) and is_list(opts) do
    case validate_opts(opts, @valid_prepare_opts) do
      :ok ->
        optimize? = Keyword.get(opts, :optimize, true)
        stats = Keyword.get(opts, :stats, %{})

        # Extract parameters from the query (variables starting with $)
        {normalized_sparql, parameters} = extract_parameters(sparql)

        with {:ok, ast} <- parse_query(normalized_sparql),
             {:ok, query_type, pattern, metadata} <- extract_query_info(ast),
             {:ok, optimized} <- maybe_optimize(pattern, optimize?, stats) do
          prepared = %Prepared{
            sparql: sparql,
            query_type: query_type,
            pattern: pattern,
            optimized_pattern: optimized,
            metadata: metadata,
            parameters: parameters
          }

          {:ok, prepared}
        end

      {:error, _} = error ->
        error
    end
  rescue
    e -> {:error, {:exception, e.__struct__, Exception.message(e)}}
  end

  @doc """
  Prepares a SPARQL query, raising on error.

  Same as `prepare/2` but raises `RuntimeError` on failure.

  ## Examples

      prepared = Query.prepare!("SELECT ?name WHERE { ?s foaf:name ?name }")

  """
  @spec prepare!(String.t(), prepare_opts()) :: prepared_query()
  def prepare!(sparql, opts \\ []) do
    case prepare(sparql, opts) do
      {:ok, prepared} -> prepared
      {:error, reason} -> raise "Prepare failed: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a prepared query with parameter bindings.

  Uses the pre-compiled algebra from the prepared query, substituting
  parameter values from the params map.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `prepared` - Prepared query from `prepare/1`
  - `params` - Map of parameter names to values (optional, default: %{})

  ## Parameter Values

  Parameter values can be:
  - URIs as strings: `"http://example.org/resource"`
  - Literal tuples: `{:literal, :simple, "value"}`
  - Typed literals: `{:literal, {:typed, "xsd:integer"}, "42"}`

  ## Returns

  - `{:ok, results}` - Query results (type depends on query form)
  - `{:error, reason}` - Error with reason

  ## Examples

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person foaf:name ?name }")

      # Execute with parameter binding
      {:ok, results} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})

      # Execute without parameters (for non-parameterized queries)
      {:ok, results} = Query.execute(ctx, prepared)

  """
  @spec execute(context(), prepared_query(), params()) :: {:ok, query_result()} | {:error, term()}
  def execute(ctx, %Prepared{} = prepared, params \\ %{}) do
    execute(ctx, prepared, params, [])
  end

  @doc """
  Executes a prepared query with options.

  ## Options

  - `:timeout` - Maximum execution time in milliseconds (default: 30000)

  ## Examples

      {:ok, results} = Query.execute(ctx, prepared, params, timeout: 5000)

  """
  @spec execute(context(), prepared_query(), params(), keyword()) ::
          {:ok, query_result()} | {:error, term()}
  def execute(ctx, %Prepared{} = prepared, params, opts) when is_map(params) and is_list(opts) do
    # Validate options and parameters
    with :ok <- validate_opts(opts, @valid_execute_opts),
         :ok <- validate_parameters(prepared.parameters, params) do
      with_timeout(opts, fn -> execute_prepared(ctx, prepared, params) end)
    end
  end

  @doc """
  Executes a prepared query, raising on error.

  Same as `execute/4` but raises `RuntimeError` on failure.

  ## Examples

      results = Query.execute!(ctx, prepared, %{"person" => "http://ex.org/Alice"})

  """
  @spec execute!(context(), prepared_query(), params(), keyword()) :: query_result()
  def execute!(ctx, prepared, params \\ %{}, opts \\ []) do
    case execute(ctx, prepared, params, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise "Execute failed: #{inspect(reason)}"
    end
  end

  # Execute a prepared query with bound parameters
  defp execute_prepared(ctx, prepared, params) do
    # Substitute parameters in the optimized pattern
    pattern = substitute_parameters(prepared.optimized_pattern, params)

    # Execute and serialize
    execute_and_serialize(ctx, prepared.query_type, pattern, prepared.metadata)
  end

  # Extract $param placeholders and convert to regular ?param variables
  defp extract_parameters(sparql) do
    # Find all $param patterns (parameter names)
    param_regex = ~r/\$([a-zA-Z_][a-zA-Z0-9_]*)/
    params = Regex.scan(param_regex, sparql) |> Enum.map(fn [_, name] -> name end) |> Enum.uniq()

    # Replace $param with ?param for the parser
    normalized = Regex.replace(param_regex, sparql, "?\\1")

    {normalized, params}
  end

  # Validate that all required parameters are provided
  defp validate_parameters(required, provided) do
    missing = required -- Map.keys(provided)

    if Enum.empty?(missing) do
      :ok
    else
      {:error, {:missing_parameters, missing}}
    end
  end

  # Substitute parameter values into the pattern (C4: added is_map guard)
  defp substitute_parameters(pattern, params) when is_map(params) and map_size(params) == 0 do
    pattern
  end

  defp substitute_parameters(pattern, params) do
    do_substitute(pattern, params)
  end

  defp do_substitute({:bgp, triples}, params) do
    substituted_triples = Enum.map(triples, fn triple -> substitute_triple(triple, params) end)
    {:bgp, substituted_triples}
  end

  defp do_substitute({:join, left, right}, params) do
    {:join, do_substitute(left, params), do_substitute(right, params)}
  end

  defp do_substitute({:left_join, left, right, expr}, params) do
    {:left_join, do_substitute(left, params), do_substitute(right, params),
     substitute_expr(expr, params)}
  end

  defp do_substitute({:union, left, right}, params) do
    {:union, do_substitute(left, params), do_substitute(right, params)}
  end

  defp do_substitute({:filter, expr, inner}, params) do
    {:filter, substitute_expr(expr, params), do_substitute(inner, params)}
  end

  defp do_substitute({:project, inner, vars}, params) do
    {:project, do_substitute(inner, params), vars}
  end

  defp do_substitute({:distinct, inner}, params) do
    {:distinct, do_substitute(inner, params)}
  end

  defp do_substitute({:reduced, inner}, params) do
    {:reduced, do_substitute(inner, params)}
  end

  defp do_substitute({:slice, inner, offset, limit}, params) do
    {:slice, do_substitute(inner, params), offset, limit}
  end

  defp do_substitute({:order_by, inner, conditions}, params) do
    {:order_by, do_substitute(inner, params), conditions}
  end

  defp do_substitute({:extend, inner, var, expr}, params) do
    {:extend, do_substitute(inner, params), var, substitute_expr(expr, params)}
  end

  defp do_substitute({:minus, left, right}, params) do
    {:minus, do_substitute(left, params), do_substitute(right, params)}
  end

  defp do_substitute({:graph, graph_name, inner}, params) do
    {:graph, substitute_term(graph_name, params), do_substitute(inner, params)}
  end

  defp do_substitute(nil, _params), do: nil

  defp do_substitute(other, _params), do: other

  # Substitute parameters in a triple pattern
  defp substitute_triple({:triple, s, p, o}, params) do
    {:triple, substitute_term(s, params), substitute_term(p, params), substitute_term(o, params)}
  end

  defp substitute_triple({s, p, o}, params) do
    {substitute_term(s, params), substitute_term(p, params), substitute_term(o, params)}
  end

  # Substitute a term if it's a variable matching a parameter
  defp substitute_term({:variable, name}, params) do
    case Map.get(params, name) do
      nil -> {:variable, name}
      value -> convert_param_value(value)
    end
  end

  defp substitute_term(term, _params), do: term

  # Convert parameter value to the appropriate internal representation (C8: improved URI validation)
  defp convert_param_value(value) when is_binary(value) do
    if looks_like_uri?(value) do
      {:named_node, value}
    else
      {:literal, :simple, value}
    end
  end

  defp convert_param_value({:literal, _, _} = literal), do: literal
  defp convert_param_value({:named_node, _} = node), do: node
  defp convert_param_value({:blank_node, _} = node), do: node

  defp convert_param_value(value) when is_integer(value) do
    {:literal, {:typed, "http://www.w3.org/2001/XMLSchema#integer"}, Integer.to_string(value)}
  end

  defp convert_param_value(value) when is_float(value) do
    {:literal, {:typed, "http://www.w3.org/2001/XMLSchema#double"}, Float.to_string(value)}
  end

  defp convert_param_value(true) do
    {:literal, {:typed, "http://www.w3.org/2001/XMLSchema#boolean"}, "true"}
  end

  defp convert_param_value(false) do
    {:literal, {:typed, "http://www.w3.org/2001/XMLSchema#boolean"}, "false"}
  end

  defp convert_param_value(value), do: {:literal, :simple, inspect(value)}

  # Validate URI-like strings more robustly (C8)
  defp looks_like_uri?(value) do
    case URI.parse(value) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) ->
        # Validate host is not empty
        host != ""

      %URI{scheme: "urn", path: path} when is_binary(path) ->
        # URNs have scheme and path (urn:isbn:123)
        path != ""

      _ ->
        false
    end
  end

  # Substitute parameters in an expression
  defp substitute_expr(nil, _params), do: nil
  defp substitute_expr({:variable, name}, params), do: substitute_term({:variable, name}, params)

  defp substitute_expr({op, args}, params) when is_list(args) do
    {op, Enum.map(args, fn arg -> substitute_expr(arg, params) end)}
  end

  defp substitute_expr({op, arg1, arg2}, params) do
    {op, substitute_expr(arg1, params), substitute_expr(arg2, params)}
  end

  defp substitute_expr({op, arg}, params) when is_atom(op) do
    {op, substitute_expr(arg, params)}
  end

  defp substitute_expr(expr, _params), do: expr

  # ===========================================================================
  # Streaming Query API
  # Task 2.5.2: Query Streaming
  # ===========================================================================

  @typedoc "Streaming query result - lazy stream of bindings"
  @type stream_result :: Enumerable.t()

  @doc """
  Executes a SPARQL SELECT query and returns a lazy stream of bindings.

  Unlike `query/2` which materializes all results, this function returns a
  lazy `Stream` that produces bindings on demand. This is ideal for:

  - Large result sets that shouldn't be held in memory
  - Early termination (e.g., finding the first N matches)
  - Backpressure-aware processing pipelines

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `sparql` - SPARQL SELECT query string

  ## Returns

  - `{:ok, stream}` - Lazy stream of binding maps
  - `{:error, reason}` - Error with reason

  ## Important Limitations

  - Only SELECT queries are supported for streaming
  - The stream is lazy - no work is done until consumed
  - Early termination (e.g., `Enum.take/2`) is efficient
  - Backpressure is naturally handled by Elixir Streams
  - **No timeout protection**: The stream is lazy, so timeouts cannot be
    applied at setup time. The consumer controls execution timing.
  - **ORDER BY forces materialization**: When ORDER BY is used, the entire
    result set must be materialized in memory before streaming begins,
    negating the memory benefits for large result sets.

  ## Examples

      # Stream all results
      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      stream |> Enum.each(&process_binding/1)

      # Take only first 10 results efficiently
      {:ok, stream} = Query.stream_query(ctx, sparql)
      first_10 = stream |> Enum.take(10)

      # Process with backpressure using Flow
      {:ok, stream} = Query.stream_query(ctx, sparql)
      stream
      |> Flow.from_enumerable()
      |> Flow.map(&process/1)
      |> Flow.run()

  """
  @spec stream_query(context(), String.t()) :: {:ok, stream_result()} | {:error, term()}
  def stream_query(ctx, sparql) do
    stream_query(ctx, sparql, [])
  end

  @doc """
  Executes a SPARQL SELECT query with options and returns a lazy stream.

  ## Options

  - `:optimize` - Enable query optimization (default: true)
  - `:stats` - Statistics map for optimizer
  - `:variables` - Project to specific variables (default: all from query)

  Note: `:timeout` is not supported for streaming queries as the stream
  is lazy - timeout would only apply during setup, not consumption.

  ## Examples

      # Disable optimization
      {:ok, stream} = Query.stream_query(ctx, sparql, optimize: false)

      # Project to specific variables
      {:ok, stream} = Query.stream_query(ctx, sparql, variables: ["name", "age"])

  """
  @spec stream_query(context(), String.t(), keyword()) ::
          {:ok, stream_result()} | {:error, term()}
  def stream_query(ctx, sparql, opts) when is_binary(sparql) and is_list(opts) do
    case validate_opts(opts, @valid_stream_opts) do
      :ok ->
        optimize? = Keyword.get(opts, :optimize, true)
        stats = Keyword.get(opts, :stats, %{})
        project_vars = Keyword.get(opts, :variables, nil)

        with {:ok, ast} <- parse_query(sparql),
             :ok <- validate_select_query(ast),
             {:ok, _query_type, pattern, metadata} <- extract_query_info(ast),
             {:ok, optimized} <- maybe_optimize(pattern, optimize?, stats) do
          build_stream(ctx, optimized, metadata, project_vars)
        end

      {:error, _} = error ->
        error
    end
  rescue
    e -> {:error, {:exception, e.__struct__, Exception.message(e)}}
  end

  @doc """
  Executes a streaming SPARQL query, raising on error.

  Same as `stream_query/3` but raises `RuntimeError` on failure.

  ## Examples

      stream = Query.stream_query!(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      stream |> Enum.take(10)

  """
  @spec stream_query!(context(), String.t(), keyword()) :: stream_result()
  def stream_query!(ctx, sparql, opts \\ []) do
    case stream_query(ctx, sparql, opts) do
      {:ok, stream} -> stream
      {:error, reason} -> raise "Stream query failed: #{inspect(reason)}"
    end
  end

  # Validate that the query is a SELECT query (only SELECT supports streaming)
  defp validate_select_query({:select, _}), do: :ok
  defp validate_select_query({type, _}), do: {:error, {:unsupported_stream_type, type}}
  defp validate_select_query(_), do: {:error, :invalid_query}

  # Build lazy stream from pattern
  defp build_stream(ctx, pattern, metadata, project_vars) do
    case execute_pattern(ctx, pattern) do
      {:ok, stream} ->
        # Apply solution modifiers (these are all lazy except order_by)
        stream = apply_modifiers(stream, metadata.modifiers)

        # Project to specified variables if provided
        stream =
          case project_vars do
            nil ->
              # Use variables from query
              case metadata.variables do
                nil -> stream
                vars -> Executor.project(stream, extract_variable_names(vars))
              end

            vars when is_list(vars) ->
              Executor.project(stream, vars)
          end

        {:ok, stream}

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Query Execution Pipeline
  # ===========================================================================

  # Execute the full query pipeline
  defp execute_query(ctx, sparql, opts) do
    explain? = Keyword.get(opts, :explain, false)
    optimize? = Keyword.get(opts, :optimize, true)
    stats = Keyword.get(opts, :stats, %{})

    with {:ok, ast} <- parse_query(sparql),
         {:ok, query_type, pattern, metadata} <- extract_query_info(ast),
         {:ok, optimized} <- maybe_optimize(pattern, optimize?, stats) do
      if explain? do
        build_explanation(query_type, pattern, optimized, metadata)
      else
        execute_and_serialize(ctx, query_type, optimized, metadata)
      end
    end
  end

  # Parse the SPARQL query string
  defp parse_query(sparql) do
    case Parser.parse(sparql) do
      {:ok, ast} -> {:ok, ast}
      {:error, reason} -> {:error, {:parse_error, reason}}
    end
  end

  # Extract query type, pattern, and metadata from AST
  defp extract_query_info({query_type, props}) when query_type in [:select, :construct, :ask, :describe] do
    pattern = get_prop(props, "pattern")
    variables = get_prop(props, "variables")
    template = get_prop(props, "template")
    modifiers = extract_modifiers(props)

    metadata = %{
      variables: variables,
      template: template,
      modifiers: modifiers,
      props: props
    }

    {:ok, query_type, pattern, metadata}
  end

  defp extract_query_info(_) do
    {:error, :unsupported_query_type}
  end

  # Extract solution modifiers from props
  defp extract_modifiers(props) do
    %{
      distinct: get_prop(props, "distinct") == true,
      reduced: get_prop(props, "reduced") == true,
      order_by: get_prop(props, "order_by"),
      limit: get_prop(props, "limit"),
      offset: get_prop(props, "offset")
    }
  end

  # Optionally optimize the pattern
  defp maybe_optimize(pattern, false, _stats), do: {:ok, pattern}

  defp maybe_optimize(pattern, true, stats) do
    optimized = Optimizer.optimize(pattern, stats: stats)
    {:ok, optimized}
  end

  # Build explanation without executing
  defp build_explanation(query_type, original, optimized, metadata) do
    variables =
      case metadata.variables do
        nil -> Parser.extract_variables(original)
        vars -> extract_variable_names(vars)
      end

    explanation = %{
      query_type: query_type,
      original_pattern: original,
      optimized_pattern: optimized,
      variables: variables,
      modifiers: metadata.modifiers,
      optimizations: %{
        pattern_changed: original != optimized
      }
    }

    {:ok, {:explain, explanation}}
  end

  # Execute query and serialize results based on query type
  defp execute_and_serialize(ctx, query_type, pattern, metadata) do
    # Execute the pattern to get binding stream
    case execute_pattern(ctx, pattern) do
      {:ok, stream} ->
        # Apply solution modifiers
        stream = apply_modifiers(stream, metadata.modifiers)

        # Serialize based on query type
        serialize_results(ctx, query_type, stream, metadata)

      {:error, _} = error ->
        error
    end
  end

  # Execute a pattern and return a stream of bindings
  # Tracks recursion depth to prevent stack overflow from adversarial queries
  defp execute_pattern(ctx, pattern, depth \\ 0)

  defp execute_pattern(_ctx, _pattern, depth) when depth > @max_pattern_depth do
    {:error, {:pattern_depth_exceeded, depth}}
  end

  defp execute_pattern(ctx, pattern, depth) do
    case pattern do
      {:bgp, triples} ->
        Executor.execute_bgp(ctx, triples)

      {:join, left, right} ->
        # Check if either side is a path with a blank node subject
        # In that case, we need a bind-join where bindings are passed from the other side
        cond do
          path_with_blank_node_subject?(right) ->
            execute_bind_join_with_path(ctx, left, right, depth)

          path_with_blank_node_subject?(left) ->
            # Symmetric case: path on left, bind from right
            execute_bind_join_with_path(ctx, right, left, depth)

          true ->
            with {:ok, left_stream} <- execute_pattern(ctx, left, depth + 1),
                 {:ok, right_stream} <- execute_pattern(ctx, right, depth + 1) do
              {:ok, Executor.hash_join(left_stream, right_stream)}
            end
        end

      {:left_join, left, right, expr} ->
        with {:ok, left_stream} <- execute_pattern(ctx, left, depth + 1),
             {:ok, right_stream} <- execute_pattern(ctx, right, depth + 1) do
          opts =
            if expr do
              [filter: fn binding -> Executor.evaluate_filter(expr, binding) end]
            else
              []
            end

          {:ok, Executor.left_join(left_stream, right_stream, opts)}
        end

      {:union, left, right} ->
        with {:ok, left_stream} <- execute_pattern(ctx, left, depth + 1),
             {:ok, right_stream} <- execute_pattern(ctx, right, depth + 1) do
          {:ok, Executor.union(left_stream, right_stream)}
        end

      {:filter, expr, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          {:ok, Executor.filter(stream, expr)}
        end

      {:project, inner, vars} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          var_names = extract_variable_names(vars)
          {:ok, Executor.project(stream, var_names)}
        end

      {:distinct, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          {:ok, Executor.distinct(stream)}
        end

      {:reduced, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          {:ok, Executor.reduced(stream)}
        end

      {:slice, inner, offset, limit} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          {:ok, Executor.slice(stream, offset || 0, limit)}
        end

      {:order_by, inner, order_conditions} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          comparators = convert_order_conditions(order_conditions)
          {:ok, Executor.order_by(stream, comparators)}
        end

      {:extend, inner, {:variable, var_name}, expr} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          extended =
            Stream.map(stream, fn binding ->
              case TripleStore.SPARQL.Expression.evaluate(expr, binding) do
                {:ok, value} -> Map.put(binding, var_name, value)
                :error -> binding
              end
            end)

          {:ok, extended}
        end

      {:group, inner, group_vars, aggregates} ->
        with {:ok, stream} <- execute_pattern(ctx, inner, depth + 1) do
          grouped =
            if Enum.empty?(group_vars) do
              # Implicit grouping - all solutions form one group
              Executor.implicit_group(stream, aggregates)
            else
              # Explicit GROUP BY
              Executor.group_by(stream, group_vars, aggregates)
            end

          {:ok, grouped}
        end

      # Property path pattern
      {:path, subject, path_expr, object} ->
        PropertyPath.evaluate(%{db: ctx.db, dict_manager: ctx.dict_manager}, %{}, subject, path_expr, object)

      nil ->
        # Empty pattern - return unit stream
        {:ok, Executor.unit_stream()}

      other ->
        {:error, {:unsupported_pattern, other}}
    end
  end

  # Check if a pattern is a path with a blank node as subject
  # These require bind-join to pass bindings from the left side
  defp path_with_blank_node_subject?({:path, {:blank_node, _}, _path_expr, _object}), do: true
  defp path_with_blank_node_subject?(_), do: false

  # Execute a bind-join where left bindings are passed to the path evaluation
  # This is necessary when the path's subject is a blank node that gets bound by the left side
  #
  # NOTE: This function includes result limiting to prevent memory exhaustion
  # from adversarial queries that could produce billions of results.
  defp execute_bind_join_with_path(ctx, left, {:path, subject, path_expr, object}, depth) do
    with {:ok, left_stream} <- execute_pattern(ctx, left, depth + 1) do
      path_ctx = %{db: ctx.db, dict_manager: ctx.dict_manager}

      # For each left binding, evaluate the path with that binding
      # Apply a limit to prevent memory exhaustion from adversarial queries
      result_stream =
        left_stream
        |> Stream.flat_map(fn left_binding ->
          case PropertyPath.evaluate(path_ctx, left_binding, subject, path_expr, object) do
            {:ok, path_stream} ->
              # The path stream already has left_binding merged in
              Enum.to_list(path_stream)

            {:error, _} ->
              []
          end
        end)
        |> Stream.take(@max_bind_join_results)

      {:ok, result_stream}
    end
  end

  # Apply solution modifiers to stream
  defp apply_modifiers(stream, modifiers) do
    stream
    |> maybe_apply_distinct(modifiers.distinct)
    |> maybe_apply_reduced(modifiers.reduced)
    |> maybe_apply_order_by(modifiers.order_by)
    |> maybe_apply_slice(modifiers.offset, modifiers.limit)
  end

  defp maybe_apply_distinct(stream, true), do: Executor.distinct(stream)
  defp maybe_apply_distinct(stream, _), do: stream

  defp maybe_apply_reduced(stream, true), do: Executor.reduced(stream)
  defp maybe_apply_reduced(stream, _), do: stream

  defp maybe_apply_order_by(stream, nil), do: stream

  defp maybe_apply_order_by(stream, order_conditions) do
    comparators = convert_order_conditions(order_conditions)
    Executor.order_by(stream, comparators)
  end

  defp maybe_apply_slice(stream, nil, nil), do: stream
  defp maybe_apply_slice(stream, offset, limit), do: Executor.slice(stream, offset || 0, limit)

  # Convert order conditions to executor format
  defp convert_order_conditions(conditions) when is_list(conditions) do
    Enum.map(conditions, fn
      {:asc, {:variable, name}} -> {name, :asc}
      {:desc, {:variable, name}} -> {name, :desc}
      {:asc, expr} -> {expr, :asc}
      {:desc, expr} -> {expr, :desc}
    end)
  end

  defp convert_order_conditions(_), do: []

  # Serialize results based on query type
  defp serialize_results(_ctx, :select, stream, metadata) do
    var_names =
      case metadata.variables do
        nil -> nil
        vars -> extract_variable_names(vars)
      end

    results = Executor.to_select_results(stream, var_names)
    {:ok, results}
  end

  defp serialize_results(_ctx, :ask, stream, _metadata) do
    result = Executor.to_ask_result(stream)
    {:ok, result}
  end

  defp serialize_results(ctx, :construct, stream, metadata) do
    template = metadata.template || []
    Executor.to_construct_result(ctx, stream, template)
  end

  defp serialize_results(ctx, :describe, stream, metadata) do
    # DESCRIBE uses the variables from the projection to describe resources
    # The pattern is {:project, inner, vars} and vars tells us which variables to describe
    var_names = extract_describe_variables(metadata)

    Executor.to_describe_result(ctx, stream, var_names)
  end

  # Extract describe variables from pattern structure
  defp extract_describe_variables(metadata) do
    # First check if variables were explicitly set
    case metadata.variables do
      nil ->
        # For DESCRIBE, extract from props which has the raw pattern
        case get_prop(metadata.props, "pattern") do
          {:project, _inner, vars} -> extract_variable_names(vars)
          _ -> []
        end

      vars ->
        extract_variable_names(vars)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Extract variable names from variable tuples
  # Handles both {:variable, name} tuples and [variable: name] keyword lists
  defp extract_variable_names(vars) when is_list(vars) do
    Enum.flat_map(vars, fn
      {:variable, name} -> [name]
      {key, value} when is_atom(key) ->
        # Keyword list format like [variable: "s"]
        if key == :variable, do: [value], else: []
      name when is_binary(name) -> [name]
      _ -> []
    end)
  end

  defp extract_variable_names(_), do: []

  # Get property from props list
  defp get_prop(props, key) when is_list(props) do
    Enum.find_value(props, fn
      {^key, v} -> v
      _ -> nil
    end)
  end

  defp get_prop(_, _), do: nil

  # Validate options against allowed list (C5-C6)
  defp validate_opts(opts, valid_opts) do
    invalid = Keyword.keys(opts) -- valid_opts

    case invalid do
      [] -> :ok
      [opt | _] -> {:error, {:invalid_option, opt}}
    end
  end

  # Execute function with timeout protection (B1, S2)
  # Properly handles Task.yield return values including {:exit, reason}
  defp with_timeout(opts, fun) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    log? = Keyword.get(opts, :log, false)
    start_time = System.monotonic_time()

    # S6: Optional debug logging
    if log?, do: Logger.debug("[Query] Starting execution with timeout=#{timeout}ms")

    # S1: Telemetry start event
    :telemetry.execute(
      [:triple_store, :sparql, :query, :start],
      %{system_time: System.system_time()},
      %{timeout: timeout}
    )

    task = Task.async(fun)

    result =
      case Task.yield(task, timeout) do
        {:ok, result} ->
          result

        {:exit, reason} ->
          {:error, {:task_exit, reason}}

        nil ->
          Task.shutdown(task, :brutal_kill)
          {:error, :timeout}
      end

    # S1: Telemetry stop event
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:triple_store, :sparql, :query, :stop],
      %{duration: duration},
      %{timeout: timeout, result: result_status(result)}
    )

    # S6: Optional debug logging
    duration_ms = System.convert_time_unit(duration, :native, :millisecond)
    if log?, do: Logger.debug("[Query] Completed in #{duration_ms}ms with status=#{result_status(result)}")

    result
  rescue
    e ->
      # S1: Telemetry exception event (sanitized - no stacktrace exposure)
      sanitized = TripleStore.Telemetry.sanitize_exception(e, __STACKTRACE__)

      :telemetry.execute(
        [:triple_store, :sparql, :query, :exception],
        %{system_time: System.system_time()},
        sanitized
      )

      {:error, {:exception, e.__struct__, Exception.message(e)}}
  end

  defp result_status({:ok, _}), do: :ok
  defp result_status({:error, :timeout}), do: :timeout
  defp result_status({:error, _}), do: :error
end
