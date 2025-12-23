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

  """

  alias TripleStore.SPARQL.{Parser, Optimizer, Executor}

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Query execution context with db and dict_manager"
  @type context :: %{db: reference(), dict_manager: pid()}

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

  @typedoc "Query options"
  @type query_opts :: [
          timeout: pos_integer(),
          explain: boolean(),
          optimize: boolean(),
          stats: map()
        ]

  # Default timeout: 30 seconds
  @default_timeout 30_000

  # ===========================================================================
  # Public API
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
  @spec query(context(), String.t()) :: {:ok, query_result()} | {:error, term()}
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
          {:ok, query_result() | {:explain, explanation()}} | {:error, term()}
  def query(ctx, sparql, opts) when is_binary(sparql) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    # Run query with timeout protection
    task =
      Task.async(fn ->
        execute_query(ctx, sparql, opts)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} ->
        result

      nil ->
        {:error, :timeout}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
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
  # Streaming Query API
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

  ## Notes

  - Only SELECT queries are supported for streaming
  - The stream is lazy - no work is done until consumed
  - Early termination (e.g., `Enum.take/2`) is efficient
  - Backpressure is naturally handled by Elixir Streams

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
    optimize? = Keyword.get(opts, :optimize, true)
    stats = Keyword.get(opts, :stats, %{})
    project_vars = Keyword.get(opts, :variables, nil)

    with {:ok, ast} <- parse_query(sparql),
         :ok <- validate_select_query(ast),
         {:ok, _query_type, pattern, metadata} <- extract_query_info(ast),
         {:ok, optimized} <- maybe_optimize(pattern, optimize?, stats) do
      build_stream(ctx, optimized, metadata, project_vars)
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
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
  defp execute_pattern(ctx, pattern) do
    case pattern do
      {:bgp, triples} ->
        Executor.execute_bgp(ctx, triples)

      {:join, left, right} ->
        with {:ok, left_stream} <- execute_pattern(ctx, left),
             {:ok, right_stream} <- execute_pattern(ctx, right) do
          {:ok, Executor.hash_join(left_stream, right_stream)}
        end

      {:left_join, left, right, expr} ->
        with {:ok, left_stream} <- execute_pattern(ctx, left),
             {:ok, right_stream} <- execute_pattern(ctx, right) do
          opts =
            if expr do
              [filter: fn binding -> Executor.evaluate_filter(expr, binding) end]
            else
              []
            end

          {:ok, Executor.left_join(left_stream, right_stream, opts)}
        end

      {:union, left, right} ->
        with {:ok, left_stream} <- execute_pattern(ctx, left),
             {:ok, right_stream} <- execute_pattern(ctx, right) do
          {:ok, Executor.union(left_stream, right_stream)}
        end

      {:filter, expr, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          {:ok, Executor.filter(stream, expr)}
        end

      {:project, inner, vars} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          var_names = extract_variable_names(vars)
          {:ok, Executor.project(stream, var_names)}
        end

      {:distinct, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          {:ok, Executor.distinct(stream)}
        end

      {:reduced, inner} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          {:ok, Executor.reduced(stream)}
        end

      {:slice, inner, offset, limit} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          {:ok, Executor.slice(stream, offset || 0, limit)}
        end

      {:order_by, inner, order_conditions} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          comparators = convert_order_conditions(order_conditions)
          {:ok, Executor.order_by(stream, comparators)}
        end

      {:extend, inner, {:variable, var_name}, expr} ->
        with {:ok, stream} <- execute_pattern(ctx, inner) do
          extended =
            Stream.map(stream, fn binding ->
              case TripleStore.SPARQL.Expression.evaluate(expr, binding) do
                {:ok, value} -> Map.put(binding, var_name, value)
                :error -> binding
              end
            end)

          {:ok, extended}
        end

      nil ->
        # Empty pattern - return unit stream
        {:ok, Executor.unit_stream()}

      other ->
        {:error, {:unsupported_pattern, other}}
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
end
