defmodule TripleStore.SPARQL.UpdateExecutor do
  @moduledoc """
  SPARQL UPDATE operation executor.

  This module executes SPARQL UPDATE operations against the triple/quad store,
  providing support for all SPARQL 1.1 Update operations:

  - **INSERT DATA**: Direct insertion of ground triples/quads
  - **DELETE DATA**: Direct deletion of ground triples/quads
  - **DELETE WHERE**: Pattern-based deletion
  - **INSERT WHERE**: Pattern-based insertion (using templates)
  - **DELETE/INSERT WHERE**: Combined delete and insert in single operation
  - **CREATE/DROP/CLEAR GRAPH**: Named graph management
  - **COPY/MOVE/ADD**: Bulk graph operations

  ## Quad Store Support

  For quad stores (schema: :quad), all operations support named graphs:
  - INSERT DATA can target specific named graphs
  - DELETE DATA can target specific named graphs
  - MODIFY operations can use GRAPH clauses in WHERE patterns
  - CREATE/DROP/CLEAR GRAPH manage named graphs
  - COPY/MOVE/ADD transfer quads between graphs

  For triple stores, operations work on the default graph only.

  ## Execution Model

  All update operations are executed atomically using RocksDB's WriteBatch.
  Operations that involve WHERE clauses first query the database to find
  matching bindings, then apply those bindings to templates to generate
  the actual triples/quads to insert or delete.

  ## Cache Invalidation

  After successful graph modifications (CREATE, DROP, CLEAR, COPY, MOVE, ADD),
  the query cache is automatically invalidated if running.

  ## Usage

      # Parse and execute an update
      {:ok, ast} = Parser.parse_update("INSERT DATA { <s> <p> <o> }")
      {:ok, count} = UpdateExecutor.execute(ctx, ast)

      # Execute specific operations
      {:ok, count} = UpdateExecutor.execute_insert_data(ctx, quads)
      {:ok, count} = UpdateExecutor.execute_delete_where(ctx, pattern)

      # Graph operations (quad stores only)
      {:ok, 0} = UpdateExecutor.execute_create_graph(ctx, graph_iri)
      {:ok, count} = UpdateExecutor.execute_copy(ctx, source_graph, target_graph)

  ## Security

  - All operations validate input size to prevent DoS
  - Pattern-based operations have result limits
  - Templates are validated before execution
  - **Authorization**: All UPDATE operations check permissions via the Authorization module

  ### Authorization

  UPDATE operations require the following permissions:

  - **INSERT DATA**: `:write` permission on target graph(s)
  - **DELETE DATA**: `:write` permission on target graph(s)
  - **MODIFY (DELETE/INSERT WHERE)**: `:write` permission on affected graph(s)
  - **CREATE GRAPH**: `:admin` permission on graph
  - **DROP GRAPH**: `:admin` permission on graph
  - **CLEAR GRAPH**: `:write` permission on graph
  - **COPY**: `:read` permission on source, `:write` permission on target
  - **MOVE**: `:admin` permission on both source and target
  - **ADD**: `:read` permission on source, `:write` permission on target

  To provide a user context, include `:user` in the execution context:

      ctx = %{
        db: db,
        dict_manager: dict_manager,
        user: %{id: "user123", roles: [:editor]}
      }

  For internal operations where authorization should be bypassed (e.g., maintenance),
  omit the `:user` key from the context.

  The default graph (`:default`) is always writable without explicit authorization.

  ## Architecture

  The UpdateExecutor delegates to specialized modules for each operation type:
  - `TripleStore.SPARQL.Update.InsertData` - INSERT DATA operations
  - `TripleStore.SPARQL.Update.DeleteData` - DELETE DATA and DELETE WHERE operations
  - `TripleStore.SPARQL.Update.Modify` - MODIFY (DELETE/INSERT WHERE) operations
  - `TripleStore.SPARQL.Update.GraphOperations` - CREATE/DROP/CLEAR/COPY/MOVE/ADD operations
  - `TripleStore.SPARQL.Update.Helpers` - Common utilities (authorization, conversion, etc.)

  """

  alias TripleStore.Query.Cache, as: QueryCache
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.Update.DeleteData
  alias TripleStore.SPARQL.Update.GraphOperations
  alias TripleStore.SPARQL.Update.InsertData
  alias TripleStore.SPARQL.Update.Modify

  # Suppress MapSet opaque type warnings in predicate extraction functions
  @dialyzer {:nowarn_function, extract_predicates_from_operations: 1}
  @dialyzer {:nowarn_function, extract_predicates_from_operation: 1}
  @dialyzer {:nowarn_function, extract_predicates_from_quads: 1}
  @dialyzer {:nowarn_function, extract_predicates_from_template: 1}
  @dialyzer {:nowarn_function, extract_predicates_from_pattern: 1}

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Execution context containing database and dictionary references"
  @type context :: %{
          required(:db) => TripleStore.db_ref(),
          required(:dict_manager) => TripleStore.manager(),
          optional(:user) => map() | nil
        }

  @typedoc "A quad (subject, predicate, object, optional graph)"
  @type quad :: {term(), term(), term()} | {term(), term(), term(), term()}

  @typedoc "Result count from update operation"
  @type update_result :: {:ok, non_neg_integer()} | {:error, term()}

  # ===========================================================================
  # Configuration
  # ===========================================================================

  # Maximum triples in a single INSERT/DELETE DATA operation
  @max_data_triples 100_000

  # Maximum pattern matches for DELETE/INSERT WHERE
  @max_pattern_matches 1_000_000

  @doc """
  Returns the maximum number of triples allowed in INSERT/DELETE DATA.
  """
  @spec max_data_triples() :: pos_integer()
  def max_data_triples, do: @max_data_triples

  @doc """
  Returns the maximum number of pattern matches for WHERE operations.
  """
  @spec max_pattern_matches() :: pos_integer()
  def max_pattern_matches, do: @max_pattern_matches

  # ===========================================================================
  # Main Entry Point
  # ===========================================================================

  @doc """
  Executes a parsed SPARQL UPDATE AST.

  Takes a parsed UPDATE AST (from `Parser.parse_update/1`) and executes
  all operations it contains sequentially. Returns the total number of
  triples affected.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `ast` - Parsed UPDATE AST

  ## Returns

  - `{:ok, count}` - Total number of triples affected
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, ast} = Parser.parse_update("INSERT DATA { <s> <p> <o> }")
      {:ok, count} = UpdateExecutor.execute(ctx, ast)
      # => {:ok, 1}

  """
  @spec execute(context(), term()) :: update_result()
  def execute(ctx, {:update, props}) when is_list(props) do
    start_time = System.monotonic_time()
    operations = Parser.get_operations({:update, props})
    operation_count = length(operations)

    :telemetry.execute(
      [:triple_store, :sparql, :update, :start],
      %{system_time: System.system_time()},
      %{operation_count: operation_count}
    )

    result =
      Enum.reduce_while(operations, {:ok, 0}, fn op, {:ok, total} ->
        case execute_operation(ctx, op) do
          {:ok, count} -> {:cont, {:ok, total + count}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    duration = System.monotonic_time() - start_time
    {status, triple_count} = telemetry_result(result)

    :telemetry.execute(
      [:triple_store, :sparql, :update, :stop],
      %{duration: duration, triple_count: triple_count || 0},
      %{status: status, operation_count: operation_count}
    )

    result
  end

  def execute(_ctx, _ast) do
    {:error, :invalid_update_ast}
  end

  defp telemetry_result({:ok, count}), do: {:ok, count}
  defp telemetry_result({:error, _}), do: {:error, 0}

  # ===========================================================================
  # Operation Dispatch
  # ===========================================================================

  @doc false
  @spec execute_operation(context(), term()) :: update_result()
  # Operations come as keyword list items from the parser
  def execute_operation(ctx, {:insert_data, quads}) do
    execute_insert_data(ctx, quads)
  end

  def execute_operation(ctx, {:delete_data, quads}) do
    execute_delete_data(ctx, quads)
  end

  def execute_operation(ctx, {:delete_insert, props}) when is_list(props) do
    # Parser returns charlist keys like {"delete", ...}, not atoms
    # We need to extract values by matching the key
    delete_template = get_prop_value(props, "delete", [])
    insert_template = get_prop_value(props, "insert", [])
    pattern = get_prop_value(props, "pattern")
    _using = get_prop_value(props, "using")

    execute_modify(ctx, delete_template, insert_template, pattern)
  end

  def execute_operation(_ctx, {:load, _props}) do
    # LOAD is handled separately through the Loader module
    {:error, :load_not_implemented}
  end

  def execute_operation(ctx, {:clear, props}) when is_list(props) do
    execute_clear(ctx, props)
  end

  def execute_operation(ctx, {:clear, target}) when is_atom(target) do
    # Normalize parser atoms and use correct key name for execute_clear
    normalized = normalize_clear_target(target)
    execute_clear(ctx, graph: normalized)
  end

  def execute_operation(ctx, {:create, props}) when is_list(props) do
    execute_create_graph(ctx, props)
  end

  def execute_operation(ctx, {:drop, props}) when is_list(props) do
    execute_drop_graph(ctx, props)
  end

  def execute_operation(ctx, {:move, props}) when is_list(props) do
    source = get_prop_value(props, "source")
    target = get_prop_value(props, "target")
    silent = get_prop_value(props, "silent", false)
    execute_move(ctx, source, target, silent: silent)
  end

  def execute_operation(ctx, {:copy, props}) when is_list(props) do
    source = get_prop_value(props, "source")
    target = get_prop_value(props, "target")
    silent = get_prop_value(props, "silent", false)
    execute_copy(ctx, source, target, silent: silent)
  end

  def execute_operation(ctx, {:add, props}) when is_list(props) do
    source = get_prop_value(props, "source")
    target = get_prop_value(props, "target")
    silent = get_prop_value(props, "silent", false)
    execute_add(ctx, source, target, silent: silent)
  end

  def execute_operation(_ctx, op) do
    {:error, {:unsupported_operation, op}}
  end

  # Normalize parser target atoms to internal atoms
  defp normalize_clear_target(:all_graphs), do: :all
  defp normalize_clear_target(:default_graph), do: :default
  defp normalize_clear_target(:all_named), do: :named
  defp normalize_clear_target(other), do: other

  # Helper to get value from parser properties (which use charlist keys)
  defp get_prop_value(props, key, default \\ nil) do
    case List.keyfind(props, key, 0) do
      {^key, value} -> value
      _ -> default
    end
  end

  # ===========================================================================
  # INSERT DATA
  # ===========================================================================

  @doc """
  Executes an INSERT DATA operation.

  Inserts ground quads (no variables) directly into the database.
  All quads are inserted atomically using a single WriteBatch.

  For quad stores, the graph component of each quad is respected.
  For triple stores, all data is inserted into the default graph.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `quads` - List of ground quad patterns from the parser

  ## Returns

  - `{:ok, count}` - Number of quads inserted
  - `{:error, :too_many_triples}` - If quad count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      # Insert into default graph
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]
      {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Insert into named graph (quad stores)
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                {:named_node, "http://example.org/named"}}
      ]
      {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

  """
  @spec execute_insert_data(context(), [term()]) :: update_result()
  def execute_insert_data(ctx, quads) do
    InsertData.execute(ctx, quads)
  end

  # ===========================================================================
  # DELETE DATA
  # ===========================================================================

  @doc """
  Executes a DELETE DATA operation.

  Deletes ground quads (no variables) directly from the database.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `quads` - List of ground quad patterns from the parser

  ## Returns

  - `{:ok, count}` - Number of quads deleted
  - `{:error, :too_many_triples}` - If quad count exceeds limit
  - `{:error, reason}` - On other failures

  """
  @spec execute_delete_data(context(), [term()]) :: update_result()
  def execute_delete_data(ctx, quads) do
    DeleteData.execute(ctx, quads)
  end

  # ===========================================================================
  # DELETE WHERE
  # ===========================================================================

  @doc """
  Executes a DELETE WHERE operation.

  Executes a WHERE clause and deletes all matching triples.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `pattern` - WHERE clause pattern (e.g., {:bgp, [...]})

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, reason}` - On failure

  """
  @spec execute_delete_where(context(), term()) :: update_result()
  def execute_delete_where(ctx, pattern) do
    Modify.execute(ctx, [pattern], [], pattern)
  end

  # ===========================================================================
  # INSERT WHERE
  # ===========================================================================

  @doc """
  Executes an INSERT WHERE operation.

  Executes a WHERE clause and inserts triples generated from templates.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `template` - List of template patterns (may contain variables)
  - `pattern` - WHERE clause pattern (e.g., {:bgp, [...]})

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, reason}` - On failure

  """
  @spec execute_insert_where(context(), [term()], term()) :: update_result()
  def execute_insert_where(ctx, template, pattern) do
    Modify.execute(ctx, [], template, pattern)
  end

  # ===========================================================================
  # MODIFY (DELETE/INSERT WHERE)
  # ===========================================================================

  @doc """
  Executes a MODIFY operation (DELETE/INSERT WHERE).

  Executes a WHERE clause, then deletes and inserts triples based on templates.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `delete_template` - List of delete patterns (may contain variables)
  - `insert_template` - List of insert patterns (may contain variables)
  - `pattern` - WHERE clause pattern (e.g., {:bgp, [...]})

  ## Returns

  - `{:ok, count}` - Total number of triples affected
  - `{:error, reason}` - On failure

  ## Examples

      # Delete and insert in single operation
      delete_tmpl = [{:triple, {:variable, "s"},
                               {:named_node, "http://example.org/name"},
                               {:variable, "name"}}]
      insert_tmpl = [{:triple, {:variable, "s"},
                               {:named_node, "http://example.org/name"},
                               {:variable, "upper_name"}}]
      pattern = {:bgp, [...]}
      {:ok, count} = UpdateExecutor.execute_modify(ctx, delete_tmpl, insert_tmpl, pattern)

  """
  @spec execute_modify(context(), [term()], [term()], term()) :: update_result()
  def execute_modify(ctx, delete_template, insert_template, pattern) do
    Modify.execute(ctx, delete_template, insert_template, pattern)
  end

  # ===========================================================================
  # CLEAR
  # ===========================================================================

  @doc """
  Executes a CLEAR operation.

  CLEAR removes all triples from the default graph, a named graph, or all graphs.

  ## Arguments

  - `ctx` - Execution context
  - `props` - Properties from the CLEAR operation

  ## Returns

  - `{:ok, count}` - Number of triples removed
  - `{:error, reason}` - On failure

  """
  @spec execute_clear(context(), keyword()) :: update_result()
  def execute_clear(ctx, props) do
    GraphOperations.execute_clear(ctx, props)
  end

  # ===========================================================================
  # CREATE
  # ===========================================================================

  @doc """
  Executes a CREATE GRAPH operation.

  CREATE GRAPH creates an empty named graph. In our implementation, this
  reserves a graph ID in the dictionary.

  ## Arguments

  - `ctx` - Execution context
  - `props` - Properties from the CREATE operation

  ## Returns

  - `{:ok, 0}` - Graph created (SPARQL CREATE returns no count)
  - `{:error, reason}` - On failure

  """
  @spec execute_create_graph(context(), keyword()) :: update_result()
  def execute_create_graph(ctx, props) do
    GraphOperations.execute_create_graph(ctx, props)
  end

  # ===========================================================================
  # DROP
  # ===========================================================================

  @doc """
  Executes a DROP GRAPH operation.

  DROP GRAPH removes all quads from a named graph.

  ## Arguments

  - `ctx` - Execution context
  - `props` - Properties from the DROP operation

  ## Returns

  - `{:ok, count}` - Number of quads removed
  - `{:error, reason}` - On failure

  """
  @spec execute_drop_graph(context(), keyword()) :: update_result()
  def execute_drop_graph(ctx, props) do
    GraphOperations.execute_drop_graph(ctx, props)
  end

  # ===========================================================================
  # COPY
  # ===========================================================================

  @doc """
  Executes a COPY GRAPH operation.

  Copies all triples from source graph to target graph, replacing target.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI or :default)
  - `target_graph` - Target graph term (RDF.IRI or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples copied
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures

  """
  @spec execute_copy(context(), term(), term(), keyword()) :: update_result()
  def execute_copy(ctx, source_graph, target_graph, opts \\ []) do
    GraphOperations.execute_copy(ctx, source_graph, target_graph, opts)
  end

  # ===========================================================================
  # MOVE
  # ===========================================================================

  @doc """
  Executes a MOVE GRAPH operation.

  Moves all triples from source graph to target graph, then clears source.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI or :default)
  - `target_graph` - Target graph term (RDF.IRI or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples moved
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures

  """
  @spec execute_move(context(), term(), term(), keyword()) :: update_result()
  def execute_move(ctx, source_graph, target_graph, opts \\ []) do
    GraphOperations.execute_move(ctx, source_graph, target_graph, opts)
  end

  # ===========================================================================
  # ADD
  # ===========================================================================

  @doc """
  Executes an ADD GRAPH operation.

  Adds all triples from source graph to target graph (merge, no replace).

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI or :default)
  - `target_graph` - Target graph term (RDF.IRI or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples added
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures

  """
  @spec execute_add(context(), term(), term(), keyword()) :: update_result()
  def execute_add(ctx, source_graph, target_graph, opts \\ []) do
    GraphOperations.execute_add(ctx, source_graph, target_graph, opts)
  end

  # ===========================================================================
  # Cache Invalidation
  # ===========================================================================

  @doc false
  @spec invalidate_cache_for_operations([term()]) :: :ok
  def invalidate_cache_for_operations(operations) do
    # Skip if cache is not running
    if cache_running?() do
      operations
      |> extract_predicates_from_operations()
      |> invalidate_cache_for_predicates()
    else
      :ok
    end
  end

  defp cache_running? do
    case Process.whereis(TripleStore.Query.Cache) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end

  # Extracts predicates from update operations
  # Returns :full_invalidation for complex operations we can't analyze
  @spec extract_predicates_from_operations(list()) :: MapSet.t() | :full_invalidation
  defp extract_predicates_from_operations(operations) do
    Enum.reduce_while(operations, MapSet.new(), fn op, acc ->
      case extract_predicates_from_operation(op) do
        :full_invalidation -> {:halt, :full_invalidation}
        predicates -> {:cont, MapSet.union(acc, predicates)}
      end
    end)
  end

  @spec extract_predicates_from_operation(term()) :: MapSet.t() | :full_invalidation
  defp extract_predicates_from_operation({:insert_data, quads}) do
    extract_predicates_from_quads(quads)
  end

  defp extract_predicates_from_operation({:delete_data, quads}) do
    extract_predicates_from_quads(quads)
  end

  defp extract_predicates_from_operation({:delete_insert, props}) when is_list(props) do
    delete_template = get_prop_value(props, "delete", [])
    insert_template = get_prop_value(props, "insert", [])
    pattern = get_prop_value(props, "pattern")

    # Extract predicates from templates and pattern
    predicates =
      MapSet.new()
      |> MapSet.union(extract_predicates_from_template(delete_template))
      |> MapSet.union(extract_predicates_from_template(insert_template))
      |> MapSet.union(extract_predicates_from_pattern(pattern))

    predicates
  end

  defp extract_predicates_from_operation({:clear, _props}) do
    # CLEAR invalidates everything
    :full_invalidation
  end

  defp extract_predicates_from_operation({:load, _props}) do
    # LOAD invalidates everything
    :full_invalidation
  end

  defp extract_predicates_from_operation({:drop, _props}) do
    # DROP invalidates everything
    :full_invalidation
  end

  defp extract_predicates_from_operation({:create, _props}) do
    # CREATE doesn't affect data
    MapSet.new()
  end

  defp extract_predicates_from_operation(_op) do
    # Unknown operation - full invalidation to be safe
    :full_invalidation
  end

  # Extract predicates from quads (INSERT DATA, DELETE DATA)
  @spec extract_predicates_from_quads(list()) :: MapSet.t()
  defp extract_predicates_from_quads(quads) do
    Enum.reduce(quads, MapSet.new(), fn quad, acc ->
      case extract_predicate_from_quad(quad) do
        nil -> acc
        pred -> MapSet.put(acc, pred)
      end
    end)
  end

  defp extract_predicate_from_quad({:quad, _s, p, _o, _graph}), do: ast_term_to_rdf(p)
  defp extract_predicate_from_quad({:triple, _s, p, _o}), do: ast_term_to_rdf(p)
  defp extract_predicate_from_quad({_s, p, _o}), do: ast_term_to_rdf(p)
  defp extract_predicate_from_quad(_), do: nil

  # Extract predicates from template patterns (DELETE/INSERT WHERE)
  @spec extract_predicates_from_template(list() | term()) :: MapSet.t()
  defp extract_predicates_from_template(template) when is_list(template) do
    Enum.reduce(template, MapSet.new(), fn pattern, acc ->
      MapSet.union(acc, extract_predicates_from_pattern(pattern))
    end)
  end

  defp extract_predicates_from_template(_), do: MapSet.new()

  # Extract predicates from WHERE patterns
  @spec extract_predicates_from_pattern(term()) :: MapSet.t()
  defp extract_predicates_from_pattern({:bgp, triples}) when is_list(triples) do
    Enum.reduce(triples, MapSet.new(), &extract_bgp_predicate/2)
  end

  defp extract_predicates_from_pattern({:triple, _s, p, _o}) do
    case ast_term_to_rdf(p) do
      nil -> MapSet.new()
      pred -> MapSet.new([pred])
    end
  end

  defp extract_predicates_from_pattern(nil), do: MapSet.new()

  defp extract_predicates_from_pattern(_pattern) do
    # Complex patterns (OPTIONAL, UNION, FILTER, etc.) - can't analyze
    # Return empty and let caller decide
    MapSet.new()
  end

  defp invalidate_cache_for_predicates(:full_invalidation) do
    emit_full_invalidation()
    QueryCache.invalidate()
  end

  defp invalidate_cache_for_predicates(predicates) when map_size(predicates) == 0, do: :ok

  defp invalidate_cache_for_predicates(predicates) do
    emit_predicate_invalidation(predicates)
    QueryCache.invalidate_predicates(predicates)
  end

  defp extract_bgp_predicate({:triple, _s, p, _o}, acc) do
    case ast_term_to_rdf(p) do
      nil -> acc
      pred -> MapSet.put(acc, pred)
    end
  end

  defp extract_bgp_predicate(_triple, acc), do: acc

  # Convert AST term to RDF term (only for ground terms)
  defp ast_term_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_term_to_rdf({:variable, _name}), do: nil
  defp ast_term_to_rdf(_), do: nil

  # Telemetry for cache invalidation
  defp emit_full_invalidation do
    :telemetry.execute(
      [:triple_store, :cache, :query, :invalidate],
      %{count: 1},
      %{type: :full}
    )
  end

  defp emit_predicate_invalidation(predicates) do
    :telemetry.execute(
      [:triple_store, :cache, :query, :invalidate],
      %{count: 1, predicate_count: MapSet.size(predicates)},
      %{type: :predicate}
    )
  end
end
