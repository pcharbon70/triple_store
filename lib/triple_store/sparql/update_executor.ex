defmodule TripleStore.SPARQL.UpdateExecutor do
  @moduledoc """
  SPARQL UPDATE operation executor.

  This module executes SPARQL UPDATE operations against the triple store,
  providing support for all SPARQL 1.1 Update operations:

  - **INSERT DATA**: Direct insertion of ground triples
  - **DELETE DATA**: Direct deletion of ground triples
  - **DELETE WHERE**: Pattern-based deletion
  - **INSERT WHERE**: Pattern-based insertion (using templates)
  - **DELETE/INSERT WHERE**: Combined delete and insert in single operation

  ## Execution Model

  All update operations are executed atomically using RocksDB's WriteBatch.
  Operations that involve WHERE clauses first query the database to find
  matching bindings, then apply those bindings to templates to generate
  the actual triples to insert or delete.

  ## Usage

      # Parse and execute an update
      {:ok, ast} = Parser.parse_update("INSERT DATA { <s> <p> <o> }")
      {:ok, count} = UpdateExecutor.execute(ctx, ast)

      # Execute specific operations
      {:ok, count} = UpdateExecutor.execute_insert_data(ctx, quads)
      {:ok, count} = UpdateExecutor.execute_delete_where(ctx, pattern)

  ## Security

  - All operations validate input size to prevent DoS
  - Pattern-based operations have result limits
  - Templates are validated before execution
  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Index
  alias TripleStore.QuadOperations
  alias TripleStore.Query.Cache, as: QueryCache
  alias TripleStore.SPARQL.Executor
  alias TripleStore.SPARQL.Parser

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
          db: reference(),
          dict_manager: GenServer.server()
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

  # Maximum template size (triples per template)
  @max_template_size 1_000

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
      %{duration: duration, triple_count: triple_count},
      %{operation_count: operation_count, status: status}
    )

    # Invalidate query cache after successful update
    if status == :ok and triple_count > 0 do
      invalidate_cache_for_operations(operations)
    end

    result
  end

  def execute(_ctx, _ast), do: {:error, :invalid_update_ast}

  # Extract status and triple count for telemetry
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

  # Helper to get value from parser properties (which use charlist keys)
  defp get_prop_value(props, key, default \\ nil) do
    case List.keyfind(props, key, 0) do
      {^key, value} -> value
      _ -> default
    end
  end

  def execute_operation(_ctx, {:load, _props}) do
    # LOAD is handled separately through the Loader module
    {:error, :load_not_implemented}
  end

  def execute_operation(ctx, {:clear, props}) when is_list(props) do
    execute_clear(ctx, props)
  end

  def execute_operation(ctx, {:create, props}) when is_list(props) do
    execute_create_graph(ctx, props)
  end

  def execute_operation(ctx, {:drop, props}) when is_list(props) do
    execute_drop_graph(ctx, props)
  end

  # Handle clear with atom target directly
  def execute_operation(ctx, {:clear, target}) when is_atom(target) do
    execute_clear(ctx, target: target)
  end

  def execute_operation(_ctx, op) do
    {:error, {:unsupported_operation, op}}
  end

  # ===========================================================================
  # INSERT DATA
  # ===========================================================================

  @doc """
  Executes an INSERT DATA operation.

  Inserts ground triples (no variables) directly into the database.
  All triples are inserted atomically using a single WriteBatch.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `quads` - List of ground quad patterns from the parser

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, :too_many_triples}` - If quad count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]
      {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

  """
  @spec execute_insert_data(context(), [term()]) :: update_result()
  def execute_insert_data(_ctx, []), do: {:ok, 0}

  def execute_insert_data(_ctx, quads) when length(quads) > @max_data_triples do
    {:error, :too_many_triples}
  end

  def execute_insert_data(ctx, quads) when is_list(quads) do
    # Check if we're using a quad store
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        # Use quad operations
        insert_quads(ctx, quads)
      {:ok, false} ->
        # Use triple operations (existing behavior)
        insert_triples(ctx, quads)
    end
  end

  # ===========================================================================
  # Quad Insertion (for quad stores)
  # ===========================================================================

  @doc """
  Inserts quads into a quad store.

  Converts AST quads to RDF quads (preserving graph component) and
  inserts them using QuadOperations.insert_quad/4.

  ## Arguments
  - `ctx` - Execution context
  - `quads` - List of quad AST from parser

  ## Returns
  - `{:ok, count}` - Number of quads inserted
  - `{:error, reason}` - On failure
  """
  defp insert_quads(ctx, quads) do
    with {:ok, rdf_quads} <- quads_to_rdf_quads(quads),
         {:ok, count} <- do_insert_quads(ctx, rdf_quads) do
      {:ok, count}
    end
  end

  # Perform the actual quad insertion
  defp do_insert_quads(ctx, rdf_quads) do
    # Insert each quad and count total
    total =
      Enum.reduce(rdf_quads, 0, fn rdf_quad, count ->
        {subject, predicate, object, graph} = rdf_quad

        with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, subject),
             {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, predicate),
             {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, object),
             {:ok, g_id} <- get_graph_id_for_insert(ctx, graph),
             :ok <- QuadOperations.insert_quad(ctx.db, {s_id, p_id, o_id, g_id}) do
          count + 1
        else
          _error -> count
        end
      end)

    {:ok, total}
  end

  # Get graph ID for insertion (creates ID if needed)
  defp get_graph_id_for_insert(ctx, :default), do: {:ok, 0}
  defp get_graph_id_for_insert(ctx, %RDF.IRI{} = graph_iri) do
    Adapter.term_to_id(ctx.dict_manager, graph_iri)
  end
  defp get_graph_id_for_insert(_ctx, _), do: {:ok, 0}

  # ===========================================================================
  # Triple Insertion (for triple stores)
  # ===========================================================================

  @doc """
  Inserts triples into a triple store.

  Legacy function for triple stores. Converts AST quads to RDF triples
  and inserts using Index.insert_triples.
  """
  defp insert_triples(ctx, quads) do
    with {:ok, rdf_triples} <- quads_to_rdf_triples(quads),
         {:ok, internal_triples} <- Adapter.from_rdf_triples(ctx.dict_manager, rdf_triples) do
      case Index.insert_triples(ctx.db, internal_triples) do
        :ok -> {:ok, length(internal_triples)}
        {:error, _} = error -> error
      end
    end
  end

  # ===========================================================================
  # DELETE DATA
  # ===========================================================================

  @doc """
  Executes a DELETE DATA operation.

  Deletes ground triples (no variables) directly from the database.
  All deletions are performed atomically using a single DeleteBatch.
  Deleting non-existent triples is a no-op (idempotent).

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `quads` - List of ground quad patterns from the parser

  ## Returns

  - `{:ok, count}` - Number of triples in the delete request
  - `{:error, :too_many_triples}` - If quad count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]
      {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, quads)

  """
  @spec execute_delete_data(context(), [term()]) :: update_result()
  def execute_delete_data(_ctx, []), do: {:ok, 0}

  def execute_delete_data(_ctx, quads) when length(quads) > @max_data_triples do
    {:error, :too_many_triples}
  end

  def execute_delete_data(ctx, quads) when is_list(quads) do
    # Check if we're using a quad store
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        # Use quad operations
        delete_quads(ctx, quads)
      {:ok, false} ->
        # Use triple operations (existing behavior)
        delete_triples_from_store(ctx, quads)
    end
  end

  # ===========================================================================
  # Quad Deletion (for quad stores)
  # ===========================================================================

  @doc """
  Deletes quads from a quad store.

  Converts AST quads to RDF quads (preserving graph component) and
  deletes them using QuadOperations.delete_quads/2.

  ## Arguments
  - `ctx` - Execution context
  - `quads` - List of quad AST from parser

  ## Returns
  - `{:ok, count}` - Number of quads deleted
  - `{:error, reason}` - On failure
  """
  defp delete_quads(ctx, quads) do
    with {:ok, rdf_quads} <- quads_to_rdf_quads(quads),
         {:ok, count} <- do_delete_quads(ctx, rdf_quads) do
      {:ok, count}
    end
  end

  # Perform the actual quad deletion
  defp do_delete_quads(ctx, rdf_quads) do
    # Look up IDs for each quad and check if they exist
    quads_to_delete =
      Enum.reduce(rdf_quads, [], fn rdf_quad, acc ->
        {subject, predicate, object, graph} = rdf_quad

        with {:ok, s_id} <- lookup_term_id_no_create(ctx.db, subject),
             {:ok, p_id} <- lookup_term_id_no_create(ctx.db, predicate),
             {:ok, o_id} <- lookup_term_id_no_create(ctx.db, object),
             {:ok, g_id} <- get_graph_id_for_delete(ctx, graph) do
          quad = {s_id, p_id, o_id, g_id}
          # Only add to list if quad actually exists in the database
          if QuadOperations.quad_exists?(ctx.db, quad) do
            [quad | acc]
          else
            acc
          end
        else
          _ -> acc
        end
      end)
      |> Enum.reverse()

    if quads_to_delete == [] do
      {:ok, 0}
    else
      case QuadOperations.delete_quads(ctx.db, quads_to_delete, []) do
        :ok -> {:ok, length(quads_to_delete)}
        {:error, _} = error -> error
      end
    end
  end

  # Get graph ID for deletion (lookup only, don't create)
  defp get_graph_id_for_delete(_ctx, :default), do: {:ok, 0}
  defp get_graph_id_for_delete(ctx, %RDF.IRI{} = graph_iri) do
    # For DELETE, only lookup - don't create if not found
    case lookup_term_id_no_create(ctx.db, graph_iri) do
      {:ok, _id} = result -> result
      _ -> {:error, :not_found}
    end
  end
  defp get_graph_id_for_delete(_ctx, _), do: {:error, :not_found}

  # Lookup term ID without creating (for DELETE operations)
  defp lookup_term_id_no_create(db, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      # Inline-encoded literals can be checked directly
      case encode_inline_literal(literal) do
        {:ok, id} -> {:ok, id}
        {:error, _} -> {:error, :not_found}
      end
    else
      case TripleStore.Dictionary.StringToId.lookup_id(db, literal) do
        {:ok, _id} = result -> result
        _ -> {:error, :not_found}
      end
    end
  end

  defp lookup_term_id_no_create(db, term) do
    case TripleStore.Dictionary.StringToId.lookup_id(db, term) do
      {:ok, _id} = result -> result
      _ -> {:error, :not_found}
    end
  end

  # ===========================================================================
  # Triple Deletion (for triple stores)
  # ===========================================================================

  @doc """
  Deletes triples from a triple store.

  Legacy function for triple stores. Converts AST quads to RDF triples
  and deletes using Index.delete_triples.
  """
  defp delete_triples_from_store(ctx, quads) do
    with {:ok, rdf_triples} <- quads_to_rdf_triples(quads),
         {:ok, internal_triples} <- lookup_triple_ids(ctx, rdf_triples) do
      # Only delete triples that exist (have valid IDs)
      valid_triples = Enum.filter(internal_triples, &(&1 != nil))

      case Index.delete_triples(ctx.db, valid_triples) do
        :ok -> {:ok, length(valid_triples)}
        {:error, _} = error -> error
      end
    end
  end

  # ===========================================================================
  # DELETE WHERE
  # ===========================================================================

  @doc """
  Executes a DELETE WHERE operation.

  Finds all triples matching the WHERE pattern and deletes them.
  This is equivalent to DELETE { pattern } WHERE { pattern }.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `pattern` - The WHERE pattern to match and delete

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, :too_many_matches}` - If match count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      # Delete all triples with predicate :name
      pattern = {:bgp, [{:triple, {:variable, "s"},
                                  {:named_node, "http://example.org/name"},
                                  {:variable, "o"}}]}
      {:ok, count} = UpdateExecutor.execute_delete_where(ctx, pattern)

  """
  @spec execute_delete_where(context(), term()) :: update_result()
  def execute_delete_where(ctx, pattern) do
    # DELETE WHERE uses the pattern as both template and query
    execute_modify(ctx, [pattern], [], pattern)
  end

  # ===========================================================================
  # INSERT WHERE
  # ===========================================================================

  @doc """
  Executes an INSERT operation with WHERE pattern.

  Queries the database using the WHERE pattern, then for each matching
  binding, instantiates the insert template and inserts the resulting triples.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `template` - Template patterns to instantiate
  - `pattern` - The WHERE pattern to match

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, :too_many_matches}` - If match count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      # Copy all :name values to :label
      template = [{:triple, {:variable, "s"},
                            {:named_node, "http://example.org/label"},
                            {:variable, "name"}}]
      pattern = {:bgp, [{:triple, {:variable, "s"},
                                  {:named_node, "http://example.org/name"},
                                  {:variable, "name"}}]}
      {:ok, count} = UpdateExecutor.execute_insert_where(ctx, template, pattern)

  """
  @spec execute_insert_where(context(), [term()], term()) :: update_result()
  def execute_insert_where(ctx, template, pattern) do
    execute_modify(ctx, [], template, pattern)
  end

  # ===========================================================================
  # DELETE/INSERT WHERE (MODIFY)
  # ===========================================================================

  @doc """
  Executes a combined DELETE/INSERT WHERE operation.

  This is the most general form of SPARQL update that:
  1. Evaluates the WHERE pattern to get bindings
  2. For each binding, instantiates both delete and insert templates
  3. Deletes all resulting delete triples
  4. Inserts all resulting insert triples

  The delete and insert happen atomically via a single WriteBatch.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `delete_template` - Template patterns for deletion
  - `insert_template` - Template patterns for insertion
  - `pattern` - The WHERE pattern to match

  ## Returns

  - `{:ok, count}` - Number of triples affected (deleted + inserted)
  - `{:error, :too_many_matches}` - If match count exceeds limit
  - `{:error, reason}` - On other failures

  ## Examples

      # Change all :name values to uppercase (conceptually)
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
  def execute_modify(_ctx, [], [], _pattern), do: {:ok, 0}

  def execute_modify(ctx, delete_template, insert_template, pattern) do
    # Validate template sizes
    if length(delete_template) > @max_template_size or
         length(insert_template) > @max_template_size do
      {:error, :template_too_large}
    else
      # Check if we're using a quad store
      case ErlangAdapter.is_quad_store?(ctx.db) do
        {:ok, true} ->
          do_execute_modify_quad(ctx, delete_template, insert_template, pattern)
        {:ok, false} ->
          do_execute_modify_triples(ctx, delete_template, insert_template, pattern)
      end
    end
  end

  # Legacy MODIFY for triple stores
  defp do_execute_modify_triples(ctx, delete_template, insert_template, pattern) do
    # Execute WHERE pattern to get bindings
    case execute_where_pattern(ctx, pattern) do
      {:ok, bindings} when length(bindings) > @max_pattern_matches ->
        {:error, :too_many_matches}

      {:ok, bindings} ->
        # Instantiate templates with bindings
        delete_triples = instantiate_template(delete_template, bindings)
        insert_triples = instantiate_template(insert_template, bindings)

        # Convert to internal representation
        with {:ok, delete_internal} <- triples_to_internal(ctx, delete_triples, :lookup),
             {:ok, insert_internal} <- triples_to_internal(ctx, insert_triples, :create) do
          # Perform atomic delete + insert
          execute_atomic_modify(ctx.db, delete_internal, insert_internal)
        end

      {:error, _} = error ->
        error
    end
  end

  # MODIFY for quad stores - always use quad operations
  defp do_execute_modify_quad(ctx, delete_template, insert_template, pattern) do
    # Execute WHERE pattern to get bindings
    case execute_where_pattern(ctx, pattern) do
      {:ok, bindings} when length(bindings) > @max_pattern_matches ->
        {:error, :too_many_matches}

      {:ok, bindings} ->
        # Instantiate templates with bindings (handles both quads and triples)
        delete_patterns = instantiate_template(delete_template, bindings)
        insert_patterns = instantiate_template(insert_template, bindings)

        # Always use quad operations for quad stores
        # (3-tuples from triple templates are converted to default graph quads)
        with {:ok, delete_internal} <- quads_to_internal(ctx, delete_patterns, :lookup),
             {:ok, insert_internal} <- quads_to_internal(ctx, insert_patterns, :create) do
          execute_atomic_modify_quads(ctx, delete_internal, insert_internal)
        end

      {:error, _} = error ->
        error
    end
  end

  # Check if a list contains quad patterns (4-element tuples or {:quad, ...})
  defp has_quads?(list) when is_list(list) do
    Enum.any?(list, fn
      {:quad, _, _, _, _} -> true
      {_, _, _, _} -> true
      _ -> false
    end)
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
    target = Keyword.get(props, :target, :all)
    silent = Keyword.get(props, :silent, false)

    # Route to appropriate implementation based on schema
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        execute_clear_quad(ctx, target, silent)

      {:ok, false} ->
        execute_clear_triple(ctx, target, silent)
    end
  end

  # Clear for triple stores
  defp execute_clear_triple(ctx, :all, _silent) do
    # Delete all triples from the store using existing helper
    clear_all_triples(ctx)
  end

  defp execute_clear_triple(ctx, :default, _silent) do
    # For triple stores, default is the same as all
    clear_all_triples(ctx)
  end

  defp execute_clear_triple(_ctx, target, silent) do
    # :named and {:graph, iri} not supported for triple stores
    if silent, do: {:ok, 0}, else: {:error, {:invalid_clear_target, target}}
  end

  # Clear for quad stores
  defp execute_clear_quad(ctx, target, silent) do
    case target do
      :all ->
        clear_all_graphs(ctx)

      :default ->
        clear_default_graph(ctx)

      :named ->
        # Clear all named graphs (not default)
        clear_all_named_graphs(ctx, silent)

      {:graph, iri} ->
        clear_named_graph(ctx, iri, silent)

      _ ->
        if silent, do: {:ok, 0}, else: {:error, {:invalid_clear_target, target}}
    end
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
    graph_iri = get_prop(props, "graph")
    silent = get_prop(props, "silent", false)

    cond do
      is_nil(graph_iri) ->
        {:error, :missing_graph_iri}

      graph_iri == :default or graph_iri == :default_graph ->
        # Default graph always exists
        if silent, do: {:ok, 0}, else: {:error, :default_graph_exists}

      true ->
        # Convert AST graph term to RDF term
        rdf_graph = ast_term_to_rdf_graph(graph_iri)

        case QuadOperations.create_graph(ctx.db, ctx.dict_manager, rdf_graph) do
          {:ok, :created} ->
            {:ok, 0}

          {:ok, :already_exists} ->
            if silent, do: {:ok, 0}, else: {:error, :graph_already_exists}

          {:error, reason} ->
            if silent, do: {:ok, 0}, else: {:error, reason}
        end
    end
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
    graph_iri = get_prop(props, "graph")
    silent = get_prop(props, "silent", false)

    cond do
      is_nil(graph_iri) ->
        {:error, :missing_graph_iri}

      graph_iri == :default or graph_iri == :default_graph ->
        # Can't drop default graph
        if silent, do: {:ok, 0}, else: {:error, :cannot_drop_default}

      true ->
        # Convert AST graph term to RDF term
        rdf_graph = ast_term_to_rdf_graph(graph_iri)

        case QuadOperations.delete_graph(ctx.db, ctx.dict_manager, rdf_graph) do
          {:ok, count} ->
            {:ok, count}

          {:error, :not_found} ->
            if silent, do: {:ok, 0}, else: {:error, :graph_not_found}

          {:error, reason} ->
            if silent, do: {:ok, 0}, else: {:error, reason}
        end
    end
  end

  # ===========================================================================
  # CLEAR Helpers
  # ===========================================================================

  # Batch size for chunked clear operations to prevent OOM
  @clear_batch_size 10_000

  # Clear all graphs (default + named)
  defp clear_all_graphs(ctx) do
    # Use quad operations to clear all graphs
    case clear_all_triples(ctx) do
      {:ok, count} -> {:ok, count}
      {:error, _} = error -> error
    end
  end

  # Clear default graph only
  defp clear_default_graph(ctx) do
    case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, :default) do
      {:ok, count} -> {:ok, count}
      {:error, _} = error -> error
    end
  end

  # Clear all named graphs (not default)
  defp clear_all_named_graphs(ctx, silent) do
    case QuadOperations.list_graphs(ctx.db, include_default: false) do
      {:ok, graphs} ->
        # Clear each named graph and sum the counts
        Enum.reduce_while(graphs, {:ok, 0}, fn graph_iri, {:ok, total} ->
          case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, graph_iri) do
            {:ok, count} -> {:cont, {:ok, total + count}}
            {:error, _} -> {:halt, if(silent, do: {:ok, total}, else: {:error, :clear_failed})}
          end
        end)

      {:error, _} ->
        if silent, do: {:ok, 0}, else: {:error, :list_graphs_failed}
    end
  end

  # Clear a specific named graph
  defp clear_named_graph(ctx, graph_iri, silent) do
    rdf_graph = ast_term_to_rdf_graph(graph_iri)

    case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, rdf_graph) do
      {:ok, count} ->
        {:ok, count}

      {:error, :not_found} ->
        if silent, do: {:ok, 0}, else: {:error, :graph_not_found}

      {:error, reason} ->
        if silent, do: {:ok, 0}, else: {:error, reason}
    end
  end

  # Legacy: clear all triples from the default graph
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp clear_all_triples(ctx) do
    # Stream triples and delete in batches to prevent OOM on large databases
    {:ok, stream} = Index.lookup(ctx.db, {:var, :var, :var})

    stream
    |> Stream.chunk_every(@clear_batch_size)
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, count} ->
      case Index.delete_triples(ctx.db, chunk) do
        :ok -> {:cont, {:ok, count + length(chunk)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # ===========================================================================
  # COPY/MOVE/ADD Operations
  # ===========================================================================

  @doc """
  Executes a COPY GRAPH operation.

  Copies all triples from source graph to target graph, replacing target contents.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `target_graph` - Target graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples copied
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures
  """
  @spec execute_copy(context(), term(), term(), keyword()) :: update_result()
  def execute_copy(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    cond do
      source_graph == target_graph ->
        if silent, do: {:ok, 0}, else: {:error, :source_equals_target}

      true ->
        case ErlangAdapter.is_quad_store?(ctx.db) do
          {:ok, true} ->
            do_copy_quad(ctx, source_graph, target_graph, silent)

          {:ok, false} ->
            {:error, :copy_requires_quad_store}
        end
    end
  end

  @doc """
  Executes a MOVE GRAPH operation.

  Moves all triples from source graph to target graph, then clears source.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `target_graph` - Target graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples moved
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures
  """
  @spec execute_move(context(), term(), term(), keyword()) :: update_result()
  def execute_move(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    cond do
      source_graph == target_graph ->
        if silent, do: {:ok, 0}, else: {:error, :source_equals_target}

      true ->
        case ErlangAdapter.is_quad_store?(ctx.db) do
          {:ok, true} ->
            do_move_quad(ctx, source_graph, target_graph, silent)

          {:ok, false} ->
            {:error, :move_requires_quad_store}
        end
    end
  end

  @doc """
  Executes an ADD GRAPH operation.

  Adds all triples from source graph to target graph (merge, no replace).

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `source_graph` - Source graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `target_graph` - Target graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `opts` - Options, including `:silent` to suppress errors

  ## Returns

  - `{:ok, count}` - Number of triples added
  - `{:error, :source_equals_target}` - Source and target are the same
  - `{:error, reason}` - On other failures
  """
  @spec execute_add(context(), term(), term(), keyword()) :: update_result()
  def execute_add(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    cond do
      source_graph == target_graph ->
        if silent, do: {:ok, 0}, else: {:error, :source_equals_target}

      true ->
        case ErlangAdapter.is_quad_store?(ctx.db) do
          {:ok, true} ->
            do_add_quad(ctx, source_graph, target_graph, silent)

          {:ok, false} ->
            {:error, :add_requires_quad_store}
        end
    end
  end

  # ===========================================================================
  # COPY/MOVE/ADD Internal Implementation
  # ===========================================================================

  defp do_copy_quad(ctx, source_graph, target_graph, silent) do
    source_rdf = normalize_graph_term(source_graph)
    target_rdf = normalize_graph_term(target_graph)

    # Check if source graph exists (has quads)
    source_exists? =
      case source_rdf do
        :default -> QuadOperations.default_graph_exists?(ctx.db)
        graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
      end

    if !source_exists? do
      if silent, do: {:ok, 0}, else: {:error, :source_graph_not_found}
    else
      # Use copy_graph with :replace option (COPY replaces target)
      case QuadOperations.copy_graph(
             ctx.db,
             ctx.dict_manager,
             source_rdf,
             target_rdf,
             on_conflict: :replace
           ) do
        {:ok, count} ->
          {:ok, count}

        {:error, reason} ->
          if silent, do: {:ok, 0}, else: {:error, reason}
      end
    end
  end

  defp do_move_quad(ctx, source_graph, target_graph, silent) do
    source_rdf = normalize_graph_term(source_graph)
    target_rdf = normalize_graph_term(target_graph)

    # Check if source graph exists (has quads)
    source_exists? =
      case source_rdf do
        :default -> QuadOperations.default_graph_exists?(ctx.db)
        graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
      end

    if !source_exists? do
      if silent, do: {:ok, 0}, else: {:error, :source_graph_not_found}
    else
      # First copy with replace
      with {:ok, _count} <-
             QuadOperations.copy_graph(
               ctx.db,
               ctx.dict_manager,
               source_rdf,
               target_rdf,
               on_conflict: :replace
             ),
           {:ok, _} <-
             QuadOperations.clear_graph(ctx.db, ctx.dict_manager, source_rdf) do
        # Get final count from target graph
        case QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, target_rdf) do
          {:ok, count} -> {:ok, count}
          {:error, _} -> {:ok, :unknown}
        end
      else
        {:error, reason} ->
          if silent, do: {:ok, 0}, else: {:error, reason}
      end
    end
  end

  defp do_add_quad(ctx, source_graph, target_graph, silent) do
    source_rdf = normalize_graph_term(source_graph)
    target_rdf = normalize_graph_term(target_graph)

    # Check if source graph exists (has quads)
    source_exists? =
      case source_rdf do
        :default -> QuadOperations.default_graph_exists?(ctx.db)
        graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
      end

    if !source_exists? do
      if silent, do: {:ok, 0}, else: {:error, :source_graph_not_found}
    else
      # Use copy_graph with :merge option (ADD merges with target)
      case QuadOperations.copy_graph(
             ctx.db,
             ctx.dict_manager,
             source_rdf,
             target_rdf,
             on_conflict: :merge
           ) do
        {:ok, count} ->
          {:ok, count}

        {:error, reason} ->
          if silent, do: {:ok, 0}, else: {:error, reason}
      end
    end
  end

  # Normalize graph terms to a consistent format
  defp normalize_graph_term(:default), do: :default
  defp normalize_graph_term(:default_graph), do: :default
  defp normalize_graph_term({:named_node, iri}), do: RDF.iri(iri)
  defp normalize_graph_term({:named_graph, iri}), do: RDF.iri(iri)
  defp normalize_graph_term(%RDF.IRI{} = iri), do: iri
  defp normalize_graph_term(iri) when is_binary(iri), do: RDF.iri(iri)
  defp normalize_graph_term(_other), do: :default

  # ===========================================================================
  # Private Helpers: Quad/Triple Conversion
  # ===========================================================================

  # Converts parser quads to RDF.ex triples
  defp quads_to_rdf_triples(quads) do
    triples =
      Enum.map(quads, fn
        {:quad, s, p, o, _graph} -> {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
        {:triple, s, p, o} -> {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
        {s, p, o} -> {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
      end)

    {:ok, triples}
  rescue
    e -> {:error, {:conversion_error, e}}
  end

  # Converts parser quads to RDF.ex quads (preserving graph component)
  defp quads_to_rdf_quads(quads) do
    rdf_quads =
      Enum.map(quads, fn
        {:quad, s, p, o, g} ->
          {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), ast_graph_to_rdf(g)}

        {:triple, s, p, o} ->
          # Legacy triple format - default to default graph
          {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}

        {s, p, o} ->
          # Bare triple format - default to default graph
          {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}
      end)

    {:ok, rdf_quads}
  rescue
    e -> {:error, {:conversion_error, e}}
  end

  # Converts AST graph term to RDF.IRI or :default atom
  defp ast_graph_to_rdf(:default), do: :default
  defp ast_graph_to_rdf(:default_graph), do: :default
  defp ast_graph_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:named_graph, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:iri, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
  defp ast_graph_to_rdf(_other), do: :default

  # Converts parser AST term to RDF.ex term
  defp ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  defp ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :language_tagged, value, lang}),
    do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :typed, value, datatype}) do
    RDF.literal(value, datatype: datatype)
  end

  defp ast_to_rdf({:variable, _name}) do
    raise ArgumentError, "Variables not allowed in INSERT/DELETE DATA"
  end

  defp ast_to_rdf(term), do: term

  # Converts AST graph term to RDF.IRI or :default atom
  defp ast_term_to_rdf_graph(:default), do: :default
  defp ast_term_to_rdf_graph(:default_graph), do: :default
  defp ast_term_to_rdf_graph({:named_node, iri}), do: RDF.iri(iri)
  defp ast_term_to_rdf_graph({:named_graph, iri}), do: RDF.iri(iri)
  defp ast_term_to_rdf_graph({:iri, iri}), do: RDF.iri(iri)
  defp ast_term_to_rdf_graph(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
  defp ast_term_to_rdf_graph(_other), do: {:error, :invalid_graph_term}

  # Looks up existing IDs for triples (for DELETE - doesn't create new IDs)
  defp lookup_triple_ids(ctx, rdf_triples) do
    results =
      Enum.map(rdf_triples, fn {s, p, o} ->
        with {:ok, s_id} <- lookup_term_id(ctx.db, s),
             {:ok, p_id} <- lookup_term_id(ctx.db, p),
             {:ok, o_id} <- lookup_term_id(ctx.db, o) do
          {s_id, p_id, o_id}
        else
          :not_found -> nil
          {:error, _} -> nil
        end
      end)

    {:ok, results}
  end

  # Lookup term ID - uses inline encoding for numeric types, dictionary for others
  defp lookup_term_id(db, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      StringToId.lookup_id(db, literal)
    end
  end

  defp lookup_term_id(db, term) do
    StringToId.lookup_id(db, term)
  end

  # Encode inline-encodable literals directly
  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}})
       when is_integer(value) do
    Dictionary.encode_integer(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    Dictionary.encode_decimal(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    Dictionary.encode_datetime(value)
  end

  defp encode_inline_literal(_literal) do
    {:error, :not_inline_encodable}
  end

  # Converts triples to internal representation
  defp triples_to_internal(_ctx, [], _mode), do: {:ok, []}

  defp triples_to_internal(ctx, triples, :create) do
    Adapter.from_rdf_triples(ctx.dict_manager, triples)
  end

  defp triples_to_internal(ctx, triples, :lookup) do
    lookup_triple_ids(ctx, triples)
  end

  # Converts quads to internal representation
  defp quads_to_internal(_ctx, [], _mode), do: {:ok, []}

  defp quads_to_internal(ctx, quads, :create) do
    # For INSERT, create IDs as needed
    results =
      Enum.map(quads, fn
        # Handle 3-tuples (from triple templates) - treat as default graph quads
        # Must come before 4-tuple patterns to match correctly
        {s, p, o} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            _ -> {:error, :conversion_failed}
          end

        {s, p, o, :default} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            _ -> {:error, :conversion_failed}
          end

        {s, p, o, %RDF.IRI{} = g} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o),
               {:ok, g_id} <- Adapter.term_to_id(ctx.dict_manager, g) do
            {:ok, {s_id, p_id, o_id, g_id}}
          else
            _ -> {:error, :conversion_failed}
          end

        _ ->
          {:error, :invalid_quad}
      end)

    # Split into successes and failures
    {successes, failures} = Enum.split_with(results, &match?({:ok, _}, &1))

    if failures == [] do
      {:ok, Enum.map(successes, fn {:ok, quad} -> quad end)}
    else
      {:error, {:partial_conversion, length(successes), length(failures)}}
    end
  end

  defp quads_to_internal(ctx, quads, :lookup) do
    # For DELETE, only look up existing IDs
    results =
      Enum.map(quads, fn
        # Handle 3-tuples (from triple templates) - treat as default graph quads
        # Must come before 4-tuple patterns to match correctly
        {s, p, o} ->
          with {:ok, s_id} <- lookup_term_id_no_create(ctx.db, s),
               {:ok, p_id} <- lookup_term_id_no_create(ctx.db, p),
               {:ok, o_id} <- lookup_term_id_no_create(ctx.db, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            _ -> {:error, :not_found}
          end

        {s, p, o, :default} ->
          with {:ok, s_id} <- lookup_term_id_no_create(ctx.db, s),
               {:ok, p_id} <- lookup_term_id_no_create(ctx.db, p),
               {:ok, o_id} <- lookup_term_id_no_create(ctx.db, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            _ -> {:error, :not_found}
          end

        {s, p, o, %RDF.IRI{} = g} ->
          with {:ok, s_id} <- lookup_term_id_no_create(ctx.db, s),
               {:ok, p_id} <- lookup_term_id_no_create(ctx.db, p),
               {:ok, o_id} <- lookup_term_id_no_create(ctx.db, o),
               {:ok, g_id} <- lookup_term_id_no_create(ctx.db, g) do
            {:ok, {s_id, p_id, o_id, g_id}}
          else
            _ -> {:error, :not_found}
          end

        _ ->
          {:error, :invalid_quad}
      end)

    # Filter out not_found quads (they don't exist, can't be deleted)
    successes =
      results
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, quad} -> quad end)

    {:ok, successes}
  end

  # ===========================================================================
  # Private Helpers: Pattern Execution
  # ===========================================================================

  # Executes a WHERE pattern and returns all bindings
  defp execute_where_pattern(_ctx, nil), do: {:ok, [%{}]}

  defp execute_where_pattern(ctx, {:bgp, patterns}) do
    # execute_bgp always returns {:ok, stream}
    {:ok, stream} = Executor.execute_bgp(ctx, patterns)

    # Materialize the stream with limit
    bindings =
      stream
      |> Stream.take(@max_pattern_matches + 1)
      |> Enum.to_list()

    {:ok, bindings}
  end

  defp execute_where_pattern(ctx, {:graph, graph_irn, {:bgp, patterns}}) do
    # For GRAPH clauses, we need to execute the BGP in the context of the specified graph
    # The graph IRN should be added to the pattern as the graph component
    # Convert triple patterns to quad patterns with the specified graph
    quad_patterns = Enum.map(patterns, fn
      {:triple, s, p, o} -> {:quad, s, p, o, graph_irn}
      quad_pattern -> quad_pattern  # Already a quad pattern
    end)

    # Execute with quad patterns
    execute_where_pattern(ctx, {:bgp, quad_patterns})
  end

  defp execute_where_pattern(_ctx, pattern) do
    # For now, only BGP and GRAPH patterns are supported
    {:error, {:unsupported_pattern, pattern}}
  end

  # ===========================================================================
  # Private Helpers: Template Instantiation
  # ===========================================================================

  # Instantiates a template with bindings to produce ground triples
  defp instantiate_template([], _bindings), do: []

  defp instantiate_template(template, bindings) do
    for binding <- bindings,
        pattern <- template,
        triple <- instantiate_pattern(pattern, binding),
        do: triple
  end

  # Instantiates a single pattern with a binding
  defp instantiate_pattern({:triple, s, p, o}, binding) do
    with {:ok, s_val} <- substitute(s, binding),
         {:ok, p_val} <- substitute(p, binding),
         {:ok, o_val} <- substitute(o, binding) do
      [{ast_to_rdf(s_val), ast_to_rdf(p_val), ast_to_rdf(o_val)}]
    else
      :unbound -> []
    end
  end

  defp instantiate_pattern({:quad, s, p, o, g}, binding) do
    with {:ok, s_val} <- substitute(s, binding),
         {:ok, p_val} <- substitute(p, binding),
         {:ok, o_val} <- substitute(o, binding),
         {:ok, g_val} <- substitute_graph(g, binding) do
      [{ast_to_rdf(s_val), ast_to_rdf(p_val), ast_to_rdf(o_val), ast_graph_to_rdf(g_val)}]
    else
      :unbound -> []
    end
  end

  defp instantiate_pattern({:bgp, triples}, binding) do
    Enum.flat_map(triples, &instantiate_pattern(&1, binding))
  end

  defp instantiate_pattern(_, _binding), do: []

  # Substitutes variables in a graph term
  defp substitute_graph(:default_graph, _binding), do: {:ok, :default_graph}
  defp substitute_graph(:default, _binding), do: {:ok, :default}
  defp substitute_graph({:variable, name}, binding) do
    case Map.get(binding, name) do
      nil -> :unbound
      value -> {:ok, value}
    end
  end
  defp substitute_graph(graph, _binding), do: {:ok, graph}

  # Substitutes variables in a term with values from binding
  defp substitute({:variable, name}, binding) do
    case Map.get(binding, name) do
      nil -> :unbound
      value -> {:ok, value}
    end
  end

  defp substitute(term, _binding), do: {:ok, term}

  # ===========================================================================
  # Private Helpers: Atomic Operations
  # ===========================================================================

  # Performs atomic delete + insert operation
  defp execute_atomic_modify(db, delete_triples, insert_triples) do
    # Filter out nil entries from lookup failures
    valid_deletes = Enum.filter(delete_triples, &(&1 != nil))
    valid_inserts = Enum.filter(insert_triples, &is_tuple/1)

    # Build combined operations
    delete_ops =
      for {s, p, o} <- valid_deletes,
          {cf, key} <- Index.encode_triple_keys(s, p, o) do
        {:delete, cf, key}
      end

    insert_ops =
      for {s, p, o} <- valid_inserts,
          {cf, key} <- Index.encode_triple_keys(s, p, o) do
        {:put, cf, key, <<>>}
      end

    # Execute as single batch
    all_ops = delete_ops ++ insert_ops

    case execute_batch(db, all_ops) do
      :ok ->
        {:ok, length(valid_deletes) + length(valid_inserts)}

      {:error, _} = error ->
        error
    end
  end

  # Performs atomic delete + insert operation for quad stores
  defp execute_atomic_modify_quads(ctx, delete_quads, insert_quads) do
    # Filter to ensure we only have valid quads
    valid_deletes = Enum.filter(delete_quads, &is_valid_quad/1)
    valid_inserts = Enum.filter(insert_quads, &is_valid_quad/1)

    # First delete, then insert
    delete_count =
      if valid_deletes == [] do
        0
      else
        case QuadOperations.delete_quads(ctx.db, valid_deletes, []) do
          :ok -> length(valid_deletes)
          {:error, _} -> 0
        end
      end

    insert_count =
      if valid_inserts == [] do
        0
      else
        # Insert each quad and count
        Enum.reduce(valid_inserts, 0, fn {s_id, p_id, o_id, g_id}, count ->
          case QuadOperations.insert_quad(ctx.db, {s_id, p_id, o_id, g_id}) do
            :ok -> count + 1
            {:error, _} -> count
          end
        end)
      end

    {:ok, delete_count + insert_count}
  end

  # Check if a quad is valid (4-element tuple with integers)
  defp is_valid_quad({s, p, o, g}) when is_integer(s) and is_integer(p) and is_integer(o) and is_integer(g), do: true
  defp is_valid_quad(_), do: false

  # Executes a batch of operations
  defp execute_batch(_db, []), do: :ok

  defp execute_batch(db, operations) do
    alias TripleStore.Backend.RocksDB.NIF

    # Convert to NIF format
    {puts, deletes} =
      Enum.reduce(operations, {[], []}, fn
        {:put, cf, key, value}, {puts, deletes} ->
          {[{cf, key, value} | puts], deletes}

        {:delete, cf, key}, {puts, deletes} ->
          {puts, [{cf, key} | deletes]}
      end)

    # Execute deletes first, then puts
    # SPARQL updates use sync: true for data integrity
    with :ok <- if(deletes == [], do: :ok, else: NIF.delete_batch(db, deletes, true)) do
      if(puts == [], do: :ok, else: NIF.write_batch(db, puts, true))
    end
  end

  # ===========================================================================
  # Cache Invalidation
  # ===========================================================================

  @doc false
  @spec invalidate_cache_for_operations([term()]) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def invalidate_cache_for_operations(operations) do
    # Skip if cache is not running
    if cache_running?() do
      predicates = extract_predicates_from_operations(operations)

      if predicates == :full_invalidation do
        # Complex operation - invalidate everything
        emit_full_invalidation()
        QueryCache.invalidate()
      else
        case MapSet.size(predicates) do
          0 ->
            # No predicates extracted - skip invalidation
            :ok

          _n ->
            # Targeted invalidation based on predicates
            emit_predicate_invalidation(predicates)
            QueryCache.invalidate_predicates(predicates)
        end
      end
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
    delete_template = Keyword.get(props, :delete, [])
    insert_template = Keyword.get(props, :insert, [])
    pattern = Keyword.get(props, :pattern)

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
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp extract_predicates_from_pattern({:bgp, triples}) when is_list(triples) do
    Enum.reduce(triples, MapSet.new(), fn triple, acc ->
      case triple do
        {:triple, _s, p, _o} ->
          case ast_term_to_rdf(p) do
            nil -> acc
            pred -> MapSet.put(acc, pred)
          end

        _ ->
          acc
      end
    end)
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

  # Convert AST term to RDF term (only for ground terms)
  defp ast_term_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_term_to_rdf({:variable, _name}), do: nil
  defp ast_term_to_rdf(_), do: nil

  # Get a property value from a list that may have string or atom keys
  defp get_prop(props, key, default \\ nil) do
    # Try string key first (from parser)
    case List.keyfind(props, key, 0) do
      {^key, value} -> value
      nil ->
        # Try atom key (for keyword lists)
        atom_key = String.to_atom(key)
        Keyword.get(props, atom_key, default)
    end
  end

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
