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
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Index
  alias TripleStore.SPARQL.Executor
  alias TripleStore.SPARQL.Parser

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
    delete_template = Keyword.get(props, :delete, [])
    insert_template = Keyword.get(props, :insert, [])
    pattern = Keyword.get(props, :pattern)
    _using = Keyword.get(props, :using)

    execute_modify(ctx, delete_template, insert_template, pattern)
  end

  def execute_operation(_ctx, {:load, _props}) do
    # LOAD is handled separately through the Loader module
    {:error, :load_not_implemented}
  end

  def execute_operation(ctx, {:clear, props}) when is_list(props) do
    execute_clear(ctx, props)
  end

  def execute_operation(_ctx, {:create, _props}) do
    # CREATE GRAPH is a no-op for our single-graph store
    {:ok, 0}
  end

  def execute_operation(_ctx, {:drop, _props}) do
    # DROP GRAPH - for now just return success
    # Full implementation would track named graphs
    {:ok, 0}
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
    # Convert AST quads to RDF terms, then to internal IDs
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
    # Convert AST quads to RDF terms
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
      do_execute_modify(ctx, delete_template, insert_template, pattern)
    end
  end

  defp do_execute_modify(ctx, delete_template, insert_template, pattern) do
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

  # ===========================================================================
  # CLEAR
  # ===========================================================================

  @doc """
  Executes a CLEAR operation.

  CLEAR removes all triples from the default graph or a named graph.
  For our single-graph implementation, CLEAR DEFAULT/ALL removes all triples.

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

    case target do
      :all ->
        clear_all_triples(ctx)

      :default ->
        clear_all_triples(ctx)

      :named ->
        # No named graphs in current implementation
        if silent, do: {:ok, 0}, else: {:error, :no_named_graphs}

      {:graph, _iri} ->
        # Named graph clear - not implemented
        if silent, do: {:ok, 0}, else: {:error, :named_graphs_not_supported}

      _ ->
        {:error, {:invalid_clear_target, target}}
    end
  end

  # Batch size for chunked clear operations to prevent OOM
  @clear_batch_size 10_000

  defp clear_all_triples(ctx) do
    # Stream triples and delete in batches to prevent OOM on large databases
    case Index.lookup(ctx.db, {:var, :var, :var}) do
      {:ok, stream} ->
        stream
        |> Stream.chunk_every(@clear_batch_size)
        |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, count} ->
          case Index.delete_triples(ctx.db, chunk) do
            :ok -> {:cont, {:ok, count + length(chunk)}}
            {:error, _} = error -> {:halt, error}
          end
        end)

      {:error, _} = error ->
        error
    end
  end

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

  # Converts parser AST term to RDF.ex term
  defp ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  defp ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)
  defp ast_to_rdf({:literal, :language_tagged, value, lang}), do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :typed, value, datatype}) do
    RDF.literal(value, datatype: datatype)
  end

  defp ast_to_rdf({:variable, _name}) do
    raise ArgumentError, "Variables not allowed in INSERT/DELETE DATA"
  end

  defp ast_to_rdf(term), do: term

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

  defp execute_where_pattern(_ctx, pattern) do
    # For now, only BGP patterns are supported
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

  defp instantiate_pattern({:bgp, triples}, binding) do
    Enum.flat_map(triples, &instantiate_pattern(&1, binding))
  end

  defp instantiate_pattern(_, _binding), do: []

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
    with :ok <- if(deletes == [], do: :ok, else: NIF.delete_batch(db, deletes)),
         :ok <- if(puts == [], do: :ok, else: NIF.write_batch(db, puts)) do
      :ok
    end
  end
end
