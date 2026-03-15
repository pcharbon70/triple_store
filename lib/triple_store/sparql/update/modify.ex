defmodule TripleStore.SPARQL.Update.Modify do
  @moduledoc """
  MODIFY (DELETE/INSERT WHERE) operation handlers for SPARQL UPDATE.

  This module handles pattern-based DELETE and INSERT operations for both
  triple and quad stores.

  MODIFY operations execute a WHERE clause to get bindings, then apply
  those bindings to DELETE and INSERT templates to generate the actual
  triples/quads to delete and insert.

  ## Authorization

  MODIFY operations check write authorization on all affected graphs.
  If graphs can't be determined statically (variables in templates),
  the operation proceeds and authorization is checked during execution.

  ## Examples

      # DELETE WHERE
      delete_template = [{:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}]
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}]}
      {:ok, count} = Modify.execute(ctx, delete_template, [], pattern)

      # INSERT WHERE
      insert_template = [{:triple, {:variable, "s"}, {:named_node, "http://example.org/upper"}, {:variable, "upper"}}]
      {:ok, count} = Modify.execute(ctx, [], insert_template, pattern)

      # DELETE/INSERT WHERE (combined)
      {:ok, count} = Modify.execute(ctx, delete_template, insert_template, pattern)

  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary
  alias TripleStore.Index
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Executor
  alias TripleStore.SPARQL.Update.Helpers

  # Maximum pattern matches to prevent DoS
  @max_pattern_matches 1_000_000

  # Maximum template size (triples per template)
  @max_template_size 1_000

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Executes a MODIFY operation (DELETE/INSERT WHERE).

  ## Arguments

  - `ctx` - Execution context with :db, :dict_manager, and optional :user
  - `delete_template` - List of patterns to delete (ground or with variables)
  - `insert_template` - List of patterns to insert (ground or with variables)
  - `pattern` - WHERE clause pattern (e.g., {:bgp, [...]})

  ## Returns

  - `{:ok, count}` - Total number of triples affected
  - `{:error, reason}` - On failure

  ## Authorization

  Checks write authorization on all affected graphs that can be determined
  statically from templates.

  """
  @spec execute(map(), [term()], [term()], term()) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute(_ctx, [], [], _pattern), do: {:ok, 0}

  def execute(ctx, delete_template, insert_template, pattern) do
    # Validate template sizes
    if length(delete_template) > @max_template_size or
         length(insert_template) > @max_template_size do
      {:error, :template_too_large}
    else
      # Extract graphs from templates for authorization check
      delete_graphs = Helpers.extract_graphs_from_templates(delete_template)
      insert_graphs = Helpers.extract_graphs_from_templates(insert_template)
      all_graphs = Enum.uniq(delete_graphs ++ insert_graphs)

      authorize_modify_graphs(ctx, all_graphs, delete_template, insert_template, pattern)
    end
  end

  # Execute after authorization check
  defp execute_after_auth(ctx, delete_template, insert_template, pattern) do
    # Check if we're using a quad store
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        execute_modify_quad(ctx, delete_template, insert_template, pattern)

      {:ok, false} ->
        execute_modify_triples(ctx, delete_template, insert_template, pattern)
    end
  end

  # ===========================================================================
  # Triple Store MODIFY
  # ===========================================================================

  defp execute_modify_triples(ctx, delete_template, insert_template, pattern) do
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
          execute_atomic_modify_triples(ctx.db, delete_internal, insert_internal)
        end

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Quad Store MODIFY
  # ===========================================================================

  defp execute_modify_quad(ctx, delete_template, insert_template, pattern) do
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

  # ===========================================================================
  # Pattern Execution
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
    quad_patterns =
      Enum.map(patterns, fn
        {:triple, s, p, o} -> {:quad, s, p, o, graph_irn}
        # Already a quad pattern
        quad_pattern -> quad_pattern
      end)

    # Execute with quad patterns
    execute_where_pattern(ctx, {:bgp, quad_patterns})
  end

  defp execute_where_pattern(_ctx, pattern) do
    # For now, only BGP and GRAPH patterns are supported
    {:error, {:unsupported_pattern, pattern}}
  end

  # ===========================================================================
  # Template Instantiation
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
  # Internal Conversion
  # ===========================================================================

  # Converts triples to internal representation with mode
  defp triples_to_internal(_ctx, [], _mode), do: {:ok, []}

  defp triples_to_internal(ctx, triples, :lookup) do
    results =
      Enum.map(triples, fn {s, p, o} ->
        with {:ok, s_id} <- lookup_term_id_no_create(ctx.db, s),
             {:ok, p_id} <- lookup_term_id_no_create(ctx.db, p),
             {:ok, o_id} <- lookup_term_id_no_create(ctx.db, o) do
          {s_id, p_id, o_id}
        else
          _ -> nil
        end
      end)

    {:ok, Enum.filter(results, &(&1 != nil))}
  end

  defp triples_to_internal(ctx, triples, :create) do
    Adapter.from_rdf_triples(ctx.dict_manager, triples)
  end

  # Converts quads to internal representation with mode
  defp quads_to_internal(_ctx, [], _mode), do: {:ok, []}

  defp quads_to_internal(ctx, quads, :lookup) do
    results =
      Enum.map(quads, fn
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

    successes =
      results
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, quad} -> quad end)

    {:ok, successes}
  end

  defp quads_to_internal(ctx, quads, :create) do
    # For create mode, convert each quad and create IDs as needed
    results =
      Enum.map(quads, fn
        {s, p, o} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            {:error, _} = error -> error
          end

        {s, p, o, :default} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o) do
            {:ok, {s_id, p_id, o_id, 0}}
          else
            {:error, _} = error -> error
          end

        {s, p, o, %RDF.IRI{} = g} ->
          with {:ok, s_id} <- Adapter.term_to_id(ctx.dict_manager, s),
               {:ok, p_id} <- Adapter.term_to_id(ctx.dict_manager, p),
               {:ok, o_id} <- Adapter.term_to_id(ctx.dict_manager, o),
               {:ok, g_id} <- Adapter.term_to_id(ctx.dict_manager, g) do
            {:ok, {s_id, p_id, o_id, g_id}}
          else
            {:error, _} = error -> error
          end

        _ ->
          {:error, :invalid_quad}
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        internal_quads = Enum.map(results, fn {:ok, quad} -> quad end)
        {:ok, internal_quads}

      {:error, _} = error ->
        error
    end
  end

  # Look up term ID without creating it
  defp lookup_term_id_no_create(db, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      Dictionary.StringToId.lookup_id(db, literal)
    end
  end

  defp lookup_term_id_no_create(db, term) do
    Dictionary.StringToId.lookup_id(db, term)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}}) do
    {:ok, Dictionary.encode_integer(value)}
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    {:ok, Dictionary.encode_decimal(value)}
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    {:ok, Dictionary.encode_datetime(value)}
  end

  defp encode_inline_literal(_literal) do
    {:error, :not_inline_encodable}
  end

  # ===========================================================================
  # Atomic Operations
  # ===========================================================================

  # Performs atomic delete + insert operation for triple stores
  defp execute_atomic_modify_triples(db, delete_triples, insert_triples) do
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
    valid_deletes = Enum.filter(delete_quads, &valid_quad?/1)
    valid_inserts = Enum.filter(insert_quads, &valid_quad?/1)

    delete_count = delete_valid_quads(ctx, valid_deletes)
    insert_count = insert_valid_quads(ctx, valid_inserts)

    {:ok, delete_count + insert_count}
  end

  # Check if a quad is valid (4-element tuple with integers)
  defp valid_quad?({s, p, o, g})
       when is_integer(s) and is_integer(p) and is_integer(o) and is_integer(g),
       do: true

  defp valid_quad?(_), do: false

  defp authorize_modify_graphs(ctx, [], delete_template, insert_template, pattern) do
    execute_after_auth(ctx, delete_template, insert_template, pattern)
  end

  defp authorize_modify_graphs(ctx, all_graphs, delete_template, insert_template, pattern) do
    case Helpers.check_multi_graph_authorization(ctx, all_graphs, :write) do
      :ok -> execute_after_auth(ctx, delete_template, insert_template, pattern)
      {:error, :unauthorized} -> {:error, :unauthorized}
    end
  end

  defp delete_valid_quads(_ctx, []), do: 0

  defp delete_valid_quads(ctx, valid_deletes) do
    case QuadOperations.delete_quads(ctx.db, valid_deletes, []) do
      :ok -> length(valid_deletes)
      {:error, _} -> 0
    end
  end

  defp insert_valid_quads(_ctx, []), do: 0

  defp insert_valid_quads(ctx, valid_inserts) do
    Enum.reduce(valid_inserts, 0, fn {s_id, p_id, o_id, g_id}, count ->
      case QuadOperations.insert_quad(ctx.db, {s_id, p_id, o_id, g_id}) do
        :ok -> count + 1
        {:error, _} -> count
      end
    end)
  end

  # Executes a batch of operations
  defp execute_batch(_db, []), do: :ok

  defp execute_batch(db, operations) do
    alias TripleStore.Backend.RocksDB.ErlangAdapter

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
    with :ok <- if(deletes == [], do: :ok, else: ErlangAdapter.delete_batch(db, deletes, true)) do
      if(puts == [], do: :ok, else: ErlangAdapter.write_batch(db, puts, true))
    end
  end

  # ===========================================================================
  # AST Conversion
  # ===========================================================================

  defp ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  defp ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :language_tagged, value, lang}),
    do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :typed, value, datatype}),
    do: RDF.literal(value, datatype: RDF.iri(datatype))

  defp ast_to_rdf(term), do: term

  defp ast_graph_to_rdf(:default), do: :default
  defp ast_graph_to_rdf(:default_graph), do: :default
  defp ast_graph_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:named_graph, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:iri, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
end
