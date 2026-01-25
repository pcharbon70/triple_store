defmodule TripleStore.SPARQL.Update.DeleteData do
  @moduledoc """
  DELETE DATA operation handlers for SPARQL UPDATE.

  This module handles DELETE DATA operations for both triple and quad stores.
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Index
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Update.Helpers
  alias TripleStore.Statistics

  @max_data_triples 10_000

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Executes a DELETE DATA operation.
  """
  @spec execute(map(), [term()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute(_ctx, []), do: {:ok, 0}

  def execute(_ctx, quads) when length(quads) > @max_data_triples do
    {:error, :too_many_triples}
  end

  def execute(ctx, quads) when is_list(quads) do
    # Extract graphs from quads for authorization check
    graph_terms = Helpers.extract_graphs_from_quads(quads)

    # Check write authorization on all target graphs
    case Helpers.check_multi_graph_authorization(ctx, graph_terms, :write) do
      :ok ->
        # Check if we're using a quad store
        case ErlangAdapter.is_quad_store?(ctx.db) do
          {:ok, true} -> delete_quads(ctx, quads)
          {:ok, false} -> delete_triples_from_store(ctx, quads)
        end

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  @doc """
  Executes a DELETE WHERE operation.
  """
  @spec execute_delete_where(map(), term()) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute_delete_where(ctx, pattern) do
    # Execute WHERE pattern to get bindings
    case execute_where_pattern(ctx, pattern) do
      {:ok, []} ->
        {:ok, 0}

      {:ok, bindings} ->
        # Convert pattern to quads with bindings
        case pattern_to_quads(ctx, pattern, bindings) do
          {:ok, quads} -> delete_quads(ctx, quads)
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Quad Deletion (for quad stores)
  # ===========================================================================

  defp delete_quads(ctx, quads) do
    with {:ok, rdf_quads} <- quads_to_rdf_quads(quads),
         {:ok, internal_quads} <- quads_to_internal(ctx, rdf_quads, :lookup) do
      if internal_quads == [] do
        {:ok, 0}
      else
        # Filter out quads that don't actually exist in the store (for idempotence)
        existing_quads =
          Enum.filter(internal_quads, fn quad ->
            case quad do
              {s, p, o, g}
              when is_integer(s) and is_integer(p) and is_integer(o) and is_integer(g) ->
                QuadOperations.quad_exists?(ctx.db, {s, p, o, g})

              _ ->
                false
            end
          end)

        if existing_quads == [] do
          {:ok, 0}
        else
          case QuadOperations.delete_quads(ctx.db, existing_quads, []) do
            :ok ->
              # Invalidate statistics cache for affected graphs
              invalidate_graphs_cache(ctx.db, existing_quads)
              {:ok, length(existing_quads)}

            {:error, _} = error ->
              error
          end
        end
      end
    end
  end

  # Invalidate statistics cache for graphs affected by the operation
  defp invalidate_graphs_cache(db, quads) do
    quads
    |> Enum.map(fn
      {s, _p, _o, g} when is_integer(s) -> g
      {_s, _p, _o, g} -> g
    end)
    |> Enum.uniq()
    |> Enum.each(fn graph_id -> Statistics.invalidate_quad_cache(db, graph_id) end)
  end

  # ===========================================================================
  # Triple Deletion (for triple stores)
  # ===========================================================================

  defp delete_triples_from_store(ctx, quads) do
    with {:ok, rdf_triples} <- quads_to_rdf_triples(quads),
         {:ok, internal_triples} <- triples_to_internal(ctx, rdf_triples, :lookup) do
      if internal_triples == [] do
        {:ok, 0}
      else
        case Index.delete_triples(ctx.db, internal_triples) do
          :ok -> {:ok, length(internal_triples)}
          {:error, _} = error -> error
        end
      end
    end
  end

  # ===========================================================================
  # Private Helpers - Conversion
  # ===========================================================================

  defp quads_to_rdf_triples(quads) do
    # Convert AST quads to RDF triples (extract from default graph quads)
    triples =
      Enum.map(quads, fn
        {:quad, s, p, o, _g} -> {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
        quad -> ast_to_triple(quad)
      end)

    {:ok, triples}
  end

  defp quads_to_rdf_quads(quads) do
    rdf_quads = Enum.map(quads, &ast_to_rdf_quad/1)
    {:ok, rdf_quads}
  end

  defp quads_to_internal(_ctx, [], _mode), do: {:ok, []}

  defp quads_to_internal(ctx, quads, :lookup) do
    # For DELETE, only look up existing IDs
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

    # Filter out not_found quads (they don't exist, can't be deleted)
    successes =
      results
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, quad} -> quad end)

    {:ok, successes}
  end

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

    successes = Enum.filter(results, &(&1 != nil))
    {:ok, successes}
  end

  # ===========================================================================
  # Private Helpers - Pattern Execution
  # ===========================================================================

  # Executes a WHERE pattern and returns all bindings
  defp execute_where_pattern(_ctx, nil), do: {:ok, [%{}]}

  defp execute_where_pattern(ctx, {:bgp, patterns}) do
    # Delegate to SPARQL.Executor for pattern execution
    alias TripleStore.SPARQL.Executor

    # execute_bgp always returns {:ok, stream}
    {:ok, stream} = Executor.execute_bgp(ctx, patterns)

    # Materialize the stream
    bindings = Enum.to_list(stream)
    {:ok, bindings}
  end

  defp execute_where_pattern(_ctx, pattern) do
    {:error, {:unsupported_pattern, pattern}}
  end

  # Converts pattern and bindings to quads for deletion
  defp pattern_to_quads(_ctx, _pattern, []), do: {:ok, []}

  defp pattern_to_quads(_ctx, {:bgp, _patterns}, _bindings) do
    # This is a simplified implementation - a full implementation would
    # need to properly instantiate pattern variables with bindings
    # For now, return empty as DELETE WHERE is already handled elsewhere
    {:ok, []}
  end

  # ===========================================================================
  # Private Helpers - Term Lookup
  # ===========================================================================

  defp lookup_term_id_no_create(db, %RDF.Literal{} = literal) do
    if TripleStore.Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      TripleStore.Dictionary.StringToId.lookup_id(db, literal)
    end
  end

  defp lookup_term_id_no_create(db, term) do
    TripleStore.Dictionary.StringToId.lookup_id(db, term)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}}) do
    TripleStore.Dictionary.encode_integer(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    TripleStore.Dictionary.encode_decimal(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    TripleStore.Dictionary.encode_datetime(value)
  end

  defp encode_inline_literal(_literal) do
    {:error, :not_inline_encodable}
  end

  # ===========================================================================
  # Private Helpers - AST Conversion
  # ===========================================================================

  defp ast_to_rdf_quad({:quad, s, p, o, g}) do
    {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), ast_graph_to_rdf(g)}
  end

  defp ast_to_rdf_quad({:triple, s, p, o}) do
    {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}
  end

  defp ast_to_rdf_quad({s, p, o}) do
    {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}
  end

  defp ast_to_triple({:triple, s, p, o}) do
    {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
  end

  defp ast_to_triple({s, p, o}) do
    {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
  end

  defp ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  defp ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :language_tagged, value, lang}),
    do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :typed, value, datatype}),
    do: RDF.literal(value, datatype: RDF.iri(datatype))

  defp ast_to_rdf({:variable, _name}),
    do: raise(ArgumentError, "Variables not allowed in DELETE DATA")

  defp ast_to_rdf(term), do: term

  defp ast_graph_to_rdf(:default), do: :default
  defp ast_graph_to_rdf(:default_graph), do: :default
  defp ast_graph_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:named_graph, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:iri, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
end
