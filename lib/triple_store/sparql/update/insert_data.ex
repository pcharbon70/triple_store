defmodule TripleStore.SPARQL.Update.InsertData do
  @moduledoc """
  INSERT DATA operation handlers for SPARQL UPDATE.

  This module handles INSERT DATA operations for both triple and quad stores.
  """

  alias TripleStore.Adapter
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
  Executes an INSERT DATA operation.
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
          {:ok, true} -> insert_quads(ctx, quads)
          {:ok, false} -> insert_triples(ctx, quads)
        end

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  # ===========================================================================
  # Quad Insertion (for quad stores)
  # ===========================================================================

  defp insert_quads(ctx, quads) do
    with {:ok, rdf_quads} <- quads_to_rdf_quads(quads),
         {:ok, count} <- do_insert_quads(ctx, rdf_quads) do
      {:ok, count}
    end
  end

  # Perform the actual quad insertion
  defp do_insert_quads(ctx, rdf_quads) do
    # Optimized batch insertion
    with {:ok, internal_quads} <- convert_rdf_quads_to_internal(ctx, rdf_quads),
         :ok <- QuadOperations.insert_quads(ctx.db, internal_quads, sync: true) do
      # Invalidate statistics cache for affected graphs
      invalidate_graphs_cache(ctx.db, internal_quads)
      {:ok, length(internal_quads)}
    else
      {:error, _} = error -> error
    end
  end

  # Invalidate statistics cache for graphs affected by the operation
  defp invalidate_graphs_cache(db, quads) do
    quads
    |> Enum.map(fn {_s, _p, _o, g_id} -> g_id end)
    |> Enum.uniq()
    |> Enum.each(fn graph_id -> Statistics.invalidate_quad_cache(db, graph_id) end)
  end

  # Converts RDF quads to internal quad representation with IDs
  defp convert_rdf_quads_to_internal(ctx, rdf_quads) do
    # Collect all unique terms that need ID conversion
    {subjects, predicates, objects, graphs} =
      Enum.reduce(rdf_quads, {MapSet.new(), MapSet.new(), MapSet.new(), MapSet.new()}, fn
        {s, p, o, g}, {s_acc, p_acc, o_acc, g_acc} ->
          {MapSet.put(s_acc, s), MapSet.put(p_acc, p), MapSet.put(o_acc, o), MapSet.put(g_acc, g)}
      end)

    # Convert terms to IDs (graph terms separately as they may be :default)
    with {:ok, subject_ids} <-
           convert_terms_to_id_map(ctx.dict_manager, MapSet.to_list(subjects)),
         {:ok, predicate_ids} <-
           convert_terms_to_id_map(ctx.dict_manager, MapSet.to_list(predicates)),
         {:ok, object_ids} <- convert_terms_to_id_map(ctx.dict_manager, MapSet.to_list(objects)),
         {:ok, graph_ids} <- convert_graph_terms_to_id_map(ctx, MapSet.to_list(graphs)) do
      # Build all quad tuples with their IDs
      all_maps = %{
        subject: subject_ids,
        predicate: predicate_ids,
        object: object_ids,
        graph: graph_ids
      }

      internal_quads =
        Enum.reduce(rdf_quads, [], fn {s, p, o, g}, acc ->
          case get_quad_ids(all_maps, s, p, o, g) do
            {:ok, quad} -> [quad | acc]
            :error -> acc
          end
        end)

      {:ok, Enum.reverse(internal_quads)}
    else
      {:error, _} = error -> error
    end
  end

  # Converts a list of RDF terms to a map of term => ID
  defp convert_terms_to_id_map(manager, terms) do
    Enum.reduce_while(terms, {:ok, %{}}, fn term, {:ok, acc} ->
      case Adapter.term_to_id(manager, term) do
        {:ok, id} -> {:cont, {:ok, Map.put(acc, term, id)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # Converts graph terms to IDs (handles :default specially)
  defp convert_graph_terms_to_id_map(ctx, graphs) do
    Enum.reduce_while(graphs, {:ok, %{}}, fn graph, {:ok, acc} ->
      id =
        case graph do
          :default -> {:ok, 0}
          :default_graph -> {:ok, 0}
          %RDF.IRI{} = iri -> Adapter.term_to_id(ctx.dict_manager, iri)
          term -> Adapter.term_to_id(ctx.dict_manager, term)
        end

      case id do
        {:ok, graph_id} -> {:cont, {:ok, Map.put(acc, graph, graph_id)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # Gets the quad tuple from the ID maps
  defp get_quad_ids(maps, s, p, o, g) do
    with {:ok, s_id} <- Map.fetch(maps.subject, s),
         {:ok, p_id} <- Map.fetch(maps.predicate, p),
         {:ok, o_id} <- Map.fetch(maps.object, o),
         {:ok, g_id} <- Map.fetch(maps.graph, g) do
      {:ok, {s_id, p_id, o_id, g_id}}
    else
      :error -> :error
    end
  end

  # ===========================================================================
  # Triple Insertion (for triple stores)
  # ===========================================================================

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
    # Convert AST quads to RDF quads (preserving graph component)
    rdf_quads = Enum.map(quads, &ast_to_rdf_quad/1)
    {:ok, rdf_quads}
  end

  # Converts AST quad to RDF quad
  defp ast_to_rdf_quad({:quad, s, p, o, g}),
    do: {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), ast_graph_to_rdf(g)}

  defp ast_to_rdf_quad({:triple, s, p, o}),
    do: {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}

  defp ast_to_rdf_quad({s, p, o}), do: {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o), :default}

  # Converts AST triple to RDF triple
  defp ast_to_triple({:triple, s, p, o}), do: {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}
  defp ast_to_triple({s, p, o}), do: {ast_to_rdf(s), ast_to_rdf(p), ast_to_rdf(o)}

  # Converts AST term to RDF term
  defp ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  defp ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :language_tagged, value, lang}),
    do: RDF.literal(value, language: lang)

  defp ast_to_rdf({:literal, :typed, value, datatype}),
    do: RDF.literal(value, datatype: RDF.iri(datatype))

  defp ast_to_rdf({:variable, _name}),
    do: raise(ArgumentError, "Variables not allowed in INSERT DATA")

  defp ast_to_rdf(term), do: term

  # Converts AST graph term to RDF graph term
  defp ast_graph_to_rdf(:default), do: :default
  defp ast_graph_to_rdf(:default_graph), do: :default
  defp ast_graph_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:named_graph, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf({:iri, iri}), do: RDF.iri(iri)
  defp ast_graph_to_rdf(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
end
