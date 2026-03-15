defmodule TripleStore.SPARQL.Update.GraphOperations do
  @moduledoc """
  Graph management operations for SPARQL UPDATE.

  This module handles CREATE, DROP, CLEAR, COPY, MOVE, and ADD operations
  for both triple and quad stores.
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Update.Helpers

  # Maximum size for chunked clear operations
  @clear_batch_size 10_000

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Executes a CREATE GRAPH operation.
  """
  @spec execute_create_graph(map(), keyword()) :: {:ok, 0} | {:error, term()}
  def execute_create_graph(ctx, props) do
    graph_iri = Helpers.get_prop(props, "graph")
    silent = Helpers.get_prop(props, "silent", false)

    case create_graph_request(graph_iri, silent) do
      {:perform, rdf_graph} -> create_graph_with_authorization(ctx, rdf_graph, silent)
      result -> result
    end
  end

  @doc """
  Executes a DROP GRAPH operation.
  """
  @spec execute_drop_graph(map(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute_drop_graph(ctx, props) do
    graph_iri = Helpers.get_prop(props, "graph")
    silent = Helpers.get_prop(props, "silent", false)

    case drop_graph_request(graph_iri, silent) do
      {:perform, rdf_graph} -> drop_graph_with_authorization(ctx, rdf_graph, silent)
      result -> result
    end
  end

  @doc """
  Executes a CLEAR operation.
  """
  @spec execute_clear(map(), keyword() | atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute_clear(ctx, {:clear, target}) do
    graph_target = Keyword.get(target, "graph", target)
    silent = Keyword.get(target, "silent", false)
    # Normalize parser atoms to internal atoms
    normalized_target = normalize_clear_target(graph_target)
    execute_clear(ctx, normalized_target, silent)
  end

  def execute_clear(ctx, props) when is_list(props) do
    target = Helpers.get_prop(props, "graph", :default)
    silent = Helpers.get_prop(props, "silent", false)
    # Normalize parser atoms to internal atoms
    normalized_target = normalize_clear_target(target)
    execute_clear(ctx, normalized_target, silent)
  end

  defp execute_clear(ctx, target, silent) do
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        execute_clear_quad(ctx, target, silent)

      {:ok, false} ->
        execute_clear_triple(ctx, target, silent)
    end
  end

  # Normalize parser target atoms to internal atoms
  defp normalize_clear_target(:all_graphs), do: :all
  defp normalize_clear_target(:default_graph), do: :default
  defp normalize_clear_target(:all_named), do: :named
  defp normalize_clear_target(other), do: other

  @doc """
  Executes a COPY GRAPH operation.
  """
  @spec execute_copy(map(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_copy(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)
    execute_graph_transfer(ctx, source_graph, target_graph, silent, :copy)
  end

  @doc """
  Executes a MOVE GRAPH operation.
  """
  @spec execute_move(map(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_move(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    maybe_execute_distinct_graph_transfer(ctx, source_graph, target_graph, silent, :move)
  end

  @doc """
  Executes an ADD GRAPH operation.
  """
  @spec execute_add(map(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_add(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    maybe_execute_distinct_graph_transfer(ctx, source_graph, target_graph, silent, :add)
  end

  # ===========================================================================
  # Private Helpers - CLEAR Operations
  # ===========================================================================

  defp create_graph_request(nil, _silent), do: {:error, :missing_graph_iri}

  defp create_graph_request(graph_iri, true) when graph_iri in [:default, :default_graph],
    do: {:ok, 0}

  defp create_graph_request(graph_iri, false) when graph_iri in [:default, :default_graph],
    do: {:error, :default_graph_exists}

  defp create_graph_request(graph_iri, _silent),
    do: {:perform, Helpers.ast_term_to_rdf_graph(graph_iri)}

  defp create_graph_with_authorization(ctx, rdf_graph, silent) do
    case Helpers.check_admin_authorization(ctx, rdf_graph) do
      :ok ->
        QuadOperations.create_graph(ctx.db, ctx.dict_manager, rdf_graph)
        |> create_graph_result(silent)

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  defp create_graph_result({:ok, :created}, _silent) do
    Helpers.invalidate_cache_if_running()
    {:ok, 0}
  end

  defp create_graph_result({:ok, :already_exists}, true), do: {:ok, 0}
  defp create_graph_result({:ok, :already_exists}, false), do: {:error, :graph_already_exists}
  defp create_graph_result({:error, _reason}, true), do: {:ok, 0}
  defp create_graph_result({:error, reason}, false), do: {:error, reason}

  defp drop_graph_request(nil, _silent), do: {:error, :missing_graph_iri}

  defp drop_graph_request(graph_iri, true) when graph_iri in [:default, :default_graph],
    do: {:ok, 0}

  defp drop_graph_request(graph_iri, false) when graph_iri in [:default, :default_graph],
    do: {:error, :cannot_drop_default}

  defp drop_graph_request(graph_iri, _silent),
    do: {:perform, Helpers.ast_term_to_rdf_graph(graph_iri)}

  defp drop_graph_with_authorization(ctx, rdf_graph, silent) do
    case Helpers.check_admin_authorization(ctx, rdf_graph) do
      :ok ->
        QuadOperations.delete_graph(ctx.db, ctx.dict_manager, rdf_graph)
        |> drop_graph_result(silent)

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  defp drop_graph_result({:ok, count}, _silent) do
    Helpers.invalidate_cache_if_running()
    {:ok, count}
  end

  defp drop_graph_result({:error, :not_found}, true), do: {:ok, 0}
  defp drop_graph_result({:error, :not_found}, false), do: {:error, :graph_not_found}
  defp drop_graph_result({:error, _reason}, true), do: {:ok, 0}
  defp drop_graph_result({:error, reason}, false), do: {:error, reason}

  defp maybe_execute_distinct_graph_transfer(ctx, source_graph, target_graph, silent, operation) do
    source_rdf = Helpers.normalize_graph_term(source_graph)
    target_rdf = Helpers.normalize_graph_term(target_graph)

    if graphs_equal?(source_rdf, target_rdf) do
      {:ok, 0}
    else
      execute_graph_transfer(ctx, source_rdf, target_rdf, silent, operation, normalized: true)
    end
  end

  defp execute_graph_transfer(ctx, source_graph, target_graph, silent, operation, opts \\ []) do
    source_rdf = transfer_graph_term(source_graph, opts)
    target_rdf = transfer_graph_term(target_graph, opts)

    case authorize_graph_transfer(ctx, source_rdf, target_rdf, operation) do
      :ok ->
        if source_graph_exists?(ctx, source_rdf) do
          perform_graph_transfer(ctx, source_rdf, target_rdf, silent, operation)
        else
          {:ok, 0}
        end

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  defp transfer_graph_term(graph_term, normalized: true), do: graph_term
  defp transfer_graph_term(graph_term, _opts), do: Helpers.normalize_graph_term(graph_term)

  defp authorize_graph_transfer(ctx, source_rdf, target_rdf, :copy) do
    case Helpers.check_read_authorization(ctx, source_rdf) do
      :ok -> Helpers.check_write_authorization(ctx, target_rdf)
      error -> error
    end
  end

  defp authorize_graph_transfer(ctx, source_rdf, target_rdf, :move) do
    case Helpers.check_admin_authorization(ctx, source_rdf) do
      :ok -> Helpers.check_admin_authorization(ctx, target_rdf)
      error -> error
    end
  end

  defp authorize_graph_transfer(ctx, source_rdf, target_rdf, :add) do
    case Helpers.check_read_authorization(ctx, source_rdf) do
      :ok -> Helpers.check_write_authorization(ctx, target_rdf)
      error -> error
    end
  end

  defp source_graph_exists?(ctx, :default), do: QuadOperations.default_graph_exists?(ctx.db)

  defp source_graph_exists?(ctx, graph),
    do: QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)

  defp perform_graph_transfer(ctx, source_rdf, target_rdf, silent, operation) do
    case execute_transfer_operation(ctx, source_rdf, target_rdf, operation) do
      {:ok, count} ->
        Helpers.invalidate_cache_if_running()
        {:ok, count}

      {:error, reason} ->
        if silent, do: {:ok, 0}, else: {:error, reason}
    end
  end

  defp execute_transfer_operation(ctx, source_rdf, target_rdf, :copy) do
    QuadOperations.copy_graph(
      ctx.db,
      ctx.dict_manager,
      source_rdf,
      target_rdf,
      on_conflict: :replace
    )
  end

  defp execute_transfer_operation(ctx, source_rdf, target_rdf, :move) do
    QuadOperations.move_quads(
      ctx.db,
      ctx.dict_manager,
      source_rdf,
      target_rdf,
      on_conflict: :replace
    )
  end

  defp execute_transfer_operation(ctx, source_rdf, target_rdf, :add) do
    QuadOperations.copy_graph(
      ctx.db,
      ctx.dict_manager,
      source_rdf,
      target_rdf,
      on_conflict: :merge
    )
  end

  # CLEAR for triple stores
  defp execute_clear_triple(ctx, :all, _silent) do
    clear_all_triples(ctx)
  end

  defp execute_clear_triple(ctx, :default, _silent) do
    clear_default_graph(ctx)
  end

  defp execute_clear_triple(_ctx, :named, _silent) do
    {:error, :clear_named_not_supported_for_triple_store}
  end

  defp execute_clear_triple(ctx, graph_iri, silent) do
    clear_named_graph_triple(ctx, graph_iri, silent)
  end

  # CLEAR for quad stores
  defp execute_clear_quad(ctx, :all, silent) do
    case clear_all_graphs(ctx) do
      {:ok, count} -> {:ok, count}
      {:error, _} -> if silent, do: {:ok, 0}, else: {:error, :clear_failed}
    end
  end

  defp execute_clear_quad(ctx, :default, _silent) do
    clear_default_graph(ctx)
  end

  defp execute_clear_quad(ctx, :named, silent) do
    clear_all_named_graphs(ctx, silent)
  end

  defp execute_clear_quad(ctx, graph_iri, silent) do
    clear_named_graph(ctx, graph_iri, silent)
  end

  # Clear all graphs (default + named)
  defp clear_all_graphs(ctx) do
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} -> clear_all_graphs_quad(ctx)
      {:ok, false} -> clear_all_triples(ctx)
    end
  end

  # Clear all graphs for quad stores
  defp clear_all_graphs_quad(ctx) do
    case QuadOperations.list_graphs(ctx.db, include_default: true) do
      {:ok, []} ->
        {:ok, 0}

      {:ok, graphs} ->
        clear_quad_graphs(ctx, graphs, fn _total -> {:error, :clear_failed} end)

      {:error, _} ->
        clear_quad_graph(ctx, :default)
    end
  end

  # Clear default graph only
  defp clear_default_graph(ctx) do
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        clear_quad_graph(ctx, :default)

      {:ok, false} ->
        clear_default_triples(ctx)
    end
  end

  # Clear all named graphs (not default)
  defp clear_all_named_graphs(ctx, silent) do
    case QuadOperations.list_graphs(ctx.db, include_default: false) do
      {:ok, graphs} ->
        case Helpers.check_multi_graph_authorization(ctx, graphs, :write) do
          :ok ->
            clear_quad_graphs(ctx, graphs, &clear_named_graph_failure_result(&1, silent))

          {:error, :unauthorized} ->
            {:error, :unauthorized}
        end

      {:error, _} ->
        if silent, do: {:ok, 0}, else: {:error, :list_graphs_failed}
    end
  end

  # Clear a specific named graph
  defp clear_named_graph(ctx, graph_iri, silent) do
    rdf_graph = Helpers.ast_term_to_rdf_graph(graph_iri)

    case Helpers.check_write_authorization(ctx, rdf_graph) do
      :ok ->
        QuadOperations.clear_graph(ctx.db, ctx.dict_manager, rdf_graph)
        |> clear_named_graph_result(silent)

      {:error, :unauthorized} ->
        {:error, :unauthorized}
    end
  end

  # Legacy: clear all triples from the default graph
  defp clear_all_triples(ctx) do
    alias TripleStore.Index

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

  # Legacy: clear named graph for triple stores
  defp clear_named_graph_triple(_ctx, _graph_iri, _silent) do
    {:error, :named_graphs_not_supported_for_triple_store}
  end

  # Check if two graph terms are equal
  defp graphs_equal?(g1, g2) when g1 == g2, do: true
  defp graphs_equal?(:default, :default_graph), do: true
  defp graphs_equal?(:default_graph, :default), do: true
  defp graphs_equal?(g1, g2) when is_binary(g1) and is_binary(g2), do: g1 == g2
  defp graphs_equal?(%RDF.IRI{} = g1, %RDF.IRI{} = g2), do: g1.value == g2.value
  defp graphs_equal?(g1, g2) when is_binary(g1) and is_struct(g2), do: g1 == to_string(g2)
  defp graphs_equal?(g1, g2) when is_struct(g1) and is_binary(g2), do: to_string(g1) == g2
  defp graphs_equal?(_, _), do: false

  defp clear_quad_graphs(ctx, graphs, on_error) do
    graphs
    |> Enum.reduce_while({:ok, 0}, fn graph_term, {:ok, total} ->
      clear_graph_step(ctx, graph_term, total, on_error)
    end)
    |> invalidate_cache_on_success()
  end

  defp clear_graph_step(ctx, graph_term, total, on_error) do
    case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, graph_term) do
      {:ok, count} -> {:cont, {:ok, total + count}}
      {:error, _} -> {:halt, on_error.(total)}
    end
  end

  defp clear_quad_graph(ctx, graph_term) do
    QuadOperations.clear_graph(ctx.db, ctx.dict_manager, graph_term)
    |> invalidate_cache_on_success()
  end

  defp clear_default_triples(ctx) do
    {:ok, stream} = TripleStore.Index.lookup(ctx.db, {:var, :var, :var})

    stream
    |> Stream.chunk_every(@clear_batch_size)
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, count} ->
      delete_triple_chunk(ctx, chunk, count)
    end)
    |> invalidate_cache_when_counted()
  end

  defp delete_triple_chunk(ctx, chunk, count) do
    case TripleStore.Index.delete_triples(ctx.db, chunk) do
      :ok -> {:cont, {:ok, count + length(chunk)}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp invalidate_cache_on_success({:ok, count} = result) when count > 0 do
    Helpers.invalidate_cache_if_running()
    result
  end

  defp invalidate_cache_on_success(result), do: result

  defp invalidate_cache_when_counted({:ok, count} = result) when count > 0 do
    Helpers.invalidate_cache_if_running()
    result
  end

  defp invalidate_cache_when_counted(result), do: result

  defp clear_named_graph_result({:ok, count}, _silent) do
    Helpers.invalidate_cache_if_running()
    {:ok, count}
  end

  defp clear_named_graph_result({:error, :not_found}, true), do: {:ok, 0}
  defp clear_named_graph_result({:error, :not_found}, false), do: {:error, :graph_not_found}
  defp clear_named_graph_result({:error, _reason}, true), do: {:ok, 0}
  defp clear_named_graph_result({:error, reason}, false), do: {:error, reason}

  defp clear_named_graph_failure_result(total, true), do: {:ok, total}
  defp clear_named_graph_failure_result(_total, false), do: {:error, :clear_failed}
end
