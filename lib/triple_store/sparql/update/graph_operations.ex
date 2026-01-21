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

    cond do
      is_nil(graph_iri) ->
        {:error, :missing_graph_iri}

      graph_iri == :default or graph_iri == :default_graph ->
        if silent, do: {:ok, 0}, else: {:error, :default_graph_exists}

      true ->
        rdf_graph = Helpers.ast_term_to_rdf_graph(graph_iri)

        case Helpers.check_admin_authorization(ctx, rdf_graph) do
          :ok ->
            case QuadOperations.create_graph(ctx.db, ctx.dict_manager, rdf_graph) do
              {:ok, :created} ->
                Helpers.invalidate_cache_if_running()
                {:ok, 0}

              {:ok, :already_exists} ->
                if silent, do: {:ok, 0}, else: {:error, :graph_already_exists}

              {:error, reason} ->
                if silent, do: {:ok, 0}, else: {:error, reason}
            end

          {:error, :unauthorized} ->
            {:error, :unauthorized}
        end
    end
  end

  @doc """
  Executes a DROP GRAPH operation.
  """
  @spec execute_drop_graph(map(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def execute_drop_graph(ctx, props) do
    graph_iri = Helpers.get_prop(props, "graph")
    silent = Helpers.get_prop(props, "silent", false)

    cond do
      is_nil(graph_iri) ->
        {:error, :missing_graph_iri}

      graph_iri == :default or graph_iri == :default_graph ->
        if silent, do: {:ok, 0}, else: {:error, :cannot_drop_default}

      true ->
        rdf_graph = Helpers.ast_term_to_rdf_graph(graph_iri)

        case Helpers.check_admin_authorization(ctx, rdf_graph) do
          :ok ->
            case QuadOperations.delete_graph(ctx.db, ctx.dict_manager, rdf_graph) do
              {:ok, count} ->
                Helpers.invalidate_cache_if_running()
                {:ok, count}

              {:error, :not_found} ->
                if silent, do: {:ok, 0}, else: {:error, :graph_not_found}

              {:error, reason} ->
                if silent, do: {:ok, 0}, else: {:error, reason}
            end

          {:error, :unauthorized} ->
            {:error, :unauthorized}
        end
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

    source_rdf = Helpers.normalize_graph_term(source_graph)
    target_rdf = Helpers.normalize_graph_term(target_graph)

    with :ok <- Helpers.check_read_authorization(ctx, source_rdf),
         :ok <- Helpers.check_write_authorization(ctx, target_rdf) do
      source_exists? =
        case source_rdf do
          :default -> QuadOperations.default_graph_exists?(ctx.db)
          graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
        end

      if !source_exists? do
        # Copy from non-existent source returns 0 quads (no-op)
        {:ok, 0}
      else
        case QuadOperations.copy_graph(
               ctx.db,
               ctx.dict_manager,
               source_rdf,
               target_rdf,
               on_conflict: :replace
             ) do
          {:ok, count} ->
            Helpers.invalidate_cache_if_running()
            {:ok, count}

          {:error, reason} ->
            if silent, do: {:ok, 0}, else: {:error, reason}
        end
      end
    else
      {:error, :unauthorized} -> {:error, :unauthorized}
    end
  end

  @doc """
  Executes a MOVE GRAPH operation.
  """
  @spec execute_move(map(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_move(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    source_rdf = Helpers.normalize_graph_term(source_graph)
    target_rdf = Helpers.normalize_graph_term(target_graph)

    # Check if source equals target
    if graphs_equal?(source_rdf, target_rdf) do
      # Source equals target returns ok with 0 count (no-op)
      {:ok, 0}
    else
      with :ok <- Helpers.check_admin_authorization(ctx, source_rdf),
           :ok <- Helpers.check_admin_authorization(ctx, target_rdf) do
        source_exists? =
          case source_rdf do
            :default -> QuadOperations.default_graph_exists?(ctx.db)
            graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
          end

        # Moving from non-existent source is OK - returns 0 quads moved
        if !source_exists? do
          {:ok, 0}
        else
          case QuadOperations.move_quads(
                 ctx.db,
                 ctx.dict_manager,
                 source_rdf,
                 target_rdf,
                 on_conflict: :replace
               ) do
            {:ok, count} ->
              Helpers.invalidate_cache_if_running()
              {:ok, count}

            {:error, reason} ->
              if silent, do: {:ok, 0}, else: {:error, reason}
          end
        end
      else
        {:error, :unauthorized} -> {:error, :unauthorized}
      end
    end
  end

  @doc """
  Executes an ADD GRAPH operation.
  """
  @spec execute_add(map(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_add(ctx, source_graph, target_graph, opts \\ []) do
    silent = Keyword.get(opts, :silent, false)

    source_rdf = Helpers.normalize_graph_term(source_graph)
    target_rdf = Helpers.normalize_graph_term(target_graph)

    # Check if source equals target
    if graphs_equal?(source_rdf, target_rdf) do
      # Source equals target returns ok with 0 count (no-op)
      {:ok, 0}
    else
      with :ok <- Helpers.check_read_authorization(ctx, source_rdf),
           :ok <- Helpers.check_write_authorization(ctx, target_rdf) do
        source_exists? =
          case source_rdf do
            :default -> QuadOperations.default_graph_exists?(ctx.db)
            graph -> QuadOperations.graph_exists?(ctx.db, ctx.dict_manager, graph)
          end

        # Adding from non-existent source is OK - returns 0 quads added
        if !source_exists? do
          {:ok, 0}
        else
          case QuadOperations.copy_graph(
                 ctx.db,
                 ctx.dict_manager,
                 source_rdf,
                 target_rdf,
                 on_conflict: :merge
               ) do
            {:ok, count} ->
              Helpers.invalidate_cache_if_running()
              {:ok, count}

            {:error, reason} ->
              if silent, do: {:ok, 0}, else: {:error, reason}
          end
        end
      else
        {:error, :unauthorized} -> {:error, :unauthorized}
      end
    end
  end

  # ===========================================================================
  # Private Helpers - CLEAR Operations
  # ===========================================================================

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
        result =
          Enum.reduce_while(graphs, {:ok, 0}, fn graph_term, {:ok, total} ->
            case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, graph_term) do
              {:ok, count} -> {:cont, {:ok, total + count}}
              {:error, _} -> {:halt, {:error, :clear_failed}}
            end
          end)

        case result do
          {:ok, count} ->
            Helpers.invalidate_cache_if_running()
            {:ok, count}

          {:error, _} = error ->
            error
        end

      {:error, _} ->
        case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, :default) do
          {:ok, count} ->
            Helpers.invalidate_cache_if_running()
            {:ok, count}

          {:error, _} = error ->
            error
        end
    end
  end

  # Clear default graph only
  defp clear_default_graph(ctx) do
    case ErlangAdapter.is_quad_store?(ctx.db) do
      {:ok, true} ->
        # Quad store - use QuadOperations
        case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, :default) do
          {:ok, count} ->
            Helpers.invalidate_cache_if_running()
            {:ok, count}

          {:error, _} = error ->
            error
        end

      {:ok, false} ->
        # Triple store - use Index operations
        alias TripleStore.Index

        {:ok, stream} = Index.lookup(ctx.db, {:var, :var, :var})

        result =
          stream
          |> Stream.chunk_every(@clear_batch_size)
          |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, count} ->
            case Index.delete_triples(ctx.db, chunk) do
              :ok -> {:cont, {:ok, count + length(chunk)}}
              {:error, _} = error -> {:halt, error}
            end
          end)

        case result do
          {:ok, count} when count > 0 -> Helpers.invalidate_cache_if_running()
          _ -> :ok
        end

        result
    end
  end

  # Clear all named graphs (not default)
  defp clear_all_named_graphs(ctx, silent) do
    case QuadOperations.list_graphs(ctx.db, include_default: false) do
      {:ok, graphs} ->
        case Helpers.check_multi_graph_authorization(ctx, graphs, :write) do
          :ok ->
            result =
              Enum.reduce_while(graphs, {:ok, 0}, fn graph_iri, {:ok, total} ->
                case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, graph_iri) do
                  {:ok, count} ->
                    {:cont, {:ok, total + count}}

                  {:error, _} ->
                    {:halt, if(silent, do: {:ok, total}, else: {:error, :clear_failed})}
                end
              end)

            case result do
              {:ok, count} when count > 0 -> Helpers.invalidate_cache_if_running()
              _ -> :ok
            end

            result

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
        case QuadOperations.clear_graph(ctx.db, ctx.dict_manager, rdf_graph) do
          {:ok, count} ->
            Helpers.invalidate_cache_if_running()
            {:ok, count}

          {:error, :not_found} ->
            if silent, do: {:ok, 0}, else: {:error, :graph_not_found}

          {:error, reason} ->
            if silent, do: {:ok, 0}, else: {:error, reason}
        end

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
end
