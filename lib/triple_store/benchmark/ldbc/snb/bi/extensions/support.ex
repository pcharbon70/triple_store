defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Support do
  @moduledoc """
  Shared parameter, traversal, and telemetry boundary for BI graph adapters.

  The helpers preserve typed parameter failures, pass protocol bounds into the
  graph algorithms, and attach the canonical operation identity to evidence.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.{GraphAlgorithms, IndexAccess}

  @telemetry_prefix [:triple_store, :benchmark, :ldbc, :snb_bi, :graph_operation]

  @spec fetch_integer(map(), String.t(), keyword()) :: {:ok, integer()} | {:error, term()}
  def fetch_integer(parameters, name, opts \\ []) do
    case Map.fetch(parameters, name) do
      {:ok, value} when is_integer(value) -> validate_integer(value, name, opts)
      {:ok, value} -> {:error, {:invalid_bi_parameter, name, value}}
      :error -> {:error, {:missing_bi_parameter, name}}
    end
  end

  @spec fetch_string(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def fetch_string(parameters, name) do
    case Map.fetch(parameters, name) do
      {:ok, value} when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_bi_parameter, name, value}}
      :error -> {:error, {:missing_bi_parameter, name}}
    end
  end

  @spec fetch_date(map(), String.t()) :: {:ok, Date.t()} | {:error, term()}
  def fetch_date(parameters, name) do
    case Map.fetch(parameters, name) do
      {:ok, %Date{} = date} -> {:ok, date}
      {:ok, value} when is_binary(value) -> parse_date(value, name)
      {:ok, value} -> {:error, {:invalid_bi_parameter, name, value}}
      :error -> {:error, {:missing_bi_parameter, name}}
    end
  end

  @spec resolve_entity(IndexAccess.t(), String.t(), integer()) ::
          {:ok, non_neg_integer() | :missing} | {:error, term()}
  def resolve_entity(access, type, id) do
    case IndexAccess.entity(access, type, id) do
      {:ok, internal} -> {:ok, internal}
      :not_found -> {:ok, :missing}
      {:error, _} = error -> error
    end
  end

  @spec unweighted_provider(IndexAccess.t()) :: (term() -> {:ok, list()} | {:error, term()})
  def unweighted_provider(access) do
    fn vertex ->
      with {:ok, neighbours} <- IndexAccess.neighbours(access, vertex, "knows") do
        {:ok, Enum.map(neighbours, &{&1, 1})}
      end
    end
  end

  @spec weighted_provider(IndexAccess.t(), (term(), term() ->
                                              {:ok, number() | nil} | {:error, term()})) ::
          (term() -> {:ok, list()} | {:error, term()})
  def weighted_provider(access, weight) do
    fn vertex ->
      with {:ok, neighbours} <- IndexAccess.neighbours(access, vertex, "knows") do
        weighted_edges(neighbours, vertex, weight)
      end
    end
  end

  defp weighted_edges(neighbours, vertex, weight) do
    Enum.reduce_while(neighbours, {:ok, []}, fn neighbour, {:ok, edges} ->
      weighted_edge_result(weight.(vertex, neighbour), neighbour, edges)
    end)
    |> case do
      {:ok, edges} -> {:ok, Enum.reverse(edges)}
      {:error, _} = error -> error
    end
  end

  defp weighted_edge_result({:ok, nil}, _neighbour, edges), do: {:cont, {:ok, edges}}

  defp weighted_edge_result({:ok, edge_weight}, neighbour, edges),
    do: {:cont, {:ok, [{neighbour, edge_weight} | edges]}}

  defp weighted_edge_result({:error, _} = error, _neighbour, _edges), do: {:halt, error}

  @spec run_range(map(), IndexAccess.t(), term(), integer(), integer()) ::
          {:ok, map()} | {:error, term()}
  def run_range(definition, access, start, minimum, maximum) do
    GraphAlgorithms.shortest_path_range(
      start,
      minimum,
      maximum,
      IndexAccess.traversal_options(access, definition.id, unweighted_provider(access))
    )
  end

  @spec run_weighted(map(), IndexAccess.t(), [term()], [term()], function()) ::
          {:ok, map()} | {:error, term()}
  def run_weighted(definition, access, sources, targets, weight) do
    GraphAlgorithms.cheapest_pairs(
      sources,
      targets,
      IndexAccess.traversal_options(access, definition.id, weighted_provider(access, weight))
    )
  end

  @spec emit_evidence(map(), map(), non_neg_integer()) :: :ok
  def emit_evidence(definition, traversal, row_count) do
    :telemetry.execute(
      @telemetry_prefix ++ [:stop],
      Map.merge(traversal.metrics, %{row_count: row_count}),
      %{operation: definition.id, explain: traversal.explain}
    )
  end

  defp validate_integer(value, name, opts) do
    minimum = Keyword.get(opts, :min)

    if is_integer(minimum) and value < minimum do
      {:error, {:invalid_bi_parameter, name, value}}
    else
      {:ok, value}
    end
  end

  defp parse_date(value, name) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _reason} -> {:error, {:invalid_bi_parameter, name, value}}
    end
  end
end
