defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Read19 do
  @moduledoc """
  Exact BI 19 interaction-path adapter over mapped city and reply indices.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Extensions.{Interactions, Support}
  alias TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess

  @doc "Executes one BI 19 catalog variant."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def execute(definition, context, parameters, opts) do
    with {:ok, access} <- IndexAccess.new(context, opts),
         {:ok, city1_id} <- Support.fetch_integer(parameters, "city1Id", min: 0),
         {:ok, city2_id} <- Support.fetch_integer(parameters, "city2Id", min: 0),
         {:ok, city1} <- Support.resolve_entity(access, "Place", city1_id),
         {:ok, city2} <- Support.resolve_entity(access, "Place", city2_id),
         {:ok, sources} <- residents(access, city1, city1_id),
         {:ok, targets} <- residents(access, city2, city2_id),
         {:ok, traversal} <-
           Support.run_weighted(
             definition,
             access,
             sources,
             targets,
             &edge_weight(access, &1, &2)
           ),
         {:ok, rows} <- external_rows(access, traversal.rows) do
      Support.emit_evidence(definition, traversal, length(rows))
      {:ok, rows}
    end
  end

  defp residents(_access, :missing, _external_id), do: {:ok, []}

  defp residents(access, city, external_id) do
    with {:ok, true} <- IndexAccess.subtype?(access, city, "City"),
         {:ok, residents} <- IndexAccess.subjects(access, "isLocatedIn", city),
         {:ok, people} <- IndexAccess.entities_of_type(access, residents, "Person") do
      {:ok, people}
    else
      {:ok, false} -> {:error, {:invalid_city_reference, external_id}}
      {:error, _} = error -> error
    end
  end

  defp edge_weight(access, left, right) do
    with {:ok, interactions} <- Interactions.between(access, left, right) do
      case length(interactions) do
        0 -> {:ok, nil}
        count -> {:ok, max(round(40 - :math.sqrt(count)), 1)}
      end
    end
  end

  defp external_rows(access, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, converted} ->
      with {:ok, source} <- IndexAccess.external_id(access, row.source),
           {:ok, target} <- IndexAccess.external_id(access, row.target) do
        result = %{
          "person1.id" => source,
          "person2.id" => target,
          "totalWeight" => row.cost
        }

        {:cont, {:ok, [result | converted]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, converted} -> {:ok, Enum.reverse(converted)}
      {:error, _} = error -> error
    end
  end
end
