defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Read20 do
  @moduledoc """
  Exact BI 20 recruitment-path adapter over mapped employment and study indices.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Support
  alias TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess

  @doc "Executes one BI 20 catalog variant."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def execute(definition, context, parameters, opts) do
    with {:ok, access} <- IndexAccess.new(context, opts),
         {:ok, company_name} <- Support.fetch_string(parameters, "company"),
         {:ok, person2_id} <- Support.fetch_integer(parameters, "person2Id", min: 0),
         {:ok, companies} <-
           IndexAccess.entities_by_name(access, "Organisation", company_name, "Company"),
         {:ok, sources} <- employees(access, companies),
         {:ok, person2} <- Support.resolve_entity(access, "Person", person2_id) do
      execute_to_target(definition, access, Enum.reject(sources, &(&1 == person2)), person2)
    end
  end

  defp execute_to_target(_definition, _access, _sources, :missing), do: {:ok, []}

  defp execute_to_target(definition, access, sources, target) do
    with {:ok, traversal} <-
           Support.run_weighted(
             definition,
             access,
             sources,
             [target],
             &edge_weight(access, &1, &2)
           ),
         {:ok, rows} <- external_rows(access, traversal.rows) do
      Support.emit_evidence(definition, traversal, length(rows))
      {:ok, rows}
    end
  end

  defp employees(access, companies) do
    Enum.reduce_while(companies, {:ok, []}, fn company, {:ok, people} ->
      employee_result(IndexAccess.subjects(access, "workAt", company), access, people)
    end)
    |> case do
      {:ok, people} ->
        people = Enum.uniq(people)

        case IndexAccess.ensure_bound(access, people) do
          :ok -> {:ok, people}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  defp employee_result({:ok, found}, access, people) do
    case IndexAccess.entities_of_type(access, found, "Person") do
      {:ok, people_found} -> {:cont, {:ok, people ++ people_found}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp employee_result({:error, _} = error, _access, _people), do: {:halt, error}

  defp edge_weight(access, left, right) do
    with {:ok, left_studies} <- studies(access, left),
         {:ok, right_studies} <- studies(access, right) do
      weights =
        for {university, left_year} <- left_studies,
            {^university, right_year} <- right_studies,
            do: abs(left_year - right_year) + 1

      case weights do
        [] -> {:ok, nil}
        values -> {:ok, Enum.min(values)}
      end
    end
  end

  defp studies(access, person) do
    with {:ok, universities} <- IndexAccess.objects(access, person, "studyAt") do
      Enum.reduce_while(universities, {:ok, []}, fn university, {:ok, studies} ->
        study_result(
          IndexAccess.relationship_property(access, person, "studyAt", university, "classYear"),
          person,
          university,
          studies
        )
      end)
    end
  end

  defp study_result({:ok, []}, person, university, _studies),
    do: {:halt, {:error, {:missing_edge_weight, :study_at, person, university}}}

  defp study_result({:ok, years}, _person, university, studies) do
    if Enum.all?(years, &is_integer/1),
      do: {:cont, {:ok, studies ++ Enum.map(years, &{university, &1})}},
      else: {:halt, {:error, {:malformed_edge_weight, :study_at, years}}}
  end

  defp study_result({:error, _} = error, _person, _university, _studies), do: {:halt, error}

  defp external_rows(access, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, converted} ->
      case IndexAccess.external_id(access, row.source) do
        {:ok, source} ->
          result = %{"person1.id" => source, "totalWeight" => row.cost}
          {:cont, {:ok, [result | converted]}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, converted} -> {:ok, Enum.reverse(converted)}
      {:error, _} = error -> error
    end
  end
end
