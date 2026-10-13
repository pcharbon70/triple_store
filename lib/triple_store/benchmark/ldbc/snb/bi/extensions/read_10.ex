defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Read10 do
  @moduledoc """
  Exact BI 10 adapter over the mapped SNB social, place, message, and tag indices.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Support
  alias TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess

  @doc "Executes one BI 10 catalog variant."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def execute(definition, context, parameters, opts) do
    with {:ok, access} <- IndexAccess.new(context, opts),
         {:ok, person_id} <- Support.fetch_integer(parameters, "personId", min: 0),
         {:ok, country} <- Support.fetch_string(parameters, "country"),
         {:ok, tag_class} <- Support.fetch_string(parameters, "tagClass"),
         {:ok, minimum} <- Support.fetch_integer(parameters, "minPathDistance", min: 0),
         {:ok, maximum} <- Support.fetch_integer(parameters, "maxPathDistance", min: minimum),
         {:ok, start} <- Support.resolve_entity(access, "Person", person_id) do
      execute_from_start(definition, access, start, country, tag_class, minimum, maximum)
    end
  end

  defp execute_from_start(_definition, _access, :missing, _country, _tag_class, _min, _max),
    do: {:ok, []}

  defp execute_from_start(definition, access, start, country, tag_class, minimum, maximum) do
    with {:ok, traversal} <- Support.run_range(definition, access, start, minimum, maximum),
         {:ok, rows} <- candidate_rows(access, traversal.rows, country, tag_class),
         :ok <- IndexAccess.ensure_bound(access, rows) do
      Support.emit_evidence(definition, traversal, length(rows))
      {:ok, rows}
    end
  end

  defp candidate_rows(access, candidates, country, tag_class) do
    Enum.reduce_while(candidates, {:ok, []}, fn %{vertex: candidate}, {:ok, rows} ->
      with :ok <- IndexAccess.checkpoint(access),
           {:ok, true} <- located_in_country?(access, candidate, country),
           {:ok, candidate_rows} <- expert_rows(access, candidate, tag_class) do
        {:cont, {:ok, rows ++ candidate_rows}}
      else
        {:ok, false} -> {:cont, {:ok, rows}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp located_in_country?(access, person, country_name) do
    with {:ok, cities} <- IndexAccess.objects(access, person, "isLocatedIn") do
      Enum.reduce_while(cities, {:ok, false}, fn city, {:ok, false} ->
        case IndexAccess.objects(access, city, "isPartOf") do
          {:ok, countries} -> named_reduce_result(any_named?(access, countries, country_name))
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  defp expert_rows(access, candidate, tag_class) do
    with {:ok, messages} <- IndexAccess.subjects(access, "hasCreator", candidate),
         {:ok, person_id} <- IndexAccess.external_id(access, candidate) do
      Enum.reduce_while(messages, {:ok, %{}}, fn message, {:ok, counts} ->
        with {:ok, tags} <- IndexAccess.objects(access, message, "hasTag"),
             {:ok, qualifies?} <- has_tag_class?(access, tags, tag_class),
             {:ok, next} <- maybe_count_message(access, tags, message, qualifies?, counts) do
          {:cont, {:ok, next}}
        else
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, counts} ->
          {:ok,
           Enum.map(counts, fn {tag_name, message_ids} ->
             %{
               "expertCandidatePerson.id" => person_id,
               "tag.name" => tag_name,
               "messageCount" => MapSet.size(message_ids)
             }
           end)}

        {:error, _} = error ->
          error
      end
    end
  end

  defp has_tag_class?(access, tags, tag_class) do
    Enum.reduce_while(tags, {:ok, false}, fn tag, {:ok, false} ->
      case IndexAccess.objects(access, tag, "hasType") do
        {:ok, classes} -> named_reduce_result(any_named?(access, classes, tag_class))
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp maybe_count_message(_access, _tags, _message, false, counts), do: {:ok, counts}

  defp maybe_count_message(access, tags, message, true, counts) do
    Enum.reduce_while(tags, {:ok, counts}, fn tag, {:ok, acc} ->
      case IndexAccess.property_values(access, tag, "name") do
        {:ok, names} ->
          {:cont, {:ok, count_names(names, message, acc)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp any_named?(access, entities, expected) do
    Enum.reduce_while(entities, {:ok, false}, fn entity, {:ok, false} ->
      named_entity_result(IndexAccess.property_values(access, entity, "name"), expected)
    end)
  end

  defp named_entity_result({:ok, names}, expected) do
    if expected in names, do: {:halt, {:ok, true}}, else: {:cont, {:ok, false}}
  end

  defp named_entity_result({:error, _} = error, _expected), do: {:halt, error}

  defp named_reduce_result({:ok, true}), do: {:halt, {:ok, true}}
  defp named_reduce_result({:ok, false}), do: {:cont, {:ok, false}}
  defp named_reduce_result({:error, _} = error), do: {:halt, error}

  defp count_names(names, message, counts) do
    Enum.reduce(names, counts, fn name, current ->
      Map.update(current, name, MapSet.new([message]), &MapSet.put(&1, message))
    end)
  end
end
