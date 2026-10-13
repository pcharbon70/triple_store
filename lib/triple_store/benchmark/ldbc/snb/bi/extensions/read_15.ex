defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Read15 do
  @moduledoc """
  Exact BI 15 weighted trusted-path adapter over mapped SNB forum interactions.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Extensions.{Interactions, Support}
  alias TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess

  @doc "Executes one BI 15 catalog variant."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def execute(definition, context, parameters, opts) do
    with {:ok, access} <- IndexAccess.new(context, opts),
         {:ok, person1_id} <- Support.fetch_integer(parameters, "person1Id", min: 0),
         {:ok, person2_id} <- Support.fetch_integer(parameters, "person2Id", min: 0),
         {:ok, start_date} <- Support.fetch_date(parameters, "startDate"),
         {:ok, end_date} <- Support.fetch_date(parameters, "endDate"),
         :ok <- validate_range(start_date, end_date),
         {:ok, person1} <- Support.resolve_entity(access, "Person", person1_id),
         {:ok, person2} <- Support.resolve_entity(access, "Person", person2_id) do
      execute_between(definition, access, person1, person2, start_date, end_date)
    end
  end

  defp execute_between(_definition, _access, :missing, _person2, _start, _end),
    do: {:ok, [%{"weight" => -1.0}]}

  defp execute_between(_definition, _access, _person1, :missing, _start, _end),
    do: {:ok, [%{"weight" => -1.0}]}

  defp execute_between(definition, access, person1, person2, start_date, end_date) do
    weight = &edge_weight(access, &1, &2, start_date, end_date)

    with {:ok, traversal} <-
           Support.run_weighted(definition, access, [person1], [person2], weight) do
      rows =
        case traversal.rows do
          [] -> [%{"weight" => -1.0}]
          [%{cost: cost}] -> [%{"weight" => cost}]
        end

      Support.emit_evidence(definition, traversal, length(rows))
      {:ok, rows}
    end
  end

  defp edge_weight(access, left, right, start_date, end_date) do
    with {:ok, interactions} <- Interactions.between(access, left, right),
         {:ok, interaction_weight} <-
           sum_interactions(access, interactions, start_date, end_date) do
      {:ok, 1.0 / (interaction_weight + 1.0)}
    end
  end

  defp sum_interactions(access, interactions, start_date, end_date) do
    Enum.reduce_while(interactions, {:ok, 0.0}, fn {left_message, right_message}, {:ok, sum} ->
      accumulate_interaction(access, left_message, right_message, start_date, end_date, sum)
    end)
  end

  defp accumulate_interaction(access, left_message, right_message, start_date, end_date, sum) do
    with {:ok, in_range?} <- message_in_forum_range?(access, left_message, start_date, end_date),
         {:ok, left_post?} <- IndexAccess.entity_type(access, left_message, "Post"),
         {:ok, right_post?} <- IndexAccess.entity_type(access, right_message, "Post") do
      {:cont, {:ok, sum + interaction_contribution(in_range?, left_post?, right_post?)}}
    else
      {:error, _} = error -> {:halt, error}
    end
  end

  defp interaction_contribution(false, _left_post?, _right_post?), do: 0.0
  defp interaction_contribution(true, true, _right_post?), do: 1.0
  defp interaction_contribution(true, _left_post?, true), do: 1.0
  defp interaction_contribution(true, false, false), do: 0.5

  defp message_in_forum_range?(access, message, start_date, end_date) do
    with {:ok, posts} <- Interactions.posts(access, message) do
      Enum.reduce_while(posts, {:ok, false}, fn post, {:ok, false} ->
        with {:ok, forums} <- IndexAccess.subjects(access, "containerOf", post),
             {:ok, dates} <- forum_dates(access, forums) do
          if Enum.any?(dates, &date_in_range?(&1, start_date, end_date)) do
            {:halt, {:ok, true}}
          else
            {:cont, {:ok, false}}
          end
        else
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  defp forum_dates(access, forums) do
    Enum.reduce_while(forums, {:ok, []}, fn forum, {:ok, dates} ->
      case IndexAccess.property_values(access, forum, "creationDate") do
        {:ok, found} -> {:cont, {:ok, dates ++ found}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp date_in_range?(%DateTime{} = value, start_date, end_date),
    do: date_in_range?(DateTime.to_date(value), start_date, end_date)

  defp date_in_range?(%Date{} = value, start_date, end_date),
    do: Date.compare(value, start_date) != :lt and Date.compare(value, end_date) != :gt

  defp date_in_range?(_value, _start_date, _end_date), do: false

  defp validate_range(start_date, end_date) do
    if Date.compare(start_date, end_date) == :gt,
      do: {:error, {:invalid_bi_date_range, start_date, end_date}},
      else: :ok
  end
end
