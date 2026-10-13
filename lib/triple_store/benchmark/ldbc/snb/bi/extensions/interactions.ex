defmodule TripleStore.Benchmark.LDBC.SNB.BI.Extensions.Interactions do
  @moduledoc """
  Lazy mapped-RDF interaction lookups shared by weighted BI graph adapters.

  Reply pairs and post ancestry are discovered per expanded vertex through
  bounded index access, without extracting the message graph into the driver.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess

  @spec between(IndexAccess.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def between(access, left_person, right_person) do
    with {:ok, messages} <- IndexAccess.subjects(access, "hasCreator", left_person) do
      Enum.reduce_while(messages, {:ok, []}, fn message, {:ok, interactions} ->
        with :ok <- IndexAccess.checkpoint(access),
             {:ok, neighbours} <- IndexAccess.neighbours(access, message, "replyOf"),
             {:ok, matching} <- authored_by(access, neighbours, right_person),
             next <- interactions ++ Enum.map(matching, &{message, &1}),
             :ok <- IndexAccess.ensure_bound(access, next) do
          {:cont, {:ok, next}}
        else
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  @spec posts(IndexAccess.t(), non_neg_integer()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def posts(access, message) do
    posts(access, [message], MapSet.new(), [])
  end

  defp posts(_access, [], _visited, found), do: {:ok, Enum.uniq(found)}

  defp posts(access, [message | pending], visited, found) do
    cond do
      MapSet.member?(visited, message) ->
        posts(access, pending, visited, found)

      MapSet.size(visited) >= access.max_intermediate ->
        {:error, {:intermediate_bound_exceeded, access.max_intermediate}}

      true ->
        with :ok <- IndexAccess.checkpoint(access),
             {:ok, post?} <- IndexAccess.entity_type(access, message, "Post") do
          if post? do
            posts(access, pending, MapSet.put(visited, message), [message | found])
          else
            with {:ok, parents} <- IndexAccess.objects(access, message, "replyOf") do
              posts(access, pending ++ parents, MapSet.put(visited, message), found)
            end
          end
        end
    end
  end

  defp authored_by(access, messages, person) do
    Enum.reduce_while(messages, {:ok, []}, fn message, {:ok, matching} ->
      authored_message_result(
        IndexAccess.objects(access, message, "hasCreator"),
        message,
        matching,
        person
      )
    end)
    |> case do
      {:ok, matching} -> {:ok, Enum.reverse(matching)}
      {:error, _} = error -> error
    end
  end

  defp authored_message_result({:ok, creators}, message, matching, person) do
    if person in creators,
      do: {:cont, {:ok, [message | matching]}},
      else: {:cont, {:ok, matching}}
  end

  defp authored_message_result({:error, _} = error, _message, _matching, _person),
    do: {:halt, error}
end
