defmodule TripleStore.Benchmark.LDBC.SNB.UpdateStream do
  @moduledoc """
  Safe line-oriented encoding for ordered SNB update events.

  Every line is Base64 over a deterministic Erlang external term. Decoding uses
  the safe option, and sequence validation rejects reordered or duplicated events.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.UpdateSchema

  @doc "Writes ordered update records to a temporary file and promotes it."
  @spec write(Path.t(), Enumerable.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def write(path, records) do
    temporary = path <> ".partial"
    records = Enum.to_list(records)

    with :ok <- validate_records(records),
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, io} <- File.open(temporary, [:write, :binary, :exclusive]) do
      result =
        try do
          records
          |> Enum.reduce_while({:ok, 0, -1}, fn record, {:ok, count, previous} ->
            with true <- record.sequence > previous,
                 binary <- :erlang.term_to_binary(record, [:deterministic]),
                 :ok <- IO.binwrite(io, Base.encode64(binary) <> "\n") do
              {:cont, {:ok, count + 1, record.sequence}}
            else
              false -> {:halt, {:error, :invalid_update_record}}
              {:error, _} = error -> {:halt, error}
            end
          end)
        after
          File.close(io)
        end

      case result do
        {:ok, count, _sequence} ->
          with :ok <- File.rename(temporary, path), do: {:ok, count}

        {:error, _} = error ->
          File.rm(temporary)
          error
      end
    end
  end

  @doc "Returns a lazy stream of safely decoded update records."
  @spec stream(Path.t()) :: Enumerable.t()
  def stream(path) do
    path
    |> File.stream!(:line)
    |> Stream.map(&decode_line/1)
  end

  defp validate_records(records) do
    if Enum.all?(records, &match?(%{type: _, fields: _}, &1)) do
      UpdateSchema.validate_records(records)
    else
      validate_legacy_records(records)
    end
  end

  defp validate_legacy_records(records) do
    records
    |> Enum.reduce_while(-1, fn
      %{sequence: sequence, operation: operation, quads: quads}, previous
      when is_integer(sequence) and sequence > previous and operation in [:insert, :delete] and
             is_list(quads) ->
        {:cont, sequence}

      %{error: reason}, _previous ->
        {:halt, {:error, reason}}

      _record, _previous ->
        {:halt, {:error, :invalid_update_record}}
    end)
    |> case do
      {:error, _} = error -> error
      _sequence -> :ok
    end
  end

  defp decode_line(line) do
    case Base.decode64(String.trim(line)) do
      {:ok, binary} -> :erlang.binary_to_term(binary, [:safe])
      :error -> {:error, :invalid_update_encoding}
    end
  rescue
    ArgumentError -> {:error, :invalid_update_encoding}
  end
end
