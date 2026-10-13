defmodule TripleStore.Benchmark.LDBC.SNB.BI.PlanEvidence do
  @moduledoc """
  Stable per-operation evidence for BI optimization work.

  It keeps reference and candidate answer digests beside plan, cardinality,
  iterator, materialization, memory, and I/O measurements. A candidate can only
  be accepted when its complete answer digest matches the reference digest.
  """

  @doc "Builds and validates one optimization evidence record."
  @spec record(String.t(), term(), term(), map()) :: {:ok, map()} | {:error, term()}
  def record(operation_id, reference_rows, candidate_rows, measurements)
      when is_map(measurements) do
    required = [
      :plan,
      :estimated_cardinality,
      :iterator_count,
      :materialized_rows,
      :memory_bytes,
      :io_bytes
    ]

    missing = Enum.reject(required, &Map.has_key?(measurements, &1))
    reference_digest = digest(reference_rows)
    candidate_digest = digest(candidate_rows)

    cond do
      missing != [] ->
        {:error, {:missing_plan_measurements, missing}}

      reference_digest != candidate_digest ->
        {:error, :answer_changed}

      true ->
        {:ok,
         Map.merge(measurements, %{operation_id: operation_id, answer_digest: reference_digest})}
    end
  end

  defp digest(rows) do
    rows
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
