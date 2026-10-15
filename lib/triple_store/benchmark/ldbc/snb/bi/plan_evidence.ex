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
      :actual_cardinality,
      :elapsed_us,
      :iterator_count,
      :materialized_rows,
      :memory_bytes,
      :io_bytes,
      :environment_identity,
      :cleanup_status,
      :protocol_eligible?
    ]

    missing = Enum.reject(required, &Map.has_key?(measurements, &1))
    reference_digest = digest(reference_rows)
    candidate_digest = digest(candidate_rows)

    cond do
      missing != [] ->
        {:error, {:missing_plan_measurements, missing}}

      reference_digest != candidate_digest ->
        {:error, :answer_changed}

      not valid_measurements?(measurements) ->
        {:error, :invalid_plan_measurements}

      true ->
        {:ok,
         Map.merge(measurements, %{operation_id: operation_id, answer_digest: reference_digest})}
    end
  end

  @doc "Ranks qualified cost centers without mixing environment identities."
  @spec rank([map()]) :: {:ok, [map()]} | {:error, term()}
  def rank([]), do: {:error, :empty_plan_evidence}

  def rank(records) when is_list(records) do
    environments = records |> Enum.map(& &1[:environment_identity]) |> Enum.uniq()

    cond do
      length(environments) != 1 ->
        {:error, :environment_identity_mismatch}

      not Enum.all?(records, &qualified_evidence?/1) ->
        {:error, :unqualified_plan_evidence}

      true ->
        ranked =
          records
          |> Enum.sort_by(
            &{-&1.elapsed_us, -&1.iterator_count, -&1.materialized_rows, -&1.memory_bytes,
             -&1.io_bytes, &1.operation_id}
          )
          |> Enum.with_index(1)
          |> Enum.map(fn {record, rank} ->
            record
            |> Map.put(:rank, rank)
            |> Map.put(
              :cardinality_error_ratio,
              cardinality_error_ratio(record.estimated_cardinality, record.actual_cardinality)
            )
          end)

        {:ok, ranked}
    end
  end

  @doc "Accepts a measured improvement only when answers and execution gates remain intact."
  @spec accept_improvement(map(), map()) :: {:ok, map()} | {:error, term()}
  def accept_improvement(baseline, candidate) when is_map(baseline) and is_map(candidate) do
    with :ok <- matching_value(baseline, candidate, :operation_id, :operation_identity_mismatch),
         :ok <-
           matching_value(
             baseline,
             candidate,
             :environment_identity,
             :environment_identity_mismatch
           ),
         :ok <- matching_value(baseline, candidate, :answer_digest, :answer_changed),
         :ok <-
           required_value(candidate, :cleanup_status, :complete, :resource_cleanup_incomplete),
         :ok <- required_value(candidate, :protocol_eligible?, true, :protocol_ineligible),
         :ok <- improved_latency(baseline, candidate) do
      {:ok,
       Map.merge(candidate, %{
         baseline_elapsed_us: baseline.elapsed_us,
         elapsed_improvement_us: baseline.elapsed_us - candidate.elapsed_us
       })}
    end
  end

  def accept_improvement(_baseline, _candidate), do: {:error, :invalid_plan_evidence}

  defp matching_value(left, right, key, error) do
    if left[key] == right[key], do: :ok, else: {:error, error}
  end

  defp required_value(value, key, expected, error) do
    if value[key] == expected, do: :ok, else: {:error, error}
  end

  defp improved_latency(baseline, candidate) do
    if is_number(candidate[:elapsed_us]) and candidate.elapsed_us < baseline.elapsed_us,
      do: :ok,
      else: {:error, :latency_not_improved}
  end

  defp valid_measurements?(measurements) do
    Enum.all?(
      [
        :estimated_cardinality,
        :actual_cardinality,
        :elapsed_us,
        :iterator_count,
        :materialized_rows,
        :memory_bytes,
        :io_bytes
      ],
      fn field -> is_number(measurements[field]) and measurements[field] >= 0 end
    ) and is_binary(measurements.environment_identity) and
      measurements.cleanup_status in [:complete, :incomplete] and
      is_boolean(measurements.protocol_eligible?)
  end

  defp qualified_evidence?(record) do
    is_binary(record[:answer_digest]) and record[:cleanup_status] == :complete and
      record[:protocol_eligible?] == true and is_number(record[:elapsed_us])
  end

  defp cardinality_error_ratio(0, 0), do: 1.0
  defp cardinality_error_ratio(_estimated, 0), do: :infinity

  defp cardinality_error_ratio(estimated, actual) do
    max(estimated / actual, actual / max(estimated, 1))
  end

  defp digest(rows) do
    rows
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
