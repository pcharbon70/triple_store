defmodule TripleStore.Benchmark.LDBC.Measurement do
  @moduledoc """
  Aggregates LDBC execution records without admitting invalid samples.

  Official-style score eligibility is false when any measured operation fails,
  times out, is cancelled, or produces an incorrect result.
  """

  @doc "Builds per-operation distributions and the enclosing score gate."
  @spec summarize([map()]) :: map()
  def summarize(records) when is_list(records) do
    measured = Enum.filter(records, &(&1.mode == :measured))
    warmup = Enum.filter(records, &(&1.mode == :warmup))
    valid = Enum.filter(measured, & &1.score_eligible?)
    invalid = Enum.reject(measured, & &1.score_eligible?)

    %{
      measured_count: length(measured),
      warmup_count: length(warmup),
      valid_sample_count: length(valid),
      invalid_sample_count: length(invalid),
      score_eligible?: measured != [] and invalid == [],
      operations: distributions(valid),
      failures: Enum.map(invalid, &Map.take(&1, [:operation_id, :status, :result]))
    }
  end

  defp distributions(records) do
    records
    |> Enum.group_by(& &1.operation_id, & &1.sample_us)
    |> Map.new(fn {id, samples} ->
      sorted = Enum.sort(samples)

      {id,
       %{
         count: length(sorted),
         min_us: hd(sorted),
         max_us: List.last(sorted),
         p50_us: percentile(sorted, 0.50),
         p95_us: percentile(sorted, 0.95),
         p99_us: percentile(sorted, 0.99)
       }}
    end)
  end

  defp percentile(samples, fraction) do
    index = max(ceil(length(samples) * fraction) - 1, 0)
    Enum.at(samples, index)
  end
end
