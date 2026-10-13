defmodule TripleStore.Benchmark.LDBC.SNB.BI.Scoring do
  @moduledoc """
  Local equivalent of the pinned SNB BI v1.0.3 scoring formulas.

  The scorer is fail closed. It emits no score unless answers, update batches,
  operation completeness, schedule, duration, and load-time inputs are valid.
  Smoke protocols use a diagnostic namespace even when their arithmetic is the
  same as the upstream tool.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Protocol

  @power_labels ["writes" | Protocol.official_variants()]

  @doc "Calculates power and throughput scores from validated raw timing records."
  @spec calculate([map()], keyword()) :: {:ok, map()} | {:error, [term()]}
  def calculate(records, opts) when is_list(records) do
    scale_factor = Keyword.fetch!(opts, :scale_factor)
    load_time_s = Keyword.fetch!(opts, :load_time_s)
    comparable? = Keyword.get(opts, :comparable, false)
    throughput_min_s = Keyword.get(opts, :throughput_min_s, if(comparable?, do: 3_600, else: 0))

    errors = eligibility_errors(records, load_time_s, throughput_min_s, comparable?)

    if errors == [] do
      power = power_score(records)
      throughput = throughput_score(records, load_time_s, throughput_min_s)

      {:ok,
       %{
         namespace: if(comparable?, do: :canonical, else: :diagnostic),
         power: power,
         power_at_scale: power * scale_factor,
         throughput: throughput,
         throughput_at_scale: if(throughput, do: throughput * scale_factor),
         input_digest: digest(records),
         qualified?: comparable?
       }}
    else
      {:error, errors}
    end
  end

  defp eligibility_errors(records, load_time_s, throughput_min_s, comparable?) do
    power = Enum.filter(records, &(&1.batch_type == :power and &1.label in @power_labels))

    throughput =
      Enum.filter(records, &(&1.batch_type == :throughput and &1.label in ["reads", "writes"]))

    power_labels = power |> Enum.map(& &1.label) |> MapSet.new()

    []
    |> require(Enum.all?(records, &eligible?/1), :incorrect_or_incomplete_operation)
    |> require(power_labels == MapSet.new(@power_labels), :incomplete_power_block)
    |> require(
      is_number(load_time_s) and load_time_s >= 0 and load_time_s < 86_400,
      :invalid_load_time
    )
    |> require(valid_throughput_pairs?(throughput), :incomplete_throughput_batch)
    |> require(
      not comparable? or throughput_duration(throughput) >= throughput_min_s,
      :throughput_duration
    )
  end

  defp eligible?(record) do
    record[:correct?] == true and record[:complete?] == true and is_number(record[:duration_s]) and
      record.duration_s > 0
  end

  defp power_score(records) do
    totals =
      records
      |> Enum.filter(&(&1.batch_type == :power and &1.label in @power_labels))
      |> Enum.group_by(& &1.label, & &1.duration_s)
      |> Enum.map(fn {_label, times} -> Enum.sum(times) end)

    3_600 / geometric_mean(totals)
  end

  defp throughput_score(records, load_time_s, minimum) do
    batches =
      records
      |> Enum.filter(&(&1.batch_type == :throughput and &1.label in ["writes", "reads"]))
      |> Enum.group_by(& &1.day)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {day, rows} -> {day, Enum.sum(Enum.map(rows, & &1.duration_s))} end)
      |> take_through_minimum(minimum)

    if batches == [] do
      nil
    else
      duration_s = Enum.sum(Enum.map(batches, &elem(&1, 1)))
      (24 - load_time_s / 3_600) * (length(batches) / (duration_s / 3_600))
    end
  end

  defp take_through_minimum(batches, minimum) when minimum <= 0, do: batches

  defp take_through_minimum(batches, minimum) do
    Enum.reduce_while(batches, {[], 0.0}, fn batch, {selected, total} ->
      next = total + elem(batch, 1)

      if next >= minimum,
        do: {:halt, {selected ++ [batch], next}},
        else: {:cont, {selected ++ [batch], next}}
    end)
    |> elem(0)
  end

  defp valid_throughput_pairs?([]), do: true

  defp valid_throughput_pairs?(records) do
    records
    |> Enum.group_by(& &1.day)
    |> Enum.all?(fn {_day, rows} ->
      Enum.sort(Enum.map(rows, & &1.label)) == ["reads", "writes"]
    end)
  end

  defp throughput_duration(records), do: Enum.sum(Enum.map(records, & &1.duration_s))

  defp geometric_mean(values) do
    values |> Enum.map(&:math.log/1) |> Enum.sum() |> Kernel./(length(values)) |> :math.exp()
  end

  defp digest(records) do
    records
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp require(errors, true, _error), do: errors
  defp require(errors, false, error), do: errors ++ [error]
end
