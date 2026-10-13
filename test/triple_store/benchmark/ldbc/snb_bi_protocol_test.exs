defmodule TripleStore.Benchmark.LDBC.SNBBIProtocolTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.{PlanEvidence, Protocol, Scoring}

  test "official schedule preserves update-read blocks and canonical variant order" do
    assert {:ok, [power, throughput]} = Protocol.schedule(:throughput, max_batches: 2)
    assert power.date == ~D[2012-11-29]
    assert power.kind == :power
    assert throughput.date == ~D[2012-11-30]
    assert throughput.kind == :throughput
    assert power.stages == [:updates, :precomputations, :reads]
    assert power.variants == Protocol.official_variants()

    assert {:error, :comparable_schedule_cannot_be_reduced} =
             Protocol.schedule(:throughput, max_batches: 2, comparable: true)
  end

  test "local scorer matches pinned formulas and fails closed" do
    power =
      ["writes" | Protocol.official_variants()]
      |> Enum.map(&record(:power, ~D[2012-11-29], &1, 4.0))

    throughput = [
      record(:throughput, ~D[2012-11-30], "writes", 1_800.0),
      record(:throughput, ~D[2012-11-30], "reads", 1_800.0)
    ]

    assert {:ok, score} =
             Scoring.calculate(power ++ throughput,
               scale_factor: 10,
               load_time_s: 3_600,
               throughput_min_s: 3_600,
               comparable: true
             )

    assert_in_delta score.power, 900.0, 1.0e-9
    assert_in_delta score.power_at_scale, 9_000.0, 1.0e-8
    assert_in_delta score.throughput, 23.0, 1.0e-9
    assert score.namespace == :canonical

    bad = put_in(hd(power)[:correct?], false)

    assert {:error, errors} =
             Scoring.calculate([bad | tl(power)] ++ throughput,
               scale_factor: 10,
               load_time_s: 3_600,
               comparable: true
             )

    assert :incorrect_or_incomplete_operation in errors
  end

  test "optimization evidence rejects changed answers and incomplete measurements" do
    measurements = %{
      plan: :hash_join,
      estimated_cardinality: 3,
      iterator_count: 2,
      materialized_rows: 3,
      memory_bytes: 128,
      io_bytes: 256
    }

    assert {:ok, evidence} = PlanEvidence.record("bi-1", [[1], [2]], [[1], [2]], measurements)
    assert evidence.plan == :hash_join
    assert {:error, :answer_changed} = PlanEvidence.record("bi-1", [[1]], [[2]], measurements)
  end

  defp record(batch_type, day, label, duration) do
    %{
      batch_type: batch_type,
      day: day,
      label: label,
      duration_s: duration,
      correct?: true,
      complete?: true
    }
  end
end
