defmodule TripleStore.Benchmark.LDBC.SNBBIScoringEquivalenceTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.Scoring

  test "local scores match golden outputs from the pinned upstream scorer" do
    golden = golden_fixture()

    assert {:ok, score} =
             Scoring.calculate(golden.records,
               scale_factor: golden.inputs.scale_factor,
               load_time_s: golden.inputs.load_time_s,
               throughput_min_s: golden.inputs.throughput_min_s,
               comparable: true
             )

    assert_in_delta score.power, golden.expected.power, 1.0e-9
    assert_in_delta score.power_at_scale, golden.expected.power_at_scale, 1.0e-8
    assert_in_delta score.throughput, golden.expected.throughput, 1.0e-9
    assert_in_delta score.throughput_at_scale, golden.expected.throughput_at_scale, 1.0e-8
    assert score.scorer == golden.source
    assert String.starts_with?(score.input_digest, "sha256:")
  end

  test "duplicate, incomplete, invalid, and unreconciled inputs fail closed" do
    golden = golden_fixture()
    opts = [scale_factor: 10, load_time_s: 3_600.0, throughput_min_s: 3_600.0]

    duplicate = [hd(golden.records) | golden.records]
    assert {:error, errors} = Scoring.calculate(duplicate, opts)
    assert :duplicate_power_operation in errors

    incomplete = Enum.reject(golden.records, &(&1.label == "20a"))
    assert {:error, errors} = Scoring.calculate(incomplete, opts)
    assert :incomplete_power_block in errors

    invalid = put_in(golden.records, [Access.at(0), :duration_s], 0.0)
    assert {:error, errors} = Scoring.calculate(invalid, opts)
    assert :incorrect_or_incomplete_operation in errors

    assert {:error, errors} = Scoring.calculate(golden.records, opts ++ [reconciled?: false])
    assert :protocol_reconciliation_failed in errors
  end

  defp golden_fixture do
    path =
      Path.join([
        to_string(:code.priv_dir(:triple_store)),
        "benchmarks",
        "ldbc",
        "snb-bi",
        "scoring",
        "golden-v1.exs"
      ])

    {fixture, _binding} = Code.eval_file(path)
    fixture
  end
end
