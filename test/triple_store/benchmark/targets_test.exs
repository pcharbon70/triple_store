defmodule TripleStore.Benchmark.TargetsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Targets

  @moduletag :benchmark

  describe "all/0" do
    test "returns 4 targets" do
      targets = Targets.all()
      assert length(targets) == 4
    end

    test "all targets have required fields" do
      for target <- Targets.all() do
        assert Map.has_key?(target, :id)
        assert Map.has_key?(target, :name)
        assert Map.has_key?(target, :description)
        assert Map.has_key?(target, :metric)
        assert Map.has_key?(target, :threshold)
        assert Map.has_key?(target, :unit)
        assert Map.has_key?(target, :operator)
        assert Map.has_key?(target, :dataset_size)
      end
    end

    test "target IDs are unique" do
      ids = Enum.map(Targets.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end
  end

  describe "get/1" do
    test "returns target by ID" do
      {:ok, target} = Targets.get(:simple_bgp)
      assert target.id == :simple_bgp
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = Targets.get(:unknown)
    end

    test "can retrieve all targets by ID" do
      for id <- [:simple_bgp, :complex_join, :bulk_load, :bsbm_mix] do
        {:ok, target} = Targets.get(id)
        assert target.id == id
      end
    end
  end

  describe "reference_dataset_size/0" do
    test "returns 1 million" do
      assert Targets.reference_dataset_size() == 1_000_000
    end
  end

  describe "simple_bgp_target/0" do
    test "has correct threshold of 10ms" do
      target = Targets.simple_bgp_target()
      assert target.threshold == 10_000
      assert target.unit == :microseconds
      assert target.operator == :lt
    end
  end

  describe "complex_join_target/0" do
    test "has correct threshold of 100ms" do
      target = Targets.complex_join_target()
      assert target.threshold == 100_000
      assert target.unit == :microseconds
      assert target.operator == :lt
    end
  end

  describe "bulk_load_target/0" do
    test "has correct threshold of 100K triples/sec" do
      target = Targets.bulk_load_target()
      assert target.threshold == 100_000
      assert target.unit == :triples_per_sec
      assert target.operator == :gt
    end
  end

  describe "bsbm_mix_target/0" do
    test "has correct threshold of 50ms" do
      target = Targets.bsbm_mix_target()
      assert target.threshold == 50_000
      assert target.unit == :microseconds
      assert target.operator == :lt
    end
  end

  describe "check_simple_bgp/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_simple_bgp(p95_us: 5000)
      assert :pass = Targets.check_simple_bgp(p95_us: 9999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_simple_bgp(p95_us: 10_001)
      assert String.contains?(msg, "exceeds target")
    end

    test "fails when latency equals threshold" do
      assert {:fail, _} = Targets.check_simple_bgp(p95_us: 10_000)
    end
  end

  describe "check_complex_join/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_complex_join(p95_us: 50_000)
      assert :pass = Targets.check_complex_join(p95_us: 99_999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_complex_join(p95_us: 100_001)
      assert String.contains?(msg, "exceeds target")
    end
  end

  describe "check_bulk_load/1" do
    test "passes when throughput exceeds threshold" do
      assert :pass = Targets.check_bulk_load(triples_per_sec: 150_000)
      assert :pass = Targets.check_bulk_load(triples_per_sec: 100_001)
    end

    test "fails when throughput is below threshold" do
      assert {:fail, msg} = Targets.check_bulk_load(triples_per_sec: 99_999)
      assert String.contains?(msg, "below target")
    end

    test "fails when throughput equals threshold" do
      assert {:fail, _} = Targets.check_bulk_load(triples_per_sec: 100_000)
    end
  end

  describe "check_bsbm_mix/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_bsbm_mix(p95_us: 25_000)
      assert :pass = Targets.check_bsbm_mix(p95_us: 49_999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_bsbm_mix(p95_us: 50_001)
      assert String.contains?(msg, "exceeds target")
    end
  end

  describe "validate/1" do
    test "validates BSBM benchmark results" do
      result = mock_bsbm_result(p95: 25_000)
      {:ok, report} = Targets.validate(result)

      assert report.passed == true
      assert report.targets_checked == 1
      assert report.targets_passed == 1
      assert report.targets_failed == 0
    end

    test "reports failure for BSBM exceeding threshold" do
      result = mock_bsbm_result(p95: 75_000)
      {:ok, report} = Targets.validate(result)

      assert report.passed == false
      assert report.targets_failed == 1
    end

    test "validates LUBM benchmark results" do
      result = mock_lubm_result(simple_p95: 5000, complex_p95: 50_000)
      {:ok, report} = Targets.validate(result)

      assert report.passed == true
      assert report.targets_checked == 2
      assert report.targets_passed == 2
    end

    test "reports mixed results for LUBM" do
      result = mock_lubm_result(simple_p95: 5000, complex_p95: 150_000)
      {:ok, report} = Targets.validate(result)

      assert report.passed == false
      assert report.targets_passed == 1
      assert report.targets_failed == 1
    end
  end

  describe "validate_bulk_load/2" do
    test "passes when throughput exceeds target" do
      # 1M triples in 5 seconds = 200K/sec
      {:ok, report} = Targets.validate_bulk_load(1_000_000, 5000)

      assert report.passed == true
      assert report.targets_passed == 1
    end

    test "fails when throughput is below target" do
      # 1M triples in 20 seconds = 50K/sec
      {:ok, report} = Targets.validate_bulk_load(1_000_000, 20_000)

      assert report.passed == false
      assert report.targets_failed == 1
    end
  end

  describe "format_report/1" do
    test "includes status line" do
      {:ok, report} = Targets.validate(mock_bsbm_result(p95: 25_000))
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "PASSED")
      assert String.contains?(formatted, "Performance Target Validation")
    end

    test "includes target details" do
      {:ok, report} = Targets.validate(mock_bsbm_result(p95: 25_000))
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "BSBM Query Mix")
      assert String.contains?(formatted, "PASS")
    end

    test "shows failure status" do
      {:ok, report} = Targets.validate(mock_bsbm_result(p95: 75_000))
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "FAILED")
      assert String.contains?(formatted, "FAIL")
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp mock_bsbm_result(opts) do
    p95 = Keyword.get(opts, :p95, 25_000)

    %{
      benchmark: :bsbm,
      aggregate: %{
        p95_us: p95,
        total_queries: 100,
        total_time_us: 1_000_000,
        queries_per_sec: 100.0
      },
      query_results: []
    }
  end

  defp mock_lubm_result(opts) do
    simple_p95 = Keyword.get(opts, :simple_p95, 5000)
    complex_p95 = Keyword.get(opts, :complex_p95, 50_000)

    # Create latencies that will produce the desired p95 values
    simple_latencies = List.duplicate(simple_p95, 100)
    complex_latencies = List.duplicate(complex_p95, 100)

    %{
      benchmark: :lubm,
      aggregate: %{
        p95_us: max(simple_p95, complex_p95),
        total_queries: 200,
        total_time_us: 2_000_000,
        queries_per_sec: 100.0
      },
      query_results: [
        %{query_id: :q3, latencies_us: simple_latencies, p95_us: simple_p95},
        %{query_id: :q14, latencies_us: simple_latencies, p95_us: simple_p95},
        %{query_id: :q2, latencies_us: complex_latencies, p95_us: complex_p95},
        %{query_id: :q7, latencies_us: complex_latencies, p95_us: complex_p95}
      ]
    }
  end
end
