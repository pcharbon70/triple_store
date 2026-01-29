defmodule TripleStore.Benchmark.TargetsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Targets

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
      end
    end

    test "target IDs are unique" do
      ids = Enum.map(Targets.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end
  end

  describe "get/1" do
    test "returns target by ID" do
      {:ok, target} = Targets.get(:simple_query)
      assert target.id == :simple_query
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = Targets.get(:unknown)
    end

    test "can retrieve all targets by ID" do
      for id <- [:simple_query, :complex_query, :bulk_load, :query_mix] do
        {:ok, target} = Targets.get(id)
        assert target.id == id
      end
    end
  end

  describe "simple_query_target/0" do
    test "has correct threshold of 10ms" do
      target = Targets.simple_query_target()
      assert target.threshold == 10_000
      assert target.unit == :microseconds
      assert target.operator == :lt
      assert target.description =~ "WatDiv"
    end
  end

  describe "complex_query_target/0" do
    test "has correct threshold of 100ms" do
      target = Targets.complex_query_target()
      assert target.threshold == 100_000
      assert target.unit == :microseconds
      assert target.operator == :lt
      assert target.description =~ "WatDiv"
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

  describe "query_mix_target/0" do
    test "has correct threshold of 50ms" do
      target = Targets.query_mix_target()
      assert target.threshold == 50_000
      assert target.unit == :microseconds
      assert target.operator == :lt
      assert target.description =~ "WatDiv"
    end
  end

  describe "check_simple_query/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_simple_query(p95_us: 5000)
      assert :pass = Targets.check_simple_query(p95_us: 9999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_simple_query(p95_us: 10_001)
      assert String.contains?(msg, "exceeds target")
    end

    test "fails when latency equals threshold" do
      assert {:fail, _} = Targets.check_simple_query(p95_us: 10_000)
    end
  end

  describe "check_complex_query/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_complex_query(p95_us: 50_000)
      assert :pass = Targets.check_complex_query(p95_us: 99_999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_complex_query(p95_us: 100_001)
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

  describe "check_query_mix/1" do
    test "passes when latency is below threshold" do
      assert :pass = Targets.check_query_mix(p95_us: 25_000)
      assert :pass = Targets.check_query_mix(p95_us: 49_999)
    end

    test "fails when latency exceeds threshold" do
      assert {:fail, msg} = Targets.check_query_mix(p95_us: 50_001)
      assert String.contains?(msg, "exceeds target")
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
      {:ok, report} = Targets.validate_bulk_load(1_000_000, 5000)
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "PASSED")
      assert String.contains?(formatted, "Performance Target Validation")
    end

    test "includes target details" do
      {:ok, report} = Targets.validate_bulk_load(1_000_000, 5000)
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "Bulk Load Throughput")
      assert String.contains?(formatted, "PASS")
    end

    test "shows failure status" do
      {:ok, report} = Targets.validate_bulk_load(100_000, 2000)
      formatted = Targets.format_report(report)

      assert String.contains?(formatted, "FAILED")
      assert String.contains?(formatted, "FAIL")
    end

    test "formats throughput numbers correctly" do
      {:ok, report} = Targets.validate_bulk_load(1_500_000, 10_000)
      formatted = Targets.format_report(report)

      # 1.5M triples in 10 seconds = 150K/sec
      assert String.contains?(formatted, "150")
    end
  end

  describe "print_report/1" do
    test "outputs formatted report to stdout" do
      {:ok, report} = Targets.validate_bulk_load(1_000_000, 5000)

      assert :ok = Targets.print_report(report)
    end
  end
end
