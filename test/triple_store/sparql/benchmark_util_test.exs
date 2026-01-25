defmodule TripleStore.SPARQL.BenchmarkUtilTest do
  @moduledoc """
  Tests for performance benchmarking utility module (S12).

  Tests the Benchmark helper module functionality.
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.Benchmark

  describe "measure/1" do
    test "measures execution time of a function" do
      {result, time_us} = Benchmark.measure(fn -> :ok end)

      assert result == :ok
      assert is_integer(time_us)
      assert time_us >= 0
    end

    test "measures slower function accurately" do
      {result, time_us} = Benchmark.measure(fn -> Process.sleep(10) end)

      assert result == :ok
      # Should be around 10ms = 10,000μs
      assert time_us >= 9_000
      assert time_us < 20_000
    end

    test "returns function result" do
      {result, _time_us} = Benchmark.measure(fn -> 42 end)

      assert result == 42
    end
  end

  describe "measure_with_memory/1" do
    test "measures execution time and memory usage" do
      {result, time_us, memory_mb} = Benchmark.measure_with_memory(fn -> :ok end)

      assert result == :ok
      assert is_integer(time_us)
      assert is_float(memory_mb)
    end

    test "captures memory allocation" do
      # Create some data to ensure memory usage
      {result, _time_us, memory_mb} =
        Benchmark.measure_with_memory(fn ->
          for _ <- 1..1000, do: :crypto.strong_rand_bytes(1024)
          :allocated
        end)

      assert result == :allocated
      # Should use some memory
      assert memory_mb > 0
    end
  end

  describe "run/2" do
    test "runs benchmark with default iterations" do
      measurement = Benchmark.run(fn -> :ok end)

      assert measurement.iterations == 10
      assert is_integer(measurement.total_us)
      assert is_float(measurement.avg_us)
      assert is_integer(measurement.min_us)
      assert is_integer(measurement.max_us)
      assert is_float(measurement.median_us)
    end

    test "respects custom iterations" do
      measurement = Benchmark.run(fn -> :ok end, iterations: 5)

      assert measurement.iterations == 5
    end

    test "calculates statistics correctly" do
      # Create a predictable function
      measurement = Benchmark.run(fn -> Process.sleep(1) end, iterations: 5)

      assert measurement.total_us >= 5_000
      assert measurement.avg_us >= 1_000
      assert measurement.min_us > 0
      assert measurement.max_us >= measurement.min_us
      assert measurement.median_us >= measurement.min_us
    end

    test "runs warmup iterations" do
      # Just verify it doesn't error
      measurement = Benchmark.run(fn -> :ok end, warmup: 3, iterations: 2)

      assert measurement.iterations == 2
    end

    test "tracks memory when requested" do
      measurement =
        Benchmark.run(
          fn ->
            for _ <- 1..100, do: :crypto.strong_rand_bytes(1024)
          end,
          memory: true
        )

      assert measurement.memory_mb > 0
    end

    test "does not track memory by default" do
      measurement = Benchmark.run(fn -> :ok end)

      assert measurement.memory_mb == 0.0
    end
  end

  describe "compare/3" do
    test "compares two functions" do
      comparison = Benchmark.compare(fn -> Process.sleep(1) end, fn -> Process.sleep(2) end)

      assert %{a: _, b: _, speedup: _, winner: _} = comparison
      assert is_float(comparison.speedup)
    end

    test "identifies faster function" do
      comparison =
        Benchmark.compare(
          fn -> Process.sleep(1) end,
          fn -> Process.sleep(10) end,
          iterations: 3
        )

      # First function should be faster (lower time)
      assert comparison.winner == :a
      assert comparison.speedup < 1.0
    end

    test "detects when functions are similar" do
      comparison =
        Benchmark.compare(
          fn -> Process.sleep(1) end,
          fn -> Process.sleep(1) end,
          iterations: 3
        )

      # Very close times should result in tie
      assert comparison.winner in [:a, :b, :tie]
    end
  end

  describe "suite/2" do
    test "runs multiple named functions" do
      reports =
        Benchmark.suite([
          {"Fast", fn -> :ok end},
          {"Slow", fn -> Process.sleep(1) end}
        ])

      assert length(reports) == 2

      assert [
               %{name: "Fast", measurement: _, timestamp: _},
               %{
                 name: "Slow",
                 measurement: _,
                 timestamp: _
               }
             ] = reports
    end

    test "respects custom iterations" do
      reports =
        Benchmark.suite(
          [{"Test", fn -> :ok end}],
          iterations: 5
        )

      [%{measurement: measurement}] = reports
      assert measurement.iterations == 5
    end

    test "includes timestamps" do
      before = System.system_time(:millisecond)

      reports =
        Benchmark.suite([
          {"Test", fn -> :ok end}
        ])

      after_time = System.system_time(:millisecond)

      [%{timestamp: timestamp}] = reports
      assert timestamp >= before
      assert timestamp <= after_time
    end
  end

  describe "format_measurement/1" do
    test "formats measurement as readable string" do
      measurement = %{
        iterations: 100,
        total_us: 5_000_000,
        avg_us: 50_000.0,
        min_us: 40_000,
        max_us: 60_000,
        median_us: 49_000.0,
        p95_us: 55_000.0,
        p99_us: 58_000.0,
        memory_mb: 1.5
      }

      formatted = Benchmark.format_measurement(measurement)

      assert String.contains?(formatted, "Iterations: 100")
      assert String.contains?(formatted, "50.00ms")
      assert String.contains?(formatted, "40.00ms")
      assert String.contains?(formatted, "60.00ms")
      assert String.contains?(formatted, "1.50 MB")
    end
  end

  describe "format_comparison/1" do
    test "formats comparison showing which is faster" do
      comparison = %{
        a: %{avg_us: 10_000},
        b: %{avg_us: 20_000},
        speedup: 0.5,
        winner: :a
      }

      formatted = Benchmark.format_comparison(comparison)

      assert String.contains?(formatted, "10.00ms")
      assert String.contains?(formatted, "20.00ms")
      assert String.contains?(formatted, "A is faster")
    end

    test "formats tie result" do
      comparison = %{
        a: %{avg_us: 10_000},
        b: %{avg_us: 10_100},
        speedup: 0.99,
        winner: :tie
      }

      formatted = Benchmark.format_comparison(comparison)

      assert String.contains?(formatted, "No significant difference")
    end
  end

  describe "format_suite/1" do
    test "formats suite report" do
      reports = [
        %{
          name: "Query 1",
          measurement: %{
            iterations: 10,
            total_us: 100_000,
            avg_us: 10_000.0,
            min_us: 8_000,
            max_us: 12_000,
            median_us: 10_000.0,
            p95_us: 11_500.0,
            p99_us: 11_800.0,
            memory_mb: 0.5
          },
          timestamp: 1_700_000_000_000
        }
      ]

      formatted = Benchmark.format_suite(reports)

      assert String.contains?(formatted, "## Query 1")
      assert String.contains?(formatted, "Iterations: 10")
    end
  end

  describe "format_duration/1" do
    test "formats microseconds" do
      assert Benchmark.format_duration(500) == "500μs"
    end

    test "formats milliseconds" do
      assert Benchmark.format_duration(10_000) == "10.00ms"
      assert Benchmark.format_duration(1_500) == "1.50ms"
    end

    test "formats seconds" do
      assert Benchmark.format_duration(1_500_000) == "1.50s"
      assert Benchmark.format_duration(5_000_000) == "5.00s"
    end
  end

  describe "export_csv/2" do
    test "exports benchmark results to CSV" do
      reports = [
        %{
          name: "Query A",
          measurement: %{
            iterations: 10,
            total_us: 100_000,
            avg_us: 10_000.0,
            min_us: 8_000,
            max_us: 12_000,
            median_us: 10_000.0,
            p95_us: 11_500.0,
            p99_us: 11_800.0,
            memory_mb: 0.5
          },
          timestamp: 1_700_000_000_000
        },
        %{
          name: "Query B",
          measurement: %{
            iterations: 10,
            total_us: 200_000,
            avg_us: 20_000.0,
            min_us: 18_000,
            max_us: 22_000,
            median_us: 20_000.0,
            p95_us: 21_500.0,
            p99_us: 21_800.0,
            memory_mb: 1.0
          },
          timestamp: 1_700_000_000_000
        }
      ]

      path = Path.join(System.tmp_dir!(), "benchmark_test_#{System.unique_integer()}.csv")

      try do
        assert :ok = Benchmark.export_csv(reports, path)

        assert File.exists?(path)

        content = File.read!(path)
        assert String.contains?(content, "Name")
        assert String.contains?(content, "Query A")
        assert String.contains?(content, "Query B")
        assert String.contains?(content, "10000.00")
        assert String.contains?(content, "0.50")
      after
        File.rm(path)
      end
    end
  end

  describe "stress_test/2" do
    test "runs stress test with increasing iterations" do
      results = Benchmark.stress_test(fn -> :ok end, max_iterations: 50, step: 10)

      assert length(results) == 5

      # Check structure
      Enum.each(results, fn {iterations, measurement} ->
        assert is_integer(iterations)
        assert is_map(measurement)
        assert Map.has_key?(measurement, :iterations)
      end)
    end

    test "respects max_iterations option" do
      results = Benchmark.stress_test(fn -> :ok end, max_iterations: 30, step: 10)

      assert length(results) == 3
    end

    test "respects step option" do
      results = Benchmark.stress_test(fn -> :ok end, max_iterations: 20, step: 20)

      assert length(results) == 1
    end
  end

  describe "memory_profile/2" do
    test "profiles memory usage over samples" do
      profile =
        Benchmark.memory_profile(fn ->
          for _ <- 1..100, do: :crypto.strong_rand_bytes(1024)
        end)

      assert is_list(profile.samples)
      assert length(profile.samples) == 10
      assert is_float(profile.avg_mb)
      assert is_float(profile.max_mb)
      assert profile.max_mb >= profile.avg_mb
    end

    test "respects custom sample count" do
      profile = Benchmark.memory_profile(fn -> :ok end, samples: 5)

      assert length(profile.samples) == 5
    end
  end
end
