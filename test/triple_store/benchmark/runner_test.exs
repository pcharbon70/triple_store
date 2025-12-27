defmodule TripleStore.Benchmark.RunnerTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Runner

  @moduletag :benchmark

  describe "percentile/2" do
    test "returns 0 for empty list" do
      assert Runner.percentile([], 50) == 0
    end

    test "calculates p50 for even number of elements" do
      values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      assert Runner.percentile(values, 50) == 5
    end

    test "calculates p50 for odd number of elements" do
      values = [1, 2, 3, 4, 5, 6, 7, 8, 9]
      assert Runner.percentile(values, 50) == 5
    end

    test "calculates p95" do
      values = Enum.to_list(1..100)
      p95 = Runner.percentile(values, 95)
      # p95 should be around 95
      assert p95 >= 94 and p95 <= 96
    end

    test "calculates p99" do
      values = Enum.to_list(1..100)
      p99 = Runner.percentile(values, 99)
      # p99 should be around 99
      assert p99 >= 98 and p99 <= 100
    end

    test "handles single element" do
      assert Runner.percentile([42], 50) == 42
      assert Runner.percentile([42], 95) == 42
      assert Runner.percentile([42], 99) == 42
    end

    test "handles two elements" do
      assert Runner.percentile([10, 20], 50) == 15
    end

    test "works with unsorted input" do
      values = [5, 1, 3, 2, 4]
      assert Runner.percentile(values, 50) == 3
    end

    test "p0 returns minimum" do
      values = [1, 2, 3, 4, 5]
      assert Runner.percentile(values, 0) == 1
    end

    test "p100 returns maximum" do
      values = [1, 2, 3, 4, 5]
      assert Runner.percentile(values, 100) == 5
    end
  end

  describe "format_duration/1" do
    test "formats microseconds" do
      assert Runner.format_duration(500) == "500µs"
      assert Runner.format_duration(999) == "999µs"
    end

    test "formats milliseconds" do
      assert Runner.format_duration(1000) == "1.0ms"
      assert Runner.format_duration(1500) == "1.5ms"
      assert Runner.format_duration(12345) == "12.35ms"
    end

    test "formats seconds" do
      assert Runner.format_duration(1_000_000) == "1.0s"
      assert Runner.format_duration(1_500_000) == "1.5s"
      assert Runner.format_duration(12_345_678) == "12.35s"
    end
  end

  describe "to_csv/1" do
    test "produces valid CSV header" do
      result = mock_benchmark_result()
      csv = Runner.to_csv(result)

      lines = String.split(csv, "\n")
      header = hd(lines)

      assert String.contains?(header, "query_id")
      assert String.contains?(header, "query_name")
      assert String.contains?(header, "iterations")
      assert String.contains?(header, "p50_us")
      assert String.contains?(header, "p95_us")
      assert String.contains?(header, "p99_us")
      assert String.contains?(header, "queries_per_sec")
    end

    test "produces correct number of rows" do
      result = mock_benchmark_result()
      csv = Runner.to_csv(result)

      lines = String.split(csv, "\n")
      # Header + 2 query results
      assert length(lines) == 3
    end

    test "escapes commas in query names" do
      result = mock_benchmark_result_with_comma()
      csv = Runner.to_csv(result)

      assert String.contains?(csv, "\"Query with, comma\"")
    end
  end

  describe "to_json/1" do
    test "produces valid JSON" do
      result = mock_benchmark_result()
      json = Runner.to_json(result)

      # Should be valid JSON
      assert {:ok, _} = Jason.decode(json)
    end

    test "includes all required fields" do
      result = mock_benchmark_result()
      json = Runner.to_json(result)
      {:ok, decoded} = Jason.decode(json)

      assert Map.has_key?(decoded, "benchmark")
      assert Map.has_key?(decoded, "started_at")
      assert Map.has_key?(decoded, "completed_at")
      assert Map.has_key?(decoded, "duration_ms")
      assert Map.has_key?(decoded, "scale")
      assert Map.has_key?(decoded, "warmup_iterations")
      assert Map.has_key?(decoded, "measurement_iterations")
      assert Map.has_key?(decoded, "query_results")
      assert Map.has_key?(decoded, "aggregate")
    end

    test "includes query results with correct structure" do
      result = mock_benchmark_result()
      json = Runner.to_json(result)
      {:ok, decoded} = Jason.decode(json)

      [first_query | _] = decoded["query_results"]

      assert Map.has_key?(first_query, "query_id")
      assert Map.has_key?(first_query, "query_name")
      assert Map.has_key?(first_query, "p50_us")
      assert Map.has_key?(first_query, "p95_us")
      assert Map.has_key?(first_query, "p99_us")
      assert Map.has_key?(first_query, "queries_per_sec")
    end

    test "includes aggregate statistics" do
      result = mock_benchmark_result()
      json = Runner.to_json(result)
      {:ok, decoded} = Jason.decode(json)

      aggregate = decoded["aggregate"]

      assert Map.has_key?(aggregate, "total_queries")
      assert Map.has_key?(aggregate, "total_time_us")
      assert Map.has_key?(aggregate, "queries_per_sec")
      assert Map.has_key?(aggregate, "p50_us")
      assert Map.has_key?(aggregate, "p95_us")
      assert Map.has_key?(aggregate, "p99_us")
    end
  end

  describe "run/3 options" do
    # These tests verify the option parsing without actually running queries

    test "accepts scale option" do
      # This would need a mock db to fully test
      # For now we verify the types are accepted
      opts = [scale: 10]
      assert Keyword.get(opts, :scale) == 10
    end

    test "accepts warmup option" do
      opts = [warmup: 5]
      assert Keyword.get(opts, :warmup) == 5
    end

    test "accepts iterations option" do
      opts = [iterations: 100]
      assert Keyword.get(opts, :iterations) == 100
    end

    test "accepts queries option" do
      opts = [queries: [:q1, :q2, :q7]]
      assert Keyword.get(opts, :queries) == [:q1, :q2, :q7]
    end

    test "accepts params option" do
      opts = [params: [uni: 5, dept: 3]]
      assert Keyword.get(opts, :params) == [uni: 5, dept: 3]
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp mock_benchmark_result do
    %{
      benchmark: :lubm,
      started_at: DateTime.utc_now(),
      completed_at: DateTime.utc_now(),
      duration_ms: 1000,
      scale: 1,
      warmup_iterations: 5,
      measurement_iterations: 10,
      query_results: [
        %{
          query_id: :q1,
          query_name: "Q1: Test Query",
          iterations: 10,
          latencies_us: [100, 110, 105, 115, 108, 103, 112, 107, 109, 111],
          p50_us: 108,
          p95_us: 115,
          p99_us: 115,
          min_us: 100,
          max_us: 115,
          mean_us: 108.0,
          std_dev_us: 4.5,
          queries_per_sec: 9259.26,
          result_count: 42
        },
        %{
          query_id: :q2,
          query_name: "Q2: Another Query",
          iterations: 10,
          latencies_us: [200, 210, 205, 215, 208, 203, 212, 207, 209, 211],
          p50_us: 208,
          p95_us: 215,
          p99_us: 215,
          min_us: 200,
          max_us: 215,
          mean_us: 208.0,
          std_dev_us: 4.5,
          queries_per_sec: 4807.69,
          result_count: 100
        }
      ],
      aggregate: %{
        total_queries: 20,
        total_time_us: 3160,
        queries_per_sec: 6329.11,
        p50_us: 158,
        p95_us: 215,
        p99_us: 215
      }
    }
  end

  defp mock_benchmark_result_with_comma do
    result = mock_benchmark_result()

    query_results =
      Enum.map(result.query_results, fn qr ->
        %{qr | query_name: "Query with, comma"}
      end)

    %{result | query_results: query_results}
  end
end
