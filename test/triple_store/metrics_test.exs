defmodule TripleStore.MetricsTest do
  @moduledoc """
  Tests for the metrics collection module.

  Verifies that the Metrics GenServer correctly collects and aggregates
  telemetry events for queries, throughput, cache, and reasoning operations.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Metrics

  # Use a unique name per test to avoid conflicts
  defp unique_name, do: :"metrics_test_#{:erlang.unique_integer([:positive])}"

  describe "start_link/1" do
    test "starts the metrics collector" do
      name = unique_name()
      assert {:ok, pid} = Metrics.start_link(name: name)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "accepts custom histogram buckets" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name, histogram_buckets: [1, 10, 100])

      metrics = Metrics.query_metrics(name: name)
      histogram_keys = Map.keys(metrics.histogram)

      assert :le_1ms in histogram_keys
      assert :le_10ms in histogram_keys
      assert :le_100ms in histogram_keys
      assert :inf in histogram_keys

      GenServer.stop(pid)
    end
  end

  describe "get_all/1" do
    test "returns all metric categories" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      metrics = Metrics.get_all(name: name)

      assert Map.has_key?(metrics, :query)
      assert Map.has_key?(metrics, :throughput)
      assert Map.has_key?(metrics, :cache)
      assert Map.has_key?(metrics, :reasoning)
      assert Map.has_key?(metrics, :collected_at)
      assert %DateTime{} = metrics.collected_at

      GenServer.stop(pid)
    end
  end

  describe "query_metrics/1" do
    test "returns initial query metrics" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      metrics = Metrics.query_metrics(name: name)

      assert metrics.count == 0
      assert metrics.total_duration_ms == 0.0
      assert metrics.min_duration_ms == nil
      assert metrics.max_duration_ms == nil
      assert metrics.mean_duration_ms == 0.0
      assert is_map(metrics.histogram)
      assert metrics.percentiles == %{p50: 0.0, p90: 0.0, p95: 0.0, p99: 0.0}

      GenServer.stop(pid)
    end

    test "collects query duration from telemetry events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # Emit query execute stop event
      :telemetry.execute(
        [:triple_store, :query, :execute, :stop],
        %{duration: System.convert_time_unit(50, :millisecond, :native)},
        %{sparql: "SELECT * WHERE { ?s ?p ?o }"}
      )

      # Give time for the message to be processed
      Process.sleep(10)

      metrics = Metrics.query_metrics(name: name)

      assert metrics.count == 1
      assert metrics.total_duration_ms >= 50
      assert metrics.min_duration_ms >= 50
      assert metrics.max_duration_ms >= 50

      GenServer.stop(pid)
    end

    test "calculates percentiles correctly" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # Emit multiple query events with varying durations
      for duration_ms <- [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] do
        :telemetry.execute(
          [:triple_store, :query, :execute, :stop],
          %{duration_ms: duration_ms},
          %{}
        )
      end

      Process.sleep(20)

      metrics = Metrics.query_metrics(name: name)

      assert metrics.count == 10
      assert metrics.percentiles.p50 >= 50
      assert metrics.percentiles.p90 >= 90
      assert metrics.percentiles.p99 >= 99

      GenServer.stop(pid)
    end

    test "histogram buckets are updated correctly" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name, histogram_buckets: [10, 50, 100])

      # Emit events in different buckets
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 5}, %{})
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 25}, %{})
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 75}, %{})
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 200}, %{})

      Process.sleep(20)

      metrics = Metrics.query_metrics(name: name)

      assert metrics.histogram[:le_10ms] == 1
      assert metrics.histogram[:le_50ms] == 1
      assert metrics.histogram[:le_100ms] == 1
      assert metrics.histogram[:inf] == 1

      GenServer.stop(pid)
    end
  end

  describe "throughput_metrics/1" do
    test "returns initial throughput metrics" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      metrics = Metrics.throughput_metrics(name: name)

      assert metrics.insert_count == 0
      assert metrics.delete_count == 0
      assert metrics.insert_triple_count == 0
      assert metrics.delete_triple_count == 0
      assert is_integer(metrics.window_start)

      GenServer.stop(pid)
    end

    test "tracks insert events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :insert, :stop],
        %{duration: 1000},
        %{count: 100}
      )

      Process.sleep(10)

      metrics = Metrics.throughput_metrics(name: name)

      assert metrics.insert_count == 1
      assert metrics.insert_triple_count == 100

      GenServer.stop(pid)
    end

    test "tracks delete events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :delete, :stop],
        %{duration: 1000},
        %{count: 50}
      )

      Process.sleep(10)

      metrics = Metrics.throughput_metrics(name: name)

      assert metrics.delete_count == 1
      assert metrics.delete_triple_count == 50

      GenServer.stop(pid)
    end

    test "tracks load events for insert throughput" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :load, :stop],
        %{duration: 1000},
        %{total_count: 10_000}
      )

      Process.sleep(10)

      metrics = Metrics.throughput_metrics(name: name)

      assert metrics.insert_count == 1
      assert metrics.insert_triple_count == 10_000

      GenServer.stop(pid)
    end

    test "calculates rates per second" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :insert, :stop],
        %{duration: 1000},
        %{count: 1000}
      )

      # Wait a bit to get a measurable window duration
      Process.sleep(100)

      metrics = Metrics.throughput_metrics(name: name)

      assert metrics.insert_rate_per_sec > 0
      assert metrics.window_duration_ms >= 100

      GenServer.stop(pid)
    end
  end

  describe "cache_metrics/1" do
    test "returns initial cache metrics" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      metrics = Metrics.cache_metrics(name: name)

      assert metrics.hits == 0
      assert metrics.misses == 0
      assert metrics.hit_rate == 0.0
      assert metrics.by_type == %{}

      GenServer.stop(pid)
    end

    test "tracks cache hits by type" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute([:triple_store, :cache, :plan, :hit], %{count: 1}, %{})
      :telemetry.execute([:triple_store, :cache, :plan, :hit], %{count: 1}, %{})
      :telemetry.execute([:triple_store, :cache, :query, :hit], %{count: 1}, %{})

      Process.sleep(10)

      metrics = Metrics.cache_metrics(name: name)

      assert metrics.hits == 3
      assert metrics.by_type[:plan].hits == 2
      assert metrics.by_type[:query].hits == 1

      GenServer.stop(pid)
    end

    test "tracks cache misses by type" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute([:triple_store, :cache, :plan, :miss], %{count: 1}, %{})
      :telemetry.execute([:triple_store, :cache, :query, :miss], %{count: 1}, %{})
      :telemetry.execute([:triple_store, :cache, :stats, :miss], %{count: 1}, %{})

      Process.sleep(10)

      metrics = Metrics.cache_metrics(name: name)

      assert metrics.misses == 3
      assert metrics.by_type[:plan].misses == 1
      assert metrics.by_type[:query].misses == 1
      assert metrics.by_type[:stats].misses == 1

      GenServer.stop(pid)
    end

    test "calculates hit rate correctly" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # 7 hits, 3 misses = 70% hit rate
      for _ <- 1..7 do
        :telemetry.execute([:triple_store, :cache, :plan, :hit], %{count: 1}, %{})
      end

      for _ <- 1..3 do
        :telemetry.execute([:triple_store, :cache, :plan, :miss], %{count: 1}, %{})
      end

      Process.sleep(10)

      metrics = Metrics.cache_metrics(name: name)

      assert_in_delta metrics.hit_rate, 0.7, 0.001
      assert_in_delta metrics.by_type[:plan].hit_rate, 0.7, 0.001

      GenServer.stop(pid)
    end
  end

  describe "reasoning_metrics/1" do
    test "returns initial reasoning metrics" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      metrics = Metrics.reasoning_metrics(name: name)

      assert metrics.materialization_count == 0
      assert metrics.total_iterations == 0
      assert metrics.total_derived == 0
      assert metrics.total_duration_ms == 0.0

      GenServer.stop(pid)
    end

    test "tracks materialization events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :stop],
        %{duration_ms: 100},
        %{iterations: 5, total_derived: 1000}
      )

      Process.sleep(10)

      metrics = Metrics.reasoning_metrics(name: name)

      assert metrics.materialization_count == 1
      assert metrics.total_iterations == 5
      assert metrics.total_derived == 1000
      assert metrics.total_duration_ms == 100

      GenServer.stop(pid)
    end

    test "tracks iteration events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :iteration],
        %{derivations: 100},
        %{iteration: 1}
      )

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :iteration],
        %{derivations: 50},
        %{iteration: 2}
      )

      Process.sleep(10)

      metrics = Metrics.reasoning_metrics(name: name)

      assert metrics.total_derived == 150

      GenServer.stop(pid)
    end

    test "accumulates multiple materialization events" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :stop],
        %{duration_ms: 100},
        %{iterations: 3, total_derived: 500}
      )

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :stop],
        %{duration_ms: 200},
        %{iterations: 4, total_derived: 750}
      )

      Process.sleep(10)

      metrics = Metrics.reasoning_metrics(name: name)

      assert metrics.materialization_count == 2
      assert metrics.total_iterations == 7
      assert metrics.total_derived == 1250
      assert metrics.total_duration_ms == 300

      GenServer.stop(pid)
    end
  end

  describe "reset/1" do
    test "resets all metrics to initial values" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # Generate some events
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 50}, %{})
      :telemetry.execute([:triple_store, :insert, :stop], %{duration: 1000}, %{count: 100})
      :telemetry.execute([:triple_store, :cache, :plan, :hit], %{count: 1}, %{})

      :telemetry.execute(
        [:triple_store, :reasoner, :materialize, :stop],
        %{duration_ms: 100},
        %{iterations: 5, total_derived: 500}
      )

      Process.sleep(10)

      # Verify metrics were collected
      metrics_before = Metrics.get_all(name: name)
      assert metrics_before.query.count == 1
      assert metrics_before.throughput.insert_count == 1
      assert metrics_before.cache.hits == 1
      assert metrics_before.reasoning.materialization_count == 1

      # Reset
      assert :ok = Metrics.reset(name: name)

      # Verify all metrics are reset
      metrics_after = Metrics.get_all(name: name)
      assert metrics_after.query.count == 0
      assert metrics_after.throughput.insert_count == 0
      assert metrics_after.cache.hits == 0
      assert metrics_after.reasoning.materialization_count == 0

      GenServer.stop(pid)
    end
  end

  describe "handler cleanup on terminate" do
    test "detaches telemetry handlers when stopped" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # Stop the process
      GenServer.stop(pid)

      # Emit events - they should not cause errors since handlers are detached
      # If handlers weren't cleaned up, this could cause issues
      :telemetry.execute([:triple_store, :query, :execute, :stop], %{duration_ms: 50}, %{})
      :telemetry.execute([:triple_store, :cache, :plan, :hit], %{count: 1}, %{})

      # No assertion needed - we're just verifying no errors are raised
    end
  end

  describe "concurrent access" do
    test "handles concurrent metric requests" do
      name = unique_name()
      {:ok, pid} = Metrics.start_link(name: name)

      # Start multiple tasks that access metrics concurrently
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            Metrics.get_all(name: name)
            Metrics.query_metrics(name: name)
            Metrics.cache_metrics(name: name)
            Metrics.throughput_metrics(name: name)
            Metrics.reasoning_metrics(name: name)
            :ok
          end)
        end

      # All tasks should complete without errors
      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      GenServer.stop(pid)
    end
  end
end
