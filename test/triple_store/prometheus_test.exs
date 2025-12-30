defmodule TripleStore.PrometheusTest do
  @moduledoc """
  Tests for the Prometheus metrics integration module.

  Verifies metric collection, telemetry event handling, and
  Prometheus text exposition format output.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Prometheus
  alias TripleStore.Backend.RocksDB.NIF

  # Unique name for each test to avoid conflicts
  defp unique_name do
    :"prometheus_test_#{:erlang.unique_integer([:positive])}"
  end

  defp start_prometheus(opts \\ []) do
    name = Keyword.get(opts, :name, unique_name())
    full_opts = Keyword.put(opts, :name, name)
    {:ok, pid} = Prometheus.start_link(full_opts)
    {pid, name}
  end

  defp stop_prometheus(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  end

  # Helper to create a mock store
  defp create_mock_store do
    path = Path.join(System.tmp_dir!(), "prometheus_test_#{:erlang.unique_integer([:positive])}")
    File.rm_rf!(path)

    case NIF.open(path) do
      {:ok, db} ->
        {:ok, agent} = Agent.start_link(fn -> %{} end)

        store = %{
          db: db,
          dict_manager: agent,
          transaction: nil,
          path: path
        }

        {:ok, store, path}

      {:error, _} = error ->
        error
    end
  end

  defp cleanup_store(%{db: db, dict_manager: dict_manager, path: path}) do
    if is_pid(dict_manager) and Process.alive?(dict_manager) do
      Agent.stop(dict_manager)
    end

    NIF.close(db)
    File.rm_rf!(path)
  end

  describe "start_link/1" do
    test "starts the Prometheus server with default name" do
      {pid, _name} = start_prometheus()
      assert Process.alive?(pid)
      stop_prometheus(pid)
    end

    test "starts the Prometheus server with custom name" do
      name = unique_name()
      {pid, ^name} = start_prometheus(name: name)
      assert Process.alive?(pid)
      assert Process.whereis(name) == pid
      stop_prometheus(pid)
    end

    test "starts with custom buckets" do
      buckets = [0.1, 0.5, 1.0, 5.0]
      {pid, name} = start_prometheus(buckets: buckets)

      metrics = Prometheus.get_metrics(name: name)
      bucket_keys = Map.keys(metrics.histograms.query_duration.buckets)
      # The histogram should have exactly the custom buckets
      assert length(bucket_keys) == length(buckets)

      Enum.each(buckets, fn b ->
        assert b in bucket_keys, "Expected bucket #{b} not found in #{inspect(bucket_keys)}"
      end)

      stop_prometheus(pid)
    end
  end

  describe "metric_definitions/0" do
    test "returns a list of metric definitions" do
      definitions = Prometheus.metric_definitions()

      assert is_list(definitions)
      assert length(definitions) > 0

      Enum.each(definitions, fn def ->
        assert is_map(def)
        assert Map.has_key?(def, :name)
        assert Map.has_key?(def, :type)
        assert Map.has_key?(def, :help)
        assert Map.has_key?(def, :labels)
        assert def.type in [:counter, :gauge, :histogram]
      end)
    end

    test "includes all expected metrics" do
      names =
        Prometheus.metric_definitions()
        |> Enum.map(& &1.name)

      expected = [
        "triple_store_query_duration_seconds",
        "triple_store_query_total",
        "triple_store_query_errors_total",
        "triple_store_insert_total",
        "triple_store_insert_triples_total",
        "triple_store_delete_total",
        "triple_store_delete_triples_total",
        "triple_store_load_total",
        "triple_store_load_triples_total",
        "triple_store_cache_hits_total",
        "triple_store_cache_misses_total",
        "triple_store_reasoning_total",
        "triple_store_reasoning_iterations_total",
        "triple_store_reasoning_derived_total",
        "triple_store_reasoning_duration_seconds",
        "triple_store_triples",
        "triple_store_memory_bytes",
        "triple_store_index_entries"
      ]

      Enum.each(expected, fn name ->
        assert name in names, "Expected metric #{name} not found"
      end)
    end
  end

  describe "get_metrics/1" do
    test "returns initial metric values" do
      {pid, name} = start_prometheus()

      metrics = Prometheus.get_metrics(name: name)

      assert is_map(metrics)
      assert is_map(metrics.counters)
      assert is_map(metrics.histograms)
      assert is_map(metrics.gauges)

      # Counters should be 0
      assert metrics.counters.query_total == 0
      assert metrics.counters.insert_total == 0

      # Histograms should have count 0
      assert metrics.histograms.query_duration.count == 0
      assert metrics.histograms.reasoning_duration.count == 0

      # Gauges should be 0
      assert metrics.gauges.triples == 0
      assert metrics.gauges.memory_bytes == 0

      stop_prometheus(pid)
    end
  end

  describe "format/1" do
    test "returns Prometheus text exposition format" do
      {pid, name} = start_prometheus()

      output = Prometheus.format(name: name)

      assert is_binary(output)
      assert String.contains?(output, "# HELP")
      assert String.contains?(output, "# TYPE")
      assert String.contains?(output, "triple_store_query_total")
      assert String.contains?(output, "triple_store_query_duration_seconds")

      stop_prometheus(pid)
    end

    test "includes all counter metrics" do
      {pid, name} = start_prometheus()

      output = Prometheus.format(name: name)

      assert String.contains?(output, "triple_store_query_total 0")
      assert String.contains?(output, "triple_store_query_errors_total 0")
      assert String.contains?(output, "triple_store_insert_total 0")
      assert String.contains?(output, "triple_store_delete_total 0")
      assert String.contains?(output, "triple_store_load_total 0")
      assert String.contains?(output, "triple_store_reasoning_total 0")

      stop_prometheus(pid)
    end

    test "includes histogram bucket lines" do
      {pid, name} = start_prometheus()

      output = Prometheus.format(name: name)

      assert String.contains?(output, "triple_store_query_duration_seconds_bucket{le=\"0.001\"}")
      assert String.contains?(output, "triple_store_query_duration_seconds_bucket{le=\"+Inf\"}")
      assert String.contains?(output, "triple_store_query_duration_seconds_sum")
      assert String.contains?(output, "triple_store_query_duration_seconds_count")

      stop_prometheus(pid)
    end

    test "includes gauge metrics" do
      {pid, name} = start_prometheus()

      output = Prometheus.format(name: name)

      assert String.contains?(output, "triple_store_triples 0")
      assert String.contains?(output, "triple_store_memory_bytes 0")

      stop_prometheus(pid)
    end
  end

  describe "reset/1" do
    test "resets all metrics to initial values" do
      {pid, name} = start_prometheus()

      # Simulate some metrics via direct state manipulation
      # (telemetry events would normally do this)
      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration: 1_000_000}, %{}}
      )

      # Wait for message to be processed
      :timer.sleep(10)

      metrics_before = Prometheus.get_metrics(name: name)
      assert metrics_before.counters.query_total == 1

      :ok = Prometheus.reset(name: name)

      metrics_after = Prometheus.get_metrics(name: name)
      assert metrics_after.counters.query_total == 0

      stop_prometheus(pid)
    end
  end

  describe "telemetry event handling" do
    test "handles query stop events" do
      {pid, name} = start_prometheus()

      # Simulate query stop event
      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration: 1_000_000}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.query_total == 1
      assert metrics.histograms.query_duration.count == 1

      stop_prometheus(pid)
    end

    test "handles query exception events" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :exception], %{}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.query_errors_total == 1

      stop_prometheus(pid)
    end

    test "handles insert events with count" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :insert, :stop], %{}, %{count: 100}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.insert_total == 1
      assert metrics.counters.insert_triples_total == 100

      stop_prometheus(pid)
    end

    test "handles delete events with count" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :delete, :stop], %{}, %{count: 50}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.delete_total == 1
      assert metrics.counters.delete_triples_total == 50

      stop_prometheus(pid)
    end

    test "handles load events with total_count" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :load, :stop], %{}, %{total_count: 1000}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.load_total == 1
      assert metrics.counters.load_triples_total == 1000

      stop_prometheus(pid)
    end

    test "handles cache hit events" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :cache, :plan, :hit], %{}, %{}}
      )

      send(
        pid,
        {:telemetry_event, [:triple_store, :cache, :query, :hit], %{}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.cache_hits_total[:plan] == 1
      assert metrics.counters.cache_hits_total[:query] == 1

      stop_prometheus(pid)
    end

    test "handles cache miss events" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :cache, :plan, :miss], %{}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.cache_misses_total[:plan] == 1

      stop_prometheus(pid)
    end

    test "handles reasoning events" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :reasoner, :materialize, :stop],
         %{duration: 5_000_000}, %{iterations: 3, total_derived: 500}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      assert metrics.counters.reasoning_total == 1
      assert metrics.counters.reasoning_iterations_total == 3
      assert metrics.counters.reasoning_derived_total == 500
      assert metrics.histograms.reasoning_duration.count == 1

      stop_prometheus(pid)
    end

    test "handles duration_ms in measurements" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration_ms: 150.0}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      # 150ms = 0.15s, should be in 0.25 bucket
      assert metrics.histograms.query_duration.buckets[0.25] == 1

      stop_prometheus(pid)
    end
  end

  describe "histogram buckets" do
    test "correctly buckets values" do
      {pid, name} = start_prometheus()

      # 5ms query
      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration_ms: 5.0}, %{}}
      )

      # 50ms query
      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration_ms: 50.0}, %{}}
      )

      # 500ms query
      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration_ms: 500.0}, %{}}
      )

      :timer.sleep(10)

      metrics = Prometheus.get_metrics(name: name)
      h = metrics.histograms.query_duration

      # 5ms = 0.005s should be in 0.005 bucket and all above
      assert h.buckets[0.005] == 1

      # 50ms = 0.05s should be in 0.05 bucket
      assert h.buckets[0.05] == 2

      # 500ms = 0.5s should be in 0.5 bucket
      assert h.buckets[0.5] == 3

      assert h.count == 3
      assert_in_delta h.sum, 0.555, 0.001

      stop_prometheus(pid)
    end
  end

  describe "labeled metrics format" do
    test "formats cache hits with labels" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :cache, :plan, :hit], %{}, %{}}
      )

      send(
        pid,
        {:telemetry_event, [:triple_store, :cache, :query, :hit], %{}, %{}}
      )

      :timer.sleep(10)

      output = Prometheus.format(name: name)

      assert String.contains?(output, "triple_store_cache_hits_total{cache_type=\"plan\"} 1")
      assert String.contains?(output, "triple_store_cache_hits_total{cache_type=\"query\"} 1")

      stop_prometheus(pid)
    end
  end

  describe "update_gauges/2" do
    test "updates gauge values from store" do
      case create_mock_store() do
        {:ok, store, _path} ->
          {pid, name} = start_prometheus()

          Prometheus.update_gauges(store, name: name)

          # Give async cast time to process
          :timer.sleep(50)

          metrics = Prometheus.get_metrics(name: name)

          # Triple count should be 0 for empty store
          assert metrics.gauges.triples == 0
          # Memory should be positive
          assert metrics.gauges.memory_bytes > 0
          # Index entries should be populated
          assert is_map(metrics.gauges.index_entries)

          stop_prometheus(pid)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "format output validation" do
    test "output ends with newline" do
      {pid, name} = start_prometheus()

      output = Prometheus.format(name: name)
      assert String.ends_with?(output, "\n")

      stop_prometheus(pid)
    end

    test "floats are formatted correctly" do
      {pid, name} = start_prometheus()

      send(
        pid,
        {:telemetry_event, [:triple_store, :query, :execute, :stop], %{duration_ms: 1.5}, %{}}
      )

      :timer.sleep(10)

      output = Prometheus.format(name: name)

      # Should contain sum with decimal
      assert String.contains?(output, "triple_store_query_duration_seconds_sum 0.0015")

      stop_prometheus(pid)
    end
  end
end
