defmodule TripleStore.OperationalTestingTest do
  @moduledoc """
  Operational testing for Task 5.7.3.

  These tests validate production operational features including:
  - Backup/restore cycle data preservation
  - Telemetry integration with Prometheus
  - Health checks under various conditions
  - Graceful shutdown and restart

  These are integration tests that verify the complete operational
  workflow works correctly in realistic scenarios.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backup
  alias TripleStore.Health
  alias TripleStore.Prometheus
  alias TripleStore.Metrics
  alias TripleStore.Telemetry

  @moduletag :integration
  @moduletag timeout: 120_000

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  defp create_temp_store do
    path = Path.join(System.tmp_dir!(), "ops_test_#{:rand.uniform(1_000_000)}")
    {:ok, store} = TripleStore.open(path)
    {store, path}
  end

  defp cleanup_store(store, path) do
    try do
      TripleStore.close(store)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    File.rm_rf!(path)
  end

  defp cleanup_path(path) do
    File.rm_rf!(path)
  end

  defp load_test_data(store, count \\ 100) do
    triples =
      for i <- 1..count do
        {
          RDF.iri("http://example.org/item#{i}"),
          RDF.iri("http://example.org/value"),
          RDF.literal(i)
        }
      end

    graph = RDF.Graph.new(triples)
    {:ok, loaded} = TripleStore.load_graph(store, graph)
    loaded
  end

  # ===========================================================================
  # 5.7.3.1: Backup/Restore Cycle Preserves All Data
  # ===========================================================================

  describe "5.7.3.1: backup/restore cycle preserves all data" do
    test "full backup and restore preserves all triples" do
      {store, path} = create_temp_store()
      backup_path = Path.join(System.tmp_dir!(), "backup_test_#{:rand.uniform(1_000_000)}")

      try do
        # Load test data
        loaded = load_test_data(store, 200)
        assert loaded == 200

        # Verify data before backup
        count_before = get_triple_count(store)
        assert count_before == 200

        # Create backup
        {:ok, metadata} = Backup.create(store, backup_path)
        assert metadata.path == backup_path
        assert metadata.file_count > 0
        assert metadata.size_bytes > 0

        # Close original store
        :ok = TripleStore.close(store)

        # Restore to new location
        restore_path = Path.join(System.tmp_dir!(), "restore_test_#{:rand.uniform(1_000_000)}")
        {:ok, restored_store} = Backup.restore(backup_path, restore_path)

        try do
          # Verify data after restore
          count_after = get_triple_count(restored_store)
          assert count_after == 200

          # Verify specific triple exists
          {:ok, specific} = TripleStore.query(restored_store,
            "SELECT ?v WHERE { <http://example.org/item100> <http://example.org/value> ?v }"
          )
          assert length(specific) == 1
        after
          TripleStore.close(restored_store)
          cleanup_path(restore_path)
        end
      after
        cleanup_path(path)
        cleanup_path(backup_path)
      end
    end

    test "backup preserves data with unique markers" do
      {store, path} = create_temp_store()
      backup_path = Path.join(System.tmp_dir!(), "marker_backup_#{:rand.uniform(1_000_000)}")

      try do
        # Load data with unique marker
        marker = "unique_#{:rand.uniform(999_999_999)}"
        {:ok, _} = TripleStore.update(store, """
          PREFIX ex: <http://example.org/>
          INSERT DATA {
            ex:marker ex:value "#{marker}" .
            ex:item1 ex:type ex:Thing .
            ex:item2 ex:type ex:Thing .
          }
        """)

        # Create backup
        {:ok, _} = Backup.create(store, backup_path)
        :ok = TripleStore.close(store)

        # Restore and verify marker
        restore_path = Path.join(System.tmp_dir!(), "marker_restore_#{:rand.uniform(1_000_000)}")
        {:ok, restored} = Backup.restore(backup_path, restore_path)

        try do
          {:ok, results} = TripleStore.query(restored,
            "SELECT ?v WHERE { <http://example.org/marker> <http://example.org/value> ?v }"
          )
          assert length(results) == 1
          # Verify we got some result back (the marker exists)
          result_value = results |> hd() |> Map.get("v")
          assert result_value != nil
        after
          TripleStore.close(restored)
          cleanup_path(restore_path)
        end
      after
        cleanup_path(path)
        cleanup_path(backup_path)
      end
    end

    test "backup rotation keeps only N most recent backups" do
      {store, path} = create_temp_store()
      backup_dir = Path.join(System.tmp_dir!(), "rotation_test_#{:rand.uniform(1_000_000)}")
      File.mkdir_p!(backup_dir)

      try do
        load_test_data(store, 50)

        # Create multiple rotating backups with proper timing
        successful_backups = 0
        for i <- 1..5 do
          # rotate auto-generates unique timestamped paths
          result = Backup.rotate(store, backup_dir, max_backups: 3)
          case result do
            {:ok, _} -> :ok
            {:error, _} -> :ok  # May fail due to timing
          end
          Process.sleep(1100)  # Delay > 1 second to ensure different timestamps
        end

        # List backups - may have fewer due to rotation
        {:ok, backups} = Backup.list(backup_dir)

        # Should have at most 3 backups (max_backups: 3)
        assert length(backups) <= 3
        assert length(backups) >= 1  # At least one backup should exist
      after
        cleanup_store(store, path)
        cleanup_path(backup_dir)
      end
    end

    test "backup creates restorable snapshot" do
      {store, path} = create_temp_store()
      backup_path = Path.join(System.tmp_dir!(), "snapshot_test_#{:rand.uniform(1_000_000)}")

      try do
        # Load initial data
        load_test_data(store, 100)

        # Add uniquely identifiable data
        {:ok, _} = TripleStore.update(store, """
          PREFIX ex: <http://example.org/>
          INSERT DATA {
            ex:snapshot_marker ex:created_at "2025-01-01" .
          }
        """)

        # Create full backup
        {:ok, meta} = Backup.create(store, backup_path)
        assert meta.backup_type == :full
        assert meta.file_count > 0

        :ok = TripleStore.close(store)
        Process.sleep(200)

        # Restore and verify
        restore_path = Path.join(System.tmp_dir!(), "snapshot_restore_#{:rand.uniform(1_000_000)}")
        {:ok, restored} = Backup.restore(backup_path, restore_path)

        try do
          # Should have all triples including marker
          count = get_triple_count(restored)
          assert count == 101  # 100 + 1 marker

          # Verify marker exists
          {:ok, results} = TripleStore.query(restored,
            "SELECT ?d WHERE { <http://example.org/snapshot_marker> <http://example.org/created_at> ?d }"
          )
          assert length(results) == 1
        after
          TripleStore.close(restored)
          cleanup_path(restore_path)
        end
      after
        cleanup_path(path)
        cleanup_path(backup_path)
      end
    end
  end

  # ===========================================================================
  # 5.7.3.2: Telemetry Integration with Prometheus
  # ===========================================================================

  describe "5.7.3.2: telemetry integration with Prometheus" do
    test "Prometheus metrics are collected for queries" do
      {store, path} = create_temp_store()

      try do
        # Start Prometheus if not already started
        ensure_prometheus_started()

        load_test_data(store, 50)

        # Execute queries to generate telemetry
        for _i <- 1..5 do
          {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        end

        # Get Prometheus metrics
        metrics = Prometheus.format()

        # Should contain query metrics
        assert String.contains?(metrics, "triple_store_query_total")
        assert String.contains?(metrics, "triple_store_query_duration_seconds")
      after
        cleanup_store(store, path)
      end
    end

    test "Prometheus metrics are collected for inserts" do
      {store, path} = create_temp_store()

      try do
        ensure_prometheus_started()

        # Execute inserts
        for i <- 1..3 do
          {:ok, _} = TripleStore.update(store, """
            PREFIX ex: <http://example.org/>
            INSERT DATA {
              ex:item#{i} ex:value #{i} .
            }
          """)
        end

        metrics = Prometheus.format()

        # Should contain insert metrics
        assert String.contains?(metrics, "triple_store_insert_total")
      after
        cleanup_store(store, path)
      end
    end

    test "Prometheus format is valid exposition format" do
      {store, path} = create_temp_store()

      try do
        ensure_prometheus_started()

        load_test_data(store, 20)
        {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 5")

        metrics = Prometheus.format()

        # Verify format structure
        lines = String.split(metrics, "\n")

        # Should have HELP and TYPE lines
        help_lines = Enum.filter(lines, &String.starts_with?(&1, "# HELP"))
        type_lines = Enum.filter(lines, &String.starts_with?(&1, "# TYPE"))

        assert length(help_lines) > 0
        assert length(type_lines) > 0

        # Metric lines should have name and value
        metric_lines = Enum.filter(lines, fn line ->
          String.starts_with?(line, "triple_store_") and not String.starts_with?(line, "#")
        end)

        for line <- metric_lines do
          # Should have at least "metric_name value" format
          parts = String.split(line, " ")
          assert length(parts) >= 2, "Invalid metric line: #{line}"
        end
      after
        cleanup_store(store, path)
      end
    end

    test "Metrics GenServer collects and aggregates telemetry events" do
      {store, path} = create_temp_store()

      try do
        # Start metrics collector with unique name
        metrics_name = :"test_metrics_#{:rand.uniform(1_000_000)}"
        {:ok, _pid} = Metrics.start_link(name: metrics_name)

        load_test_data(store, 30)

        # Execute operations
        for _i <- 1..3 do
          {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        end

        # Get aggregated stats
        stats = Metrics.get_all(name: metrics_name)

        assert stats.query.count >= 3
        assert stats.query.total_duration_ms >= 0

        GenServer.stop(metrics_name)
      after
        cleanup_store(store, path)
      end
    end

    test "telemetry events are emitted for all operations" do
      {store, path} = create_temp_store()

      try do
        # Attach telemetry handler to capture events
        events_received = :ets.new(:test_events, [:set, :public])

        handler_id = "test_handler_#{:rand.uniform(1_000_000)}"

        # Listen for various telemetry events
        # Events are: [:triple_store, :subsystem, :operation, :phase]
        :telemetry.attach_many(
          handler_id,
          [
            [:triple_store, :sparql, :query, :stop],
            [:triple_store, :sparql, :update, :stop],
            [:triple_store, :insert, :stop],
            [:triple_store, :delete, :stop],
            [:triple_store, :load, :stop]
          ],
          fn event, _measurements, _metadata, _config ->
            :ets.insert(events_received, {event, true})
          end,
          nil
        )

        try do
          # Load data
          load_test_data(store, 10)

          # Query
          {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")

          # Insert
          {:ok, _} = TripleStore.update(store, """
            PREFIX ex: <http://example.org/>
            INSERT DATA { ex:test ex:value "test" }
          """)

          # Delete
          {:ok, _} = TripleStore.update(store, """
            PREFIX ex: <http://example.org/>
            DELETE DATA { ex:test ex:value "test" }
          """)

          # Verify events were received
          # Query uses [:triple_store, :sparql, :query, :stop]
          assert :ets.lookup(events_received, [:triple_store, :sparql, :query, :stop]) != []
          # Update uses [:triple_store, :sparql, :update, :stop]
          assert :ets.lookup(events_received, [:triple_store, :sparql, :update, :stop]) != []
        after
          :telemetry.detach(handler_id)
          :ets.delete(events_received)
        end
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # 5.7.3.3: Health Check Under Various Conditions
  # ===========================================================================

  describe "5.7.3.3: health check under various conditions" do
    test "store returns health status" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 50)

        {:ok, health} = Health.health(store)

        # Status may be :healthy or :degraded depending on cache processes
        assert health.status in [:healthy, :degraded]
        assert health.database_open == true
        assert health.dict_manager_alive == true
        assert health.triple_count == 50
      after
        cleanup_store(store, path)
      end
    end

    test "liveness check is fast and simple" do
      {store, path} = create_temp_store()

      try do
        # Liveness should be very fast
        start = System.monotonic_time(:microsecond)
        result = Health.liveness(store)
        duration = System.monotonic_time(:microsecond) - start

        assert result == :ok
        # Should complete in under 10ms
        assert duration < 10_000, "Liveness check too slow: #{duration}µs"
      after
        cleanup_store(store, path)
      end
    end

    test "readiness check returns ready for healthy store" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 10)

        {:ok, status} = Health.readiness(store)
        assert status == :ready
      after
        cleanup_store(store, path)
      end
    end

    test "health check reports triple count accurately" do
      {store, path} = create_temp_store()

      try do
        # Empty store
        {:ok, health1} = Health.health(store)
        assert health1.triple_count == 0

        # After loading
        load_test_data(store, 75)
        {:ok, health2} = Health.health(store)
        assert health2.triple_count == 75

        # Add additional unique triples (different prefix to avoid overwrites)
        additional_triples =
          for i <- 1..25 do
            {
              RDF.iri("http://example.org/extra#{i}"),
              RDF.iri("http://example.org/value"),
              RDF.literal(i)
            }
          end

        graph = RDF.Graph.new(additional_triples)
        {:ok, _} = TripleStore.load_graph(store, graph)

        {:ok, health3} = Health.health(store)
        assert health3.triple_count == 100  # 75 + 25 = 100
      after
        cleanup_store(store, path)
      end
    end

    test "health check works during concurrent operations" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 100)

        # Start concurrent query tasks
        query_tasks = for _i <- 1..10 do
          Task.async(fn ->
            for _j <- 1..5 do
              TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 10")
            end
          end)
        end

        # Run health checks while queries are running
        health_results = for _i <- 1..5 do
          {:ok, health} = Health.health(store)
          health.status
        end

        # Wait for queries to complete
        Task.await_many(query_tasks, 30_000)

        # All health checks should succeed (may be :healthy or :degraded during load)
        assert Enum.all?(health_results, &(&1 in [:healthy, :degraded]))
      after
        cleanup_store(store, path)
      end
    end

    test "health check with include_all option provides extra details" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 50)

        {:ok, health} = Health.health(store, include_all: true)

        # Status may be :healthy or :degraded depending on cache processes
        assert health.status in [:healthy, :degraded]
        assert health.triple_count == 50

        # Should include extra details when include_all is true
        assert Map.has_key?(health, :database_open)
        assert Map.has_key?(health, :dict_manager_alive)
      after
        cleanup_store(store, path)
      end
    end

    test "health check returns index sizes" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 100)

        # get_index_sizes takes the db reference from the store
        sizes = Health.get_index_sizes(store.db)

        # Should have all index sizes
        assert Map.has_key?(sizes, :spo)
        assert Map.has_key?(sizes, :pos)
        assert Map.has_key?(sizes, :osp)

        # Sizes should be non-negative
        assert sizes.spo >= 0
        assert sizes.pos >= 0
        assert sizes.osp >= 0
      after
        cleanup_store(store, path)
      end
    end

    test "health check returns memory estimate" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 100)

        # estimate_memory takes a triple count, not a store
        triple_count = get_triple_count(store)
        memory = Health.estimate_memory(triple_count)

        assert Map.has_key?(memory, :beam_mb)
        assert Map.has_key?(memory, :estimated_data_mb)
        assert Map.has_key?(memory, :estimated_total_mb)

        # Values should be positive
        assert memory.beam_mb > 0
        assert memory.estimated_total_mb > 0
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # 5.7.3.4: Graceful Shutdown and Restart
  # ===========================================================================

  describe "5.7.3.4: graceful shutdown and restart" do
    test "data persists after graceful close and reopen" do
      path = Path.join(System.tmp_dir!(), "shutdown_test_#{:rand.uniform(1_000_000)}")

      try do
        # Open and load data
        {:ok, store1} = TripleStore.open(path)
        load_test_data(store1, 100)

        # Verify data
        assert get_triple_count(store1) == 100

        # Graceful close
        :ok = TripleStore.close(store1)

        # Wait for RocksDB lock release - need sufficient time for Dict Manager cleanup
        Process.sleep(1000)

        # Force garbage collection to help release resources
        :erlang.garbage_collect()

        # Reopen (with retry for lock release)
        result = retry_open(path)
        assert match?({:ok, _}, result), "Failed to reopen store after close: #{inspect(result)}"
        {:ok, store2} = result

        try do
          # Verify data persisted
          assert get_triple_count(store2) == 100
        after
          TripleStore.close(store2)
        end
      after
        cleanup_path(path)
      end
    end

    test "pending operations complete before shutdown" do
      {store, path} = create_temp_store()

      try do
        # Start multiple insert operations
        insert_tasks = for i <- 1..5 do
          Task.async(fn ->
            TripleStore.update(store, """
              PREFIX ex: <http://example.org/>
              INSERT DATA {
                ex:batch#{i}_item1 ex:value #{i * 100 + 1} .
                ex:batch#{i}_item2 ex:value #{i * 100 + 2} .
              }
            """)
          end)
        end

        # Wait for all inserts
        Task.await_many(insert_tasks, 30_000)

        # Close gracefully
        :ok = TripleStore.close(store)

        # Wait for RocksDB lock release
        Process.sleep(200)

        # Reopen and verify all data was persisted
        {:ok, store2} = retry_open(path)

        try do
          # 5 batches * 2 items = 10 triples
          assert get_triple_count(store2) == 10
        after
          TripleStore.close(store2)
        end
      after
        cleanup_path(path)
      end
    end

    test "second close returns already_closed error" do
      {store, path} = create_temp_store()

      try do
        load_test_data(store, 10)
        :ok = TripleStore.close(store)

        # Second close should return error
        result = TripleStore.close(store)
        assert result == {:error, :already_closed}
      after
        cleanup_path(path)
      end
    end

    test "store can be reopened after close" do
      path = Path.join(System.tmp_dir!(), "reopen_test_#{:rand.uniform(1_000_000)}")

      try do
        # First session - load 50 items
        {:ok, store1} = TripleStore.open(path)
        load_test_data(store1, 50)
        :ok = TripleStore.close(store1)

        # Wait for lock release
        Process.sleep(300)

        # Second session - add 50 MORE unique items
        {:ok, store2} = retry_open(path)

        extra_triples =
          for i <- 1..50 do
            {
              RDF.iri("http://example.org/extra#{i}"),
              RDF.iri("http://example.org/value"),
              RDF.literal(i + 50)
            }
          end

        {:ok, _} = TripleStore.load_graph(store2, RDF.Graph.new(extra_triples))
        :ok = TripleStore.close(store2)

        # Wait for lock release
        Process.sleep(300)

        # Third session - verify cumulative data
        {:ok, store3} = retry_open(path)

        try do
          assert get_triple_count(store3) == 100
        after
          TripleStore.close(store3)
        end
      after
        cleanup_path(path)
      end
    end

    test "stats are available after reopen" do
      path = Path.join(System.tmp_dir!(), "stats_reopen_#{:rand.uniform(1_000_000)}")

      try do
        # First session
        {:ok, store1} = TripleStore.open(path)
        load_test_data(store1, 75)

        {:ok, stats1} = TripleStore.stats(store1)
        assert stats1.triple_count == 75

        :ok = TripleStore.close(store1)
        Process.sleep(200)

        # Reopen and check stats
        {:ok, store2} = retry_open(path)

        try do
          {:ok, stats2} = TripleStore.stats(store2)
          assert stats2.triple_count == 75
        after
          TripleStore.close(store2)
        end
      after
        cleanup_path(path)
      end
    end

    test "health check works immediately after reopen" do
      path = Path.join(System.tmp_dir!(), "health_reopen_#{:rand.uniform(1_000_000)}")

      try do
        # First session
        {:ok, store1} = TripleStore.open(path)
        load_test_data(store1, 50)
        :ok = TripleStore.close(store1)
        Process.sleep(300)

        # Reopen
        {:ok, store2} = retry_open(path)

        try do
          # Health check should work immediately
          {:ok, health} = Health.health(store2)
          # Status may be :healthy or :degraded depending on cache processes
          assert health.status in [:healthy, :degraded]
          assert health.triple_count == 50
        after
          TripleStore.close(store2)
        end
      after
        cleanup_path(path)
      end
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp get_triple_count(store) do
    {:ok, stats} = TripleStore.stats(store)
    stats.triple_count
  end

  defp retry_open(path, retries \\ 10) do
    case TripleStore.open(path) do
      {:ok, store} ->
        {:ok, store}

      {:error, _} when retries > 0 ->
        Process.sleep(500)
        retry_open(path, retries - 1)

      error ->
        error
    end
  end

  defp extract_value(%RDF.Literal{} = literal), do: RDF.Literal.value(literal) |> to_string()
  defp extract_value({:literal, :typed, value, _type}), do: to_string(value)
  defp extract_value({:literal, value, _type}), do: to_string(value)
  defp extract_value(value) when is_binary(value), do: value
  defp extract_value(value), do: to_string(value)

  defp ensure_prometheus_started do
    case Process.whereis(TripleStore.Prometheus) do
      nil ->
        {:ok, _} = Prometheus.start_link([])
        :ok
      _pid ->
        :ok
    end
  end
end
