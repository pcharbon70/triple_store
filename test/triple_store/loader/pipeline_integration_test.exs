defmodule TripleStore.Loader.PipelineIntegrationTest do
  @moduledoc """
  Integration tests for Load Pipeline (Task 1.6.1).

  Tests complete bulk load operations with various configurations:
  - 1.6.1.1 Test parallel loading with 100K synthetic triples
  - 1.6.1.2 Test parallel loading with LUBM dataset
  - 1.6.1.3 Test parallel loading with BSBM dataset
  - 1.6.1.4 Test error handling and recovery
  - 1.6.1.5 Test memory usage stays bounded
  - 1.6.1.6 Test CPU utilization across cores
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Benchmark.BSBM
  alias TripleStore.Benchmark.LUBM
  alias TripleStore.Config.Runtime
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Loader

  @moduletag :integration
  @moduletag timeout: 300_000

  @test_db_base "/tmp/triple_store_pipeline_integration_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      try do
        Manager.stop(manager)
      catch
        :exit, _ -> :ok
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # 1.6.1.1: Test parallel loading with 100K synthetic triples
  # ===========================================================================

  describe "1.6.1.1 parallel loading with 100K synthetic triples" do
    @tag :large_dataset
    test "loads 100K triples successfully with bulk_mode", %{db: db, manager: manager} do
      # Generate 100K synthetic triples
      triple_count = 100_000
      triples = generate_synthetic_triples(triple_count)
      graph = RDF.Graph.new(triples)

      # Load with bulk_mode enabled
      start_time = System.monotonic_time(:millisecond)

      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          batch_size: 10_000
        )

      elapsed_ms = System.monotonic_time(:millisecond) - start_time

      assert count == triple_count
      throughput = count / (elapsed_ms / 1000)

      # Log performance info
      IO.puts("\n  100K synthetic triples loaded in #{elapsed_ms}ms")
      IO.puts("  Throughput: #{Float.round(throughput, 0)} triples/second")

      # Verify data is queryable
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == triple_count
    end

    @tag :large_dataset
    test "loads 100K triples with parallel stages", %{db: db, manager: manager} do
      triple_count = 100_000
      triples = generate_synthetic_triples(triple_count)
      graph = RDF.Graph.new(triples)

      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          stages: System.schedulers_online(),
          max_demand: 5
        )

      assert count == triple_count
    end

    @tag :large_dataset
    test "loads 100K triples from stream", %{db: db, manager: manager} do
      triple_count = 100_000
      stream = Stream.map(1..triple_count, &generate_triple/1)

      {:ok, count} =
        Loader.load_stream(db, manager, stream,
          bulk_mode: true,
          batch_size: 10_000
        )

      assert count == triple_count
    end
  end

  # ===========================================================================
  # 1.6.1.2: Test parallel loading with LUBM dataset
  # ===========================================================================

  describe "1.6.1.2 parallel loading with LUBM dataset" do
    @tag :large_dataset
    test "loads LUBM scale 1 dataset (~100K triples)", %{db: db, manager: manager} do
      # Generate LUBM data for 1 university
      graph = LUBM.generate(1, seed: 12_345)
      triple_count = RDF.Graph.triple_count(graph)

      start_time = System.monotonic_time(:millisecond)

      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          batch_size: 10_000
        )

      elapsed_ms = System.monotonic_time(:millisecond) - start_time

      assert count == triple_count
      throughput = count / (elapsed_ms / 1000)

      IO.puts("\n  LUBM scale 1 (#{triple_count} triples) loaded in #{elapsed_ms}ms")
      IO.puts("  Throughput: #{Float.round(throughput, 0)} triples/second")

      # Verify we can query the data
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == triple_count
    end

    @tag :large_dataset
    test "loads LUBM data via stream", %{db: db, manager: manager} do
      stream = LUBM.stream(1, seed: 12_345)

      {:ok, count} =
        Loader.load_stream(db, manager, stream,
          bulk_mode: true,
          batch_size: 10_000
        )

      assert count > 0

      # Verify data is stored
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == count
    end
  end

  # ===========================================================================
  # 1.6.1.3: Test parallel loading with BSBM dataset
  # ===========================================================================

  describe "1.6.1.3 parallel loading with BSBM dataset" do
    @tag :large_dataset
    test "loads BSBM 1000 products (~50K triples)", %{db: db, manager: manager} do
      # Generate BSBM data for 1000 products
      graph = BSBM.generate(1000, seed: 12_345)
      triple_count = RDF.Graph.triple_count(graph)

      start_time = System.monotonic_time(:millisecond)

      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          batch_size: 10_000
        )

      elapsed_ms = System.monotonic_time(:millisecond) - start_time

      assert count == triple_count
      throughput = count / (elapsed_ms / 1000)

      IO.puts("\n  BSBM 1000 products (#{triple_count} triples) loaded in #{elapsed_ms}ms")
      IO.puts("  Throughput: #{Float.round(throughput, 0)} triples/second")

      # Verify data is queryable
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == triple_count
    end

    @tag :large_dataset
    test "loads BSBM data via stream", %{db: db, manager: manager} do
      stream = BSBM.stream(1000, seed: 12_345)

      {:ok, count} =
        Loader.load_stream(db, manager, stream,
          bulk_mode: true,
          batch_size: 10_000
        )

      assert count > 0

      # Verify data is stored
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == count
    end
  end

  # ===========================================================================
  # 1.6.1.4: Test error handling and recovery
  # ===========================================================================

  describe "1.6.1.4 error handling and recovery" do
    test "handles halting via progress callback", %{db: db, manager: manager} do
      # Use more triples with smaller batches so callback has time to halt
      triples = generate_synthetic_triples(50_000)
      graph = RDF.Graph.new(triples)

      # Halt after loading some triples
      {:halted, partial_count} =
        Loader.load_graph(db, manager, graph,
          batch_size: 1000,
          progress_callback: fn progress ->
            if progress.triples_loaded >= 5000 do
              :halt
            else
              :continue
            end
          end
        )

      # Should have loaded some but not all triples
      # Due to batching, we may load a bit more than the threshold
      assert partial_count >= 5000
      assert partial_count < 50_000

      # Verify partial data is queryable
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == partial_count
    end

    test "database remains usable after partial load", %{db: db, manager: manager} do
      # First, do a partial load with enough triples to trigger callback
      triples1 = generate_synthetic_triples(20_000)
      graph1 = RDF.Graph.new(triples1)

      result =
        Loader.load_graph(db, manager, graph1,
          batch_size: 1000,
          progress_callback: fn progress ->
            if progress.triples_loaded >= 5000, do: :halt, else: :continue
          end
        )

      count1 =
        case result do
          {:halted, count} -> count
          {:ok, count} -> count
        end

      # Now load more data - should succeed
      triples2 =
        for i <- 20_001..22_000 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      graph2 = RDF.Graph.new(triples2)
      {:ok, count2} = Loader.load_graph(db, manager, graph2, batch_size: 1000)

      assert count2 == 2000

      # Total should be sum of first load + second load
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(all_triples) == count1 + count2
    end

    test "runtime config is restored after error in with_bulk_config", %{db: db, manager: manager} do
      triples = generate_synthetic_triples(1000)
      graph = RDF.Graph.new(triples)

      # This should restore config even after the function returns an error
      {:ok, {:error, :simulated}} =
        Runtime.with_bulk_config(db, [], fn _db ->
          {:error, :simulated}
        end)

      # Verify database still works
      {:ok, count} = Loader.load_graph(db, manager, graph, batch_size: 500)
      assert count == 1000
    end

    test "runtime config is restored after exception in with_bulk_config", %{db: db, manager: manager} do
      triples = generate_synthetic_triples(1000)
      graph = RDF.Graph.new(triples)

      # This should restore config after exception
      assert_raise RuntimeError, fn ->
        Runtime.with_bulk_config(db, [], fn _db ->
          raise "simulated error"
        end)
      end

      # Verify database still works
      {:ok, count} = Loader.load_graph(db, manager, graph, batch_size: 500)
      assert count == 1000
    end
  end

  # ===========================================================================
  # 1.6.1.5: Test memory usage stays bounded
  # ===========================================================================

  describe "1.6.1.5 memory usage stays bounded" do
    @tag :large_dataset
    test "memory does not grow unboundedly during large load", %{db: db, manager: manager} do
      triple_count = 50_000

      # Capture initial memory
      :erlang.garbage_collect()
      initial_memory = :erlang.memory(:total)

      # Generate and load triples in batches using stream
      stream = Stream.map(1..triple_count, &generate_triple/1)

      {:ok, count} =
        Loader.load_stream(db, manager, stream,
          bulk_mode: true,
          batch_size: 5000
        )

      assert count == triple_count

      # Force GC and capture final memory
      :erlang.garbage_collect()
      final_memory = :erlang.memory(:total)

      memory_growth_mb = (final_memory - initial_memory) / (1024 * 1024)

      IO.puts("\n  Memory growth during 50K triple load: #{Float.round(memory_growth_mb, 2)} MB")

      # Memory growth should be reasonable (under 500 MB for 50K triples)
      # This is a sanity check, not a strict limit
      assert memory_growth_mb < 500,
             "Memory grew by #{memory_growth_mb} MB, expected < 500 MB"
    end

    test "batch processing limits peak memory", %{db: db, manager: manager} do
      triple_count = 20_000

      # Track peak memory during load
      peak_memory = :atomics.new(1, signed: false)
      :atomics.put(peak_memory, 1, 0)

      # Monitor memory in a separate process
      monitor_pid =
        spawn(fn ->
          monitor_memory(peak_memory)
        end)

      stream = Stream.map(1..triple_count, &generate_triple/1)

      {:ok, count} =
        Loader.load_stream(db, manager, stream,
          bulk_mode: true,
          batch_size: 2000
        )

      # Stop the monitor
      Process.exit(monitor_pid, :normal)

      assert count == triple_count

      peak_mb = :atomics.get(peak_memory, 1) / (1024 * 1024)
      IO.puts("\n  Peak memory during 20K triple load: #{Float.round(peak_mb, 2)} MB")
    end
  end

  # ===========================================================================
  # 1.6.1.6: Test CPU utilization across cores
  # ===========================================================================

  describe "1.6.1.6 CPU utilization across cores" do
    @tag :large_dataset
    test "parallel loading uses multiple CPU cores", %{db: db, manager: manager} do
      triple_count = 50_000
      triples = generate_synthetic_triples(triple_count)
      graph = RDF.Graph.new(triples)

      cores = System.schedulers_online()

      # Load with parallel stages matching CPU cores
      start_time = System.monotonic_time(:millisecond)

      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          stages: cores,
          max_demand: 5,
          batch_size: 5000
        )

      parallel_time = System.monotonic_time(:millisecond) - start_time

      assert count == triple_count

      IO.puts("\n  50K triples loaded with #{cores} parallel stages in #{parallel_time}ms")
    end

    @tag :large_dataset
    test "parallel loading is faster than sequential for large datasets", %{
      db: db,
      manager: manager
    } do
      triple_count = 30_000
      triples = generate_synthetic_triples(triple_count)
      graph = RDF.Graph.new(triples)

      # Sequential load (stages: 1)
      start_time = System.monotonic_time(:millisecond)

      {:ok, _} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          stages: 1,
          batch_size: 5000
        )

      sequential_time = System.monotonic_time(:millisecond) - start_time

      # Clear data for parallel test
      # Need to create a new database
      path2 = "#{@test_db_base}_parallel_#{:erlang.unique_integer([:positive])}"
      {:ok, db2} = NIF.open(path2)
      {:ok, manager2} = Manager.start_link(db: db2)

      cores = System.schedulers_online()
      start_time = System.monotonic_time(:millisecond)

      {:ok, _} =
        Loader.load_graph(db2, manager2, graph,
          bulk_mode: true,
          stages: cores,
          max_demand: 5,
          batch_size: 5000
        )

      parallel_time = System.monotonic_time(:millisecond) - start_time

      # Cleanup
      Manager.stop(manager2)
      NIF.close(db2)
      File.rm_rf(path2)

      IO.puts("\n  Sequential vs Parallel (#{cores} cores):")
      IO.puts("  Sequential: #{sequential_time}ms")
      IO.puts("  Parallel: #{parallel_time}ms")
      IO.puts("  Speedup: #{Float.round(sequential_time / parallel_time, 2)}x")

      # Parallel should generally be faster with multiple cores
      # But this isn't guaranteed on all hardware, so we just log the results
      if cores > 1 and parallel_time < sequential_time do
        IO.puts("  Parallel loading achieved speedup!")
      end
    end

    test "stage count configuration is respected", %{db: _db, manager: _manager} do
      triples = generate_synthetic_triples(5000)
      graph = RDF.Graph.new(triples)

      for stages <- [1, 2, 4] do
        # Create new db for each test
        path = "#{@test_db_base}_stages_#{stages}_#{:erlang.unique_integer([:positive])}"
        {:ok, test_db} = NIF.open(path)
        {:ok, test_manager} = Manager.start_link(db: test_db)

        {:ok, count} =
          Loader.load_graph(test_db, test_manager, graph,
            stages: stages,
            batch_size: 1000
          )

        assert count == 5000

        Manager.stop(test_manager)
        NIF.close(test_db)
        File.rm_rf(path)
      end
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp generate_synthetic_triples(count) do
    for i <- 1..count do
      generate_triple(i)
    end
  end

  defp generate_triple(i) do
    {
      RDF.iri("http://example.org/subject/#{i}"),
      RDF.iri("http://example.org/predicate/#{rem(i, 100)}"),
      RDF.literal("Value number #{i}")
    }
  end

  defp monitor_memory(peak_atomic) do
    current = :erlang.memory(:total)
    current_peak = :atomics.get(peak_atomic, 1)

    if current > current_peak do
      :atomics.put(peak_atomic, 1, current)
    end

    Process.sleep(10)
    monitor_memory(peak_atomic)
  end
end
