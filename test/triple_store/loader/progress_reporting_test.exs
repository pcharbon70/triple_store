defmodule TripleStore.Loader.ProgressReportingTest do
  @moduledoc """
  Unit tests for Loader progress reporting (Task 1.3.4).

  Tests:
  - Progress callback invocation
  - Progress interval configuration
  - Progress info contents (triples loaded, elapsed time, rate)
  - Cancellation via :halt return value
  - Parallel and sequential mode consistency
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader

  @test_db_base "/tmp/progress_reporting_test"

  # ===========================================================================
  # Test Helpers
  # ===========================================================================

  defp setup_test_db(suffix) do
    test_path = "#{@test_db_base}_#{suffix}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager, test_path}
  end

  defp cleanup_test_db(manager, db, test_path) do
    if Process.alive?(manager), do: Manager.stop(manager)
    NIF.close(db)
    File.rm_rf(test_path)
  end

  defp create_test_graph(count) do
    1..count
    |> Enum.map(fn i ->
      {RDF.iri("http://example.org/subject/#{i}"),
       RDF.iri("http://example.org/predicate"),
       RDF.literal("value_#{i}")}
    end)
    |> RDF.Graph.new()
  end

  # ===========================================================================
  # 1.3.4.1: Progress Callback Invocation
  # ===========================================================================

  describe "progress callback invocation" do
    setup do
      {db, manager, test_path} = setup_test_db("callback")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "progress callback is called during loading", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      # 400 triples with batch_size 100 = 4 batches, interval 1 means callback on each batch
      graph = create_test_graph(400)

      {:ok, 400} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      # Should receive 4 progress messages
      assert_receive {:progress, %{batch_number: 1}}, 1000
      assert_receive {:progress, %{batch_number: 2}}, 1000
      assert_receive {:progress, %{batch_number: 3}}, 1000
      assert_receive {:progress, %{batch_number: 4}}, 1000
    end

    test "no callback when progress_callback is nil", %{db: db, manager: manager} do
      # Simulate the effect of no callback - just ensure load works
      graph = create_test_graph(200)
      {:ok, 200} = Loader.load_graph(db, manager, graph, batch_size: 100, parallel: false)

      # No messages should be received
      refute_receive {:progress, _}, 100
    end

    test "callback works in parallel mode", %{db: db, manager: manager} do
      test_pid = self()
      callback_count = :counters.new(1, [])

      callback = fn _info ->
        :counters.add(callback_count, 1, 1)
        send(test_pid, :progress_called)
        :continue
      end

      graph = create_test_graph(400)

      {:ok, 400} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: true
        )

      # Should receive progress messages (at least some, parallel ordering may vary)
      assert_receive :progress_called, 1000
      count = :counters.get(callback_count, 1)
      assert count >= 1
    end
  end

  # ===========================================================================
  # 1.3.4.2: Progress Interval Configuration
  # ===========================================================================

  describe "progress interval" do
    setup do
      {db, manager, test_path} = setup_test_db("interval")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "default interval is 10 batches", %{db: db, manager: manager} do
      test_pid = self()
      callback_count = :counters.new(1, [])

      callback = fn info ->
        :counters.add(callback_count, 1, 1)
        send(test_pid, {:progress, info.batch_number})
        :continue
      end

      # 1100 triples with batch_size 100 = 11 batches
      # Default interval of 10 means callback at batch 10 only
      graph = create_test_graph(1100)

      {:ok, 1100} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          parallel: false
        )

      # Should only receive progress at batch 10 (default interval)
      assert_receive {:progress, 10}, 1000
      count = :counters.get(callback_count, 1)
      assert count == 1
    end

    test "interval of 2 calls every other batch", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info.batch_number})
        :continue
      end

      # 400 triples with batch_size 100 = 4 batches
      # Interval 2 means callback at batches 2 and 4
      graph = create_test_graph(400)

      {:ok, 400} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 2,
          parallel: false
        )

      assert_receive {:progress, 2}, 1000
      assert_receive {:progress, 4}, 1000
      refute_receive {:progress, 1}, 100
      refute_receive {:progress, 3}, 100
    end

    test "interval of 3 calls every third batch", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info.batch_number})
        :continue
      end

      # 600 triples with batch_size 100 = 6 batches
      # Interval 3 means callback at batches 3 and 6
      graph = create_test_graph(600)

      {:ok, 600} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 3,
          parallel: false
        )

      assert_receive {:progress, 3}, 1000
      assert_receive {:progress, 6}, 1000
      refute_receive {:progress, 1}, 100
      refute_receive {:progress, 2}, 100
    end
  end

  # ===========================================================================
  # 1.3.4.3: Progress Info Contents
  # ===========================================================================

  describe "progress info contents" do
    setup do
      {db, manager, test_path} = setup_test_db("info")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "progress info contains triples_loaded", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      graph = create_test_graph(300)

      {:ok, 300} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      # Batch 1: 100 triples, Batch 2: 200 triples, Batch 3: 300 triples
      assert_receive {:progress, %{triples_loaded: 100}}, 1000
      assert_receive {:progress, %{triples_loaded: 200}}, 1000
      assert_receive {:progress, %{triples_loaded: 300}}, 1000
    end

    test "progress info contains batch_number", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      graph = create_test_graph(200)

      {:ok, 200} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert_receive {:progress, %{batch_number: 1}}, 1000
      assert_receive {:progress, %{batch_number: 2}}, 1000
    end

    test "progress info contains elapsed_ms", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      graph = create_test_graph(200)

      {:ok, 200} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert_receive {:progress, %{elapsed_ms: elapsed}}, 1000
      assert is_integer(elapsed)
      assert elapsed >= 0
    end

    test "progress info contains rate_per_second", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      graph = create_test_graph(200)

      {:ok, 200} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert_receive {:progress, %{rate_per_second: rate}}, 1000
      assert is_float(rate)
      assert rate >= 0.0
    end

    test "elapsed_ms increases between batches", %{db: db, manager: manager} do
      test_pid = self()
      elapsed_values = :ets.new(:elapsed_values, [:ordered_set, :public])

      callback = fn info ->
        :ets.insert(elapsed_values, {info.batch_number, info.elapsed_ms})
        send(test_pid, {:progress, info})
        :continue
      end

      graph = create_test_graph(300)

      {:ok, 300} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert_receive {:progress, _}, 1000
      assert_receive {:progress, _}, 1000
      assert_receive {:progress, _}, 1000

      [{1, elapsed1}] = :ets.lookup(elapsed_values, 1)
      [{2, elapsed2}] = :ets.lookup(elapsed_values, 2)
      [{3, elapsed3}] = :ets.lookup(elapsed_values, 3)

      # Elapsed time should increase or stay same (non-decreasing)
      assert elapsed2 >= elapsed1
      assert elapsed3 >= elapsed2

      :ets.delete(elapsed_values)
    end
  end

  # ===========================================================================
  # 1.3.4.4: Cancellation Support
  # ===========================================================================

  describe "cancellation via :halt" do
    setup do
      {db, manager, test_path} = setup_test_db("cancel")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "returning :halt stops sequential loading", %{db: db, manager: manager} do
      callback = fn info ->
        if info.batch_number >= 2, do: :halt, else: :continue
      end

      # 400 triples with batch_size 100 = 4 batches
      # Halt at batch 2, so only 200 triples should be loaded
      graph = create_test_graph(400)

      result =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert {:halted, count} = result
      # Should stop after batch 2
      assert count == 200
    end

    test "returning :halt stops parallel loading", %{db: db, manager: manager} do
      callback = fn info ->
        if info.batch_number >= 2, do: :halt, else: :continue
      end

      graph = create_test_graph(400)

      result =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: true
        )

      assert {:halted, count} = result
      # Should have loaded at least the first 2 batches
      assert count >= 200
      # But should not have loaded all 400 triples (stopped early)
      assert count < 400
    end

    test "halt at first batch returns minimal count", %{db: db, manager: manager} do
      callback = fn _info -> :halt end

      graph = create_test_graph(400)

      result =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      # First batch of 100 is processed, then halt
      assert {:halted, 100} = result
    end

    test "load_stream respects cancellation", %{db: db, manager: manager} do
      callback = fn info ->
        if info.batch_number >= 2, do: :halt, else: :continue
      end

      triples =
        1..400
        |> Enum.map(fn i ->
          {RDF.iri("http://example.org/s/#{i}"),
           RDF.iri("http://example.org/p"),
           RDF.literal("v#{i}")}
        end)

      result =
        Loader.load_stream(db, manager, triples,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert {:halted, 200} = result
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    setup do
      {db, manager, test_path} = setup_test_db("edge")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "empty graph does not call progress callback", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      graph = RDF.Graph.new()

      {:ok, 0} =
        Loader.load_graph(db, manager, graph,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      refute_receive {:progress, _}, 100
    end

    test "single batch with interval 1 calls once", %{db: db, manager: manager} do
      test_pid = self()
      callback_count = :counters.new(1, [])

      callback = fn info ->
        :counters.add(callback_count, 1, 1)
        send(test_pid, {:progress, info})
        :continue
      end

      # 50 triples with batch_size 100 = 1 batch (partial)
      graph = create_test_graph(50)

      {:ok, 50} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 1,
          parallel: false
        )

      assert_receive {:progress, %{batch_number: 1, triples_loaded: 50}}, 1000
      count = :counters.get(callback_count, 1)
      assert count == 1
    end

    test "fewer batches than interval does not call callback", %{db: db, manager: manager} do
      test_pid = self()

      callback = fn info ->
        send(test_pid, {:progress, info})
        :continue
      end

      # 200 triples with batch_size 100 = 2 batches
      # Interval 10 means no callback (2 < 10)
      graph = create_test_graph(200)

      {:ok, 200} =
        Loader.load_graph(db, manager, graph,
          batch_size: 100,
          progress_callback: callback,
          progress_interval: 10,
          parallel: false
        )

      refute_receive {:progress, _}, 100
    end
  end
end
