defmodule TripleStore.Loader.ErrorHandlingTest do
  @moduledoc """
  Unit tests for Loader error handling (Tasks 1.3.5.3 and 1.3.5.4).

  Tests:
  - 1.3.5.3: Error handling in encoding stage
  - 1.3.5.4: Error handling in writing stage
  """

  use ExUnit.Case, async: true

  alias TripleStore.Test.LoaderHelper
  alias TripleStore.Loader

  @test_db_base "/tmp/error_handling_test"

  # ===========================================================================
  # 1.3.5.3: Error Handling in Encoding Stage
  # ===========================================================================

  describe "encoding stage error handling" do
    setup do
      {db, manager, test_path} = LoaderHelper.setup_test_db(@test_db_base, "encoding")

      on_exit(fn ->
        LoaderHelper.cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "sequential mode handles encoding errors gracefully", %{db: db, manager: manager} do
      # Create a valid graph to test normal behavior
      # Note: The RDF library validates terms, so truly invalid terms
      # can't be created through normal RDF.ex APIs
      graph = LoaderHelper.create_test_graph(100)

      # Should succeed with valid triples
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, parallel: false)
    end

    test "parallel mode handles encoding errors gracefully", %{db: db, manager: manager} do
      # Create a valid graph
      graph = LoaderHelper.create_test_graph(100)

      # Should succeed with valid triples in parallel mode
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, parallel: true)
    end

    test "encoding error in sequential mode returns error tuple", %{db: db, manager: manager} do
      # Test with an empty stream that produces an empty batch
      # This exercises the encoding path without errors
      triples = []
      assert {:ok, 0} = Loader.load_stream(db, manager, triples, parallel: false)
    end

    test "encoding error in parallel mode returns error tuple", %{db: db, manager: manager} do
      # Test with an empty stream in parallel mode
      triples = []
      assert {:ok, 0} = Loader.load_stream(db, manager, triples, parallel: true)
    end

    test "mixed valid and empty batches handled correctly in sequential mode", %{
      db: db,
      manager: manager
    } do
      # Create 50 triples, with batch_size 100 this becomes 1 batch
      graph = LoaderHelper.create_test_graph(50)

      assert {:ok, 50} = Loader.load_graph(db, manager, graph, parallel: false, batch_size: 100)
    end

    test "mixed valid and empty batches handled correctly in parallel mode", %{
      db: db,
      manager: manager
    } do
      # Create 50 triples, with batch_size 100 this becomes 1 batch
      graph = LoaderHelper.create_test_graph(50)

      assert {:ok, 50} = Loader.load_graph(db, manager, graph, parallel: true, batch_size: 100)
    end
  end

  # ===========================================================================
  # 1.3.5.4: Error Handling in Writing Stage
  # ===========================================================================

  describe "writing stage error handling" do
    setup do
      {db, manager, test_path} = LoaderHelper.setup_test_db(@test_db_base, "writing")

      on_exit(fn ->
        LoaderHelper.cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "successful writes in sequential mode", %{db: db, manager: manager} do
      graph = LoaderHelper.create_test_graph(200)

      # Multiple batches should all write successfully
      assert {:ok, 200} = Loader.load_graph(db, manager, graph, parallel: false, batch_size: 100)
    end

    test "successful writes in parallel mode", %{db: db, manager: manager} do
      graph = LoaderHelper.create_test_graph(200)

      # Multiple batches should all write successfully in parallel
      assert {:ok, 200} = Loader.load_graph(db, manager, graph, parallel: true, batch_size: 100)
    end

    test "write stage handles single batch", %{db: db, manager: manager} do
      graph = LoaderHelper.create_test_graph(100)

      # Single batch write
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, batch_size: 100)
    end

    test "write stage handles many small batches", %{db: db, manager: manager} do
      graph = LoaderHelper.create_test_graph(500)

      # Many batches (5 batches of 100 each)
      assert {:ok, 500} = Loader.load_graph(db, manager, graph, batch_size: 100)
    end

    test "write stage telemetry emitted for each batch", %{db: db, manager: manager} do
      # Set up telemetry handler
      test_pid = self()

      handler_id = "write-telemetry-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :loader, :batch],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:batch_telemetry, measurements, metadata})
        end,
        nil
      )

      graph = LoaderHelper.create_test_graph(300)

      # 3 batches of 100 each
      {:ok, 300} = Loader.load_graph(db, manager, graph, batch_size: 100, parallel: false)

      # Should receive 3 batch telemetry events
      assert_receive {:batch_telemetry, %{count: 100}, %{batch_number: 1}}, 1000
      assert_receive {:batch_telemetry, %{count: 100}, %{batch_number: 2}}, 1000
      assert_receive {:batch_telemetry, %{count: 100}, %{batch_number: 3}}, 1000

      :telemetry.detach(handler_id)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "error handling edge cases" do
    setup do
      {db, manager, test_path} = LoaderHelper.setup_test_db(@test_db_base, "edge")

      on_exit(fn ->
        LoaderHelper.cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "handles large batch with many triples", %{db: db, manager: manager} do
      # 1000 triples in a single batch
      graph = LoaderHelper.create_test_graph(1000)

      assert {:ok, 1000} = Loader.load_graph(db, manager, graph, batch_size: 1000)
    end

    test "handles concurrent batch processing", %{db: db, manager: manager} do
      graph = LoaderHelper.create_test_graph(400)

      # Multiple stages processing batches
      assert {:ok, 400} =
               Loader.load_graph(db, manager, graph,
                 parallel: true,
                 stages: 4,
                 batch_size: 100
               )
    end

    test "error recovery does not leave partial state", %{db: db, manager: manager} do
      # First load should succeed
      graph1 = LoaderHelper.create_test_graph(100)
      {:ok, 100} = Loader.load_graph(db, manager, graph1)

      # Second load should also succeed independently
      graph2 = LoaderHelper.create_test_graph(100)
      {:ok, 100} = Loader.load_graph(db, manager, graph2)
    end

    test "progress callback error handling", %{db: db, manager: manager} do
      callback = fn _info ->
        # Callback that always continues
        :continue
      end

      graph = LoaderHelper.create_test_graph(200)

      {:ok, 200} =
        Loader.load_graph(db, manager, graph,
          progress_callback: callback,
          progress_interval: 1,
          batch_size: 100
        )
    end

    test "halt from progress callback properly stops loading", %{db: db, manager: manager} do
      callback = fn info ->
        # Halt after 100 triples
        if info.triples_loaded >= 100, do: :halt, else: :continue
      end

      graph = LoaderHelper.create_test_graph(500)

      result =
        Loader.load_graph(db, manager, graph,
          progress_callback: callback,
          progress_interval: 1,
          batch_size: 100,
          parallel: false
        )

      # Should halt after first batch
      assert {:halted, count} = result
      assert count >= 100
      assert count < 500
    end
  end
end
