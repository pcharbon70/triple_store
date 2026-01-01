defmodule TripleStore.Loader.ParallelLoadingTest do
  @moduledoc """
  Unit tests for Loader Flow pipeline design (Task 1.3.1).

  Tests:
  - Parallel loading mode (default enabled)
  - Sequential loading mode (parallel: false)
  - Stage count configuration
  - Backpressure via max_demand
  - Parallel vs sequential consistency
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader

  @test_db_base "/tmp/parallel_loading_test"

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
  # 1.3.1.5: Parallel Loading Mode
  # ===========================================================================

  describe "parallel loading" do
    setup do
      {db, manager, test_path} = setup_test_db("parallel")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "parallel mode is enabled by default", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      # Default behavior - parallel mode
      assert {:ok, 100} = Loader.load_graph(db, manager, graph)
    end

    test "parallel loading produces correct triple count", %{db: db, manager: manager} do
      graph = create_test_graph(500)
      assert {:ok, 500} = Loader.load_graph(db, manager, graph, parallel: true)
    end

    test "parallel loading handles empty graph", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, parallel: true)
    end

    test "parallel loading handles single triple", %{db: db, manager: manager} do
      graph = create_test_graph(1)
      assert {:ok, 1} = Loader.load_graph(db, manager, graph, parallel: true)
    end

    test "parallel loading handles multiple batches", %{db: db, manager: manager} do
      # With batch_size: 50 and 200 triples, we get 4 batches
      graph = create_test_graph(200)
      assert {:ok, 200} = Loader.load_graph(db, manager, graph, parallel: true, batch_size: 50)
    end
  end

  # ===========================================================================
  # Sequential Fallback Mode
  # ===========================================================================

  describe "sequential loading" do
    setup do
      {db, manager, test_path} = setup_test_db("sequential")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "sequential mode can be enabled explicitly", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, parallel: false)
    end

    test "sequential loading produces correct triple count", %{db: db, manager: manager} do
      graph = create_test_graph(500)
      assert {:ok, 500} = Loader.load_graph(db, manager, graph, parallel: false)
    end

    test "sequential loading handles empty graph", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, parallel: false)
    end

    test "sequential loading handles multiple batches", %{db: db, manager: manager} do
      graph = create_test_graph(200)
      assert {:ok, 200} = Loader.load_graph(db, manager, graph, parallel: false, batch_size: 50)
    end
  end

  # ===========================================================================
  # 1.3.1.6: Stage Count Configuration
  # ===========================================================================

  describe "stage count configuration" do
    setup do
      {db, manager, test_path} = setup_test_db("stages")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "accepts custom stage count", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: 4)
    end

    test "accepts minimum stage count of 1", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: 1)
    end

    test "accepts maximum stage count of 64", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: 64)
    end

    test "clamps stage count below minimum to 1", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      # Should be clamped to 1, not error
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: 0)
    end

    test "clamps stage count above maximum to 64", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      # Should be clamped to 64, not error
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: 100)
    end

    test "handles invalid stage count (uses default)", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      # Invalid value should use System.schedulers_online()
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, stages: "4")
    end
  end

  # ===========================================================================
  # 1.3.1.7: Backpressure via max_demand
  # ===========================================================================

  describe "backpressure configuration" do
    setup do
      {db, manager, test_path} = setup_test_db("backpressure")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "accepts custom max_demand", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, max_demand: 10)
    end

    test "accepts low max_demand for strict backpressure", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, max_demand: 1)
    end

    test "accepts high max_demand for throughput", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, max_demand: 50)
    end
  end

  # ===========================================================================
  # Parallel vs Sequential Consistency
  # ===========================================================================

  describe "parallel/sequential consistency" do
    test "parallel and sequential produce same triple count" do
      graph = create_test_graph(300)

      # Sequential
      {db1, manager1, path1} = setup_test_db("consistency_seq")
      {:ok, seq_count} = Loader.load_graph(db1, manager1, graph, parallel: false, batch_size: 50)
      cleanup_test_db(manager1, db1, path1)

      # Parallel
      {db2, manager2, path2} = setup_test_db("consistency_par")
      {:ok, par_count} = Loader.load_graph(db2, manager2, graph, parallel: true, batch_size: 50)
      cleanup_test_db(manager2, db2, path2)

      assert seq_count == par_count
      assert seq_count == 300
    end
  end

  # ===========================================================================
  # Integration with Other Load Functions
  # ===========================================================================

  describe "load_stream parallel options" do
    setup do
      {db, manager, test_path} = setup_test_db("stream")

      on_exit(fn ->
        cleanup_test_db(manager, db, test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "load_stream accepts parallel option", %{db: db, manager: manager} do
      triples =
        1..100
        |> Enum.map(fn i ->
          {RDF.iri("http://example.org/s/#{i}"),
           RDF.iri("http://example.org/p"),
           RDF.literal("v#{i}")}
        end)

      assert {:ok, 100} = Loader.load_stream(db, manager, triples, parallel: true)
    end

    test "load_stream accepts stages option", %{db: db, manager: manager} do
      triples =
        1..100
        |> Enum.map(fn i ->
          {RDF.iri("http://example.org/s/#{i}"),
           RDF.iri("http://example.org/p"),
           RDF.literal("v#{i}")}
        end)

      assert {:ok, 100} = Loader.load_stream(db, manager, triples, stages: 2)
    end

    test "load_stream accepts max_demand option", %{db: db, manager: manager} do
      triples =
        1..100
        |> Enum.map(fn i ->
          {RDF.iri("http://example.org/s/#{i}"),
           RDF.iri("http://example.org/p"),
           RDF.literal("v#{i}")}
        end)

      assert {:ok, 100} = Loader.load_stream(db, manager, triples, max_demand: 3)
    end

    test "load_stream accepts combined options", %{db: db, manager: manager} do
      triples =
        1..200
        |> Enum.map(fn i ->
          {RDF.iri("http://example.org/s/#{i}"),
           RDF.iri("http://example.org/p"),
           RDF.literal("v#{i}")}
        end)

      assert {:ok, 200} =
               Loader.load_stream(db, manager, triples,
                 parallel: true,
                 stages: 4,
                 max_demand: 5,
                 batch_size: 50
               )
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

    test "handles batch size larger than triple count", %{db: db, manager: manager} do
      graph = create_test_graph(50)
      assert {:ok, 50} = Loader.load_graph(db, manager, graph, batch_size: 1000, parallel: true)
    end

    test "handles exactly one batch", %{db: db, manager: manager} do
      graph = create_test_graph(100)
      assert {:ok, 100} = Loader.load_graph(db, manager, graph, batch_size: 100, parallel: true)
    end

    test "handles partial final batch", %{db: db, manager: manager} do
      # 175 triples with batch_size 50 = 3 full batches + 1 partial (25)
      graph = create_test_graph(175)
      assert {:ok, 175} = Loader.load_graph(db, manager, graph, batch_size: 50, parallel: true)
    end
  end
end
