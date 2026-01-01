defmodule TripleStore.Loader.BatchSizeTest do
  @moduledoc """
  Unit tests for Loader batch size optimization (Task 1.2).

  Tests:
  - Default batch size configuration
  - Batch size validation and bounds
  - Memory budget options
  - optimal_batch_size/1 function
  - batch_size_config/0 function
  - Integration with load functions
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader

  @test_db_base "/tmp/batch_size_test"

  # ===========================================================================
  # 1.2.1: Default Batch Size Configuration
  # ===========================================================================

  describe "batch_size_config/0" do
    test "returns default batch size of 10,000" do
      config = Loader.batch_size_config()
      assert config.default == 10_000
    end

    test "returns min batch size of 100" do
      config = Loader.batch_size_config()
      assert config.min == 100
    end

    test "returns max batch size of 100,000" do
      config = Loader.batch_size_config()
      assert config.max == 100_000
    end

    test "returns a map with all keys" do
      config = Loader.batch_size_config()
      assert Map.keys(config) |> Enum.sort() == [:default, :max, :min]
    end
  end

  # ===========================================================================
  # 1.2.2: Memory Budget Options
  # ===========================================================================

  describe "optimal_batch_size/1" do
    test "returns 5,000 for :low memory budget" do
      assert Loader.optimal_batch_size(:low) == 5_000
    end

    test "returns 10,000 for :medium memory budget" do
      assert Loader.optimal_batch_size(:medium) == 10_000
    end

    test "returns 50,000 for :high memory budget" do
      assert Loader.optimal_batch_size(:high) == 50_000
    end

    test "returns default for invalid budget" do
      assert Loader.optimal_batch_size(:invalid) == 10_000
      assert Loader.optimal_batch_size("medium") == 10_000
      assert Loader.optimal_batch_size(nil) == 10_000
    end

    test ":auto returns a positive integer" do
      result = Loader.optimal_batch_size(:auto)
      assert is_integer(result)
      assert result > 0
      # Should be one of the valid batch sizes
      assert result in [5_000, 10_000, 50_000]
    end
  end

  # ===========================================================================
  # 1.2.3: Integration Tests with Load Functions
  # ===========================================================================

  describe "load function batch size resolution" do
    # These tests verify that the load functions correctly resolve batch size
    # from options. We can't easily test the internal batch size used without
    # modifying the loader, but we can verify the functions accept the options.

    setup do
      test_path = "#{@test_db_base}_load_#{:erlang.unique_integer([:positive])}"
      {:ok, db} = NIF.open(test_path)
      {:ok, manager} = Manager.start_link(db: db)

      on_exit(fn ->
        if Process.alive?(manager), do: Manager.stop(manager)
        NIF.close(db)
        File.rm_rf(test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "load_graph accepts :batch_size option", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, batch_size: 5_000)
    end

    test "load_graph accepts :memory_budget option", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, memory_budget: :low)
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, memory_budget: :medium)
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, memory_budget: :high)
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, memory_budget: :auto)
    end

    test "load_stream accepts :batch_size option", %{db: db, manager: manager} do
      assert {:ok, 0} = Loader.load_stream(db, manager, [], batch_size: 50_000)
    end

    test "load_stream accepts :memory_budget option", %{db: db, manager: manager} do
      assert {:ok, 0} = Loader.load_stream(db, manager, [], memory_budget: :high)
    end

    test ":batch_size takes precedence over :memory_budget", %{db: db, manager: manager} do
      # If both options are provided, explicit batch_size should take precedence
      # We can't directly verify which is used, but the function should accept both
      graph = RDF.Graph.new()

      assert {:ok, 0} =
               Loader.load_graph(db, manager, graph, batch_size: 7_500, memory_budget: :low)
    end
  end

  # ===========================================================================
  # Batch Size Bounds Validation
  # ===========================================================================

  describe "batch size bounds" do
    setup do
      test_path = "#{@test_db_base}_bounds_#{:erlang.unique_integer([:positive])}"
      {:ok, db} = NIF.open(test_path)
      {:ok, manager} = Manager.start_link(db: db)

      on_exit(fn ->
        if Process.alive?(manager), do: Manager.stop(manager)
        NIF.close(db)
        File.rm_rf(test_path)
      end)

      {:ok, db: db, manager: manager}
    end

    test "accepts batch size at minimum bound", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      # Should clamp to min, not error
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, batch_size: 100)
    end

    test "accepts batch size at maximum bound", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      # Should clamp to max, not error
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, batch_size: 100_000)
    end

    test "accepts batch size below minimum (clamped)", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      # Should clamp to min, not error
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, batch_size: 50)
    end

    test "accepts batch size above maximum (clamped)", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      # Should clamp to max, not error
      assert {:ok, 0} = Loader.load_graph(db, manager, graph, batch_size: 200_000)
    end
  end

  # ===========================================================================
  # Memory Budget Selection
  # ===========================================================================

  describe "memory budget batch sizes" do
    test ":low budget is appropriate for memory-constrained systems" do
      batch_size = Loader.optimal_batch_size(:low)
      # 5,000 triples × 72 bytes ≈ 360KB
      estimated_memory = batch_size * 72
      # Less than 500KB
      assert estimated_memory < 500_000
    end

    test ":medium budget is balanced for typical systems" do
      batch_size = Loader.optimal_batch_size(:medium)
      # 10,000 triples × 72 bytes ≈ 720KB
      estimated_memory = batch_size * 72
      # Less than 1MB
      assert estimated_memory < 1_000_000
    end

    test ":high budget for systems with ample memory" do
      batch_size = Loader.optimal_batch_size(:high)
      # 50,000 triples × 72 bytes ≈ 3.6MB
      estimated_memory = batch_size * 72
      # Less than 5MB
      assert estimated_memory < 5_000_000
    end
  end
end
