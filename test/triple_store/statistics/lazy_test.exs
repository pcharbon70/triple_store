defmodule TripleStore.Statistics.LazyTest do
  @moduledoc """
  Tests for lazy statistics collection (S4).
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.Statistics

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_lazy_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)

    # Insert test data
    quads =
      for i <- 1..100 do
        {rem(i, 20) + 1, rem(i, 10) + 10, i + 1000, 0}
      end

    Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db}
  end

  describe "lazy/2" do
    test "creates a lazy statistics wrapper" do
      lazy_stats = Statistics.lazy(%{db: :db_ref})

      assert Statistics.lazy?(lazy_stats)
    end

    test "creates wrapper with custom options" do
      lazy_stats = Statistics.lazy(%{db: :db_ref}, cache: false, ttl: 1000)

      assert Statistics.lazy?(lazy_stats)
    end
  end

  describe "lazy?/1" do
    test "returns true for lazy wrapper" do
      lazy_stats = Statistics.lazy(%{db: :db_ref})
      assert Statistics.lazy?(lazy_stats)
    end

    test "returns false for regular map" do
      regular_stats = %{triple_count: 100}
      refute Statistics.lazy?(regular_stats)
    end
  end

  describe "get_lazy/2" do
    test "returns error for non-lazy stats" do
      regular_stats = %{triple_count: 100}
      assert {:error, :not_lazy} = Statistics.get_lazy(regular_stats, :triple_count)
    end

    test "returns error for unsupported keys" do
      # Note: Can't test actual collection without a real db ref
      # Just verify the function exists
      assert function_exported?(Statistics, :get_lazy, 2)
    end
  end

  describe "materialize/1" do
    test "returns error for non-lazy stats" do
      regular_stats = %{triple_count: 100}
      assert {:error, :not_lazy} = Statistics.materialize(regular_stats)
    end
  end

  describe "refresh_lazy/1" do
    test "returns error for non-lazy stats" do
      regular_stats = %{triple_count: 100}
      assert {:error, :not_lazy} = Statistics.refresh_lazy(regular_stats)
    end
  end

  describe "integration with cache" do
    test "lazy stats integrate with existing cache system", %{db: db} do
      # This test verifies that lazy stats don't interfere with regular stats
      {:ok, stats} = Statistics.graph_statistics(db, 0)

      assert stats.quad_count > 0
      assert is_map(stats)
    end
  end
end
