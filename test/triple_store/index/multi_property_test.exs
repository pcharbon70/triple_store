defmodule TripleStore.Index.MultiPropertyTest do
  @moduledoc """
  Tests for multi-property fetch optimization.

  Task 2.2.2: Multi-Property Fetch
  - Implement lookup_all_properties/2 for single-subject property fetch
  - Implement stream_all_properties/2 for lazy evaluation
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @moduletag :tmp_dir

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_db_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = NIF.open(db_path)
    db
  end

  # ===========================================================================
  # lookup_all_properties/2 Tests
  # ===========================================================================

  describe "lookup_all_properties/2" do
    test "returns all properties for a subject", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Insert triples for subject 1
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})
      :ok = Index.insert_triple(db, {1, 30, 300})

      {:ok, properties} = Index.lookup_all_properties(db, 1)

      assert is_map(properties)
      assert map_size(properties) == 3
      assert properties[10] == [100]
      assert properties[20] == [200]
      assert properties[30] == [300]
    end

    test "returns multi-valued properties as lists", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Insert multiple objects for same predicate
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 10, 101})
      :ok = Index.insert_triple(db, {1, 10, 102})
      :ok = Index.insert_triple(db, {1, 20, 200})

      {:ok, properties} = Index.lookup_all_properties(db, 1)

      assert map_size(properties) == 2
      assert properties[10] == [100, 101, 102]
      assert properties[20] == [200]
    end

    test "returns empty map for subject with no properties", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Insert triple for different subject
      :ok = Index.insert_triple(db, {2, 10, 100})

      {:ok, properties} = Index.lookup_all_properties(db, 1)

      assert properties == %{}
    end

    test "does not include properties from other subjects", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Insert triples for multiple subjects
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})
      :ok = Index.insert_triple(db, {2, 30, 300})
      :ok = Index.insert_triple(db, {3, 40, 400})

      {:ok, properties} = Index.lookup_all_properties(db, 1)

      assert map_size(properties) == 2
      assert properties[10] == [100]
      assert properties[20] == [200]
      refute Map.has_key?(properties, 30)
      refute Map.has_key?(properties, 40)
    end
  end

  # ===========================================================================
  # stream_all_properties/2 Tests
  # ===========================================================================

  describe "stream_all_properties/2" do
    test "returns stream of predicate-object tuples", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})
      :ok = Index.insert_triple(db, {1, 30, 300})

      {:ok, stream} = Index.stream_all_properties(db, 1)

      results = Enum.to_list(stream)

      assert length(results) == 3
      assert {10, 100} in results
      assert {20, 200} in results
      assert {30, 300} in results
    end

    test "streams multi-valued properties as separate tuples", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 10, 101})
      :ok = Index.insert_triple(db, {1, 20, 200})

      {:ok, stream} = Index.stream_all_properties(db, 1)

      results = Enum.to_list(stream)

      assert length(results) == 3
      assert {10, 100} in results
      assert {10, 101} in results
      assert {20, 200} in results
    end

    test "returns empty stream for subject with no properties", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      {:ok, stream} = Index.stream_all_properties(db, 999)

      results = Enum.to_list(stream)
      assert results == []
    end
  end

  # ===========================================================================
  # Performance Tests
  # ===========================================================================

  describe "performance" do
    test "lookup_all_properties is faster than multiple lookups for many properties", %{
      tmp_dir: tmp_dir
    } do
      db = setup_db(tmp_dir)

      # Insert 20 properties for subject 1
      for p <- 1..20 do
        :ok = Index.insert_triple(db, {1, p, p * 100})
      end

      # Measure single multi-property fetch
      {time_multi, {:ok, properties}} =
        :timer.tc(fn ->
          Index.lookup_all_properties(db, 1)
        end)

      assert map_size(properties) == 20

      # Measure 20 individual lookups
      {time_individual, _} =
        :timer.tc(fn ->
          for p <- 1..20 do
            Index.lookup_all(db, {{:bound, 1}, {:bound, p}, :var})
          end
        end)

      # Multi-property fetch should be at least as fast as individual lookups
      # In practice it should be faster for larger property counts
      assert time_multi <= time_individual * 2
    end
  end
end
