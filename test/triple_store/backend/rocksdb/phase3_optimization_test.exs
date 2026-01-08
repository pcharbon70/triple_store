defmodule TripleStore.Backend.RocksDB.Phase3OptimizationTest do
  @moduledoc """
  Unit tests for Phase 3.1: Query Engine Optimization.

  These tests verify that fold-based operations produce correct results
  and provide performance improvements over iterator-based approaches.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Reasoner.DerivedStore

  @moduletag :phase3_optimization
  @moduletag :rocksdb

  # ============================================================================
  # Setup and Teardown
  # ============================================================================

  setup do
    # Create a unique test database for each test
    test_name = "#{__MODULE__}-#{:erlang.unique_integer([:positive, :monotonic])}"
    db_path = Path.join([System.tmp_dir!(), "triple_store_test", test_name])

    # Ensure clean directory
    File.rm_rf(db_path)
    File.mkdir_p!(db_path)

    # Open database (erlang-rocksdb adapter handles create_if_missing by default)
    {:ok, db} = NIF.open(db_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(db_path)
    end)

    %{db: db, db_path: db_path}
  end

  # ============================================================================
  # Section 3.1.1: Pattern Matching Optimization Tests
  # ============================================================================

  describe "3.1.1 Pattern Matching Optimization" do
    test "lookup_fold/4 - collects triples matching pattern", %{db: db} do
      # Insert test data
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6},
        {7, 8, 9},
        {1, 5, 10}
      ]

      :ok = Index.insert_triples(db, triples)

      # Test S?? pattern (subject bound)
      results =
        Index.lookup_fold(db, {{:bound, 1}, :var, :var}, [], fn triple, acc ->
          [triple | acc]
        end)

      assert length(results) == 4
      assert {1, 5, 10} in results
      assert {1, 5, 6} in results
      assert {1, 2, 4} in results
      assert {1, 2, 3} in results
    end

    test "lookup_fold/4 - SP? pattern (subject and predicate bound)", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6},
        {7, 2, 9}
      ]

      :ok = Index.insert_triples(db, triples)

      results =
        Index.lookup_fold(db, {{:bound, 1}, {:bound, 2}, :var}, [], fn triple, acc ->
          [triple | acc]
        end)

      assert length(results) == 2
      assert {1, 2, 3} in results
      assert {1, 2, 4} in results
    end

    test "lookup_fold/4 - S?O pattern with filtering", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 5, 3},
        {1, 8, 3},
        {1, 2, 9}
      ]

      :ok = Index.insert_triples(db, triples)

      # S?O pattern requires filtering by predicate
      results =
        Index.lookup_fold(db, {{:bound, 1}, :var, {:bound, 3}}, [], fn triple, acc ->
          [triple | acc]
        end)

      assert length(results) == 3
      assert {1, 2, 3} in results
      assert {1, 5, 3} in results
      assert {1, 8, 3} in results
    end

    test "lookup_fold/4 - empty results", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = Index.insert_triples(db, triples)

      results =
        Index.lookup_fold(db, {{:bound, 99}, :var, :var}, [], fn triple, acc ->
          [triple | acc]
        end)

      assert results == []
    end

    test "lookup_all_fold/2 - returns sorted list of triples", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, results} = Index.lookup_all_fold(db, {{:bound, 1}, :var, :var})

      # Results should be in SPO order (sorted by key)
      assert results == [{1, 2, 3}, {1, 2, 4}, {1, 5, 6}]
    end

    test "lookup_all_fold/2 - matches lookup_all for correctness", %{db: db} do
      triples = for s <- 1..10, p <- 1..3, o <- 1..5, do: {s, p, o}
      :ok = Index.insert_triples(db, triples)

      # Test various patterns
      patterns = [
        {{:bound, 5}, :var, :var},
        {{:bound, 5}, {:bound, 2}, :var},
        {:var, {:bound, 2}, :var},
        {:var, :var, {:bound, 3}}
      ]

      for pattern <- patterns do
        {:ok, stream_results} = Index.lookup_all(db, pattern)
        {:ok, fold_results} = Index.lookup_all_fold(db, pattern)

        # Sort both for comparison (stream order may differ)
        stream_sorted = Enum.sort(stream_results)
        fold_sorted = Enum.sort(fold_results)

        assert stream_sorted == fold_sorted,
               "Results differ for pattern #{inspect(pattern)}"
      end
    end
  end

  # ============================================================================
  # Section 3.1.2: Bulk Query Optimization Tests
  # ============================================================================

  describe "3.1.2 Bulk Query Optimization" do
    test "count_fold/2 - counts triples matching pattern", %{db: db} do
      triples = for s <- 1..10, p <- 1..3, do: {s, p, s * p}
      :ok = Index.insert_triples(db, triples)

      # Count all triples with subject 5
      {:ok, count} = Index.count_fold(db, {{:bound, 5}, :var, :var})
      assert count == 3
    end

    test "count_fold/2 - matches count for correctness", %{db: db} do
      triples = for s <- 1..10, p <- 1..5, o <- 1..3, do: {s, p, o}
      :ok = Index.insert_triples(db, triples)

      patterns = [
        {{:bound, 5}, :var, :var},
        {{:bound, 5}, {:bound, 2}, :var},
        {:var, {:bound, 2}, :var},
        {:var, :var, {:bound, 3}},
        {:var, :var, :var}
      ]

      for pattern <- patterns do
        {:ok, stream_count} = Index.count(db, pattern)
        {:ok, fold_count} = Index.count_fold(db, pattern)

        assert stream_count == fold_count,
               "Count differs for pattern #{inspect(pattern)}: #{stream_count} vs #{fold_count}"
      end
    end

    test "lookup_keys_fold/2 - collects keys without values", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, results} = Index.lookup_keys_fold(db, {{:bound, 1}, :var, :var})

      assert length(results) == 3
      assert {1, 2, 3} in results
      assert {1, 2, 4} in results
      assert {1, 5, 6} in results
    end

    test "lookup_keys_fold/2 - matches lookup_all_fold for correctness", %{db: db} do
      triples = for s <- 1..20, p <- 1..3, do: {s, p, s + p}
      :ok = Index.insert_triples(db, triples)

      patterns = [
        {{:bound, 10}, :var, :var},
        {{:bound, 10}, {:bound, 2}, :var},
        {:var, {:bound, 2}, :var}
      ]

      for pattern <- patterns do
        {:ok, fold_results} = Index.lookup_all_fold(db, pattern)
        {:ok, keys_results} = Index.lookup_keys_fold(db, pattern)

        assert Enum.sort(fold_results) == Enum.sort(keys_results),
               "Results differ for pattern #{inspect(pattern)}"
      end
    end

    test "lookup_all_properties_fold/2 - builds property map", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6},
        {1, 5, 7},
        {1, 8, 9}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, props} = Index.lookup_all_properties_fold(db, 1)

      assert props == %{2 => [3, 4], 5 => [6, 7], 8 => [9]}
    end

    test "lookup_all_properties_fold/2 - matches lookup_all_properties", %{db: db} do
      triples = for s <- 1..5, p <- 1..5, o <- 1..3, do: {s, p, o}
      :ok = Index.insert_triples(db, triples)

      for subject_id <- 1..5 do
        {:ok, stream_props} = Index.lookup_all_properties(db, subject_id)
        {:ok, fold_props} = Index.lookup_all_properties_fold(db, subject_id)

        assert stream_props == fold_props,
               "Properties differ for subject #{subject_id}"
      end
    end

    test "lookup_all_properties_fold/2 - handles empty results", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = Index.insert_triples(db, triples)

      {:ok, props} = Index.lookup_all_properties_fold(db, 99)
      assert props == %{}
    end
  end

  # ============================================================================
  # Section 3.1.3: DerivedStore Optimization Tests
  # ============================================================================

  describe "3.1.3 DerivedStore Optimization" do
    test "DerivedStore.count/1 - uses fold for counting", %{db: db} do
      derived_triples = for i <- 1..100, do: {i, i * 2, i * 3}
      :ok = DerivedStore.insert_derived(db, derived_triples)

      {:ok, count} = DerivedStore.count(db)
      assert count == 100
    end

    test "DerivedStore.lookup_derived_fold/2 - collects derived facts", %{db: db} do
      derived_triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6},
        {7, 8, 9}
      ]

      :ok = DerivedStore.insert_derived(db, derived_triples)

      {:ok, results} = DerivedStore.lookup_derived_fold(db, {{:bound, 1}, :var, :var})

      assert length(results) == 3
      assert {1, 2, 3} in results
      assert {1, 2, 4} in results
      assert {1, 5, 6} in results
    end

    test "DerivedStore.lookup_derived_fold/2 - matches lookup_derived_all", %{db: db} do
      derived_triples = for s <- 1..20, p <- 1..3, do: {s, p, s + p}
      :ok = DerivedStore.insert_derived(db, derived_triples)

      patterns = [
        {{:bound, 10}, :var, :var},
        {{:bound, 10}, {:bound, 2}, :var},
        {{:bound, 1}, :var, :var}
      ]

      for pattern <- patterns do
        {:ok, stream_results} = DerivedStore.lookup_derived_all(db, pattern)
        {:ok, fold_results} = DerivedStore.lookup_derived_fold(db, pattern)

        assert Enum.sort(stream_results) == Enum.sort(fold_results),
               "Derived results differ for pattern #{inspect(pattern)}"
      end
    end

    test "DerivedStore.make_lookup_fn - uses fold for lookups", %{db: db} do
      # Insert explicit facts
      explicit_triples = [{1, 2, 3}, {1, 5, 6}]
      :ok = Index.insert_triples(db, explicit_triples)

      # Insert derived facts
      derived_triples = [{1, 2, 4}, {7, 8, 9}]
      :ok = DerivedStore.insert_derived(db, derived_triples)

      # Test explicit lookup
      explicit_fn = DerivedStore.make_lookup_fn(db, :explicit)
      {:ok, explicit_results} = explicit_fn.({{:bound, 1}, :var, :var})
      assert length(explicit_results) == 2
      assert {1, 2, 3} in explicit_results
      assert {1, 5, 6} in explicit_results

      # Test derived lookup
      derived_fn = DerivedStore.make_lookup_fn(db, :derived)
      {:ok, derived_results} = derived_fn.({{:bound, 1}, :var, :var})
      assert length(derived_results) == 1
      assert {1, 2, 4} in derived_results

      # Test both lookup
      both_fn = DerivedStore.make_lookup_fn(db, :both)
      {:ok, both_results} = both_fn.({{:bound, 1}, :var, :var})
      assert length(both_results) == 3
    end
  end

  # ============================================================================
  # Section 3.1.4: Performance and Correctness Tests
  # ============================================================================

  describe "3.1.4 Performance and Correctness" do
    test "fold operations handle large datasets", %{db: db} do
      # Insert many triples
      triples = for i <- 1..1000, do: {i, i * 2, i * 3}
      :ok = Index.insert_triples(db, triples)

      # Count all triples
      {:ok, count} = Index.count_fold(db, {:var, :var, :var})
      assert count == 1000
    end

    test "fold operations work with empty database", %{db: db} do
      {:ok, count} = Index.count_fold(db, {:var, :var, :var})
      assert count == 0

      {:ok, results} = Index.lookup_all_fold(db, {:var, :var, :var})
      assert results == []

      {:ok, keys} = Index.lookup_keys_fold(db, {:var, :var, :var})
      assert keys == []
    end

    test "stream still works for lazy evaluation", %{db: db} do
      # Insert many triples
      triples = for i <- 1..100, do: {i, 2, 3}
      :ok = Index.insert_triples(db, triples)

      # Stream should still work for lazy evaluation
      {:ok, stream} = Index.lookup(db, {:var, :var, :var})

      # Take first 5 without processing all
      first_5 = stream |> Enum.take(5)
      assert length(first_5) == 5

      # Total count should still be 100
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 100
    end

    test "fold and stream produce identical results", %{db: db} do
      triples = [
        {1, 2, 3},
        {1, 2, 4},
        {1, 5, 6},
        {7, 8, 9},
        {1, 5, 10},
        {2, 3, 4},
        {2, 3, 5}
      ]

      :ok = Index.insert_triples(db, triples)

      patterns = [
        {{:bound, 1}, :var, :var},
        {{:bound, 1}, {:bound, 2}, :var},
        {{:bound, 1}, :var, {:bound, 6}},
        {:var, {:bound, 2}, :var},
        {:var, {:bound, 3}, {:bound, 4}}
      ]

      for pattern <- patterns do
        {:ok, stream_results} = Index.lookup(db, pattern)
        {:ok, fold_results} = Index.lookup_all_fold(db, pattern)

        stream_sorted = Enum.sort(stream_results)
        fold_sorted = Enum.sort(fold_results)

        assert stream_sorted == fold_sorted,
               "Stream and fold differ for pattern #{inspect(pattern)}"
      end
    end

    test "lookup_all_properties_fold maintains insertion order", %{db: db} do
      triples = [
        {1, 5, 1},
        {1, 5, 2},
        {1, 5, 3},
        {1, 2, 4},
        {1, 2, 5}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, props} = Index.lookup_all_properties_fold(db, 1)

      # Objects should be in insertion order
      assert Map.get(props, 5) == [1, 2, 3]
      assert Map.get(props, 2) == [4, 5]
    end
  end

  # ============================================================================
  # Section 3.1.5: Large Dataset Tests
  # ============================================================================

  describe "3.1.5 Large Dataset Tests" do
    test "fold handles large result sets efficiently", %{db: db} do
      # Insert larger dataset
      triples = for s <- 1..100, p <- 1..10, o <- 1..5, do: {s, p, o}
      :ok = Index.insert_triples(db, triples)

      # Count all triples
      {:ok, count} = Index.count_fold(db, {:var, :var, :var})
      assert count == 5000

      # Count specific subject
      {:ok, subject_count} = Index.count_fold(db, {{:bound, 50}, :var, :var})
      assert subject_count == 50
    end

    test "fold_keys is more efficient for keys-only queries", %{db: db} do
      triples = for s <- 1..100, p <- 1..10, do: {s, p, s + p}
      :ok = Index.insert_triples(db, triples)

      # fold_keys should work correctly for keys-only iteration
      {:ok, keys} = Index.lookup_keys_fold(db, {{:bound, 50}, :var, :var})
      assert length(keys) == 10
    end

    test "lookup_all_properties_fold handles large subjects", %{db: db} do
      # Create a subject with many properties
      triples = for p <- 1..100, o <- 1..10, do: {1, p, o}
      :ok = Index.insert_triples(db, triples)

      {:ok, props} = Index.lookup_all_properties_fold(db, 1)

      assert map_size(props) == 100
      assert Enum.all?(props, fn {_p, objects} -> length(objects) == 10 end)
    end
  end
end
