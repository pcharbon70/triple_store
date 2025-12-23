defmodule TripleStore.Index.IndexLookupTest do
  @moduledoc """
  Tests for Task 1.4.5: Index Lookup for Triple Index Layer.

  Verifies that:
  - lookup/2 returns correct triples for all 8 patterns
  - Results are properly decoded from index keys
  - S?O pattern filtering works correctly
  - lookup_all/2 convenience function works
  - count/2 returns correct counts
  - Edge cases are handled properly
  """

  use TripleStore.PooledDbCase

  alias TripleStore.Index

  setup %{db: db} do
    assert NIF.is_open(db)
    :ok
  end

  # ===========================================================================
  # lookup/2 - Basic Functionality
  # ===========================================================================

  describe "lookup/2 basic functionality" do
    test "returns empty stream for empty database", %{db: db} do
      {:ok, stream} = Index.lookup(db, {:var, :var, :var})
      assert Enum.to_list(stream) == []
    end

    test "returns stream that can be enumerated", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      {:ok, stream} = Index.lookup(db, {:var, :var, :var})
      assert is_function(stream) or is_struct(stream, Stream)

      results = Enum.to_list(stream)
      assert results == [{1, 2, 3}]
    end

    test "stream is lazy", %{db: db} do
      # Insert many triples
      for i <- 1..100 do
        :ok = Index.insert_triple(db, {i, i + 1000, i + 2000})
      end

      {:ok, stream} = Index.lookup(db, {:var, :var, :var})

      # Taking only 5 should not enumerate all 100
      results = Enum.take(stream, 5)
      assert length(results) == 5
    end
  end

  # ===========================================================================
  # lookup/2 - All 8 Patterns
  # ===========================================================================

  describe "lookup/2 pattern: SPO (all bound)" do
    test "finds exact match", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      :ok = Index.insert_triple(db, {4, 5, 6})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, {:bound, 2}, {:bound, 3}})
      assert Enum.to_list(stream) == [{1, 2, 3}]
    end

    test "returns empty for non-existent triple", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      {:ok, stream} = Index.lookup(db, {{:bound, 9}, {:bound, 9}, {:bound, 9}})
      assert Enum.to_list(stream) == []
    end
  end

  describe "lookup/2 pattern: SP? (subject and predicate bound)" do
    test "finds all matching triples", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      :ok = Index.insert_triple(db, {1, 2, 4})
      :ok = Index.insert_triple(db, {1, 2, 5})
      :ok = Index.insert_triple(db, {1, 3, 6})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, {:bound, 2}, :var})
      results = Enum.to_list(stream)

      assert length(results) == 3
      assert {1, 2, 3} in results
      assert {1, 2, 4} in results
      assert {1, 2, 5} in results
    end
  end

  describe "lookup/2 pattern: S?? (only subject bound)" do
    test "finds all triples with subject", %{db: db} do
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})
      :ok = Index.insert_triple(db, {2, 10, 100})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, :var, :var})
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {1, 10, 100} in results
      assert {1, 20, 200} in results
    end
  end

  describe "lookup/2 pattern: ?PO (predicate and object bound)" do
    test "finds all matching triples", %{db: db} do
      :ok = Index.insert_triple(db, {1, 100, 1000})
      :ok = Index.insert_triple(db, {2, 100, 1000})
      :ok = Index.insert_triple(db, {3, 100, 2000})

      {:ok, stream} = Index.lookup(db, {:var, {:bound, 100}, {:bound, 1000}})
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {1, 100, 1000} in results
      assert {2, 100, 1000} in results
    end
  end

  describe "lookup/2 pattern: ?P? (only predicate bound)" do
    test "finds all triples with predicate", %{db: db} do
      :ok = Index.insert_triple(db, {1, 100, 1000})
      :ok = Index.insert_triple(db, {2, 100, 2000})
      :ok = Index.insert_triple(db, {3, 200, 3000})

      {:ok, stream} = Index.lookup(db, {:var, {:bound, 100}, :var})
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {1, 100, 1000} in results
      assert {2, 100, 2000} in results
    end
  end

  describe "lookup/2 pattern: ??O (only object bound)" do
    test "finds all triples with object", %{db: db} do
      :ok = Index.insert_triple(db, {1, 10, 999})
      :ok = Index.insert_triple(db, {2, 20, 999})
      :ok = Index.insert_triple(db, {3, 30, 888})

      {:ok, stream} = Index.lookup(db, {:var, :var, {:bound, 999}})
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {1, 10, 999} in results
      assert {2, 20, 999} in results
    end
  end

  describe "lookup/2 pattern: S?O (subject and object bound - with filter)" do
    test "finds matching triples with correct predicate filtering", %{db: db} do
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 100})
      :ok = Index.insert_triple(db, {1, 30, 100})
      :ok = Index.insert_triple(db, {2, 10, 100})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, :var, {:bound, 100}})
      results = Enum.to_list(stream)

      assert length(results) == 3
      assert {1, 10, 100} in results
      assert {1, 20, 100} in results
      assert {1, 30, 100} in results
      refute {2, 10, 100} in results
    end

    test "returns empty when no match after filter", %{db: db} do
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {2, 20, 200})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, :var, {:bound, 200}})
      assert Enum.to_list(stream) == []
    end
  end

  describe "lookup/2 pattern: ??? (full scan)" do
    test "returns all triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = Index.insert_triples(db, triples)

      {:ok, stream} = Index.lookup(db, {:var, :var, :var})
      results = Enum.to_list(stream)

      assert length(results) == 3

      for triple <- triples do
        assert triple in results
      end
    end
  end

  # ===========================================================================
  # lookup_all/2
  # ===========================================================================

  describe "lookup_all/2" do
    test "returns list of all matching triples", %{db: db} do
      :ok = Index.insert_triples(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])

      {:ok, results} = Index.lookup_all(db, {{:bound, 1}, {:bound, 2}, :var})

      assert is_list(results)
      assert length(results) == 2
      assert {1, 2, 3} in results
      assert {1, 2, 4} in results
    end

    test "returns empty list for no matches", %{db: db} do
      {:ok, results} = Index.lookup_all(db, {{:bound, 999}, :var, :var})
      assert results == []
    end
  end

  # ===========================================================================
  # count/2
  # ===========================================================================

  describe "count/2" do
    test "returns correct count", %{db: db} do
      :ok = Index.insert_triples(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])

      {:ok, count} = Index.count(db, {{:bound, 1}, :var, :var})
      assert count == 3
    end

    test "returns 0 for no matches", %{db: db} do
      {:ok, count} = Index.count(db, {{:bound, 999}, :var, :var})
      assert count == 0
    end

    test "returns 1 for exact match", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      {:ok, count} = Index.count(db, {{:bound, 1}, {:bound, 2}, {:bound, 3}})
      assert count == 1
    end

    test "counts all triples for full scan", %{db: db} do
      :ok = Index.insert_triples(db, [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}])

      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 4
    end
  end

  # ===========================================================================
  # Ordering
  # ===========================================================================

  describe "result ordering" do
    test "SPO pattern returns results in lexicographic order", %{db: db} do
      :ok = Index.insert_triple(db, {1, 3, 300})
      :ok = Index.insert_triple(db, {1, 1, 100})
      :ok = Index.insert_triple(db, {1, 2, 200})

      {:ok, stream} = Index.lookup(db, {{:bound, 1}, :var, :var})
      results = Enum.to_list(stream)

      # Should be ordered by predicate since subject is fixed
      assert results == [{1, 1, 100}, {1, 2, 200}, {1, 3, 300}]
    end

    test "POS pattern returns results in predicate-object-subject order", %{db: db} do
      :ok = Index.insert_triple(db, {3, 100, 1000})
      :ok = Index.insert_triple(db, {1, 100, 1000})
      :ok = Index.insert_triple(db, {2, 100, 1000})

      {:ok, stream} = Index.lookup(db, {:var, {:bound, 100}, {:bound, 1000}})
      results = Enum.to_list(stream)

      # With P and O fixed, should be ordered by subject
      assert results == [{1, 100, 1000}, {2, 100, 1000}, {3, 100, 1000}]
    end

    test "OSP pattern returns results in object-subject-predicate order", %{db: db} do
      :ok = Index.insert_triple(db, {2, 20, 999})
      :ok = Index.insert_triple(db, {1, 10, 999})
      :ok = Index.insert_triple(db, {1, 30, 999})

      {:ok, stream} = Index.lookup(db, {:var, :var, {:bound, 999}})
      results = Enum.to_list(stream)

      # With O fixed, should be ordered by subject, then predicate
      assert results == [{1, 10, 999}, {1, 30, 999}, {2, 20, 999}]
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles zero IDs", %{db: db} do
      :ok = Index.insert_triple(db, {0, 0, 0})

      {:ok, stream} = Index.lookup(db, {{:bound, 0}, {:bound, 0}, {:bound, 0}})
      assert Enum.to_list(stream) == [{0, 0, 0}]
    end

    test "handles large IDs", %{db: db} do
      import Bitwise
      max = (1 <<< 62) - 1

      :ok = Index.insert_triple(db, {max, max, max})

      {:ok, stream} = Index.lookup(db, {{:bound, max}, :var, :var})
      assert Enum.to_list(stream) == [{max, max, max}]
    end

    test "handles many triples efficiently", %{db: db} do
      triples =
        for i <- 1..1000 do
          {i, i + 1000, i + 2000}
        end

      :ok = Index.insert_triples(db, triples)

      # Query for specific subject
      {:ok, stream} = Index.lookup(db, {{:bound, 500}, :var, :var})
      results = Enum.to_list(stream)
      assert results == [{500, 1500, 2500}]

      # Full count
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 1000
    end

    test "multiple queries return consistent results", %{db: db} do
      :ok = Index.insert_triples(db, [{1, 2, 3}, {1, 2, 4}])

      {:ok, results1} = Index.lookup_all(db, {{:bound, 1}, {:bound, 2}, :var})
      {:ok, results2} = Index.lookup_all(db, {{:bound, 1}, {:bound, 2}, :var})

      assert results1 == results2
    end
  end

  # ===========================================================================
  # Integration with Insert/Delete
  # ===========================================================================

  describe "integration with insert/delete" do
    test "lookup reflects insertions", %{db: db} do
      {:ok, count1} = Index.count(db, {:var, :var, :var})
      assert count1 == 0

      :ok = Index.insert_triple(db, {1, 2, 3})

      {:ok, count2} = Index.count(db, {:var, :var, :var})
      assert count2 == 1
    end

    test "lookup reflects deletions", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      :ok = Index.insert_triple(db, {4, 5, 6})

      {:ok, count1} = Index.count(db, {:var, :var, :var})
      assert count1 == 2

      :ok = Index.delete_triple(db, {1, 2, 3})

      {:ok, count2} = Index.count(db, {:var, :var, :var})
      assert count2 == 1

      {:ok, results} = Index.lookup_all(db, {:var, :var, :var})
      assert results == [{4, 5, 6}]
    end
  end
end
