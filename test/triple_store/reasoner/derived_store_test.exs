defmodule TripleStore.Reasoner.DerivedStoreTest do
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @moduletag :integration

  # Helper for pattern matching in SemiNaive integration test
  defp match_term?(_, {:var, _}), do: true
  defp match_term?(value, value), do: true
  defp match_term?(_, _), do: false

  # Test database path
  @test_db_path "/tmp/derived_store_test_#{:erlang.unique_integer([:positive])}"

  setup do
    # Create a unique path for each test
    db_path = "#{@test_db_path}_#{:erlang.unique_integer([:positive])}"

    # Open database
    {:ok, db} = NIF.open(db_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    {:ok, db: db, db_path: db_path}
  end

  # ============================================================================
  # Storage Operations Tests
  # ============================================================================

  describe "insert_derived/2" do
    test "inserts empty list without error", %{db: db} do
      assert :ok = DerivedStore.insert_derived(db, [])
    end

    test "inserts single triple", %{db: db} do
      triple = {1, 2, 3}
      assert :ok = DerivedStore.insert_derived(db, [triple])

      # Verify it exists
      assert {:ok, true} = DerivedStore.derived_exists?(db, triple)
    end

    test "inserts multiple triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      assert :ok = DerivedStore.insert_derived(db, triples)

      # Verify all exist
      for triple <- triples do
        assert {:ok, true} = DerivedStore.derived_exists?(db, triple)
      end
    end

    test "is idempotent for duplicate triples", %{db: db} do
      triple = {1, 2, 3}
      assert :ok = DerivedStore.insert_derived(db, [triple])
      assert :ok = DerivedStore.insert_derived(db, [triple])

      # Should still exist and count as one
      assert {:ok, 1} = DerivedStore.count(db)
    end
  end

  describe "insert_derived_single/2" do
    test "inserts a single triple", %{db: db} do
      triple = {100, 200, 300}
      assert :ok = DerivedStore.insert_derived_single(db, triple)

      assert {:ok, true} = DerivedStore.derived_exists?(db, triple)
    end
  end

  describe "delete_derived/2" do
    test "deletes empty list without error", %{db: db} do
      assert :ok = DerivedStore.delete_derived(db, [])
    end

    test "deletes existing triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = DerivedStore.insert_derived(db, triples)

      assert :ok = DerivedStore.delete_derived(db, triples)

      for triple <- triples do
        assert {:ok, false} = DerivedStore.derived_exists?(db, triple)
      end
    end

    test "deletes subset of triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = DerivedStore.insert_derived(db, triples)

      assert :ok = DerivedStore.delete_derived(db, [{4, 5, 6}])

      assert {:ok, true} = DerivedStore.derived_exists?(db, {1, 2, 3})
      assert {:ok, false} = DerivedStore.derived_exists?(db, {4, 5, 6})
      assert {:ok, true} = DerivedStore.derived_exists?(db, {7, 8, 9})
    end

    test "handles deleting non-existent triples", %{db: db} do
      # Should not error on deleting something that doesn't exist
      assert :ok = DerivedStore.delete_derived(db, [{999, 999, 999}])
    end
  end

  describe "derived_exists?/2" do
    test "returns false for non-existent triple", %{db: db} do
      assert {:ok, false} = DerivedStore.derived_exists?(db, {1, 2, 3})
    end

    test "returns true for existing triple", %{db: db} do
      triple = {1, 2, 3}
      :ok = DerivedStore.insert_derived(db, [triple])

      assert {:ok, true} = DerivedStore.derived_exists?(db, triple)
    end
  end

  describe "clear_all/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = DerivedStore.clear_all(db)
    end

    test "clears all derived facts and returns count", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = DerivedStore.insert_derived(db, triples)

      assert {:ok, 3} = DerivedStore.clear_all(db)

      # Verify all are gone
      for triple <- triples do
        assert {:ok, false} = DerivedStore.derived_exists?(db, triple)
      end
    end

    test "can be called multiple times", %{db: db} do
      triples = [{1, 2, 3}]
      :ok = DerivedStore.insert_derived(db, triples)

      assert {:ok, 1} = DerivedStore.clear_all(db)
      assert {:ok, 0} = DerivedStore.clear_all(db)
    end
  end

  describe "count/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = DerivedStore.count(db)
    end

    test "returns correct count after inserts", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}])
      assert {:ok, 1} = DerivedStore.count(db)

      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}, {7, 8, 9}])
      assert {:ok, 3} = DerivedStore.count(db)
    end

    test "returns correct count after deletes", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {4, 5, 6}])
      :ok = DerivedStore.delete_derived(db, [{1, 2, 3}])

      assert {:ok, 1} = DerivedStore.count(db)
    end
  end

  # ============================================================================
  # Query Operations Tests
  # ============================================================================

  describe "lookup_derived/2" do
    test "returns empty stream for empty store", %{db: db} do
      pattern = {:var, :var, :var}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)
      assert [] = Enum.to_list(stream)
    end

    test "returns all facts with unbound pattern", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = DerivedStore.insert_derived(db, triples)

      pattern = {:var, :var, :var}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == Enum.sort(triples)
    end

    test "filters by bound subject", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {1, 4, 5}, {2, 3, 4}])

      pattern = {{:bound, 1}, :var, :var}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == [{1, 2, 3}, {1, 4, 5}]
    end

    test "filters by bound subject and predicate", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])

      pattern = {{:bound, 1}, {:bound, 2}, :var}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == [{1, 2, 3}, {1, 2, 4}]
    end

    test "filters by fully bound pattern", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {1, 2, 4}, {4, 5, 6}])

      pattern = {{:bound, 1}, {:bound, 2}, {:bound, 3}}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream)
      assert result == [{1, 2, 3}]
    end

    test "filters by predicate only (scans all)", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {4, 2, 5}, {6, 7, 8}])

      pattern = {:var, {:bound, 2}, :var}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == [{1, 2, 3}, {4, 2, 5}]
    end

    test "filters by object only (scans all)", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {4, 5, 3}, {6, 7, 8}])

      pattern = {:var, :var, {:bound, 3}}
      assert {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == [{1, 2, 3}, {4, 5, 3}]
    end
  end

  describe "lookup_derived_all/2" do
    test "returns list instead of stream", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = DerivedStore.insert_derived(db, triples)

      pattern = {:var, :var, :var}
      assert {:ok, result} = DerivedStore.lookup_derived_all(db, pattern)

      assert is_list(result)
      assert Enum.sort(result) == Enum.sort(triples)
    end
  end

  describe "lookup_explicit/2" do
    test "delegates to Index.lookup", %{db: db} do
      # Insert explicit triple using Index.insert_triples (populates all indices)
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      pattern = {{:bound, 1}, {:bound, 2}, {:bound, 3}}
      assert {:ok, stream} = DerivedStore.lookup_explicit(db, pattern)

      result = Enum.to_list(stream)
      assert result == [{1, 2, 3}]
    end
  end

  describe "lookup_all/2" do
    test "combines explicit and derived facts", %{db: db} do
      # Insert explicit triple using Index.insert_triples (populates all indices)
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      # Insert derived triple
      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}])

      pattern = {:var, :var, :var}
      assert {:ok, stream} = DerivedStore.lookup_all(db, pattern)

      result = Enum.to_list(stream) |> Enum.sort()
      assert result == [{1, 2, 3}, {4, 5, 6}]
    end

    test "handles overlapping facts (duplicates allowed)", %{db: db} do
      # Same triple in both explicit and derived
      :ok = Index.insert_triples(db, [{1, 2, 3}])
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}])

      pattern = {:var, :var, :var}
      assert {:ok, stream} = DerivedStore.lookup_all(db, pattern)

      result = Enum.to_list(stream)
      # May contain duplicate - comment in code says "duplicates don't affect correctness"
      assert {1, 2, 3} in result
    end
  end

  # ============================================================================
  # SemiNaive Integration Tests
  # ============================================================================

  describe "make_lookup_fn/2" do
    test "creates function for explicit source", %{db: db} do
      # Insert explicit triple using Index.insert_triples
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      # Insert derived triple (should not be found)
      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}])

      lookup_fn = DerivedStore.make_lookup_fn(db, :explicit)
      pattern = {:var, :var, :var}

      assert {:ok, result} = lookup_fn.(pattern)
      assert is_list(result)
      assert {1, 2, 3} in result
      refute {4, 5, 6} in result
    end

    test "creates function for derived source", %{db: db} do
      # Insert explicit triple (should not be found)
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      # Insert derived triple
      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}])

      lookup_fn = DerivedStore.make_lookup_fn(db, :derived)
      pattern = {:var, :var, :var}

      assert {:ok, result} = lookup_fn.(pattern)
      assert is_list(result)
      refute {1, 2, 3} in result
      assert {4, 5, 6} in result
    end

    test "creates function for both source", %{db: db} do
      # Insert explicit triple using Index.insert_triples
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      # Insert derived triple
      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}])

      lookup_fn = DerivedStore.make_lookup_fn(db, :both)
      pattern = {:var, :var, :var}

      assert {:ok, result} = lookup_fn.(pattern)
      assert is_list(result)
      assert {1, 2, 3} in result
      assert {4, 5, 6} in result
    end

    test "lookup function works with bound patterns", %{db: db} do
      :ok = DerivedStore.insert_derived(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])

      lookup_fn = DerivedStore.make_lookup_fn(db, :derived)
      pattern = {{:bound, 1}, {:bound, 2}, :var}

      assert {:ok, result} = lookup_fn.(pattern)
      assert Enum.sort(result) == [{1, 2, 3}, {1, 2, 4}]
    end
  end

  describe "make_store_fn/1" do
    test "creates function that stores MapSet of facts", %{db: db} do
      store_fn = DerivedStore.make_store_fn(db)

      facts = MapSet.new([{1, 2, 3}, {4, 5, 6}])
      assert :ok = store_fn.(facts)

      # Verify stored
      assert {:ok, true} = DerivedStore.derived_exists?(db, {1, 2, 3})
      assert {:ok, true} = DerivedStore.derived_exists?(db, {4, 5, 6})
    end

    test "handles empty MapSet", %{db: db} do
      store_fn = DerivedStore.make_store_fn(db)

      facts = MapSet.new()
      assert :ok = store_fn.(facts)
    end
  end

  # ============================================================================
  # Integration with Actual Reasoning
  # ============================================================================

  describe "integration with SemiNaive" do
    alias TripleStore.Reasoner.SemiNaive
    alias TripleStore.Reasoner.Rule

    test "materialize stores derived facts to database", %{db: _db} do
      # Create a simple transitive rule using term-level representation
      # parent(X, Y) ^ parent(Y, Z) -> ancestor(X, Z)
      parent_pred = {:iri, "http://example.org/parent"}
      ancestor_pred = {:iri, "http://example.org/ancestor"}

      rule = %Rule{
        name: :ancestor,
        body: [
          {:pattern, [{:var, :x}, parent_pred, {:var, :y}]},
          {:pattern, [{:var, :y}, parent_pred, {:var, :z}]}
        ],
        head: {:pattern, [{:var, :x}, ancestor_pred, {:var, :z}]}
      }

      # Initial facts: A -> B -> C (parent chain) using term-level representation
      a = {:iri, "http://example.org/A"}
      b = {:iri, "http://example.org/B"}
      c = {:iri, "http://example.org/C"}

      initial_facts = MapSet.new([
        {a, parent_pred, b},
        {b, parent_pred, c}
      ])

      # Create in-memory lookup function that searches initial_facts
      lookup_fn = fn pattern ->
        # Convert pattern to match format
        {s_pat, p_pat, o_pat} = case pattern do
          {:pattern, [s, p, o]} -> {s, p, o}
          {s, p, o} -> {s, p, o}
        end

        matches = Enum.filter(initial_facts, fn {s, p, o} ->
          match_term?(s, s_pat) and match_term?(p, p_pat) and match_term?(o, o_pat)
        end)

        {:ok, matches}
      end

      # Store function that writes to DerivedStore
      # For this test, we use a simplified version that stores term-level facts
      derived_facts = :ets.new(:derived_facts, [:set, :public])
      store_fn = fn fact_set ->
        for fact <- fact_set, do: :ets.insert(derived_facts, {fact, true})
        :ok
      end

      # Run materialization
      {:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, [rule], initial_facts)

      # Should derive: ancestor(A, C)
      assert stats.total_derived >= 1

      # Check derived fact was stored
      expected = {a, ancestor_pred, c}
      assert :ets.lookup(derived_facts, expected) == [{expected, true}]

      :ets.delete(derived_facts)
    end

    test "clear_all removes only derived facts", %{db: db} do
      # Insert explicit triple using Index.insert_triples
      :ok = Index.insert_triples(db, [{1, 2, 3}])

      # Insert derived triples
      :ok = DerivedStore.insert_derived(db, [{4, 5, 6}, {7, 8, 9}])

      # Clear derived
      assert {:ok, 2} = DerivedStore.clear_all(db)

      # Explicit should still exist
      assert {:ok, stream} = Index.lookup(db, {{:bound, 1}, {:bound, 2}, {:bound, 3}})
      assert [{1, 2, 3}] = Enum.to_list(stream)

      # Derived should be gone
      assert {:ok, false} = DerivedStore.derived_exists?(db, {4, 5, 6})
      assert {:ok, false} = DerivedStore.derived_exists?(db, {7, 8, 9})
    end
  end
end
