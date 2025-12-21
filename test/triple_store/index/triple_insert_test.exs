defmodule TripleStore.Index.TripleInsertTest do
  @moduledoc """
  Tests for Task 1.4.2: Triple Insert for Triple Index Layer.

  Verifies that:
  - Single triple insertion works correctly
  - Batch triple insertion works correctly
  - All three indices (SPO, POS, OSP) are written atomically
  - Duplicate insertion is idempotent
  - Triple existence checking works
  - Edge cases are handled properly
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @test_db_base "/tmp/triple_store_index_insert_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, path: test_path}
  end

  # ===========================================================================
  # insert_triple/2
  # ===========================================================================

  describe "insert_triple/2" do
    test "inserts a single triple", %{db: db} do
      assert :ok = Index.insert_triple(db, {1, 2, 3})
    end

    test "writes to SPO index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})

      spo_key = Index.spo_key(100, 200, 300)
      assert {:ok, <<>>} = NIF.get(db, :spo, spo_key)
    end

    test "writes to POS index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})

      pos_key = Index.pos_key(200, 300, 100)
      assert {:ok, <<>>} = NIF.get(db, :pos, pos_key)
    end

    test "writes to OSP index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})

      osp_key = Index.osp_key(300, 100, 200)
      assert {:ok, <<>>} = NIF.get(db, :osp, osp_key)
    end

    test "writes to all three indices atomically", %{db: db} do
      :ok = Index.insert_triple(db, {10, 20, 30})

      # Verify all three indices have the entry
      spo_key = Index.spo_key(10, 20, 30)
      pos_key = Index.pos_key(20, 30, 10)
      osp_key = Index.osp_key(30, 10, 20)

      assert {:ok, <<>>} = NIF.get(db, :spo, spo_key)
      assert {:ok, <<>>} = NIF.get(db, :pos, pos_key)
      assert {:ok, <<>>} = NIF.get(db, :osp, osp_key)
    end

    test "is idempotent - duplicate insertion succeeds", %{db: db} do
      # Insert same triple twice
      assert :ok = Index.insert_triple(db, {1, 2, 3})
      assert :ok = Index.insert_triple(db, {1, 2, 3})

      # Should still exist once
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "uses empty value for index entries", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      spo_key = Index.spo_key(1, 2, 3)
      {:ok, value} = NIF.get(db, :spo, spo_key)

      assert value == <<>>
      assert byte_size(value) == 0
    end

    test "handles zero IDs", %{db: db} do
      assert :ok = Index.insert_triple(db, {0, 0, 0})
      assert {:ok, true} = Index.triple_exists?(db, {0, 0, 0})
    end

    test "handles large IDs", %{db: db} do
      import Bitwise
      max = (1 <<< 62) - 1  # Large 62-bit value (within 64-bit range)

      assert :ok = Index.insert_triple(db, {max, max, max})
      assert {:ok, true} = Index.triple_exists?(db, {max, max, max})
    end

    test "handles mixed zero and non-zero IDs", %{db: db} do
      test_cases = [
        {0, 1, 2},
        {1, 0, 2},
        {1, 2, 0},
        {0, 0, 1},
        {0, 1, 0},
        {1, 0, 0}
      ]

      for triple <- test_cases do
        assert :ok = Index.insert_triple(db, triple)
        assert {:ok, true} = Index.triple_exists?(db, triple)
      end
    end
  end

  # ===========================================================================
  # insert_triples/2
  # ===========================================================================

  describe "insert_triples/2" do
    test "inserts empty list", %{db: db} do
      assert :ok = Index.insert_triples(db, [])
    end

    test "inserts single triple in list", %{db: db} do
      assert :ok = Index.insert_triples(db, [{1, 2, 3}])
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "inserts multiple triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]

      assert :ok = Index.insert_triples(db, triples)

      for triple <- triples do
        assert {:ok, true} = Index.triple_exists?(db, triple)
      end
    end

    test "writes all three indices for each triple", %{db: db} do
      triples = [{10, 20, 30}, {40, 50, 60}]

      :ok = Index.insert_triples(db, triples)

      for {s, p, o} <- triples do
        assert {:ok, <<>>} = NIF.get(db, :spo, Index.spo_key(s, p, o))
        assert {:ok, <<>>} = NIF.get(db, :pos, Index.pos_key(p, o, s))
        assert {:ok, <<>>} = NIF.get(db, :osp, Index.osp_key(o, s, p))
      end
    end

    test "handles duplicates in batch", %{db: db} do
      triples = [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}]

      assert :ok = Index.insert_triples(db, triples)
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "handles large batch", %{db: db} do
      triples =
        for s <- 1..100 do
          {s, s + 1000, s + 2000}
        end

      assert :ok = Index.insert_triples(db, triples)

      # Verify a sample of triples
      assert {:ok, true} = Index.triple_exists?(db, {1, 1001, 2001})
      assert {:ok, true} = Index.triple_exists?(db, {50, 1050, 2050})
      assert {:ok, true} = Index.triple_exists?(db, {100, 1100, 2100})
    end

    test "creates correct number of operations", %{db: db} do
      # Each triple creates 3 index entries (SPO, POS, OSP)
      triples = [{1, 2, 3}, {4, 5, 6}]

      :ok = Index.insert_triples(db, triples)

      # Count entries in each index using prefix iteration
      {:ok, spo_iter} = NIF.prefix_iterator(db, :spo, <<>>)
      {:ok, spo_entries} = NIF.iterator_collect(spo_iter)
      NIF.iterator_close(spo_iter)

      {:ok, pos_iter} = NIF.prefix_iterator(db, :pos, <<>>)
      {:ok, pos_entries} = NIF.iterator_collect(pos_iter)
      NIF.iterator_close(pos_iter)

      {:ok, osp_iter} = NIF.prefix_iterator(db, :osp, <<>>)
      {:ok, osp_entries} = NIF.iterator_collect(osp_iter)
      NIF.iterator_close(osp_iter)

      assert length(spo_entries) == 2
      assert length(pos_entries) == 2
      assert length(osp_entries) == 2
    end
  end

  # ===========================================================================
  # triple_exists?/2
  # ===========================================================================

  describe "triple_exists?/2" do
    test "returns false for non-existent triple", %{db: db} do
      assert {:ok, false} = Index.triple_exists?(db, {999, 999, 999})
    end

    test "returns true for existing triple", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "distinguishes between different triples", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 4})
      assert {:ok, false} = Index.triple_exists?(db, {1, 3, 3})
      assert {:ok, false} = Index.triple_exists?(db, {2, 2, 3})
    end

    test "works after batch insert", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = Index.insert_triples(db, triples)

      for triple <- triples do
        assert {:ok, true} = Index.triple_exists?(db, triple)
      end

      assert {:ok, false} = Index.triple_exists?(db, {10, 11, 12})
    end
  end

  # ===========================================================================
  # Index Consistency
  # ===========================================================================

  describe "index consistency" do
    test "all indices contain same logical triple", %{db: db} do
      s = 100
      p = 200
      o = 300

      :ok = Index.insert_triple(db, {s, p, o})

      # Decode from each index and verify they represent the same triple
      spo_key = Index.spo_key(s, p, o)
      pos_key = Index.pos_key(p, o, s)
      osp_key = Index.osp_key(o, s, p)

      # All should decode to the same canonical triple
      assert Index.key_to_triple(:spo, spo_key) == {s, p, o}
      assert Index.key_to_triple(:pos, pos_key) == {s, p, o}
      assert Index.key_to_triple(:osp, osp_key) == {s, p, o}
    end

    test "prefixes work correctly for inserted triples", %{db: db} do
      # Insert triples with same subject
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})
      :ok = Index.insert_triple(db, {2, 10, 100})

      # Query by subject prefix
      prefix = Index.spo_prefix(1)
      {:ok, iter} = NIF.prefix_iterator(db, :spo, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      # Should find exactly 2 triples with subject 1
      assert length(entries) == 2

      # Decode and verify
      triples = Enum.map(entries, fn {key, _value} -> Index.decode_spo_key(key) end)
      assert {1, 10, 100} in triples
      assert {1, 20, 200} in triples
    end

    test "POS prefix works for predicate queries", %{db: db} do
      # Insert triples with same predicate
      :ok = Index.insert_triple(db, {1, 100, 1000})
      :ok = Index.insert_triple(db, {2, 100, 2000})
      :ok = Index.insert_triple(db, {3, 200, 3000})

      # Query by predicate prefix
      prefix = Index.pos_prefix(100)
      {:ok, iter} = NIF.prefix_iterator(db, :pos, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      # Should find exactly 2 triples with predicate 100
      assert length(entries) == 2

      # Decode and verify
      triples = Enum.map(entries, fn {key, _value} -> Index.key_to_triple(:pos, key) end)
      assert {1, 100, 1000} in triples
      assert {2, 100, 2000} in triples
    end

    test "OSP prefix works for object queries", %{db: db} do
      # Insert triples with same object
      :ok = Index.insert_triple(db, {1, 10, 999})
      :ok = Index.insert_triple(db, {2, 20, 999})
      :ok = Index.insert_triple(db, {3, 30, 888})

      # Query by object prefix
      prefix = Index.osp_prefix(999)
      {:ok, iter} = NIF.prefix_iterator(db, :osp, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      # Should find exactly 2 triples with object 999
      assert length(entries) == 2

      # Decode and verify
      triples = Enum.map(entries, fn {key, _value} -> Index.key_to_triple(:osp, key) end)
      assert {1, 10, 999} in triples
      assert {2, 20, 999} in triples
    end
  end

  # ===========================================================================
  # Lexicographic Ordering
  # ===========================================================================

  describe "lexicographic ordering in indices" do
    test "SPO index orders by subject first", %{db: db} do
      :ok = Index.insert_triple(db, {3, 1, 1})
      :ok = Index.insert_triple(db, {1, 9, 9})
      :ok = Index.insert_triple(db, {2, 5, 5})

      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      triples = Enum.map(entries, fn {key, _} -> Index.decode_spo_key(key) end)

      assert Enum.at(triples, 0) == {1, 9, 9}
      assert Enum.at(triples, 1) == {2, 5, 5}
      assert Enum.at(triples, 2) == {3, 1, 1}
    end

    test "POS index orders by predicate first", %{db: db} do
      :ok = Index.insert_triple(db, {9, 3, 1})
      :ok = Index.insert_triple(db, {9, 1, 9})
      :ok = Index.insert_triple(db, {9, 2, 5})

      {:ok, iter} = NIF.prefix_iterator(db, :pos, <<>>)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      triples = Enum.map(entries, fn {key, _} -> Index.key_to_triple(:pos, key) end)

      # Should be ordered by predicate
      predicates = Enum.map(triples, fn {_, p, _} -> p end)
      assert predicates == [1, 2, 3]
    end

    test "OSP index orders by object first", %{db: db} do
      :ok = Index.insert_triple(db, {1, 1, 3})
      :ok = Index.insert_triple(db, {9, 9, 1})
      :ok = Index.insert_triple(db, {5, 5, 2})

      {:ok, iter} = NIF.prefix_iterator(db, :osp, <<>>)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      triples = Enum.map(entries, fn {key, _} -> Index.key_to_triple(:osp, key) end)

      # Should be ordered by object
      objects = Enum.map(triples, fn {_, _, o} -> o end)
      assert objects == [1, 2, 3]
    end
  end
end
