defmodule TripleStore.Index.TripleDeleteTest do
  @moduledoc """
  Tests for Task 1.4.3: Triple Delete for Triple Index Layer.

  Verifies that:
  - Single triple deletion works correctly
  - Batch triple deletion works correctly
  - All three indices (SPO, POS, OSP) are deleted atomically
  - Deletion of non-existent triple is a no-op (idempotent)
  - Edge cases are handled properly
  """

  use TripleStore.PooledDbCase

  alias TripleStore.Index

  # ===========================================================================
  # delete_triple/2
  # ===========================================================================

  describe "delete_triple/2" do
    test "deletes an existing triple", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})

      assert :ok = Index.delete_triple(db, {1, 2, 3})
      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "removes from SPO index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})
      :ok = Index.delete_triple(db, {100, 200, 300})

      spo_key = Index.spo_key(100, 200, 300)
      assert :not_found = NIF.get(db, :spo, spo_key)
    end

    test "removes from POS index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})
      :ok = Index.delete_triple(db, {100, 200, 300})

      pos_key = Index.pos_key(200, 300, 100)
      assert :not_found = NIF.get(db, :pos, pos_key)
    end

    test "removes from OSP index", %{db: db} do
      :ok = Index.insert_triple(db, {100, 200, 300})
      :ok = Index.delete_triple(db, {100, 200, 300})

      osp_key = Index.osp_key(300, 100, 200)
      assert :not_found = NIF.get(db, :osp, osp_key)
    end

    test "removes from all three indices atomically", %{db: db} do
      :ok = Index.insert_triple(db, {10, 20, 30})
      :ok = Index.delete_triple(db, {10, 20, 30})

      # Verify all three indices are empty
      spo_key = Index.spo_key(10, 20, 30)
      pos_key = Index.pos_key(20, 30, 10)
      osp_key = Index.osp_key(30, 10, 20)

      assert :not_found = NIF.get(db, :spo, spo_key)
      assert :not_found = NIF.get(db, :pos, pos_key)
      assert :not_found = NIF.get(db, :osp, osp_key)
    end

    test "is idempotent - deleting non-existent triple succeeds", %{db: db} do
      # Triple was never inserted
      assert :ok = Index.delete_triple(db, {999, 999, 999})
    end

    test "is idempotent - double deletion succeeds", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      assert :ok = Index.delete_triple(db, {1, 2, 3})
      assert :ok = Index.delete_triple(db, {1, 2, 3})

      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "only deletes specified triple", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      :ok = Index.insert_triple(db, {1, 2, 4})
      :ok = Index.insert_triple(db, {1, 3, 3})

      :ok = Index.delete_triple(db, {1, 2, 3})

      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 4})
      assert {:ok, true} = Index.triple_exists?(db, {1, 3, 3})
    end

    test "handles zero IDs", %{db: db} do
      :ok = Index.insert_triple(db, {0, 0, 0})
      assert {:ok, true} = Index.triple_exists?(db, {0, 0, 0})

      :ok = Index.delete_triple(db, {0, 0, 0})
      assert {:ok, false} = Index.triple_exists?(db, {0, 0, 0})
    end

    test "handles large IDs", %{db: db} do
      import Bitwise
      max = (1 <<< 62) - 1

      :ok = Index.insert_triple(db, {max, max, max})
      assert {:ok, true} = Index.triple_exists?(db, {max, max, max})

      :ok = Index.delete_triple(db, {max, max, max})
      assert {:ok, false} = Index.triple_exists?(db, {max, max, max})
    end
  end

  # ===========================================================================
  # delete_triples/2
  # ===========================================================================

  describe "delete_triples/2" do
    test "deletes empty list", %{db: db} do
      assert :ok = Index.delete_triples(db, [])
    end

    test "deletes single triple in list", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      assert :ok = Index.delete_triples(db, [{1, 2, 3}])
      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "deletes multiple triples", %{db: db} do
      triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      :ok = Index.insert_triples(db, triples)

      assert :ok = Index.delete_triples(db, triples)

      for triple <- triples do
        assert {:ok, false} = Index.triple_exists?(db, triple)
      end
    end

    test "removes all three indices for each triple", %{db: db} do
      triples = [{10, 20, 30}, {40, 50, 60}]
      :ok = Index.insert_triples(db, triples)
      :ok = Index.delete_triples(db, triples)

      for {s, p, o} <- triples do
        assert :not_found = NIF.get(db, :spo, Index.spo_key(s, p, o))
        assert :not_found = NIF.get(db, :pos, Index.pos_key(p, o, s))
        assert :not_found = NIF.get(db, :osp, Index.osp_key(o, s, p))
      end
    end

    test "handles duplicates in batch", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})

      # Delete same triple multiple times in one batch
      triples = [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
      assert :ok = Index.delete_triples(db, triples)

      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "handles mix of existing and non-existing triples", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      # {4, 5, 6} is never inserted

      triples = [{1, 2, 3}, {4, 5, 6}]
      assert :ok = Index.delete_triples(db, triples)

      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
      assert {:ok, false} = Index.triple_exists?(db, {4, 5, 6})
    end

    test "handles large batch", %{db: db} do
      triples =
        for s <- 1..100 do
          {s, s + 1000, s + 2000}
        end

      :ok = Index.insert_triples(db, triples)

      # Verify some exist
      assert {:ok, true} = Index.triple_exists?(db, {1, 1001, 2001})
      assert {:ok, true} = Index.triple_exists?(db, {100, 1100, 2100})

      :ok = Index.delete_triples(db, triples)

      # Verify all deleted
      assert {:ok, false} = Index.triple_exists?(db, {1, 1001, 2001})
      assert {:ok, false} = Index.triple_exists?(db, {50, 1050, 2050})
      assert {:ok, false} = Index.triple_exists?(db, {100, 1100, 2100})
    end

    test "creates correct number of delete operations", %{db: db} do
      # Each triple requires 3 delete operations (SPO, POS, OSP)
      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = Index.insert_triples(db, triples)
      :ok = Index.delete_triples(db, triples)

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

      assert Enum.empty?(spo_entries)
      assert Enum.empty?(pos_entries)
      assert Enum.empty?(osp_entries)
    end
  end

  # ===========================================================================
  # Index Consistency After Delete
  # ===========================================================================

  describe "index consistency after delete" do
    test "prefix queries return empty after deletion", %{db: db} do
      # Insert triples with same subject
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})

      # Delete one triple
      :ok = Index.delete_triple(db, {1, 10, 100})

      # Query by subject prefix
      prefix = Index.spo_prefix(1)
      {:ok, iter} = NIF.prefix_iterator(db, :spo, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      # Should find exactly 1 triple
      assert length(entries) == 1

      # Decode and verify
      [{key, _value}] = entries
      assert Index.decode_spo_key(key) == {1, 20, 200}
    end

    test "POS prefix reflects deletion", %{db: db} do
      :ok = Index.insert_triple(db, {1, 100, 1000})
      :ok = Index.insert_triple(db, {2, 100, 2000})

      :ok = Index.delete_triple(db, {1, 100, 1000})

      # Query by predicate prefix
      prefix = Index.pos_prefix(100)
      {:ok, iter} = NIF.prefix_iterator(db, :pos, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      assert length(entries) == 1
      [{key, _}] = entries
      assert Index.key_to_triple(:pos, key) == {2, 100, 2000}
    end

    test "OSP prefix reflects deletion", %{db: db} do
      :ok = Index.insert_triple(db, {1, 10, 999})
      :ok = Index.insert_triple(db, {2, 20, 999})

      :ok = Index.delete_triple(db, {1, 10, 999})

      # Query by object prefix
      prefix = Index.osp_prefix(999)
      {:ok, iter} = NIF.prefix_iterator(db, :osp, prefix)
      {:ok, entries} = NIF.iterator_collect(iter)
      NIF.iterator_close(iter)

      assert length(entries) == 1
      [{key, _}] = entries
      assert Index.key_to_triple(:osp, key) == {2, 20, 999}
    end
  end

  # ===========================================================================
  # Insert/Delete Interleaving
  # ===========================================================================

  describe "insert/delete interleaving" do
    test "can re-insert after deletion", %{db: db} do
      :ok = Index.insert_triple(db, {1, 2, 3})
      :ok = Index.delete_triple(db, {1, 2, 3})
      :ok = Index.insert_triple(db, {1, 2, 3})

      assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})
    end

    test "multiple insert/delete cycles", %{db: db} do
      for _ <- 1..5 do
        :ok = Index.insert_triple(db, {1, 2, 3})
        assert {:ok, true} = Index.triple_exists?(db, {1, 2, 3})

        :ok = Index.delete_triple(db, {1, 2, 3})
        assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
      end
    end

    test "interleaved batch operations", %{db: db} do
      batch1 = [{1, 2, 3}, {4, 5, 6}]
      batch2 = [{7, 8, 9}, {10, 11, 12}]

      :ok = Index.insert_triples(db, batch1)
      :ok = Index.insert_triples(db, batch2)
      :ok = Index.delete_triples(db, batch1)

      # batch1 should be deleted
      assert {:ok, false} = Index.triple_exists?(db, {1, 2, 3})
      assert {:ok, false} = Index.triple_exists?(db, {4, 5, 6})

      # batch2 should still exist
      assert {:ok, true} = Index.triple_exists?(db, {7, 8, 9})
      assert {:ok, true} = Index.triple_exists?(db, {10, 11, 12})
    end
  end
end
