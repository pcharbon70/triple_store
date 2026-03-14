defmodule TripleStore.IndexProtocolTest do
  @moduledoc """
  Tests for IndexProtocol module (S21).
  """

  use ExUnit.Case

  alias TripleStore.IndexProtocol.GOPS
  alias TripleStore.IndexProtocol.GPOS
  alias TripleStore.IndexProtocol.GOSP
  alias TripleStore.IndexProtocol.GSPO
  alias TripleStore.IndexProtocol.OSP
  alias TripleStore.IndexProtocol.OSPG
  alias TripleStore.IndexProtocol.POS
  alias TripleStore.IndexProtocol.POSG
  alias TripleStore.IndexProtocol.SPO
  alias TripleStore.IndexProtocol.SPOG

  alias TripleStore.IndexProtocol.Helpers

  # ===========================================================================
  # Protocol Implementation Tests
  # ===========================================================================

  describe "SPO Index" do
    test "key_size/0 returns 3" do
      assert SPO.key_size() == 3
    end

    test "column_family/0 returns :spo" do
      assert SPO.column_family() == :spo
    end

    test "name/0 returns descriptive name" do
      assert SPO.name() == "Subject-Predicate-Object"
    end

    test "key_order/0 returns component order" do
      assert SPO.key_order() == [:subject, :predicate, :object]
    end

    test "is_quad?/0 returns false" do
      refute SPO.quad?()
    end
  end

  describe "POS Index" do
    test "key_size/0 returns 3" do
      assert POS.key_size() == 3
    end

    test "column_family/0 returns :pos" do
      assert POS.column_family() == :pos
    end

    test "name/0 returns descriptive name" do
      assert POS.name() == "Predicate-Object-Subject"
    end

    test "key_order/0 returns component order" do
      assert POS.key_order() == [:predicate, :object, :subject]
    end

    test "is_quad?/0 returns false" do
      refute POS.quad?()
    end
  end

  describe "OSP Index" do
    test "key_size/0 returns 3" do
      assert OSP.key_size() == 3
    end

    test "column_family/0 returns :osp" do
      assert OSP.column_family() == :osp
    end

    test "name/0 returns descriptive name" do
      assert OSP.name() == "Object-Subject-Predicate"
    end

    test "key_order/0 returns component order" do
      assert OSP.key_order() == [:object, :subject, :predicate]
    end

    test "is_quad?/0 returns false" do
      refute OSP.quad?()
    end
  end

  describe "GSPO Index" do
    test "key_size/0 returns 4" do
      assert GSPO.key_size() == 4
    end

    test "column_family/0 returns :gspo" do
      assert GSPO.column_family() == :gspo
    end

    test "name/0 returns descriptive name" do
      assert GSPO.name() == "Graph-Subject-Predicate-Object"
    end

    test "key_order/0 returns component order" do
      assert GSPO.key_order() == [:graph, :subject, :predicate, :object]
    end

    test "is_quad?/0 returns true" do
      assert GSPO.quad?()
    end
  end

  describe "GPOS Index" do
    test "key_size/0 returns 4" do
      assert GPOS.key_size() == 4
    end

    test "column_family/0 returns :gpos" do
      assert GPOS.column_family() == :gpos
    end

    test "name/0 returns descriptive name" do
      assert GPOS.name() == "Graph-Predicate-Object-Subject"
    end

    test "key_order/0 returns component order" do
      assert GPOS.key_order() == [:graph, :predicate, :object, :subject]
    end

    test "is_quad?/0 returns true" do
      assert GPOS.quad?()
    end
  end

  describe "GOSP Index" do
    test "key_size/0 returns 4" do
      assert GOSP.key_size() == 4
    end

    test "column_family/0 returns :gosp" do
      assert GOSP.column_family() == :gosp
    end

    test "name/0 returns descriptive name" do
      assert GOSP.name() == "Graph-Object-Subject-Predicate"
    end

    test "key_order/0 returns component order" do
      assert GOSP.key_order() == [:graph, :object, :subject, :predicate]
    end

    test "is_quad?/0 returns true" do
      assert GOSP.quad?()
    end
  end

  describe "SPOG Index" do
    test "key_size/0 returns 4" do
      assert SPOG.key_size() == 4
    end

    test "column_family/0 returns :spog" do
      assert SPOG.column_family() == :spog
    end

    test "name/0 returns descriptive name" do
      assert SPOG.name() == "Subject-Predicate-Object-Graph"
    end

    test "key_order/0 returns component order" do
      assert SPOG.key_order() == [:subject, :predicate, :object, :graph]
    end

    test "is_quad?/0 returns true" do
      assert SPOG.quad?()
    end
  end

  describe "POSG Index" do
    test "key_size/0 returns 4" do
      assert POSG.key_size() == 4
    end

    test "column_family/0 returns :posg" do
      assert POSG.column_family() == :posg
    end

    test "name/0 returns descriptive name" do
      assert POSG.name() == "Predicate-Object-Subject-Graph"
    end

    test "key_order/0 returns component order" do
      assert POSG.key_order() == [:predicate, :object, :subject, :graph]
    end

    test "is_quad?/0 returns true" do
      assert POSG.quad?()
    end
  end

  describe "OSPG Index" do
    test "key_size/0 returns 4" do
      assert OSPG.key_size() == 4
    end

    test "column_family/0 returns :ospg" do
      assert OSPG.column_family() == :ospg
    end

    test "name/0 returns descriptive name" do
      assert OSPG.name() == "Object-Subject-Predicate-Graph"
    end

    test "key_order/0 returns component order" do
      assert OSPG.key_order() == [:object, :subject, :predicate, :graph]
    end

    test "is_quad?/0 returns true" do
      assert OSPG.quad?()
    end
  end

  # ===========================================================================
  # Helper Function Tests
  # ===========================================================================

  describe "Helpers.key_size/1" do
    test "returns 3 for triple indices" do
      assert Helpers.key_size(:spo) == 3
      assert Helpers.key_size(:pos) == 3
      assert Helpers.key_size(:osp) == 3
    end

    test "returns 4 for quad indices" do
      assert Helpers.key_size(:gspo) == 4
      assert Helpers.key_size(:gpos) == 4
      assert Helpers.key_size(:gosp) == 4
      assert Helpers.key_size(:spog) == 4
      assert Helpers.key_size(:posg) == 4
      assert Helpers.key_size(:ospg) == 4
    end

    test "returns 0 for unknown indices" do
      assert Helpers.key_size(:unknown) == 0
      assert Helpers.key_size(:invalid) == 0
    end
  end

  describe "Helpers.key_order/1" do
    test "returns key order for triple indices" do
      assert Helpers.key_order(:spo) == [:subject, :predicate, :object]
      assert Helpers.key_order(:pos) == [:predicate, :object, :subject]
      assert Helpers.key_order(:osp) == [:object, :subject, :predicate]
    end

    test "returns key order for quad indices" do
      assert Helpers.key_order(:gspo) == [:graph, :subject, :predicate, :object]
      assert Helpers.key_order(:spog) == [:subject, :predicate, :object, :graph]
    end

    test "returns empty list for unknown indices" do
      assert Helpers.key_order(:unknown) == []
    end
  end

  describe "Helpers.is_quad?/1" do
    test "returns true for quad indices" do
      assert Helpers.quad?(:gspo)
      assert Helpers.quad?(:gpos)
      assert Helpers.quad?(:gosp)
      assert Helpers.quad?(:spog)
      assert Helpers.quad?(:posg)
      assert Helpers.quad?(:ospg)
    end

    test "returns false for triple indices" do
      refute Helpers.quad?(:spo)
      refute Helpers.quad?(:pos)
      refute Helpers.quad?(:osp)
    end

    test "returns false for unknown indices" do
      refute Helpers.quad?(:unknown)
    end
  end

  describe "Helpers.is_triple?/1" do
    test "returns true for triple indices" do
      assert Helpers.triple?(:spo)
      assert Helpers.triple?(:pos)
      assert Helpers.triple?(:osp)
    end

    test "returns false for quad indices" do
      refute Helpers.triple?(:gspo)
      refute Helpers.triple?(:spog)
    end

    test "returns false for unknown indices" do
      refute Helpers.triple?(:unknown)
    end
  end

  describe "Helpers.triple_indices/0" do
    test "returns all triple index types" do
      assert Helpers.triple_indices() == [:spo, :pos, :osp]
    end
  end

  describe "Helpers.quad_indices/0" do
    test "returns all quad index types" do
      assert Helpers.quad_indices() == [:gspo, :gpos, :gosp, :spog, :posg, :ospg]
    end
  end

  describe "Helpers.all_indices/0" do
    test "returns all index types" do
      all = Helpers.all_indices()

      assert length(all) == 9
      assert :spo in all
      assert :pos in all
      assert :osp in all
      assert :gspo in all
      assert :gpos in all
      assert :gosp in all
      assert :spog in all
      assert :posg in all
      assert :ospg in all
    end
  end

  describe "Helpers.validate_index/1" do
    test "returns :ok for valid triple indices" do
      assert Helpers.validate_index(:spo) == :ok
      assert Helpers.validate_index(:pos) == :ok
      assert Helpers.validate_index(:osp) == :ok
    end

    test "returns :ok for valid quad indices" do
      assert Helpers.validate_index(:gspo) == :ok
      assert Helpers.validate_index(:gpos) == :ok
      assert Helpers.validate_index(:gosp) == :ok
      assert Helpers.validate_index(:spog) == :ok
      assert Helpers.validate_index(:posg) == :ok
      assert Helpers.validate_index(:ospg) == :ok
    end

    test "returns error for invalid indices" do
      assert Helpers.validate_index(:invalid) == {:error, :invalid_index}
      assert Helpers.validate_index(:xyz) == {:error, :invalid_index}
      assert Helpers.validate_index(:spo2) == {:error, :invalid_index}
    end
  end

  describe "Helpers.validate_index!/1" do
    test "returns :ok for valid indices" do
      assert Helpers.validate_index!(:spo) == :ok
      assert Helpers.validate_index!(:gspo) == :ok
    end

    test "raises ArgumentError for invalid indices" do
      assert_raise ArgumentError, ~r/Invalid index type/, fn ->
        Helpers.validate_index!(:invalid)
      end

      assert_raise ArgumentError, ~r/Invalid index type/, fn ->
        Helpers.validate_index!(:xyz)
      end
    end
  end
end
