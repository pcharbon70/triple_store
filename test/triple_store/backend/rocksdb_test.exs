defmodule TripleStore.Backend.RocksDBTest do
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB

  describe "RocksDB module" do
    test "module is defined" do
      assert Code.ensure_loaded?(RocksDB)
    end
  end

  describe "ErlangAdapter" do
    test "ErlangAdapter module is defined" do
      assert Code.ensure_loaded?(RocksDB.ErlangAdapter)
    end

    test "NIF loads successfully" do
      assert RocksDB.ErlangAdapter.nif_loaded() == "rocksdb_nif"
    end
  end
end
