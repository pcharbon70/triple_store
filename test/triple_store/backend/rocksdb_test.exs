defmodule TripleStore.Backend.RocksDBTest do
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB

  describe "RocksDB module" do
    test "module is defined" do
      assert Code.ensure_loaded?(RocksDB)
    end
  end

  describe "NIF" do
    test "NIF module is defined" do
      assert Code.ensure_loaded?(RocksDB.NIF)
    end

    test "NIF loads successfully" do
      assert RocksDB.NIF.nif_loaded() == "rocksdb_nif"
    end
  end
end
