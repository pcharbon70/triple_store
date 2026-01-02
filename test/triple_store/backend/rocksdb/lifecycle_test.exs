defmodule TripleStore.Backend.RocksDB.LifecycleTest do
  @moduledoc """
  Tests for RocksDB database lifecycle operations (Task 1.2.1).
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF

  @test_db_base "/tmp/triple_store_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    on_exit(fn -> File.rm_rf(test_path) end)
    {:ok, path: test_path}
  end

  describe "open/1" do
    test "opens a database successfully", %{path: path} do
      assert {:ok, db} = NIF.open(path)
      assert is_reference(db)
      NIF.close(db)
    end

    test "creates database directory if it doesn't exist", %{path: path} do
      refute File.exists?(path)
      {:ok, db} = NIF.open(path)
      assert File.exists?(path)
      NIF.close(db)
    end

    test "creates all column families", %{path: path} do
      {:ok, db} = NIF.open(path)
      NIF.close(db)

      cfs = NIF.list_column_families()
      assert :id2str in cfs
      assert :str2id in cfs
      assert :spo in cfs
      assert :pos in cfs
      assert :osp in cfs
      assert :derived in cfs
    end

    test "can reopen an existing database", %{path: path} do
      {:ok, db1} = NIF.open(path)
      NIF.close(db1)

      {:ok, db2} = NIF.open(path)
      assert is_reference(db2)
      NIF.close(db2)
    end
  end

  describe "close/1" do
    test "closes database successfully", %{path: path} do
      {:ok, db} = NIF.open(path)
      assert :ok = NIF.close(db)
    end

    test "returns error when closing already closed database", %{path: path} do
      {:ok, db} = NIF.open(path)
      assert :ok = NIF.close(db)
      assert {:error, :already_closed} = NIF.close(db)
    end
  end

  describe "get_path/1" do
    test "returns the database path", %{path: path} do
      {:ok, db} = NIF.open(path)
      assert {:ok, ^path} = NIF.get_path(db)
      NIF.close(db)
    end
  end

  describe "is_open/1" do
    test "returns true for open database", %{path: path} do
      {:ok, db} = NIF.open(path)
      assert NIF.is_open(db) == true
      NIF.close(db)
    end

    test "returns false for closed database", %{path: path} do
      {:ok, db} = NIF.open(path)
      NIF.close(db)
      assert NIF.is_open(db) == false
    end
  end

  describe "list_column_families/0" do
    test "returns all configured column families" do
      cfs = NIF.list_column_families()
      assert length(cfs) == 7
      assert :id2str in cfs
      assert :str2id in cfs
      assert :spo in cfs
      assert :pos in cfs
      assert :osp in cfs
      assert :derived in cfs
      assert :numeric_range in cfs
    end
  end

  describe "error handling" do
    test "returns error for invalid path" do
      result = NIF.open("/nonexistent/deeply/nested/path/that/should/fail")

      case result do
        {:error, {:open_failed, reason}} ->
          assert is_binary(reason)

        {:ok, db} ->
          NIF.close(db)
          flunk("Expected error for invalid path")
      end
    end
  end
end
