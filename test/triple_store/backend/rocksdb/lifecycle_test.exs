defmodule TripleStore.Backend.RocksDB.LifecycleTest do
  @moduledoc """
  Tests for RocksDB database lifecycle operations (Task 1.2.1).
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  @test_db_base "/tmp/triple_store_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    on_exit(fn -> File.rm_rf(test_path) end)
    {:ok, path: test_path}
  end

  describe "open/1" do
    test "opens a database successfully", %{path: path} do
      assert {:ok, db} = ErlangAdapter.open(path)
      assert is_pid(db)
      ErlangAdapter.close(db)
    end

    test "creates database directory if it doesn't exist", %{path: path} do
      refute File.exists?(path)
      {:ok, db} = ErlangAdapter.open(path)
      assert File.exists?(path)
      ErlangAdapter.close(db)
    end

    test "creates all column families", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      ErlangAdapter.close(db)

      cfs = ErlangAdapter.list_column_families(path)
      assert "id2str" in cfs
      assert "str2id" in cfs
      assert "spo" in cfs
      assert "pos" in cfs
      assert "osp" in cfs
      assert "derived" in cfs
    end

    test "can reopen an existing database", %{path: path} do
      {:ok, db1} = ErlangAdapter.open(path)
      ErlangAdapter.close(db1)

      {:ok, db2} = ErlangAdapter.open(path)
      assert is_pid(db2)
      ErlangAdapter.close(db2)
    end
  end

  describe "close/1" do
    test "closes database successfully", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      assert :ok = ErlangAdapter.close(db)
    end

    test "returns error when closing already closed database", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      assert :ok = ErlangAdapter.close(db)
      assert {:error, :already_closed} = ErlangAdapter.close(db)
    end
  end

  describe "get_path/1" do
    test "returns the database path", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      assert {:ok, ^path} = ErlangAdapter.get_path(db)
      ErlangAdapter.close(db)
    end
  end

  describe "is_open/1" do
    test "returns true for open database", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      assert ErlangAdapter.is_open(db) == true
      ErlangAdapter.close(db)
    end

    test "returns false for closed database", %{path: path} do
      {:ok, db} = ErlangAdapter.open(path)
      ErlangAdapter.close(db)
      assert ErlangAdapter.is_open(db) == false
    end
  end

  describe "list_column_families/1" do
    test "returns all configured column families", %{path: path} do
      {:ok, _db} = ErlangAdapter.open(path)
      cfs = ErlangAdapter.list_column_families(path)
      assert length(cfs) >= 7
      assert "id2str" in cfs
      assert "str2id" in cfs
      assert "spo" in cfs
      assert "pos" in cfs
      assert "osp" in cfs
      assert "derived" in cfs
      assert "numeric_range" in cfs
    end
  end

  describe "error handling" do
    test "returns error for invalid absolute path" do
      result = ErlangAdapter.open("/nonexistent/deeply/nested/path/that/should/fail")

      case result do
        {:error, :absolute_path_not_allowed} ->
          # Expected - path validation rejects absolute paths
          :ok

        {:error, {:open_failed, _reason}} ->
          # Also acceptable - erlang-rocksdb failed to open
          :ok

        {:ok, db} ->
          ErlangAdapter.close(db)
          flunk("Expected error for invalid path")
      end
    end

    test "returns error for path traversal attempt" do
      result = ErlangAdapter.open("/tmp/test/../etc/passwd")

      case result do
        {:error, :path_traversal_attempt} ->
          :ok

        {:error, _reason} ->
          :ok

        {:ok, db} ->
          ErlangAdapter.close(db)
          flunk("Expected error for path traversal attempt")
      end
    end
  end
end
