defmodule TripleStore.Backend.RocksDB.SchemaVersioningTest do
  @moduledoc """
  Unit tests for Section 1.1.3: Database Schema Versioning

  These tests verify that the schema versioning system correctly:
  - Sets schema version on database creation
  - Validates schema version on database open
  - Rejects mismatched schemas
  - Reports schema type via is_quad_store?/1
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  @moduletag :integration

  # Temporary database path helper
  defp temp_db_path(suffix \\ "") do
    name = "test_schema_#{System.unique_integer([:positive, :monotonic])}#{suffix}"
    Path.join(System.tmp_dir!(), name)
  end

  # Helper to clean up database
  defp cleanup_db(db_path) do
    File.rm_rf!(db_path)
  end

  # ===========================================================================
  # Section 1.1.3.1: Schema Version Constants
  # ===========================================================================

  describe "1.1.3.1 Schema Version Constants" do
    @tag :skip
    test "Schema version constants are defined" do
      # Module attributes are private implementation details
      # Schema versioning is tested implicitly through database operations
      # This test placeholder exists for documentation purposes
    end
  end

  # ===========================================================================
  # Section 1.1.3.2: Triple Store Creation (Schema v1)
  # ===========================================================================

  describe "1.1.3.2 Triple Store Creation" do
    test "1.1.3.2.1 Creating triple store sets schema version v1" do
      db_path = temp_db_path("triple_create")

      try do
        # Open with :triple schema (default)
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)

        # Verify schema version is set correctly
        assert {:ok, false} = ErlangAdapter.is_quad_store?(adapter)

        # Close and reopen to verify persistence
        :ok = ErlangAdapter.close(adapter)
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        assert {:ok, false} = ErlangAdapter.is_quad_store?(adapter)

        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.2.2 Triple store has correct column families" do
      db_path = temp_db_path("triple_cfs")

      try do
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)

        # List column families
        cf_names = ErlangAdapter.list_column_families(db_path)

        # Should have triple indices
        assert "spo" in cf_names
        assert "pos" in cf_names
        assert "osp" in cf_names

        # Should NOT have quad indices
        refute "gspo" in cf_names
        refute "gpos" in cf_names
        refute "spog" in cf_names
        refute "posg" in cf_names

        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.2.3 Default schema is triple store" do
      db_path = temp_db_path("default_schema")

      try do
        # Open without specifying schema (should default to :triple)
        assert {:ok, adapter} = ErlangAdapter.open(db_path)

        # Verify it's a triple store
        assert {:ok, false} = ErlangAdapter.is_quad_store?(adapter)

        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end
  end

  # ===========================================================================
  # Section 1.1.3.3: Quad Store Creation (Schema v2)
  # ===========================================================================

  describe "1.1.3.3 Quad Store Creation" do
    test "1.1.3.3.1 Creating quad store sets schema version v2" do
      db_path = temp_db_path("quad_create")

      try do
        # Open with :quad schema
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :quad)

        # Verify schema version is set correctly
        assert {:ok, true} = ErlangAdapter.is_quad_store?(adapter)

        # Close and reopen to verify persistence
        :ok = ErlangAdapter.close(adapter)
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :quad)
        assert {:ok, true} = ErlangAdapter.is_quad_store?(adapter)

        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.3.2 Quad store has correct column families" do
      db_path = temp_db_path("quad_cfs")

      try do
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :quad)

        # List column families
        cf_names = ErlangAdapter.list_column_families(db_path)

        # Should have quad indices
        assert "gspo" in cf_names
        assert "gpos" in cf_names
        assert "spog" in cf_names
        assert "posg" in cf_names

        # Should NOT have triple indices
        refute "spo" in cf_names
        refute "pos" in cf_names
        refute "osp" in cf_names

        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end
  end

  # ===========================================================================
  # Section 1.1.3.4: Schema Validation
  # ===========================================================================

  describe "1.1.3.4 Schema Validation" do
    test "1.1.3.4.1 Opening triple store with quad schema fails" do
      db_path = temp_db_path("mismatch_triple")

      try do
        # Create as triple store
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        :ok = ErlangAdapter.close(adapter)

        # Try to open as quad store (should fail)
        assert {:error, :schema_mismatch} = ErlangAdapter.open(db_path, schema: :quad)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.4.2 Opening quad store with triple schema fails" do
      db_path = temp_db_path("mismatch_quad")

      try do
        # Create as quad store
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :quad)
        :ok = ErlangAdapter.close(adapter)

        # Try to open as triple store (should fail)
        assert {:error, :schema_mismatch} = ErlangAdapter.open(db_path, schema: :triple)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.4.3 Reopening with same schema succeeds" do
      db_path = temp_db_path("same_schema")

      try do
        # Create as triple store
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        :ok = ErlangAdapter.close(adapter)

        # Reopen with same schema (should succeed)
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        :ok = ErlangAdapter.close(adapter)

        # Also test with quad
        db_path2 = temp_db_path("same_schema_quad")
        assert {:ok, adapter} = ErlangAdapter.open(db_path2, schema: :quad)
        :ok = ErlangAdapter.close(adapter)

        assert {:ok, adapter} = ErlangAdapter.open(db_path2, schema: :quad)
        :ok = ErlangAdapter.close(adapter)

        cleanup_db(db_path2)
      after
        cleanup_db(db_path)
      end
    end
  end

  # ===========================================================================
  # Section 1.1.3.5: is_quad_store? Function
  # ===========================================================================

  describe "1.1.3.5 is_quad_store? Function" do
    test "1.1.3.5.1 Returns false for triple store" do
      db_path = temp_db_path("is_quad_false")

      try do
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        assert {:ok, false} = ErlangAdapter.is_quad_store?(adapter)
        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end

    test "1.1.3.5.2 Returns true for quad store" do
      db_path = temp_db_path("is_quad_true")

      try do
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :quad)
        assert {:ok, true} = ErlangAdapter.is_quad_store?(adapter)
        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end

    @tag :skip
    test "1.1.3.5.3 Returns error for invalid adapter" do
      # Note: Testing with an invalid PID is problematic because:
      # 1. Using self() causes the process to call itself
      # 2. Using a fake PID can't be distinguished from a dead process
      # In production, invalid adapters are handled by GenServer timeouts
    end
  end

  # ===========================================================================
  # Section 1.1.3.6: Backward Compatibility
  # ===========================================================================

  describe "1.1.3.6 Backward Compatibility" do
    test "1.1.3.6.1 Old triple store without schema version can be opened" do
      db_path = temp_db_path("old_triple")

      try do
        # Create a database without schema version metadata
        # by creating it with raw erlang-rocksdb
        db_opts = [create_if_missing: true, error_if_exists: false]
        db_path_charlist = String.to_charlist(db_path)

        # Create minimal database with triple CFs
        {:ok, db, [default_cf]} =
          :rocksdb.open_with_cf(db_path_charlist, db_opts, [{~c"default", []}])

        {:ok, _id2str_cf} = :rocksdb.create_column_family(db, ~c"id2str", [])
        {:ok, _str2id_cf} = :rocksdb.create_column_family(db, ~c"str2id", [])
        {:ok, _spo_cf} = :rocksdb.create_column_family(db, ~c"spo", [])
        {:ok, _pos_cf} = :rocksdb.create_column_family(db, ~c"pos", [])
        {:ok, _osp_cf} = :rocksdb.create_column_family(db, ~c"osp", [])
        {:ok, _derived_cf} = :rocksdb.create_column_family(db, ~c"derived", [])
        {:ok, _numeric_cf} = :rocksdb.create_column_family(db, ~c"numeric_range", [])

        :rocksdb.close(db)

        # Now try to open with ErlangAdapter as triple store
        # This should work because we detect triple CFs
        assert {:ok, adapter} = ErlangAdapter.open(db_path, schema: :triple)
        :ok = ErlangAdapter.close(adapter)
      after
        cleanup_db(db_path)
      end
    end
  end
end
