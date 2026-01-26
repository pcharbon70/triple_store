defmodule TripleStore.QuadOperationsTest do
  @moduledoc """
  Unit tests for Section 1.5: Quad Insert and Delete

  These tests verify quad CRUD operations including insert, delete,
  existence checking, and pattern-based lookup.
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations

  # ===========================================================================
  # Setup and Teardown
  # ===========================================================================

  setup do
    # Use relative path that will be expanded to current working directory
    path = "test_quad_ops_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    {:ok, db: db}
  end

  # ===========================================================================
  # Section 1.5.1: Quad Insert Operations
  # ===========================================================================

  describe "1.5.1 Quad Insert Operations" do
    test "insert_quad/2 inserts a quad to all four indices", %{db: db} do
      quad = {1, 2, 3, 0}

      assert :ok = QuadOperations.insert_quad(db, quad)

      # Verify the quad exists in all indices by checking existence
      assert QuadOperations.quad_exists?(db, quad)
    end

    test "insert_quad/2 is idempotent", %{db: db} do
      quad = {1, 2, 3, 0}

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert :ok = QuadOperations.insert_quad(db, quad)
    end

    test "insert_quad/2 handles default graph (ID 0)", %{db: db} do
      quad = {10, 20, 30, 0}

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert QuadOperations.quad_exists?(db, quad)
    end

    test "insert_quad/2 handles named graph (positive ID)", %{db: db} do
      quad = {10, 20, 30, 100}

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert QuadOperations.quad_exists?(db, quad)
    end

    test "insert_quads/3 inserts multiple quads atomically", %{db: db} do
      quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]

      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Verify all quads exist
      for quad <- quads do
        assert QuadOperations.quad_exists?(db, quad)
      end
    end

    test "insert_quads/3 with sync: false for bulk loading", %{db: db} do
      quads = [{1, 2, 3, 0}, {4, 5, 6, 0}]

      assert :ok = QuadOperations.insert_quads(db, quads, sync: false)

      for quad <- quads do
        assert QuadOperations.quad_exists?(db, quad)
      end
    end

    test "insert_quads/3 handles empty list", %{db: db} do
      assert :ok = QuadOperations.insert_quads(db, [], [])
    end
  end

  # ===========================================================================
  # Section 1.5.2: Quad Delete Operations
  # ===========================================================================

  describe "1.5.2 Quad Delete Operations" do
    test "delete_quad/2 deletes a quad from all four indices", %{db: db} do
      quad = {1, 2, 3, 0}

      # Insert the quad first
      assert :ok = QuadOperations.insert_quad(db, quad)

      # Delete the quad
      assert :ok = QuadOperations.delete_quad(db, quad)

      # Verify it no longer exists
      refute QuadOperations.quad_exists?(db, quad)
    end

    test "delete_quad/2 is idempotent - deleting non-existent quad succeeds", %{db: db} do
      quad = {999, 888, 777, 0}

      # Deleting a non-existent quad returns :ok (idempotent operation)
      assert :ok = QuadOperations.delete_quad(db, quad)
    end

    test "delete_quad/2 is idempotent", %{db: db} do
      quad = {1, 2, 3, 0}

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert :ok = QuadOperations.delete_quad(db, quad)
      assert :ok = QuadOperations.delete_quad(db, quad)
    end

    test "delete_quads/3 deletes multiple quads atomically", %{db: db} do
      quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]

      # Insert all quads
      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Delete the first two
      to_delete = [{1, 2, 3, 0}, {4, 5, 6, 0}]
      assert :ok = QuadOperations.delete_quads(db, to_delete, [])

      # Verify deletions
      refute QuadOperations.quad_exists?(db, {1, 2, 3, 0})
      refute QuadOperations.quad_exists?(db, {4, 5, 6, 0})
      assert QuadOperations.quad_exists?(db, {7, 8, 9, 1})
    end

    test "delete_quads/3 handles non-existent quads gracefully", %{db: db} do
      # Only insert the first quad
      assert :ok = QuadOperations.insert_quad(db, {1, 2, 3, 0})

      # Try to delete multiple quads, only one exists
      # Operation should succeed regardless (idempotent)
      quads = [{1, 2, 3, 0}, {999, 888, 777, 0}]
      assert :ok = QuadOperations.delete_quads(db, quads, [])
    end

    test "delete_quads/3 handles empty list", %{db: db} do
      assert :ok = QuadOperations.delete_quads(db, [], [])
    end
  end

  # ===========================================================================
  # Section 1.5.3: Quad Existence Check
  # ===========================================================================

  describe "1.5.3 Quad Existence Check" do
    test "quad_exists?/2 returns true for existing quad", %{db: db} do
      quad = {1, 2, 3, 0}

      refute QuadOperations.quad_exists?(db, quad)

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert QuadOperations.quad_exists?(db, quad)
    end

    test "quad_exists?/2 returns false for non-existent quad", %{db: db} do
      refute QuadOperations.quad_exists?(db, {999, 888, 777, 0})
    end

    test "quad_exists?/2 handles default graph correctly", %{db: db} do
      default_quad = {1, 2, 3, 0}
      named_quad = {1, 2, 3, 100}

      assert :ok = QuadOperations.insert_quad(db, default_quad)
      assert :ok = QuadOperations.insert_quad(db, named_quad)

      assert QuadOperations.quad_exists?(db, default_quad)
      assert QuadOperations.quad_exists?(db, named_quad)

      # Same triple in different graphs are different quads
      assert QuadOperations.quad_exists?(db, {1, 2, 3, 0})
      assert QuadOperations.quad_exists?(db, {1, 2, 3, 100})
    end
  end

  # ===========================================================================
  # Section 1.5.4: Quad Lookup
  # ===========================================================================

  describe "1.5.4 Quad Lookup" do
    test "lookup_quads/3 returns all quads in default graph", %{db: db} do
      quads = [
        {1, 2, 3, 0},
        {4, 5, 6, 0},
        {7, 8, 9, 0}
      ]

      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Also insert a quad in a different graph
      assert :ok = QuadOperations.insert_quad(db, {1, 2, 3, 100})

      # Lookup all quads in default graph
      pattern = {:var, :var, :var, :bound}
      values = %{g: 0}

      results = QuadOperations.lookup_quads(db, pattern, values) |> Enum.sort()
      expected = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 0}] |> Enum.sort()

      assert results == expected
    end

    test "lookup_quads/3 returns quads by subject across graphs", %{db: db} do
      quads = [
        {1, 2, 3, 0},
        {1, 5, 6, 0},
        {1, 2, 9, 100}
      ]

      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Also insert a quad with different subject
      assert :ok = QuadOperations.insert_quad(db, {99, 2, 3, 0})

      # Lookup all quads with subject=1
      pattern = {:bound, :var, :var, :var}
      values = %{s: 1}

      results = QuadOperations.lookup_quads(db, pattern, values) |> Enum.sort()
      expected = [{1, 2, 3, 0}, {1, 2, 9, 100}, {1, 5, 6, 0}] |> Enum.sort()

      assert results == expected
    end

    test "lookup_quads/3 returns quads by subject-predicate in graph", %{db: db} do
      quads = [
        {1, 2, 3, 0},
        {1, 2, 9, 0},
        {1, 5, 6, 0}
      ]

      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Lookup all quads with s=1, p=2 in graph 0
      pattern = {:bound, :bound, :var, :bound}
      values = %{s: 1, p: 2, g: 0}

      results = QuadOperations.lookup_quads(db, pattern, values) |> Enum.sort()
      expected = [{1, 2, 3, 0}, {1, 2, 9, 0}] |> Enum.sort()

      assert results == expected
    end

    test "lookup_quads/3 returns all quads for all-var pattern", %{db: db} do
      quads = [
        {1, 2, 3, 0},
        {4, 5, 6, 1}
      ]

      assert :ok = QuadOperations.insert_quads(db, quads, [])

      # Lookup all quads
      pattern = {:var, :var, :var, :var}
      values = %{}

      results = QuadOperations.lookup_quads(db, pattern, values) |> Enum.sort()
      expected = quads |> Enum.sort()

      assert results == expected
    end

    test "lookup_quads/3 returns empty list for no matches", %{db: db} do
      # Don't insert anything

      pattern = {:bound, :var, :var, :var}
      values = %{s: 999}

      results = QuadOperations.lookup_quads(db, pattern, values)

      assert results == []
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "Integration: Insert and Delete" do
    test "insert then delete same quad works correctly", %{db: db} do
      quad = {42, 43, 44, 0}

      assert :ok = QuadOperations.insert_quad(db, quad)
      assert QuadOperations.quad_exists?(db, quad)

      assert :ok = QuadOperations.delete_quad(db, quad)
      refute QuadOperations.quad_exists?(db, quad)
    end

    test "multiple graphs with same triple are independent", %{db: db} do
      quad_default = {1, 2, 3, 0}
      quad_named = {1, 2, 3, 100}

      assert :ok = QuadOperations.insert_quad(db, quad_default)
      assert :ok = QuadOperations.insert_quad(db, quad_named)

      # Both exist independently
      assert QuadOperations.quad_exists?(db, quad_default)
      assert QuadOperations.quad_exists?(db, quad_named)

      # Delete from default graph
      assert :ok = QuadOperations.delete_quad(db, quad_default)

      # Default graph quad is gone, named graph quad remains
      refute QuadOperations.quad_exists?(db, quad_default)
      assert QuadOperations.quad_exists?(db, quad_named)
    end
  end
end
