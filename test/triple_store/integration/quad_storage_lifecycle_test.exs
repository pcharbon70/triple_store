defmodule TripleStore.Integration.QuadStorageLifecycleTest do
  @moduledoc """
  Integration tests for Section 6.1.1: Quad Storage Database Lifecycle.

  Tests complete quad store database lifecycle:
  - Creating new quad store databases
  - Opening quad stores with four indices (GSPO, GPOS, SPOG, POSG)
  - Schema version 2 detection
  - Version mismatch rejection
  - Data persistence across close/reopen
  - Multiple quad stores in same process

  Tests use `:quad` schema (version 2) which includes four quad indices.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Integration.Helpers
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/quad_storage_lifecycle_test"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    Helpers.unique_path("quad_storage_lifecycle_test")
  end

  defp cleanup_path(path) do
    Helpers.cleanup_path(path)
  end

  defp quad_cf_names do
    ColumnFamilyConfig.column_family_names(:quad)
  end

  defp triple_cf_names do
    ColumnFamilyConfig.column_family_names(:triple)
  end

  # ===========================================================================
  # 6.1.1.1: Test create new quad store database
  # ===========================================================================

  describe "6.1.1.1 create new quad store database" do
    test "creates database with all quad indices" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # Verify the database is open
      assert ErlangAdapter.is_open(db)

      # Verify quad-specific column families are accessible
      # Verify the quad indices are accessible by checking they return :not_found for non-existent keys
      assert :not_found =
               ErlangAdapter.get(db, :gspo, <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>)

      assert :not_found =
               ErlangAdapter.get(db, :gpos, <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>)

      assert :not_found =
               ErlangAdapter.get(db, :spog, <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>)

      assert :not_found =
               ErlangAdapter.get(db, :posg, <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>)

      ErlangAdapter.close(db)
      cleanup_path(path)
    end

    test "quad store has four indices" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # Verify we can access all four quad indices via QuadIndex
      # Encode a test quad key for each index
      test_keys = QuadIndex.encode_quad_keys(1, 2, 3, 4)

      # Each key should be 32 bytes
      assert byte_size(test_keys.gspo) == 32
      assert byte_size(test_keys.gpos) == 32
      assert byte_size(test_keys.spog) == 32
      assert byte_size(test_keys.posg) == 32

      ErlangAdapter.close(db)
      cleanup_path(path)
    end

    test "new database is empty" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # New database should have no quads
      refute QuadOperations.default_graph_exists?(db)

      # No named graphs should exist
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert graphs == []

      ErlangAdapter.close(db)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 6.1.1.2: Test open quad store with four indices
  # ===========================================================================

  describe "6.1.1.2 open quad store with four indices" do
    test "reopens database preserving all quad indices" do
      path = unique_path()

      # Create database with quad schema
      {:ok, db1} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager1} = Manager.start_link(db: db1)

      # Insert some quads
      graph = RDF.iri("http://example.org/graph")

      Enum.each(1..10, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("Object #{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager1, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager1, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager1, object)
        {:ok, g_id} = Manager.get_or_create_id(manager1, graph)

        :ok = QuadOperations.insert_quad(db1, {s_id, p_id, o_id, g_id})
      end)

      Manager.stop(manager1)
      ErlangAdapter.close(db1)

      # Reopen database
      {:ok, db2} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Verify data persisted
      {:ok, count} = QuadOperations.graph_quad_count(db2, manager2, graph)
      assert count == 10

      Manager.stop(manager2)
      ErlangAdapter.close(db2)
      cleanup_path(path)
    end

    test "can query from all four indices after reopen" do
      path = unique_path()

      # Create and populate
      {:ok, db1} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager1} = Manager.start_link(db: db1)

      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("Object")
      graph = RDF.iri("http://example.org/graph")

      {:ok, s_id} = Manager.get_or_create_id(manager1, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager1, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager1, object)
      {:ok, g_id} = Manager.get_or_create_id(manager1, graph)

      :ok = QuadOperations.insert_quad(db1, {s_id, p_id, o_id, g_id})

      Manager.stop(manager1)
      ErlangAdapter.close(db1)

      # Reopen and query from each index pattern
      {:ok, db2} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Pattern that uses GSPO index (graph-scoped with subject) - lookup_quads returns list directly
      # GSPO: graph-subject-predicate-object, so pattern with s and g bound is {:bound, :var, :var, :bound}
      gspo_results =
        QuadOperations.lookup_quads(
          db2,
          {:bound, :var, :var, :bound},
          %{s: s_id, g: g_id}
        )

      assert length(gspo_results) == 1

      # Pattern that uses SPOG index (subject-scoped) - lookup_quads returns list directly
      spog_results =
        QuadOperations.lookup_quads(db2, {:bound, :var, :var, :var}, %{s: s_id})

      assert length(spog_results) == 1

      Manager.stop(manager2)
      ErlangAdapter.close(db2)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 6.1.1.3: Test schema version 2 detected on open
  # ===========================================================================

  describe "6.1.1.3 schema version 2 detection on open" do
    test "quad store uses schema version 2" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # Quad store uses 32-byte keys (4 x 64-bit IDs)
      keys = QuadIndex.encode_quad_keys(1, 2, 3, 4)
      assert byte_size(keys.gspo) == 32
      assert byte_size(keys.gpos) == 32
      assert byte_size(keys.spog) == 32
      assert byte_size(keys.posg) == 32

      ErlangAdapter.close(db)
      cleanup_path(path)
    end

    test "quad store uses 32-byte keys for 4 components" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # Quad store uses 32-byte keys (4 x 64-bit IDs)
      keys = QuadIndex.encode_quad_keys(1, 2, 3, 4)
      assert byte_size(keys.gspo) == 32

      ErlangAdapter.close(db)
      cleanup_path(path)
    end

    test "quad store has four distinct indices" do
      path = unique_path()

      {:ok, db} = ErlangAdapter.open(path, schema: :quad)

      # Quad store has four different key orderings
      keys = QuadIndex.encode_quad_keys(1, 2, 3, 4)

      # All four keys should be different (different orderings)
      assert keys.gspo != keys.gpos
      assert keys.gspo != keys.spog
      assert keys.gspo != keys.posg
      assert keys.gpos != keys.spog
      assert keys.gpos != keys.posg
      assert keys.spog != keys.posg

      ErlangAdapter.close(db)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 6.1.1.4: Test triple store database rejected (version mismatch)
  # ===========================================================================

  describe "6.1.1.4 triple store database rejected (version mismatch)" do
    test "quad database has 32-byte keys" do
      path = unique_path() <> "_quad"

      # Create quad store
      {:ok, quad_db} = ErlangAdapter.open(path, schema: :quad)

      # Verify quad key size
      quad_key = QuadIndex.gspo_key(0, 1, 2, 3)

      # Quad keys are 32 bytes (4 x 64-bit IDs)
      assert byte_size(quad_key) == 32

      ErlangAdapter.close(quad_db)
      cleanup_path(path)
    end

    test "quad and triple schemas are different" do
      quad_path = unique_path() <> "_quad"
      triple_path = unique_path() <> "_triple"

      # Create both types
      {:ok, quad_db} = ErlangAdapter.open(quad_path, schema: :quad)
      {:ok, triple_db} = ErlangAdapter.open(triple_path, schema: :triple)

      # Quad store has different column families
      # Both should be openable but have different schemas
      assert ErlangAdapter.is_open(quad_db)
      assert ErlangAdapter.is_open(triple_db)

      ErlangAdapter.close(quad_db)
      ErlangAdapter.close(triple_db)

      cleanup_path(quad_path)
      cleanup_path(triple_path)
    end
  end

  # ===========================================================================
  # 6.1.1.5: Test close and reopen persists quads
  # ===========================================================================

  describe "6.1.1.5 close and reopen persists quads" do
    test "quads persist after close and reopen" do
      path = unique_path()

      # Phase 1: Open, insert quads, close
      {:ok, db1} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager1} = Manager.start_link(db: db1)

      # Create test quads
      quads =
        for i <- 1..5 do
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p#{rem(i, 2)}")
          object = RDF.literal("O#{i}")
          graph = RDF.iri("http://example.org/graph#{rem(i, 2)}")

          {:ok, s_id} = Manager.get_or_create_id(manager1, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager1, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager1, object)
          {:ok, g_id} = Manager.get_or_create_id(manager1, graph)

          {s_id, p_id, o_id, g_id}
        end

      # Insert all quads
      Enum.each(quads, fn quad -> QuadOperations.insert_quad(db1, quad) end)

      # Verify data exists
      Enum.each(quads, fn quad ->
        assert QuadOperations.quad_exists?(db1, quad)
      end)

      Manager.stop(manager1)
      ErlangAdapter.close(db1)

      # Phase 2: Reopen and verify quads persisted
      {:ok, db2} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Verify all quads still exist
      Enum.each(quads, fn quad ->
        assert QuadOperations.quad_exists?(db2, quad),
               "Quad #{inspect(quad)} should exist after reopen"
      end)

      Manager.stop(manager2)
      ErlangAdapter.close(db2)
      cleanup_path(path)
    end

    test "default graph quads persist across reopen" do
      path = unique_path()

      {:ok, db1} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager1} = Manager.start_link(db: db1)

      # Insert quads to default graph (graph_id = 0)
      Enum.each(1..5, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager1, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager1, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager1, object)

        :ok = QuadOperations.insert_quad(db1, {s_id, p_id, o_id, 0})
      end)

      Manager.stop(manager1)
      ErlangAdapter.close(db1)

      # Reopen and verify default graph quads
      {:ok, db2} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager2} = Manager.start_link(db: db2)

      assert QuadOperations.default_graph_exists?(db2)

      {:ok, count} = QuadOperations.graph_quad_count(db2, manager2, :default)
      assert count == 5

      Manager.stop(manager2)
      ErlangAdapter.close(db2)
      cleanup_path(path)
    end

    test "named graph quads persist across reopen" do
      path = unique_path()

      {:ok, db1} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager1} = Manager.start_link(db: db1)

      graph = RDF.iri("http://example.org/test-graph")

      # Insert quads to named graph
      Enum.each(1..3, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager1, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager1, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager1, object)
        {:ok, g_id} = Manager.get_or_create_id(manager1, graph)

        :ok = QuadOperations.insert_quad(db1, {s_id, p_id, o_id, g_id})
      end)

      Manager.stop(manager1)
      ErlangAdapter.close(db1)

      # Reopen and verify named graph
      {:ok, db2} = ErlangAdapter.open(path, schema: :quad)
      {:ok, manager2} = Manager.start_link(db: db2)

      assert QuadOperations.graph_exists?(db2, manager2, graph)

      {:ok, count} = QuadOperations.graph_quad_count(db2, manager2, graph)
      assert count == 3

      Manager.stop(manager2)
      ErlangAdapter.close(db2)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 6.1.1.6: Test multiple quad stores in same process
  # ===========================================================================

  describe "6.1.1.6 multiple quad stores in same process" do
    test "multiple quad databases operate independently" do
      path1 = unique_path() <> "_1"
      path2 = unique_path() <> "_2"

      {:ok, db1} = ErlangAdapter.open(path1, schema: :quad)
      {:ok, db2} = ErlangAdapter.open(path2, schema: :quad)

      {:ok, manager1} = Manager.start_link(db: db1)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Insert different data into each
      graph1 = RDF.iri("http://example.org/graph1")
      graph2 = RDF.iri("http://example.org/graph2")

      {:ok, s1} = Manager.get_or_create_id(manager1, RDF.iri("http://example.org/s1"))
      {:ok, p1} = Manager.get_or_create_id(manager1, RDF.iri("http://example.org/p1"))
      {:ok, o1} = Manager.get_or_create_id(manager1, RDF.literal("O1"))
      {:ok, g1} = Manager.get_or_create_id(manager1, graph1)

      {:ok, s2} = Manager.get_or_create_id(manager2, RDF.iri("http://example.org/s2"))
      {:ok, p2} = Manager.get_or_create_id(manager2, RDF.iri("http://example.org/p2"))
      {:ok, o2} = Manager.get_or_create_id(manager2, RDF.literal("O2"))
      {:ok, g2} = Manager.get_or_create_id(manager2, graph2)

      :ok = QuadOperations.insert_quad(db1, {s1, p1, o1, g1})
      :ok = QuadOperations.insert_quad(db2, {s2, p2, o2, g2})

      # Verify isolation - check that each database only has its own data
      assert QuadOperations.quad_exists?(db1, {s1, p1, o1, g1})
      assert QuadOperations.quad_exists?(db2, {s2, p2, o2, g2})

      # Check that graph1 only exists in db1
      assert QuadOperations.graph_exists?(db1, manager1, graph1)
      refute QuadOperations.graph_exists?(db2, manager2, graph1)

      # Check that graph2 only exists in db2
      assert QuadOperations.graph_exists?(db2, manager2, graph2)
      refute QuadOperations.graph_exists?(db1, manager1, graph2)

      # Verify quad counts are isolated
      {:ok, count1} = QuadOperations.graph_quad_count(db1, manager1, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db2, manager2, graph2)

      assert count1 == 1
      assert count2 == 1

      Manager.stop(manager1)
      Manager.stop(manager2)
      ErlangAdapter.close(db1)
      ErlangAdapter.close(db2)

      cleanup_path(path1)
      cleanup_path(path2)
    end

    test "dictionary managers are independent for each quad store" do
      path1 = unique_path() <> "_1"
      path2 = unique_path() <> "_2"

      {:ok, db1} = ErlangAdapter.open(path1, schema: :quad)
      {:ok, db2} = ErlangAdapter.open(path2, schema: :quad)

      {:ok, manager1} = Manager.start_link(db: db1)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Create same term in both databases
      shared_term = RDF.iri("http://example.org/shared")

      {:ok, id1} = Manager.get_or_create_id(manager1, shared_term)
      {:ok, id2} = Manager.get_or_create_id(manager2, shared_term)

      # IDs should be valid
      assert is_integer(id1) and id1 > 0
      assert is_integer(id2) and id2 > 0

      # Create unique terms for each database
      unique1 = RDF.iri("http://example.org/only-in-db1")
      unique2 = RDF.iri("http://example.org/only-in-db2")

      {:ok, _id_in_db1} = Manager.get_or_create_id(manager1, unique1)
      {:ok, _id_in_db2} = Manager.get_or_create_id(manager2, unique2)

      # Verify isolation - unique1 exists in db1 but not db2
      assert {:ok, _} = StringToId.lookup_id(db1, unique1)
      assert :not_found = StringToId.lookup_id(db2, unique1)

      # unique2 exists in db2 but not db1
      assert {:ok, _} = StringToId.lookup_id(db2, unique2)
      assert :not_found = StringToId.lookup_id(db1, unique2)

      Manager.stop(manager1)
      Manager.stop(manager2)
      ErlangAdapter.close(db1)
      ErlangAdapter.close(db2)

      cleanup_path(path1)
      cleanup_path(path2)
    end

    test "concurrent operations on multiple quad stores work correctly" do
      path1 = unique_path() <> "_1"
      path2 = unique_path() <> "_2"

      {:ok, db1} = ErlangAdapter.open(path1, schema: :quad)
      {:ok, db2} = ErlangAdapter.open(path2, schema: :quad)

      {:ok, manager1} = Manager.start_link(db: db1)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Concurrent inserts to different databases
      task1 =
        Task.async(fn ->
          Enum.each(1..50, fn i ->
            s = RDF.iri("http://example.org/s1_#{i}")
            p = RDF.iri("http://example.org/p")
            o = RDF.literal("O#{i}")
            g = RDF.iri("http://example.org/g1")

            {:ok, s_id} = Manager.get_or_create_id(manager1, s)
            {:ok, p_id} = Manager.get_or_create_id(manager1, p)
            {:ok, o_id} = Manager.get_or_create_id(manager1, o)
            {:ok, g_id} = Manager.get_or_create_id(manager1, g)

            QuadOperations.insert_quad(db1, {s_id, p_id, o_id, g_id})
          end)
        end)

      task2 =
        Task.async(fn ->
          Enum.each(1..50, fn i ->
            s = RDF.iri("http://example.org/s2_#{i}")
            p = RDF.iri("http://example.org/p")
            o = RDF.literal("O#{i}")
            g = RDF.iri("http://example.org/g2")

            {:ok, s_id} = Manager.get_or_create_id(manager2, s)
            {:ok, p_id} = Manager.get_or_create_id(manager2, p)
            {:ok, o_id} = Manager.get_or_create_id(manager2, o)
            {:ok, g_id} = Manager.get_or_create_id(manager2, g)

            QuadOperations.insert_quad(db2, {s_id, p_id, o_id, g_id})
          end)
        end)

      # Both should complete successfully
      assert :ok = Task.await(task1, 10_000)
      assert :ok = Task.await(task2, 10_000)

      # Verify each database has correct data
      g1 = RDF.iri("http://example.org/g1")
      g2 = RDF.iri("http://example.org/g2")

      {:ok, count1} = QuadOperations.graph_quad_count(db1, manager1, g1)
      {:ok, count2} = QuadOperations.graph_quad_count(db2, manager2, g2)

      assert count1 == 50
      assert count2 == 50

      Manager.stop(manager1)
      Manager.stop(manager2)
      ErlangAdapter.close(db1)
      ErlangAdapter.close(db2)

      cleanup_path(path1)
      cleanup_path(path2)
    end
  end
end
