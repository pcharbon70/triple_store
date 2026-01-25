defmodule TripleStore.Integration.QuadInsertLookupTest do
  @moduledoc """
  Integration tests for Section 6.1.2: Quad Insert and Lookup.

  Tests quad insertion and retrieval end-to-end:
  - Single quad insert and retrieval
  - Batch quad insert and retrieval
  - Insert to default graph
  - Insert to named graph
  - Quad exists in all four indices
  - Duplicate insert is idempotent
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.QuadIndex

  @test_db_base "/tmp/quad_insert_lookup_test"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("quad_insert_lookup_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    test_path = unique_path()

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(test_path)
    end)

    %{db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # 6.1.2.1: Test insert single quad retrieves correctly
  # ===========================================================================

  describe "6.1.2.1 insert single quad retrieves correctly" do
    test "single quad can be retrieved after insert", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("Object")
      graph = RDF.iri("http://example.org/graph")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Insert the quad
      :ok = QuadOperations.insert_quad(db, quad)

      # Verify it exists
      assert QuadOperations.quad_exists?(db, quad)

      # Retrieve by full pattern - lookup_quads returns list directly
      results =
        QuadOperations.lookup_quads(
          db,
          {:bound, :bound, :bound, :bound},
          %{s: s_id, p: p_id, o: o_id, g: g_id}
        )

      assert length(results) == 1
      assert hd(results) == quad
    end

    test "single quad in default graph retrieves correctly", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("Object")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)

      # Default graph ID
      quad = {s_id, p_id, o_id, 0}

      :ok = QuadOperations.insert_quad(db, quad)

      assert QuadOperations.quad_exists?(db, quad)
      assert QuadOperations.default_graph_exists?(db)
    end

    test "single quad can be looked up by subject", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("Object")
      graph = RDF.iri("http://example.org/graph")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      :ok = QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})

      # Lookup by subject only (uses SPOG index) - lookup_quads returns list directly
      results = QuadOperations.lookup_quads(db, {:bound, :var, :var, :var}, %{s: s_id})

      assert length(results) == 1
      assert {s_id, p_id, o_id, g_id} in results
    end
  end

  # ===========================================================================
  # 6.1.2.2: Test insert batch quads retrieves all
  # ===========================================================================

  describe "6.1.2.2 insert batch quads retrieves all" do
    test "batch insert retrieves all quads", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/graph")

      quads =
        Enum.map(1..10, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p")
          object = RDF.literal("O#{i}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)
          {:ok, g_id} = Manager.get_or_create_id(manager, graph)

          {s_id, p_id, o_id, g_id}
        end)

      # Batch insert - insert_quads requires opts as 3rd arg
      :ok = QuadOperations.insert_quads(db, quads, [])

      # Verify all exist
      assert {:ok, 10} = QuadOperations.graph_quad_count(db, manager, graph)

      # Verify each quad exists
      Enum.each(quads, fn quad ->
        assert QuadOperations.quad_exists?(db, quad)
      end)
    end

    test "batch insert across multiple graphs", %{db: db, manager: manager} do
      # Create quads across multiple graphs
      quads =
        for i <- 1..20, graph_num <- [1, 2] do
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p")
          object = RDF.literal("O#{i}")
          graph = RDF.iri("http://example.org/graph#{graph_num}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)
          {:ok, g_id} = Manager.get_or_create_id(manager, graph)

          {s_id, p_id, o_id, g_id}
        end

      :ok = QuadOperations.insert_quads(db, quads, [])

      # Verify total count
      graph1 = RDF.iri("http://example.org/graph1")
      graph2 = RDF.iri("http://example.org/graph2")

      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, graph2)

      assert count1 + count2 == 40
    end

    test "empty batch insert is no-op", %{db: db} do
      assert :ok = QuadOperations.insert_quads(db, [], [])
    end
  end

  # ===========================================================================
  # 6.1.2.3: Test insert to default graph works
  # ===========================================================================

  describe "6.1.2.3 insert to default graph works" do
    test "single quad to default graph", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)

      :ok = QuadOperations.insert_quad(db, {s_id, p_id, o_id, 0})

      # Verify default graph exists
      assert QuadOperations.default_graph_exists?(db)

      # Count in default graph
      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count == 1
    end

    test "multiple quads to default graph", %{db: db, manager: manager} do
      quads =
        Enum.map(1..5, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p#{rem(i, 2)}")
          object = RDF.literal("O#{i}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)

          {s_id, p_id, o_id, 0}
        end)

      :ok = QuadOperations.insert_quads(db, quads, [])

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count == 5

      # Query default graph - lookup_quads returns list directly
      results =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(results) == 5
    end

    test "default graph ID is 0", %{db: db} do
      assert QuadIndex.default_graph_id() == 0
      assert QuadIndex.is_default_graph?(0)
      refute QuadIndex.is_default_graph?(1)
    end
  end

  # ===========================================================================
  # 6.1.2.4: Test insert to named graph works
  # ===========================================================================

  describe "6.1.2.4 insert to named graph" do
    test "single quad to named graph", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/named-graph")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      :ok = QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})

      # Verify graph exists
      assert QuadOperations.graph_exists?(db, manager, graph)

      # Verify count
      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 1
    end

    test "multiple quads to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/my-graph")

      quads =
        Enum.map(1..10, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p")
          object = RDF.literal("O#{i}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)
          {:ok, g_id} = Manager.get_or_create_id(manager, graph)

          {s_id, p_id, o_id, g_id}
        end)

      :ok = QuadOperations.insert_quads(db, quads, [])

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 10
    end

    test "quads in different named graphs are isolated", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      # Insert to graph1
      Enum.each(1..3, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g_id} = Manager.get_or_create_id(manager, graph1)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
      end)

      # Insert to graph2
      Enum.each(1..5, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g_id} = Manager.get_or_create_id(manager, graph2)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
      end)

      # Verify counts
      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, graph2)

      assert count1 == 3
      assert count2 == 5
    end
  end

  # ===========================================================================
  # 6.1.2.5: Test quad exists in all four indices
  # ===========================================================================

  describe "6.1.2.5 quad exists in all four indices" do
    test "quad is accessible via all four index patterns", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}
      :ok = QuadOperations.insert_quad(db, quad)

      # Pattern that uses GSPO index (graph-scoped with subject) - lookup_quads returns list directly
      # GSPO: graph is first component, so pattern is {:bound, :var, :var, :bound} for s and g bound
      gspo_results =
        QuadOperations.lookup_quads(db, {:bound, :var, :var, :bound}, %{s: s_id, g: g_id})

      assert quad in gspo_results

      # Pattern that uses GPOS index (graph-scoped with predicate)
      # GPOS: graph-predicate-object-subject order, so pattern with g and p bound is {:var, :bound, :var, :bound}
      gpos_results =
        QuadOperations.lookup_quads(db, {:var, :bound, :var, :bound}, %{p: p_id, g: g_id})

      assert quad in gpos_results

      # Pattern that uses SPOG index (subject-scoped cross-graph)
      # SPOG: subject-predicate-object-graph order, so pattern with only s bound is {:bound, :var, :var, :var}
      spog_results =
        QuadOperations.lookup_quads(db, {:bound, :var, :var, :var}, %{s: s_id})

      assert quad in spog_results

      # Pattern that uses POSG index (predicate-scoped cross-graph)
      # POSG: predicate-object-subject-graph order, so pattern with only p bound is {:var, :bound, :var, :var}
      posg_results =
        QuadOperations.lookup_quads(db, {:var, :bound, :var, :var}, %{p: p_id})

      assert quad in posg_results
    end

    test "quad keys exist in all four column families", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      :ok = QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})

      # Check each column family directly
      keys = QuadIndex.encode_quad_keys(s_id, p_id, o_id, g_id)

      # GSPO
      assert {:ok, _} = ErlangAdapter.get(db, :gspo, keys.gspo)

      # GPOS
      assert {:ok, _} = ErlangAdapter.get(db, :gpos, keys.gpos)

      # SPOG
      assert {:ok, _} = ErlangAdapter.get(db, :spog, keys.spog)

      # POSG
      assert {:ok, _} = ErlangAdapter.get(db, :posg, keys.posg)
    end

    test "pattern matching uses optimal index", %{db: db, manager: manager} do
      # Insert multiple quads with same subject across graphs
      subject = RDF.iri("http://example.org/s")

      graphs =
        Enum.map(1..3, fn i ->
          RDF.iri("http://example.org/g#{i}")
        end)

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)

      quads =
        Enum.flat_map(graphs, fn graph ->
          Enum.map(1..2, fn i ->
            predicate = RDF.iri("http://example.org/p#{i}")
            object = RDF.literal("O#{i}")

            {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
            {:ok, o_id} = Manager.get_or_create_id(manager, object)
            {:ok, g_id} = Manager.get_or_create_id(manager, graph)

            QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
            {s_id, p_id, o_id, g_id}
          end)
        end)

      # Subject-scoped query should use SPOG index - lookup_quads returns list directly
      results = QuadOperations.lookup_quads(db, {:bound, :var, :var, :var}, %{s: s_id})

      assert length(results) == 6
      Enum.each(quads, fn quad -> assert quad in results end)
    end
  end

  # ===========================================================================
  # 6.1.2.6: Test duplicate insert is idempotent
  # ===========================================================================

  describe "6.1.2.6 duplicate insert is idempotent" do
    test "inserting same quad multiple times is idempotent", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Insert same quad 3 times
      :ok = QuadOperations.insert_quad(db, quad)
      :ok = QuadOperations.insert_quad(db, quad)
      :ok = QuadOperations.insert_quad(db, quad)

      # Should still have exactly 1 quad
      assert QuadOperations.quad_exists?(db, quad)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 1
    end

    test "batch insert with duplicates is idempotent", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Batch insert with duplicates - insert_quads requires opts as 3rd arg
      :ok = QuadOperations.insert_quads(db, [quad, quad, quad], [])

      # Should still have exactly 1 quad
      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 1
    end

    test "duplicate inserts in separate batches is idempotent", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Insert in separate batches - insert_quads requires opts as 3rd arg
      :ok = QuadOperations.insert_quads(db, [quad], [])
      :ok = QuadOperations.insert_quads(db, [quad, quad], [])
      :ok = QuadOperations.insert_quads(db, [quad], [])

      # Should still have exactly 1 quad
      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 1
    end
  end
end
