defmodule TripleStore.SPARQL.GraphManagementTest do
  @moduledoc """
  Unit tests for SPARQL Graph Management Operations (Section 4.1).

  Tests the execution of CREATE, DROP, and CLEAR GRAPH operations.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.SPARQL.Parser

  # Setup for tmp_dir (ExUnit.Callbacks)
  setup context do
    # Use a unique temporary directory for each test
    tmp_dir = Path.join(System.tmp_dir!(), "triple_store_test_#{System.unique_integer()}")

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  # Database setup helpers
  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_graph_#{System.unique_integer([:positive])}")

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)

    {:ok, manager} = Manager.start_link(db: db)

    {db, manager}
  end

  defp cleanup({db, manager}) do
    Manager.stop(manager)
    ErlangAdapter.close(db)
  end

  defp create_context(db, manager) do
    %{db: db, dict_manager: manager}
  end

  # ===========================================================================
  # 4.1.5.1 - CREATE GRAPH
  # ===========================================================================

  describe "CREATE GRAPH" do
    test "creates empty named graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/new_graph"

        # Parse and execute CREATE GRAPH
        {:ok, ast} = Parser.parse_update("CREATE GRAPH <#{graph_iri}>")
        assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)

        # Verify graph exists by checking we can get its ID
        assert {:ok, _graph_id} = Manager.get_or_create_id(manager, RDF.iri(graph_iri))
      after
        cleanup({db, manager})
      end
    end

    test "returns error when creating existing graph without SILENT", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/existing"

        # Create the graph first
        assert {:ok, _graph_id} = Manager.get_or_create_id(manager, RDF.iri(graph_iri))

        # Try to create again - should error
        {:ok, ast} = Parser.parse_update("CREATE GRAPH <#{graph_iri}>")
        assert {:error, :graph_already_exists} = UpdateExecutor.execute(ctx, ast)
      after
        cleanup({db, manager})
      end
    end

    test "CREATE SILENT ignores existing graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/existing"

        # Create the graph first
        assert {:ok, _graph_id} = Manager.get_or_create_id(manager, RDF.iri(graph_iri))

        # Try to create again with SILENT - should succeed
        {:ok, ast} = Parser.parse_update("CREATE SILENT GRAPH <#{graph_iri}>")
        assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)
      after
        cleanup({db, manager})
      end
    end
  end

  # ===========================================================================
  # 4.1.5.4 & 4.1.5.5 - DROP GRAPH
  # ===========================================================================

  describe "DROP GRAPH" do
    test "drops named graph with all quads", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/to_drop"

        # Add some quads to the graph using INSERT DATA
        # The quad format is handled by the parser

        {:ok, ast} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph_iri}> { <http://example.org/s> <http://example.org/p> \"value\" } }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

        # Drop the graph
        {:ok, drop_ast} = Parser.parse_update("DROP GRAPH <#{graph_iri}>")
        assert {:ok, 1} = UpdateExecutor.execute(ctx, drop_ast)

        # Verify graph is empty (no quads)
        # After DROP, all quads are removed so graph_exists? should return false
        refute QuadOperations.graph_exists?(db, manager, RDF.iri(graph_iri))
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, RDF.iri(graph_iri))
      after
        cleanup({db, manager})
      end
    end

    test "DROP SILENT ignores missing graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/nonexistent"

        # Try to drop non-existent graph with SILENT
        {:ok, ast} = Parser.parse_update("DROP SILENT GRAPH <#{graph_iri}>")
        assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)
      after
        cleanup({db, manager})
      end
    end

    test "returns ok when dropping non-existent graph (no-op)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/nonexistent"

        # Try to drop non-existent graph - returns ok with 0 count
        {:ok, ast} = Parser.parse_update("DROP GRAPH <#{graph_iri}>")
        assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)
      after
        cleanup({db, manager})
      end
    end
  end

  # ===========================================================================
  # 4.1.5.6 - 4.1.5.8 - CLEAR GRAPH
  # ===========================================================================

  describe "CLEAR GRAPH" do
    test "clears named graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/to_clear"

        # Add some quads to the graph
        {:ok, ast} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph_iri}> { <http://example.org/s1> <http://example.org/p> \"v1\" . <http://example.org/s2> <http://example.org/p> \"v2\" } }"
          )

        assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)

        # Clear the graph
        {:ok, clear_ast} = Parser.parse_update("CLEAR GRAPH <#{graph_iri}>")
        assert {:ok, 2} = UpdateExecutor.execute(ctx, clear_ast)

        # Verify graph is empty
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, RDF.iri(graph_iri))
      after
        cleanup({db, manager})
      end
    end

    test "clears default graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        # Add some quads to default graph
        {:ok, ast} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s1> <http://example.org/p> \"v1\" }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

        # Clear default graph
        {:ok, clear_ast} = Parser.parse_update("CLEAR DEFAULT")
        assert {:ok, 1} = UpdateExecutor.execute(ctx, clear_ast)

        # Verify default graph is empty
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, :default)
      after
        cleanup({db, manager})
      end
    end

    test "clears all graphs", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph1 = "http://example.org/g1"
        graph2 = "http://example.org/g2"

        # Add quads to multiple graphs
        {:ok, ast1} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph1}> { <http://example.org/s> <http://example.org/p> \"v\" } }"
          )

        {:ok, ast2} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph2}> { <http://example.org/s> <http://example.org/p> \"v\" } }"
          )

        {:ok, ast3} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"default\" }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast1)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast2)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast3)

        # Clear all graphs
        {:ok, clear_ast} = Parser.parse_update("CLEAR ALL")
        assert {:ok, 3} = UpdateExecutor.execute(ctx, clear_ast)

        # Verify all graphs are empty
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, :default)

        assert {:ok, 0} =
                 QuadOperations.graph_quad_count(db, manager, RDF.iri(graph1))

        assert {:ok, 0} =
                 QuadOperations.graph_quad_count(db, manager, RDF.iri(graph2))
      after
        cleanup({db, manager})
      end
    end

    test "clears named graphs (NAMED)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph1 = "http://example.org/g1"
        graph2 = "http://example.org/g2"

        # Add quads to multiple graphs and default
        {:ok, ast1} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph1}> { <http://example.org/s> <http://example.org/p> \"v1\" } }"
          )

        {:ok, ast2} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <#{graph2}> { <http://example.org/s> <http://example.org/p> \"v2\" } }"
          )

        {:ok, ast3} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"default\" }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast1)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast2)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast3)

        # Clear named graphs only
        {:ok, clear_ast} = Parser.parse_update("CLEAR NAMED")
        assert {:ok, 2} = UpdateExecutor.execute(ctx, clear_ast)

        # Verify named graphs are empty but default has data
        assert {:ok, 1} = QuadOperations.graph_quad_count(db, manager, :default)
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, RDF.iri(graph1))
        assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, RDF.iri(graph2))
      after
        cleanup({db, manager})
      end
    end
  end

  # ===========================================================================
  # Direct Function Tests
  # ===========================================================================

  describe "QuadOperations.create_graph/3" do
    test "creates graph and returns :created", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)

      try do
        graph = RDF.iri("http://example.org/new")

        assert {:ok, :created} = QuadOperations.create_graph(db, manager, graph)
      after
        cleanup({db, manager})
      end
    end

    test "returns :already_exists for existing graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)

      try do
        graph = RDF.iri("http://example.org/test")

        # First call creates
        assert {:ok, :created} = QuadOperations.create_graph(db, manager, graph)

        # Second call acknowledges existence
        # Implementation now checks if graph exists using lookup_id first
        assert {:ok, :already_exists} = QuadOperations.create_graph(db, manager, graph)
      after
        cleanup({db, manager})
      end
    end
  end

  describe "UpdateExecutor.execute_create_graph/3" do
    test "executes CREATE GRAPH operation", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        props = [graph: {:named_node, "http://example.org/test"}]
        assert {:ok, 0} = UpdateExecutor.execute_create_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "returns error for missing graph IRI", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        props = []
        assert {:error, :missing_graph_iri} = UpdateExecutor.execute_create_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "returns error for default graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        props = [graph: :default]
        assert {:error, :default_graph_exists} = UpdateExecutor.execute_create_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end
  end

  describe "UpdateExecutor.execute_drop_graph/3" do
    test "executes DROP GRAPH operation", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        graph_iri = "http://example.org/to_drop"

        # Add a quad first
        quad = [
          {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
           {:literal, :simple, "value"}, {:named_node, graph_iri}}
        ]

        assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quad)

        # Drop the graph
        props = [graph: {:named_node, graph_iri}]
        assert {:ok, 1} = UpdateExecutor.execute_drop_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "returns ok for missing graph (no-op)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        props = [graph: {:named_node, "http://example.org/nonexistent"}]
        # Dropping a non-existent graph returns ok with 0 count (no-op)
        assert {:ok, 0} = UpdateExecutor.execute_drop_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "returns ok for missing graph with SILENT", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        props = [graph: {:named_node, "http://example.org/nonexistent"}, silent: true]
        assert {:ok, 0} = UpdateExecutor.execute_drop_graph(ctx, props)
      after
        cleanup({db, manager})
      end
    end
  end

  describe "UpdateExecutor.execute_clear/2" do
    test "executes CLEAR DEFAULT", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        # Add data to default graph
        {:ok, ast} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"value\" }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

        # Clear default graph
        props = [graph: :default]
        assert {:ok, 1} = UpdateExecutor.execute_clear(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "executes CLEAR ALL", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        # Add data to default and named graph
        {:ok, ast1} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"default\" }"
          )

        {:ok, ast2} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <http://example.org/g1> { <http://example.org/s> <http://example.org/p> \"named\" } }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast1)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast2)

        # Clear all graphs
        props = [graph: :all]
        assert {:ok, 2} = UpdateExecutor.execute_clear(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "executes CLEAR NAMED", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        # Add data to default and named graph
        {:ok, ast1} =
          Parser.parse_update(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"default\" }"
          )

        {:ok, ast2} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <http://example.org/g1> { <http://example.org/s> <http://example.org/p> \"named\" } }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast1)
        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast2)

        # Clear named graphs only
        props = [graph: :named]
        assert {:ok, 1} = UpdateExecutor.execute_clear(ctx, props)
      after
        cleanup({db, manager})
      end
    end

    test "executes CLEAR for specific graph", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = create_context(db, manager)

      try do
        # Add data to specific graph
        {:ok, ast} =
          Parser.parse_update(
            "INSERT DATA { GRAPH <http://example.org/g1> { <http://example.org/s> <http://example.org/p> \"value\" } }"
          )

        assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

        # Clear specific graph
        props = [graph: "http://example.org/g1"]
        assert {:ok, 1} = UpdateExecutor.execute_clear(ctx, props)
      after
        cleanup({db, manager})
      end
    end
  end
end
