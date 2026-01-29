defmodule TripleStore.SPARQL.UpdateAuthorizationTest do
  @moduledoc """
  Comprehensive integration tests for UPDATE operation authorization.

  These tests verify that all SPARQL UPDATE operations properly check
  user permissions before executing.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :integration
  @moduletag :update_authorization

  setup do
    test_id = :erlang.unique_integer([:positive, :monotonic])
    db_path = Path.join(System.tmp_dir!(), "update_auth_test_#{test_id}")

    File.rm_rf!(db_path)

    # Open quad store for testing
    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    # Create test users
    admin_user = %{id: "admin", roles: [:admin]}
    editor_user = %{id: "editor", roles: [:editor]}
    viewer_user = %{id: "viewer", roles: [:viewer]}

    ctx = %{
      db: db,
      dict_manager: manager
    }

    admin_ctx = Map.put(ctx, :user, admin_user)
    editor_ctx = Map.put(ctx, :user, editor_user)
    viewer_ctx = Map.put(ctx, :user, viewer_user)
    public_ctx = ctx

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf!(db_path)
    end)

    {:ok,
     %{
       ctx: ctx,
       admin_ctx: admin_ctx,
       editor_ctx: editor_ctx,
       viewer_ctx: viewer_ctx,
       public_ctx: public_ctx,
       db: db,
       manager: manager
     }}
  end

  # Helper to set up a named graph with ACL
  defp setup_graph_with_acl(ctx, graph_iri, permission_type, user) do
    # Insert a quad to create the graph
    s = RDF.iri("http://example.org/subject")
    p = RDF.iri("http://example.org/predicate")
    o = RDF.literal("object")
    g = RDF.iri(graph_iri)

    {:ok, s_id} = Manager.get_or_create_id(ctx.dict_manager, s)
    {:ok, p_id} = Manager.get_or_create_id(ctx.dict_manager, p)
    {:ok, o_id} = Manager.get_or_create_id(ctx.dict_manager, o)
    {:ok, g_id} = Manager.get_or_create_id(ctx.dict_manager, g)

    TripleStore.QuadOperations.insert_quad(ctx.db, {s_id, p_id, o_id, g_id})

    # Set ACL for the user
    :ok = Authorization.grant(ctx, graph_iri, user.id, permission_type)
  end

  # Helper to insert test data
  defp insert_test_data(ctx, graph_iri) do
    insert_sparql = """
    INSERT DATA {
      GRAPH <#{graph_iri}> {
        <http://example.org/s1> <http://example.org/p> "v1" .
        <http://example.org/s2> <http://example.org/p> "v2" .
      }
    }
    """

    {:ok, ast} = Parser.parse_update(insert_sparql)
    {:ok, _} = UpdateExecutor.execute(ctx, ast)
  end

  # ===========================================================================
  # INSERT DATA Authorization Tests
  # ===========================================================================

  describe "INSERT DATA authorization" do
    test "requires write permission on target graph", %{
      admin_ctx: admin_ctx,
      viewer_ctx: viewer_ctx,
      public_ctx: public_ctx
    } do
      graph_iri = "http://example.org/test_graph"

      # Setup: Admin creates graph and grants themselves write access
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)

      # Admin can INSERT DATA
      insert_sparql = """
      INSERT DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(insert_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(admin_ctx, ast)

      # Viewer without write permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)

      # Public context (no user) succeeds for internal operations
      assert {:ok, 1} = UpdateExecutor.execute(public_ctx, ast)
    end

    test "allows INSERT DATA to default graph without authorization", %{viewer_ctx: ctx} do
      # Default graph doesn't require explicit authorization
      insert_sparql = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "value" .
      }
      """

      {:ok, ast} = Parser.parse_update(insert_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)
    end
  end

  # ===========================================================================
  # DELETE DATA Authorization Tests
  # ===========================================================================

  describe "DELETE DATA authorization" do
    test "requires write permission on target graph", %{
      admin_ctx: admin_ctx,
      viewer_ctx: viewer_ctx
    } do
      graph_iri = "http://example.org/test_graph"

      # Setup: Admin creates graph with data
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)
      insert_test_data(admin_ctx, graph_iri)

      # Admin can DELETE DATA
      delete_sparql = """
      DELETE DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s1> <http://example.org/p> "v1" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(delete_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(admin_ctx, ast)

      # Viewer without write permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # CREATE GRAPH Authorization Tests
  # ===========================================================================

  describe "CREATE GRAPH authorization" do
    test "requires admin permission on graph", %{
      admin_ctx: admin_ctx,
      editor_ctx: editor_ctx,
      viewer_ctx: viewer_ctx
    } do
      graph_iri = "http://example.org/new_graph"

      # Admin can CREATE GRAPH
      create_sparql = "CREATE GRAPH <#{graph_iri}>"
      {:ok, ast} = Parser.parse_update(create_sparql)

      assert {:ok, 0} = UpdateExecutor.execute(admin_ctx, ast)

      # Editor without admin permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(editor_ctx, ast)

      # Viewer without admin permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # DROP GRAPH Authorization Tests
  # ===========================================================================

  describe "DROP GRAPH authorization" do
    test "requires admin permission on graph", %{
      admin_ctx: admin_ctx,
      editor_ctx: editor_ctx,
      viewer_ctx: viewer_ctx
    } do
      graph_iri = "http://example.org/drop_test"

      # Setup: Admin creates graph (setup_graph_with_acl adds 1 quad)
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)

      # Admin can DROP GRAPH (deletes 1 quad from setup)
      drop_sparql = "DROP GRAPH <#{graph_iri}>"
      {:ok, ast} = Parser.parse_update(drop_sparql)

      assert {:ok, 1} = UpdateExecutor.execute(admin_ctx, ast)

      # Re-create for next test
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)

      # Editor without admin permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(editor_ctx, ast)

      # Viewer without admin permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # CLEAR GRAPH Authorization Tests
  # ===========================================================================

  describe "CLEAR GRAPH authorization" do
    test "requires write permission on graph", %{admin_ctx: admin_ctx, viewer_ctx: viewer_ctx} do
      graph_iri = "http://example.org/clear_test"

      # Setup: Admin creates graph with data
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)
      insert_test_data(admin_ctx, graph_iri)

      # Admin can CLEAR GRAPH (clears 3 quads: 1 from setup + 2 from insert_test_data)
      clear_sparql = "CLEAR GRAPH <#{graph_iri}>"
      {:ok, ast} = Parser.parse_update(clear_sparql)

      assert {:ok, 3} = UpdateExecutor.execute(admin_ctx, ast)

      # Re-insert data for viewer test
      insert_test_data(admin_ctx, graph_iri)

      # Viewer without write permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # COPY Authorization Tests
  # ===========================================================================

  describe "COPY authorization" do
    test "requires read on source and write on target", %{
      admin_ctx: admin_ctx,
      editor_ctx: editor_ctx,
      viewer_ctx: viewer_ctx
    } do
      source_graph = "http://example.org/copy_source_auth"
      target_graph = "http://example.org/copy_target_auth"

      # Clean up any existing data from previous test runs
      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(source_graph)
      )

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      # Setup: Create source graph with data, target graph empty
      setup_graph_with_acl(admin_ctx, source_graph, :read, admin_ctx.user)
      setup_graph_with_acl(admin_ctx, target_graph, :write, admin_ctx.user)

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      insert_test_data(admin_ctx, source_graph)

      # Grant editor read access to source, write access to target
      Authorization.grant(admin_ctx, source_graph, editor_ctx.user.id, :read)
      Authorization.grant(admin_ctx, target_graph, editor_ctx.user.id, :write)

      # Verify source count
      {:ok, source_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(source_graph)
        )

      # Admin can COPY - call execute_copy directly to avoid parser issues
      assert {:ok, copied} = UpdateExecutor.execute_copy(admin_ctx, source_graph, target_graph)
      # Copied count should equal source count
      assert copied == source_count

      # Verify final state
      {:ok, final_source_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(source_graph)
        )

      {:ok, final_target_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(target_graph)
        )

      # Source should still have data (COPY doesn't remove)
      assert final_source_count == source_count
      # Target should have a copy
      assert final_target_count == source_count

      # Editor with correct permissions can also COPY (adds to target)
      assert {:ok, copied2} = UpdateExecutor.execute_copy(editor_ctx, source_graph, target_graph)
      # Target should now have 2x the source count (merge mode)
      assert copied2 == source_count

      # Viewer without permissions is denied
      assert {:error, :unauthorized} =
               UpdateExecutor.execute_copy(viewer_ctx, source_graph, target_graph)
    end
  end

  # ===========================================================================
  # MOVE Authorization Tests
  # ===========================================================================

  describe "MOVE authorization" do
    test "requires admin permission on both source and target", %{
      admin_ctx: admin_ctx,
      editor_ctx: editor_ctx,
      viewer_ctx: _viewer_ctx
    } do
      source_graph = "http://example.org/move_source_auth_test"
      target_graph = "http://example.org/move_target_auth_test"

      # Clean up any existing data from previous test runs
      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(source_graph)
      )

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      # Setup: Create source graph with data, target empty
      setup_graph_with_acl(admin_ctx, source_graph, :admin, admin_ctx.user)
      setup_graph_with_acl(admin_ctx, target_graph, :admin, admin_ctx.user)

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      insert_test_data(admin_ctx, source_graph)

      # Verify source count
      {:ok, source_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(source_graph)
        )

      # Admin can MOVE - call execute_move directly to avoid parser issues
      assert {:ok, moved} = UpdateExecutor.execute_move(admin_ctx, source_graph, target_graph)
      # Moved count should equal source count
      assert moved == source_count, "Expected to move #{source_count} quads, but moved #{moved}"

      # Verify final state
      {:ok, final_source_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(source_graph)
        )

      {:ok, final_target_count} =
        TripleStore.QuadOperations.graph_quad_count(
          admin_ctx.db,
          admin_ctx.dict_manager,
          RDF.iri(target_graph)
        )

      assert final_source_count == 0
      assert final_target_count == source_count

      # Re-setup for editor test
      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(source_graph)
      )

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      setup_graph_with_acl(admin_ctx, source_graph, :admin, admin_ctx.user)

      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      insert_test_data(admin_ctx, source_graph)

      # Editor without admin permission is denied
      assert {:error, :unauthorized} =
               UpdateExecutor.execute_move(editor_ctx, source_graph, target_graph)
    end
  end

  # ===========================================================================
  # ADD Authorization Tests
  # ===========================================================================

  describe "ADD authorization" do
    test "requires read on source and write on target", %{
      admin_ctx: admin_ctx,
      editor_ctx: editor_ctx,
      viewer_ctx: viewer_ctx
    } do
      source_graph = "http://example.org/add_source"
      target_graph = "http://example.org/add_target"

      # Setup: Create source graph with data, target graph empty
      setup_graph_with_acl(admin_ctx, source_graph, :read, admin_ctx.user)
      setup_graph_with_acl(admin_ctx, target_graph, :write, admin_ctx.user)

      # Clear target graph to remove the setup quad
      TripleStore.QuadOperations.clear_graph(
        admin_ctx.db,
        admin_ctx.dict_manager,
        RDF.iri(target_graph)
      )

      insert_test_data(admin_ctx, source_graph)

      # Grant editor read access to source, write access to target
      Authorization.grant(admin_ctx, source_graph, editor_ctx.user.id, :read)
      Authorization.grant(admin_ctx, target_graph, editor_ctx.user.id, :write)

      # Admin can ADD (3 quads: 1 from setup_graph_with_acl + 2 from insert_test_data)
      add_sparql = "ADD GRAPH <#{source_graph}> TO GRAPH <#{target_graph}>"
      {:ok, ast} = Parser.parse_update(add_sparql)

      assert {:ok, 3} = UpdateExecutor.execute(admin_ctx, ast)

      # Editor with correct permissions can ADD (source still has data)
      assert {:ok, 3} = UpdateExecutor.execute(editor_ctx, ast)

      # Viewer without permissions is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # MODIFY (DELETE/INSERT WHERE) Authorization Tests
  # ===========================================================================

  describe "DELETE/INSERT WHERE authorization" do
    test "requires write permission on affected graphs", %{
      admin_ctx: admin_ctx,
      viewer_ctx: viewer_ctx
    } do
      graph_iri = "http://example.org/modify_test"

      # Setup: Create graph with data
      setup_graph_with_acl(admin_ctx, graph_iri, :write, admin_ctx.user)
      insert_test_data(admin_ctx, graph_iri)

      # Admin can execute DELETE/INSERT WHERE
      modify_sparql = """
      DELETE {
        GRAPH <#{graph_iri}> {
          ?s <http://example.org/p> ?o .
        }
      }
      INSERT {
        GRAPH <#{graph_iri}> {
          ?s <http://example.org/p> "modified" .
        }
      }
      WHERE {
        GRAPH <#{graph_iri}> {
          ?s <http://example.org/p> ?o .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(modify_sparql)

      assert {:ok, 4} = UpdateExecutor.execute(admin_ctx, ast)

      # Viewer without write permission is denied
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end
  end

  # ===========================================================================
  # Error Handling Tests
  # ===========================================================================

  describe "error handling" do
    test "returns specific error for unauthorized operations", %{
      viewer_ctx: viewer_ctx,
      db: db,
      manager: manager
    } do
      # Create a graph the viewer can't access
      graph_iri = "http://example.org/private"
      s = RDF.iri("http://example.org/s")
      p = RDF.iri("http://example.org/p")
      o = RDF.literal("data")
      g = RDF.iri(graph_iri)

      {:ok, s_id} = Manager.get_or_create_id(manager, s)
      {:ok, p_id} = Manager.get_or_create_id(manager, p)
      {:ok, o_id} = Manager.get_or_create_id(manager, o)
      {:ok, g_id} = Manager.get_or_create_id(manager, g)

      TripleStore.QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})

      # Try to INSERT without permission
      insert_sparql = """
      INSERT DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/new> <http://example.org/p> "data" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(insert_sparql)
      assert {:error, :unauthorized} = UpdateExecutor.execute(viewer_ctx, ast)
    end

    test "returns unauthorized for CREATE GRAPH without admin", %{editor_ctx: ctx} do
      create_sparql = "CREATE GRAPH <http://example.org/new_graph>"
      {:ok, ast} = Parser.parse_update(create_sparql)

      assert {:error, :unauthorized} = UpdateExecutor.execute(ctx, ast)
    end

    test "returns unauthorized for DROP GRAPH without admin", %{editor_ctx: ctx} do
      drop_sparql = "DROP GRAPH <http://example.org/some_graph>"
      {:ok, ast} = Parser.parse_update(drop_sparql)

      assert {:error, :unauthorized} = UpdateExecutor.execute(ctx, ast)
    end

    test "returns unauthorized for MOVE without admin", %{editor_ctx: ctx} do
      move_sparql = "MOVE GRAPH <http://example.org/g1> TO GRAPH <http://example.org/g2>"
      {:ok, ast} = Parser.parse_update(move_sparql)

      assert {:error, :unauthorized} = UpdateExecutor.execute(ctx, ast)
    end
  end

  # ===========================================================================
  # Bypass Tests (Internal Operations)
  # ===========================================================================

  describe "internal operations (no user context)" do
    test "allows operations without user context for internal use", %{public_ctx: ctx} do
      graph_iri = "http://example.org/internal_test"

      # CREATE GRAPH works without user
      create_sparql = "CREATE GRAPH <#{graph_iri}>"
      {:ok, create_ast} = Parser.parse_update(create_sparql)
      assert {:ok, 0} = UpdateExecutor.execute(ctx, create_ast)

      # INSERT DATA works without user
      insert_sparql = """
      INSERT DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # DELETE DATA works without user
      delete_sparql = """
      DELETE DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, delete_ast} = Parser.parse_update(delete_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, delete_ast)

      # DROP GRAPH works without user (returns 0 since graph is empty after delete)
      drop_sparql = "DROP GRAPH <#{graph_iri}>"
      {:ok, drop_ast} = Parser.parse_update(drop_sparql)
      assert {:ok, 0} = UpdateExecutor.execute(ctx, drop_ast)
    end
  end
end
