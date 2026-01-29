defmodule TripleStore.SPARQL.AuthorizationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.Authorization

  @moduletag :integration
  @moduletag :authorization

  @test_db_base "/tmp/triple_store_auth_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive, :monotonic])}"

    # Ensure clean directory
    File.rm_rf(test_path)

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{
      db: db,
      dict_manager: manager
    }

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, %{ctx: ctx, db: db, manager: manager}}
  end

  # Helper function to insert a quad for testing
  defp insert_test_quad(db, manager, graph_iri) do
    # Create test terms
    s = RDF.iri("http://example.org/subject")
    p = RDF.iri("http://example.org/predicate")
    o = RDF.literal("object")
    g = RDF.iri(graph_iri)

    # Get term IDs
    {:ok, s_id} = Manager.get_or_create_id(manager, s)
    {:ok, p_id} = Manager.get_or_create_id(manager, p)
    {:ok, o_id} = Manager.get_or_create_id(manager, o)
    {:ok, g_id} = Manager.get_or_create_id(manager, g)

    TripleStore.QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
  end

  # ===========================================================================
  # Permission Checking Tests
  # ===========================================================================

  describe "can_read?/3" do
    test "returns true for default graph when no ACL is set", %{ctx: ctx} do
      # The actual default graph (ID 0) is always readable
      assert {:ok, true} = Authorization.can_access_graph?(ctx, :default_graph, :public, :read)
    end

    test "returns true for public graph when public ACL is set", %{
      db: db,
      ctx: ctx,
      manager: manager
    } do
      graph_iri = "http://example.org/public_graph"
      insert_test_quad(db, manager, graph_iri)

      :ok = Authorization.set_public(ctx, graph_iri)

      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, :public)
    end

    test "returns false for private graph when no ACL is set", %{
      db: db,
      ctx: ctx,
      manager: manager
    } do
      graph_iri = "http://example.org/private_graph"
      insert_test_quad(db, manager, graph_iri)

      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, :public)
    end

    test "returns true for user with explicit permission", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/user_graph"
      insert_test_quad(db, manager, graph_iri)

      user_id = "user123"
      :ok = Authorization.grant(ctx, graph_iri, user_id, :read)

      user = %{id: user_id}
      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)
    end

    test "returns true for user with role permission", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/role_graph"
      insert_test_quad(db, manager, graph_iri)

      role = :editor
      :ok = Authorization.grant_role(ctx, graph_iri, role, :read)

      user = %{id: "user456", roles: [role]}
      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)
    end

    test "returns true for owner", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/owned_graph"
      insert_test_quad(db, manager, graph_iri)

      user_id = "owner123"
      :ok = Authorization.set_owner(ctx, graph_iri, user_id)

      user = %{id: user_id}
      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)
      assert {:ok, true} = Authorization.can_write?(ctx, graph_iri, user)
      assert {:ok, true} = Authorization.can_admin?(ctx, graph_iri, user)
    end
  end

  # ===========================================================================
  # Permission Management Tests
  # ===========================================================================

  describe "grant/4 and revoke/4" do
    test "grants and revokes user permissions", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/test_graph"
      insert_test_quad(db, manager, graph_iri)

      user_id = "user123"
      user = %{id: user_id}

      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, user)

      :ok = Authorization.grant(ctx, graph_iri, user_id, :read)
      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)

      :ok = Authorization.revoke(ctx, graph_iri, user_id, :read)
      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, user)
    end

    test "grants multiple permissions to same user", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/multi_perm_graph"
      insert_test_quad(db, manager, graph_iri)

      user_id = "user123"
      user = %{id: user_id}

      :ok = Authorization.grant(ctx, graph_iri, user_id, :read)
      :ok = Authorization.grant(ctx, graph_iri, user_id, :write)
      :ok = Authorization.grant(ctx, graph_iri, user_id, :admin)

      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)
      assert {:ok, true} = Authorization.can_write?(ctx, graph_iri, user)
      assert {:ok, true} = Authorization.can_admin?(ctx, graph_iri, user)

      :ok = Authorization.revoke(ctx, graph_iri, user_id, :write)

      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)
      assert {:ok, false} = Authorization.can_write?(ctx, graph_iri, user)
      assert {:ok, true} = Authorization.can_admin?(ctx, graph_iri, user)
    end
  end

  describe "set_public/2 and remove_public/2" do
    test "sets and removes public access", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/public_graph"
      insert_test_quad(db, manager, graph_iri)

      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, :public)

      :ok = Authorization.set_public(ctx, graph_iri)
      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, :public)

      :ok = Authorization.remove_public(ctx, graph_iri)
      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, :public)
    end
  end

  describe "set_owner/3 and get_owner/2" do
    test "sets and gets graph owner", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/owned_graph"
      insert_test_quad(db, manager, graph_iri)

      assert {:ok, nil} = Authorization.get_owner(ctx, graph_iri)

      user_id = "owner123"
      :ok = Authorization.set_owner(ctx, graph_iri, user_id)

      assert {:ok, ^user_id} = Authorization.get_owner(ctx, graph_iri)
    end
  end

  # ===========================================================================
  # Graph Listing Tests
  # ===========================================================================

  describe "list_accessible_graphs/4" do
    test "lists only graphs accessible to user", %{db: db, ctx: ctx, manager: manager} do
      public_graph = "http://example.org/public"
      private_graph = "http://example.org/private"
      user_graph = "http://example.org/user_only"

      Enum.each(
        [public_graph, private_graph, user_graph],
        fn iri ->
          insert_test_quad(db, manager, iri)
        end
      )

      :ok = Authorization.set_public(ctx, public_graph)

      user_id = "user123"
      user = %{id: user_id}
      :ok = Authorization.grant(ctx, user_graph, user_id, :read)

      {:ok, accessible} = Authorization.list_accessible_graphs(ctx, user, :read)

      assert public_graph in accessible
      assert user_graph in accessible
      refute private_graph in accessible
    end

    test "lists only public graphs for public user", %{db: db, ctx: ctx, manager: manager} do
      public_graph = "http://example.org/public"
      private_graph = "http://example.org/private"

      Enum.each(
        [public_graph, private_graph],
        fn iri ->
          insert_test_quad(db, manager, iri)
        end
      )

      :ok = Authorization.set_public(ctx, public_graph)

      {:ok, accessible} = Authorization.list_accessible_graphs(ctx, :public, :read)

      assert public_graph in accessible
      refute private_graph in accessible
    end
  end

  # ===========================================================================
  # can_access_graph?/4 Tests
  # ===========================================================================

  describe "can_access_graph?/4" do
    test "works with different graph term formats", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/test_graph"
      insert_test_quad(db, manager, graph_iri)

      :ok = Authorization.set_public(ctx, graph_iri)

      rdf_iri = RDF.iri(graph_iri)
      assert {:ok, true} = Authorization.can_access_graph?(ctx, rdf_iri, :public, :read)

      assert {:ok, true} =
               Authorization.can_access_graph?(ctx, {:named_node, graph_iri}, :public, :read)

      assert {:ok, true} = Authorization.can_access_graph?(ctx, :default_graph, :public, :read)

      assert {:ok, true} = Authorization.can_access_graph?(ctx, :default, :public, :read)
    end
  end

  # ===========================================================================
  # ACL Storage Tests
  # ===========================================================================

  describe "ACL storage" do
    test "persists ACL entries", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/persistent"
      insert_test_quad(db, manager, graph_iri)

      user_id = "user123"
      user = %{id: user_id}

      :ok = Authorization.grant(ctx, graph_iri, user_id, :read)

      assert {:ok, true} = Authorization.can_read?(ctx, graph_iri, user)

      # Get the actual graph ID
      {:ok, graph_id} = Manager.get_or_create_id(manager, RDF.iri(graph_iri))
      acl_key = "acl:graph:#{graph_id}:user:#{user_id}"
      {:ok, binary} = ErlangAdapter.get(db, :acl, acl_key)
      acl_entry = :erlang.binary_to_term(binary)
      assert Map.has_key?(acl_entry, "user:#{user_id}")
      assert :read in acl_entry["user:#{user_id}"]
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "telemetry events" do
    test "emits telemetry when public access is denied", %{db: db, ctx: ctx, manager: manager} do
      graph_iri = "http://example.org/private"
      insert_test_quad(db, manager, graph_iri)

      # Attach telemetry handler
      handler_id = "test-auth-denied-public--#{:erlang.unique_integer()}"
      parent = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :authorization, :denied],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:auth_denied, metadata})
        end,
        nil
      )

      # Try to access private graph as public
      assert {:ok, false} = Authorization.can_read?(ctx, graph_iri, :public)

      # Verify telemetry was emitted
      assert_receive {:auth_denied, metadata}
      assert metadata.graph == graph_iri
      assert metadata.user == :public
      assert metadata.permission == :read
      assert metadata.reason == :no_public_access

      :telemetry.detach(handler_id)
    end

    test "emits telemetry when role-based permission is denied", %{
      db: db,
      ctx: ctx,
      manager: manager
    } do
      graph_iri = "http://example.org/restricted"
      insert_test_quad(db, manager, graph_iri)

      user_id = "user456"
      user = %{id: user_id, roles: [:viewer]}

      # Attach telemetry handler
      handler_id = "test-auth-denied-role--#{:erlang.unique_integer()}"
      parent = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :authorization, :denied],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:auth_denied, metadata})
        end,
        nil
      )

      # Try to access restricted graph with user that has no matching role permissions
      assert {:ok, false} = Authorization.can_write?(ctx, graph_iri, user)

      # Verify telemetry was emitted
      assert_receive {:auth_denied, metadata}
      assert metadata.graph == graph_iri
      assert metadata.user == user_id
      assert metadata.permission == :write
      assert metadata.reason == :no_role_permission

      :telemetry.detach(handler_id)
    end
  end
end
