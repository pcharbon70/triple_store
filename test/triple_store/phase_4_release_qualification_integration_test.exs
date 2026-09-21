defmodule TripleStore.Phase4ReleaseQualificationIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.Query.Cache
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.RuleCompiler
  alias TripleStore.Reasoner.RuleOptimizer
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :integration
  @cache_name __MODULE__.ResultCache
  @provenance_cf :derivation_provenance

  @tag scn_005: true, scn_008: true, scn_012: true, scn_017: true
  test "named-graph workflow preserves authorization and committed results through restore" do
    root = unique_root("quad_workflow")
    source_path = Path.join(root, "source")
    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")
    graph_iri = "https://example.test/graphs/release"
    user = %{id: "phase4-editor", roles: []}
    query = graph_query(graph_iri)

    {:ok, store} = TripleStore.open(source_path, schema: :quad)
    {:ok, cache} = Cache.start_link(name: @cache_name)

    on_exit(fn ->
      stop_if_alive(cache)
      safe_close(store)
      File.rm_rf!(root)
    end)

    graph =
      RDF.Graph.new([
        {RDF.iri("https://example.test/subjects/loaded"),
         RDF.iri("https://example.test/predicate"), RDF.literal("loaded")}
      ])

    assert {:ok, 1} = TripleStore.load_graph(store, graph, graph: RDF.iri(graph_iri))
    assert :ok = Authorization.grant(store, graph_iri, user.id, :read)
    assert :ok = Authorization.grant(store, graph_iri, user.id, :write)

    user_ctx = %{db: store.db, dict_manager: store.dict_manager, user: user}

    assert {:ok, [%{"value" => {:literal, :simple, "loaded"}}]} =
             Query.query(user_ctx, query, use_cache: true, cache_name: @cache_name)

    # ACL-governed queries bypass caching. A privileged internal context proves
    # the same store's result-cache generation is invalidated by the request.
    privileged_ctx = Map.put(user_ctx, :permit_all, true)

    assert {:ok, [_]} =
             Query.query(privileged_ctx, query, use_cache: true, cache_name: @cache_name)

    assert Cache.size(name: @cache_name) == 1

    assert {:ok, ast} =
             Parser.parse_update("""
             DELETE DATA {
               GRAPH <#{graph_iri}> {
                 <https://example.test/subjects/loaded>
                   <https://example.test/predicate> "loaded"
               }
             } ;
             INSERT DATA {
               GRAPH <#{graph_iri}> {
                 <https://example.test/subjects/committed>
                   <https://example.test/predicate> "committed"
               }
             }
             """)

    assert {:ok, 2} = UpdateExecutor.execute(user_ctx, ast)
    assert Cache.size(name: @cache_name) == 0
    assert_query_value(user_ctx, query, "committed")

    assert {:ok, _metadata} = TripleStore.backup(store, backup_path, verify: false)
    assert :ok = TripleStore.close(store)

    assert {:ok, restored} = TripleStore.restore(backup_path, restore_path)
    assert restored.schema == :quad
    assert_query_value(user_context(restored, user), query, "committed")
    assert :ok = TripleStore.close(restored)

    assert {:ok, reopened} = TripleStore.open(restore_path, schema: :quad)
    assert_query_value(user_context(reopened, user), query, "committed")
    assert :ok = TripleStore.close(reopened)
  end

  @tag scn_008: true, scn_012: true
  test "triple workflow preserves atomic update and cache state through restore" do
    root = unique_root("triple_workflow")
    source_path = Path.join(root, "source")
    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")
    query = "SELECT ?value WHERE { ?s <https://example.test/predicate> ?value }"

    {:ok, store} = TripleStore.open(source_path)
    {:ok, cache} = Cache.start_link(name: @cache_name)

    on_exit(fn ->
      stop_if_alive(cache)
      safe_close(store)
      File.rm_rf!(root)
    end)

    assert {:ok, 2} =
             TripleStore.load_string(
               store,
               "<https://example.test/subjects/loaded> " <>
                 "<https://example.test/predicate> \"loaded\" .\n" <>
                 "<https://example.test/subjects/loaded> " <>
                 "<https://example.test/age> " <>
                 "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
               :ntriples
             )

    assert {:ok, [%{"value" => {:literal, :simple, "loaded"}}]} =
             TripleStore.query(store, query, use_cache: true, cache_name: @cache_name)

    assert Cache.size(name: @cache_name) == 1

    assert {:ok, 3} =
             TripleStore.update(store, """
             DELETE WHERE {
               <https://example.test/subjects/loaded> ?predicate ?object
             } ;
             INSERT DATA {
               <https://example.test/subjects/committed>
                 <https://example.test/predicate> "committed"
             }
             """)

    assert Cache.size(name: @cache_name) == 0
    assert_triple_query_value(store, query, "committed")

    assert {:ok, _metadata} = TripleStore.backup(store, backup_path, verify: false)
    assert :ok = TripleStore.close(store)

    assert {:ok, restored} = TripleStore.restore(backup_path, restore_path)
    assert restored.schema == :triple
    assert_triple_query_value(restored, query, "committed")
    assert :ok = TripleStore.close(restored)

    assert {:ok, reopened} = TripleStore.open(restore_path)
    assert_triple_query_value(reopened, query, "committed")
    assert :ok = TripleStore.close(reopened)
  end

  @tag scn_010: true, scn_011: true, scn_012: true, scn_017: true
  test "corrupt ACL and provenance remain fail-closed after restore and reopen" do
    root = unique_root("corrupt_restore")
    source_path = Path.join(root, "source")
    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")
    graph_iri = "https://example.test/graphs/corrupt"
    user = %{id: "phase4-reader", roles: []}

    {:ok, store} = TripleStore.open(source_path, schema: :quad)

    on_exit(fn ->
      safe_close(store)
      File.rm_rf!(root)
    end)

    {explicit_rdf, {graph, subject, predicate, object}} =
      encode_quad(store, graph_iri, "explicit")

    {derived_rdf, derived} = encode_quad(store, graph_iri, "derived")

    assert :ok = QuadOperations.insert_quad(store.db, {subject, predicate, object, graph})
    assert :ok = DerivedStore.insert_derived_quads(store.db, [derived])

    acl_key = "acl:graph:#{graph}:user:#{user.id}"
    corrupt_acl = :erlang.term_to_binary(%{"user:#{user.id}" => :read})
    corrupt_provenance = <<131>>

    assert :ok = ErlangAdapter.put(store.db, :acl, acl_key, corrupt_acl)

    assert :ok =
             ErlangAdapter.put(
               store.db,
               @provenance_cf,
               provenance_key(derived),
               corrupt_provenance
             )

    query =
      "SELECT ?value WHERE { GRAPH <#{graph_iri}> { ?s <https://example.test/predicate> ?value } }"

    assert_corruption_errors(store, user, query, derived_rdf, graph)
    assert {:ok, _metadata} = TripleStore.backup(store, backup_path, verify: false)
    assert :ok = TripleStore.close(store)

    assert {:ok, restored} = TripleStore.restore(backup_path, restore_path)
    assert_corruption_errors(restored, user, query, derived_rdf, graph)
    assert {:ok, ^corrupt_acl} = ErlangAdapter.get(restored.db, :acl, acl_key)

    assert {:ok, ^corrupt_provenance} =
             ErlangAdapter.get(restored.db, @provenance_cf, provenance_key(derived))

    assert :ok = TripleStore.close(restored)

    assert {:ok, reopened} = TripleStore.open(restore_path, schema: :quad)
    assert_corruption_errors(reopened, user, query, derived_rdf, graph)
    assert :ok = TripleStore.close(reopened)

    # Keep the explicit RDF value live in this test so the encoded fixture and
    # the query are verified against the same persisted dictionary.
    assert elem(explicit_rdf, 3) == RDF.iri(graph_iri)
  end

  @tag scn_009: true, scn_017: true
  test "combined query and rule identifiers do not grow the atom table" do
    root = unique_root("atom_boundary")
    {:ok, store} = TripleStore.open(Path.join(root, "store"))

    on_exit(fn ->
      safe_close(store)
      File.rm_rf!(root)
    end)

    assert {:ok, []} = run_variable_query(store, "warm")
    warm_reasoning_identifiers(25)
    :erlang.garbage_collect()
    before_count = :erlang.system_info(:atom_count)

    for index <- 1..150 do
      assert {:ok, []} = run_variable_query(store, index)
    end

    warm_reasoning_identifiers(200)
    :erlang.garbage_collect()
    assert :erlang.system_info(:atom_count) == before_count
  end

  defp assert_corruption_errors(store, user, query, derived_rdf, graph_id) do
    assert {:error, {:corrupt_acl, {:invalid_permissions, :read}}} =
             Query.query(user_context(store, user), query, use_cache: false)

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             TripleStore.explain_inference(store, Tuple.delete_at(derived_rdf, 3), graph_id,
               provenance_source: :database
             )
  end

  defp assert_query_value(ctx, query, value) do
    assert {:ok, [%{"value" => {:literal, :simple, ^value}}]} =
             Query.query(ctx, query, use_cache: false)
  end

  defp assert_triple_query_value(store, query, value) do
    assert {:ok, [%{"value" => {:literal, :simple, ^value}}]} =
             TripleStore.query(store, query, use_cache: false)
  end

  defp graph_query(graph_iri) do
    "SELECT ?value WHERE { GRAPH <#{graph_iri}> { ?s <https://example.test/predicate> ?value } }"
  end

  defp user_context(store, user) do
    %{db: store.db, dict_manager: store.dict_manager, user: user}
  end

  defp encode_quad(store, graph_iri, suffix) do
    subject = RDF.iri("https://example.test/subjects/#{suffix}")
    predicate = RDF.iri("https://example.test/predicate")
    object = RDF.literal(suffix)
    graph = RDF.iri(graph_iri)

    {:ok, subject_id} = Adapter.term_to_id(store.dict_manager, subject)
    {:ok, predicate_id} = Adapter.term_to_id(store.dict_manager, predicate)
    {:ok, object_id} = Adapter.term_to_id(store.dict_manager, object)
    {:ok, graph_id} = Adapter.term_to_id(store.dict_manager, graph)

    {{subject, predicate, object, graph}, {graph_id, subject_id, predicate_id, object_id}}
  end

  defp provenance_key({g, s, p, o}), do: <<g::64-big, s::64-big, p::64-big, o::64-big>>

  defp run_variable_query(store, suffix) do
    TripleStore.query(
      store,
      "SELECT ?subject_#{suffix} WHERE { ?subject_#{suffix} " <>
        "<https://example.test/predicate> ?object_#{suffix} }",
      use_cache: false
    )
  end

  defp warm_reasoning_identifiers(count) do
    properties = Enum.map(1..count, &"https://example.test/properties/#{&1}")
    schema = %{RuleCompiler.empty_schema_info() | transitive_properties: properties}

    {:ok, compiled} =
      RuleCompiler.compile_with_schema(schema,
        profile: :owl2rl,
        specialize: true,
        max_specializations: count + 10
      )

    compiled.specialized_rules
    |> RuleOptimizer.optimize_rules()
    |> RuleOptimizer.batch_rules()
  end

  defp unique_root(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "triple_store_phase4_#{suffix}_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)
    root
  end

  defp safe_close(store) do
    if Process.alive?(store.dict_manager), do: TripleStore.close(store)
  catch
    :exit, _ -> :ok
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end
end
