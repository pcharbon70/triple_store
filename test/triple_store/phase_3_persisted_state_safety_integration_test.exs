defmodule TripleStore.Phase3PersistedStateSafetyIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.DeleteWithReasoningQuad
  alias TripleStore.Reasoner.DerivationProvenance
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.RuleCompiler
  alias TripleStore.Reasoner.RuleOptimizer
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Query

  @moduletag :integration
  @provenance_cf :derivation_provenance

  @tag scn_017: true
  test "high-cardinality query variables complete without growing the atom table" do
    store = open_store(:triple, "query_variables")
    on_exit(fn -> cleanup_store(store) end)

    assert {:ok, []} = run_variable_query(store, 0)
    :erlang.garbage_collect()
    before_count = :erlang.system_info(:atom_count)

    for index <- 1..250 do
      assert {:ok, []} = run_variable_query(store, index)
    end

    :erlang.garbage_collect()
    assert :erlang.system_info(:atom_count) == before_count
  end

  @tag scn_010: true, scn_011: true
  test "many external property IRIs compile and optimize deterministically without atom growth" do
    properties = Enum.map(1..300, &"https://example.test/property/#{&1}")
    schema = %{RuleCompiler.empty_schema_info() | transitive_properties: properties}

    {:ok, warmed} = compile_schema(schema)
    warmed.specialized_rules |> RuleOptimizer.optimize_rules() |> RuleOptimizer.batch_rules()
    :erlang.garbage_collect()
    before_count = :erlang.system_info(:atom_count)

    {:ok, first} = compile_schema(schema)
    {:ok, second} = compile_schema(schema)

    first_optimized = RuleOptimizer.optimize_rules(first.specialized_rules)
    second_optimized = RuleOptimizer.optimize_rules(second.specialized_rules)

    assert first_optimized == second_optimized
    assert length(first_optimized) == 300
    assert Enum.all?(first_optimized, &is_binary(&1.name))

    assert RuleOptimizer.batch_rules(first_optimized) ==
             RuleOptimizer.batch_rules(second_optimized)

    :erlang.garbage_collect()
    assert :erlang.system_info(:atom_count) == before_count
  end

  @tag scn_017: true
  test "corrupt ACL data blocks queries and grants without overwriting stored bytes" do
    store = open_store(:quad, "corrupt_acl")
    on_exit(fn -> cleanup_store(store) end)

    graph_iri = "https://example.test/graph/private"
    user = %{id: "phase3-reader", roles: []}
    {_rdf_quad, {_graph, subject, predicate, object} = id_quad} = encode_quad(store, graph_iri)

    assert :ok =
             QuadOperations.insert_quad(store.db, {subject, predicate, object, elem(id_quad, 0)})

    graph_id = elem(id_quad, 0)
    acl_key = "acl:graph:#{graph_id}:user:#{user.id}"
    corrupt = :erlang.term_to_binary(%{"user:#{user.id}" => :read})
    assert :ok = ErlangAdapter.put(store.db, :acl, acl_key, corrupt)

    query =
      "SELECT ?o WHERE { GRAPH <#{graph_iri}> { ?s <https://example.test/predicate> ?o } }"

    ctx = %{db: store.db, dict_manager: store.dict_manager, user: user}

    assert {:error, {:corrupt_acl, {:invalid_permissions, :read}}} =
             Query.query(ctx, query, use_cache: false)

    assert {:error, {:corrupt_acl, {:invalid_permissions, :read}}} =
             Authorization.grant(ctx, graph_iri, user.id, :write)

    assert {:ok, ^corrupt} = ErlangAdapter.get(store.db, :acl, acl_key)
  end

  @tag scn_010: true, scn_011: true
  test "corrupt provenance blocks explanation and maintained deletion without changing facts" do
    store = open_store(:quad, "corrupt_provenance")
    on_exit(fn -> cleanup_store(store) end)

    graph_iri = "https://example.test/graph/reasoning"

    {_explicit_rdf, {graph, subject, predicate, object} = explicit} =
      encode_quad(store, graph_iri, "explicit")

    {derived_rdf, {_graph, _derived_subject, _derived_predicate, _derived_object} = derived} =
      encode_quad(store, graph_iri, "derived")

    assert :ok = QuadOperations.insert_quad(store.db, {subject, predicate, object, graph})
    assert :ok = DerivedStore.insert_derived_quads(store.db, [derived])

    corrupt_key = provenance_key(derived)
    corrupt = <<131>>
    assert :ok = ErlangAdapter.put(store.db, @provenance_cf, corrupt_key, corrupt)

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             TripleStore.explain_inference(store, Tuple.delete_at(derived_rdf, 3), graph,
               provenance_source: :database
             )

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             DeleteWithReasoningQuad.delete_quads_with_reasoning(store.db, [explicit], [],
               graph_id: graph,
               emit_telemetry: false
             )

    assert QuadOperations.quad_exists?(store.db, {subject, predicate, object, graph})
    assert {:ok, true} = DerivedStore.derived_quad_exists?(store.db, derived)
    assert {:ok, ^corrupt} = ErlangAdapter.get(store.db, @provenance_cf, corrupt_key)
  end

  @tag scn_010: true, scn_012: true, scn_017: true
  test "quad backup restore and reopen preserve ACL policy and provenance lineage" do
    store = open_store(:quad, "backup_source")
    on_exit(fn -> cleanup_store(store) end)
    root = Path.dirname(store.path)
    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")
    graph_iri = "https://example.test/graph/backup"
    user = %{id: "phase3-backup-reader", roles: []}

    {derived_rdf, {graph, _subject, _predicate, _object} = derived} =
      encode_quad(store, graph_iri, "restored-derived")

    tracker =
      DerivationProvenance.new()
      |> DerivationProvenance.record_derivation(derived,
        rule_name: "phase3_backup_rule",
        premises: [],
        bindings: %{},
        metadata: %{graph_id: graph, scope: :local, iteration: 1}
      )

    assert :ok = Authorization.grant(store, graph_iri, user.id, :read)
    assert :ok = DerivedStore.insert_derived_quads(store.db, [derived])
    assert :ok = DerivationProvenance.save(store.db, tracker)
    assert {:ok, _metadata} = TripleStore.backup(store, backup_path, verify: false)
    cleanup_store(store, remove_path: false)

    assert {:ok, restored} = TripleStore.restore(backup_path, restore_path)
    on_exit(fn -> cleanup_store(restored) end)
    assert restored.schema == :quad
    assert_restored_policy_and_lineage(restored, graph_iri, user, derived_rdf, graph)

    cleanup_store(restored, remove_path: false)
    assert {:ok, reopened} = TripleStore.open(restore_path, schema: :quad)
    on_exit(fn -> cleanup_store(reopened) end)
    assert_restored_policy_and_lineage(reopened, graph_iri, user, derived_rdf, graph)
  end

  defp run_variable_query(store, index) do
    TripleStore.query(
      store,
      "SELECT ?subject_#{index} WHERE { ?subject_#{index} " <>
        "<https://example.test/predicate> ?object_#{index} }",
      use_cache: false
    )
  end

  defp compile_schema(schema) do
    RuleCompiler.compile_with_schema(schema,
      profile: :owl2rl,
      specialize: true,
      max_specializations: 500
    )
  end

  defp encode_quad(store, graph_iri, suffix \\ "object") do
    subject = RDF.iri("https://example.test/subject/#{suffix}")
    predicate = RDF.iri("https://example.test/predicate")
    object = RDF.literal(suffix)
    graph = RDF.iri(graph_iri)

    {:ok, subject_id} = Adapter.term_to_id(store.dict_manager, subject)
    {:ok, predicate_id} = Adapter.term_to_id(store.dict_manager, predicate)
    {:ok, object_id} = Adapter.term_to_id(store.dict_manager, object)
    {:ok, graph_id} = Adapter.term_to_id(store.dict_manager, graph)

    {{subject, predicate, object, graph}, {graph_id, subject_id, predicate_id, object_id}}
  end

  defp assert_restored_policy_and_lineage(store, graph_iri, user, derived_rdf, graph_id) do
    assert {:ok, true} = Authorization.can_read?(store, graph_iri, user)

    assert {:ok, explanation} =
             TripleStore.explain_inference(store, Tuple.delete_at(derived_rdf, 3), graph_id,
               provenance_source: :database
             )

    assert explanation.rule_name == "phase3_backup_rule"
    assert explanation.graph_id == graph_id
  end

  defp provenance_key({g, s, p, o}), do: <<g::64-big, s::64-big, p::64-big, o::64-big>>

  defp open_store(schema, name) do
    root =
      Path.join(
        System.tmp_dir!(),
        "triple_store_phase3_#{name}_#{System.unique_integer([:positive])}"
      )

    path = Path.join(root, "db")
    File.rm_rf!(root)
    File.mkdir_p!(root)
    {:ok, store} = TripleStore.open(path, schema: schema)
    store
  end

  defp cleanup_store(store, opts \\ []) do
    if Process.alive?(store.dict_manager) do
      try do
        TripleStore.close(store)
      catch
        _, _ -> :ok
      end
    end

    if Keyword.get(opts, :remove_path, true), do: File.rm_rf(Path.dirname(store.path))
    :ok
  end
end
