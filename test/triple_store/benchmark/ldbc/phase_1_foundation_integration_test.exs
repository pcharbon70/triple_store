defmodule TripleStore.Benchmark.LDBC.Phase1FoundationIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{CapabilityMatrix, Catalog, Foundation, SourceManifest}
  alias TripleStore.SPARQL.Parser
  alias TripleStore.Transaction

  @moduletag :integration

  test "the immutable metadata, catalogs, profiles, and capability matrix pass one gate" do
    assert {:ok, report} = Foundation.validate()

    assert report.source_count == 8
    assert report.profile_count == 13
    assert report.catalog_count == 4
    assert report.operation_count == 119
    assert report.finding_count == 12
    assert report.architecture_decision == "ADR-0002"
    assert map_size(report.catalog_digests) == 4
    assert report.capability_summary.requires_extension == 6
    assert report.capability_summary.profile_exclusion == 17
  end

  test "moving source refs and unknown operation versions fail closed" do
    {:ok, sources} = SourceManifest.load()
    [source | rest] = sources

    assert {:error, source_errors} = SourceManifest.validate([%{source | release: "main"} | rest])
    assert Enum.any?(source_errors, &match?({_, :release, _}, &1))

    {:ok, [catalog | other_catalogs]} = Catalog.load_all()
    [operation | other_operations] = catalog.operations
    invalid_operation = %{operation | id: String.replace(operation.id, ~r/@[^@]+$/, "@latest")}

    invalid_catalog = %{
      catalog
      | operations: [invalid_operation | other_operations],
        expected_operation_ids: [invalid_operation.id | Enum.map(other_operations, & &1.id)]
    }

    assert {:error, catalog_errors} =
             Catalog.validate([invalid_catalog | other_catalogs], sources)

    assert Enum.any?(catalog_errors, &match?({_, :operation_version, _}, &1))
  end

  test "representative operations from all suites reach the real parser" do
    for probe <- CapabilityMatrix.parser_probes() do
      result =
        case probe.parser do
          :query -> Parser.parse(probe.sparql)
          :update -> Parser.parse_update(probe.sparql)
        end

      assert {:ok, _ast} = result, "parser probe failed: #{probe.id}: #{inspect(result)}"
    end

    {:ok, matrix} = CapabilityMatrix.load()

    assert Enum.all?(matrix.operations, fn entry ->
             entry.parse_support in [
               :verified,
               :template_requires_binding,
               :translation_required,
               :not_applicable
             ] and
               entry.execution_support in [:verified, :unverified, :unsupported, :not_applicable]
           end)
  end

  test "coordinated update and graph reasoning paths execute through a real quad store" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ldbc_phase_1_store_#{System.unique_integer([:positive])}"
      )

    {:ok, store} = TripleStore.open(path, schema: :quad)

    on_exit(fn ->
      safe_close(store)
      File.rm_rf!(path)
    end)

    assert is_pid(store.transaction)

    assert {:ok, 2} =
             TripleStore.update(store, """
             INSERT DATA {
               <urn:ldbc:Student> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:ldbc:Person> .
               <urn:ldbc:Alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:ldbc:Student> .
             }
             """)

    query = """
    SELECT ?type WHERE {
      <urn:ldbc:Alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type
    }
    ORDER BY ?type
    """

    assert {:ok, direct_results} = TripleStore.query(store, query)
    assert {:ok, coordinated_results} = Transaction.query(store.transaction, query)
    assert Enum.to_list(direct_results) == Enum.to_list(coordinated_results)

    assert {:ok, stats} =
             TripleStore.materialize_graph(store, 0,
               profile: :rdfs,
               tbox_graph: 0,
               parallel: false
             )

    assert stats.graph_id == 0
    assert stats.total_derived == 0

    assert {:ok, status} = TripleStore.reasoning_status(store, graph_id: 0)
    assert status.state == :materialized
    assert status.derived_count == 0

    {:ok, matrix} = CapabilityMatrix.load()
    reasoning_gap = Enum.find(matrix.system_findings, &(&1.id == "LDBC-CAP-005"))
    assert reasoning_gap.status == :requires_fix
    assert reasoning_gap.implementation_phase == 4
  end

  defp safe_close(store) do
    TripleStore.close(store)
  catch
    :exit, _reason -> :ok
  end
end
