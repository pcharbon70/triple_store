defmodule TripleStore.Benchmark.LDBC.SNBBIQualificationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SNB.BI.Qualification
  alias TripleStore.Benchmark.LDBC.SNB.BI.Workload
  alias TripleStore.Benchmark.LDBC.SNB.Converter
  alias TripleStore.Benchmark.LDBC.StoreFixture
  alias TripleStore.SPARQL.Query

  test "generates stable stage and reference evidence for every canonical variant" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, first} = Qualification.matrix(definitions)
    assert {:ok, repeated} = Qualification.matrix(definitions)

    assert first == repeated
    assert length(first.entries) == 31

    assert first.classifications == %{
             execution_gap: 22,
             extension_adapter_gap: 9
           }

    assert first.reference_coverage == %{contract_fixture: 1, missing: 30}
    assert first.groups[:qualification_context_required] == 22
    assert first.groups[:graph_qualification_context_required] == 9

    standard =
      Enum.reject(
        first.entries,
        &match?({:extension, _, _}, &1.strategy)
      )

    assert Enum.all?(standard, fn entry ->
             entry.stages.parse.status == :pass and
               entry.stages.algebra.status == :pass and
               entry.stages.optimized_plan.status == :pass and
               entry.stages.implementation.status == :pass and
               entry.stages.execution.status == :blocked and
               entry.stages.result_contract.status == :blocked and
               String.starts_with?(entry.stages.parse.digest, "sha256:") and
               String.starts_with?(entry.stages.algebra.digest, "sha256:") and
               String.starts_with?(entry.stages.optimized_plan.digest, "sha256:")
           end)

    [read_12] = entries_for(first, 12)
    assert read_12.classification == :execution_gap
    assert read_12.stages.parse.status == :pass
    assert read_12.stages.algebra.status == :pass
    assert read_12.stages.optimized_plan.status == :pass
    assert read_12.stages.implementation.status == :pass
    assert read_12.stages.execution.reason == :qualification_context_required
    assert read_12.confirmed_gaps == []

    extensions = Enum.filter(first.entries, &match?({:extension, _, _}, &1.strategy))

    assert Enum.all?(extensions, fn entry ->
             entry.stages.parse.status == :not_applicable and
               entry.stages.algebra.status == :not_applicable and
               entry.stages.optimized_plan.status == :not_applicable and
               entry.stages.implementation.status == :pass and
               entry.stages.execution.status == :blocked
           end)
  end

  test "records registered implementations without promoting unqualified operations" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, matrix} = Qualification.matrix(definitions)

    assert length(matrix.operations) == 20
    assert Enum.all?(matrix.operations, & &1.implementation_available?)
    refute Enum.any?(matrix.operations, & &1.qualified?)

    assert Enum.all?(matrix.operations, fn operation ->
             :execution_incomplete in operation.blockers and
               :accepted_answers_incomplete in operation.blockers and
               :result_comparison_incomplete in operation.blockers
           end)

    read_1 = Enum.find(matrix.operations, &String.contains?(&1.operation_id, "read-01@"))
    assert read_1.variants == ["default"]
    refute read_1.accepted_answers_complete?
  end

  test "records shared semantic requirements without claiming they already fail" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, matrix} = Qualification.matrix(definitions)

    read_2 = entries_for(matrix, 2)
    assert Enum.all?(read_2, &(:date_time in &1.semantic_requirements))
    assert Enum.all?(read_2, &(:expression_coercion in &1.semantic_requirements))
    assert Enum.all?(read_2, &(:aggregation in &1.semantic_requirements))

    read_13 = entries_for(matrix, 13)
    assert Enum.all?(read_13, &(:subquery in &1.semantic_requirements))
    assert Enum.all?(read_13, &(:having in &1.semantic_requirements))

    assert Enum.all?(
             matrix.entries,
             &(:typed_result_conversion in &1.semantic_requirements or
                 :typed_numeric_result in &1.semantic_requirements)
           )
  end

  test "canonical quad property paths execute through the active BI dataset",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)

    assert {:ok, definitions} = Workload.load()

    sparql = """
    PREFIX snb: <https://ldbcouncil.org/snb/ontology/>
    SELECT ?message ?post
    FROM <https://ldbcouncil.org/snb/graph/snb_bi/initial>
    FROM <https://ldbcouncil.org/snb/graph/snb_bi/updates>
    WHERE { ?message snb:replyOf* ?post }
    """

    assert {:ok, rows} =
             Query.query(Map.put(fixture.store, :permit_all, true), sparql, timeout: 2_000)

    assert is_list(rows)

    assert {:ok, matrix} = Qualification.matrix(definitions)

    affected =
      matrix.entries
      |> Enum.filter(&(:quad_property_path_column_family in &1.confirmed_gaps))
      |> Enum.map(&variant_label/1)

    assert affected == []
  end

  test "classifies parser algebra execution type and ordering failures independently" do
    passing = %{
      parse: %{status: :pass},
      algebra: %{status: :pass},
      optimized_plan: %{status: :pass},
      execution: %{status: :pass},
      result_contract: %{status: :pass}
    }

    assert Qualification.classify(put_in(passing, [:parse], failure(:syntax)), false) ==
             :parser_gap

    assert Qualification.classify(put_in(passing, [:algebra], failure(:subquery)), false) ==
             :algebra_gap

    assert Qualification.classify(put_in(passing, [:optimized_plan], failure(:having)), false) ==
             :algebra_gap

    assert Qualification.classify(put_in(passing, [:execution], failure(:duration)), false) ==
             :execution_gap

    assert Qualification.classify(put_in(passing, [:execution], failure(:path)), true) ==
             :extension_adapter_gap

    assert Qualification.classify(
             put_in(passing, [:result_contract], failure(:numeric_width)),
             false
           ) == :type_mismatch

    assert Qualification.classify(
             put_in(passing, [:result_contract], failure(:ordering_mismatch)),
             false
           ) == :ordering_mismatch

    assert Qualification.classify(passing, false) == :passing
  end

  defp entries_for(matrix, number), do: Enum.filter(matrix.entries, &(&1.number == number))
  defp failure(reason), do: %{status: :fail, reason: reason}

  defp variant_label(%{number: number, variant: "default"}), do: Integer.to_string(number)

  defp variant_label(%{number: number, variant: variant}),
    do: Integer.to_string(number) <> variant

  defp fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_dir(test) do
    Path.join(
      System.tmp_dir!(),
      "snb_bi_qualification_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
    )
  end
end
