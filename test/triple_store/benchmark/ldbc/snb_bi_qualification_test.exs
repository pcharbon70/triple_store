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
             execution_gap: 21,
             extension_adapter_gap: 9,
             parser_gap: 1
           }

    assert first.reference_coverage == %{embedded: 1, missing: 30}
    assert first.groups[:standard_handler_registry] == 21

    standard =
      Enum.reject(
        first.entries,
        &(match?({:extension, _, _}, &1.strategy) or &1.number == 12)
      )

    assert Enum.all?(standard, fn entry ->
             entry.stages.parse.status == :pass and
               entry.stages.algebra.status == :pass and
               entry.stages.optimized_plan.status == :pass and
               entry.stages.execution.status == :fail and
               entry.stages.result_contract.status == :blocked and
               String.starts_with?(entry.stages.parse.digest, "sha256:") and
               String.starts_with?(entry.stages.algebra.digest, "sha256:") and
               String.starts_with?(entry.stages.optimized_plan.digest, "sha256:")
           end)

    [read_12] = entries_for(first, 12)
    assert read_12.classification == :parser_gap
    assert read_12.stages.parse.status == :fail
    assert match?({:parse_error, _reason}, read_12.stages.parse.reason)
    assert :typed_list_parameter_values in read_12.confirmed_gaps

    extensions = Enum.filter(first.entries, &match?({:extension, _, _}, &1.strategy))

    assert Enum.all?(extensions, fn entry ->
             entry.stages.parse.status == :not_applicable and
               entry.stages.algebra.status == :not_applicable and
               entry.stages.optimized_plan.status == :not_applicable and
               entry.stages.execution.status == :fail
           end)
  end

  test "groups the nine graph variants by their registered extension contract" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, matrix} = Qualification.matrix(definitions)

    graph_groups =
      matrix.entries
      |> Enum.filter(&(&1.classification == :extension_adapter_gap))
      |> Enum.frequencies_by(& &1.gap_group)

    assert graph_groups == %{
             {:extension_adapter, "ldbc.snb.bi.shortest-path-range.v1"} => 2,
             {:extension_adapter, "ldbc.snb.bi-bounded-path.v1"} => 2,
             {:extension_adapter, "ldbc.snb.bi-weighted-interaction-path.v1"} => 2,
             {:extension_adapter, "ldbc.snb.bi-weighted-tag-path.v1"} => 3
           }
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

  test "canonical quad property paths retain the confirmed column-family execution regression",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)

    assert {:ok, definitions} = Workload.load()
    assert {:ok, read_3} = Workload.fetch(definitions, 3)
    sparql = String.replace(read_3.translation.sparql, "{{country}}", ~S["value"])
    sparql = String.replace(sparql, "{{tagClass}}", ~S["value"])

    previous_trap_exit = Process.flag(:trap_exit, true)

    result =
      try do
        Query.query(fixture.store, sparql, timeout: 2_000)
      after
        Process.flag(:trap_exit, previous_trap_exit)
      end

    assert {:error, {:task_exit, {%RuntimeError{message: message}, _stacktrace}}} = result

    assert message =~ "invalid_column_family"

    assert {:ok, matrix} = Qualification.matrix(definitions)

    affected =
      matrix.entries
      |> Enum.filter(&(:quad_property_path_column_family in &1.confirmed_gaps))
      |> Enum.map(&variant_label/1)

    assert affected == [
             "3",
             "4",
             "8a",
             "8b",
             "8m",
             "9",
             "12",
             "14a",
             "14b",
             "16a",
             "16b",
             "17",
             "18"
           ]
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
