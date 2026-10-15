defmodule TripleStore.Benchmark.LDBC.CapabilityMatrixTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.{CapabilityMatrix, Catalog}
  alias TripleStore.Benchmark.LDBC.SNB.BI.{Qualification, Workload}

  test "classifies every catalog operation exactly once" do
    assert {:ok, matrix} = CapabilityMatrix.load()
    assert {:ok, catalogs} = Catalog.load_all()

    operation_ids = catalogs |> Catalog.operations() |> Enum.map(& &1.id) |> Enum.sort()
    capability_ids = matrix.operations |> Enum.map(& &1.operation_id) |> Enum.sort()

    assert capability_ids == operation_ids
    assert length(capability_ids) == 119

    assert CapabilityMatrix.summary(matrix) == expected_summary(matrix)
  end

  test "keeps parser support distinct from verified execution" do
    {:ok, matrix} = CapabilityMatrix.load()

    spb = find_operation(matrix, "ldbc/spb/aggregation-01@v2.0.2")
    assert spb.status == :supported
    assert spb.parse_support == :verified
    assert spb.execution_support == :verified

    bi_path = find_operation(matrix, "ldbc/snb-bi/read-19@v1.0.3")
    assert bi_path.status == :requires_extension
    assert bi_path.parse_support == :not_applicable
    assert bi_path.execution_support == :unverified
    assert :path_cost in bi_path.features

    bi_standard = find_operation(matrix, "ldbc/snb-bi/read-12@v1.0.3")
    assert bi_standard.status == :requires_fix
    assert bi_standard.parse_support == :verified
    assert bi_standard.execution_support == :unverified

    bi_update = find_operation(matrix, "ldbc/snb-bi/update-batch@v1.0.3")
    assert bi_update.status == :supported
    assert bi_update.parse_support == :not_applicable
    assert bi_update.execution_support == :verified

    interactive_path = find_operation(matrix, "ldbc/snb-interactive/complex-read-14@v1.2.0")
    assert interactive_path.status == :requires_extension

    v2_delete = find_operation(matrix, "ldbc/snb-interactive/delete-01@commit-30a73a28")
    assert v2_delete.status == :profile_exclusion
  end

  test "records ownership and evidence for transaction, reasoning, and resilience gaps" do
    {:ok, matrix} = CapabilityMatrix.load()
    assert length(matrix.system_findings) == 12

    assert %{status: :requires_fix, owner: "TripleStore.Transaction"} =
             find_finding(matrix, "LDBC-CAP-003")

    assert %{status: :requires_fix, owner: "TripleStore.Reasoner"} =
             find_finding(matrix, "LDBC-CAP-005")

    assert %{status: :profile_exclusion} = find_finding(matrix, "LDBC-CAP-009")

    assert %{status: :requires_fix, owner: "TripleStore.Benchmark.LDBC.Bridge"} =
             find_finding(matrix, "LDBC-CAP-011")
  end

  test "links the accepted architecture decision" do
    {:ok, matrix} = CapabilityMatrix.load()
    assert matrix.architecture_decision == "ADR-0002"

    adr = Path.expand("../../../../specs/adr/ADR-0002-ldbc-benchmark-boundary.md", __DIR__)
    contents = File.read!(adr)

    assert contents =~ "## Status\n\nAccepted"
    assert contents =~ "benchmark-owned Erlang Port"
    assert contents =~ "one versioned SNB RDF mapping"
    assert contents =~ "hide missing joins"
  end

  defp find_operation(matrix, id), do: Enum.find(matrix.operations, &(&1.operation_id == id))
  defp find_finding(matrix, id), do: Enum.find(matrix.system_findings, &(&1.id == id))

  defp expected_summary(matrix) do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, qualification} = Qualification.matrix(definitions)

    bi_evidence =
      Map.new(qualification.operations, &{&1.operation_id, &1})

    bi_statuses =
      Map.new(qualification.operations, fn evidence ->
        status =
          cond do
            evidence.qualified? -> :supported
            evidence.extension? -> :requires_extension
            true -> :requires_fix
          end

        {evidence.operation_id, status}
      end)

    statuses = Enum.map(matrix.operations, &expected_status(&1, bi_statuses, bi_evidence))

    counts = Enum.frequencies(statuses)

    Map.new(
      [:supported, :requires_fix, :requires_extension, :profile_exclusion],
      &{&1, Map.get(counts, &1, 0)}
    )
  end

  defp expected_status(operation, bi_statuses, bi_evidence) do
    case Map.fetch(bi_statuses, operation.operation_id) do
      {:ok, expected} ->
        assert operation.status == expected
        assert_supported_is_qualified(operation, bi_evidence[operation.operation_id])
        expected

      :error ->
        operation.status
    end
  end

  defp assert_supported_is_qualified(%{status: :supported}, evidence),
    do: assert(evidence.qualified?)

  defp assert_supported_is_qualified(_operation, _evidence), do: :ok
end
