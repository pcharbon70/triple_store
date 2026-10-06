defmodule TripleStore.Benchmark.LDBC.ProfileAndCatalogTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.{Catalog, Profile, SourceManifest}

  test "profiles distinguish diagnostic, comparable, audit-preparation, and v2 work" do
    assert {:ok, profiles} = Profile.load()
    assert length(profiles) == 13

    assert {:ok, smoke} = Profile.fetch(profiles, "spb-smoke-v2.0.2")
    assert smoke.score_namespace == :diagnostic
    refute smoke.protocol.complete_operation_mix

    assert {:ok, comparable} = Profile.fetch(profiles, "snb-bi-comparable-v1.0.3")
    assert comparable.score_namespace == :canonical
    assert Enum.all?(comparable.protocol, fn {_key, enabled?} -> enabled? end)

    assert {:ok, deep_delete} =
             Profile.fetch(profiles, "snb-interactive-deep-delete-development-30a73a28")

    assert deep_delete.claim_level == :development
    assert deep_delete.score_namespace == :diagnostic
  end

  test "protected report claims require completed external audit metadata" do
    {:ok, profiles} = Profile.load()
    {:ok, audit_profile} = Profile.fetch(profiles, "snb-interactive-audit-preparation-v1.2.0")

    assert {:error, {:protected_claim_requires_completed_audit, _claim}} =
             Profile.validate_report_claim(audit_profile, "Official audited result")

    assert :ok =
             Profile.validate_report_claim(audit_profile, "Official audited result", %{
               status: :completed,
               auditor: "Example accredited auditor",
               report_url: "https://example.test/audit/report"
             })

    assert :ok = Profile.validate_report_claim(audit_profile, "Audit preparation run")
  end

  test "catalogs cover each pinned operation exactly once" do
    assert {:ok, catalogs} = Catalog.load_all()
    operations = Catalog.operations(catalogs)

    assert length(catalogs) == 4
    assert length(operations) == 119
    assert length(Enum.uniq_by(operations, & &1.id)) == 119

    assert count_family(catalogs, "ldbc-spb-v2.0.2", :aggregation) == 25
    assert count_family(catalogs, "ldbc-snb-bi-v1.0.3", :read) == 20
    assert count_family(catalogs, "ldbc-snb-bi-v1.0.3", :update_batch) == 1
    assert count_family(catalogs, "ldbc-snb-interactive-v1.2.0", :complex_read) == 14
    assert count_family(catalogs, "ldbc-snb-interactive-v1.2.0", :short_read) == 7
    assert count_family(catalogs, "ldbc-snb-interactive-v1.2.0", :insert) == 8
  end

  test "catalog validation rejects omitted and duplicated canonical operations" do
    {:ok, sources} = SourceManifest.load()
    {:ok, [catalog | _]} = Catalog.load_all()
    [first | rest] = catalog.operations

    omitted = %{catalog | operations: rest}
    assert {:error, errors} = Catalog.validate([omitted], sources)
    assert Enum.any?(errors, &match?({_, :expected_operation_ids, _}, &1))

    duplicated = %{
      catalog
      | operations: [first, first | rest],
        expected_operation_ids: [first.id, first.id | Enum.map(rest, & &1.id)]
    }

    assert {:error, duplicate_errors} = Catalog.validate([duplicated], sources)

    assert Enum.any?(duplicate_errors, fn error ->
             elem(error, tuple_size(error) - 1) == "is duplicated"
           end)
  end

  test "catalog digests are deterministic" do
    {:ok, catalogs_a} = Catalog.load_all()
    {:ok, catalogs_b} = Catalog.load_all()

    assert Enum.map(catalogs_a, &Catalog.digest/1) == Enum.map(catalogs_b, &Catalog.digest/1)
  end

  defp count_family(catalogs, id, family) do
    catalogs
    |> Enum.find(&(&1.id == id))
    |> Map.fetch!(:operations)
    |> Enum.count(&(&1.family == family))
  end
end
