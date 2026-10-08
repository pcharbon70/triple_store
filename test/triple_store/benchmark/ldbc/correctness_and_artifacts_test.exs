defmodule TripleStore.Benchmark.LDBC.CorrectnessAndArtifactsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.{Artifacts, Baseline, Correctness, Result}

  test "compares ordered, unordered, duplicate-bearing, empty, numeric, and timestamp answers" do
    ordered = result([[1], [2]], %{mode: :ordered}, [:id])

    assert %{status: :incorrect, category: :misordered} =
             Correctness.compare(ordered, result([[2], [1]], %{mode: :ordered}, [:id]))

    unordered = result([[1], [1], [2]], %{mode: :unordered}, [:id])

    assert %{status: :correct} =
             Correctness.compare(unordered, result([[2], [1], [1]], %{mode: :unordered}, [:id]))

    assert %{status: :incorrect, details: %{missing: [%{count: 1}]}} =
             Correctness.compare(unordered, result([[2], [1]], %{mode: :unordered}, [:id]))

    assert %{status: :correct} =
             Correctness.compare(
               result([], %{mode: :unordered}, []),
               result([], %{mode: :unordered}, [])
             )

    assert %{status: :correct} =
             Correctness.compare(
               result([[1.0]], %{mode: :ordered}, [:float64]),
               result([[1.0001]], %{mode: :ordered}, [:float64]),
               numeric_tolerance: 0.001
             )

    timestamp = DateTime.from_iso8601("2025-01-01T00:00:00Z") |> elem(1)

    assert %{status: :correct} =
             Correctness.compare(
               result([[timestamp]], %{mode: :ordered}, [:timestamp]),
               result([[timestamp]], %{mode: :ordered}, [:timestamp])
             )
  end

  test "uses ordered and multiset hashes above the full comparison threshold" do
    expected = result([[1], [2], [1]], %{mode: :unordered}, [:id])
    actual = result([[2], [1], [1]], %{mode: :unordered}, [:id])

    assert %{status: :correct, comparison: {:hash, :multiset, _digest}} =
             Correctness.compare(expected, actual, full_comparison_limit: 1)

    ordered = result([[1], [2]], %{mode: :ordered}, [:id])

    assert %{status: :incorrect, category: :hash_mismatch} =
             Correctness.compare(
               ordered,
               result([[2], [1]], %{mode: :ordered}, [:id]),
               full_comparison_limit: 1
             )
  end

  test "accepted divergences require explicit permission, reason, and pinned version" do
    divergence = %{
      permitted?: true,
      reason: "Specification leaves tie order open",
      source_version: "v1"
    }

    assert :ok = Correctness.validate_divergence(divergence, "v1")

    assert {:error, :invalid_accepted_divergence} =
             Correctness.validate_divergence(%{reason: "x", source_version: "v1"}, "v1")

    assert %{status: :accepted_divergence} =
             Correctness.compare(
               result([[1]], %{mode: :ordered}, [:id]),
               result([[2]], %{mode: :ordered}, [:id]),
               accepted_divergence: divergence
             )
  end

  test "writes agreeing JSON CSV and Markdown artifacts and gates official scores" do
    directory =
      Path.join(System.tmp_dir!(), "ldbc-artifacts-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(directory) end)

    sample = %{
      operation_id: "read-1",
      mode: :measured,
      status: :success,
      total_us: 20,
      sample_us: 20,
      score_eligible?: true
    }

    run = %{
      manifest: %{run_id: "run-1"},
      input_checksums: %{dataset: "abc"},
      environment: %{runtime: "test"},
      catalog: [%{id: "read-1"}],
      raw_samples: [sample],
      errors: [],
      correctness: [%{operation_id: "read-1", status: :correct}],
      disclosure: %{supported_profiles: ["smoke"], unsupported_profiles: ["failover"]},
      resources: %{store_bytes: 10},
      summary: %{measured_count: 1, valid_sample_count: 1, invalid_sample_count: 0},
      gates: %{
        profile: true,
        correctness: true,
        scheduling: true,
        duration: true,
        completeness: true
      },
      official_score: 123.4
    }

    assert {:ok, artifact} = Artifacts.write(directory, run)
    assert map_size(artifact.paths) == 11
    assert map_size(artifact.checksums) == 11
    assert File.exists?(artifact.paths.disclosure)

    assert {:ok, summary} = artifact.paths.summary |> File.read!() |> Jason.decode()
    assert summary["official_score"] == 123.4
    assert summary["measured_count"] == 1
    assert File.read!(artifact.paths.samples_csv) =~ "read-1,measured,success,20,20,true"
    assert File.read!(artifact.paths.report_markdown) =~ "Measured operations: 1"

    blocked = put_in(run, [:gates, :correctness], false)
    assert {:ok, blocked_artifact} = Artifacts.write(Path.join(directory, "blocked"), blocked)
    blocked_summary = blocked_artifact.paths.summary |> File.read!() |> Jason.decode!()
    refute Map.has_key?(blocked_summary, "official_score")
  end

  test "baseline acceptance is explicit, checks correctness, and leaves provenance" do
    directory =
      Path.join(System.tmp_dir!(), "ldbc-baseline-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(directory) end)
    input = Path.join(directory, "correctness.json")
    output = Path.join(directory, "accepted/baseline.json")
    File.mkdir_p!(directory)
    File.write!(input, Jason.encode!(%{schema_version: 1, records: [%{status: :correct}]}))

    assert :ok = Baseline.accept(input, output, reason: "reviewed", source_version: "v1")
    accepted = output |> File.read!() |> Jason.decode!()
    assert accepted["reason"] == "reviewed"
    assert accepted["source_version"] == "v1"
    assert String.length(accepted["input_sha256"]) == 64
  end

  defp result(rows, ordering, types) do
    columns = Enum.with_index(types, 1) |> Enum.map(fn {_type, index} -> "c#{index}" end)
    %Result{columns: columns, types: types, rows: rows, ordering: ordering}
  end
end
