defmodule TripleStore.Benchmark.LDBC.SNBBIOptimizationTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.PlanEvidence

  test "ranks qualified cost centers under one environment identity" do
    slow = evidence("bi-1", 900, 100, 1_000, 100)
    fast = evidence("bi-2a", 120, 10, 10, 8)

    assert {:ok, [first, second]} = PlanEvidence.rank([fast, slow])
    assert first.operation_id == "bi-1"
    assert first.rank == 1
    assert first.cardinality_error_ratio == 10.0
    assert second.operation_id == "bi-2a"

    mismatched = put_in(fast.environment_identity, "other-environment")
    assert {:error, :environment_identity_mismatch} = PlanEvidence.rank([slow, mismatched])
  end

  test "accepts only faster answer-preserving and resource-complete candidates" do
    baseline = evidence("bi-1", 900, 100, 100, 40)
    candidate = evidence("bi-1", 500, 95, 100, 40)

    assert {:ok, accepted} = PlanEvidence.accept_improvement(baseline, candidate)
    assert accepted.elapsed_improvement_us == 400

    assert {:error, :answer_changed} =
             PlanEvidence.accept_improvement(baseline, %{candidate | answer_digest: "changed"})

    assert {:error, :resource_cleanup_incomplete} =
             PlanEvidence.accept_improvement(baseline, %{candidate | cleanup_status: :incomplete})

    assert {:error, :protocol_ineligible} =
             PlanEvidence.accept_improvement(baseline, %{candidate | protocol_eligible?: false})

    assert {:error, :latency_not_improved} =
             PlanEvidence.accept_improvement(baseline, %{candidate | elapsed_us: 1_000})
  end

  defp evidence(operation, elapsed_us, iterators, estimated, actual) do
    measurements = %{
      plan: :hash_join,
      estimated_cardinality: estimated,
      actual_cardinality: actual,
      elapsed_us: elapsed_us,
      iterator_count: iterators,
      materialized_rows: actual,
      memory_bytes: actual * 64,
      io_bytes: actual * 128,
      environment_identity: "sha256:environment",
      cleanup_status: :complete,
      protocol_eligible?: true
    }

    assert {:ok, evidence} = PlanEvidence.record(operation, [[1], [2]], [[1], [2]], measurements)
    evidence
  end
end
