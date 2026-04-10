defmodule TripleStore.Benchmark.Wikidata.ContractTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.Contract

  describe "workload_families/0" do
    test "returns the supported workload families" do
      assert Contract.workload_families() == [:wgpb, :wdbench, :wdqs, :scholia]
    end
  end

  describe "execution_variants/0" do
    test "returns the supported execution variants" do
      assert Contract.execution_variants() == [:raw, :count_only, :distinct_only]
    end
  end

  describe "required_result_artifacts/0" do
    test "returns the canonical benchmark artifacts" do
      assert Contract.required_result_artifacts() == [
               :raw_timings,
               :adjusted_timings,
               :errors,
               :timeouts,
               :divergences,
               :metadata
             ]
    end
  end

  describe "dataset_tier/1" do
    test "returns tier metadata for a known tier" do
      assert {:ok, tier} = Contract.dataset_tier(:smoke)
      assert tier.id == :smoke
      assert :ci in tier.intended_use
    end

    test "returns an error for unknown tiers" do
      assert Contract.dataset_tier(:unknown) == {:error, :unknown_tier}
    end
  end

  describe "success_criteria/0" do
    test "defines the initial acceptance criteria" do
      criteria = Contract.success_criteria()

      assert Map.has_key?(criteria, :load_completion)
      assert Map.has_key?(criteria, :query_completion_rate)
      assert Map.has_key?(criteria, :report_generation)
      assert criteria.report_generation.artifacts == Contract.required_result_artifacts()
    end
  end

  describe "required_run_metadata_fields/0" do
    test "returns the runtime metadata contract" do
      assert :hostname in Contract.required_run_metadata_fields()
      assert :elixir_version in Contract.required_run_metadata_fields()
      assert :schedulers_online in Contract.required_run_metadata_fields()
    end
  end

  describe "capture_runtime_metadata/0" do
    test "captures the required metadata fields" do
      metadata = Contract.capture_runtime_metadata()

      Enum.each(Contract.required_run_metadata_fields(), fn field ->
        assert Map.has_key?(metadata, field)
      end)

      assert %DateTime{} = metadata.captured_at
      assert is_binary(metadata.hostname)
      assert is_binary(metadata.elixir_version)
      assert is_integer(metadata.schedulers_online)
    end
  end

  describe "predicate helpers" do
    test "validate known contract values" do
      assert Contract.workload_family?(:wdqs)
      assert Contract.execution_variant?(:count_only)
      assert Contract.dataset_tier?(:large)
      assert Contract.result_artifact?(:metadata)
    end

    test "reject unknown contract values" do
      refute Contract.workload_family?(:unknown)
      refute Contract.execution_variant?(:sample)
      refute Contract.dataset_tier?(:tiny)
      refute Contract.result_artifact?(:json_report)
    end
  end
end
