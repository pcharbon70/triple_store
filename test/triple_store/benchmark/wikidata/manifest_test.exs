defmodule TripleStore.Benchmark.Wikidata.ManifestTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{Contract, Manifest}

  describe "new/1" do
    test "builds a valid manifest with defaults" do
      assert {:ok, manifest} =
               Manifest.new(
                 benchmark_id: "wdqs/q42",
                 suite: :wdqs,
                 category: :entity_lookup,
                 source: %{kind: :paper, location: "paper3.pdf"},
                 dataset: %{
                   tier: :smoke,
                   dump_version: "2024-10",
                   checksum: "sha256:test",
                   format: :ntriples,
                   normalization_flags: [:truthy_only]
                 }
               )

      assert manifest.execution_variant == :raw
      assert manifest.required_artifacts == Contract.required_result_artifacts()
      assert manifest.required_run_metadata == Contract.required_run_metadata_fields()
    end

    test "returns validation errors for invalid attributes" do
      assert {:error, errors} =
               Manifest.new(
                 benchmark_id: "",
                 suite: :unknown,
                 category: :entity_lookup,
                 source: %{kind: :paper},
                 dataset: %{tier: :tiny}
               )

      assert {:benchmark_id, _} = Enum.find(errors, fn {field, _} -> field == :benchmark_id end)
      assert {:suite, _} = Enum.find(errors, fn {field, _} -> field == :suite end)
      assert {:source, _} = Enum.find(errors, fn {field, _} -> field == :source end)
      assert {:dataset, _} = Enum.find(errors, fn {field, _} -> field == :dataset end)
    end
  end

  describe "validate/1" do
    test "rejects unknown artifacts" do
      manifest = %Manifest{
        version: 1,
        benchmark_id: "wgpb/l1",
        suite: :wgpb,
        category: :linear,
        execution_variant: :raw,
        source: %{kind: :fixture, location: "queries/wgpb.json"},
        dataset: %{
          tier: :smoke,
          dump_version: "2024-10",
          checksum: "sha256:test",
          format: :ntriples,
          normalization_flags: []
        },
        required_artifacts: [:raw_timings, :unknown_artifact],
        success_criteria: Contract.success_criteria(),
        required_run_metadata: Contract.required_run_metadata_fields(),
        tags: []
      }

      assert {:error, errors} = Manifest.validate(manifest)

      assert {:required_artifacts, _} =
               Enum.find(errors, fn {field, _} -> field == :required_artifacts end)
    end

    test "rejects unknown required runtime metadata fields" do
      manifest = %Manifest{
        version: 1,
        benchmark_id: "scholia/author/1",
        suite: :scholia,
        category: :template,
        execution_variant: :count_only,
        source: %{kind: :template, label: "Scholia author template"},
        dataset: %{
          tier: :medium,
          dump_version: "2024-10",
          checksum: "sha256:test",
          format: :ntriples,
          normalization_flags: []
        },
        required_artifacts: Contract.required_result_artifacts(),
        success_criteria: Contract.success_criteria(),
        required_run_metadata: [:hostname, :unknown_field],
        tags: [:scholarly]
      }

      assert {:error, errors} = Manifest.validate(manifest)

      assert {:required_run_metadata, _} =
               Enum.find(errors, fn {field, _} -> field == :required_run_metadata end)
    end
  end

  describe "to_map/1" do
    test "returns a serializable representation" do
      {:ok, manifest} =
        Manifest.new(
          benchmark_id: "wdbench/optional/17",
          suite: :wdbench,
          category: :optional,
          source: %{kind: :query_log, location: "fixtures/wdbench_optional.txt"},
          dataset: %{
            tier: :large,
            dump_version: "2024-10",
            checksum: "sha256:test",
            format: :ntriples,
            normalization_flags: [:truthy_only]
          }
        )

      manifest_map = Manifest.to_map(manifest)

      assert manifest_map.benchmark_id == "wdbench/optional/17"
      assert manifest_map.suite == :wdbench
      assert manifest_map.dataset.tier == :large
    end
  end
end
