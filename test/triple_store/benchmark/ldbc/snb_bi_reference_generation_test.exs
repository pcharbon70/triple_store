defmodule TripleStore.Benchmark.LDBC.SNBBIReferenceGenerationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SNB.BI.{Parameters, ResultContract, Workload}
  alias TripleStore.Benchmark.LDBC.SNB.Converter

  @reference_commit "5f7967235593eefa98b336e240039780b6a76a9a"

  test "generated reference bundle covers every canonical variant with typed rows" do
    spec = decode!(reference_path("run-spec.json"))
    bundle = decode!(generated_path("answers.json"))
    assert {:ok, definitions} = Workload.load()

    expected_identities =
      MapSet.new(definitions, &{&1.catalog_id, &1.variant, 0})

    assert spec["schema_version"] == 1
    assert spec["catalog_id"] == Workload.catalog_id()
    assert spec["reference_source"]["commit"] == @reference_commit
    assert length(spec["entries"]) == 31
    assert identities(spec["entries"]) == expected_identities
    assert_run_spec_contract(spec["entries"], definitions)

    conversion_root = tmp_dir()
    on_exit(fn -> File.rm_rf!(conversion_root) end)

    assert {:ok, dataset} =
             Converter.convert(:snb_bi_smoke, fixture_root(), conversion_root)

    assert spec["dataset"]["source_checksum"] == dataset.source.checksum
    assert spec["dataset"]["output_checksum"] == dataset.transformation.output_checksum
    assert spec["dataset"]["mapping_version"] == dataset.transformation.mapping_version

    assert {:ok, parameters} =
             Parameters.new(dataset, parameter_sequences(spec["entries"]), provenance: :smoke)

    assert parameters.source_id == spec["parameter_source"]["source_id"]
    assert parameters.generator_pin == spec["parameter_source"]["generator_pin"]
    assert :ok = Parameters.validate(parameters, dataset, definitions)

    assert bundle["schema_version"] == 1
    assert bundle["catalog_id"] == Workload.catalog_id()
    assert bundle["dataset"] == spec["dataset"]
    assert bundle["parameter_source"] == spec["parameter_source"]
    assert bundle["reference_source"] == spec["reference_source"]
    assert bundle["reference_source"]["commit"] == @reference_commit
    assert length(bundle["answers"]) == 31
    assert identities(bundle["answers"]) == expected_identities

    definitions_by_identity =
      Map.new(definitions, &{{&1.catalog_id, &1.variant}, &1})

    Enum.each(bundle["answers"], fn answer ->
      definition =
        Map.fetch!(definitions_by_identity, {answer["operation_id"], answer["variant"]})

      assert {:ok, _result} = ResultContract.decode_reference(definition, answer["rows"])
      assert answer["rows_checksum"] == checksum(canonical_json(answer["rows"]))
    end)
  end

  test "generation receipt fixes the external oracle boundary and immutable inputs" do
    bundle_path = generated_path("answers.json")
    bundle = decode!(bundle_path)

    assert bundle["mapping_boundary"] == %{
             "external_oracle" => "Neo4j property graph running pinned upstream Cypher",
             "purpose" => "answer generation only; no Cypher support in TripleStore",
             "source" => "LDBC SNB CSV fixture",
             "triple_store" => "RDF snb-rdf-v1 mapping with SPARQL and explicit graph adapters"
           }

    assert bundle["runtime"] == %{
             "apoc" => "5.1.0",
             "graph_data_science" => "2.4.6",
             "image_digest" =>
               "neo4j@sha256:8e5d3d32ab4d18544694091a825a13a8e7a48af35a177a6e7645f74a9186de4a",
             "image_id" =>
               "sha256:43eb20e91b26463d9a206c03ae97182ee6a83e63b83255bba256fb5f517603ef",
             "image_reference" => "neo4j:5.1.0",
             "neo4j" => "5.1.0",
             "plugin_checksums" => %{
               "apoc" =>
                 "sha256:f45f7b313e48288c35c01fec33d390626d4a5cbddab298332127dc1150f6007d",
               "graph_data_science" =>
                 "sha256:480fd5836cc8bc1563ed4a22c7a9a71acc122c4df3bba59d2ff3e31399e7f3bc"
             },
             "python" => "3.12.12",
             "python_driver" => "4.4.8"
           }

    assert bundle["generation"]["tool_checksum"] ==
             file_checksum(Path.expand("scripts/generate_ldbc_snb_bi_reference_answers.py"))

    assert bundle["generation"]["run_spec_checksum"] ==
             file_checksum(reference_path("run-spec.json"))

    [line] =
      generated_path("SHA256SUMS")
      |> File.read!()
      |> String.split("\n", trim: true)

    assert line ==
             "#{String.replace_prefix(file_checksum(bundle_path), "sha256:", "")}  answers.json"
  end

  defp identities(entries) do
    MapSet.new(entries, fn entry ->
      {entry["operation_id"], entry["variant"], entry["parameter_row_index"]}
    end)
  end

  defp assert_run_spec_contract(entries, definitions) do
    entries_by_identity = Map.new(entries, &{{&1["operation_id"], &1["variant"]}, &1})

    Enum.each(definitions, fn definition ->
      entry = Map.fetch!(entries_by_identity, {definition.catalog_id, definition.variant})

      expected_types = Map.new(definition.parameters, &{&1.name, &1.type})
      actual_types = Map.new(entry["parameters"], fn {name, value} -> {name, value["type"]} end)

      assert actual_types == expected_types
    end)
  end

  defp parameter_sequences(entries) do
    Map.new(entries, fn entry ->
      suffix = if entry["variant"] == "default", do: "", else: entry["variant"]
      label = Integer.to_string(entry["number"]) <> suffix
      row = Map.new(entry["parameters"], fn {name, descriptor} -> {name, descriptor["value"]} end)
      {label, [row]}
    end)
  end

  defp canonical_json(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &canonical_json/1) <> "]"

  defp canonical_json(value) when is_map(value) do
    encoded =
      value
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_join(",", fn {key, nested} ->
        Jason.encode!(key) <> ":" <> canonical_json(nested)
      end)

    "{" <> encoded <> "}"
  end

  defp canonical_json(value), do: Jason.encode!(value)

  defp checksum(value),
    do: "sha256:" <> (:crypto.hash(:sha256, value) |> Base.encode16(case: :lower))

  defp file_checksum(path), do: path |> File.read!() |> checksum()
  defp decode!(path), do: path |> File.read!() |> Jason.decode!()

  defp reference_path(relative) do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "snb-bi",
      "reference",
      relative
    ])
  end

  defp generated_path(relative),
    do: reference_path(Path.join(["generated", "smoke-v1.0.3", relative]))

  defp fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_dir do
    Path.join(
      System.tmp_dir!(),
      "snb_bi_reference_generation_#{System.unique_integer([:positive])}"
    )
  end
end
