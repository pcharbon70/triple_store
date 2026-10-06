defmodule TripleStore.Benchmark.LDBC.SPB.PipelineTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.RDFStream
  alias TripleStore.Benchmark.LDBC.SPB.Pipeline

  test "registered inputs distinguish core, graph, and optional sources" do
    assert {:ok, inputs} = Pipeline.inputs()
    assert inputs.generator_pin == "ce6323c0936306729408233dc70d26f2389b34c6"
    assert inputs.graphs.ontology != inputs.graphs.creative_works
    assert inputs.reference_datasets != []
    assert inputs.ontologies != []
    assert inputs.optional_inputs.text != []
    assert inputs.optional_inputs.geospatial != []
  end

  test "smoke generation is deterministic and parameters are checksum bound", %{test: test} do
    first = tmp_dir(test, "first")
    second = tmp_dir(test, "second")

    on_exit(fn ->
      File.rm_rf!(first)
      File.rm_rf!(second)
    end)

    assert {:ok, first_manifest} = Pipeline.generate_smoke(first, seed: 73)
    assert {:ok, second_manifest} = Pipeline.generate_smoke(second, seed: 73)

    assert first_manifest.transformation.output_checksum ==
             second_manifest.transformation.output_checksum

    assert first_manifest.transformation.statement_count == 7
    assert first_manifest.store.schema == :quad

    parameter_component =
      Enum.find(first_manifest.components, &(&1.role == :parameters))

    assert {:ok, parameters} = Pipeline.read_parameters(parameter_component.path)
    assert parameters.dataset_checksum == first_manifest.transformation.output_checksum
    assert "urn:ldbc:spb:entity:73" in parameters.values
  end

  test "normalization preserves bytes and rejects malformed RDF", %{test: test} do
    root = tmp_dir(test, "normalize")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    source = Path.join(root, "source.nq")
    destination = Path.join(root, "normalized.nq")
    content = "<urn:s> <urn:p> \"hello\"@en <urn:g> .\n"
    File.write!(source, content)

    assert {:ok, %{normalization: :byte_preserving_copy}} =
             Pipeline.normalize(source, destination)

    assert File.read!(destination) == content
    assert Artifact.checksum(source) == Artifact.checksum(destination)

    File.write!(source, "<urn:s> broken\n")

    assert {:error, {:rdf_parse_error, 1, _reason}} =
             RDFStream.scan(source, :nquads)
  end

  test "external generation requires a valid pinned checkout and explicit execution" do
    assert {:error, {:missing_spb_input, "build.xml"}} =
             Pipeline.external_generator_spec("/tmp/not-an-spb-checkout", "/tmp/output",
               jar: "driver.jar",
               dataset_size: 1_000,
               seed: 42
             )
  end

  defp tmp_dir(test, suffix) do
    Path.join(
      System.tmp_dir!(),
      "ldbc_spb_#{test}_#{suffix}_#{System.unique_integer([:positive])}"
    )
  end
end
