defmodule TripleStore.Benchmark.LDBC.SPB.WorkloadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SPB.{Pipeline, Template, Workload}

  test "loads the complete pinned profile with separate capability classes" do
    assert {:ok, package} = Workload.load()

    assert length(package.operations) == 55

    assert Enum.frequencies_by(package.operations, & &1.family) == %{
             aggregation: 25,
             conformance: 10,
             editorial: 3,
             lifecycle: 9,
             resilience: 5,
             validation: 3
           }

    assert length(package.core) == 49
    assert Enum.map(package.optional, & &1.operation.upstream_id) == ["cleanup"]
    assert Enum.all?(package.audit_only, &(&1.family == :resilience))
    assert Enum.all?(package.operations, &(&1.operation.source.checksum == package.source.commit))
    assert package.agent_mix.query_timeout_ms == 300_000
  end

  test "templates use exact typed names and reject lexical injection" do
    template = "SELECT * WHERE { ?s ?p {{{cwUri}}} } LIMIT {{{randomLimit}}}"

    assert {:ok, fields} = Template.schema(template)
    assert Enum.map(fields, & &1.name) == ["cwUri", "randomLimit"]

    assert {:error, {:invalid_parameter, "cwUri", :invalid_iri}} =
             Template.render(template, %{
               "cwUri" => "urn:ok> . DROP ALL #",
               "randomLimit" => 10
             })

    assert {:error, {:unknown_parameters, ["raw"]}} =
             Template.render(template, %{
               "cwUri" => "urn:ok",
               "randomLimit" => 10,
               "raw" => "DROP ALL"
             })
  end

  test "generated parameters remain bound to dataset identity and scale", %{test: test} do
    root =
      Path.join(System.tmp_dir!(), "spb_workload_#{test}_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(root, seed: 47, scale_factor: "smoke-47")
    parameter_component = Enum.find(manifest.components, &(&1.role == :parameters))
    assert {:ok, parameters} = Pipeline.read_parameters(parameter_component.path)
    assert {:ok, binding} = Workload.bind_parameters(manifest, parameters)
    assert binding.dataset.scale_factor == "smoke-47"
    assert binding.dataset.output_checksum == parameters.dataset_checksum

    assert {:error, :parameter_dataset_mismatch} =
             Workload.bind_parameters(manifest, %{parameters | dataset_checksum: "sha256:wrong"})
  end

  test "every aggregation template is audited through parser and algebra" do
    assert {:ok, package} = Workload.load()

    assert {:error, failures} =
             Workload.validate_aggregation_queries(package, %{
               values: ["urn:ldbc:spb:entity:smoke"]
             })

    assert Enum.map(failures, &elem(&1, 0)) == [
             "ldbc/spb/aggregation-11@v2.0.2",
             "ldbc/spb/aggregation-12@v2.0.2",
             "ldbc/spb/aggregation-20@v2.0.2"
           ]
  end
end
