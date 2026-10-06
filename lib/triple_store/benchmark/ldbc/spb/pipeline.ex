defmodule TripleStore.Benchmark.LDBC.SPB.Pipeline do
  @moduledoc """
  Reproducible SPB input, generation, validation, and parameter pipeline.

  The offline smoke generator and the external SPB generator enter the same
  validated N-Quads boundary. The smoke path is deterministic and network-free;
  larger runs require an explicit pinned upstream checkout.
  """

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{DatasetManifest, ExternalCommand, RDFStream}

  @mapping_version "spb-rdf-v2.0.2"
  @transformation_version "spb-pipeline-v1"

  @doc "Returns the registered SPB source inputs and validates their shape."
  @spec inputs() :: {:ok, map()} | {:error, term()}
  def inputs do
    path = priv_path("datasets/spb_inputs.exs")

    with {inputs, _binding} <- Code.eval_file(path),
         :ok <- validate_inputs(inputs) do
      {:ok, inputs}
    end
  rescue
    error -> {:error, {:invalid_spb_inputs, Exception.message(error)}}
  end

  @doc "Generates the deterministic, license-compatible SPB smoke dataset."
  @spec generate_smoke(Path.t(), keyword()) :: {:ok, DatasetManifest.t()} | {:error, term()}
  def generate_smoke(output_dir, opts \\ []) when is_binary(output_dir) do
    seed = Keyword.get(opts, :seed, 42)
    scale = Keyword.get(opts, :scale_factor, "smoke-1")
    output_path = Path.join(output_dir, "spb-smoke.nq")
    parameters_path = Path.join(output_dir, "spb-smoke-parameters.etf")

    with {:ok, input} <- inputs(),
         :ok <- File.mkdir_p(output_dir),
         :ok <- File.write(output_path, smoke_nquads(seed)),
         {:ok, scan} <- RDFStream.scan(output_path, :nquads),
         {:ok, parameters} <- generate_parameters(output_path, scan),
         :ok <- write_parameters(parameters_path, parameters),
         {:ok, parameter_checksum} <- Artifact.checksum(parameters_path),
         {:ok, source_checksum} <- input_checksum(input),
         {:ok, manifest} <-
           build_manifest(
             input,
             seed,
             scale,
             output_path,
             parameters_path,
             scan,
             parameter_checksum,
             source_checksum
           ) do
      {:ok, manifest}
    end
  end

  @doc "Validates a generated SPB file and copies its bytes without RDF repair."
  @spec normalize(Path.t(), Path.t()) :: {:ok, map()} | {:error, term()}
  def normalize(source, destination) do
    RDFStream.validate_and_copy(source, destination, :nquads)
  end

  @doc "Builds a pinned upstream Java-generator command and properties file."
  @spec external_generator_spec(Path.t(), Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def external_generator_spec(checkout, output_dir, opts) do
    with {:ok, input} <- inputs(),
         :ok <- validate_checkout_layout(checkout),
         {:ok, jar} <- fetch_non_empty(opts, :jar),
         {:ok, scale} <- fetch_positive_integer(opts, :dataset_size),
         {:ok, seed} <- fetch_integer(opts, :seed),
         :ok <- File.mkdir_p(output_dir),
         {:ok, properties_path} <-
           write_external_properties(checkout, output_dir, scale, seed, opts) do
      {:ok,
       %{
         executable: "java",
         args: ["-jar", jar, properties_path],
         working_directory: checkout,
         commit: input.generator_pin,
         env: []
       }}
    end
  end

  @doc "Runs an external SPB generator only after explicit caller authorization."
  @spec run_external(Path.t(), Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_external(checkout, output_dir, opts) do
    with {:ok, spec} <- external_generator_spec(checkout, output_dir, opts) do
      ExternalCommand.run(spec, allow_external: Keyword.get(opts, :allow_external, false))
    end
  end

  @doc "Reads parameters that were generated only after dataset validation."
  @spec read_parameters(Path.t()) :: {:ok, map()} | {:error, term()}
  def read_parameters(path) do
    with {:ok, binary} <- File.read(path) do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    end
  rescue
    ArgumentError -> {:error, :invalid_parameter_file}
  end

  defp generate_parameters(path, scan) do
    reducer = fn {subject, _predicate, object, _graph}, acc ->
      acc
      |> maybe_add_parameter(subject)
      |> maybe_add_parameter(object)
    end

    with {:ok, terms, count} <- RDFStream.reduce(path, :nquads, MapSet.new(), reducer),
         true <- count == scan.statement_count do
      values = terms |> MapSet.to_list() |> Enum.sort() |> Enum.take(16)

      {:ok,
       %{
         dataset_checksum: scan.checksum,
         mapping_version: @mapping_version,
         values: values
       }}
    else
      false -> {:error, :statement_count_changed}
      {:error, _} = error -> error
    end
  end

  defp maybe_add_parameter(set, %RDF.IRI{} = iri), do: MapSet.put(set, to_string(iri))
  defp maybe_add_parameter(set, _term), do: set

  defp build_manifest(
         input,
         seed,
         scale,
         output_path,
         parameters_path,
         scan,
         parameter_checksum,
         source_checksum
       ) do
    DatasetManifest.new(%{
      dataset_id: "spb-smoke-#{seed}",
      suite: :spb,
      profile: "spb-smoke",
      scale_factor: scale,
      source: %{
        generator_source_id: input.generator_source_id,
        generator_pin: input.generator_pin,
        generator_settings: %{format: "N-Quads", profile: "smoke"},
        seed: seed,
        format: :nquads,
        checksum: source_checksum,
        license: input.license
      },
      transformation: %{
        version: @transformation_version,
        mapping_version: @mapping_version,
        output_checksum: scan.checksum,
        statement_count: scan.statement_count,
        entity_count: 3,
        relationship_count: 2,
        update_streams: input.editorial_inputs
      },
      store: %{
        schema: :quad,
        loader_settings: %{batch_size: 1_000, parallel: false},
        path_identity: "spb-smoke-#{seed}-quad",
        post_load_stats: %{}
      },
      components: [
        %{
          role: :initial,
          path: output_path,
          checksum: scan.checksum,
          count: scan.statement_count
        },
        %{role: :parameters, path: parameters_path, checksum: parameter_checksum, count: 1}
      ]
    })
  end

  defp smoke_nquads(seed) do
    """
    <https://ldbcouncil.org/spb/ontology/CreativeWork> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://schema.org/CreativeWork> <urn:ldbc:spb:graph:ontology> .
    <urn:ldbc:spb:entity:#{seed}> <http://www.w3.org/2000/01/rdf-schema#label> "Entity #{seed}"@en <urn:ldbc:spb:graph:reference> .
    <urn:ldbc:spb:creative-work:#{seed}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://ldbcouncil.org/spb/ontology/CreativeWork> <urn:ldbc:spb:graph:creative-works> .
    <urn:ldbc:spb:creative-work:#{seed}> <http://schema.org/about> <urn:ldbc:spb:entity:#{seed}> <urn:ldbc:spb:graph:creative-works> .
    <urn:ldbc:spb:creative-work:#{seed}> <http://schema.org/headline> "Deterministic smoke work"@en <urn:ldbc:spb:graph:creative-works> .
    <urn:ldbc:spb:creative-work:#{seed}> <http://schema.org/dateCreated> "2026-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> <urn:ldbc:spb:graph:creative-works> .
    _:source#{seed} <http://schema.org/url> <urn:ldbc:spb:entity:#{seed}> <urn:ldbc:spb:graph:reference> .
    """
  end

  defp write_parameters(path, parameters) do
    File.write(path, :erlang.term_to_binary(parameters, [:deterministic]), [:binary])
  end

  defp input_checksum(input) do
    digest =
      input
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    {:ok, "sha256:#{digest}"}
  end

  defp validate_inputs(input) when is_map(input) do
    required = [
      :generator_source_id,
      :generator_pin,
      :license,
      :reference_datasets,
      :ontologies,
      :rule_configuration,
      :generator_definitions,
      :scale_settings,
      :editorial_inputs,
      :optional_inputs,
      :graphs
    ]

    if Enum.all?(required, &Map.has_key?(input, &1)) and
         String.match?(input.generator_pin, ~r/\A[0-9a-f]{40}\z/) do
      :ok
    else
      {:error, :incomplete_spb_inputs}
    end
  end

  defp validate_inputs(_), do: {:error, :invalid_spb_inputs}

  defp validate_checkout_layout(checkout) do
    required = ["build.xml", "test.properties", "datasets_and_queries"]

    if Enum.all?(required, &File.exists?(Path.join(checkout, &1))) do
      :ok
    else
      {:error, :invalid_spb_checkout}
    end
  end

  defp write_external_properties(checkout, output_dir, scale, seed, opts) do
    destination = Path.join(output_dir, "spb-generator.properties")
    template = Path.join(checkout, "test.properties")

    with {:ok, content} <- File.read(template) do
      overrides = """

      # TripleStore LDBC Phase 2 deterministic overrides
      datasetSize=#{scale}
      generatorRandomSeed=#{seed}
      generateCreativeWorksFormat=N-Quads
      creativeWorksPath=#{Path.expand(output_dir)}
      querySubstitutionParameters=#{Keyword.get(opts, :parameter_count, 100_000)}
      """

      with :ok <- File.write(destination, content <> overrides), do: {:ok, destination}
    end
  end

  defp fetch_non_empty(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:invalid_option, key}}
    end
  end

  defp fetch_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, {:invalid_option, key}}
    end
  end

  defp fetch_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) -> {:ok, value}
      _ -> {:error, {:invalid_option, key}}
    end
  end

  defp priv_path(relative) do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../../../priv/benchmarks/ldbc/#{relative}", __DIR__)

      priv_dir ->
        Path.join([to_string(priv_dir), "benchmarks", "ldbc", relative])
    end
  end
end
