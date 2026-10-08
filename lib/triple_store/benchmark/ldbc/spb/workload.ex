defmodule TripleStore.Benchmark.LDBC.SPB.Workload do
  @moduledoc """
  Canonical operation package for the pinned SPB 2.0.2 advanced profile.

  The package verifies every copied upstream template before exposing it, binds
  parameter bundles to a Phase 2 dataset manifest, and records core, optional,
  and unsupported resilience capabilities separately.
  """

  alias TripleStore.Benchmark.LDBC.{Catalog, DatasetManifest, Operation, SourceManifest}
  alias TripleStore.Benchmark.LDBC.SPB.Template
  alias TripleStore.SPARQL.{Algebra, Parser}

  @source_id "spb-2.0.2"
  @profile_id "spb-comparable-v2.0.2"
  @transformation "triplestore-spb-v2.0.2-v1"

  @doc "Loads the complete SPB operation package and verifies copied source bytes."
  @spec load() :: {:ok, map()} | {:error, term()}
  def load do
    with :ok <- verify_checksums(),
         {:ok, catalogs} <- Catalog.load_all(),
         {:ok, sources} <- SourceManifest.load(),
         {:ok, source} <- SourceManifest.fetch(sources, @source_id),
         {:ok, catalog} <- fetch_catalog(catalogs),
         {:ok, operations} <- build_operations(catalog.operations, source) do
      {:ok,
       %{
         source: source,
         profile_id: @profile_id,
         operations: operations,
         core: Enum.filter(operations, &(&1.availability == :mandatory)),
         optional: Enum.filter(operations, &(&1.availability == :optional)),
         audit_only: Enum.filter(operations, &(&1.availability == :audit_only)),
         graphs: graph_contract(),
         agent_mix: %{aggregation_agents: 8, editorial_agents: 2, query_timeout_ms: 300_000}
       }}
    end
  end

  @doc "Binds a generated parameter component to its exact dataset identity and scale."
  @spec bind_parameters(DatasetManifest.t(), map()) :: {:ok, map()} | {:error, term()}
  def bind_parameters(%DatasetManifest{suite: :spb} = manifest, parameters)
      when is_map(parameters) do
    parameter_component = Enum.find(manifest.components, &(&1.role == :parameters))

    cond do
      is_nil(parameter_component) ->
        {:error, :parameter_component_missing}

      parameters[:dataset_checksum] != manifest.transformation.output_checksum ->
        {:error, :parameter_dataset_mismatch}

      parameters[:mapping_version] != manifest.transformation.mapping_version ->
        {:error, :parameter_mapping_mismatch}

      true ->
        {:ok,
         %{
           dataset: DatasetManifest.identity(manifest),
           parameter_component_checksum: parameter_component.checksum,
           values: parameters[:values] || []
         }}
    end
  end

  def bind_parameters(%DatasetManifest{}, _parameters), do: {:error, :not_spb_manifest}

  @doc "Instantiates and passes every aggregation query through parser and algebra."
  @spec validate_aggregation_queries(map(), map()) :: :ok | {:error, [term()]}
  def validate_aggregation_queries(package, binding) do
    errors =
      package.operations
      |> Enum.filter(&(&1.family == :aggregation))
      |> Enum.flat_map(&validate_query(&1, binding))

    if errors == [], do: :ok, else: {:error, errors}
  end

  @doc "Returns the declared SPB graph identity contract."
  @spec graph_contract() :: map()
  def graph_contract do
    %{
      schema: :quad,
      default_graph_id: 0,
      ontology: "urn:ldbc:spb:graph:ontology",
      reference: "urn:ldbc:spb:graph:reference",
      creative_works: "urn:ldbc:spb:graph:creative-works",
      acl_mode: :disabled,
      preserve_across: [:update, :export, :backup, :restore]
    }
  end

  defp build_operations(catalog_operations, source) do
    catalog_operations
    |> Enum.reduce_while({:ok, []}, fn catalog_operation, {:ok, operations} ->
      case build_operation(catalog_operation, source) do
        {:ok, operation} -> {:cont, {:ok, [operation | operations]}}
        {:error, reason} -> {:halt, {:error, {catalog_operation.id, reason}}}
      end
    end)
    |> case do
      {:ok, operations} -> {:ok, Enum.reverse(operations)}
      error -> error
    end
  end

  defp build_operation(%{family: family} = catalog, source)
       when family in [:aggregation, :editorial, :validation] do
    local_path = local_template_path(catalog)

    with {:ok, template} <- File.read(local_path),
         {:ok, parameter_schema} <- template_schema(family, template),
         {:ok, checksum} <- file_checksum(local_path) do
      operation = %Operation{
        id: catalog.id,
        suite: :spb,
        profile_id: @profile_id,
        catalog_id: catalog.id,
        upstream_id: catalog.upstream_id,
        kind: kind(family),
        parameter_schema: parameter_schema,
        result_schema: result_schema(family),
        ordering: ordering(catalog.ordering),
        limit: static_limit(template),
        timeout_class: :long,
        tags: [:spb, family, :advanced, availability_tag(catalog.availability)],
        strategy: {:driver_callback, __MODULE__, :execute},
        source: %{
          source_id: source.id,
          path: catalog.source_path,
          checksum: source.checksum.value
        },
        transformation_version: @transformation
      }

      with :ok <- Operation.validate(operation) do
        {:ok,
         %{
           operation: operation,
           family: family,
           availability: catalog.availability,
           dependencies: catalog.dependencies,
           frequency: catalog.frequency,
           inference: inference_expectation(template),
           template: template,
           template_sha256: checksum,
           local_path: local_path
         }}
      end
    end
  end

  defp build_operation(catalog, source) do
    operation = %Operation{
      id: catalog.id,
      suite: :spb,
      profile_id: @profile_id,
      catalog_id: catalog.id,
      upstream_id: catalog.upstream_id,
      kind: kind(catalog.family),
      parameter_schema: [],
      result_schema: result_schema(catalog.family),
      ordering: %{mode: :not_applicable},
      limit: nil,
      timeout_class: if(catalog.family == :resilience, do: :long, else: :standard),
      tags: [:spb, catalog.family, availability_tag(catalog.availability)],
      strategy: {:driver_callback, __MODULE__, :execute},
      source: %{source_id: source.id, path: catalog.source_path, checksum: source.checksum.value},
      transformation_version: @transformation
    }

    with :ok <- Operation.validate(operation) do
      {:ok,
       %{
         operation: operation,
         family: catalog.family,
         availability: catalog.availability,
         dependencies: catalog.dependencies,
         frequency: catalog.frequency,
         inference: :not_applicable,
         template: nil,
         template_sha256: nil,
         local_path: nil
       }}
    end
  end

  # Driver bridge callback; concrete execution is supplied by SPB.Execution.
  @doc false
  def execute(_context, _parameters), do: {:error, :spb_execution_context_required}

  defp validate_query(entry, binding) do
    with {:ok, parameters} <- Template.defaults(entry.template, binding.values),
         {:ok, query} <- Template.render(entry.template, parameters),
         {:ok, ast} <- Parser.parse(query),
         {:ok, _algebra} <- Algebra.from_ast(ast) do
      []
    else
      {:error, reason} -> [{entry.operation.id, reason}]
    end
  rescue
    error ->
      [{entry.operation.id, {:parser_exception, error.__struct__, Exception.message(error)}}]
  catch
    kind, reason -> [{entry.operation.id, {:parser_failure, kind, reason}}]
  end

  defp verify_checksums do
    root = spb_root()

    root
    |> Path.join("checksums.sha256")
    |> File.stream!()
    |> Enum.reduce_while(:ok, fn line, :ok ->
      [expected, relative] = String.split(String.trim(line), ~r/\s+/, parts: 2)
      path = Path.join(root, relative)

      case file_checksum(path) do
        {:ok, ^expected} -> {:cont, :ok}
        {:ok, actual} -> {:halt, {:error, {:checksum_mismatch, relative, expected, actual}}}
        {:error, reason} -> {:halt, {:error, {:source_read_failed, relative, reason}}}
      end
    end)
  rescue
    error -> {:error, {:checksum_manifest_invalid, Exception.message(error)}}
  end

  defp fetch_catalog(catalogs) do
    case Enum.find(catalogs, &(&1.id == "ldbc-spb-v2.0.2")) do
      nil -> {:error, :spb_catalog_missing}
      catalog -> {:ok, catalog}
    end
  end

  defp local_template_path(%{family: :aggregation, upstream_id: id}),
    do: Path.join([spb_root(), "aggregation", id <> ".txt"])

  defp local_template_path(%{family: :editorial, upstream_id: id}),
    do: Path.join([spb_root(), "editorial", String.downcase(id) <> ".txt"])

  defp local_template_path(%{family: :validation, upstream_id: id}),
    do: Path.join([spb_root(), "validation", id <> ".txt"])

  defp template_schema(:aggregation, template), do: Template.schema(template)

  defp template_schema(:editorial, template) do
    fields =
      if String.contains?(template, "INSERT DATA") do
        [
          field("cwGraphUri", :iri),
          field("cwUri", :iri),
          field("cwType", :iri),
          field("cwTitle", :string),
          field("cwShortTitle", :string),
          field("cwCategory", :iri),
          field("cwDescription", :string),
          field("cwAboutsList", {:list, :iri}),
          field("cwMentionsList", {:list, :iri}),
          field("cwAudienceType", :iri),
          field("cwLiveCoverage", :iri),
          field("cwPrimaryFormatList", {:list, :iri}),
          field("cwDateCreated", :timestamp),
          field("cwDateModified", :timestamp),
          field("cwThumbnailUri", :iri),
          field("cwPrimaryContentList", {:list, %{uri: :iri, web_document_type: :iri}})
        ]
      else
        [field("cwGraphUri", :iri)]
      end

    {:ok, fields}
  end

  defp template_schema(:validation, _template), do: {:ok, [field("contextURI", :iri)]}

  defp field(name, type), do: %{name: name, type: type, required: true}

  defp file_checksum(path) do
    with {:ok, bytes} <- File.read(path) do
      {:ok, :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)}
    end
  end

  defp spb_root do
    case :code.priv_dir(:triple_store) do
      {:error, _} -> Path.expand("../../../../../priv/benchmarks/ldbc/spb", __DIR__)
      path -> Path.join(to_string(path), "benchmarks/ldbc/spb")
    end
  end

  defp kind(:aggregation), do: :read
  defp kind(:editorial), do: :write
  defp kind(:validation), do: :validation
  defp kind(:conformance), do: :validation
  defp kind(:lifecycle), do: :batch
  defp kind(:resilience), do: :resilience

  defp result_schema(:aggregation), do: [%{name: "solutions", type: :rdf_term}]
  defp result_schema(:editorial), do: [%{name: "stateTransition", type: :boolean}]
  defp result_schema(:validation), do: [%{name: "valid", type: :boolean}]
  defp result_schema(:conformance), do: [%{name: "conforms", type: :boolean}]
  defp result_schema(_family), do: [%{name: "completed", type: :boolean}]

  defp ordering(:query_defined), do: %{mode: :ordered, keys: []}
  defp ordering(:editorial_stream), do: %{mode: :ordered, keys: []}
  defp ordering(_), do: %{mode: :not_applicable}

  defp static_limit(template) do
    case Regex.run(~r/LIMIT\s+(\d+)\s*\z/i, String.trim(template), capture: :all_but_first) do
      [limit] -> String.to_integer(limit)
      _ -> nil
    end
  end

  defp inference_expectation(template) do
    if String.contains?(String.downcase(template), ["rdfs:", "owl:", "subclass", "sameas"]),
      do: :required,
      else: :query_visible
  end

  defp availability_tag(:mandatory), do: :core
  defp availability_tag(:optional), do: :optional
  defp availability_tag(:audit_only), do: :audit_only
end
