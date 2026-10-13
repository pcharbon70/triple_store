defmodule TripleStore.Benchmark.LDBC.SNB.BI.Parameters do
  @moduledoc """
  Provenance and type validation for SNB BI parameter sequences.

  Comparable runs accept only files produced by the pinned parameter generator
  for the exact dataset checksum. The row order is retained because the official
  power and throughput schedules consume each variant as a cyclic sequence.
  """

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{DatasetManifest, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SNB.BI.{ParameterReferences, Workload}

  @parameter_source_id "snb-bi-1.0.3"
  @parameter_generator_pin "5f7967235593eefa98b336e240039780b6a76a9a"

  @type bundle :: %{
          required(:dataset_checksum) => String.t(),
          required(:scale_factor) => String.t(),
          required(:source_id) => String.t(),
          required(:generator_pin) => String.t(),
          required(:provenance) => :official | :smoke,
          required(:dataset_identity) => map(),
          required(:sequences) => %{String.t() => [map()]}
        }

  @doc "Reads a deterministic parameter bundle from disk."
  @spec read(Path.t()) :: {:ok, bundle()} | {:error, term()}
  def read(path) do
    with {:ok, binary} <- File.read(path),
         bundle <- :erlang.binary_to_term(binary, [:safe]),
         :ok <- validate_shape(bundle) do
      {:ok, bundle}
    end
  rescue
    ArgumentError -> {:error, :invalid_parameter_bundle_encoding}
  end

  @doc "Writes a validated bundle and returns its SHA-256 checksum."
  @spec write(Path.t(), bundle()) :: {:ok, String.t()} | {:error, term()}
  def write(path, bundle) do
    with :ok <- validate_shape(bundle),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, :erlang.term_to_binary(bundle, [:deterministic]), [:binary]) do
      Artifact.checksum(path)
    end
  end

  @doc "Builds an ordered parameter bundle with its immutable provenance."
  @spec new(map(), map(), keyword()) :: {:ok, bundle()} | {:error, term()}
  def new(manifest, sequences, opts \\ []) when is_map(manifest) and is_map(sequences) do
    bundle = %{
      dataset_checksum: manifest.transformation.output_checksum,
      scale_factor: manifest.scale_factor,
      source_id: Keyword.get(opts, :source_id, @parameter_source_id),
      generator_pin: Keyword.get(opts, :generator_pin, @parameter_generator_pin),
      provenance: Keyword.get(opts, :provenance, :official),
      dataset_identity: dataset_identity(manifest),
      sequences: sequences
    }

    with :ok <- validate_shape(bundle), do: {:ok, bundle}
  end

  @doc "Validates identity, row schemas, ordering, and comparable-run provenance."
  @spec validate(bundle(), map() | StoreFixture.t(), [Workload.definition()], keyword()) ::
          :ok | {:error, [term()]}
  def validate(bundle, manifest_or_fixture, definitions, opts \\ [])

  def validate(bundle, %StoreFixture{} = fixture, definitions, opts) do
    manifest = fixture.manifest
    comparable? = Keyword.get(opts, :comparable, false)
    reference_profile = Keyword.get(opts, :reference_profile, :official)

    identity_errors =
      []
      |> require(
        bundle.dataset_checksum == manifest.transformation.output_checksum,
        :dataset_checksum
      )
      |> require(bundle.scale_factor == manifest.scale_factor, :scale_factor)
      |> require(not comparable? or bundle.provenance == :official, :official_provenance)
      |> require(not comparable? or bundle.source_id == @parameter_source_id, :parameter_source)
      |> require(
        not comparable? or bundle.generator_pin == @parameter_generator_pin,
        :generator_pin
      )
      |> require(bundle.dataset_identity == dataset_identity(manifest), :dataset_identity)
      |> Kernel.++(store_identity_errors(fixture))

    errors =
      if identity_errors == [] do
        case ParameterReferences.validate_profile(
               definitions,
               reference_profile,
               manifest,
               bundle
             ) do
          :ok ->
            sequence_errors(bundle.sequences, definitions, {reference_profile, fixture.store})

          {:error, reason} ->
            [reason]
        end
      else
        identity_errors
      end

    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(bundle, manifest, definitions, opts) when is_map(manifest) do
    comparable? = Keyword.get(opts, :comparable, false)
    reference_profile = Keyword.get(opts, :reference_profile)

    errors =
      []
      |> require(
        bundle.dataset_checksum == manifest.transformation.output_checksum,
        :dataset_checksum
      )
      |> require(bundle.scale_factor == manifest.scale_factor, :scale_factor)
      |> require(not comparable? or bundle.provenance == :official, :official_provenance)
      |> require(not comparable? or bundle.source_id == @parameter_source_id, :parameter_source)
      |> require(
        not comparable? or bundle.generator_pin == @parameter_generator_pin,
        :generator_pin
      )
      |> require(bundle.dataset_identity == dataset_identity(manifest), :dataset_identity)
      |> require(not comparable? and is_nil(reference_profile), :reference_store_required)
      |> Kernel.++(sequence_errors(bundle.sequences, definitions, nil))

    if errors == [], do: :ok, else: {:error, errors}
  end

  defp sequence_errors(sequences, definitions, reference_context) do
    expected = MapSet.new(Workload.variant_labels(definitions))
    actual = sequences |> Map.keys() |> MapSet.new()

    coverage_errors =
      if expected == actual, do: [], else: [{:sequence_coverage, expected, actual}]

    coverage_errors ++
      Enum.flat_map(definitions, &definition_sequence_errors(&1, sequences, reference_context))
  end

  defp definition_sequence_errors(definition, sequences, reference_context) do
    operation_label = label(definition)

    case Map.get(sequences, operation_label, []) do
      [] ->
        [{operation_label, :empty_sequence}]

      rows ->
        rows
        |> Enum.with_index()
        |> Enum.flat_map(fn {row, index} ->
          row_errors(row, index, definition, reference_context)
        end)
    end
  end

  defp row_errors(row, index, definition, reference_context) when is_map(row) do
    expected_names = Enum.map(definition.parameters, & &1.name)
    actual_names = row |> Map.keys() |> Enum.sort()

    name_errors =
      if Enum.sort(expected_names) == actual_names,
        do: [],
        else: [{label(definition), index, :parameter_names, expected_names, actual_names}]

    name_errors ++
      Enum.flat_map(
        definition.parameters,
        &field_errors(&1, row, index, definition, reference_context)
      )
  end

  defp row_errors(_row, index, definition, _reference_context),
    do: [{label(definition), index, :invalid_row}]

  defp field_errors(field, row, index, definition, reference_context) do
    value = Map.get(row, field.name)

    case validate_value(field.type, value) do
      :ok -> reference_errors(reference_context, field, value, index, definition)
      {:error, reason} -> [{label(definition), index, field.name, reason}]
    end
  end

  defp reference_errors(nil, _field, _value, _index, _definition), do: []

  defp reference_errors({profile, store}, field, value, index, definition) do
    if ParameterReferences.selected?(definition, profile, field.name) do
      rule = Map.fetch!(definition.parameter_references, field.name)

      case ParameterReferences.validate_rule(rule, value, store) do
        :ok -> []
        {:error, reason} -> [{label(definition), index, field.name, reason}]
      end
    else
      []
    end
  end

  defp validate_value("ID", value), do: positive_integer(value)
  defp validate_value("32-bit Integer", value), do: integer(value, -2_147_483_648, 2_147_483_647)

  defp validate_value("64-bit Integer", value),
    do: integer(value, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807)

  defp validate_value("32-bit Float", value) when is_float(value) or is_integer(value), do: :ok
  defp validate_value("Boolean", value) when is_boolean(value), do: :ok

  defp validate_value(type, value) when type in ["String", "Long String"] and is_binary(value),
    do: :ok

  defp validate_value("\\{String\\}", value) when is_list(value) do
    if Enum.all?(value, &is_binary/1), do: :ok, else: {:error, :invalid_string_set}
  end

  defp validate_value("Date", value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, _date} -> :ok
      _error -> {:error, :invalid_date}
    end
  end

  defp validate_value("DateTime", %DateTime{}), do: :ok

  defp validate_value("DateTime", value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _datetime, _offset} -> :ok
      _error -> {:error, :invalid_datetime}
    end
  end

  defp validate_value(_type, _value), do: {:error, :invalid_type}

  defp positive_integer(value) do
    case normalize_integer(value) do
      integer when is_integer(integer) and integer >= 0 -> :ok
      _other -> {:error, :invalid_id}
    end
  end

  defp integer(value, minimum, maximum) do
    case normalize_integer(value) do
      integer when is_integer(integer) and integer >= minimum and integer <= maximum -> :ok
      _other -> {:error, :integer_out_of_range}
    end
  end

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _other -> nil
    end
  end

  defp normalize_integer(_value), do: nil

  defp validate_shape(%{
         dataset_checksum: checksum,
         scale_factor: scale_factor,
         source_id: source_id,
         generator_pin: generator_pin,
         provenance: provenance,
         dataset_identity: dataset_identity,
         sequences: sequences
       })
       when is_binary(checksum) and is_binary(scale_factor) and is_binary(source_id) and
              is_binary(generator_pin) and provenance in [:official, :smoke] and
              is_map(dataset_identity) and is_map(sequences),
       do: :ok

  defp validate_shape(_bundle), do: {:error, :invalid_parameter_bundle}

  defp label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp dataset_identity(%DatasetManifest{} = manifest), do: DatasetManifest.identity(manifest)

  defp dataset_identity(manifest) when is_map(manifest) do
    %{
      dataset_id: Map.get(manifest, :dataset_id),
      suite: Map.get(manifest, :suite),
      profile: Map.get(manifest, :profile),
      scale_factor: manifest.scale_factor,
      output_checksum: manifest.transformation.output_checksum,
      mapping_version: get_in(manifest, [:transformation, :mapping_version]),
      store_path_identity: get_in(manifest, [:store, :path_identity])
    }
  end

  defp store_identity_errors(%StoreFixture{store: nil}), do: [:reference_store_closed]

  defp store_identity_errors(%StoreFixture{} = fixture) do
    manifest = fixture.manifest
    store = fixture.store

    []
    |> require(live_process?(store.db), :reference_store_closed)
    |> require(live_process?(store.dict_manager), :reference_store_closed)
    |> require(store.path == fixture.store_path, :store_identity)
    |> require(Path.basename(store.path) == manifest.store.path_identity, :store_identity)
    |> require(store.schema == manifest.store.schema, :store_schema)
    |> Enum.uniq()
  end

  defp live_process?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp live_process?(_pid), do: false

  defp require(errors, true, _error), do: errors
  defp require(errors, false, error), do: errors ++ [error]
end
