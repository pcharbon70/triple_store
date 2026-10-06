defmodule TripleStore.Benchmark.LDBC.DatasetManifest do
  @moduledoc """
  Versioned provenance and store-state record for an LDBC dataset.

  Source, transformation, and loaded-store metadata remain separate so a source
  artifact can be reused without confusing it with a particular RocksDB image.
  Manifests use deterministic Erlang external-term encoding and safe decoding.
  """

  @schema_version 1
  @suites [:spb, :snb_bi, :snb_interactive]
  @schemas [:triple, :quad]

  @enforce_keys [
    :dataset_id,
    :suite,
    :profile,
    :scale_factor,
    :source,
    :transformation,
    :store,
    :components
  ]
  defstruct schema_version: @schema_version,
            dataset_id: nil,
            suite: nil,
            profile: nil,
            scale_factor: nil,
            source: nil,
            transformation: nil,
            store: nil,
            components: []

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          dataset_id: String.t(),
          suite: atom(),
          profile: String.t(),
          scale_factor: String.t() | number(),
          source: map(),
          transformation: map(),
          store: map(),
          components: [map()]
        }

  @doc "Returns the only manifest schema version understood by this release."
  @spec schema_version() :: pos_integer()
  def schema_version, do: @schema_version

  @doc "Builds and validates a dataset manifest."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)
    version = Map.get(attrs, :schema_version, @schema_version)

    if version != @schema_version do
      {:error, {:unsupported_schema_version, version, @schema_version}}
    else
      manifest = struct(__MODULE__, attrs)

      case validate(manifest) do
        :ok -> {:ok, manifest}
        {:error, _} = error -> error
      end
    end
  rescue
    KeyError -> {:error, :missing_required_field}
  end

  @doc "Validates provenance, transformation, store, and component metadata."
  @spec validate(t()) :: :ok | {:error, [term()]}
  def validate(%__MODULE__{} = manifest) do
    errors =
      []
      |> require_value(:dataset_id, manifest.dataset_id, &non_empty_string?/1)
      |> require_value(:suite, manifest.suite, &(&1 in @suites))
      |> require_value(:profile, manifest.profile, &non_empty_string?/1)
      |> require_value(:scale_factor, manifest.scale_factor, &valid_scale?/1)
      |> validate_source(manifest.source)
      |> validate_transformation(manifest.transformation)
      |> validate_store(manifest.store)
      |> validate_components(manifest.components)

    if errors == [], do: :ok, else: {:error, Enum.reverse(errors)}
  end

  @doc "Encodes a validated manifest deterministically."
  @spec encode(t()) :: {:ok, binary()} | {:error, term()}
  def encode(%__MODULE__{} = manifest) do
    with :ok <- validate(manifest) do
      {:ok, :erlang.term_to_binary(Map.from_struct(manifest), [:deterministic])}
    end
  end

  @doc "Decodes a manifest without creating atoms or evaluating source code."
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(binary) when is_binary(binary) do
    binary
    |> :erlang.binary_to_term([:safe])
    |> new()
  rescue
    ArgumentError -> {:error, :invalid_manifest_encoding}
  end

  @doc "Writes a manifest through an atomic sibling temporary file."
  @spec write(t(), Path.t()) :: :ok | {:error, term()}
  def write(%__MODULE__{} = manifest, path) when is_binary(path) do
    with {:ok, encoded} <- encode(manifest),
         :ok <- File.mkdir_p(Path.dirname(path)) do
      temporary = path <> ".partial.#{System.unique_integer([:positive])}"

      case File.write(temporary, encoded, [:binary, :exclusive]) do
        :ok ->
          case File.rename(temporary, path) do
            :ok ->
              :ok

            {:error, _} = error ->
              File.rm(temporary)
              error
          end

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Reads and safely decodes a manifest file."
  @spec read(Path.t()) :: {:ok, t()} | {:error, term()}
  def read(path) when is_binary(path) do
    with {:ok, binary} <- File.read(path), do: decode(binary)
  end

  @doc "Returns the stable identity fields that bind parameters to a dataset."
  @spec identity(t()) :: map()
  def identity(%__MODULE__{} = manifest) do
    %{
      dataset_id: manifest.dataset_id,
      suite: manifest.suite,
      profile: manifest.profile,
      scale_factor: manifest.scale_factor,
      output_checksum: manifest.transformation.output_checksum,
      mapping_version: manifest.transformation.mapping_version
    }
  end

  defp validate_source(errors, source) when is_map(source) do
    required = [
      {:generator_source_id, &non_empty_string?/1},
      {:generator_pin, &immutable_pin?/1},
      {:generator_settings, &is_map/1},
      {:seed, &is_integer/1},
      {:format, &(is_atom(&1) or non_empty_string?(&1))},
      {:checksum, &checksum?/1},
      {:license, &non_empty_string?/1}
    ]

    require_map_fields(errors, :source, source, required)
  end

  defp validate_source(errors, _source), do: [{:source, :must_be_map} | errors]

  defp validate_transformation(errors, transformation) when is_map(transformation) do
    required = [
      {:version, &non_empty_string?/1},
      {:mapping_version, &non_empty_string?/1},
      {:output_checksum, &checksum?/1},
      {:statement_count, &non_negative_integer?/1},
      {:entity_count, &non_negative_integer?/1},
      {:relationship_count, &non_negative_integer?/1},
      {:update_streams, &is_list/1}
    ]

    require_map_fields(errors, :transformation, transformation, required)
  end

  defp validate_transformation(errors, _), do: [{:transformation, :must_be_map} | errors]

  defp validate_store(errors, store) when is_map(store) do
    required = [
      {:schema, &(&1 in @schemas)},
      {:loader_settings, &is_map/1},
      {:path_identity, &non_empty_string?/1},
      {:post_load_stats, &is_map/1}
    ]

    require_map_fields(errors, :store, store, required)
  end

  defp validate_store(errors, _), do: [{:store, :must_be_map} | errors]

  defp validate_components(errors, components) when is_list(components) and components != [] do
    component_errors =
      components
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {component, index} when is_map(component) ->
          []
          |> require_map_fields({:component, index}, component, [
            {:role, &(is_atom(&1) or non_empty_string?(&1))},
            {:path, &non_empty_string?/1},
            {:checksum, &checksum?/1},
            {:count, &non_negative_integer?/1}
          ])
          |> Enum.reverse()

        {_component, index} ->
          [{{:component, index}, :must_be_map}]
      end)

    Enum.reverse(component_errors) ++ errors
  end

  defp validate_components(errors, _), do: [{:components, :must_be_non_empty_list} | errors]

  defp require_map_fields(errors, scope, map, fields) do
    Enum.reduce(fields, errors, fn {field, predicate}, acc ->
      case Map.fetch(map, field) do
        {:ok, value} -> require_value(acc, {scope, field}, value, predicate)
        :error -> [{{scope, field}, :required} | acc]
      end
    end)
  end

  defp require_value(errors, field, value, predicate) do
    if predicate.(value), do: errors, else: [{field, :invalid} | errors]
  end

  defp non_empty_string?(value), do: is_binary(value) and value != ""
  defp non_negative_integer?(value), do: is_integer(value) and value >= 0
  defp valid_scale?(value), do: (is_number(value) and value > 0) or non_empty_string?(value)

  defp checksum?("sha256:" <> digest), do: byte_size(digest) == 64 and digest =~ ~r/\A[0-9a-f]+\z/
  defp checksum?(_), do: false

  defp immutable_pin?(pin) when is_binary(pin) do
    String.match?(pin, ~r/\A[0-9a-f]{40}\z/) or String.match?(pin, ~r/\Asha256:[0-9a-f]{64}\z/)
  end

  defp immutable_pin?(_), do: false
end
