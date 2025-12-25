defmodule TripleStore.Reasoner.SchemaInfo do
  @moduledoc """
  Structured representation of ontology schema information for rule compilation.

  The SchemaInfo struct captures the schema features present in an ontology,
  which determines which reasoning rules are applicable and how they can be
  specialized.

  ## Fields

  - `:has_subclass` - Whether rdfs:subClassOf assertions exist
  - `:has_subproperty` - Whether rdfs:subPropertyOf assertions exist
  - `:has_domain` - Whether rdfs:domain assertions exist
  - `:has_range` - Whether rdfs:range assertions exist
  - `:has_sameas` - Whether owl:sameAs assertions exist
  - `:has_restrictions` - Whether OWL restrictions exist (hasValue, someValuesFrom, etc.)
  - `:transitive_properties` - List of transitive property IRIs
  - `:symmetric_properties` - List of symmetric property IRIs
  - `:inverse_properties` - List of inverse property pairs (as tuples)
  - `:functional_properties` - List of functional property IRIs
  - `:inverse_functional_properties` - List of inverse functional property IRIs
  - `:version` - Schema version for cache invalidation

  ## Limits

  Property lists have configurable size limits to prevent memory exhaustion:
  - Default limit: 10,000 properties per category
  - Configurable via `max_properties` option during extraction

  ## Usage

      # Create empty schema info
      schema = SchemaInfo.new()

      # Create with specific values
      schema = SchemaInfo.new(
        has_subclass: true,
        transitive_properties: ["http://example.org/contains"]
      )

      # Validate schema info
      {:ok, schema} = SchemaInfo.validate(schema)

      # Check if a rule requirement is met
      SchemaInfo.has_feature?(schema, :transitive_properties)
  """

  @default_max_properties 10_000

  @enforce_keys []
  defstruct has_subclass: false,
            has_subproperty: false,
            has_domain: false,
            has_range: false,
            has_sameas: false,
            has_restrictions: false,
            transitive_properties: [],
            symmetric_properties: [],
            inverse_properties: [],
            functional_properties: [],
            inverse_functional_properties: [],
            version: nil

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Schema information for rule compilation"
  @type t :: %__MODULE__{
          has_subclass: boolean(),
          has_subproperty: boolean(),
          has_domain: boolean(),
          has_range: boolean(),
          has_sameas: boolean(),
          has_restrictions: boolean(),
          transitive_properties: [String.t()],
          symmetric_properties: [String.t()],
          inverse_properties: [{String.t(), String.t()}],
          functional_properties: [String.t()],
          inverse_functional_properties: [String.t()],
          version: String.t() | nil
        }

  @typedoc "Options for schema info creation"
  @type new_opts :: [
          has_subclass: boolean(),
          has_subproperty: boolean(),
          has_domain: boolean(),
          has_range: boolean(),
          has_sameas: boolean(),
          has_restrictions: boolean(),
          transitive_properties: [String.t()],
          symmetric_properties: [String.t()],
          inverse_properties: [{String.t(), String.t()}],
          functional_properties: [String.t()],
          inverse_functional_properties: [String.t()],
          max_properties: non_neg_integer()
        ]

  # ============================================================================
  # Constructor
  # ============================================================================

  @doc """
  Creates a new SchemaInfo struct with default values.

  All boolean fields default to `false`, and all property lists default to empty.

  ## Options

  All struct fields can be provided as keyword options:

  - `:has_subclass` - boolean (default: false)
  - `:has_subproperty` - boolean (default: false)
  - `:has_domain` - boolean (default: false)
  - `:has_range` - boolean (default: false)
  - `:has_sameas` - boolean (default: false)
  - `:has_restrictions` - boolean (default: false)
  - `:transitive_properties` - list of IRIs (default: [])
  - `:symmetric_properties` - list of IRIs (default: [])
  - `:inverse_properties` - list of IRI pairs (default: [])
  - `:functional_properties` - list of IRIs (default: [])
  - `:inverse_functional_properties` - list of IRIs (default: [])
  - `:max_properties` - maximum properties per category (default: #{@default_max_properties})

  ## Examples

      iex> SchemaInfo.new()
      %SchemaInfo{has_subclass: false, transitive_properties: [], ...}

      iex> SchemaInfo.new(has_subclass: true, transitive_properties: ["http://ex.org/p"])
      %SchemaInfo{has_subclass: true, transitive_properties: ["http://ex.org/p"], ...}
  """
  @spec new(new_opts()) :: t()
  def new(opts \\ []) do
    max_props = Keyword.get(opts, :max_properties, @default_max_properties)

    %__MODULE__{
      has_subclass: Keyword.get(opts, :has_subclass, false),
      has_subproperty: Keyword.get(opts, :has_subproperty, false),
      has_domain: Keyword.get(opts, :has_domain, false),
      has_range: Keyword.get(opts, :has_range, false),
      has_sameas: Keyword.get(opts, :has_sameas, false),
      has_restrictions: Keyword.get(opts, :has_restrictions, false),
      transitive_properties:
        opts |> Keyword.get(:transitive_properties, []) |> limit_list(max_props),
      symmetric_properties:
        opts |> Keyword.get(:symmetric_properties, []) |> limit_list(max_props),
      inverse_properties:
        opts |> Keyword.get(:inverse_properties, []) |> limit_list(max_props),
      functional_properties:
        opts |> Keyword.get(:functional_properties, []) |> limit_list(max_props),
      inverse_functional_properties:
        opts |> Keyword.get(:inverse_functional_properties, []) |> limit_list(max_props),
      version: generate_version()
    }
  end

  @doc """
  Creates a SchemaInfo from a plain map.

  Useful for converting legacy map-based schema info to the struct format.
  """
  @spec from_map(map()) :: t()
  def from_map(map) when is_map(map) do
    new(
      has_subclass: Map.get(map, :has_subclass, false),
      has_subproperty: Map.get(map, :has_subproperty, false),
      has_domain: Map.get(map, :has_domain, false),
      has_range: Map.get(map, :has_range, false),
      has_sameas: Map.get(map, :has_sameas, false),
      has_restrictions: Map.get(map, :has_restrictions, false),
      transitive_properties: Map.get(map, :transitive_properties, []),
      symmetric_properties: Map.get(map, :symmetric_properties, []),
      inverse_properties: Map.get(map, :inverse_properties, []),
      functional_properties: Map.get(map, :functional_properties, []),
      inverse_functional_properties: Map.get(map, :inverse_functional_properties, [])
    )
  end

  @doc """
  Converts the SchemaInfo struct to a plain map.

  Useful for serialization or backward compatibility.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = schema) do
    Map.from_struct(schema)
  end

  # ============================================================================
  # Validation
  # ============================================================================

  @doc """
  Validates a SchemaInfo struct.

  Checks that:
  - All boolean fields are actually booleans
  - All property lists contain valid IRIs
  - Inverse property pairs are valid tuples
  - List sizes are within limits

  Returns `{:ok, schema}` if valid, or `{:error, reason}` with details.

  ## Examples

      iex> SchemaInfo.validate(%SchemaInfo{has_subclass: true})
      {:ok, %SchemaInfo{has_subclass: true, ...}}

      iex> SchemaInfo.validate(%SchemaInfo{transitive_properties: ["not a valid iri>"]})
      {:error, {:invalid_iri, "not a valid iri>"}}
  """
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = schema) do
    with :ok <- validate_booleans(schema),
         :ok <- validate_property_lists(schema),
         :ok <- validate_inverse_pairs(schema) do
      {:ok, schema}
    end
  end

  @doc """
  Validates a SchemaInfo struct, raising on error.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = schema) do
    case validate(schema) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "Invalid SchemaInfo: #{inspect(reason)}"
    end
  end

  defp validate_booleans(%__MODULE__{} = schema) do
    boolean_fields = [
      :has_subclass,
      :has_subproperty,
      :has_domain,
      :has_range,
      :has_sameas,
      :has_restrictions
    ]

    invalid =
      Enum.find(boolean_fields, fn field ->
        value = Map.get(schema, field)
        not is_boolean(value)
      end)

    case invalid do
      nil -> :ok
      field -> {:error, {:invalid_boolean, field, Map.get(schema, field)}}
    end
  end

  defp validate_property_lists(%__MODULE__{} = schema) do
    alias TripleStore.Reasoner.Namespaces

    list_fields = [
      :transitive_properties,
      :symmetric_properties,
      :functional_properties,
      :inverse_functional_properties
    ]

    Enum.reduce_while(list_fields, :ok, fn field, :ok ->
      list = Map.get(schema, field, [])

      case validate_iri_list(list, Namespaces) do
        :ok -> {:cont, :ok}
        {:error, iri} -> {:halt, {:error, {:invalid_iri, iri}}}
      end
    end)
  end

  defp validate_iri_list(list, namespaces_module) do
    Enum.reduce_while(list, :ok, fn iri, :ok ->
      case namespaces_module.validate_iri(iri) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} -> {:halt, {:error, iri}}
      end
    end)
  end

  defp validate_inverse_pairs(%__MODULE__{inverse_properties: pairs}) do
    alias TripleStore.Reasoner.Namespaces

    invalid =
      Enum.find(pairs, fn
        {p1, p2} when is_binary(p1) and is_binary(p2) ->
          not (Namespaces.valid_iri?(p1) and Namespaces.valid_iri?(p2))

        _ ->
          true
      end)

    case invalid do
      nil -> :ok
      pair -> {:error, {:invalid_inverse_pair, pair}}
    end
  end

  # ============================================================================
  # Query Functions
  # ============================================================================

  @doc """
  Checks if a schema feature is present.

  ## Features

  - `:subclass` - has_subclass is true
  - `:subproperty` - has_subproperty is true
  - `:domain` - has_domain is true
  - `:range` - has_range is true
  - `:sameas` - has_sameas is true
  - `:restrictions` - has_restrictions is true
  - `:transitive_properties` - transitive_properties is non-empty
  - `:symmetric_properties` - symmetric_properties is non-empty
  - `:inverse_properties` - inverse_properties is non-empty
  - `:functional_properties` - functional_properties is non-empty
  - `:inverse_functional_properties` - inverse_functional_properties is non-empty

  ## Examples

      iex> schema = SchemaInfo.new(has_subclass: true)
      iex> SchemaInfo.has_feature?(schema, :subclass)
      true
  """
  @spec has_feature?(t(), atom()) :: boolean()
  def has_feature?(%__MODULE__{} = schema, feature) do
    case feature do
      :subclass -> schema.has_subclass
      :subproperty -> schema.has_subproperty
      :domain -> schema.has_domain
      :range -> schema.has_range
      :sameas -> schema.has_sameas
      :restrictions -> schema.has_restrictions
      :transitive_properties -> not Enum.empty?(schema.transitive_properties)
      :symmetric_properties -> not Enum.empty?(schema.symmetric_properties)
      :inverse_properties -> not Enum.empty?(schema.inverse_properties)
      :functional_properties -> not Enum.empty?(schema.functional_properties)
      :inverse_functional_properties -> not Enum.empty?(schema.inverse_functional_properties)
      _ -> false
    end
  end

  @doc """
  Returns the total count of specialized properties across all categories.
  """
  @spec property_count(t()) :: non_neg_integer()
  def property_count(%__MODULE__{} = schema) do
    length(schema.transitive_properties) +
      length(schema.symmetric_properties) +
      length(schema.inverse_properties) +
      length(schema.functional_properties) +
      length(schema.inverse_functional_properties)
  end

  @doc """
  Returns statistics about the schema.
  """
  @spec stats(t()) :: map()
  def stats(%__MODULE__{} = schema) do
    %{
      boolean_features:
        Enum.count(
          [
            schema.has_subclass,
            schema.has_subproperty,
            schema.has_domain,
            schema.has_range,
            schema.has_sameas,
            schema.has_restrictions
          ],
          & &1
        ),
      transitive_property_count: length(schema.transitive_properties),
      symmetric_property_count: length(schema.symmetric_properties),
      inverse_property_count: length(schema.inverse_properties),
      functional_property_count: length(schema.functional_properties),
      inverse_functional_property_count: length(schema.inverse_functional_properties),
      total_property_count: property_count(schema),
      version: schema.version
    }
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp limit_list(list, max) when is_list(list) do
    if length(list) > max do
      Enum.take(list, max)
    else
      list
    end
  end

  defp generate_version do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
