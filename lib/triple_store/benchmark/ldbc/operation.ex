defmodule TripleStore.Benchmark.LDBC.Operation do
  @moduledoc """
  Versioned, suite-neutral description of one executable LDBC operation.

  The operation points back to a canonical Phase 1 catalog entry and a pinned
  source artifact. Query parameters are represented by a schema and are passed
  to the prepared-query API; they are never interpolated into SPARQL text.
  """

  @schema_version 1
  @kinds [:read, :write, :batch, :validation, :resilience]
  @timeout_classes [:short, :standard, :long]
  @ordering_modes [:ordered, :unordered, :not_applicable]

  @enforce_keys [
    :id,
    :suite,
    :profile_id,
    :catalog_id,
    :upstream_id,
    :kind,
    :parameter_schema,
    :result_schema,
    :ordering,
    :limit,
    :timeout_class,
    :tags,
    :strategy,
    :source,
    :transformation_version
  ]
  defstruct [
    :id,
    :suite,
    :profile_id,
    :catalog_id,
    :upstream_id,
    :kind,
    :parameter_schema,
    :result_schema,
    :ordering,
    :limit,
    :timeout_class,
    :tags,
    :strategy,
    :source,
    :transformation_version,
    schema_version: @schema_version
  ]

  @type field_schema :: %{
          required(:name) => String.t(),
          required(:type) => term(),
          optional(:required) => boolean()
        }
  @type strategy ::
          {:sparql, String.t()}
          | {:native, module(), atom()}
          | {:update_batch, atom()}
          | {:driver_callback, module(), atom()}
  @type t :: %__MODULE__{}

  @doc "Validates a complete operation definition."
  @spec validate(t()) :: :ok | {:error, [term()]}
  def validate(%__MODULE__{} = operation) do
    errors =
      []
      |> check(operation.schema_version == @schema_version, {:schema_version, :unsupported})
      |> check(is_binary(operation.id) and operation.id != "", {:id, :invalid})
      |> check(operation.suite in [:spb, :snb_bi, :snb_interactive], {:suite, :invalid})
      |> check(operation.kind in @kinds, {:kind, :unclassified})
      |> check(operation.timeout_class in @timeout_classes, {:timeout_class, :invalid})
      |> check(valid_ordering?(operation.ordering), {:ordering, :invalid})
      |> check(
        is_nil(operation.limit) or (is_integer(operation.limit) and operation.limit >= 0),
        {:limit, :invalid}
      )
      |> check(valid_fields?(operation.parameter_schema), {:parameter_schema, :invalid})
      |> check(valid_fields?(operation.result_schema), {:result_schema, :invalid})
      |> check(valid_strategy?(operation.strategy), {:strategy, :unclassified})
      |> check(valid_source?(operation.source), {:source, :invalid})
      |> check(
        is_binary(operation.transformation_version) and operation.transformation_version != "",
        {:transformation_version, :invalid}
      )

    if errors == [], do: :ok, else: {:error, Enum.reverse(errors)}
  end

  def validate(_operation), do: {:error, [{:operation, :invalid}]}

  @doc "Returns the declared parameter names in stable schema order."
  @spec parameter_names(t()) :: [String.t()]
  def parameter_names(%__MODULE__{parameter_schema: schema}), do: Enum.map(schema, & &1.name)

  @doc "Returns the declared result columns in stable schema order."
  @spec result_columns(t()) :: [String.t()]
  def result_columns(%__MODULE__{result_schema: schema}), do: Enum.map(schema, & &1.name)

  defp valid_fields?(fields) when is_list(fields) do
    names = Enum.map(fields, &Map.get(&1, :name))

    Enum.all?(fields, fn field ->
      is_map(field) and is_binary(Map.get(field, :name)) and Map.has_key?(field, :type)
    end) and Enum.uniq(names) == names
  end

  defp valid_fields?(_fields), do: false

  defp valid_ordering?(%{mode: mode}) when mode in @ordering_modes, do: true
  defp valid_ordering?(_ordering), do: false

  defp valid_strategy?({:sparql, query}), do: is_binary(query) and query != ""
  defp valid_strategy?({:native, module, function}), do: is_atom(module) and is_atom(function)
  defp valid_strategy?({:update_batch, role}), do: is_atom(role)

  defp valid_strategy?({:driver_callback, module, function}),
    do: is_atom(module) and is_atom(function)

  defp valid_strategy?(_strategy), do: false

  defp valid_source?(%{source_id: source_id, path: path, checksum: checksum}) do
    is_binary(source_id) and is_binary(path) and is_binary(checksum) and checksum != ""
  end

  defp valid_source?(_source), do: false

  defp check(errors, true, _error), do: errors
  defp check(errors, false, error), do: [error | errors]
end
