defmodule TripleStore.Benchmark.LDBC.SNB.BI.HandlerRegistry do
  @moduledoc """
  Versioned registry for the standard SNB BI analytical translations.

  The registry has one entry for every SPARQL-backed BI variant. Graph
  extension variants are owned by their operation-specific adapters and are
  deliberately excluded. Building a registry validates exact coverage before
  any benchmark run can use it.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Handlers.StandardQuery

  @version "triplestore-snb-bi-handler-registry-v1"
  @standard_keys [
    {1, "default"},
    {2, "a"},
    {2, "b"},
    {2, "m"},
    {3, "default"},
    {4, "default"},
    {5, "default"},
    {6, "default"},
    {7, "default"},
    {8, "a"},
    {8, "b"},
    {8, "m"},
    {9, "default"},
    {11, "default"},
    {12, "default"},
    {13, "default"},
    {14, "a"},
    {14, "b"},
    {16, "a"},
    {16, "b"},
    {17, "default"},
    {18, "default"}
  ]

  @type key :: {pos_integer(), String.t()}
  @type entry :: %{
          required(:key) => key(),
          required(:version) => String.t(),
          required(:adapter) => module(),
          required(:translation_checksum) => String.t()
        }
  @type t :: %{required(:version) => String.t(), required(:handlers) => %{key() => entry()}}

  @doc "Returns the immutable registry version."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Returns all standard operation keys in canonical order."
  @spec keys() :: [key()]
  def keys, do: @standard_keys

  @doc "Builds a registry and rejects duplicate, missing, or unexpected standard definitions."
  @spec build([map()]) :: {:ok, t()} | {:error, term()}
  def build(definitions) when is_list(definitions) do
    standard = Enum.filter(definitions, &(key(&1) in @standard_keys))
    actual = Enum.map(standard, &key/1)
    duplicates = actual -- Enum.uniq(actual)
    missing = @standard_keys -- actual
    unexpected = Enum.uniq(actual) -- @standard_keys

    cond do
      duplicates != [] ->
        {:error, {:duplicate_bi_handlers, Enum.uniq(duplicates)}}

      missing != [] ->
        {:error, {:missing_bi_handlers, missing}}

      unexpected != [] ->
        {:error, {:unexpected_bi_handlers, unexpected}}

      true ->
        with {:ok, entries} <- entries(standard) do
          {:ok, %{version: @version, handlers: Map.new(entries, &{&1.key, &1})}}
        end
    end
  end

  def build(_definitions), do: {:error, :invalid_bi_definitions}

  @doc "Resolves one registered standard definition to its execution adapter."
  @spec fetch(map()) :: {:ok, entry()} | {:error, term()}
  def fetch(definition) when is_map(definition) do
    operation_key = key(definition)

    if operation_key in @standard_keys do
      entry(definition)
    else
      {:error, {:bi_operation_not_implemented, definition.number, definition.variant}}
    end
  end

  defp entries(definitions) do
    Enum.reduce_while(definitions, {:ok, []}, fn definition, {:ok, acc} ->
      case entry(definition) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp entry(%{translation: %{checksum: checksum}} = definition) when is_binary(checksum) do
    {:ok,
     %{
       key: key(definition),
       version: @version,
       adapter: StandardQuery,
       translation_checksum: checksum
     }}
  end

  defp entry(definition), do: {:error, {:missing_bi_translation, key(definition)}}

  defp key(%{number: number, variant: variant}), do: {number, variant}
end
