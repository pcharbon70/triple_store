defmodule TripleStore.Benchmark.LDBC.SNB.BI.GraphAdapterRegistry do
  @moduledoc """
  Versioned registry for the SNB BI graph-extension operations.

  Every catalog variant of BI 10, 15, 19, and 20 resolves to one explicit
  operation adapter. The registry does not imply correctness qualification;
  capability promotion remains gated by accepted-answer evidence.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.Extensions.{Read10, Read15, Read19, Read20}

  @version "triplestore-snb-bi-graph-adapters-v1"
  @operations %{
    10 => {"ldbc.snb.bi.shortest-path-range.v1", Read10, ~w(a b)},
    15 => {"ldbc.snb.bi-bounded-path.v1", Read15, ~w(a b)},
    19 => {"ldbc.snb.bi-weighted-interaction-path.v1", Read19, ~w(a b)},
    20 => {"ldbc.snb.bi-weighted-tag-path.v1", Read20, ~w(a b m)}
  }

  @type entry :: %{
          required(:key) => {pos_integer(), String.t()},
          required(:version) => String.t(),
          required(:extension_id) => String.t(),
          required(:adapter) => module()
        }

  @doc "Returns the immutable graph-adapter registry version."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Returns the nine registered graph-operation variants in stable order."
  @spec keys() :: [{pos_integer(), String.t()}]
  def keys do
    @operations
    |> Enum.flat_map(fn {number, {_id, _module, variants}} ->
      Enum.map(variants, &{number, &1})
    end)
    |> Enum.sort()
  end

  @doc "Resolves one graph-extension definition to its operation adapter."
  @spec fetch(map()) :: {:ok, entry()} | {:error, term()}
  def fetch(%{number: number, variant: variant, strategy: {:extension, extension_id, _owner}}) do
    case Map.fetch(@operations, number) do
      {:ok, {^extension_id, adapter, variants}} ->
        if variant in variants do
          {:ok,
           %{
             key: {number, variant},
             version: @version,
             extension_id: extension_id,
             adapter: adapter
           }}
        else
          {:error, {:bi_operation_not_implemented, number, variant}}
        end

      {:ok, {expected, _adapter, _variants}} ->
        {:error, {:invalid_bi_graph_extension, number, expected, extension_id}}

      :error ->
        {:error, {:bi_operation_not_implemented, number, variant}}
    end
  end

  def fetch(%{number: number, variant: variant}),
    do: {:error, {:bi_operation_not_implemented, number, variant}}

  def fetch(_definition), do: {:error, :invalid_bi_definition}
end
