defmodule TripleStore.Benchmark.LDBC.SNB.BI.Workload do
  @moduledoc """
  Versioned execution contracts for the pinned SNB BI workload.

  The upstream catalog remains the authority for parameters, result columns,
  ordering, limits, variants, and choke points. This module turns that catalog
  into executable definitions without copying those contracts into source code.
  Operations which require path length or weighted cost are represented by an
  explicit extension identifier; they are never presented as SPARQL queries.
  """

  alias TripleStore.Benchmark.LDBC.Catalog
  alias TripleStore.Benchmark.LDBC.SNB.BI.{Analytics, TranslationCorpus}

  @catalog_id "ldbc-snb-bi-v1.0.3"
  @translation_version "triplestore-snb-bi-v1"
  @source_commit "5f7956e07a214373c363b371a3b88bc83ddcd118"
  @graph_operations %{
    10 => "ldbc.snb.bi.shortest-path-range.v1",
    15 => "ldbc.snb.bi-bounded-path.v1",
    19 => "ldbc.snb.bi-weighted-interaction-path.v1",
    20 => "ldbc.snb.bi-weighted-tag-path.v1"
  }

  @type definition :: %{
          required(:id) => String.t(),
          required(:catalog_id) => String.t(),
          required(:number) => 1..20,
          required(:variant) => String.t(),
          required(:parameters) => [map()],
          required(:result) => [map()],
          required(:ordering) => [map()],
          required(:limit) => non_neg_integer() | nil,
          required(:strategy) => {:native, module(), atom()} | {:extension, String.t(), module()},
          required(:source) => map(),
          required(:transformation_version) => String.t(),
          required(:parameter_references) => %{String.t() => TranslationCorpus.reference_rule()},
          required(:reference_profiles) => %{atom() => map()},
          optional(:translation) => TranslationCorpus.entry()
        }

  @doc "Returns the immutable upstream catalog identity for this workload."
  @spec catalog_id() :: String.t()
  def catalog_id, do: @catalog_id

  @doc "Loads and validates every BI read and canonical variant in stable order."
  @spec load() :: {:ok, [definition()]} | {:error, term()}
  def load do
    with {:ok, catalogs} <- Catalog.load_all(),
         {:ok, catalog} <- fetch_catalog(catalogs),
         definitions <- build_definitions(catalog),
         :ok <- validate(definitions, catalog),
         {:ok, corpus} <- TranslationCorpus.load(definitions),
         {:ok, references} <- TranslationCorpus.reference_metadata(definitions) do
      {:ok, definitions |> bind_translations(corpus) |> bind_references(references)}
    end
  end

  @doc "Validates exact catalog coverage and the copied execution contracts."
  @spec validate([definition()], map()) :: :ok | {:error, [term()]}
  def validate(definitions, catalog) when is_list(definitions) and is_map(catalog) do
    reads = Enum.filter(catalog.operations, &(&1.family == :read))
    expected = expected_keys(reads)
    actual = Enum.map(definitions, &{&1.catalog_id, &1.variant})

    errors =
      []
      |> require(actual == expected, {:coverage, expected, actual})
      |> Kernel.++(Enum.flat_map(definitions, &definition_errors/1))

    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(_definitions, _catalog), do: {:error, [:invalid_workload]}

  @doc "Returns one definition by canonical read number and variant."
  @spec fetch([definition()], pos_integer(), String.t()) ::
          {:ok, definition()} | {:error, term()}
  def fetch(definitions, number, variant \\ "default") do
    case Enum.find(definitions, &(&1.number == number and &1.variant == variant)) do
      nil -> {:error, {:unknown_bi_operation, number, variant}}
      definition -> {:ok, definition}
    end
  end

  @doc "Returns the canonical query variant labels in driver order."
  @spec variant_labels([definition()]) :: [String.t()]
  def variant_labels(definitions) do
    Enum.map(definitions, fn definition ->
      suffix = if definition.variant == "default", do: "", else: definition.variant
      Integer.to_string(definition.number) <> suffix
    end)
  end

  defp fetch_catalog(catalogs) do
    case Enum.find(catalogs, &(&1.id == @catalog_id)) do
      nil -> {:error, {:missing_catalog, @catalog_id}}
      catalog -> {:ok, catalog}
    end
  end

  defp build_definitions(catalog) do
    catalog.operations
    |> Enum.filter(&(&1.family == :read))
    |> Enum.flat_map(fn operation ->
      number = operation_number(operation)

      Enum.map(operation.variants, fn variant ->
        %{
          id: execution_id(number, variant, catalog.version),
          catalog_id: operation.id,
          number: number,
          variant: variant,
          parameters: operation.parameters,
          result: operation.result,
          ordering: operation.ordering,
          limit: operation.limit,
          duplicate_semantics: :bag,
          strategy: strategy(number),
          choke_points: operation.choke_points,
          source: %{
            source_id: catalog.source_id,
            path: operation.source_path,
            commit: @source_commit,
            reference: "ldbc_snb_bi:cypher/queries/bi-#{number}.cypher"
          },
          transformation_version: @translation_version
        }
      end)
    end)
  end

  defp strategy(number) do
    case Map.fetch(@graph_operations, number) do
      {:ok, extension_id} -> {:extension, extension_id, Analytics}
      :error -> {:native, Analytics, :execute}
    end
  end

  defp bind_translations(definitions, corpus) do
    translations = Map.new(corpus, &{{&1.operation_id, &1.variant}, &1})

    Enum.map(definitions, fn definition ->
      case Map.fetch(translations, {definition.catalog_id, definition.variant}) do
        {:ok, translation} -> Map.put(definition, :translation, translation)
        :error -> definition
      end
    end)
  end

  defp bind_references(definitions, %{rules: rules, profiles: profiles}) do
    Enum.map(definitions, fn definition ->
      definition
      |> Map.put(:parameter_references, Map.fetch!(rules, definition.number))
      |> Map.put(:reference_profiles, profiles)
    end)
  end

  defp expected_keys(reads) do
    Enum.flat_map(reads, fn operation ->
      Enum.map(operation.variants, &{operation.id, &1})
    end)
  end

  defp definition_errors(definition) do
    []
    |> require(definition.number in 1..20, {definition.id, :number})
    |> require(definition.parameters != nil, {definition.id, :parameters})
    |> require(definition.result != nil, {definition.id, :result})
    |> require(is_list(definition.ordering), {definition.id, :ordering})
    |> require(valid_strategy?(definition), {definition.id, :strategy})
    |> require(definition.source.commit == @source_commit, {definition.id, :source_pin})
  end

  defp valid_strategy?(%{number: number, strategy: {:extension, id, Analytics}}) do
    Map.get(@graph_operations, number) == id
  end

  defp valid_strategy?(%{number: number, strategy: {:native, Analytics, :execute}}),
    do: not Map.has_key?(@graph_operations, number)

  defp valid_strategy?(_definition), do: false

  defp operation_number(operation) do
    [number] = Regex.run(~r/read-(\d+)@/, operation.id, capture: :all_but_first)
    String.to_integer(number)
  end

  defp execution_id(number, "default", version),
    do: "ldbc/snb-bi/read-#{pad(number)}@#{version}"

  defp execution_id(number, variant, version),
    do: "ldbc/snb-bi/read-#{pad(number)}.#{variant}@#{version}"

  defp pad(number), do: number |> Integer.to_string() |> String.pad_leading(2, "0")

  defp require(errors, true, _error), do: errors
  defp require(errors, false, error), do: errors ++ [error]
end
