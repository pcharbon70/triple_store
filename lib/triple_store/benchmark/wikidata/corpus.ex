defmodule TripleStore.Benchmark.Wikidata.Corpus do
  @moduledoc """
  Container for a normalized benchmark query corpus.

  A corpus groups normalized queries, corpus-level provenance, and explicit
  exclusions so later runner and reporting phases can reason about imported
  workloads without losing why individual fragments were included or excluded.
  """

  alias TripleStore.Benchmark.Wikidata.{Contract, Query}

  @enforce_keys [:suite, :name, :metadata]
  defstruct version: 1,
            suite: nil,
            name: nil,
            description: nil,
            metadata: %{},
            queries: [],
            exclusions: []

  @type exclusion :: %{
          benchmark_id: String.t(),
          family: atom() | String.t(),
          reason: atom(),
          classification: atom(),
          fragment: String.t(),
          notes: String.t()
        }

  @type t :: %__MODULE__{
          version: pos_integer(),
          suite: Contract.workload_family(),
          name: String.t(),
          description: String.t() | nil,
          metadata: map(),
          queries: [Query.t()],
          exclusions: [exclusion()]
        }

  @type validation_error ::
          {:suite, String.t()}
          | {:name, String.t()}
          | {:metadata, String.t()}
          | {:queries, String.t()}
          | {:exclusions, String.t()}

  @doc """
  Builds and validates a query corpus.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, [validation_error()]}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    corpus = %__MODULE__{
      version: Map.get(attrs, :version, 1),
      suite: Map.get(attrs, :suite),
      name: Map.get(attrs, :name),
      description: Map.get(attrs, :description),
      metadata: Map.get(attrs, :metadata, %{}),
      queries: Map.get(attrs, :queries, []),
      exclusions: Map.get(attrs, :exclusions, [])
    }

    case validate(corpus) do
      :ok -> {:ok, corpus}
      {:error, errors} -> {:error, errors}
    end
  end

  @doc """
  Validates a query corpus struct.
  """
  @spec validate(t()) :: :ok | {:error, [validation_error()]}
  def validate(%__MODULE__{} = corpus) do
    errors =
      []
      |> validate_suite(corpus)
      |> validate_name(corpus)
      |> validate_metadata(corpus)
      |> validate_queries(corpus)
      |> validate_exclusions(corpus)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  @doc """
  Returns the benchmark IDs in the corpus.
  """
  @spec query_ids(t()) :: [String.t()]
  def query_ids(%__MODULE__{queries: queries}), do: Enum.map(queries, &Query.benchmark_id/1)

  @doc """
  Finds a query by benchmark ID.
  """
  @spec get(t(), String.t()) :: {:ok, Query.t()} | {:error, :not_found}
  def get(%__MODULE__{queries: queries}, benchmark_id) when is_binary(benchmark_id) do
    case Enum.find(queries, &(Query.benchmark_id(&1) == benchmark_id)) do
      nil -> {:error, :not_found}
      query -> {:ok, query}
    end
  end

  @doc """
  Returns queries that belong to a specific group.
  """
  @spec by_group(t(), atom() | String.t()) :: [Query.t()]
  def by_group(%__MODULE__{queries: queries}, group) do
    Enum.filter(queries, &(&1.group == group))
  end

  defp validate_suite(errors, %__MODULE__{suite: suite}) do
    if Contract.workload_family?(suite) do
      errors
    else
      [{:suite, "must be one of #{inspect(Contract.workload_families())}"} | errors]
    end
  end

  defp validate_name(errors, %__MODULE__{name: name}) when is_binary(name) and name != "",
    do: errors

  defp validate_name(errors, _corpus), do: [{:name, "must be a non-empty string"} | errors]

  defp validate_metadata(errors, %__MODULE__{metadata: metadata}) when is_map(metadata) do
    cond do
      not Map.has_key?(metadata, :source_origin) ->
        [{:metadata, "must include :source_origin"} | errors]

      not Map.has_key?(metadata, :license_notes) ->
        [{:metadata, "must include :license_notes"} | errors]

      not Map.has_key?(metadata, :preprocessing_history) ->
        [{:metadata, "must include :preprocessing_history"} | errors]

      true ->
        errors
    end
  end

  defp validate_metadata(errors, _corpus), do: [{:metadata, "must be a map"} | errors]

  defp validate_queries(errors, %__MODULE__{suite: suite, queries: queries})
       when is_list(queries) do
    cond do
      not Enum.all?(queries, &match?(%Query{}, &1)) ->
        [{:queries, "must contain only benchmark query structs"} | errors]

      not Enum.all?(queries, &(&1.manifest.suite == suite)) ->
        [{:queries, "must all belong to the corpus suite"} | errors]

      length(Enum.uniq(Enum.map(queries, &Query.benchmark_id/1))) != length(queries) ->
        [{:queries, "must use unique benchmark IDs"} | errors]

      true ->
        errors
    end
  end

  defp validate_queries(errors, _corpus), do: [{:queries, "must be a list"} | errors]

  defp validate_exclusions(errors, %__MODULE__{exclusions: exclusions})
       when is_list(exclusions) do
    if Enum.all?(exclusions, &valid_exclusion?/1) do
      errors
    else
      [{:exclusions, "must contain explicit exclusion metadata"} | errors]
    end
  end

  defp validate_exclusions(errors, _corpus), do: [{:exclusions, "must be a list"} | errors]

  defp valid_exclusion?(%{
         benchmark_id: benchmark_id,
         family: family,
         reason: reason,
         classification: classification,
         fragment: fragment,
         notes: notes
       })
       when is_binary(benchmark_id) and benchmark_id != "" and
              (is_atom(family) or is_binary(family)) and is_atom(reason) and
              is_atom(classification) and is_binary(fragment) and fragment != "" and
              is_binary(notes) and notes != "",
       do: true

  defp valid_exclusion?(_), do: false
end
