defmodule TripleStore.Benchmark.LDBC.SPB.Aggregation do
  @moduledoc """
  Correctness-first execution and plan evidence for all SPB aggregation queries.

  Every measured candidate is also executed through the non-optimized reference
  path. Rows retain duplicates and unbound values; unordered answers are sorted
  only for comparison. Graph answers are compared as RDF statement multisets.
  """

  alias TripleStore.Benchmark.LDBC.SPB.{Compatibility, Template}
  alias TripleStore.SPARQL.Query

  @required_features [
    :aggregate,
    :arithmetic_expression,
    :date_function,
    :string_function,
    :grouping,
    :ordering,
    :subquery,
    :construct,
    :property_path,
    :optional,
    :union
  ]

  @doc "Returns the semantic feature classes exercised by the complete corpus."
  @spec required_features() :: [atom()]
  def required_features, do: @required_features

  @doc "Instantiates one checksum-protected entry with strictly typed parameters."
  @spec instantiate(map(), map()) :: {:ok, map()} | {:error, term()}
  def instantiate(entry, binding) do
    with {:ok, executable} <-
           Compatibility.executable_text(entry.operation.upstream_id, entry.template),
         {:ok, parameters} <- Template.defaults(executable, binding.values),
         {:ok, query} <- Template.render(executable, parameters) do
      {:ok,
       %{
         operation_id: entry.operation.id,
         upstream_id: entry.operation.upstream_id,
         query: query,
         parameters: parameters,
         parameter_digest: digest(parameters),
         compatibility: entry.compatibility
       }}
    end
  end

  @doc "Executes all 25 queries through optimized and reference paths."
  @spec execute_all(map(), map(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def execute_all(context, package, binding, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, package.agent_mix.query_timeout_ms)

    records =
      package.operations
      |> Enum.filter(&(&1.family == :aggregation))
      |> Enum.map(&execute_entry(context, &1, binding, timeout))

    failures = Enum.reject(records, &(&1.status == :correct))

    report = %{
      operation_count: length(records),
      correct_count: length(records) - length(failures),
      failure_count: length(failures),
      score_eligible?: failures == [] and length(records) == 25,
      records: records
    }

    if failures == [], do: {:ok, report}, else: {:error, report}
  end

  @doc "Captures reference and optimized explain artifacts for the full corpus."
  @spec explain_all(map(), map(), map()) :: {:ok, [map()]} | {:error, term()}
  def explain_all(context, package, binding) do
    package.operations
    |> Enum.filter(&(&1.family == :aggregation))
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, plans} ->
      with {:ok, instantiated} <- instantiate(entry, binding),
           {:ok, {:explain, reference}} <-
             Query.query(context, instantiated.query, explain: true, optimize: false),
           {:ok, {:explain, optimized}} <-
             Query.query(context, instantiated.query, explain: true, optimize: true) do
        record = %{
          operation_id: entry.operation.id,
          reference: plan_evidence(reference),
          optimized: plan_evidence(optimized),
          answer_comparison_required?: true
        }

        {:cont, {:ok, [record | plans]}}
      else
        {:error, reason} -> {:halt, {:error, {entry.operation.id, reason}}}
      end
    end)
    |> case do
      {:ok, plans} -> {:ok, Enum.reverse(plans)}
      error -> error
    end
  end

  @doc "Canonicalizes an answer without dropping duplicates or unbound cells."
  @spec canonical_answer(term(), :ordered | :unordered) :: term()
  def canonical_answer(%RDF.Graph{} = graph, _ordering) do
    graph |> RDF.Graph.triples() |> Enum.sort_by(&:erlang.term_to_binary(&1, [:deterministic]))
  end

  def canonical_answer(%RDF.Dataset{} = dataset, _ordering) do
    dataset |> RDF.Dataset.quads() |> Enum.sort_by(&:erlang.term_to_binary(&1, [:deterministic]))
  end

  def canonical_answer(rows, :ordered) when is_list(rows), do: rows

  def canonical_answer(rows, :unordered) when is_list(rows) do
    Enum.sort_by(rows, &:erlang.term_to_binary(&1, [:deterministic]))
  end

  def canonical_answer(value, _ordering), do: value

  defp execute_entry(context, entry, binding, timeout) do
    with {:ok, instantiated} <- instantiate(entry, binding),
         {:ok, reference} <-
           Query.query(context, instantiated.query, timeout: timeout, optimize: false),
         {:ok, optimized} <-
           Query.query(context, instantiated.query, timeout: timeout, optimize: true),
         ordering <- result_ordering(instantiated.query),
         reference_answer <- canonical_answer(reference, ordering),
         optimized_answer <- canonical_answer(optimized, ordering),
         true <- reference_answer == optimized_answer do
      %{
        operation_id: entry.operation.id,
        status: :correct,
        ordering: ordering,
        result_count: result_count(optimized_answer),
        result_digest: digest(optimized_answer),
        parameter_digest: instantiated.parameter_digest,
        compatibility: instantiated.compatibility,
        reference_match?: true
      }
    else
      false -> failure(entry, :optimized_reference_mismatch)
      {:error, reason} -> failure(entry, reason)
    end
  rescue
    error -> failure(entry, {:exception, error.__struct__, Exception.message(error)})
  catch
    kind, reason -> failure(entry, {kind, reason})
  end

  defp failure(entry, reason),
    do: %{operation_id: entry.operation.id, status: :incorrect, reason: reason}

  defp result_ordering(query) do
    if Regex.match?(~r/\bORDER\s+BY\b/i, query), do: :ordered, else: :unordered
  end

  defp result_count(value) when is_list(value), do: length(value)
  defp result_count(%RDF.Graph{} = graph), do: RDF.Graph.triple_count(graph)
  defp result_count(%RDF.Dataset{} = dataset), do: RDF.Dataset.statement_count(dataset)
  defp result_count(_value), do: 1

  defp plan_evidence(plan) do
    encoded = :erlang.term_to_binary(plan, [:deterministic])

    %{
      digest: :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower),
      byte_size: byte_size(encoded),
      operators: collect_operators(plan) |> Enum.frequencies()
    }
  end

  defp collect_operators(tuple) when is_tuple(tuple) do
    values = Tuple.to_list(tuple)

    own =
      case values do
        [operator | _] when is_atom(operator) -> [operator]
        _ -> []
      end

    own ++ Enum.flat_map(values, &collect_operators/1)
  end

  defp collect_operators(list) when is_list(list), do: Enum.flat_map(list, &collect_operators/1)
  defp collect_operators(map) when is_map(map), do: map |> Map.values() |> collect_operators()
  defp collect_operators(_value), do: []

  defp digest(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
