defmodule TripleStore.Benchmark.Wikidata.Baseline do
  @moduledoc """
  Reference-answer baselines for Wikidata benchmark runs.

  Baselines are intended for smoke and medium tiers where answer capture is
  practical and correctness regressions should be compared directly.
  """

  @doc """
  Builds a baseline document from a benchmark run result.
  """
  @spec from_run_result(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def from_run_result(run_result, opts \\ [])

  def from_run_result(%{query_runs: query_runs} = run_result, opts) when is_list(query_runs) do
    generated_at = Keyword.get(opts, :generated_at, DateTime.utc_now())
    baseline_id = Keyword.get(opts, :baseline_id, default_baseline_id(run_result))

    entries =
      query_runs
      |> Enum.filter(&(not is_nil(Map.get(&1, :answer_record))))
      |> Enum.map(fn query_run ->
        %{
          benchmark_id: query_run.benchmark_id,
          query_name: query_run.query_name,
          suite: query_run.suite,
          execution_variant: query_run.execution_variant,
          answer_record: query_run.answer_record
        }
      end)

    {:ok,
     %{
       schema_version: 1,
       baseline_id: baseline_id,
       generated_at: generated_at,
       dataset_manifest: Map.get(run_result, :dataset_manifest),
       runtime_config: Map.get(run_result, :runtime_config, %{}),
       entries: entries
     }}
  rescue
    KeyError -> {:error, :invalid_run_result}
  end

  def from_run_result(_run_result, _opts), do: {:error, :invalid_run_result}

  @doc """
  Finds a baseline entry by benchmark id and optional execution variant.
  """
  @spec lookup(map(), String.t(), atom() | nil) :: {:ok, map()} | {:error, :not_found}
  def lookup(%{entries: entries}, benchmark_id, execution_variant \\ nil)
      when is_list(entries) and is_binary(benchmark_id) do
    case Enum.find(entries, fn entry ->
           entry.benchmark_id == benchmark_id and
             (is_nil(execution_variant) or entry.execution_variant == execution_variant)
         end) do
      nil -> {:error, :not_found}
      entry -> {:ok, entry}
    end
  end

  @doc """
  Persists a baseline document in Erlang term format.
  """
  @spec write(Path.t(), map()) :: :ok | {:error, term()}
  def write(path, baseline) when is_binary(path) and is_map(baseline) do
    serializable =
      baseline
      |> Map.update(:generated_at, nil, &serialize_datetime/1)

    File.write(path, :erlang.term_to_binary(serializable))
  end

  @doc """
  Loads a baseline document from Erlang term format.
  """
  @spec load(Path.t()) :: {:ok, map()} | {:error, term()}
  def load(path) when is_binary(path) do
    with {:ok, binary} <- File.read(path),
         baseline when is_map(baseline) <- :erlang.binary_to_term(binary, [:safe]) do
      {:ok, Map.update(baseline, :generated_at, nil, &deserialize_datetime/1)}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_baseline_file}
    end
  end

  defp default_baseline_id(run_result) do
    tier =
      run_result
      |> Map.get(:runtime_config, %{})
      |> Map.get(:dataset_tier, :unknown)

    "wikidata-#{tier}-baseline"
  end

  defp serialize_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp serialize_datetime(other), do: other

  defp deserialize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> value
    end
  end

  defp deserialize_datetime(value), do: value
end
