defmodule TripleStore.Benchmark.WatDiv.Runner do
  @moduledoc """
  Runs correctness-gated benchmarks over official WatDiv artifacts.

  The runner verifies dataset provenance and independent reference answers,
  loads N-Triples with bounded memory, executes every query once as a
  correctness gate, and only then records warmup and measurement iterations.
  Any answer divergence aborts the run before result artifacts are written.
  """

  alias TripleStore.Benchmark.WatDiv.{Provenance, ReferenceAnswers, Workload}
  alias TripleStore.Benchmark.Wikidata.AnswerNormalizer
  alias TripleStore.Loader

  @default_warmup_iterations 2
  @default_measurement_iterations 5
  @default_timeout_ms 30_000
  @default_batch_size 10_000

  @type run_result :: %{
          schema_version: pos_integer(),
          benchmark: String.t(),
          status: :passed,
          started_at: String.t(),
          completed_at: String.t(),
          dataset: map(),
          workload: map(),
          reference: map(),
          runtime: map(),
          load: map(),
          queries: [map()],
          artifact_dir: Path.t()
        }

  @doc "Runs a fresh, correctness-gated WatDiv benchmark."
  @spec run(keyword()) :: {:ok, run_result()} | {:error, term()}
  def run(opts) when is_list(opts) do
    with {:ok, config} <- config(opts),
         {:ok, dataset} <-
           Provenance.load_dataset_manifest(
             config.dataset_manifest_path,
             config.dataset_path
           ),
         {:ok, workload} <- Workload.load_dir(config.workload_dir),
         {:ok, reference} <- ReferenceAnswers.load(config.reference_dir, workload, dataset),
         :ok <- ensure_fresh_output(config.output_dir),
         :ok <- ensure_fresh_store(config.store_path) do
      run_with_store(config, dataset, workload, reference)
    end
  end

  defp run_with_store(config, dataset, workload, reference) do
    started_at = DateTime.utc_now()

    case TripleStore.open(config.store_path) do
      {:ok, store} ->
        try do
          with {:ok, load} <- load_dataset(store, dataset, config.batch_size),
               :ok <- verify_loaded_count(dataset.triple_count, load.triple_count),
               {:ok, verified_answers} <-
                 verify_all_answers(store, workload.queries, reference, config.timeout_ms),
               {:ok, query_results} <-
                 measure_queries(store, workload.queries, verified_answers, config),
               completed_at = DateTime.utc_now(),
               result =
                 build_result(
                   config,
                   dataset,
                   workload,
                   reference,
                   load,
                   query_results,
                   started_at,
                   completed_at
                 ),
               :ok <- write_artifacts(result, config.output_dir) do
            {:ok, result}
          end
        rescue
          error -> {:error, {:watdiv_run_failed, error, __STACKTRACE__}}
        after
          TripleStore.close(store)

          unless config.keep_store do
            File.rm_rf(config.store_path)
          end
        end

      {:error, reason} ->
        {:error, {:store_open_failed, reason}}
    end
  end

  defp config(opts) do
    with {:ok, dataset_path} <- required_path(opts, :dataset_path),
         {:ok, dataset_manifest_path} <- required_path(opts, :dataset_manifest_path),
         {:ok, workload_dir} <- required_path(opts, :workload_dir),
         {:ok, reference_dir} <- required_path(opts, :reference_dir),
         {:ok, output_dir} <- required_path(opts, :output_dir),
         {:ok, warmup_iterations} <-
           non_negative_option(opts, :warmup_iterations, @default_warmup_iterations),
         {:ok, measurement_iterations} <-
           positive_option(opts, :measurement_iterations, @default_measurement_iterations),
         {:ok, timeout_ms} <- positive_option(opts, :timeout_ms, @default_timeout_ms),
         {:ok, batch_size} <- positive_option(opts, :batch_size, @default_batch_size) do
      store_path =
        opts
        |> Keyword.get_lazy(:store_path, fn ->
          Path.join(
            System.tmp_dir!(),
            "triple_store_watdiv_#{System.unique_integer([:positive, :monotonic])}"
          )
        end)
        |> Path.expand()

      {:ok,
       %{
         dataset_path: dataset_path,
         dataset_manifest_path: dataset_manifest_path,
         workload_dir: workload_dir,
         reference_dir: reference_dir,
         output_dir: output_dir,
         store_path: store_path,
         keep_store: Keyword.get(opts, :keep_store, false),
         warmup_iterations: warmup_iterations,
         measurement_iterations: measurement_iterations,
         timeout_ms: timeout_ms,
         batch_size: batch_size
       }}
    end
  end

  defp load_dataset(store, dataset, batch_size) do
    triple_stream = ntriples_stream(dataset.dataset_path)

    {elapsed_us, result} =
      :timer.tc(fn ->
        Loader.load_stream(store.db, store.dict_manager, triple_stream,
          batch_size: batch_size,
          bulk_mode: true,
          parallel: false
        )
      end)

    case result do
      {:ok, count} ->
        throughput_tps = if elapsed_us == 0, do: 0.0, else: count * 1_000_000 / elapsed_us

        {:ok,
         %{
           triple_count: count,
           elapsed_us: elapsed_us,
           throughput_tps: throughput_tps,
           mode: :streaming_ntriples
         }}

      {:halted, count} ->
        {:error, {:dataset_load_halted, count}}

      {:error, reason} ->
        {:error, {:dataset_load_failed, reason}}
    end
  end

  defp ntriples_stream(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Stream.flat_map(fn line ->
      case RDF.NTriples.read_string(line) do
        {:ok, graph} -> RDF.Graph.triples(graph)
        {:error, reason} -> raise "invalid N-Triples statement: #{inspect(reason)}"
      end
    end)
  end

  defp verify_all_answers(store, queries, reference, timeout_ms) do
    Enum.reduce_while(queries, {:ok, %{}}, fn query, {:ok, verified} ->
      expected = Map.fetch!(reference.entries, query.id)

      case verify_answer(store, query, expected, timeout_ms) do
        {:ok, actual} -> {:cont, {:ok, Map.put(verified, query.id, actual)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp verify_answer(store, query, expected, timeout_ms) do
    case execute_and_normalize(store, query, timeout_ms) do
      {:ok, actual} ->
        case compare_answer(query.id, expected, actual) do
          :ok -> {:ok, actual}
          {:error, mismatch} -> {:error, {:correctness_mismatch, mismatch}}
        end

      {:error, reason} ->
        {:error, {:correctness_execution_failed, query.id, reason}}
    end
  end

  defp measure_queries(store, queries, verified_answers, config) do
    Enum.reduce_while(queries, {:ok, []}, fn query, {:ok, results} ->
      reference = Map.fetch!(verified_answers, query.id)

      case measure_query(store, query, reference, config) do
        {:ok, result} -> {:cont, {:ok, [result | results]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  defp measure_query(store, query, reference, config) do
    with :ok <-
           run_checked_iterations(
             store,
             query,
             reference,
             config.timeout_ms,
             config.warmup_iterations
           ),
         {:ok, timings} <-
           measured_iterations(
             store,
             query,
             reference,
             config.timeout_ms,
             config.measurement_iterations
           ) do
      {:ok,
       %{
         id: query.id,
         correctness: :match,
         result_count: reference.row_count,
         answer_fingerprint: reference.fingerprint,
         warmup_iterations: config.warmup_iterations,
         measurement_iterations: config.measurement_iterations,
         timings_us: timings,
         min_us: Enum.min(timings),
         max_us: Enum.max(timings),
         mean_us: Enum.sum(timings) / length(timings),
         p50_us: percentile(timings, 50),
         p95_us: percentile(timings, 95),
         p99_us: percentile(timings, 99)
       }}
    end
  end

  defp run_checked_iterations(_store, _query, _reference, _timeout_ms, 0), do: :ok

  defp run_checked_iterations(store, query, reference, timeout_ms, count) do
    Enum.reduce_while(1..count, :ok, fn _iteration, :ok ->
      case checked_iteration(store, query, reference, timeout_ms) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp measured_iterations(store, query, reference, timeout_ms, count) do
    Enum.reduce_while(1..count, {:ok, []}, fn _iteration, {:ok, timings} ->
      case measured_iteration(store, query, reference, timeout_ms) do
        {:ok, elapsed_us} -> {:cont, {:ok, [elapsed_us | timings]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, timings} -> {:ok, Enum.reverse(timings)}
      error -> error
    end
  end

  defp checked_iteration(store, query, reference, timeout_ms) do
    case execute_and_normalize(store, query, timeout_ms) do
      {:ok, actual} ->
        case compare_answer(query.id, reference, actual) do
          :ok -> :ok
          {:error, mismatch} -> {:error, {:correctness_mismatch, mismatch}}
        end

      {:error, reason} ->
        {:error, {:warmup_failed, query.id, reason}}
    end
  end

  defp measured_iteration(store, query, reference, timeout_ms) do
    {elapsed_us, result} =
      :timer.tc(fn -> execute_and_normalize(store, query, timeout_ms) end)

    case result do
      {:ok, actual} ->
        case compare_answer(query.id, reference, actual) do
          :ok -> {:ok, elapsed_us}
          {:error, mismatch} -> {:error, {:correctness_mismatch, mismatch}}
        end

      {:error, reason} ->
        {:error, {:measurement_failed, query.id, reason}}
    end
  end

  defp execute_and_normalize(store, query, timeout_ms) do
    case TripleStore.query(store, query.sparql, timeout: timeout_ms) do
      {:ok, result} -> AnswerNormalizer.normalize(result, ordering: :unordered)
      {:error, _reason} = error -> error
    end
  end

  defp compare_answer(_query_id, %{fingerprint: fingerprint}, %{fingerprint: fingerprint}),
    do: :ok

  defp compare_answer(query_id, expected, actual) do
    {:error,
     %{
       query_id: query_id,
       expected_fingerprint: expected.fingerprint,
       actual_fingerprint: actual.fingerprint,
       expected_row_count: expected.row_count,
       actual_row_count: actual.row_count
     }}
  end

  defp build_result(
         config,
         dataset,
         workload,
         reference,
         load,
         queries,
         started_at,
         completed_at
       ) do
    %{
      schema_version: 1,
      benchmark: "watdiv",
      status: :passed,
      started_at: DateTime.to_iso8601(started_at),
      completed_at: DateTime.to_iso8601(completed_at),
      dataset: Map.drop(dataset, [:dataset_path, :manifest_path]),
      workload: %{query_count: workload.query_count, fingerprint: workload.fingerprint},
      reference: %{
        engine: reference.engine,
        engine_version: reference.engine_version,
        dataset_sha256: reference.dataset_sha256,
        workload_sha256: reference.workload_sha256
      },
      runtime: %{
        warmup_iterations: config.warmup_iterations,
        measurement_iterations: config.measurement_iterations,
        timeout_ms: config.timeout_ms,
        batch_size: config.batch_size,
        triple_store_version: Application.spec(:triple_store, :vsn) |> to_string(),
        source_revision: source_revision(),
        working_tree_dirty: working_tree_dirty?(),
        elixir: System.version(),
        otp_release: System.otp_release(),
        system_architecture: :erlang.system_info(:system_architecture) |> to_string(),
        schedulers_online: System.schedulers_online()
      },
      load: load,
      queries: queries,
      artifact_dir: config.output_dir
    }
  end

  defp write_artifacts(result, output_dir) do
    staging_dir =
      output_dir <> ".partial.#{System.unique_integer([:positive, :monotonic])}"

    write_result =
      with :ok <- File.mkdir_p(Path.dirname(output_dir)),
           :ok <- File.mkdir(staging_dir),
           :ok <-
             File.write(
               Path.join(staging_dir, "results.json"),
               Jason.encode!(result, pretty: true)
             ),
           :ok <- File.write(Path.join(staging_dir, "results.csv"), csv(result.queries)) do
        File.rename(staging_dir, output_dir)
      end

    if write_result != :ok do
      File.rm_rf(staging_dir)
    end

    write_result
  end

  defp csv(queries) do
    rows =
      Enum.map(queries, fn query ->
        [
          query.id,
          query.result_count,
          query.measurement_iterations,
          query.min_us,
          query.p50_us,
          query.p95_us,
          query.p99_us,
          query.max_us,
          query.mean_us,
          query.answer_fingerprint
        ]
        |> Enum.map_join(",", &csv_cell/1)
      end)

    ([
       "query_id,result_count,iterations,min_us,p50_us,p95_us,p99_us,max_us,mean_us,answer_fingerprint"
     ] ++ rows)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp csv_cell(value) do
    encoded = to_string(value)

    if String.contains?(encoded, [",", "\"", "\n"]) do
      "\"" <> String.replace(encoded, "\"", "\"\"") <> "\""
    else
      encoded
    end
  end

  defp percentile(values, percentile) do
    sorted = Enum.sort(values)
    index = ceil(percentile / 100 * length(sorted)) - 1
    Enum.at(sorted, max(index, 0))
  end

  defp source_revision do
    with git when is_binary(git) <- System.find_executable("git"),
         {revision, 0} <- System.cmd(git, ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      String.trim(revision)
    else
      _ -> nil
    end
  end

  defp working_tree_dirty? do
    with git when is_binary(git) <- System.find_executable("git"),
         {status, 0} <-
           System.cmd(git, ["status", "--porcelain", "--untracked-files=normal"],
             stderr_to_stdout: true
           ) do
      String.trim(status) != ""
    else
      _ -> nil
    end
  end

  defp verify_loaded_count(expected, expected), do: :ok

  defp verify_loaded_count(expected, actual),
    do: {:error, {:loaded_count_mismatch, expected, actual}}

  defp ensure_fresh_output(path) do
    if File.exists?(path), do: {:error, {:output_path_exists, path}}, else: :ok
  end

  defp ensure_fresh_store(path) do
    if File.exists?(path), do: {:error, {:store_path_exists, path}}, else: :ok
  end

  defp required_path(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, Path.expand(value)}
      _ -> {:error, {:missing_option, key}}
    end
  end

  defp positive_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, {:invalid_option, key}}
    end
  end

  defp non_negative_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:invalid_option, key}}
    end
  end
end
