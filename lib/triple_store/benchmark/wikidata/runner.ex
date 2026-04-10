defmodule TripleStore.Benchmark.Wikidata.Runner do
  @moduledoc """
  Runtime benchmark harness for Wikidata-style query corpora.

  The runner executes single queries, full suites, or multi-suite matrices with
  a consistent lifecycle:

  1. resolve the benchmark target into normalized queries
  2. apply per-tier defaults for warmup, measurement, and timeouts
  3. execute warmup runs without polluting measured timings
  4. capture structured iteration results, failures, and provenance
  5. return a stable run-result schema for later reporting phases
  """

  alias TripleStore.Benchmark.Wikidata.{
    Contract,
    Corpus,
    DatasetManifest,
    PublicWorkloads,
    Query,
    Scholia,
    StoreFixture
  }

  @tier_defaults %{
    smoke: %{
      warmup_iterations: 1,
      measurement_iterations: 2,
      timeout_ms: 250,
      penalty_us: 500_000,
      long_running_threshold_us: 250_000
    },
    medium: %{
      warmup_iterations: 2,
      measurement_iterations: 5,
      timeout_ms: 500,
      penalty_us: 1_000_000,
      long_running_threshold_us: 500_000
    },
    large: %{
      warmup_iterations: 3,
      measurement_iterations: 10,
      timeout_ms: 2_000,
      penalty_us: 4_000_000,
      long_running_threshold_us: 2_000_000
    },
    full_dump: %{
      warmup_iterations: 5,
      measurement_iterations: 10,
      timeout_ms: 10_000,
      penalty_us: 20_000_000,
      long_running_threshold_us: 10_000_000
    }
  }

  @type error_class :: :parse_error | :execution_error | :timeout | :cancelled | :out_of_memory
  @type partial_failure_class :: :none | :flaky_run | :hard_incompatibility | :resource_exhaustion
  @type run_kind :: :query | :suite | :matrix
  @type executor_fun :: (term(), String.t(), keyword() -> {:ok, term()} | {:error, term()})

  @type iteration_result :: %{
          iteration: pos_integer(),
          status: :ok | :error,
          elapsed_us: non_neg_integer(),
          adjusted_elapsed_us: non_neg_integer(),
          penalty_reason: :failure | :long_running | nil,
          result_count: non_neg_integer() | nil,
          memory_before_bytes: non_neg_integer(),
          memory_after_bytes: non_neg_integer(),
          peak_memory_bytes: non_neg_integer(),
          error_class: error_class() | nil,
          error_message: String.t() | nil,
          error_detail: String.t() | nil
        }

  @type query_run_result :: %{
          benchmark_id: String.t(),
          query_name: String.t(),
          suite: Contract.workload_family(),
          category: atom() | String.t(),
          group: atom() | String.t() | nil,
          shape: Query.shape() | nil,
          execution_variant: Contract.execution_variant(),
          source_id: String.t() | nil,
          feature_tags: [atom() | String.t()],
          answer_size_class: Query.answer_size_class(),
          raw_query_text: String.t(),
          normalized_query_text: String.t(),
          stress_points: [atom() | String.t()],
          measurement_iterations: pos_integer(),
          warmup_iterations: non_neg_integer(),
          timeout_ms: pos_integer(),
          raw_timings_us: [non_neg_integer()],
          adjusted_timings_us: [non_neg_integer()],
          iterations: [iteration_result()],
          completion_rate: float(),
          success_count: non_neg_integer(),
          timeout_count: non_neg_integer(),
          parser_error_count: non_neg_integer(),
          execution_error_count: non_neg_integer(),
          cancellation_count: non_neg_integer(),
          out_of_memory_count: non_neg_integer(),
          result_count: non_neg_integer() | nil,
          failure_count: non_neg_integer(),
          penalty_count: non_neg_integer(),
          partial_failure_class: partial_failure_class(),
          failures: [map()],
          template_metadata: map() | nil
        }

  @type run_result :: %{
          schema_version: pos_integer(),
          run_kind: run_kind(),
          started_at: DateTime.t(),
          completed_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          target: map(),
          dataset_manifest: map() | nil,
          runtime_config: map(),
          runtime_metadata: map(),
          git_sha: String.t() | nil,
          query_runs: [query_run_result()]
        }

  @type run_opts :: [
          dataset_tier: Contract.dataset_tier(),
          dataset_manifest: DatasetManifest.t(),
          query_ids: [String.t()],
          execution_variants: [Contract.execution_variant()],
          warmup_iterations: non_neg_integer(),
          measurement_iterations: pos_integer(),
          timeout_ms: pos_integer(),
          penalty_us: pos_integer(),
          long_running_threshold_us: pos_integer(),
          optimize: boolean(),
          executor: executor_fun()
        ]

  @doc """
  Returns the runner defaults for a dataset tier.
  """
  @spec defaults_for_tier(Contract.dataset_tier()) :: {:ok, map()} | {:error, :unknown_tier}
  def defaults_for_tier(tier) when is_atom(tier) do
    case Map.fetch(@tier_defaults, tier) do
      {:ok, defaults} -> {:ok, Map.put(defaults, :dataset_tier, tier)}
      :error -> {:error, :unknown_tier}
    end
  end

  @doc """
  Runs a benchmark target against a store or prepared store fixture.

  Supported targets:
  - a single `Query`
  - a `Corpus`
  - a suite atom (`:wgpb`, `:wdbench`, `:wdqs`, `:scholia`)
  - a list of suite atoms or corpora for matrix runs
  """
  @spec run(term(), Query.t() | Corpus.t() | Contract.workload_family() | [term()], run_opts()) ::
          {:ok, run_result()} | {:error, term()}
  def run(subject, target, opts \\ [])

  def run(subject, %Query{} = query, opts) do
    do_run(subject, :query, [query], %{query_ids: [Query.benchmark_id(query)]}, opts)
  end

  def run(subject, %Corpus{} = corpus, opts) when is_list(opts) do
    queries =
      corpus.queries
      |> filter_query_ids(Keyword.get(opts, :query_ids, []))
      |> expand_execution_variants(Keyword.get(opts, :execution_variants, [:raw]))

    do_run(
      subject,
      :suite,
      queries,
      %{
        suite: corpus.suite,
        corpus_name: corpus.name,
        query_ids: Enum.map(queries, &Query.benchmark_id/1)
      },
      opts
    )
  end

  def run(subject, suite, opts) when is_atom(suite) and is_list(opts) do
    with {:ok, corpus} <- resolve_suite(suite, opts) do
      run(subject, corpus, opts)
    end
  end

  def run(subject, suites_or_corpora, opts) when is_list(suites_or_corpora) and is_list(opts) do
    with {:ok, corpora} <- resolve_matrix_targets(suites_or_corpora, opts) do
      queries =
        corpora
        |> Enum.flat_map(& &1.queries)
        |> filter_query_ids(Keyword.get(opts, :query_ids, []))
        |> expand_execution_variants(Keyword.get(opts, :execution_variants, [:raw]))

      do_run(
        subject,
        :matrix,
        queries,
        %{
          suites: Enum.map(corpora, & &1.suite),
          corpus_names: Enum.map(corpora, & &1.name),
          query_ids: Enum.map(queries, &Query.benchmark_id/1)
        },
        opts
      )
    end
  end

  @doc """
  Convenience wrapper for single-query runs.
  """
  @spec run_query(term(), Query.t(), run_opts()) :: {:ok, run_result()} | {:error, term()}
  def run_query(subject, %Query{} = query, opts \\ []), do: run(subject, query, opts)

  @doc """
  Convenience wrapper for suite-level runs.
  """
  @spec run_suite(term(), Corpus.t() | Contract.workload_family(), run_opts()) ::
          {:ok, run_result()} | {:error, term()}
  def run_suite(subject, suite_or_corpus, opts \\ []), do: run(subject, suite_or_corpus, opts)

  @doc """
  Convenience wrapper for multi-suite matrix runs.
  """
  @spec run_matrix(term(), [term()], run_opts()) :: {:ok, run_result()} | {:error, term()}
  def run_matrix(subject, suites_or_corpora \\ Contract.workload_families(), opts \\ []),
    do: run(subject, suites_or_corpora, opts)

  @doc """
  Classifies a query failure into the runner's structured error taxonomy.
  """
  @spec classify_error(term()) :: error_class()
  def classify_error({:parse_error, _reason}), do: :parse_error
  def classify_error(%TripleStore.Error{category: :query_parse_error}), do: :parse_error
  def classify_error(%TripleStore.Error{category: :query_timeout}), do: :timeout
  def classify_error(%TripleStore.Error{category: :system_resource_exhausted}), do: :out_of_memory
  def classify_error(:timeout), do: :timeout
  def classify_error(:cancelled), do: :cancelled
  def classify_error(:enomem), do: :out_of_memory
  def classify_error({:exit, :cancelled}), do: :cancelled
  def classify_error({:exit, {:shutdown, :cancelled}}), do: :cancelled
  def classify_error({:exit, {:killed, _reason}}), do: :cancelled

  def classify_error(reason) do
    inspected = inspect(reason)

    cond do
      String.contains?(inspected, "out of memory") -> :out_of_memory
      String.contains?(inspected, "unsupported") -> :execution_error
      true -> :execution_error
    end
  end

  defp do_run(subject, run_kind, queries, target, opts) do
    with {:ok, context} <- resolve_context(subject, opts),
         {:ok, runtime_config} <- runtime_config(context.dataset_manifest, opts),
         {:ok, executor} <- executor(opts) do
      started_at = DateTime.utc_now()
      runtime_metadata = Contract.capture_runtime_metadata()

      query_runs =
        Enum.map(queries, &run_query_iterations(context.store, &1, runtime_config, executor))

      completed_at = DateTime.utc_now()

      {:ok,
       %{
         schema_version: 1,
         run_kind: run_kind,
         started_at: started_at,
         completed_at: completed_at,
         duration_ms: DateTime.diff(completed_at, started_at, :millisecond),
         target: Map.put(target, :query_count, length(queries)),
         dataset_manifest: dataset_manifest_map(context.dataset_manifest),
         runtime_config: runtime_config,
         runtime_metadata: runtime_metadata,
         git_sha: current_git_sha(),
         query_runs: query_runs
       }}
    end
  end

  defp resolve_context(%StoreFixture{store: store, dataset_manifest: dataset_manifest}, _opts)
       when not is_nil(store) do
    {:ok, %{store: store, dataset_manifest: dataset_manifest}}
  end

  defp resolve_context(%{db: _db, dict_manager: _dict_manager} = store, opts) do
    {:ok, %{store: store, dataset_manifest: Keyword.get(opts, :dataset_manifest)}}
  end

  defp resolve_context(_subject, _opts), do: {:error, :invalid_runner_subject}

  defp runtime_config(dataset_manifest, opts) do
    tier =
      Keyword.get_lazy(opts, :dataset_tier, fn ->
        case dataset_manifest do
          %DatasetManifest{tier: tier} -> tier
          _ -> :full_dump
        end
      end)

    with {:ok, defaults} <- defaults_for_tier(tier),
         :ok <- validate_execution_variants(Keyword.get(opts, :execution_variants, [:raw])),
         :ok <-
           validate_positive_integer(
             Keyword.get(opts, :measurement_iterations, defaults.measurement_iterations)
           ),
         :ok <-
           validate_non_negative_integer(
             Keyword.get(opts, :warmup_iterations, defaults.warmup_iterations)
           ),
         :ok <- validate_positive_integer(Keyword.get(opts, :timeout_ms, defaults.timeout_ms)),
         :ok <- validate_positive_integer(Keyword.get(opts, :penalty_us, defaults.penalty_us)),
         :ok <-
           validate_positive_integer(
             Keyword.get(opts, :long_running_threshold_us, defaults.long_running_threshold_us)
           ) do
      {:ok,
       %{
         dataset_tier: tier,
         warmup_iterations: Keyword.get(opts, :warmup_iterations, defaults.warmup_iterations),
         measurement_iterations:
           Keyword.get(opts, :measurement_iterations, defaults.measurement_iterations),
         timeout_ms: Keyword.get(opts, :timeout_ms, defaults.timeout_ms),
         penalty_us: Keyword.get(opts, :penalty_us, defaults.penalty_us),
         long_running_threshold_us:
           Keyword.get(opts, :long_running_threshold_us, defaults.long_running_threshold_us),
         execution_variants: Keyword.get(opts, :execution_variants, [:raw]),
         query_ids: Keyword.get(opts, :query_ids, []),
         optimize: Keyword.get(opts, :optimize, true)
       }}
    end
  end

  defp executor(opts) do
    case Keyword.get(opts, :executor, &TripleStore.query/3) do
      executor when is_function(executor, 3) -> {:ok, executor}
      _other -> {:error, :invalid_executor}
    end
  end

  defp run_query_iterations(store, %Query{} = query, runtime_config, executor) do
    run_warmup(store, query, runtime_config, executor)

    iterations =
      Enum.map(1..runtime_config.measurement_iterations, fn iteration ->
        execute_iteration(store, query, runtime_config, iteration, executor)
      end)

    raw_timings_us =
      iterations
      |> Enum.filter(&(&1.status == :ok))
      |> Enum.map(& &1.elapsed_us)

    adjusted_timings_us = Enum.map(iterations, & &1.adjusted_elapsed_us)
    success_count = Enum.count(iterations, &(&1.status == :ok))
    timeout_count = Enum.count(iterations, &(&1.error_class == :timeout))
    parser_error_count = Enum.count(iterations, &(&1.error_class == :parse_error))
    execution_error_count = Enum.count(iterations, &(&1.error_class == :execution_error))
    cancellation_count = Enum.count(iterations, &(&1.error_class == :cancelled))
    out_of_memory_count = Enum.count(iterations, &(&1.error_class == :out_of_memory))
    failure_count = runtime_config.measurement_iterations - success_count
    completion_rate = success_count / runtime_config.measurement_iterations

    %{
      benchmark_id: Query.benchmark_id(query),
      query_name: query.name,
      suite: query.manifest.suite,
      category: query.manifest.category,
      group: query.group,
      shape: query.shape,
      execution_variant: query.manifest.execution_variant,
      source_id: query.source_id,
      feature_tags: query.feature_tags,
      answer_size_class: query.answer_size_class,
      raw_query_text: query.raw_sparql,
      normalized_query_text: query.sparql,
      stress_points: query.stress_points,
      measurement_iterations: runtime_config.measurement_iterations,
      warmup_iterations: runtime_config.warmup_iterations,
      timeout_ms: runtime_config.timeout_ms,
      raw_timings_us: raw_timings_us,
      adjusted_timings_us: adjusted_timings_us,
      iterations: iterations,
      completion_rate: completion_rate,
      success_count: success_count,
      timeout_count: timeout_count,
      parser_error_count: parser_error_count,
      execution_error_count: execution_error_count,
      cancellation_count: cancellation_count,
      out_of_memory_count: out_of_memory_count,
      result_count: last_result_count(iterations),
      failure_count: failure_count,
      penalty_count: Enum.count(iterations, &(not is_nil(&1.penalty_reason))),
      partial_failure_class:
        classify_partial_failure(
          runtime_config.measurement_iterations,
          parser_error_count,
          execution_error_count,
          timeout_count,
          cancellation_count,
          out_of_memory_count
        ),
      failures:
        iterations
        |> Enum.filter(&(&1.status == :error))
        |> Enum.map(fn iteration ->
          %{
            iteration: iteration.iteration,
            class: iteration.error_class,
            message: iteration.error_message,
            detail: iteration.error_detail
          }
        end),
      template_metadata: query.template_metadata
    }
  end

  defp run_warmup(store, query, runtime_config, executor) do
    if runtime_config.warmup_iterations > 0 do
      Enum.each(1..runtime_config.warmup_iterations, fn iteration ->
        _ = execute_iteration(store, query, runtime_config, iteration, executor)
      end)
    end
  end

  defp execute_iteration(store, query, runtime_config, iteration, executor) do
    memory_before = current_memory_bytes()

    {elapsed_us, outcome} =
      :timer.tc(fn ->
        safely_execute_query(executor, store, query.sparql,
          timeout: runtime_config.timeout_ms,
          optimize: runtime_config.optimize
        )
      end)

    memory_after = current_memory_bytes()
    peak_memory_bytes = max(memory_before, memory_after)

    case outcome do
      {:ok, result} ->
        adjusted_elapsed_us =
          adjusted_elapsed_us(
            elapsed_us,
            nil,
            runtime_config.long_running_threshold_us,
            runtime_config.penalty_us
          )

        %{
          iteration: iteration,
          status: :ok,
          elapsed_us: elapsed_us,
          adjusted_elapsed_us: adjusted_elapsed_us,
          penalty_reason: if(adjusted_elapsed_us > elapsed_us, do: :long_running, else: nil),
          result_count: result_count(result),
          memory_before_bytes: memory_before,
          memory_after_bytes: memory_after,
          peak_memory_bytes: peak_memory_bytes,
          error_class: nil,
          error_message: nil,
          error_detail: nil
        }

      {:error, reason} ->
        error_class = classify_error(reason)

        %{
          iteration: iteration,
          status: :error,
          elapsed_us: elapsed_us,
          adjusted_elapsed_us:
            adjusted_elapsed_us(
              elapsed_us,
              error_class,
              runtime_config.long_running_threshold_us,
              runtime_config.penalty_us
            ),
          penalty_reason: :failure,
          result_count: nil,
          memory_before_bytes: memory_before,
          memory_after_bytes: memory_after,
          peak_memory_bytes: peak_memory_bytes,
          error_class: error_class,
          error_message: format_error(reason),
          error_detail: inspect(reason)
        }
    end
  end

  defp safely_execute_query(executor, store, sparql, query_opts) do
    executor.(store, sparql, query_opts)
  rescue
    error ->
      {:error, error}
  catch
    :exit, reason ->
      {:error, {:exit, reason}}
  end

  defp resolve_suite(:scholia, opts) do
    corpus_opts =
      opts
      |> Keyword.take([:dataset_tier, :limit_policy])
      |> Keyword.put_new(:tier, Keyword.get(opts, :dataset_tier, :full_dump))
      |> Keyword.put_new(:variants, [:raw])

    Scholia.corpus(Keyword.fetch!(corpus_opts, :tier), corpus_opts)
  end

  defp resolve_suite(suite, opts) when suite in [:wgpb, :wdbench, :wdqs] do
    corpus_opts =
      opts
      |> Keyword.take([:dataset_tier, :limit_policy])
      |> Keyword.put_new(:tier, Keyword.get(opts, :dataset_tier, :full_dump))

    PublicWorkloads.corpus(suite, corpus_opts)
  end

  defp resolve_suite(_suite, _opts), do: {:error, :unknown_suite}

  defp resolve_matrix_targets(suites_or_corpora, opts) do
    suites_or_corpora
    |> Enum.reduce_while({:ok, []}, fn target, {:ok, corpora} ->
      case resolve_matrix_target(target, opts) do
        {:ok, corpus} -> {:cont, {:ok, corpora ++ [corpus]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp resolve_matrix_target(%Corpus{} = corpus, _opts), do: {:ok, corpus}
  defp resolve_matrix_target(suite, opts) when is_atom(suite), do: resolve_suite(suite, opts)
  defp resolve_matrix_target(_target, _opts), do: {:error, :invalid_matrix_target}

  defp filter_query_ids(queries, []), do: queries

  defp filter_query_ids(queries, query_ids),
    do: Enum.filter(queries, &(Query.benchmark_id(&1) in query_ids))

  defp expand_execution_variants(queries, execution_variants) do
    Enum.flat_map(queries, fn query ->
      if query.manifest.execution_variant == :raw do
        Enum.map(execution_variants, fn variant ->
          if variant == :raw do
            query
          else
            {:ok, variant_query} = Query.with_variant(query, variant)
            variant_query
          end
        end)
      else
        [query]
      end
    end)
  end

  defp adjusted_elapsed_us(elapsed_us, nil, long_running_threshold_us, penalty_us) do
    if elapsed_us > long_running_threshold_us do
      max(elapsed_us, penalty_us)
    else
      elapsed_us
    end
  end

  defp adjusted_elapsed_us(elapsed_us, _error_class, _threshold_us, penalty_us),
    do: max(elapsed_us, penalty_us)

  defp classify_partial_failure(
         measurement_iterations,
         parser_error_count,
         execution_error_count,
         timeout_count,
         cancellation_count,
         out_of_memory_count
       ) do
    failure_count =
      parser_error_count + execution_error_count + timeout_count + cancellation_count +
        out_of_memory_count

    cond do
      failure_count == 0 ->
        :none

      out_of_memory_count > 0 or cancellation_count > 0 ->
        :resource_exhaustion

      parser_error_count + execution_error_count == measurement_iterations ->
        :hard_incompatibility

      failure_count < measurement_iterations ->
        :flaky_run

      true ->
        :hard_incompatibility
    end
  end

  defp result_count(results) when is_list(results), do: length(results)
  defp result_count(true), do: 1
  defp result_count(false), do: 0
  defp result_count(nil), do: 0
  defp result_count(_other), do: 1

  defp last_result_count(iterations) do
    iterations
    |> Enum.reverse()
    |> Enum.find_value(fn iteration ->
      if iteration.status == :ok, do: iteration.result_count
    end)
  end

  defp current_memory_bytes do
    :erlang.memory(:total)
  end

  defp current_git_sha do
    case System.find_executable("git") do
      nil ->
        nil

      git ->
        case System.cmd(git, ["rev-parse", "--short=12", "HEAD"], stderr_to_stdout: true) do
          {sha, 0} -> String.trim(sha)
          _ -> nil
        end
    end
  end

  defp dataset_manifest_map(%DatasetManifest{} = dataset_manifest),
    do: DatasetManifest.to_map(dataset_manifest)

  defp dataset_manifest_map(_dataset_manifest), do: nil

  defp format_error(%TripleStore.Error{} = error), do: error.message
  defp format_error(%{message: message}) when is_binary(message), do: message
  defp format_error({:parse_error, reason}), do: "Parse error: #{inspect(reason)}"
  defp format_error({:exit, reason}), do: "Exited: #{inspect(reason)}"
  defp format_error(reason), do: inspect(reason)

  defp validate_execution_variants(variants) when is_list(variants) do
    if Enum.all?(variants, &Contract.execution_variant?/1) do
      :ok
    else
      {:error, :invalid_execution_variants}
    end
  end

  defp validate_execution_variants(_variants), do: {:error, :invalid_execution_variants}

  defp validate_non_negative_integer(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_non_negative_integer(_value), do: {:error, :invalid_integer_option}

  defp validate_positive_integer(value) when is_integer(value) and value > 0, do: :ok
  defp validate_positive_integer(_value), do: {:error, :invalid_integer_option}
end
