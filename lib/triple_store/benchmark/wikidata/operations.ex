defmodule TripleStore.Benchmark.Wikidata.Operations do
  @moduledoc """
  High-level operations for parser validation, smoke execution, and benchmark
  runs across Wikidata benchmark tiers.
  """

  alias TripleStore.Benchmark.Wikidata.{
    AcceptedDivergence,
    Baseline,
    Correctness,
    Metrics,
    PublicWorkloads,
    Query,
    Report,
    Runner,
    Scholia,
    SmokeFixture,
    StoreFixture
  }

  alias TripleStore.SPARQL.Parser

  @type mode :: :parser | :corpus_smoke | :smoke | :medium | :full

  @doc """
  Runs a benchmark workflow mode and returns a structured operation report.
  """
  @spec run(mode(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(:parser, opts), do: parser_validation(opts)
  def run(:corpus_smoke, opts), do: corpus_smoke(opts)
  def run(:smoke, opts), do: benchmark_tier(:smoke, opts)
  def run(:medium, opts), do: benchmark_tier(:medium, opts)
  def run(:full, opts), do: benchmark_tier(:full_dump, opts)
  def run(_mode, _opts), do: {:error, :unknown_mode}

  @doc """
  Returns the default smoke benchmark query ids grouped by suite.
  """
  @spec smoke_query_ids() :: %{atom() => [String.t()]}
  def smoke_query_ids do
    {:ok, corpora} = smoke_corpora()

    %{
      wgpb: [hd(corpora.wgpb.queries) |> Query.benchmark_id()],
      wdbench: [hd(corpora.wdbench.queries) |> Query.benchmark_id()],
      wdqs: [hd(corpora.wdqs.queries) |> Query.benchmark_id()],
      scholia: [hd(corpora.scholia.queries) |> Query.benchmark_id()]
    }
  end

  defp parser_validation(opts) do
    tier = Keyword.get(opts, :dataset_tier, :smoke)
    {:ok, corpora} = corpora_for_tier(tier)

    queries =
      corpora
      |> Map.values()
      |> Enum.flat_map(& &1.queries)

    parsed =
      Enum.map(queries, fn query ->
        %{
          benchmark_id: Query.benchmark_id(query),
          suite: query.manifest.suite,
          status: if(match?({:ok, _}, Parser.parse(query.sparql)), do: :ok, else: :error)
        }
      end)

    failures = Enum.filter(parsed, &(&1.status == :error))

    if failures == [] do
      {:ok,
       %{
         mode: :parser,
         dataset_tier: tier,
         query_count: length(queries),
         suite_count: map_size(corpora),
         failures: []
       }}
    else
      {:error, %{mode: :parser, dataset_tier: tier, failures: failures}}
    end
  end

  defp corpus_smoke(opts) do
    fixture_root = fixture_root(opts)

    with {:ok, dataset_manifest} <- SmokeFixture.register(fixture_root),
         {:ok, corpora} <- smoke_corpora(),
         {:ok, fixture_state} <-
           StoreFixture.setup(fixture_root, dataset_manifest,
             store_id: "wikidata-corpus-smoke",
             load_preset: :truthy_only,
             warmup: fn store -> TripleStore.stats(store) end
           ) do
      try do
        results =
          corpora
          |> Map.values()
          |> Enum.map(&hd(&1.queries))
          |> Enum.map(fn query ->
            case TripleStore.query(fixture_state.store, query.sparql) do
              {:ok, rows} ->
                %{
                  benchmark_id: Query.benchmark_id(query),
                  suite: query.manifest.suite,
                  result_count: length(rows)
                }

              {:error, reason} ->
                %{
                  benchmark_id: Query.benchmark_id(query),
                  suite: query.manifest.suite,
                  error: inspect(reason)
                }
            end
          end)

        errors = Enum.filter(results, &Map.has_key?(&1, :error))

        if errors == [] do
          {:ok,
           %{
             mode: :corpus_smoke,
             dataset_tier: :smoke,
             result_count: length(results),
             results: results
           }}
        else
          {:error, %{mode: :corpus_smoke, dataset_tier: :smoke, failures: errors}}
        end
      after
        :ok = StoreFixture.teardown(fixture_state, delete_store: true)
      end
    end
  end

  defp benchmark_tier(tier, opts) do
    fixture_root = fixture_root(opts)
    output_root = output_root(opts)
    output_dir = Path.join(output_root, Atom.to_string(tier))
    suites = Keyword.get(opts, :suites, [:wgpb, :wdbench, :wdqs, :scholia])

    measurement_iterations =
      Keyword.get(opts, :measurement_iterations, default_measurement_iterations(tier))

    warmup_iterations = Keyword.get(opts, :warmup_iterations, default_warmup_iterations(tier))
    query_ids = Keyword.get(opts, :query_ids, default_query_ids(tier))

    with {:ok, dataset_manifest} <- prepare_dataset(tier, fixture_root, opts),
         {:ok, target} <- benchmark_target(suites, tier),
         {:ok, fixture_state} <-
           StoreFixture.setup(fixture_root, dataset_manifest,
             store_id: "wikidata-benchmark-#{tier}",
             load_preset: :truthy_only,
             warmup: fn store -> TripleStore.stats(store) end
           ) do
      try do
        with {:ok, answer_baseline} <- load_answer_baseline(tier, opts),
             {:ok, accepted_divergences} <- load_accepted_divergences(tier, opts),
             {:ok, run_result} <-
               Runner.run(fixture_state, target,
                 dataset_tier: tier,
                 query_ids: query_ids,
                 warmup_iterations: warmup_iterations,
                 measurement_iterations: measurement_iterations,
                 timeout_ms: Keyword.get(opts, :timeout_ms, default_timeout_ms(tier))
               ),
             {:ok, correctness_result} <-
               Correctness.attach(run_result,
                 baseline: answer_baseline,
                 accepted_divergences: accepted_divergences
               ),
             {:ok, report} <-
               Metrics.summarize(correctness_result,
                 report_id: report_id(tier, Keyword.get(opts, :report_id))
               ),
             {:ok, bundle} <- Report.write(report, output_dir),
             :ok <- validate_thresholds(bundle.report, opts),
             :ok <- persist_optional_baselines(correctness_result, bundle, opts) do
          {:ok,
           %{
             mode: tier,
             dataset_tier: tier,
             dataset_manifest: dataset_manifest,
             report: bundle.report,
             artifact_bundle: bundle
           }}
        end
      after
        :ok = StoreFixture.teardown(fixture_state, delete_store: true)
      end
    end
  end

  defp prepare_dataset(:smoke, fixture_root, opts) do
    case Keyword.get(opts, :source_path) do
      nil -> SmokeFixture.register(fixture_root)
      _source_path -> {:error, :source_path_not_supported_for_smoke}
    end
  end

  defp prepare_dataset(tier, fixture_root, opts) do
    source_path = Keyword.get(opts, :source_path)

    if is_binary(source_path) do
      dataset_id = Keyword.get(opts, :dataset_id, "wikidata-#{tier}-external")
      source_url = Keyword.get(opts, :source_url, "file://#{source_path}")
      dump_version = Keyword.get(opts, :dump_version, "2024-10")

      with {:ok, manifest} <-
             TripleStore.Benchmark.Wikidata.DatasetManifest.from_source(
               source_path,
               dataset_id: dataset_id,
               tier: tier,
               source_url: source_url,
               dump_version: dump_version,
               source_date: Keyword.get(opts, :source_date, ~D[2024-10-01]),
               normalization_flags: [:truthy_only]
             ) do
        TripleStore.Benchmark.Wikidata.Fixture.register_dataset(
          fixture_root,
          manifest,
          source_path
        )
      end
    else
      {:error, :source_path_required}
    end
  end

  defp smoke_corpora do
    with {:ok, public} <- corpora_for_tier(:smoke) do
      {:ok, public}
    end
  end

  defp corpora_for_tier(tier) do
    public = PublicWorkloads.all_corpora(tier: tier)

    with {:ok, scholia} <- Scholia.corpus(tier, variants: [:raw]) do
      {:ok, Map.put(public, :scholia, scholia)}
    end
  end

  defp benchmark_target(suites, tier) do
    with {:ok, corpora} <- corpora_for_tier(tier) do
      {:ok,
       Enum.map(suites, fn
         :scholia -> corpora.scholia
         suite -> Map.fetch!(corpora, suite)
       end)}
    end
  end

  defp load_answer_baseline(:smoke, opts) do
    path = Keyword.get(opts, :answer_baseline, SmokeFixture.answer_baseline_path())

    if File.exists?(path) do
      Baseline.load_json(path)
    else
      {:ok, nil}
    end
  end

  defp load_answer_baseline(_tier, opts) do
    case Keyword.get(opts, :answer_baseline) do
      nil -> {:ok, nil}
      path -> Baseline.load_json(path)
    end
  end

  defp load_accepted_divergences(:smoke, opts) do
    path =
      Keyword.get(opts, :accepted_divergences, SmokeFixture.accepted_divergences_path())

    if File.exists?(path) do
      AcceptedDivergence.load_json(path)
    else
      {:ok, []}
    end
  end

  defp load_accepted_divergences(_tier, opts) do
    case Keyword.get(opts, :accepted_divergences) do
      nil -> {:ok, []}
      path -> AcceptedDivergence.load_json(path)
    end
  end

  defp validate_thresholds(report, opts) do
    failures =
      runtime_threshold_failures(report, opts) ++
        error_threshold_failures(report, opts) ++
        divergence_threshold_failures(report, opts)

    if failures == [] do
      :ok
    else
      {:error, {:threshold_failed, failures}}
    end
  end

  defp runtime_threshold_failures(report, opts) do
    case Keyword.get(opts, :max_adjusted_p95_us) do
      nil ->
        []

      threshold ->
        report.query_summaries
        |> Enum.filter(&(&1.adjusted_timing_summary.p95_us > threshold))
        |> Enum.map(fn query_summary ->
          "runtime regression for #{query_summary.suite}/#{query_summary.benchmark_id}: adjusted p95=#{query_summary.adjusted_timing_summary.p95_us}us exceeds #{threshold}us"
        end)
    end
  end

  defp error_threshold_failures(report, opts) do
    case Keyword.get(opts, :max_failure_rate) do
      nil ->
        []

      threshold ->
        report.query_summaries
        |> Enum.filter(fn query_summary ->
          failure_rate =
            query_summary.failure_count / max(query_summary.measurement_iterations, 1)

          failure_rate > threshold
        end)
        |> Enum.map(fn query_summary ->
          failure_rate =
            query_summary.failure_count / max(query_summary.measurement_iterations, 1)

          "error-rate regression for #{query_summary.suite}/#{query_summary.benchmark_id}: failure_rate=#{Float.round(failure_rate, 4)} exceeds #{threshold}"
        end)
    end
  end

  defp divergence_threshold_failures(report, opts) do
    case Keyword.get(opts, :max_divergence_rate) do
      nil ->
        []

      threshold ->
        report.query_summaries
        |> Enum.filter(fn query_summary ->
          divergence_rate =
            query_summary.divergence_count / max(query_summary.measurement_iterations, 1)

          divergence_rate > threshold and
            query_summary.divergence_status in [:divergent, :not_comparable]
        end)
        |> Enum.map(fn query_summary ->
          divergence_rate =
            query_summary.divergence_count / max(query_summary.measurement_iterations, 1)

          "divergence regression for #{query_summary.suite}/#{query_summary.benchmark_id}: divergence_rate=#{Float.round(divergence_rate, 4)} exceeds #{threshold}"
        end)
    end
  end

  defp fixture_root(opts) do
    Keyword.get(opts, :fixture_root, Path.expand("tmp/wikidata_fixture_root", File.cwd!()))
  end

  defp output_root(opts) do
    Keyword.get(opts, :output_root, Path.expand("tmp/wikidata_benchmark_runs", File.cwd!()))
  end

  defp default_measurement_iterations(:smoke), do: 2
  defp default_measurement_iterations(:medium), do: 5
  defp default_measurement_iterations(:full_dump), do: 10

  defp default_warmup_iterations(:smoke), do: 0
  defp default_warmup_iterations(:medium), do: 1
  defp default_warmup_iterations(:full_dump), do: 2

  defp default_timeout_ms(:smoke), do: 500
  defp default_timeout_ms(:medium), do: 1_000
  defp default_timeout_ms(:full_dump), do: 10_000

  defp default_query_ids(:smoke) do
    smoke_query_ids()
    |> Map.values()
    |> List.flatten()
  end

  defp default_query_ids(_tier), do: []

  defp report_id(tier, nil), do: "wikidata-#{tier}"
  defp report_id(_tier, report_id), do: report_id

  defp persist_optional_baselines(run_result, bundle, opts) do
    with :ok <- maybe_write_answer_baseline(run_result, Keyword.get(opts, :write_answer_baseline)),
         :ok <- maybe_write_accepted_report(bundle, Keyword.get(opts, :write_accepted_report)) do
      :ok
    end
  end

  defp maybe_write_answer_baseline(_run_result, nil), do: :ok

  defp maybe_write_answer_baseline(run_result, path) do
    with {:ok, baseline} <- Baseline.from_run_result(run_result),
         :ok <- File.mkdir_p(Path.dirname(path)) do
      Baseline.write_json(path, baseline)
    end
  end

  defp maybe_write_accepted_report(_bundle, nil), do: :ok

  defp maybe_write_accepted_report(bundle, dir) do
    dir = Path.expand(dir)
    portable_report = sanitize_accepted_report(bundle.report)

    with :ok <- File.mkdir_p(dir),
         :ok <- File.write(Path.join(dir, "summary.json"), Report.to_json(portable_report)),
         :ok <- File.write(Path.join(dir, "query_summaries.csv"), Report.to_csv(portable_report)),
         :ok <- File.write(Path.join(dir, "summary.md"), Report.to_markdown(portable_report)) do
      :ok
    end
  end

  defp sanitize_accepted_report(report) when is_map(report) do
    report
    |> Map.update(:dataset_manifest, nil, &sanitize_dataset_manifest/1)
    |> Map.put(:artifacts, %{
      output_dir: nil,
      formats: [:json, :csv, :markdown],
      paths: %{
        json: "summary.json",
        csv: "query_summaries.csv",
        markdown: "summary.md"
      }
    })
  end

  defp sanitize_dataset_manifest(nil), do: nil

  defp sanitize_dataset_manifest(dataset_manifest) when is_map(dataset_manifest) do
    dataset_manifest
    |> Map.put(:local_data_path, nil)
    |> Map.put(:manifest_path, nil)
  end
end
