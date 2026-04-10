defmodule Mix.Tasks.Benchmark.Wikidata do
  use Mix.Task

  @moduledoc """
  Run Wikidata benchmark workflows.

  ## Modes

      mix benchmark.wikidata parser
      mix benchmark.wikidata corpus-smoke
      mix benchmark.wikidata smoke
      mix benchmark.wikidata medium --source /path/to/data.nt --source-url https://example.org/data.nt
      mix benchmark.wikidata full --source /path/to/data.nt --source-url https://example.org/data.nt
  """

  alias TripleStore.Benchmark.Wikidata.Operations

  @shortdoc "Run Wikidata benchmark workflows"

  @switches [
    source: :string,
    source_url: :string,
    dump_version: :string,
    dataset_id: :string,
    fixture_root: :string,
    output_root: :string,
    answer_baseline: :string,
    accepted_divergences: :string,
    write_answer_baseline: :string,
    write_accepted_report: :string,
    max_adjusted_p95_us: :integer,
    max_failure_rate: :float,
    max_divergence_rate: :float,
    timeout_ms: :integer,
    warmup_iterations: :integer,
    measurement_iterations: :integer,
    report_id: :string
  ]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      invalid_switches = Enum.map_join(invalid, ", ", fn {switch, _value} -> "--#{switch}" end)
      Mix.raise("unknown options for mix benchmark.wikidata: #{invalid_switches}")
    end

    mode =
      case argv do
        [mode] ->
          parse_mode(mode)

        _ ->
          Mix.raise(
            "usage: mix benchmark.wikidata <parser|corpus-smoke|smoke|medium|full> [options]"
          )
      end

    opts =
      opts
      |> Keyword.put_new_lazy(:fixture_root, fn ->
        Path.expand("tmp/wikidata_fixture_root", File.cwd!())
      end)
      |> Keyword.put_new_lazy(:output_root, fn ->
        Path.expand("tmp/wikidata_benchmark_runs", File.cwd!())
      end)
      |> normalize_option_keys()

    case Operations.run(mode, opts) do
      {:ok, result} ->
        print_success(mode, result)

      {:error, {:threshold_failed, failures}} ->
        Enum.each(failures, fn failure -> Mix.shell().error(failure) end)
        Mix.raise("wikidata benchmark thresholds failed")

      {:error, %{failures: failures}} when is_list(failures) ->
        Enum.each(failures, &Mix.shell().error(inspect(&1)))
        Mix.raise("wikidata benchmark workflow failed")

      {:error, reason} ->
        Mix.raise("wikidata benchmark workflow failed: #{inspect(reason)}")
    end
  end

  defp parse_mode("parser"), do: :parser
  defp parse_mode("corpus-smoke"), do: :corpus_smoke
  defp parse_mode("smoke"), do: :smoke
  defp parse_mode("medium"), do: :medium
  defp parse_mode("full"), do: :full
  defp parse_mode(other), do: Mix.raise("unknown benchmark mode: #{other}")

  defp normalize_option_keys(opts) do
    opts
    |> rename_opt(:source, :source_path)
  end

  defp rename_opt(opts, source_key, target_key) do
    case Keyword.pop(opts, source_key) do
      {nil, opts} -> opts
      {value, opts} -> Keyword.put(opts, target_key, value)
    end
  end

  defp print_success(:parser, result) do
    Mix.shell().info("Parser validation passed")
    Mix.shell().info("Tier: #{result.dataset_tier}")
    Mix.shell().info("Queries validated: #{result.query_count}")
  end

  defp print_success(:corpus_smoke, result) do
    Mix.shell().info("Corpus smoke execution passed")
    Mix.shell().info("Queries executed: #{result.result_count}")
  end

  defp print_success(mode, result) when mode in [:smoke, :medium, :full] do
    Mix.shell().info("Benchmark run passed")
    Mix.shell().info("Tier: #{result.dataset_tier}")
    Mix.shell().info("Report ID: #{result.report.report_id}")
    Mix.shell().info("Artifacts: #{result.artifact_bundle.output_dir}")
  end
end
