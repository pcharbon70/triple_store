defmodule Mix.Tasks.Benchmark.Watdiv do
  use Mix.Task

  @moduledoc """
  Prepare and run correctness-gated benchmarks over official WatDiv artifacts.

  ## Commands

      mix benchmark.watdiv dataset-manifest --dataset DATA.nt --model MODEL.txt \
        --scale 1 --output MANIFEST.json

      mix benchmark.watdiv split --input RAW.txt --output-dir WORKLOAD/basic \
        --template L1

      mix benchmark.watdiv reference-manifest --dataset DATA.nt \
        --dataset-manifest MANIFEST.json --workload-dir WORKLOAD \
        --reference-dir REFERENCES --engine "Apache Jena ARQ" \
        --engine-version VERSION

      mix benchmark.watdiv run --dataset DATA.nt \
        --dataset-manifest MANIFEST.json --workload-dir WORKLOAD \
        --reference-dir REFERENCES --output-dir RESULTS
  """

  alias TripleStore.Benchmark.WatDiv.{Provenance, ReferenceAnswers, Runner, Workload}

  @shortdoc "Run official WatDiv benchmark workflows"

  @switches [
    dataset: :string,
    dataset_manifest: :string,
    model: :string,
    scale: :integer,
    output: :string,
    input: :string,
    output_dir: :string,
    template: :string,
    workload_dir: :string,
    reference_dir: :string,
    engine: :string,
    engine_version: :string,
    store_path: :string,
    keep_store: :boolean,
    warmup: :integer,
    iterations: :integer,
    timeout_ms: :integer,
    batch_size: :integer
  ]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")
    {opts, argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      switches = Enum.map_join(invalid, ", ", fn {switch, _value} -> "--#{switch}" end)
      Mix.raise("unknown WatDiv options: #{switches}")
    end

    case argv do
      ["dataset-manifest"] -> write_dataset_manifest(opts)
      ["split"] -> split_workload(opts)
      ["reference-manifest"] -> write_reference_manifest(opts)
      ["run"] -> run_benchmark(opts)
      _ -> Mix.raise(usage())
    end
  end

  defp write_dataset_manifest(opts) do
    dataset = required!(opts, :dataset)
    model = required!(opts, :model)
    output = required!(opts, :output)
    scale = required!(opts, :scale)

    case Provenance.write_dataset_manifest(dataset, model, output, scale) do
      {:ok, manifest} ->
        Mix.shell().info("WatDiv dataset manifest written: #{Path.expand(output)}")
        Mix.shell().info("Dataset SHA-256: #{manifest.dataset_sha256}")
        Mix.shell().info("Triples: #{manifest.triple_count}")

      {:error, reason} ->
        Mix.raise("failed to write WatDiv dataset manifest: #{inspect(reason)}")
    end
  end

  defp split_workload(opts) do
    input = required!(opts, :input)
    output_dir = required!(opts, :output_dir)
    template = required!(opts, :template)

    case Workload.split_file(input, output_dir, template) do
      {:ok, paths} ->
        Mix.shell().info("WatDiv query instances written: #{length(paths)}")
        Mix.shell().info("Directory: #{Path.expand(output_dir)}")

      {:error, reason} ->
        Mix.raise("failed to split WatDiv workload: #{inspect(reason)}")
    end
  end

  defp write_reference_manifest(opts) do
    dataset = required!(opts, :dataset)
    dataset_manifest_path = required!(opts, :dataset_manifest)
    workload_dir = required!(opts, :workload_dir)
    reference_dir = required!(opts, :reference_dir)
    engine = required!(opts, :engine)
    engine_version = required!(opts, :engine_version)

    with {:ok, dataset_manifest} <-
           Provenance.load_dataset_manifest(dataset_manifest_path, dataset),
         {:ok, workload} <- Workload.load_dir(workload_dir),
         {:ok, manifest} <-
           ReferenceAnswers.write_manifest(
             reference_dir,
             workload,
             dataset_manifest,
             engine,
             engine_version
           ) do
      Mix.shell().info("WatDiv reference manifest written")
      Mix.shell().info("Engine: #{manifest.engine} #{manifest.engine_version}")
      Mix.shell().info("Workload SHA-256: #{manifest.workload_sha256}")
    else
      {:error, reason} ->
        Mix.raise("failed to write WatDiv reference manifest: #{inspect(reason)}")
    end
  end

  defp run_benchmark(opts) do
    runner_opts = [
      dataset_path: required!(opts, :dataset),
      dataset_manifest_path: required!(opts, :dataset_manifest),
      workload_dir: required!(opts, :workload_dir),
      reference_dir: required!(opts, :reference_dir),
      output_dir: required!(opts, :output_dir),
      warmup_iterations: Keyword.get(opts, :warmup, 2),
      measurement_iterations: Keyword.get(opts, :iterations, 5),
      timeout_ms: Keyword.get(opts, :timeout_ms, 30_000),
      batch_size: Keyword.get(opts, :batch_size, 10_000),
      keep_store: Keyword.get(opts, :keep_store, false)
    ]

    runner_opts =
      case Keyword.fetch(opts, :store_path) do
        {:ok, path} -> Keyword.put(runner_opts, :store_path, path)
        :error -> runner_opts
      end

    case Runner.run(runner_opts) do
      {:ok, result} ->
        Mix.shell().info("Official WatDiv benchmark passed")
        Mix.shell().info("Queries: #{result.workload.query_count}")

        Mix.shell().info(
          "Reference: #{result.reference.engine} #{result.reference.engine_version}"
        )

        Mix.shell().info("Artifacts: #{result.artifact_dir}")

      {:error, reason} ->
        Mix.raise("WatDiv benchmark failed: #{inspect(reason)}")
    end
  end

  defp required!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        Mix.raise("missing required option --#{key |> to_string() |> String.replace("_", "-")}")
    end
  end

  defp usage do
    "usage: mix benchmark.watdiv <dataset-manifest|split|reference-manifest|run> [options]"
  end
end
