defmodule Mix.Tasks.Benchmark.Ldbc.Baseline.Accept do
  @moduledoc """
  Accepts a reviewed LDBC correctness artifact as a versioned baseline.

      mix benchmark.ldbc.baseline.accept --input correctness.json \
        --output priv/benchmarks/ldbc/baselines/run.json \
        --source-version v1.0.3 --reason "Reviewed against reference output"
  """

  use Mix.Task

  alias TripleStore.Benchmark.LDBC.Baseline

  @shortdoc "Explicitly accepts a reviewed LDBC correctness baseline"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, invalid} =
      OptionParser.parse(args,
        strict: [input: :string, output: :string, source_version: :string, reason: :string]
      )

    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")

    input = Keyword.get(opts, :input) || Mix.raise("--input is required")
    output = Keyword.get(opts, :output) || Mix.raise("--output is required")

    case Baseline.accept(input, output, opts) do
      :ok -> Mix.shell().info("Accepted LDBC baseline at #{output}")
      {:error, reason} -> Mix.raise("baseline acceptance failed: #{inspect(reason)}")
    end
  end
end
