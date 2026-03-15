defmodule Mix.Tasks.Conformance do
  use Mix.Task

  @moduledoc """
  Mix task entrypoint for repository-local specs governance and conformance
  validation.
  """

  alias TripleStore.Specs.Validator

  @shortdoc "Validate specs governance and conformance traceability"

  @switches [
    governance_only: :boolean,
    conformance_only: :boolean,
    spec_root: :string
  ]

  @impl Mix.Task
  def run(args) do
    {opts, _argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      invalid_switches = Enum.map_join(invalid, ", ", fn {switch, _value} -> "--#{switch}" end)
      Mix.raise("unknown options for mix conformance: #{invalid_switches}")
    end

    mode =
      case {opts[:governance_only], opts[:conformance_only]} do
        {true, true} -> Mix.raise("choose only one of --governance-only or --conformance-only")
        {true, _} -> :governance
        {_, true} -> :conformance
        _ -> :all
      end

    spec_root = Keyword.get(opts, :spec_root, Path.expand("specs", File.cwd!()))

    case Validator.validate(spec_root: spec_root, mode: mode) do
      {:ok, report} ->
        print_report(report, mode)

      {:error, report} ->
        print_report(report, mode)
        Mix.raise("specs validation failed")
    end
  end

  defp print_report(report, mode) do
    Mix.shell().info("Specs validation mode: #{mode}")

    stats = report.stats

    Mix.shell().info(
      "Counts: #{stats.requirements} REQ, #{stats.acceptances} AC, #{stats.scenarios} SCN, #{stats.adrs} ADR, #{stats.matrix_rows} matrix rows"
    )

    Enum.each(report.warnings, fn warning ->
      Mix.shell().info("warning: " <> warning)
    end)

    if report.errors == [] do
      Mix.shell().info("Specs validation passed")
    else
      Enum.each(report.errors, fn error ->
        Mix.shell().error("error: " <> error)
      end)
    end
  end
end
