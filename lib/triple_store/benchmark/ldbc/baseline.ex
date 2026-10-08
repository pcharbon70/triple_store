defmodule TripleStore.Benchmark.LDBC.Baseline do
  @moduledoc """
  Explicit acceptance workflow for reviewed LDBC correctness baselines.

  Measurement code never calls this module. Acceptance copies a correct result
  artifact with its checksum, source version, reason, and acceptance timestamp.
  """

  @doc "Accepts a reviewed correctness artifact as a baseline."
  @spec accept(Path.t(), Path.t(), keyword()) :: :ok | {:error, term()}
  def accept(input, output, opts) do
    with {:ok, contents} <- File.read(input),
         {:ok, decoded} <- Jason.decode(contents),
         :ok <- require_correct(decoded),
         {:ok, reason} <- required_option(opts, :reason),
         {:ok, source_version} <- required_option(opts, :source_version) do
      baseline = %{
        schema_version: 1,
        accepted_at: DateTime.utc_now() |> DateTime.to_iso8601(),
        source_version: source_version,
        reason: reason,
        input_sha256: :crypto.hash(:sha256, contents) |> Base.encode16(case: :lower),
        correctness: decoded
      }

      File.mkdir_p!(Path.dirname(output))
      File.write(output, Jason.encode!(baseline, pretty: true) <> "\n", [:binary])
    end
  end

  defp require_correct(%{"records" => records}) when is_list(records) do
    if Enum.all?(records, &(Map.get(&1, "status") in ["correct", "accepted_divergence"])),
      do: :ok,
      else: {:error, :incorrect_results_cannot_be_accepted}
  end

  defp require_correct(_artifact), do: {:error, :invalid_correctness_artifact}

  defp required_option(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:required_option, key}}
    end
  end
end
