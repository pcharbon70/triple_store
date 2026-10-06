defmodule TripleStore.Benchmark.LDBC.Artifacts do
  @moduledoc """
  Writes versioned, checksummed evidence for an LDBC benchmark run.

  Diagnostic reports always include raw failures and correctness. The official
  score field appears only when every declared protocol gate passes.
  """

  @schema_version 1
  @required_gates [:profile, :correctness, :scheduling, :duration, :completeness]
  @json_files [
    :manifest,
    :environment,
    :catalog,
    :raw_samples,
    :errors,
    :correctness,
    :resources,
    :summary
  ]

  @doc "Writes the complete artifact set into a new or existing directory."
  @spec write(Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def write(directory, run) when is_binary(directory) and is_map(run) do
    with :ok <- File.mkdir_p(directory),
         enriched <- enrich(run),
         {:ok, json_paths} <- write_json_files(directory, enriched),
         {:ok, csv_path} <- write_csv(directory, enriched.raw_samples),
         {:ok, markdown_path} <- write_markdown(directory, enriched) do
      paths = Map.merge(json_paths, %{samples_csv: csv_path, report_markdown: markdown_path})
      {:ok, %{schema_version: @schema_version, paths: paths, checksums: checksums(paths)}}
    end
  end

  defp enrich(run) do
    gates = Map.get(run, :gates, %{})
    eligible? = Enum.all?(@required_gates, &(Map.get(gates, &1) == true))
    supplied_summary = Map.get(run, :summary, %{})

    summary =
      supplied_summary
      |> Map.put(:schema_version, @schema_version)
      |> Map.put(:official_score_eligible, eligible?)
      |> maybe_put_score(eligible?, Map.get(run, :official_score))

    manifest =
      Map.put_new(
        Map.get(run, :manifest, %{}),
        :input_checksums,
        Map.get(run, :input_checksums, %{})
      )

    run
    |> Map.put(:summary, summary)
    |> Map.put(:manifest, manifest)
    |> Map.put_new(:environment, %{})
    |> Map.put_new(:catalog, [])
    |> Map.put_new(:raw_samples, [])
    |> Map.put_new(:errors, [])
    |> Map.put_new(:correctness, [])
    |> Map.put_new(:resources, %{})
  end

  defp maybe_put_score(summary, true, score) when not is_nil(score),
    do: Map.put(summary, :official_score, score)

  defp maybe_put_score(summary, _eligible, _score), do: Map.delete(summary, :official_score)

  defp write_json_files(directory, run) do
    Enum.reduce_while(@json_files, {:ok, %{}}, fn name, {:ok, paths} ->
      path = Path.join(directory, "#{name}.json")
      payload = Map.get(run, name) |> versioned() |> json_safe()

      case File.write(path, Jason.encode!(payload, pretty: true) <> "\n", [:binary]) do
        :ok -> {:cont, {:ok, Map.put(paths, name, path)}}
        {:error, reason} -> {:halt, {:error, {:artifact_write_failed, name, reason}}}
      end
    end)
  end

  defp write_csv(directory, samples) do
    path = Path.join(directory, "samples.csv")
    header = "operation_id,mode,status,total_us,sample_us,score_eligible\n"

    rows =
      Enum.map_join(samples, fn sample ->
        [
          sample[:operation_id],
          sample[:mode],
          sample[:status],
          sample[:total_us],
          sample[:sample_us],
          sample[:score_eligible?]
        ]
        |> Enum.map_join(",", &csv_escape/1)
        |> Kernel.<>("\n")
      end)

    case File.write(path, header <> rows, [:binary]) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, {:artifact_write_failed, :samples_csv, reason}}
    end
  end

  defp write_markdown(directory, run) do
    path = Path.join(directory, "report.md")
    summary = run.summary

    body = """
    # LDBC Benchmark Run

    Artifact schema: `#{@schema_version}`

    - Measured operations: #{Map.get(summary, :measured_count, 0)}
    - Valid samples: #{Map.get(summary, :valid_sample_count, 0)}
    - Invalid samples: #{Map.get(summary, :invalid_sample_count, 0)}
    - Correctness records: #{length(run.correctness)}
    - Errors: #{length(run.errors)}
    - Official score eligible: #{summary.official_score_eligible}

    Raw evidence is stored in the adjacent JSON and CSV files.
    """

    case File.write(path, body, [:binary]) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, {:artifact_write_failed, :report_markdown, reason}}
    end
  end

  defp versioned(value) when is_map(value),
    do: Map.put_new(value, :schema_version, @schema_version)

  defp versioned(value), do: %{schema_version: @schema_version, records: value}

  defp json_safe(value) when is_struct(value), do: value |> Map.from_struct() |> json_safe()

  defp json_safe(value) when is_map(value),
    do: Map.new(value, fn {key, item} -> {to_string(key), json_safe(item)} end)

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()
  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value), do: value

  defp csv_escape(nil), do: ""

  defp csv_escape(value) do
    value = to_string(value)

    if String.contains?(value, [",", "\"", "\n"]),
      do: "\"#{String.replace(value, "\"", "\"\"")}\"",
      else: value
  end

  defp checksums(paths) do
    Map.new(paths, fn {name, path} ->
      {:ok, contents} = File.read(path)
      {name, :crypto.hash(:sha256, contents) |> Base.encode16(case: :lower)}
    end)
  end
end
