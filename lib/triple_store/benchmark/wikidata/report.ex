defmodule TripleStore.Benchmark.Wikidata.Report do
  @moduledoc """
  Artifact generation for Wikidata benchmark summaries.

  Reports are emitted as:

  - JSON for machine consumption and diffing
  - CSV for spreadsheet and notebook workflows
  - Markdown for checked-in summaries and review
  """
  @type artifact_paths :: %{
          json: String.t(),
          csv: String.t(),
          markdown: String.t()
        }

  @type artifact_bundle :: %{
          report: map(),
          output_dir: String.t(),
          paths: artifact_paths()
        }

  @type report_document :: map()

  @doc """
  Encodes a summary report as pretty JSON.
  """
  @spec to_json(report_document()) :: String.t()
  def to_json(report) when is_map(report) do
    report
    |> normalize_for_serialization()
    |> Jason.encode!(pretty: true)
  end

  @doc """
  Renders query summaries as CSV.
  """
  @spec to_csv(report_document()) :: String.t()
  def to_csv(%{query_summaries: query_summaries}) do
    header =
      [
        "benchmark_id",
        "query_name",
        "suite",
        "category",
        "group",
        "shape",
        "execution_variant",
        "measurement_iterations",
        "success_count",
        "failure_count",
        "completion_rate",
        "timeout_count",
        "parser_error_count",
        "execution_error_count",
        "cancellation_count",
        "out_of_memory_count",
        "raw_min_us",
        "raw_q1_us",
        "raw_median_us",
        "raw_q3_us",
        "raw_max_us",
        "raw_mean_us",
        "adjusted_min_us",
        "adjusted_q1_us",
        "adjusted_median_us",
        "adjusted_q3_us",
        "adjusted_max_us",
        "adjusted_mean_us",
        "raw_queries_per_sec",
        "adjusted_iterations_per_sec",
        "answer_fingerprint",
        "reference_fingerprint",
        "divergence_classification",
        "accepted_divergence",
        "partial_failure_class",
        "divergence_status"
      ]
      |> Enum.join(",")

    rows =
      Enum.map_join(query_summaries, "\n", fn query_summary ->
        [
          query_summary.benchmark_id,
          escape_csv(query_summary.query_name),
          query_summary.suite,
          query_summary.category,
          query_summary.group || "",
          query_summary.shape,
          query_summary.execution_variant,
          query_summary.measurement_iterations,
          query_summary.success_count,
          query_summary.failure_count,
          format_float(query_summary.completion_rate, 4),
          query_summary.error_totals.timeout,
          query_summary.error_totals.parse_error,
          query_summary.error_totals.execution_error,
          query_summary.error_totals.cancellation,
          query_summary.error_totals.out_of_memory,
          query_summary.raw_timing_summary.min_us,
          format_float(query_summary.raw_timing_summary.q1_us, 2),
          format_float(query_summary.raw_timing_summary.median_us, 2),
          format_float(query_summary.raw_timing_summary.q3_us, 2),
          query_summary.raw_timing_summary.max_us,
          format_float(query_summary.raw_timing_summary.mean_us, 2),
          query_summary.adjusted_timing_summary.min_us,
          format_float(query_summary.adjusted_timing_summary.q1_us, 2),
          format_float(query_summary.adjusted_timing_summary.median_us, 2),
          format_float(query_summary.adjusted_timing_summary.q3_us, 2),
          query_summary.adjusted_timing_summary.max_us,
          format_float(query_summary.adjusted_timing_summary.mean_us, 2),
          format_float(query_summary.throughput.raw_queries_per_sec, 2),
          format_float(query_summary.throughput.adjusted_iterations_per_sec, 2),
          query_summary.answer_fingerprint || "",
          query_summary.reference_fingerprint || "",
          query_summary.divergence_classification || "",
          query_summary.accepted_divergence,
          query_summary.partial_failure_class,
          query_summary.divergence_status
        ]
        |> Enum.map(&to_string/1)
        |> Enum.join(",")
      end)

    if rows == "", do: header, else: header <> "\n" <> rows
  end

  @doc """
  Renders a Markdown summary aligned with the repository's benchmark docs.
  """
  @spec to_markdown(report_document()) :: String.t()
  def to_markdown(report) when is_map(report) do
    dataset_rows =
      report.dataset_manifest
      |> key_value_rows([
        :dataset_id,
        :tier,
        :dump_version,
        :triple_count,
        :checksum,
        :source_url
      ])

    runtime_rows =
      report.runtime_config
      |> key_value_rows([
        :dataset_tier,
        :warmup_iterations,
        :measurement_iterations,
        :timeout_ms,
        :penalty_us,
        :long_running_threshold_us
      ])

    hardware_rows =
      report.runtime_metadata
      |> key_value_rows([
        :hostname,
        :elixir_version,
        :otp_release,
        :system_architecture,
        :logical_processors,
        :logical_processors_available,
        :schedulers,
        :schedulers_online,
        :dirty_cpu_schedulers,
        :dirty_io_schedulers
      ])

    """
    # Wikidata Benchmark Report

    - Report ID: `#{report.report_id}`
    - Report Version: `#{report.report_version}`
    - Generated At: `#{format_datetime(report.generated_at)}`
    - Run Kind: `#{report.run.run_kind}`
    - Git SHA: `#{report.run.git_sha || "unknown"}`

    ## Overall Summary

    #{summary_table([report.overall_summary])}

    ## Dataset Provenance

    #{markdown_table(["Field", "Value"], dataset_rows)}

    ## Runtime Configuration

    #{markdown_table(["Field", "Value"], runtime_rows)}

    ## Hardware Metadata

    #{markdown_table(["Field", "Value"], hardware_rows)}

    ## Suite Summaries

    #{summary_table(report.suite_summaries)}

    ## Query Shape Aggregates

    #{summary_table(report.aggregates.by_query_shape)}

    ## Query Summaries

    #{query_summary_table(report.query_summaries)}
    """
    |> String.trim()
  end

  @doc """
  Writes JSON, CSV, and Markdown artifacts to a versioned output directory.
  """
  @spec write(report_document(), Path.t(), keyword()) ::
          {:ok, artifact_bundle()} | {:error, term()}
  def write(report, output_root, opts \\ []) when is_map(report) and is_binary(output_root) do
    output_root = Path.expand(output_root)

    with {:ok, output_dir, report_version} <-
           resolve_output_dir(output_root, report.report_id, opts),
         :ok <- File.mkdir_p(output_dir) do
      paths = %{
        json: Path.join(output_dir, "summary.json"),
        csv: Path.join(output_dir, "query_summaries.csv"),
        markdown: Path.join(output_dir, "summary.md")
      }

      report =
        report
        |> Map.put(:report_version, report_version)
        |> Map.put(:artifacts, %{
          output_dir: output_dir,
          formats: [:json, :csv, :markdown],
          paths: paths
        })

      with :ok <- File.write(paths.json, to_json(report)),
           :ok <- File.write(paths.csv, to_csv(report)),
           :ok <- File.write(paths.markdown, to_markdown(report)) do
        {:ok, %{report: report, output_dir: output_dir, paths: paths}}
      end
    end
  end

  defp resolve_output_dir(output_root, report_id, opts) do
    preferred_version = Keyword.get(opts, :report_version, 1)
    version_existing = Keyword.get(opts, :version_existing, true)
    base_dir = Path.join(output_root, report_id)

    cond do
      preferred_version > 1 ->
        {:ok, versioned_dir(base_dir, preferred_version), preferred_version}

      not File.exists?(base_dir) ->
        {:ok, base_dir, 1}

      version_existing ->
        version =
          Stream.iterate(2, &(&1 + 1))
          |> Enum.find(fn candidate ->
            not File.exists?(versioned_dir(base_dir, candidate))
          end)

        {:ok, versioned_dir(base_dir, version), version}

      true ->
        {:error, :report_directory_exists}
    end
  end

  defp versioned_dir(base_dir, 1), do: base_dir
  defp versioned_dir(base_dir, version), do: "#{base_dir}-v#{version}"

  defp summary_table(summaries) do
    headers = [
      "Group",
      "Queries",
      "Completion",
      "Raw Mean",
      "Adjusted Mean",
      "Raw QPS",
      "Adjusted Iter/s",
      "Accepted",
      "Divergences",
      "Timeouts",
      "Errors"
    ]

    rows =
      Enum.map(summaries, fn summary ->
        total_errors =
          summary.error_totals.parse_error + summary.error_totals.execution_error +
            summary.error_totals.timeout + summary.error_totals.cancellation +
            summary.error_totals.out_of_memory

        [
          summary.group_key,
          summary.query_count,
          format_percentage(summary.completion_rate),
          format_duration(summary.raw_timing_summary.mean_us),
          format_duration(summary.adjusted_timing_summary.mean_us),
          format_float(summary.throughput.raw_queries_per_sec, 2),
          format_float(summary.throughput.adjusted_iterations_per_sec, 2),
          summary.accepted_divergence_count,
          summary.divergence_count,
          summary.error_totals.timeout,
          total_errors
        ]
      end)

    markdown_table(headers, rows)
  end

  defp query_summary_table(query_summaries) do
    headers = [
      "Benchmark ID",
      "Suite",
      "Variant",
      "Shape",
      "Completion",
      "Raw Median",
      "Adjusted Mean",
      "Correctness",
      "Classification",
      "Raw QPS",
      "Failures"
    ]

    rows =
      Enum.map(query_summaries, fn query_summary ->
        [
          query_summary.benchmark_id,
          query_summary.suite,
          query_summary.execution_variant,
          query_summary.shape,
          format_percentage(query_summary.completion_rate),
          format_duration(query_summary.raw_timing_summary.median_us),
          format_duration(query_summary.adjusted_timing_summary.mean_us),
          query_summary.divergence_status,
          query_summary.divergence_classification || "n/a",
          format_float(query_summary.throughput.raw_queries_per_sec, 2),
          query_summary.failure_count
        ]
      end)

    markdown_table(headers, rows)
  end

  defp key_value_rows(nil, _keys), do: [["Status", "Unavailable"]]

  defp key_value_rows(map, keys) do
    Enum.map(keys, fn key ->
      [Atom.to_string(key), render_value(Map.get(map, key))]
    end)
  end

  defp markdown_table(headers, rows) do
    header_line = "| " <> Enum.map_join(headers, " | ", &escape_markdown/1) <> " |"
    separator_line = "| " <> Enum.map_join(headers, " | ", fn _ -> "---" end) <> " |"

    row_lines =
      rows
      |> Enum.map(fn row ->
        "| " <> Enum.map_join(row, " | ", &escape_markdown/1) <> " |"
      end)
      |> Enum.join("\n")

    [header_line, separator_line, row_lines]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  defp normalize_for_serialization(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_for_serialization(%Date{} = date), do: Date.to_iso8601(date)

  defp normalize_for_serialization(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {key, normalize_for_serialization(value)} end)
    |> Enum.into(%{})
  end

  defp normalize_for_serialization(list) when is_list(list) do
    Enum.map(list, &normalize_for_serialization/1)
  end

  defp normalize_for_serialization(value), do: value

  defp format_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp format_datetime(value), do: to_string(value)

  defp format_percentage(value), do: "#{format_float(value * 100, 2)}%"

  defp format_duration(value) when is_integer(value), do: format_duration(value * 1.0)

  defp format_duration(value) when is_float(value) do
    cond do
      value < 1_000 -> "#{format_float(value, 2)}us"
      value < 1_000_000 -> "#{format_float(value / 1_000, 2)}ms"
      true -> "#{format_float(value / 1_000_000, 2)}s"
    end
  end

  defp format_duration(_value), do: "0us"

  defp format_float(value, decimals) when is_integer(value),
    do: :erlang.float_to_binary(value * 1.0, decimals: decimals)

  defp format_float(value, decimals) when is_float(value),
    do: :erlang.float_to_binary(value, decimals: decimals)

  defp format_float(_value, decimals), do: format_float(0.0, decimals)

  defp render_value(nil), do: "n/a"
  defp render_value(value) when is_list(value), do: Enum.map_join(value, ", ", &render_value/1)
  defp render_value(value) when is_atom(value), do: Atom.to_string(value)
  defp render_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp render_value(%Date{} = value), do: Date.to_iso8601(value)
  defp render_value(value), do: to_string(value)

  defp escape_csv(str) when is_binary(str) do
    if String.contains?(str, [",", "\"", "\n"]) do
      "\"#{String.replace(str, "\"", "\"\"")}\""
    else
      str
    end
  end

  defp escape_csv(value), do: value |> to_string() |> escape_csv()

  defp escape_markdown(value) do
    value
    |> render_value()
    |> String.replace("|", "\\|")
  end
end
