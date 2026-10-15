defmodule TripleStore.Benchmark.LDBC.Artifacts do
  @moduledoc """
  Writes versioned, checksummed evidence for an LDBC benchmark run.

  Diagnostic reports always include raw failures and correctness. The official
  score field appears only when every declared protocol gate passes.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.StateIdentity

  @schema_version 1
  @bi_schema_version 2
  @required_gates [:profile, :correctness, :scheduling, :duration, :completeness]
  @json_files [
    :manifest,
    :environment,
    :catalog,
    :raw_samples,
    :errors,
    :correctness,
    :disclosure,
    :resources,
    :summary
  ]

  @doc "Writes the complete artifact set into a new or existing directory."
  @spec write(Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def write(directory, run) when is_binary(directory) and is_map(run) do
    schema_version = schema_version(run)

    with :ok <- validate_run(run),
         :ok <- File.mkdir_p(directory),
         enriched <- enrich(run, schema_version),
         {:ok, json_paths} <- write_json_files(directory, enriched, schema_version),
         {:ok, csv_path} <- write_csv(directory, enriched.raw_samples),
         {:ok, markdown_path} <- write_markdown(directory, enriched, schema_version) do
      paths = Map.merge(json_paths, %{samples_csv: csv_path, report_markdown: markdown_path})
      {:ok, %{schema_version: schema_version, paths: paths, checksums: checksums(paths)}}
    end
  end

  defp enrich(run, schema_version) do
    gates = Map.get(run, :gates, %{})
    eligible? = Enum.all?(@required_gates, &(Map.get(gates, &1) == true))
    supplied_summary = Map.get(run, :summary, %{})

    summary =
      supplied_summary
      |> Map.put(:schema_version, schema_version)
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
    |> Map.put_new(:disclosure, %{})
    |> Map.put_new(:resources, %{})
    |> maybe_put_protocol()
  end

  defp maybe_put_score(summary, true, score) when not is_nil(score),
    do: Map.put(summary, :official_score, score)

  defp maybe_put_score(summary, _eligible, _score), do: Map.delete(summary, :official_score)

  defp write_json_files(directory, run, schema_version) do
    Enum.reduce_while(json_files(run), {:ok, %{}}, fn name, {:ok, paths} ->
      path = Path.join(directory, "#{name}.json")
      payload = Map.get(run, name) |> versioned(schema_version) |> json_safe()

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

  defp write_markdown(directory, run, schema_version) do
    path = Path.join(directory, "report.md")
    summary = run.summary

    body =
      """
      # LDBC Benchmark Run

      Artifact schema: `#{schema_version}`

      - Measured operations: #{Map.get(summary, :measured_count, 0)}
      - Valid samples: #{Map.get(summary, :valid_sample_count, 0)}
      - Invalid samples: #{Map.get(summary, :invalid_sample_count, 0)}
      - Correctness records: #{length(run.correctness)}
      - Errors: #{length(run.errors)}
      - Official score eligible: #{summary.official_score_eligible}

      Raw evidence is stored in the adjacent JSON and CSV files.
      """ <> state_identity_markdown(run)

    case File.write(path, body, [:binary]) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, {:artifact_write_failed, :report_markdown, reason}}
    end
  end

  defp versioned(value, schema_version) when is_map(value),
    do: Map.put_new(value, :schema_version, schema_version)

  defp versioned(value, schema_version), do: %{schema_version: schema_version, records: value}

  defp validate_run(run) do
    if bi_run?(run), do: validate_bi_run(run), else: :ok
  end

  defp validate_bi_run(run) do
    blocks = Map.get(run, :protocol_blocks)
    final = if is_list(blocks) and blocks != [], do: List.last(blocks)[:state_identity]

    with :ok <- validate_blocks(blocks),
         :ok <- validate_correctness(Map.get(run, :correctness), blocks),
         :ok <-
           validate_named_identity(
             run |> Map.get(:manifest, %{}) |> Map.get(:state_identity),
             final,
             :manifest
           ) do
      validate_named_identity(
        run |> Map.get(:disclosure, %{}) |> Map.get(:state_identity),
        final,
        :disclosure
      )
    end
  end

  defp validate_blocks(blocks) when is_list(blocks) and blocks != [] do
    positions = Enum.map(blocks, &Map.get(&1, :position))

    if positions == Enum.uniq(positions) do
      validate_each_block(blocks)
    else
      {:error, {:invalid_state_identity, :duplicate_protocol_position}}
    end
  end

  defp validate_blocks(_blocks), do: {:error, {:invalid_state_identity, :protocol_blocks}}

  defp validate_each_block(blocks) do
    Enum.reduce_while(blocks, :ok, fn block, :ok ->
      case validate_block(block) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_block(block) do
    position = Map.get(block, :position)

    case Map.fetch(block, :state_identity) do
      {:ok, %StateIdentity{} = identity} ->
        valid? =
          StateIdentity.validate(identity) == :ok and
            Map.get(block, :kind) in [:validation, :power, :throughput] and
            identity.last_applied_batch == position

        if valid?,
          do: :ok,
          else: {:error, {:invalid_state_identity, {:protocol_block, position}}}

      _other ->
        {:error, {:invalid_state_identity, {:protocol_block, position, :missing}}}
    end
  end

  defp validate_correctness(records, blocks) when is_list(records) do
    with :ok <- validate_correctness_positions(records, blocks) do
      compare_correctness_identities(records, blocks)
    end
  end

  defp validate_correctness(_records, _blocks),
    do: {:error, {:invalid_state_identity, :correctness}}

  defp validate_correctness_positions(records, blocks) do
    expected = blocks |> Enum.map(& &1.position) |> MapSet.new()
    positions = Enum.map(records, &Map.get(&1, :position))
    actual = MapSet.new(positions)

    details = %{
      duplicates:
        positions
        |> Enum.frequencies()
        |> Enum.filter(fn {_position, count} -> count > 1 end)
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort(),
      missing: expected |> MapSet.difference(actual) |> Enum.sort(),
      unexpected: actual |> MapSet.difference(expected) |> Enum.sort()
    }

    if details == %{duplicates: [], missing: [], unexpected: []},
      do: :ok,
      else: {:error, {:invalid_state_identity, {:correctness_positions, details}}}
  end

  defp compare_correctness_identities(records, blocks) do
    by_position = Map.new(records, &{Map.fetch!(&1, :position), &1})

    Enum.reduce_while(blocks, :ok, fn block, :ok ->
      case validate_correctness_record(block, by_position) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_correctness_record(block, by_position) do
    position = block.position

    case get_in(by_position, [position, :state_identity]) do
      %StateIdentity{} = identity ->
        if StateIdentity.compare(block.state_identity, identity) == :ok,
          do: :ok,
          else: {:error, {:inconsistent_state_identity, {:correctness, position}}}

      _other ->
        {:error, {:invalid_state_identity, {:correctness, position, :missing}}}
    end
  end

  defp validate_named_identity(%StateIdentity{} = actual, expected, location) do
    case StateIdentity.compare(expected, actual) do
      :ok -> :ok
      {:error, _reason} -> {:error, {:inconsistent_state_identity, location}}
    end
  end

  defp validate_named_identity(_actual, _expected, location),
    do: {:error, {:invalid_state_identity, {location, :missing}}}

  defp schema_version(run), do: if(bi_run?(run), do: @bi_schema_version, else: @schema_version)

  defp json_files(run), do: if(bi_run?(run), do: @json_files ++ [:protocol], else: @json_files)

  defp maybe_put_protocol(run) do
    if bi_run?(run),
      do: Map.put(run, :protocol, %{blocks: Map.fetch!(run, :protocol_blocks)}),
      else: run
  end

  defp state_identity_markdown(%{protocol_blocks: blocks} = run) do
    if bi_run?(run), do: state_identity_table(blocks), else: ""
  end

  defp state_identity_markdown(_run), do: ""

  defp state_identity_table(blocks) do
    rows =
      Enum.map_join(blocks, "\n", fn block ->
        identity = block.state_identity

        "| #{block.position} | #{block.kind} | `#{identity.dataset_checksum}` | " <>
          "#{identity.mapping_version} | #{identity.checkpoint_name} | " <>
          "#{identity.last_applied_batch} | `#{identity.update_stream_checksum}` | " <>
          "`#{identity.state_checksum}` |"
      end)

    """

    ## State identities

    | Block | Kind | Dataset | Mapping | Checkpoint | Last applied batch | Update stream | State |
    | ---: | --- | --- | --- | --- | ---: | --- | --- |
    #{rows}
    """
  end

  defp bi_run?(run) do
    suite = Map.get(run, :suite) || run |> Map.get(:manifest, %{}) |> Map.get(:suite)
    suite in [:snb_bi, "snb_bi"]
  end

  defp json_safe(value) when is_struct(value), do: value |> Map.from_struct() |> json_safe()

  defp json_safe(value) when is_map(value),
    do: Map.new(value, fn {key, item} -> {to_string(key), json_safe(item)} end)

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()
  defp json_safe(value) when is_boolean(value) or is_nil(value), do: value
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
