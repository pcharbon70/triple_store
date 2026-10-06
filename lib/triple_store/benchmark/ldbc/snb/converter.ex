defmodule TripleStore.Benchmark.LDBC.SNB.Converter do
  @moduledoc """
  Bounded-row conversion of SNB BI and Interactive CSV layouts to RDF.

  Entity and relationship files are streamed. Referential identities are kept
  in a disk-backed DETS set, avoiding a full source graph in BEAM memory. Initial
  data, parameters, and ordered updates remain separate manifest components.
  """

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{DatasetManifest, Delimited, RDFStream}
  alias TripleStore.Benchmark.LDBC.SNB.{Mapping, UpdateStream}

  @index_table __MODULE__.ReferenceIndex
  @transformation_version "snb-csv-to-rdf-v1"

  @doc "Loads the checked-in BI and Interactive conversion profiles."
  @spec profiles() :: {:ok, map()} | {:error, term()}
  def profiles do
    path = priv_path("datasets/snb_profiles.exs")

    with {profiles, _binding} <- Code.eval_file(path),
         true <- is_map(profiles) do
      {:ok, profiles}
    else
      false -> {:error, :invalid_snb_profiles}
    end
  rescue
    error -> {:error, {:invalid_snb_profiles, Exception.message(error)}}
  end

  @doc "Converts one registered profile into N-Quads, parameters, updates, and a manifest."
  @spec convert(atom(), Path.t(), Path.t(), keyword()) ::
          {:ok, DatasetManifest.t()} | {:error, term()}
  def convert(profile_id, source_root, output_dir, opts \\ []) do
    with {:ok, profiles} <- profiles(),
         {:ok, profile} <- fetch_profile(profiles, profile_id),
         :ok <- File.mkdir_p(output_dir),
         :ok <- validate_input_files(profile, source_root) do
      do_convert(profile, source_root, output_dir, opts)
    end
  end

  defp do_convert(profile, source_root, output_dir, opts) do
    output_path = Path.join(output_dir, "#{profile.id}.nq")
    temporary = output_path <> ".partial"
    index_path = Path.join(output_dir, ".#{profile.id}-references.dets")

    with {:ok, io} <- File.open(temporary, [:write, :binary, :exclusive]) do
      case :dets.open_file(@index_table,
             file: String.to_charlist(index_path),
             type: :set
           ) do
        {:ok, @index_table} ->
          run_conversion(profile, source_root, output_dir, output_path, temporary, io, opts)

        {:error, reason} ->
          File.close(io)
          File.rm(temporary)
          {:error, {:reference_index_open_failed, reason}}
      end
    end
  end

  defp run_conversion(profile, source_root, output_dir, output_path, temporary, io, opts) do
    index_path = Path.join(output_dir, ".#{profile.id}-references.dets")

    result =
      try do
        with {:ok, entity_stats} <- convert_entities(profile, source_root, io),
             {:ok, relationship_stats} <- convert_relationships(profile, source_root, io),
             :ok <- File.close(io),
             :ok <- File.rename(temporary, output_path),
             {:ok, scan} <- RDFStream.scan(output_path, :nquads),
             true <-
               scan.statement_count ==
                 entity_stats.statement_count + relationship_stats.statement_count,
             {:ok, updates} <- convert_updates(profile, source_root, output_dir),
             {:ok, parameters} <-
               convert_parameters(profile, source_root, output_dir, scan.checksum),
             {:ok, source_checksum} <- source_checksum(profile, source_root),
             {:ok, manifest} <-
               build_manifest(
                 profile,
                 %{
                   output_path: output_path,
                   scan: scan,
                   entity_stats: entity_stats,
                   relationship_stats: relationship_stats,
                   updates: updates,
                   parameters: parameters,
                   source_checksum: source_checksum
                 },
                 opts
               ) do
          {:ok, manifest}
        else
          false -> {:error, :statement_count_mismatch}
          {:error, _} = error -> error
        end
      after
        File.close(io)
        :dets.close(@index_table)
        File.rm(index_path)
      end

    case result do
      {:ok, _manifest} = success ->
        success

      {:error, _} = error ->
        File.rm(temporary)
        File.rm(output_path)
        error
    end
  end

  defp convert_entities(profile, source_root, io) do
    Enum.reduce_while(
      profile.entities,
      {:ok, %{rows: 0, statement_count: 0}},
      &convert_entity_file(&1, &2, profile, source_root, io)
    )
  end

  defp convert_entity_file(spec, {:ok, stats}, profile, source_root, io) do
    path = Path.join(source_root, spec.path)

    path
    |> reduce_rows(profile.delimiter, &map_entity_row(&1, &2, profile, spec, path, io))
    |> accumulate_file_stats(stats)
  end

  defp map_entity_row(row, row_number, profile, spec, path, io) do
    with {:ok, id} <- fetch_row(row, spec.id_column, path, row_number),
         :ok <- :dets.insert(@index_table, {{spec.type, id}, true}),
         mapped_row <- Map.put(row, "id", id),
         {:ok, quads} <-
           Mapping.entity_quads(
             spec.type,
             mapped_row,
             Mapping.graph_iri(profile.suite, :initial)
           ),
         :ok <- write_quads(io, quads) do
      {:ok, length(quads)}
    end
  end

  defp convert_relationships(profile, source_root, io) do
    Enum.reduce_while(
      profile.relationships,
      {:ok, %{rows: 0, statement_count: 0}},
      &convert_relationship_file(&1, &2, profile, source_root, io)
    )
  end

  defp convert_relationship_file(spec, {:ok, stats}, profile, source_root, io) do
    path = Path.join(source_root, spec.path)

    path
    |> reduce_rows(profile.delimiter, &map_relationship_row(&1, &2, profile, spec, path, io))
    |> accumulate_file_stats(stats)
  end

  defp map_relationship_row(row, row_number, profile, spec, path, io) do
    with {:ok, from_id} <- fetch_row(row, spec.from_column, path, row_number),
         {:ok, to_id} <- fetch_row(row, spec.to_column, path, row_number),
         :ok <- reference_exists(spec.from_type, from_id),
         :ok <- reference_exists(spec.to_type, to_id),
         properties <- Map.take(row, spec.properties),
         {:ok, quads} <-
           Mapping.relationship_quads(
             spec.type,
             spec.from_type,
             from_id,
             spec.to_type,
             to_id,
             properties,
             Mapping.graph_iri(profile.suite, :initial)
           ),
         :ok <- write_quads(io, quads) do
      {:ok, length(quads)}
    end
  end

  defp accumulate_file_stats({:ok, rows, statements}, stats) do
    {:cont,
     {:ok,
      %{
        rows: stats.rows + rows,
        statement_count: stats.statement_count + statements
      }}}
  end

  defp accumulate_file_stats({:error, _} = error, _stats), do: {:halt, error}

  defp convert_updates(profile, source_root, output_dir) do
    Enum.reduce_while(profile.updates, {:ok, []}, fn spec, {:ok, components} ->
      source = Path.join(source_root, spec.path)
      destination = Path.join(output_dir, "#{profile.id}-#{Path.basename(spec.path)}.updates")

      records =
        row_stream(source, profile.delimiter)
        |> Stream.map(fn {row, row_number} ->
          update_record(profile, spec, row, source, row_number)
        end)

      destination
      |> UpdateStream.write(records)
      |> update_component(spec, destination, components)
    end)
  rescue
    error in File.Error -> {:error, {:file_error, error.reason}}
    error in ArgumentError -> {:error, {:invalid_source_row, Exception.message(error)}}
  end

  defp update_component({:ok, count}, spec, destination, components) do
    case Artifact.checksum(destination) do
      {:ok, checksum} ->
        component = %{role: spec.role, path: destination, checksum: checksum, count: count}
        {:cont, {:ok, components ++ [component]}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp update_component({:error, _} = error, _spec, _destination, _components),
    do: {:halt, error}

  defp update_record(profile, spec, row, source, row_number) do
    with {:ok, sequence_value} <- fetch_row(row, spec.sequence_column, source, row_number),
         {sequence, ""} <- Integer.parse(sequence_value),
         {:ok, operation_value} <- fetch_row(row, spec.operation_column, source, row_number),
         {:ok, operation} <- update_operation(operation_value),
         {:ok, id} <- fetch_row(row, spec.id_column, source, row_number),
         mapped_row <-
           row
           |> Map.drop([spec.sequence_column, spec.operation_column])
           |> Map.put("id", id),
         {:ok, quads} <-
           Mapping.entity_quads(
             spec.entity_type,
             mapped_row,
             Mapping.graph_iri(profile.suite, :updates)
           ) do
      %{sequence: sequence, operation: operation, quads: quads}
    else
      :error -> %{error: {:invalid_update_sequence, source, row_number}}
      {:error, reason} -> %{error: reason}
    end
  end

  defp convert_parameters(profile, source_root, output_dir, dataset_checksum) do
    Enum.reduce_while(profile.parameters, {:ok, []}, fn spec, {:ok, components} ->
      source = Path.join(source_root, spec.path)
      destination = Path.join(output_dir, "#{profile.id}-#{Path.basename(spec.path)}.parameters")

      rows = row_stream(source, profile.delimiter) |> Enum.map(&elem(&1, 0))

      document = %{
        dataset_checksum: dataset_checksum,
        scale_factor: profile.scale_factor,
        suite: profile.suite,
        source_path: spec.path,
        rows: rows
      }

      with :ok <-
             File.write(destination, :erlang.term_to_binary(document, [:deterministic]), [:binary]),
           {:ok, checksum} <- Artifact.checksum(destination) do
        component = %{
          role: :parameters,
          path: destination,
          checksum: checksum,
          count: length(rows)
        }

        {:cont, {:ok, components ++ [component]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  rescue
    error in File.Error -> {:error, {:file_error, error.reason}}
    error in ArgumentError -> {:error, {:invalid_source_row, Exception.message(error)}}
  end

  defp reduce_rows(path, delimiter, mapper) do
    path
    |> row_stream(delimiter)
    |> Enum.reduce_while({:ok, 0, 0}, fn {row, row_number}, {:ok, rows, statements} ->
      case mapper.(row, row_number) do
        {:ok, count} -> {:cont, {:ok, rows + 1, statements + count}}
        {:error, reason} -> {:halt, {:error, {:source_row_error, path, row_number, reason}}}
      end
    end)
  rescue
    error in File.Error -> {:error, {:file_error, error.reason}}
    error in ArgumentError -> {:error, {:invalid_source_row, Exception.message(error)}}
  end

  defp row_stream(path, delimiter) do
    path
    |> File.stream!(:line)
    |> Stream.with_index(1)
    |> Stream.transform(nil, fn
      {line, 1}, nil ->
        {:ok, header} = Delimited.parse_line(line, delimiter)
        {[], header}

      {line, line_number}, header ->
        case Delimited.parse_line(line, delimiter) do
          {:ok, values} when length(values) == length(header) ->
            {[{Map.new(Enum.zip(header, values)), line_number}], header}

          {:ok, values} ->
            raise ArgumentError,
                  "row width mismatch at #{path}:#{line_number}: #{length(values)} != #{length(header)}"

          {:error, reason} ->
            raise ArgumentError, "invalid row at #{path}:#{line_number}: #{inspect(reason)}"
        end
    end)
  end

  defp write_quads(io, quads) do
    dataset = RDF.Dataset.new(quads)

    with {:ok, encoded} <- RDF.NQuads.write_string(dataset) do
      encoded
      |> String.split("\n", trim: true)
      |> Enum.sort()
      |> Enum.each(&IO.binwrite(io, &1 <> "\n"))
    end
  end

  defp reference_exists(type, id) do
    case :dets.lookup(@index_table, {type, id}) do
      [{{^type, ^id}, true}] -> :ok
      [] -> {:error, {:missing_reference, type, id}}
    end
  end

  defp fetch_row(row, column, path, row_number) do
    case Map.fetch(row, column) do
      {:ok, value} when value != "" -> {:ok, value}
      _ -> {:error, {:missing_column_value, column, path, row_number}}
    end
  end

  defp update_operation("insert"), do: {:ok, :insert}
  defp update_operation("delete"), do: {:ok, :delete}
  defp update_operation(operation), do: {:error, {:unknown_update_operation, operation}}

  defp build_manifest(profile, data, opts) do
    DatasetManifest.new(%{
      dataset_id: profile.id,
      suite: profile.suite,
      profile: Atom.to_string(profile.profile),
      scale_factor: profile.scale_factor,
      source: %{
        generator_source_id: profile.generator_source_id,
        generator_pin: profile.generator_pin,
        generator_settings: profile.generator_settings,
        seed: profile.seed,
        format: :csv,
        checksum: data.source_checksum,
        license: profile.license
      },
      transformation: %{
        version: @transformation_version,
        mapping_version: Mapping.version(),
        output_checksum: data.scan.checksum,
        statement_count: data.scan.statement_count,
        entity_count: data.entity_stats.rows,
        relationship_count: data.relationship_stats.rows,
        update_streams: Enum.map(data.updates, & &1.role)
      },
      store: %{
        schema: :quad,
        loader_settings: %{
          batch_size: Keyword.get(opts, :batch_size, 10_000),
          parallel: Keyword.get(opts, :parallel, true)
        },
        path_identity: "#{profile.id}-#{Mapping.version()}-quad",
        post_load_stats: %{}
      },
      components:
        [
          %{
            role: :initial,
            path: data.output_path,
            checksum: data.scan.checksum,
            count: data.scan.statement_count
          }
        ] ++ data.updates ++ data.parameters
    })
  end

  defp source_checksum(profile, source_root) do
    paths =
      Enum.map(profile.entities, & &1.path) ++
        Enum.map(profile.relationships, & &1.path) ++
        Enum.map(profile.updates, & &1.path) ++ Enum.map(profile.parameters, & &1.path)

    with {:ok, checksums} <- checksum_paths(paths, source_root) do
      digest =
        checksums
        |> Enum.sort()
        |> Enum.map_join("\n", fn {path, checksum} -> "#{path}\u0000#{checksum}" end)
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, "sha256:#{digest}"}
    end
  end

  defp checksum_paths(paths, root) do
    Enum.reduce_while(paths, {:ok, []}, fn relative, {:ok, checksums} ->
      case Artifact.checksum(Path.join(root, relative)) do
        {:ok, checksum} -> {:cont, {:ok, [{relative, checksum} | checksums]}}
        {:error, reason} -> {:halt, {:error, {:source_checksum_failed, relative, reason}}}
      end
    end)
  end

  defp validate_input_files(profile, source_root) do
    paths =
      Enum.map(profile.entities, & &1.path) ++
        Enum.map(profile.relationships, & &1.path) ++
        Enum.map(profile.updates, & &1.path) ++ Enum.map(profile.parameters, & &1.path)

    case Enum.find(paths, &(not File.regular?(Path.join(source_root, &1)))) do
      nil -> :ok
      missing -> {:error, {:missing_source_component, missing}}
    end
  end

  defp fetch_profile(profiles, profile_id) do
    case Map.fetch(profiles, profile_id) do
      {:ok, profile} -> {:ok, profile}
      :error -> {:error, {:unknown_snb_profile, profile_id}}
    end
  end

  defp priv_path(relative) do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../../../priv/benchmarks/ldbc/#{relative}", __DIR__)

      priv_dir ->
        Path.join([to_string(priv_dir), "benchmarks", "ldbc", relative])
    end
  end
end
