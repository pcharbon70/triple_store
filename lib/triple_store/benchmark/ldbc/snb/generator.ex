defmodule TripleStore.Benchmark.LDBC.SNB.Generator do
  @moduledoc """
  Pinned upstream command plans for SNB BI and Interactive generation.

  The module only constructs shell-free commands. Execution still passes through
  `LDBC.ExternalCommand`, which verifies the exact source checkout and requires
  explicit authorization from the caller.
  """

  alias TripleStore.Benchmark.LDBC.ExternalCommand
  alias TripleStore.Benchmark.LDBC.SNB.Converter

  @doc "Builds the pinned generator command for a registered SNB profile."
  @spec command(atom(), Path.t(), Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def command(profile_id, checkout, output_dir, opts \\ []) do
    with {:ok, profiles} <- Converter.profiles(),
         {:ok, profile} <- Map.fetch(profiles, profile_id),
         :ok <- validate_checkout(profile.generator_kind, checkout),
         :ok <- File.mkdir_p(output_dir) do
      build_command(profile, checkout, output_dir, opts)
    else
      :error -> {:error, {:unknown_snb_profile, profile_id}}
      {:error, _} = error -> error
    end
  end

  @doc "Executes a registered generator only when `allow_external: true` is supplied."
  @spec run(atom(), Path.t(), Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(profile_id, checkout, output_dir, opts \\ []) do
    allow_external? = Keyword.get(opts, :allow_external, false)

    if allow_external? do
      with {:ok, command} <- command(profile_id, checkout, output_dir, opts) do
        run_prepared(command)
      end
    else
      {:error, :explicit_external_action_required}
    end
  end

  defp build_command(%{generator_kind: :spark} = profile, checkout, output_dir, opts) do
    cores = Keyword.get(opts, :cores, System.schedulers_online())
    memory = Keyword.get(opts, :memory, "8G")

    if is_integer(cores) and cores > 0 and is_binary(memory) and memory != "" do
      {:ok,
       %{
         executable: Path.join(checkout, "tools/run.py"),
         args: [
           "--cores",
           Integer.to_string(cores),
           "--memory",
           memory,
           "--",
           "--mode",
           profile.mode,
           "--format",
           profile.serializer,
           "--scale-factor",
           to_string(profile.scale_factor),
           "--output-dir",
           Path.expand(output_dir),
           "--explode-edges",
           "--epoch-millis",
           "--format-options",
           "header=true,quoteAll=true",
           "--generate-factors"
         ],
         working_directory: checkout,
         commit: profile.generator_pin,
         env: []
       }}
    else
      {:error, :invalid_generator_resources}
    end
  end

  defp build_command(%{generator_kind: :hadoop} = profile, checkout, output_dir, opts) do
    params_path = Path.join(output_dir, "params.ini")
    partitions = Keyword.get(opts, :update_partitions, 1)

    content = """
    ldbc.snb.datagen.generator.scaleFactor:snb.interactive.#{profile.scale_factor}
    ldbc.snb.datagen.serializer.numUpdatePartitions:#{partitions}
    ldbc.snb.datagen.serializer.outputDir:#{Path.expand(output_dir)}
    ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvBasicDynamicActivitySerializer
    ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvBasicDynamicPersonSerializer
    ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvBasicStaticSerializer
    """

    with true <- is_integer(partitions) and partitions > 0,
         :ok <- File.write(params_path, content) do
      {:ok,
       %{
         executable: Path.join(checkout, "run.sh"),
         args: [],
         working_directory: checkout,
         commit: profile.generator_pin,
         env: [
           {"LDBC_SNB_DATAGEN_OUTPUT_DIR", Path.expand(output_dir)}
         ],
         prepared_parameters: params_path,
         parameters_destination: Path.join(checkout, "params.ini")
       }}
    else
      false -> {:error, :invalid_update_partition_count}
      {:error, _} = error -> error
    end
  end

  defp validate_checkout(:spark, checkout) do
    require_files(checkout, ["tools/run.py", "build.sbt"])
  end

  defp validate_checkout(:hadoop, checkout) do
    require_files(checkout, ["run.sh", "pom.xml"])
  end

  defp require_files(checkout, relative_paths) do
    if Enum.all?(relative_paths, &File.regular?(Path.join(checkout, &1))) do
      :ok
    else
      {:error, :invalid_generator_checkout}
    end
  end

  defp run_prepared(%{prepared_parameters: source, parameters_destination: destination} = command) do
    previous = File.read(destination)

    try do
      with :ok <- File.cp(source, destination) do
        ExternalCommand.run(command, allow_external: true)
      end
    after
      case previous do
        {:ok, content} -> File.write(destination, content)
        {:error, :enoent} -> File.rm(destination)
        {:error, _reason} -> :ok
      end
    end
  end

  defp run_prepared(command), do: ExternalCommand.run(command, allow_external: true)
end
