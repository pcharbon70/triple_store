defmodule TripleStore.Benchmark.LDBC.ArtifactCache do
  @moduledoc """
  Checksum-gated local cache for external LDBC dataset artifacts.

  Downloads and generators are never started implicitly. Callers must opt into
  external work, and completed files are promoted from `.partial` paths only
  after their expected digest is verified.
  """

  alias TripleStore.Benchmark.Artifact

  @marker ".ldbc-artifact.etf"

  @doc "Returns the validated path for an artifact identifier."
  @spec path(Path.t(), String.t(), String.t()) :: {:ok, Path.t()} | {:error, term()}
  def path(root, artifact_id, filename) do
    with :ok <- validate_segment(artifact_id),
         :ok <- validate_segment(filename) do
      {:ok, Path.join([root, artifact_id, filename])}
    end
  end

  @doc "Returns the resumable partial-download path for an artifact."
  @spec partial_path(Path.t(), String.t(), String.t()) :: {:ok, Path.t()} | {:error, term()}
  def partial_path(root, artifact_id, filename) do
    with {:ok, final_path} <- path(root, artifact_id, filename) do
      {:ok, final_path <> ".partial"}
    end
  end

  @doc "Returns the byte offset from which a resumable acquisition should continue."
  @spec resume_offset(Path.t(), String.t(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def resume_offset(root, artifact_id, filename) do
    with {:ok, partial} <- partial_path(root, artifact_id, filename) do
      case File.stat(partial) do
        {:ok, stat} -> {:ok, stat.size}
        {:error, :enoent} -> {:ok, 0}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Registers a local file and records a safe completion marker."
  @spec register(Path.t(), String.t(), String.t(), Path.t(), String.t()) ::
          {:ok, Path.t()} | {:error, term()}
  def register(root, artifact_id, filename, source, expected_checksum) do
    with :ok <- Artifact.verify_checksum(source, expected_checksum),
         {:ok, destination} <- path(root, artifact_id, filename),
         :ok <- ensure_absent(destination),
         :ok <- Artifact.atomic_copy(source, destination),
         :ok <- write_marker(destination, expected_checksum) do
      {:ok, destination}
    end
  end

  @doc "Promotes a completed partial acquisition after checksum verification."
  @spec promote_partial(Path.t(), String.t(), String.t(), String.t()) ::
          {:ok, Path.t()} | {:error, term()}
  def promote_partial(root, artifact_id, filename, expected_checksum) do
    with {:ok, partial} <- partial_path(root, artifact_id, filename),
         {:ok, destination} <- path(root, artifact_id, filename),
         :ok <- ensure_absent(destination),
         :ok <- Artifact.verify_checksum(partial, expected_checksum),
         :ok <- File.rename(partial, destination),
         :ok <- write_marker(destination, expected_checksum) do
      {:ok, destination}
    end
  end

  @doc "Validates the completion marker, expected identity, file size, and digest."
  @spec validate(Path.t(), String.t(), String.t(), String.t()) ::
          {:ok, Path.t()} | {:error, term()}
  def validate(root, artifact_id, filename, expected_checksum) do
    with {:ok, artifact_path} <- path(root, artifact_id, filename),
         {:ok, marker} <- read_marker(artifact_path),
         :ok <- validate_marker(marker, artifact_id, expected_checksum),
         {:ok, stat} <- File.stat(artifact_path),
         :ok <- validate_size(stat.size, marker.size),
         :ok <- Artifact.verify_checksum(artifact_path, expected_checksum) do
      {:ok, artifact_path}
    else
      {:error, :enoent} -> {:error, :artifact_incomplete}
      {:error, _} = error -> error
    end
  end

  @doc "Builds the shell-free curl command used by an explicit resumable acquisition."
  @spec acquisition_command(String.t(), Path.t()) :: {:ok, map()} | {:error, term()}
  def acquisition_command(url, partial_path)
      when is_binary(url) and is_binary(partial_path) do
    uri = URI.parse(url)

    if uri.scheme in ["https", "http"] and is_binary(uri.host) do
      {:ok,
       %{
         executable: "curl",
         args: [
           "--fail",
           "--location",
           "--continue-at",
           "-",
           "--output",
           partial_path,
           url
         ]
       }}
    else
      {:error, :invalid_acquisition_url}
    end
  end

  @doc "Runs an explicit resumable acquisition and promotes its verified output."
  @spec acquire(Path.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Path.t()} | {:error, term()}
  def acquire(root, artifact_id, filename, url, checksum, opts \\ []) do
    if Keyword.get(opts, :allow_network, false) do
      with {:ok, partial} <- partial_path(root, artifact_id, filename),
           :ok <- File.mkdir_p(Path.dirname(partial)),
           {:ok, command} <- acquisition_command(url, partial),
           {_output, 0} <- System.cmd(command.executable, command.args, stderr_to_stdout: true),
           {:ok, destination} <- promote_partial(root, artifact_id, filename, checksum) do
        {:ok, destination}
      else
        {:error, _} = error -> error
        {_output, status} -> {:error, {:acquisition_failed, status}}
      end
    else
      {:error, :explicit_network_action_required}
    end
  end

  defp write_marker(destination, checksum) do
    with {:ok, stat} <- File.stat(destination) do
      marker = %{
        artifact_id: Path.basename(Path.dirname(destination)),
        filename: Path.basename(destination),
        checksum: checksum,
        size: stat.size
      }

      File.write(marker_path(destination), :erlang.term_to_binary(marker, [:deterministic]), [
        :binary,
        :exclusive
      ])
    end
  end

  defp read_marker(destination) do
    with {:ok, binary} <- File.read(marker_path(destination)) do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    end
  rescue
    ArgumentError -> {:error, :invalid_completion_marker}
  end

  defp validate_marker(marker, artifact_id, checksum) do
    if marker.artifact_id == artifact_id and marker.checksum == checksum do
      :ok
    else
      {:error, :stale_completion_marker}
    end
  end

  defp validate_size(size, size), do: :ok
  defp validate_size(actual, expected), do: {:error, {:size_mismatch, expected, actual}}

  defp marker_path(destination), do: Path.join(Path.dirname(destination), @marker)

  defp ensure_absent(path) do
    if File.exists?(path) or File.exists?(marker_path(path)) do
      {:error, :artifact_already_registered}
    else
      :ok
    end
  end

  defp validate_segment(segment) when is_binary(segment) and segment != "" do
    if Path.basename(segment) == segment and segment not in [".", ".."] do
      :ok
    else
      {:error, :invalid_cache_segment}
    end
  end

  defp validate_segment(_), do: {:error, :invalid_cache_segment}
end
