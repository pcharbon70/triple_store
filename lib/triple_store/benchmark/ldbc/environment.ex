defmodule TripleStore.Benchmark.LDBC.Environment do
  @moduledoc """
  Captures reproducibility and resource evidence for an LDBC run.

  Collection is best effort and records unavailable fields explicitly rather
  than aborting a benchmark on platform-specific metadata gaps.
  """

  @doc "Captures source, runtime, host, engine, driver, and resource metadata."
  @spec capture(keyword()) :: map()
  def capture(opts \\ []) do
    %{
      captured_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      source: source_state(),
      runtime: runtime_state(),
      host: host_state(),
      engine: Map.new(Keyword.get(opts, :engine, [])),
      driver: Map.new(Keyword.get(opts, :driver, [])),
      resources: resource_state(Keyword.get(opts, :store_path))
    }
  end

  defp source_state do
    {sha, sha_status} = command("git", ["rev-parse", "HEAD"])
    {dirty, dirty_status} = command("git", ["status", "--porcelain"])

    %{
      git_sha: if(sha_status == 0, do: String.trim(sha), else: :unavailable),
      git_dirty: if(dirty_status == 0, do: String.trim(dirty) != "", else: :unavailable),
      mix_lock_sha256: checksum("mix.lock")
    }
  end

  defp runtime_state do
    {rust, rust_status} = command("rustc", ["--version"])

    %{
      elixir: System.version(),
      otp: System.otp_release(),
      erts: :erlang.system_info(:version) |> to_string(),
      rust: if(rust_status == 0, do: String.trim(rust), else: :unavailable),
      rocksdb_dependency: dependency_version(:rocksdb)
    }
  end

  defp host_state do
    %{
      os: :os.type(),
      kernel: :os.version(),
      schedulers: System.schedulers_online(),
      cpu_topology: :erlang.system_info(:cpu_topology),
      total_memory_bytes: total_memory_bytes(),
      filesystem: File.stat(".") |> normalize_stat()
    }
  end

  defp resource_state(store_path) do
    memory = :erlang.memory()

    %{
      beam_memory_bytes: memory[:total],
      beam_process_memory_bytes: memory[:processes],
      process_count: :erlang.system_info(:process_count),
      io: elem(:erlang.statistics(:io), 1) |> io_map(),
      store_bytes: directory_size(store_path)
    }
  end

  defp command(executable, args) do
    System.cmd(executable, args, stderr_to_stdout: true)
  rescue
    _error -> {"", 127}
  end

  defp checksum(path) do
    case File.read(path) do
      {:ok, data} -> :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
      {:error, _reason} -> :unavailable
    end
  end

  defp dependency_version(application) do
    case Application.spec(application, :vsn) do
      nil -> :unavailable
      version -> to_string(version)
    end
  end

  defp total_memory_bytes do
    with {:ok, contents} <- File.read("/proc/meminfo"),
         [_, kilobytes] <- Regex.run(~r/^MemTotal:\s+(\d+)\s+kB/m, contents),
         {value, ""} <- Integer.parse(kilobytes) do
      value * 1_024
    else
      _other -> :unavailable
    end
  end

  defp normalize_stat({:ok, stat}),
    do: %{major_device: stat.major_device, minor_device: stat.minor_device}

  defp normalize_stat({:error, reason}), do: %{unavailable: reason}

  defp io_map({input, output}), do: %{input_bytes: input, output_bytes: output}

  defp directory_size(nil), do: :unavailable

  defp directory_size(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce(0, fn file, total ->
      case File.stat(file) do
        {:ok, %{type: :regular, size: size}} -> total + size
        _other -> total
      end
    end)
  end
end
