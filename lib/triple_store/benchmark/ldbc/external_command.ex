defmodule TripleStore.Benchmark.LDBC.ExternalCommand do
  @moduledoc """
  Explicit, immutable-source command boundary for upstream LDBC generators.

  Commands are executed without a shell and only when the caller opts into
  external work. Source commands must point at a Git checkout at the manifest's
  exact commit; container commands must use an immutable image digest.
  """

  @doc "Runs a pinned external command after validating its source identity."
  @spec run(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(spec, opts \\ []) when is_map(spec) and is_list(opts) do
    if Keyword.get(opts, :allow_external, false) do
      with :ok <- validate(spec),
           {output, status} <-
             System.cmd(spec.executable, spec.args,
               cd: Map.get(spec, :working_directory),
               env: Map.get(spec, :env, []),
               stderr_to_stdout: true
             ) do
        if status == 0 do
          {:ok, %{output: output, status: status}}
        else
          {:error, {:external_command_failed, status, output}}
        end
      end
    else
      {:error, :explicit_external_action_required}
    end
  end

  @doc "Validates a shell-free command and its immutable source or container pin."
  @spec validate(map()) :: :ok | {:error, term()}
  def validate(%{executable: executable, args: args} = spec)
      when is_binary(executable) and executable != "" and is_list(args) do
    cond do
      not Enum.all?(args, &is_binary/1) ->
        {:error, :invalid_command_arguments}

      Map.has_key?(spec, :working_directory) ->
        validate_checkout(spec.working_directory, Map.get(spec, :commit))

      Map.has_key?(spec, :container_image) ->
        validate_container_digest(spec.container_image)

      true ->
        {:error, :immutable_source_required}
    end
  end

  def validate(_spec), do: {:error, :invalid_external_command}

  defp validate_checkout(directory, commit)
       when is_binary(directory) and is_binary(commit) and byte_size(commit) == 40 do
    if File.dir?(directory) do
      validate_checkout_head(directory, commit)
    else
      {:error, :source_checkout_missing}
    end
  end

  defp validate_checkout(_directory, _commit), do: {:error, :immutable_commit_required}

  defp validate_checkout_head(directory, commit) do
    case System.cmd("git", ["rev-parse", "HEAD"], cd: directory, stderr_to_stdout: true) do
      {head, 0} -> validate_commit(String.trim(head), commit)
      {_output, _status} -> {:error, :invalid_source_checkout}
    end
  end

  defp validate_commit(commit, commit), do: :ok
  defp validate_commit(_head, _commit), do: {:error, :source_pin_mismatch}

  defp validate_container_digest(image) when is_binary(image) do
    if String.match?(image, ~r/@sha256:[0-9a-f]{64}\z/) do
      :ok
    else
      {:error, :immutable_container_digest_required}
    end
  end

  defp validate_container_digest(_image), do: {:error, :immutable_container_digest_required}
end
