defmodule TripleStore.Benchmark.LDBC.SNB.BI.Checkpoint do
  @moduledoc """
  Closed-store checkpoints for independent SNB BI protocol stages.

  A checkpoint is immutable once created. Restore verifies its content digest,
  replaces the mutable fixture only while closed, reopens it through the normal
  fixture verifier, and carries update position into subsequent run artifacts.
  """

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.StoreFixture

  @names [:initial_load, :post_validation, :pre_power, :pre_throughput]

  @doc "Creates an immutable named checkpoint and reopens the fixture."
  @spec create(StoreFixture.t(), atom(), Path.t(), map()) ::
          {:ok, StoreFixture.t(), map()} | {:error, term()}
  def create(fixture, name, root, state \\ %{}) when name in @names and is_map(state) do
    destination = Path.join(root, Atom.to_string(name))
    statement_count = count_statements(fixture)

    with :ok <- ensure_absent(destination, name),
         :ok <- File.mkdir_p(root),
         {:ok, closed} <- StoreFixture.close(fixture),
         {:ok, _entries} <- File.cp_r(closed.store_path, destination),
         {:ok, fingerprint} <- fingerprint(destination),
         checkpoint <- checkpoint(name, destination, fingerprint, statement_count, state),
         :ok <- write_metadata(checkpoint),
         {:ok, reopened} <-
           StoreFixture.reopen(closed, expected_statement_count: checkpoint.statement_count) do
      {:ok, reopened, checkpoint}
    else
      {:error, _} = error -> error
    end
  end

  @doc "Restores and verifies a checkpoint, returning its recorded protocol position."
  @spec restore(StoreFixture.t(), map()) :: {:ok, StoreFixture.t(), map()} | {:error, term()}
  def restore(fixture, checkpoint) do
    stage = fixture.store_path <> ".checkpoint.#{System.unique_integer([:positive])}"
    replaced = fixture.store_path <> ".replaced.#{System.unique_integer([:positive])}"

    with :ok <- validate_checkpoint(checkpoint),
         {:ok, closed} <- StoreFixture.close(fixture),
         {:ok, _entries} <- File.cp_r(checkpoint.path, stage),
         :ok <- File.rm(Path.join(stage, ".checkpoint.etf")),
         :ok <- exchange(closed.store_path, stage, replaced),
         {:ok, reopened} <- StoreFixture.reopen(closed) do
      File.rm_rf(replaced)
      {:ok, reopened, checkpoint.state}
    else
      {:error, _} = error ->
        File.rm_rf(stage)
        rollback(fixture.store_path, replaced)
        error
    end
  end

  @doc "Verifies that a checkpoint still matches its creation digest."
  @spec validate_checkpoint(map()) :: :ok | {:error, term()}
  def validate_checkpoint(%{name: name, path: path, fingerprint: expected}) when name in @names do
    case fingerprint(path) do
      {:ok, ^expected} -> :ok
      {:ok, actual} -> {:error, {:checkpoint_changed, name, expected, actual}}
      {:error, _} = error -> error
    end
  end

  def validate_checkpoint(_checkpoint), do: {:error, :invalid_checkpoint}

  defp checkpoint(name, path, fingerprint, statement_count, state) do
    %{
      name: name,
      path: path,
      fingerprint: fingerprint,
      statement_count: statement_count,
      state: Map.take(state, [:batch_position, :batch_checksum]),
      created_at_unix_ms: System.system_time(:millisecond)
    }
  end

  defp count_statements(%StoreFixture{store: %{db: db}}) do
    ErlangAdapter.fold_keys(db, :gspo, <<>>, 0, fn _key, count -> count + 1 end,
      fill_cache: false
    )
  end

  defp count_statements(%StoreFixture{store: nil}), do: 0

  defp ensure_absent(path, name) do
    if File.exists?(path), do: {:error, {:checkpoint_exists, name}}, else: :ok
  end

  defp write_metadata(checkpoint) do
    metadata_path = Path.join(checkpoint.path, ".checkpoint.etf")
    metadata = Map.drop(checkpoint, [:path])
    File.write(metadata_path, :erlang.term_to_binary(metadata, [:deterministic]), [:binary])
  end

  defp fingerprint(root) do
    if File.dir?(root) do
      context =
        root
        |> Path.join("**/*")
        |> Path.wildcard(match_dot: true)
        |> Enum.filter(&(File.regular?(&1) and Path.basename(&1) != ".checkpoint.etf"))
        |> Enum.sort()
        |> Enum.reduce(:crypto.hash_init(:sha256), fn path, context ->
          {:ok, checksum} = Artifact.checksum(path)
          relative = Path.relative_to(path, root)
          :crypto.hash_update(context, relative <> "\0" <> checksum <> "\n")
        end)

      {:ok, "sha256:" <> (context |> :crypto.hash_final() |> Base.encode16(case: :lower))}
    else
      {:error, :checkpoint_missing}
    end
  end

  defp exchange(current, replacement, replaced) do
    with :ok <- File.rename(current, replaced) do
      case File.rename(replacement, current) do
        :ok ->
          :ok

        {:error, reason} ->
          File.rename(replaced, current)
          {:error, {:checkpoint_exchange_failed, reason}}
      end
    end
  end

  defp rollback(current, replaced) do
    if File.exists?(replaced) and not File.exists?(current), do: File.rename(replaced, current)
    :ok
  end
end
