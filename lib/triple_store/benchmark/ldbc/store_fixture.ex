defmodule TripleStore.Benchmark.LDBC.StoreFixture do
  @moduledoc """
  Exclusive, restorable TripleStore fixtures for stateful LDBC workloads.

  Initial data is loaded into a staging store, closed, verified after reopen,
  and copied into a pristine directory that is never opened. Resets copy that
  pristine directory to a sibling staging path and atomically exchange it with
  the mutable store while all database resources are closed.
  """

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{DatasetManifest, StreamLoader}
  alias TripleStore.Benchmark.LDBC.SNB.UpdateStream
  alias TripleStore.QuadOperations

  defstruct manifest: nil,
            fixture_root: nil,
            store: nil,
            store_path: nil,
            pristine_path: nil,
            pristine_fingerprint: nil,
            lock_path: nil,
            load_metrics: nil,
            verification: nil

  @type t :: %__MODULE__{
          manifest: DatasetManifest.t(),
          fixture_root: Path.t(),
          store: TripleStore.store() | nil,
          store_path: Path.t(),
          pristine_path: Path.t(),
          pristine_fingerprint: String.t(),
          lock_path: Path.t(),
          load_metrics: map(),
          verification: map()
        }

  @doc "Creates, loads, snapshots, reopens, and verifies an exclusive fixture."
  @spec setup(Path.t(), DatasetManifest.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def setup(root, %DatasetManifest{} = manifest, opts \\ []) when is_binary(root) do
    with :ok <- DatasetManifest.validate(manifest),
         :ok <- validate_identity(manifest.store.path_identity),
         :ok <- validate_expected_identity(manifest, opts),
         :ok <- validate_components(manifest),
         paths <- paths(root, manifest.store.path_identity),
         :ok <- prepare_directories(paths),
         :ok <- acquire_lock(paths.lock, manifest),
         {:ok, fixture} <- create_fixture(paths, root, manifest, opts) do
      {:ok, fixture}
    else
      {:error, _} = error -> error
    end
  end

  @doc "Closes and reopens the mutable store, then verifies its initial state."
  @spec reopen(t(), keyword()) :: {:ok, t()} | {:error, term()}
  def reopen(fixture, opts \\ [])

  def reopen(%__MODULE__{store: nil} = fixture, opts) do
    with {:ok, store} <-
           TripleStore.open(fixture.store_path, schema: :quad, create_if_missing: false),
         {:ok, verification} <- verify_state(store, fixture.manifest, opts) do
      {:ok, %{fixture | store: store, verification: verification}}
    else
      {:error, _} = error -> error
    end
  end

  def reopen(%__MODULE__{}, _opts), do: {:error, :store_already_open}

  @doc "Applies one ordered SNB update component through normal dictionary and quad batches."
  @spec apply_updates(t(), Path.t()) :: {:ok, t(), map()} | {:error, term()}
  def apply_updates(%__MODULE__{store: store} = fixture, path) when not is_nil(store) do
    result =
      path
      |> UpdateStream.stream()
      |> Enum.reduce_while({:ok, 0, -1}, fn record, {:ok, count, previous} ->
        case apply_record(store, record, previous) do
          :ok -> {:cont, {:ok, count + 1, record.sequence}}
          {:error, reason} -> {:halt, {:error, {:update_failed, reason}}}
        end
      end)

    case result do
      {:ok, count, sequence} -> {:ok, fixture, %{records: count, last_sequence: sequence}}
      {:error, _} = error -> error
    end
  rescue
    error in File.Error -> {:error, {:update_stream_failed, error.reason}}
  end

  def apply_updates(%__MODULE__{}, _path), do: {:error, :store_closed}

  @doc "Restores the immutable pristine copy and verifies counts plus caller probes."
  @spec reset(t(), keyword()) :: {:ok, t()} | {:error, term()}
  def reset(%__MODULE__{} = fixture, opts \\ []) do
    reset_stage = fixture.store_path <> ".reset.#{System.unique_integer([:positive])}"
    replaced = fixture.store_path <> ".replaced.#{System.unique_integer([:positive])}"

    with {:ok, closed} <- close(fixture),
         :ok <- verify_pristine(closed),
         :ok <- copy_directory(closed.pristine_path, reset_stage),
         :ok <- exchange_directories(closed.store_path, reset_stage, replaced),
         {:ok, reopened} <- reopen(closed, opts) do
      File.rm_rf(replaced)
      {:ok, reopened}
    else
      {:error, _} = error ->
        File.rm_rf(reset_stage)
        rollback_exchange(fixture.store_path, replaced)
        error
    end
  end

  @doc "Closes an open fixture without releasing its run lock."
  @spec close(t()) :: {:ok, t()} | {:error, term()}
  def close(%__MODULE__{store: nil} = fixture), do: {:ok, fixture}

  def close(%__MODULE__{store: store} = fixture) do
    case TripleStore.close(store) do
      :ok -> {:ok, %{fixture | store: nil}}
      {:error, reason} -> {:error, {:close_failed, reason}}
    end
  end

  @doc "Closes the fixture, releases its lock, and optionally removes store copies."
  @spec teardown(t(), keyword()) :: :ok | {:error, term()}
  def teardown(%__MODULE__{} = fixture, opts \\ []) do
    with {:ok, closed} <- close(fixture) do
      if Keyword.get(opts, :delete, false) do
        File.rm_rf(closed.store_path)
        File.rm_rf(closed.pristine_path)
      end

      File.rm(closed.lock_path)
      :ok
    end
  end

  defp create_fixture(paths, root, manifest, opts) do
    stage = paths.store <> ".load.#{System.unique_integer([:positive])}"
    pristine_stage = paths.pristine <> ".partial.#{System.unique_integer([:positive])}"

    with :ok <- ensure_absent(paths.store, :store_already_exists),
         :ok <- ensure_absent(paths.pristine, :pristine_store_already_exists) do
      result = build_fixture(paths, root, manifest, opts, stage, pristine_stage)

      case result do
        {:ok, _} = success ->
          success

        {:error, _} = error ->
          File.rm_rf(stage)
          File.rm_rf(pristine_stage)
          File.rm_rf(paths.store)
          File.rm_rf(paths.pristine)
          File.rm(paths.lock)
          error
      end
    else
      {:error, _} = error ->
        File.rm(paths.lock)
        error
    end
  end

  defp build_fixture(paths, root, manifest, opts, stage, pristine_stage) do
    with {:ok, store} <- TripleStore.open(stage, schema: :quad),
         {:ok, metrics} <- load_and_close(store, manifest, opts),
         :ok <- copy_directory(stage, pristine_stage),
         :ok <- File.rename(pristine_stage, paths.pristine),
         {:ok, fingerprint} <- directory_fingerprint(paths.pristine),
         :ok <- File.rename(stage, paths.store),
         {:ok, opened} <- TripleStore.open(paths.store, schema: :quad, create_if_missing: false),
         {:ok, verification} <- verify_state(opened, manifest, opts) do
      {:ok,
       %__MODULE__{
         manifest: manifest,
         fixture_root: root,
         store: opened,
         store_path: paths.store,
         pristine_path: paths.pristine,
         pristine_fingerprint: fingerprint,
         lock_path: paths.lock,
         load_metrics: metrics,
         verification: verification
       }}
    end
  end

  defp load_and_close(store, manifest, opts) do
    result = StreamLoader.load(store, manifest, opts)
    close_result = TripleStore.close(store)

    case {result, close_result} do
      {{:ok, metrics}, :ok} ->
        size =
          case Artifact.size(store.path) do
            {:ok, bytes} -> bytes
            _ -> nil
          end

        {:ok, %{metrics | store_size_bytes: size}}

      {{:error, reason}, :ok} ->
        {:error, reason}

      {_, {:error, reason}} ->
        {:error, {:close_failed, reason}}
    end
  end

  defp verify_state(store, manifest, opts) do
    expected = initial_component_count(manifest)

    with {:ok, verification} <- StreamLoader.verify(store, expected),
         :ok <- run_probes(store, Keyword.get(opts, :probes, [])) do
      {:ok, verification}
    else
      {:error, _} = error ->
        TripleStore.close(store)
        error
    end
  end

  defp run_probes(store, probes) when is_list(probes) do
    Enum.reduce_while(probes, :ok, fn
      probe, :ok when is_function(probe, 1) ->
        case probe.(store) do
          :ok -> {:cont, :ok}
          {:ok, _value} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:reset_probe_failed, reason}}}
          other -> {:halt, {:error, {:invalid_reset_probe_result, other}}}
        end

      _invalid, :ok ->
        {:halt, {:error, :invalid_reset_probe}}
    end)
  end

  defp run_probes(_store, _probes), do: {:error, :invalid_reset_probes}

  defp apply_record(store, %{sequence: sequence, operation: operation, quads: quads}, previous)
       when is_integer(sequence) and sequence > previous and operation in [:insert, :delete] and
              is_list(quads) do
    with {:ok, encoded} <- Adapter.from_rdf_quads(store.dict_manager, quads) do
      case operation do
        :insert -> QuadOperations.insert_quads(store.db, encoded, sync: true)
        :delete -> QuadOperations.delete_quads(store.db, encoded, sync: true)
      end
    end
  end

  defp apply_record(_store, {:error, reason}, _previous), do: {:error, reason}
  defp apply_record(_store, _record, _previous), do: {:error, :invalid_update_record}

  defp acquire_lock(path, manifest) do
    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        result = IO.binwrite(io, manifest.dataset_id <> "\n")
        File.close(io)
        result

      {:error, :eexist} ->
        {:error, {:fixture_locked, path}}

      {:error, reason} ->
        {:error, {:fixture_lock_failed, reason}}
    end
  end

  defp prepare_directories(paths) do
    with :ok <- File.mkdir_p(Path.dirname(paths.store)),
         :ok <- File.mkdir_p(Path.dirname(paths.pristine)) do
      File.mkdir_p(Path.dirname(paths.lock))
    end
  end

  defp paths(root, identity) do
    %{
      store: Path.join([root, "stores", identity]),
      pristine: Path.join([root, "pristine", identity]),
      lock: Path.join([root, "locks", identity <> ".lock"])
    }
  end

  defp validate_components(manifest) do
    Enum.reduce_while(manifest.components, :ok, fn component, :ok ->
      case Artifact.verify_checksum(component.path, component.checksum) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:component_validation_failed, component.role, reason}}}
      end
    end)
  end

  defp validate_identity(identity) when is_binary(identity) do
    if String.match?(identity, ~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/),
      do: :ok,
      else: {:error, :unsafe_store_path_identity}
  end

  defp validate_identity(_identity), do: {:error, :unsafe_store_path_identity}

  defp validate_expected_identity(manifest, opts) do
    expected_suite = Keyword.get(opts, :expected_suite, manifest.suite)
    expected_profile = Keyword.get(opts, :expected_profile, manifest.profile) |> to_string()

    cond do
      expected_suite != manifest.suite ->
        {:error, {:manifest_suite_mismatch, expected_suite, manifest.suite}}

      expected_profile != manifest.profile ->
        {:error, {:manifest_profile_mismatch, expected_profile, manifest.profile}}

      true ->
        :ok
    end
  end

  defp ensure_absent(path, error) do
    if File.exists?(path), do: {:error, error}, else: :ok
  end

  defp initial_component_count(manifest) do
    manifest.components |> Enum.find(&(&1.role == :initial)) |> Map.fetch!(:count)
  end

  defp copy_directory(source, destination) do
    case File.cp_r(source, destination) do
      {:ok, _entries} -> :ok
      {:error, reason, _entry} -> {:error, {:fixture_copy_failed, reason}}
    end
  end

  defp exchange_directories(current, replacement, replaced) do
    with :ok <- File.rename(current, replaced) do
      case File.rename(replacement, current) do
        :ok ->
          :ok

        {:error, reason} ->
          File.rename(replaced, current)
          {:error, {:fixture_exchange_failed, reason}}
      end
    end
  end

  defp rollback_exchange(current, replaced) do
    if File.exists?(replaced) and not File.exists?(current), do: File.rename(replaced, current)
    :ok
  end

  defp verify_pristine(fixture) do
    case directory_fingerprint(fixture.pristine_path) do
      {:ok, fingerprint} when fingerprint == fixture.pristine_fingerprint -> :ok
      {:ok, actual} -> {:error, {:pristine_fixture_changed, fixture.pristine_fingerprint, actual}}
      {:error, _} = error -> error
    end
  end

  defp directory_fingerprint(root) do
    if File.dir?(root) do
      digest =
        root
        |> Path.join("**/*")
        |> Path.wildcard(match_dot: true)
        |> Enum.filter(&File.regular?/1)
        |> Enum.sort()
        |> Enum.reduce(:crypto.hash_init(:sha256), fn path, context ->
          relative = Path.relative_to(path, root)
          {:ok, checksum} = Artifact.checksum(path)
          :crypto.hash_update(context, relative <> "\0" <> checksum <> "\n")
        end)
        |> :crypto.hash_final()
        |> Base.encode16(case: :lower)

      {:ok, "sha256:" <> digest}
    else
      {:error, :pristine_fixture_missing}
    end
  end
end
