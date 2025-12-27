defmodule TripleStore.Backup do
  @moduledoc """
  Backup and restore functionality for the TripleStore.

  Provides filesystem-based backup and restore operations for disaster recovery.
  Uses directory copying to create consistent backups.

  ## Backup Strategy

  Backups are created by copying the entire database directory to a backup
  location. This provides a complete snapshot of the database state.

  ## Important Notes

  - **Consistency**: Close the store before backup for guaranteed consistency
  - **Hot Backup**: Hot backups (while open) may capture writes in progress
  - **Space**: Backup requires approximately the same disk space as the original

  ## Usage

      # Create a backup
      {:ok, metadata} = TripleStore.Backup.create(store, "/backups/mydb_20251227")

      # List backups
      {:ok, backups} = TripleStore.Backup.list("/backups")

      # Restore from backup
      {:ok, store} = TripleStore.Backup.restore("/backups/mydb_20251227", "/data/restored")

      # Verify backup integrity
      {:ok, :valid} = TripleStore.Backup.verify("/backups/mydb_20251227")

  ## Scheduled Backups

      # Create a rotating backup (keeps last N backups)
      {:ok, metadata} = TripleStore.Backup.rotate(store, "/backups", max_backups: 5)

  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Telemetry

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Store handle"
  @type store :: TripleStore.store()

  @typedoc "Backup metadata"
  @type backup_metadata :: %{
          path: Path.t(),
          source_path: Path.t(),
          created_at: DateTime.t(),
          size_bytes: non_neg_integer(),
          file_count: non_neg_integer()
        }

  @typedoc "Backup options"
  @type backup_opts :: [
          compress: boolean(),
          verify: boolean()
        ]

  @typedoc "Restore options"
  @type restore_opts :: [
          overwrite: boolean()
        ]

  @typedoc "Rotation options"
  @type rotate_opts :: [
          max_backups: pos_integer(),
          prefix: String.t()
        ]

  # ===========================================================================
  # Backup Operations
  # ===========================================================================

  @doc """
  Creates a backup of the store.

  Copies the database directory to the specified backup path.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`
  - `backup_path` - Destination path for the backup

  ## Options

  - `:verify` - Verify backup after creation (default: true)

  ## Returns

  - `{:ok, metadata}` - Backup metadata including path and size
  - `{:error, :backup_path_exists}` - Backup path already exists
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, metadata} = TripleStore.Backup.create(store, "/backups/mydb_20251227")
      # => {:ok, %{
      #      path: "/backups/mydb_20251227",
      #      source_path: "/data/mydb",
      #      created_at: ~U[2025-12-27 10:00:00Z],
      #      size_bytes: 1048576,
      #      file_count: 42
      #    }}

  """
  @spec create(store(), Path.t(), backup_opts()) ::
          {:ok, backup_metadata()} | {:error, term()}
  def create(%{path: source_path} = store, backup_path, opts \\ []) do
    verify = Keyword.get(opts, :verify, true)

    Telemetry.span(:backup, :create, %{source: source_path, destination: backup_path}, fn ->
      with :ok <- validate_backup_path(backup_path),
           :ok <- ensure_parent_exists(backup_path),
           {:ok, _} <- copy_directory(source_path, backup_path),
           :ok <- write_metadata(backup_path, source_path, store),
           :ok <- maybe_verify(backup_path, verify) do
        metadata = build_metadata(backup_path, source_path)
        {metadata, %{size_bytes: metadata.size_bytes}}
      end
    end)
  end

  @doc """
  Lists available backups in a directory.

  Scans the directory for valid backup directories and returns their metadata.

  ## Arguments

  - `backup_dir` - Directory containing backups

  ## Returns

  - `{:ok, backups}` - List of backup metadata, sorted by creation time (newest first)
  - `{:error, :not_a_directory}` - Path is not a directory
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, backups} = TripleStore.Backup.list("/backups")
      # => {:ok, [
      #      %{path: "/backups/mydb_20251227", created_at: ~U[2025-12-27 10:00:00Z], ...},
      #      %{path: "/backups/mydb_20251226", created_at: ~U[2025-12-26 10:00:00Z], ...}
      #    ]}

  """
  @spec list(Path.t()) :: {:ok, [backup_metadata()]} | {:error, term()}
  def list(backup_dir) do
    if File.dir?(backup_dir) do
      backups =
        backup_dir
        |> File.ls!()
        |> Enum.map(&Path.join(backup_dir, &1))
        |> Enum.filter(&valid_backup?/1)
        |> Enum.map(&read_backup_metadata/1)
        |> Enum.reject(&is_nil/1)
        |> Enum.sort_by(& &1.created_at, {:desc, DateTime})

      {:ok, backups}
    else
      {:error, :not_a_directory}
    end
  end

  @doc """
  Restores a store from a backup.

  Copies the backup to the destination path and opens it as a new store.

  ## Arguments

  - `backup_path` - Path to the backup
  - `restore_path` - Destination path for the restored store

  ## Options

  - `:overwrite` - Overwrite existing destination (default: false)

  ## Returns

  - `{:ok, store}` - Restored store handle
  - `{:error, :invalid_backup}` - Backup is not valid
  - `{:error, :destination_exists}` - Destination already exists
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, store} = TripleStore.Backup.restore("/backups/mydb_20251227", "/data/restored")

  """
  @spec restore(Path.t(), Path.t(), restore_opts()) ::
          {:ok, store()} | {:error, term()}
  def restore(backup_path, restore_path, opts \\ []) do
    overwrite = Keyword.get(opts, :overwrite, false)

    Telemetry.span(:backup, :restore, %{source: backup_path, destination: restore_path}, fn ->
      with {:ok, :valid} <- verify(backup_path),
           :ok <- validate_restore_path(restore_path, overwrite),
           {:ok, _} <- copy_directory(backup_path, restore_path),
           {:ok, store} <- TripleStore.open(restore_path) do
        {store, %{}}
      end
    end)
  end

  @doc """
  Verifies the integrity of a backup.

  Checks that the backup directory contains all required files and
  can be opened successfully.

  ## Arguments

  - `backup_path` - Path to the backup to verify

  ## Returns

  - `{:ok, :valid}` - Backup is valid
  - `{:error, :missing_files}` - Required files are missing
  - `{:error, :cannot_open}` - Backup cannot be opened
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, :valid} = TripleStore.Backup.verify("/backups/mydb_20251227")

  """
  @spec verify(Path.t()) :: {:ok, :valid} | {:error, term()}
  def verify(backup_path) do
    cond do
      not File.dir?(backup_path) ->
        {:error, :not_a_directory}

      not has_required_files?(backup_path) ->
        {:error, :missing_files}

      true ->
        # Try to open the backup to verify it's valid
        case NIF.open(backup_path) do
          {:ok, db} ->
            NIF.close(db)
            {:ok, :valid}

          {:error, reason} ->
            {:error, {:cannot_open, reason}}
        end
    end
  end

  @doc """
  Creates a rotating backup with automatic cleanup.

  Creates a new timestamped backup and removes old backups beyond
  the specified limit.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`
  - `backup_dir` - Directory to store backups

  ## Options

  - `:max_backups` - Maximum number of backups to keep (default: 5)
  - `:prefix` - Backup name prefix (default: "backup")

  ## Returns

  - `{:ok, metadata}` - New backup metadata
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, metadata} = TripleStore.Backup.rotate(store, "/backups", max_backups: 3)

  """
  @spec rotate(store(), Path.t(), rotate_opts()) ::
          {:ok, backup_metadata()} | {:error, term()}
  def rotate(store, backup_dir, opts \\ []) do
    max_backups = Keyword.get(opts, :max_backups, 5)
    prefix = Keyword.get(opts, :prefix, "backup")

    # Create timestamped backup name
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d_%H%M%S")
    backup_name = "#{prefix}_#{timestamp}"
    backup_path = Path.join(backup_dir, backup_name)

    with {:ok, metadata} <- create(store, backup_path, opts),
         :ok <- cleanup_old_backups(backup_dir, prefix, max_backups) do
      {:ok, metadata}
    end
  end

  @doc """
  Deletes a backup.

  ## Arguments

  - `backup_path` - Path to the backup to delete

  ## Returns

  - `:ok` - Backup deleted successfully
  - `{:error, :not_a_backup}` - Path is not a valid backup
  - `{:error, reason}` - Other failures

  """
  @spec delete(Path.t()) :: :ok | {:error, term()}
  def delete(backup_path) do
    if valid_backup?(backup_path) do
      File.rm_rf!(backup_path)
      :ok
    else
      {:error, :not_a_backup}
    end
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp validate_backup_path(path) do
    if File.exists?(path) do
      {:error, :backup_path_exists}
    else
      :ok
    end
  end

  defp validate_restore_path(path, overwrite) do
    cond do
      not File.exists?(path) ->
        :ok

      overwrite ->
        File.rm_rf!(path)
        :ok

      true ->
        {:error, :destination_exists}
    end
  end

  defp ensure_parent_exists(path) do
    parent = Path.dirname(path)

    case File.mkdir_p(parent) do
      :ok -> :ok
      {:error, reason} -> {:error, {:mkdir_failed, reason}}
    end
  end

  defp copy_directory(source, destination) do
    case File.cp_r(source, destination) do
      {:ok, files} -> {:ok, files}
      {:error, reason, _file} -> {:error, {:copy_failed, reason}}
    end
  end

  defp write_metadata(backup_path, source_path, _store) do
    metadata = %{
      source_path: source_path,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      version: "1.0"
    }

    metadata_path = Path.join(backup_path, ".backup_metadata")
    content = :erlang.term_to_binary(metadata)

    case File.write(metadata_path, content) do
      :ok -> :ok
      {:error, reason} -> {:error, {:metadata_write_failed, reason}}
    end
  end

  defp maybe_verify(_backup_path, false), do: :ok

  defp maybe_verify(backup_path, true) do
    case verify(backup_path) do
      {:ok, :valid} -> :ok
      error -> error
    end
  end

  defp build_metadata(backup_path, source_path) do
    {size, file_count} = directory_stats(backup_path)

    %{
      path: backup_path,
      source_path: source_path,
      created_at: DateTime.utc_now(),
      size_bytes: size,
      file_count: file_count
    }
  end

  defp directory_stats(path) do
    files = list_all_files(path)
    size = files |> Enum.map(&File.stat!(&1).size) |> Enum.sum()
    {size, length(files)}
  end

  defp list_all_files(path) do
    path
    |> File.ls!()
    |> Enum.flat_map(fn entry ->
      full_path = Path.join(path, entry)

      if File.dir?(full_path) do
        list_all_files(full_path)
      else
        [full_path]
      end
    end)
  end

  defp valid_backup?(path) do
    File.dir?(path) and has_required_files?(path)
  end

  defp has_required_files?(path) do
    # RocksDB databases have these characteristic files
    required_patterns = ["CURRENT", "MANIFEST"]

    Enum.all?(required_patterns, fn pattern ->
      path
      |> File.ls!()
      |> Enum.any?(&String.starts_with?(&1, pattern))
    end)
  rescue
    _ -> false
  end

  defp read_backup_metadata(path) do
    metadata_path = Path.join(path, ".backup_metadata")

    if File.exists?(metadata_path) do
      case File.read(metadata_path) do
        {:ok, content} ->
          stored = :erlang.binary_to_term(content)
          {size, file_count} = directory_stats(path)

          %{
            path: path,
            source_path: stored[:source_path],
            created_at: parse_datetime(stored[:created_at]),
            size_bytes: size,
            file_count: file_count
          }

        {:error, _} ->
          nil
      end
    else
      # Try to infer metadata from directory
      {size, file_count} = directory_stats(path)

      %{
        path: path,
        source_path: nil,
        created_at: infer_creation_time(path),
        size_bytes: size,
        file_count: file_count
      }
    end
  rescue
    _ -> nil
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(iso_string) when is_binary(iso_string) do
    case DateTime.from_iso8601(iso_string) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp infer_creation_time(path) do
    case File.stat(path) do
      {:ok, %File.Stat{mtime: mtime}} ->
        mtime
        |> NaiveDateTime.from_erl!()
        |> DateTime.from_naive!("Etc/UTC")

      _ ->
        nil
    end
  end

  defp cleanup_old_backups(backup_dir, prefix, max_backups) do
    case list(backup_dir) do
      {:ok, backups} ->
        # Filter to only backups matching our prefix
        matching =
          backups
          |> Enum.filter(fn b ->
            Path.basename(b.path) |> String.starts_with?(prefix)
          end)
          |> Enum.sort_by(& &1.created_at, {:desc, DateTime})

        # Delete excess backups
        matching
        |> Enum.drop(max_backups)
        |> Enum.each(fn backup ->
          Logger.info("Removing old backup: #{backup.path}")
          File.rm_rf!(backup.path)
        end)

        :ok

      {:error, :not_a_directory} ->
        :ok

      error ->
        error
    end
  end
end
