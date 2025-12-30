defmodule TripleStore.Backup do
  @moduledoc """
  Backup and restore functionality for the TripleStore.

  Provides filesystem-based backup and restore operations for disaster recovery.
  Uses directory copying to create consistent backups.

  ## Backup Strategy

  Backups are created by copying the entire database directory to a backup
  location. This provides a complete snapshot of the database state.

  Two backup types are supported:

  - **Full backup**: Complete copy of the database directory
  - **Incremental backup**: Only copies changed files, hard-links unchanged ones

  ## Important Notes

  - **Consistency**: Close the store before backup for guaranteed consistency
  - **Hot Backup**: Hot backups (while open) may capture writes in progress
  - **Space**: Full backup requires same disk space; incremental saves space via hard links

  ## Usage

      # Create a full backup
      {:ok, metadata} = TripleStore.Backup.create(store, "/backups/mydb_20251227")

      # Create an incremental backup (based on previous full backup)
      {:ok, incr_meta} = TripleStore.Backup.create_incremental(
        store,
        "/backups/mydb_20251227_incr1",
        "/backups/mydb_20251227"
      )

      # List backups
      {:ok, backups} = TripleStore.Backup.list("/backups")

      # Restore from backup (works for both full and incremental)
      {:ok, store} = TripleStore.Backup.restore("/backups/mydb_20251227", "/data/restored")

      # Verify backup integrity
      {:ok, :valid} = TripleStore.Backup.verify("/backups/mydb_20251227")

  ## Scheduled Backups

      # Create a rotating backup (keeps last N backups)
      {:ok, metadata} = TripleStore.Backup.rotate(store, "/backups", max_backups: 5)

  ## Incremental Backup Strategy

  For large databases with infrequent changes, use incremental backups:

  1. Create a full backup weekly
  2. Create incremental backups daily based on the last full backup
  3. Each incremental backup is independently restorable

  ## Incremental Backup Limitations

  - **Best with closed store**: Incremental backups work most reliably when the
    store is closed or quiescent. Active writes during backup may cause
    verification failures due to file changes.
  - **Filesystem support**: Hard links require both source and destination to
    be on the same filesystem. Cross-filesystem backups fall back to copying.
  - **File detection**: Changes are detected by comparing file size and mtime.
    Files with the same size and modification time are assumed unchanged.

  ## Security

  - **Path traversal protection**: Backup/restore paths are validated to prevent
    directory traversal attacks.
  - **Symlink protection**: Source directories are checked for symlinks before
    copying to prevent symlink-based attacks.
  - **Safe deserialization**: Backup metadata files are deserialized with the
    `:safe` option to prevent arbitrary code execution.

  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.SequenceCounter
  alias TripleStore.Telemetry

  require Logger

  # Counter state file name within backup directory
  @counter_file ".counter_state"

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Store handle"
  @type store :: TripleStore.store()

  @typedoc "Backup metadata"
  @type backup_metadata :: %{
          required(:path) => Path.t(),
          required(:source_path) => Path.t(),
          required(:created_at) => DateTime.t(),
          required(:size_bytes) => non_neg_integer(),
          required(:file_count) => non_neg_integer(),
          optional(:backup_type) => :full | :incremental,
          optional(:base_backup) => Path.t()
        }

  @typedoc "Backup options"
  @type backup_opts :: [
          compress: boolean(),
          verify: boolean(),
          incremental: boolean(),
          base_backup: Path.t()
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

    # Use basename for telemetry to avoid exposing full paths
    telemetry_meta = %{
      source: Path.basename(source_path),
      destination: Path.basename(backup_path)
    }

    Telemetry.span(:backup, :create, telemetry_meta, fn ->
      with :ok <- validate_backup_path(backup_path),
           :ok <- ensure_parent_exists(backup_path),
           {:ok, _} <- copy_directory(source_path, backup_path),
           :ok <- write_metadata(backup_path, source_path, store),
           :ok <- maybe_verify(backup_path, verify) do
        metadata = build_metadata(backup_path, source_path)
        {{:ok, metadata}, %{size_bytes: metadata.size_bytes}}
      end
    end)
  end

  @doc """
  Creates an incremental backup based on a previous backup.

  Only copies files that have changed since the base backup, significantly
  reducing backup time and storage for large databases with few changes.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`
  - `backup_path` - Destination path for the backup
  - `base_backup` - Path to the base backup to compare against

  ## Options

  - `:verify` - Verify backup after creation (default: true)

  ## Returns

  - `{:ok, metadata}` - Backup metadata with `:backup_type` set to `:incremental`
  - `{:error, :base_backup_invalid}` - Base backup is not valid
  - `{:error, :backup_path_exists}` - Backup path already exists
  - `{:error, reason}` - Other failures

  ## How It Works

  Incremental backups work by comparing file modification times and sizes:
  1. All files from base backup are hard-linked (no copy, saves space)
  2. Changed/new files from source are copied over
  3. Deleted files are not included

  To restore an incremental backup, simply restore it directly - it contains
  all necessary files (via hard links to base backup).

  ## Examples

      # First, create a full backup
      {:ok, full} = TripleStore.Backup.create(store, "/backups/full_20251227")

      # Later, create incremental backup
      {:ok, incr} = TripleStore.Backup.create_incremental(
        store,
        "/backups/incr_20251227_1200",
        "/backups/full_20251227"
      )

  """
  @spec create_incremental(store(), Path.t(), Path.t(), backup_opts()) ::
          {:ok, backup_metadata()} | {:error, term()}
  def create_incremental(%{path: source_path} = store, backup_path, base_backup, opts \\ []) do
    verify = Keyword.get(opts, :verify, true)

    # Use basename for telemetry to avoid exposing full paths
    telemetry_meta = %{
      source: Path.basename(source_path),
      destination: Path.basename(backup_path),
      base: Path.basename(base_backup)
    }

    Telemetry.span(
      :backup,
      :create_incremental,
      telemetry_meta,
      fn ->
        with {:ok, :valid} <- verify(base_backup),
             :ok <- validate_backup_path(backup_path),
             :ok <- ensure_parent_exists(backup_path),
             {:ok, stats} <- copy_incremental(source_path, backup_path, base_backup),
             :ok <- write_incremental_metadata(backup_path, source_path, base_backup, store),
             :ok <- maybe_verify(backup_path, verify) do
          metadata = build_incremental_metadata(backup_path, source_path, base_backup, stats)
          {{:ok, metadata}, %{size_bytes: metadata.size_bytes, files_copied: stats.files_copied}}
        end
      end
    )
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

  ## Counter Restoration

  The restore operation automatically restores sequence counters from the backup
  to ensure that new IDs generated after restore don't collide with existing data.
  A safety margin is applied to counter values during import.

  **Note:** There is a small window between when the store opens (initializing
  counters from RocksDB) and when backup counters are imported. During this
  window, if concurrent writes occur, counters take the maximum of current and
  imported values. For guaranteed consistency, ensure no writes occur during
  restore or perform the restore on an exclusively-owned store handle.

  ## Examples

      {:ok, store} = TripleStore.Backup.restore("/backups/mydb_20251227", "/data/restored")

  """
  @spec restore(Path.t(), Path.t(), restore_opts()) ::
          {:ok, store()} | {:error, term()}
  def restore(backup_path, restore_path, opts \\ []) do
    overwrite = Keyword.get(opts, :overwrite, false)

    telemetry_meta = %{
      source: Path.basename(backup_path),
      destination: Path.basename(restore_path)
    }

    Telemetry.span(:backup, :restore, telemetry_meta, fn ->
      with {:ok, :valid} <- verify(backup_path),
           :ok <- validate_restore_path(restore_path, overwrite),
           {:ok, _} <- copy_directory(backup_path, restore_path),
           {:ok, store} <- TripleStore.open(restore_path),
           :ok <- restore_counter_state(backup_path, store) do
        {{:ok, store}, %{}}
      end
    end)
  end

  # Restore sequence counter state from backup
  defp restore_counter_state(backup_path, %{dict_manager: dict_manager}) do
    counter_path = Path.join(backup_path, @counter_file)

    if File.exists?(counter_path) do
      with {:ok, counter} <- Manager.get_counter(dict_manager),
           :ok <- SequenceCounter.import_from_file(counter, counter_path) do
        Logger.info("Restored counter state from backup")
        :ok
      else
        {:error, reason} ->
          Logger.warning("Failed to restore counter state: #{inspect(reason)}")
          # Don't fail the restore - the counters in RocksDB will be used with safety margin
          :ok
      end
    else
      # No counter file in backup - legacy backup, use RocksDB counters
      Logger.debug("No counter state file in backup, using RocksDB counters")
      :ok
    end
  end

  defp restore_counter_state(_backup_path, _store) do
    :ok
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

    # Create timestamped backup name with milliseconds for uniqueness
    now = DateTime.utc_now()
    timestamp = Calendar.strftime(now, "%Y%m%d_%H%M%S")
    # Add milliseconds to ensure uniqueness for rapid backups
    ms = now.microsecond |> elem(0) |> div(1000)
    backup_name = "#{prefix}_#{timestamp}_#{String.pad_leading(to_string(ms), 3, "0")}"
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
    with :ok <- validate_path_safety(path) do
      if File.exists?(path) do
        {:error, :backup_path_exists}
      else
        :ok
      end
    end
  end

  # Validate path for security - no path traversal, no symlinks in components
  defp validate_path_safety(path) do
    expanded = Path.expand(path)

    cond do
      # Check for path traversal attempts
      String.contains?(path, "..") ->
        {:error, :path_traversal_attempt}

      # Check if expanded path differs significantly (indicates traversal)
      not paths_equivalent?(path, expanded) and String.contains?(path, "..") ->
        {:error, :path_traversal_attempt}

      true ->
        :ok
    end
  end

  # Check if two paths are equivalent (accounting for absolute vs relative)
  defp paths_equivalent?(path1, path2) do
    Path.expand(path1) == Path.expand(path2)
  end

  defp validate_restore_path(path, overwrite) do
    with :ok <- validate_path_safety(path) do
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
  end

  defp ensure_parent_exists(path) do
    parent = Path.dirname(path)

    case File.mkdir_p(parent) do
      :ok -> :ok
      {:error, reason} -> {:error, {:mkdir_failed, reason}}
    end
  end

  defp copy_directory(source, destination) do
    # Check for symlinks in source before copying
    case check_for_symlinks(source) do
      :ok ->
        case File.cp_r(source, destination) do
          {:ok, files} -> {:ok, files}
          {:error, reason, _file} -> {:error, {:copy_failed, reason}}
        end

      {:error, _} = error ->
        error
    end
  end

  # Check source directory for symlinks (security measure)
  defp check_for_symlinks(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:error, {:symlink_detected, path}}

      {:ok, %File.Stat{type: :directory}} ->
        # Check all entries in directory
        case File.ls(path) do
          {:ok, entries} ->
            Enum.reduce_while(entries, :ok, fn entry, :ok ->
              case check_for_symlinks(Path.join(path, entry)) do
                :ok -> {:cont, :ok}
                error -> {:halt, error}
              end
            end)

          {:error, reason} ->
            {:error, {:ls_failed, path, reason}}
        end

      {:ok, _} ->
        # Regular file
        :ok

      {:error, reason} ->
        {:error, {:lstat_failed, path, reason}}
    end
  end

  defp copy_incremental(source, destination, base_backup) do
    # Create destination directory
    case File.mkdir_p(destination) do
      :ok ->
        do_copy_incremental(source, destination, base_backup)

      {:error, reason} ->
        {:error, {:mkdir_failed, reason}}
    end
  end

  defp do_copy_incremental(source, destination, base_backup) do
    source_files = list_all_files_with_stats(source)
    base_files = list_all_files_with_stats(base_backup)

    # Build a map of base files for quick lookup
    base_map =
      base_files
      |> Enum.map(fn {path, stat} ->
        rel_path = Path.relative_to(path, base_backup)
        {rel_path, {path, stat}}
      end)
      |> Map.new()

    # Track statistics
    initial_stats = %{files_copied: 0, files_linked: 0, bytes_copied: 0}

    Enum.reduce_while(source_files, {:ok, initial_stats}, fn {src_path, src_stat},
                                                             {:ok, acc_stats} ->
      rel_path = Path.relative_to(src_path, source)
      dest_path = Path.join(destination, rel_path)
      File.mkdir_p!(Path.dirname(dest_path))

      copy_file_incremental(src_path, dest_path, src_stat, base_map[rel_path], acc_stats)
    end)
  end

  defp copy_file_incremental(src_path, dest_path, src_stat, base_entry, acc_stats) do
    case base_entry do
      {base_path, base_stat}
      when src_stat.size == base_stat.size and src_stat.mtime == base_stat.mtime ->
        # Size and mtime match - file is unchanged, use hard link
        link_or_copy_file(src_path, dest_path, src_stat, base_path, acc_stats)

      _ ->
        # File is new or changed - copy it
        copy_new_file(src_path, dest_path, src_stat, acc_stats)
    end
  end

  defp link_or_copy_file(src_path, dest_path, src_stat, base_path, acc_stats) do
    case File.ln(base_path, dest_path) do
      :ok ->
        {:cont, {:ok, %{acc_stats | files_linked: acc_stats.files_linked + 1}}}

      {:error, _} ->
        copy_new_file(src_path, dest_path, src_stat, acc_stats)
    end
  end

  defp copy_new_file(src_path, dest_path, src_stat, acc_stats) do
    case File.cp(src_path, dest_path) do
      :ok ->
        new_stats = %{
          acc_stats
          | files_copied: acc_stats.files_copied + 1,
            bytes_copied: acc_stats.bytes_copied + src_stat.size
        }

        {:cont, {:ok, new_stats}}

      {:error, reason} ->
        {:halt, {:error, {:copy_failed, reason, src_path}}}
    end
  end

  defp list_all_files_with_stats(path) do
    path
    |> list_all_files()
    |> Enum.map(fn file_path ->
      {file_path, File.stat!(file_path)}
    end)
  end

  defp write_metadata(backup_path, source_path, store) do
    metadata = %{
      source_path: source_path,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      version: "1.0",
      backup_type: :full
    }

    metadata_path = Path.join(backup_path, ".backup_metadata")
    content = :erlang.term_to_binary(metadata)

    with :ok <- File.write(metadata_path, content),
         :ok <- save_counter_state(backup_path, store) do
      :ok
    else
      {:error, reason} -> {:error, {:metadata_write_failed, reason}}
    end
  end

  # Save sequence counter state to backup directory
  defp save_counter_state(backup_path, %{dict_manager: dict_manager}) do
    counter_path = Path.join(backup_path, @counter_file)

    with {:ok, counter} <- Manager.get_counter(dict_manager),
         :ok <- SequenceCounter.export_to_file(counter, counter_path) do
      :ok
    else
      {:error, reason} ->
        Logger.warning("Failed to save counter state to backup: #{inspect(reason)}")
        # Don't fail the backup if counter export fails - the counters are still in RocksDB
        :ok
    end
  end

  defp save_counter_state(_backup_path, _store) do
    # Store doesn't have dict_manager (shouldn't happen but be safe)
    :ok
  end

  defp write_incremental_metadata(backup_path, source_path, base_backup, store) do
    # Also save counter state for incremental backups
    save_counter_state(backup_path, store)

    metadata = %{
      source_path: source_path,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      version: "1.0",
      backup_type: :incremental,
      base_backup: base_backup
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
      file_count: file_count,
      backup_type: :full
    }
  end

  defp build_incremental_metadata(backup_path, source_path, base_backup, stats) do
    {size, file_count} = directory_stats(backup_path)

    %{
      path: backup_path,
      source_path: source_path,
      created_at: DateTime.utc_now(),
      size_bytes: size,
      file_count: file_count,
      backup_type: :incremental,
      base_backup: base_backup,
      files_copied: stats.files_copied,
      files_linked: stats.files_linked,
      bytes_copied: stats.bytes_copied
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

    case File.ls(path) do
      {:ok, entries} ->
        Enum.all?(required_patterns, fn pattern ->
          Enum.any?(entries, &String.starts_with?(&1, pattern))
        end)

      {:error, reason} ->
        Logger.debug("Failed to list directory #{path}: #{inspect(reason)}")
        false
    end
  end

  defp read_backup_metadata(path) do
    metadata_path = Path.join(path, ".backup_metadata")

    try do
      if File.exists?(metadata_path) do
        read_metadata_from_file(path, metadata_path)
      else
        infer_metadata_from_directory(path)
      end
    rescue
      e in [File.Error, ArgumentError] ->
        Logger.debug("Failed to read backup metadata from #{path}: #{inspect(e)}")
        nil
    end
  end

  defp read_metadata_from_file(path, metadata_path) do
    case File.read(metadata_path) do
      {:ok, content} ->
        case safe_binary_to_term(content) do
          {:ok, stored} -> build_metadata_from_stored(path, stored)
          {:error, _} -> nil
        end

      {:error, _} ->
        nil
    end
  end

  # Safely decode binary to term - prevents arbitrary code execution
  defp safe_binary_to_term(content) do
    try do
      {:ok, :erlang.binary_to_term(content, [:safe])}
    rescue
      ArgumentError -> {:error, :invalid_format}
    end
  end

  defp build_metadata_from_stored(path, stored) do
    {size, file_count} = directory_stats(path)

    base_meta = %{
      path: path,
      source_path: stored[:source_path],
      created_at: parse_datetime(stored[:created_at]),
      size_bytes: size,
      file_count: file_count,
      backup_type: stored[:backup_type] || :full
    }

    if stored[:backup_type] == :incremental do
      Map.put(base_meta, :base_backup, stored[:base_backup])
    else
      base_meta
    end
  end

  defp infer_metadata_from_directory(path) do
    {size, file_count} = directory_stats(path)

    %{
      path: path,
      source_path: nil,
      created_at: infer_creation_time(path),
      size_bytes: size,
      file_count: file_count,
      backup_type: :full
    }
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
