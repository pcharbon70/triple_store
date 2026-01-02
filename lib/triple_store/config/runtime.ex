defmodule TripleStore.Config.Runtime do
  @moduledoc """
  Runtime configuration changes for RocksDB.

  This module provides functions to dynamically modify RocksDB settings
  on an open database without restarting. This is useful for:

  - Optimizing for bulk loading before large imports
  - Restoring normal settings after bulk operations
  - Tuning compaction behavior based on workload

  ## Mutable Options

  Only certain RocksDB options can be changed at runtime. The key ones are:

  - `level0_file_num_compaction_trigger` - Files in L0 to trigger compaction
  - `level0_slowdown_writes_trigger` - Files in L0 to slow down writes
  - `level0_stop_writes_trigger` - Files in L0 to stop writes
  - `target_file_size_base` - Target file size in bytes
  - `max_bytes_for_level_base` - Maximum bytes in base level
  - `write_buffer_size` - Size of write buffer (for new memtables)
  - `max_write_buffer_number` - Maximum number of write buffers
  - `disable_auto_compactions` - Enable/disable automatic compactions

  ## Usage Example

      # Before bulk loading
      {:ok, original} = Runtime.prepare_for_bulk_load(db)

      # Perform bulk load...
      Loader.load_file(store, "large_dataset.nt", bulk_mode: true)

      # Restore original settings
      :ok = Runtime.restore_config(db, original)

  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Config.Compaction

  @typedoc "RocksDB database reference"
  @type db_ref :: reference()

  @typedoc "Runtime options as key-value string pairs"
  @type options :: [{String.t(), String.t()}]

  @typedoc "Saved configuration state for restoration"
  @type saved_config :: %{
          options: options(),
          preset: atom() | nil
        }

  @doc """
  Prepares the database for bulk loading by setting aggressive options.

  This function:
  1. Raises L0 compaction triggers to minimize compaction during import
  2. Optionally disables auto compaction entirely
  3. Returns the original settings for later restoration

  ## Arguments
  - `db_ref` - The database reference
  - `opts` - Options
    - `:disable_compaction` - When `true`, completely disables auto compaction (default: `false`)

  ## Returns
  - `{:ok, saved_config}` - Original settings that can be passed to `restore_config/2`
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, saved} = Runtime.prepare_for_bulk_load(db)
      # ... perform bulk load ...
      :ok = Runtime.restore_config(db, saved)

      # Or with auto compaction disabled
      {:ok, saved} = Runtime.prepare_for_bulk_load(db, disable_compaction: true)

  """
  @spec prepare_for_bulk_load(db_ref(), keyword()) :: {:ok, saved_config()} | {:error, term()}
  def prepare_for_bulk_load(db_ref, opts \\ []) do
    disable_compaction = Keyword.get(opts, :disable_compaction, false)

    # Get bulk load compaction preset for values
    bulk_config = Compaction.preset(:bulk_load)

    # Build options to apply
    bulk_options =
      [
        {"level0_file_num_compaction_trigger",
         Integer.to_string(bulk_config.level0_file_num_compaction_trigger)},
        {"level0_slowdown_writes_trigger",
         Integer.to_string(bulk_config.level0_slowdown_writes_trigger)},
        {"level0_stop_writes_trigger",
         Integer.to_string(bulk_config.level0_stop_writes_trigger)},
        {"max_bytes_for_level_base",
         Integer.to_string(bulk_config.max_bytes_for_level_base)},
        {"target_file_size_base",
         Integer.to_string(bulk_config.target_file_size_base)}
      ]

    bulk_options =
      if disable_compaction do
        [{"disable_auto_compactions", "true"} | bulk_options]
      else
        bulk_options
      end

    # Save original configuration for restoration
    # We save the default settings since we don't have a way to query current values
    default_config = Compaction.preset(:default)

    original_options =
      [
        {"level0_file_num_compaction_trigger",
         Integer.to_string(default_config.level0_file_num_compaction_trigger)},
        {"level0_slowdown_writes_trigger",
         Integer.to_string(default_config.level0_slowdown_writes_trigger)},
        {"level0_stop_writes_trigger",
         Integer.to_string(default_config.level0_stop_writes_trigger)},
        {"max_bytes_for_level_base",
         Integer.to_string(default_config.max_bytes_for_level_base)},
        {"target_file_size_base",
         Integer.to_string(default_config.target_file_size_base)}
      ]

    original_options =
      if disable_compaction do
        [{"disable_auto_compactions", "false"} | original_options]
      else
        original_options
      end

    saved_config = %{
      options: original_options,
      preset: :default
    }

    case NIF.set_options(db_ref, bulk_options) do
      :ok ->
        {:ok, saved_config}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Restores database configuration to previously saved settings.

  ## Arguments
  - `db_ref` - The database reference
  - `saved_config` - The configuration returned by `prepare_for_bulk_load/2`

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      {:ok, saved} = Runtime.prepare_for_bulk_load(db)
      # ... bulk load operations ...
      :ok = Runtime.restore_config(db, saved)

  """
  @spec restore_config(db_ref(), saved_config()) :: :ok | {:error, term()}
  def restore_config(db_ref, %{options: options}) do
    NIF.set_options(db_ref, options)
  end

  @doc """
  Restores database to default configuration.

  Uses the `:default` compaction preset values.

  ## Arguments
  - `db_ref` - The database reference

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec restore_normal_config(db_ref()) :: :ok | {:error, term()}
  def restore_normal_config(db_ref) do
    default_config = Compaction.preset(:default)

    options = [
      {"level0_file_num_compaction_trigger",
       Integer.to_string(default_config.level0_file_num_compaction_trigger)},
      {"level0_slowdown_writes_trigger",
       Integer.to_string(default_config.level0_slowdown_writes_trigger)},
      {"level0_stop_writes_trigger",
       Integer.to_string(default_config.level0_stop_writes_trigger)},
      {"max_bytes_for_level_base",
       Integer.to_string(default_config.max_bytes_for_level_base)},
      {"target_file_size_base",
       Integer.to_string(default_config.target_file_size_base)},
      {"disable_auto_compactions", "false"}
    ]

    NIF.set_options(db_ref, options)
  end

  @doc """
  Applies a named configuration preset at runtime.

  ## Arguments
  - `db_ref` - The database reference
  - `preset` - The preset name (`:default`, `:write_heavy`, `:read_heavy`, `:balanced`, `:bulk_load`)

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      # Switch to bulk load mode
      :ok = Runtime.apply_preset(db, :bulk_load)

      # Switch back to default
      :ok = Runtime.apply_preset(db, :default)

  """
  @spec apply_preset(db_ref(), Compaction.preset_name()) :: :ok | {:error, term()}
  def apply_preset(db_ref, preset) do
    config = Compaction.preset(preset)

    options = [
      {"level0_file_num_compaction_trigger",
       Integer.to_string(config.level0_file_num_compaction_trigger)},
      {"level0_slowdown_writes_trigger",
       Integer.to_string(config.level0_slowdown_writes_trigger)},
      {"level0_stop_writes_trigger",
       Integer.to_string(config.level0_stop_writes_trigger)},
      {"max_bytes_for_level_base",
       Integer.to_string(config.max_bytes_for_level_base)},
      {"target_file_size_base",
       Integer.to_string(config.target_file_size_base)}
    ]

    NIF.set_options(db_ref, options)
  end

  @doc """
  Sets individual options on the database.

  This is a thin wrapper around the NIF that accepts an option keyword list
  with atom keys and integer/boolean values, converting them to strings.

  ## Arguments
  - `db_ref` - The database reference
  - `opts` - Keyword list of options to set

  ## Options
  - `:level0_file_num_compaction_trigger` - Integer
  - `:level0_slowdown_writes_trigger` - Integer
  - `:level0_stop_writes_trigger` - Integer
  - `:target_file_size_base` - Integer (bytes)
  - `:max_bytes_for_level_base` - Integer (bytes)
  - `:write_buffer_size` - Integer (bytes)
  - `:max_write_buffer_number` - Integer
  - `:disable_auto_compactions` - Boolean

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = Runtime.set_options(db,
        level0_file_num_compaction_trigger: 16,
        disable_auto_compactions: true
      )

  """
  @spec set_options(db_ref(), keyword()) :: :ok | {:error, term()}
  def set_options(db_ref, opts) do
    string_opts =
      Enum.map(opts, fn {key, value} ->
        str_key = Atom.to_string(key)

        str_value =
          case value do
            v when is_integer(v) -> Integer.to_string(v)
            true -> "true"
            false -> "false"
            v when is_binary(v) -> v
          end

        {str_key, str_value}
      end)

    NIF.set_options(db_ref, string_opts)
  end

  @doc """
  Executes a function with bulk load configuration, automatically restoring on completion or error.

  This is the recommended way to use bulk load configuration as it ensures
  settings are always restored, even if the operation fails or raises an exception.

  ## Arguments
  - `db_ref` - The database reference
  - `opts` - Options passed to `prepare_for_bulk_load/2`
  - `fun` - Function to execute with bulk load configuration. Receives `db_ref` as argument.

  ## Returns
  - `{:ok, result}` - The result of `fun` if successful
  - `{:error, reason}` - If `fun` returns an error tuple
  - Raises if `fun` raises (after restoring configuration)

  ## Examples

      # Safe bulk load with automatic restoration
      {:ok, count} = Runtime.with_bulk_config(db, [], fn _db ->
        Loader.load_file(store, "large_dataset.nt", bulk_mode: true)
      end)

      # With compaction disabled
      {:ok, count} = Runtime.with_bulk_config(db, [disable_compaction: true], fn _db ->
        Loader.load_file(store, "huge_dataset.nt", bulk_mode: true)
      end)

  """
  @spec with_bulk_config(db_ref(), keyword(), (db_ref() -> result)) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def with_bulk_config(db_ref, opts \\ [], fun) when is_function(fun, 1) do
    case prepare_for_bulk_load(db_ref, opts) do
      {:ok, saved_config} ->
        try do
          result = fun.(db_ref)
          # Always restore configuration
          restore_config(db_ref, saved_config)
          {:ok, result}
        rescue
          error ->
            # Attempt to restore on error, but don't mask the original error
            restore_config(db_ref, saved_config)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            # Attempt to restore on throw/exit
            restore_config(db_ref, saved_config)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      {:error, reason} ->
        {:error, {:prepare_failed, reason}}
    end
  end
end
