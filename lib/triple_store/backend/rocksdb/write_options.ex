defmodule TripleStore.Backend.RocksDB.WriteOptions do
  @moduledoc """
  Write options presets for different write patterns.

  This module provides optimized write option configurations for various
  write patterns used by the TripleStore. Using appropriate write options
  allows tuning the durability vs throughput trade-off.

  ## Write Option Presets

  | Preset | Use Case | Durability | Description |
  |--------|----------|------------|-------------|
  | `default/0` | General writes | WAL only | Balanced settings |
  | `sync/0` | Critical data | Full sync | Maximum durability |
  | `async/0` | Bulk loads | WAL only | Maximum throughput |
  | `disable_wal/0` | Temporary data | None | No logging (use with caution) |
  | `bulk_load/0` | Large imports | WAL only | Optimized for throughput |

  ## Durability vs Throughput Trade-off

  1. **`sync/0`** - Maximum durability
     - WAL is synced to disk on every write
     - Slowest but safest
     - Use for: critical user data, transaction commits

  2. **`default/0` / `async/0`** - Balanced
     - WAL enabled but not synced every write
     - Good performance with reasonable durability
     - Use for: most application writes

  3. **`disable_wal/0`** - Maximum throughput (dangerous)
     - No write-ahead logging
     - Fast but data loss on crash
     - Use for: temporary data, rebuildable caches

  ## Usage

  ```elixir
  # For critical writes that must persist
  ErlangAdapter.put(db, :spo, key, value, WriteOptions.sync())

  # For bulk loading where performance matters more than immediate sync
  ErlangAdapter.write_batch(db, operations, WriteOptions.bulk_load())

  # For temporary derived data that can be rebuilt
  ErlangAdapter.put(db, :derived, key, value, WriteOptions.disable_wal())
  ```

  ## Option Details

  ### `sync`
  - `true`: Sync WAL to disk after write (slower, safer)
  - `false`: Write to WAL buffer only (faster, small window of data loss)

  ### `disable_wal`
  - `true`: No write-ahead log (fastest, data loss on crash)
  - `false`: Normal WAL operation (default)
  - **Warning**: Use only for data that can be rebuilt

  ### `no_slowdown`
  - `true`: Fail writes immediately if writes are slowed
  - `false`: Wait for writes to complete (default)

  ### `low_pri`
  - `true`: Write is low priority (background compaction)
  - `false`: Normal priority write (default)
  """

  @type write_option ::
          {:sync, boolean()}
          | {:disable_wal, boolean()}
          | {:no_slowdown, boolean()}
          | {:low_pri, boolean()}
          | {:timeout, pos_integer()}

  @type write_options :: [write_option()]

  # ===========================================================================
  # Public API - Presets
  # ===========================================================================

  @doc """
  Default write options for most writes.

  Balanced settings with WAL enabled but not synced on every write.
  Provides good performance with reasonable durability (small window of
  potential data loss on crash, typically < 1 second).

  ## Returns

  Write options keyword list.

  ## Examples

      iex> WriteOptions.default()
      [sync: false, disable_wal: false]

  """
  @spec default() :: write_options()
  def default do
    [
      sync: false,
      disable_wal: false
    ]
  end

  @doc """
  Write options for maximum durability.

  Syncs the WAL to disk on every write. Slowest but provides the
  strongest guarantee that data will persist after a crash.

  Use for:
  - Critical user data
  - Transaction commits
  - Data that cannot be easily reconstructed

  ## Returns

  Write options keyword list.

  ## Examples

      iex> WriteOptions.sync()
      [sync: true, disable_wal: false]

  """
  @spec sync() :: write_options()
  def sync do
    [
      sync: true,
      disable_wal: false
    ]
  end

  @doc """
  Write options for maximum throughput.

  WAL is enabled but not synced, providing good performance with
  reasonable durability. Similar to `default/0` but semantically
  indicates this is for high-throughput scenarios.

  Use for:
  - Bulk imports
  - Batch operations
  - Non-critical data

  ## Returns

  Write options keyword list.

  ## Examples

      iex> WriteOptions.async()
      [sync: false, disable_wal: false]

  """
  @spec async() :: write_options()
  def async do
    [
      sync: false,
      disable_wal: false
    ]
  end

  @doc """
  Write options that disable the write-ahead log.

  **WARNING**: This option is dangerous! If the process crashes before
  data is flushed to SST files, all writes since the last flush will be
  lost.

  Only use for:
  - Temporary derived data that can be rebuilt
  - Caches that can be reconstructed
  - Testing/benchmarking

  ## Returns

  Write options keyword list.

  ## Examples

      iex> WriteOptions.disable_wal()
      [sync: false, disable_wal: true]

  """
  @spec disable_wal() :: write_options()
  def disable_wal do
    [
      sync: false,
      disable_wal: true
    ]
  end

  @doc """
  Write options optimized for bulk loading.

  Combines async writes with additional optimizations for high-volume
  data loading scenarios.

  Use for:
  - Initial database population
  - Large batch imports
  - Bulk reasoning operations

  ## Returns

  Write options keyword list.

  ## Examples

      iex> opts = WriteOptions.bulk_load()
      iex> Keyword.get(opts, :sync)
      false

  """
  @spec bulk_load() :: write_options()
  def bulk_load do
    [
      sync: false,
      disable_wal: false,
      # Don't slow down writes for compaction
      no_slowdown: false,
      # Normal priority
      low_pri: false
    ]
  end

  # ===========================================================================
  # Public API - Builders
  # ===========================================================================

  @doc """
  Creates write options with custom sync behavior.

  ## Parameters

  - `sync`: Boolean indicating if WAL should be synced

  ## Returns

  Write options keyword list.

  ## Examples

      iex> opts = WriteOptions.with_sync(true)
      iex> Keyword.get(opts, :sync)
      true

  """
  @spec with_sync(boolean()) :: write_options()
  def with_sync(sync) when is_boolean(sync) do
    [
      sync: sync,
      disable_wal: false
    ]
  end

  @doc """
  Creates write options with a timeout.

  ## Parameters

  - `timeout_ms`: Timeout in milliseconds

  ## Returns

  Write options keyword list with timeout set.

  ## Examples

      iex> opts = WriteOptions.with_timeout(5000)
      iex> Keyword.get(opts, :timeout)
      5000

  """
  @spec with_timeout(pos_integer()) :: write_options()
  def with_timeout(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    [
      sync: false,
      disable_wal: false,
      timeout: timeout_ms
    ]
  end

  @doc """
  Merges custom options into a preset.

  ## Parameters

  - `preset`: Base preset (function from this module)
  - `custom_opts`: Custom options to override/add

  ## Returns

  Combined write options keyword list.

  ## Examples

      iex> opts = WriteOptions.merge(WriteOptions.async(), low_pri: true)
      iex> Keyword.get(opts, :low_pri)
      true

  """
  @spec merge(write_options(), keyword()) :: write_options()
  def merge(preset, custom_opts) when is_list(preset) and is_list(custom_opts) do
    Keyword.merge(preset, custom_opts)
  end

  # ===========================================================================
  # Public API - Utilities
  # ===========================================================================

  @doc """
  Determines if writes should be synced for a given operation type.

  ## Parameters

  - `operation_type`: Atom indicating the type of operation

  ## Returns

  Boolean indicating if sync should be used.

  ## Examples

      iex> WriteOptions.use_sync?(:transaction_commit)
      true

      iex> WriteOptions.use_sync?(:bulk_import)
      false

  """
  @spec use_sync?(atom()) :: boolean()
  def use_sync?(:transaction_commit), do: true
  def use_sync?(:critical_write), do: true
  def use_sync?(:user_data), do: true
  def use_sync?(:bulk_import), do: false
  def use_sync?(:derived_data), do: false
  def use_sync?(:temp_data), do: false
  def use_sync?(:maintenance), do: false
  def use_sync?(_), do: false

  @doc """
  Determines if WAL should be disabled for a given operation type.

  ## Parameters

  - `operation_type`: Atom indicating the type of operation

  ## Returns

  Boolean indicating if WAL should be disabled.

  ## Examples

      iex> WriteOptions.disable_wal?(:rebuildable_cache)
      true

      iex> WriteOptions.disable_wal?(:user_data)
      false

  """
  @spec disable_wal?(atom()) :: boolean()
  def disable_wal?(:rebuildable_cache), do: true
  def disable_wal?(:temp_derived), do: true
  def disable_wal?(:benchmark), do: true
  def disable_wal?(:user_data), do: false
  def disable_wal?(:critical), do: false
  def disable_wal?(_), do: false

  @doc """
  Gets the appropriate write preset for a column family.

  ## Parameters

  - `cf`: Column family atom

  ## Returns

  Write options keyword list optimized for the column family.

  ## Examples

      iex> opts = WriteOptions.for_cf(:spo)
      iex> Keyword.get(opts, :sync)
      false

      iex> opts = WriteOptions.for_cf(:derived)
      iex> Keyword.get(opts, :disable_wal)
      false

  """
  @spec for_cf(TripleStore.Backend.RocksDB.ColumnFamilyConfig.column_family()) ::
          write_options()
  def for_cf(:spo), do: default()
  def for_cf(:pos), do: default()
  def for_cf(:osp), do: default()
  def for_cf(:id2str), do: sync()
  def for_cf(:str2id), do: sync()
  def for_cf(:derived), do: async()
  def for_cf(:numeric_range), do: default()
  def for_cf(:default), do: default()

  @doc """
  Gets write options with the appropriate sync setting for a transaction.

  ## Parameters

  - `is_commit`: Whether this is a commit operation

  ## Returns

  Write options with sync enabled for commits.

  ## Examples

      iex> opts = WriteOptions.for_transaction(true)
      iex> Keyword.get(opts, :sync)
      true

      iex> opts = WriteOptions.for_transaction(false)
      iex> Keyword.get(opts, :sync)
      false

  """
  @spec for_transaction(boolean()) :: write_options()
  def for_transaction(true), do: sync()
  def for_transaction(false), do: async()
end
