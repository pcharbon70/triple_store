defmodule TripleStore.Backend.RocksDB.NIF do
  @moduledoc """
  NIF bindings for RocksDB operations.

  This module is being migrated from Rust NIF to erlang-rocksdb.
  Currently stubbed - will be replaced by erlang-rocksdb adapter in Task 1.2.

  ## Column Families

  The database uses the following column families:
  - `:id2str` - Maps 64-bit IDs to string values
  - `:str2id` - Maps string values to 64-bit IDs
  - `:spo` - Subject-Predicate-Object index
  - `:pos` - Predicate-Object-Subject index
  - `:osp` - Object-Subject-Predicate index
  - `:derived` - Stores inferred triples from reasoning
  - `:numeric_range` - Stores numeric range indices for efficient range queries
  """

  @type db_ref :: reference()
  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived | :numeric_range
  @type iterator_ref :: reference()
  @type snapshot_ref :: reference()
  @type snapshot_iterator_ref :: reference()

  @type put_operation :: {column_family(), binary(), binary()}
  @type delete_operation :: {column_family(), binary()}
  @type mixed_put :: {:put, column_family(), binary(), binary()}
  @type mixed_delete :: {:delete, column_family(), binary()}

  @doc """
  Error indicating the NIF has not been migrated yet.
  """
  @spec nif_loaded :: String.t()
  def nif_loaded, do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec open(String.t()) :: {:ok, db_ref()} | {:error, term()}
  def open(_path), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec close(db_ref()) :: :ok | {:error, term()}
  def close(_db_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec flush_wal(db_ref(), boolean()) :: :ok | {:error, term()}
  def flush_wal(_db_ref, _sync), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec set_options(db_ref(), [{String.t(), String.t()}]) :: :ok | {:error, term()}
  def set_options(_db_ref, _options), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec get_path(db_ref()) :: {:ok, String.t()}
  def get_path(_db_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec list_column_families :: [column_family()]
  def list_column_families, do: [:id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range]

  @spec is_open(db_ref()) :: boolean()
  def is_open(_db_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec get(db_ref(), column_family(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
  def get(_db_ref, _cf, _key), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec put(db_ref(), column_family(), binary(), binary()) :: :ok | {:error, term()}
  def put(_db_ref, _cf, _key, _value), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec delete(db_ref(), column_family(), binary()) :: :ok | {:error, term()}
  def delete(_db_ref, _cf, _key), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec exists(db_ref(), column_family(), binary()) :: {:ok, boolean()} | {:error, term()}
  def exists(_db_ref, _cf, _key), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec write_batch(db_ref(), [put_operation()], boolean()) :: :ok | {:error, term()}
  def write_batch(_db_ref, _operations, _sync),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec delete_batch(db_ref(), [delete_operation()], boolean()) :: :ok | {:error, term()}
  def delete_batch(_db_ref, _operations, _sync),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec mixed_batch(db_ref(), [mixed_put() | mixed_delete()], boolean()) :: :ok | {:error, term()}
  def mixed_batch(_db_ref, _operations, _sync),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec prefix_iterator(db_ref(), column_family(), binary()) :: {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(_db_ref, _cf, _prefix),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec iterator_next(iterator_ref()) :: {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def iterator_next(_iter_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec iterator_seek(iterator_ref(), binary()) :: :ok | {:error, term()}
  def iterator_seek(_iter_ref, _target),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec iterator_close(iterator_ref()) :: :ok | {:error, term()}
  def iterator_close(_iter_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec iterator_collect(iterator_ref()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def iterator_collect(_iter_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec prefix_stream(db_ref(), column_family(), binary()) :: {:ok, Enumerable.t()} | {:error, term()}
  def prefix_stream(_db_ref, _cf, _prefix),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot(db_ref()) :: {:ok, snapshot_ref()} | {:error, term()}
  def snapshot(_db_ref), do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_get(snapshot_ref(), column_family(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
  def snapshot_get(_snapshot_ref, _cf, _key),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_prefix_iterator(snapshot_ref(), column_family(), binary()) ::
          {:ok, snapshot_iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(_snapshot_ref, _cf, _prefix),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_iterator_next(snapshot_iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def snapshot_iterator_next(_iter_ref),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_iterator_close(snapshot_iterator_ref()) :: :ok | {:error, term()}
  def snapshot_iterator_close(_iter_ref),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_iterator_collect(snapshot_iterator_ref()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def snapshot_iterator_collect(_iter_ref),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec release_snapshot(snapshot_ref()) :: :ok | {:error, term()}
  def release_snapshot(_snapshot_ref),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")

  @spec snapshot_stream(snapshot_ref(), column_family(), binary()) :: {:ok, Enumerable.t()} | {:error, term()}
  def snapshot_stream(_snapshot_ref, _cf, _prefix),
    do: raise("NIF not migrated - erlang-rocksdb adapter not yet implemented")
end
