defmodule TripleStore.Backend.RocksDB.Iterator do
  @moduledoc """
  Iterator wrapper process for erlang-rocksdb iterators.

  This module provides a GenServer-based wrapper around erlang-rocksdb iterators
  to ensure proper resource cleanup. Each iterator is managed by a dedicated process
  that closes the underlying erlang-rocksdb iterator when the process terminates.

  ## Architecture

  The iterator process holds:
  - The erlang-rocksdb iterator reference (mutable resource)
  - The database reference (for validation)
  - Optional prefix for boundary checking
  - Optional snapshot reference

  The `iterator_ref` type in the NIF API is the PID of this process.

  ## Lifecycle

  1. Created via `start_link/3`
  2. Used through `next/1`, `move/2`, `seek/2` calls
  3. Closed via `close/1` or automatically on process exit

  ## Read Options

  Supports the following read options:
  - `fill_cache` - Whether to fill block cache (default: true)
  - `total_order_seek` - Use total order seek (default: false)
  - `prefix_same_as_start` - Optimize for prefix iteration (default: false)
  - `snapshot` - Use a specific snapshot (placeholder for Section 2.2)

  """

  use GenServer
  require Logger

  @type iterator_ref :: reference()
  @type snapshot_ref :: reference() | pid()
  @type move_action :: :first | :last | :next | :prev | binary()

  @type state :: %{
          iter_ref: iterator_ref() | nil,
          db_ref: reference(),
          cf_handle: reference(),
          prefix: binary() | nil,
          snapshot: snapshot_ref() | nil,
          last_seek: binary() | nil,
          exhausted: boolean(),
          positioned: boolean()
        }

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts a new iterator process for a column family.

  ## Parameters

  - `db_ref` - The erlang-rocksdb database reference
  - `cf_handle` - The column family handle
  - `opts` - Optional read options

  ## Options

  - `fill_cache` - Whether to fill block cache (default: true)
  - `total_order_seek` - Use total order seek (default: false)
  - `prefix_same_as_start` - Optimize for prefix iteration (default: false)
  - `snapshot` - Use a specific snapshot (placeholder, Section 2.2)
  - `prefix` - Track prefix for boundary checking (optional)

  ## Returns

  - `{:ok, pid}` - Iterator process started successfully
  - `{:error, reason}` - Failed to start iterator

  """
  @spec start_link(reference(), reference(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(db_ref, cf_handle, opts \\ []) do
    GenServer.start_link(__MODULE__, {db_ref, cf_handle, opts})
  end

  @doc """
  Moves the iterator and returns the next entry.

  ## Parameters

  - `iter_pid` - The iterator process PID
  - `action` - Movement action (:first, :last, :next, :prev, or binary seek key)

  ## Returns

  - `{:ok, key, value}` - Entry found
  - `:iterator_end` - Iterator exhausted
  - `{:error, reason}` - Error occurred

  """
  @spec move(pid(), move_action()) :: {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def move(iter_pid, action) when is_pid(iter_pid) do
    GenServer.call(iter_pid, {:move, action})
  end

  @doc """
  Seeks the iterator to a specific key.

  ## Parameters

  - `iter_pid` - The iterator process PID
  - `seek_key` - Binary key to seek to

  ## Returns

  - `:ok` - Seek successful (caller should call next/1 to get entry)
  - `{:error, reason}` - Error occurred

  """
  @spec seek(pid(), binary()) :: :ok | {:error, term()}
  def seek(iter_pid, seek_key) when is_pid(iter_pid) and is_binary(seek_key) do
    GenServer.call(iter_pid, {:seek, seek_key})
  end

  @doc """
  Gets the next entry from the iterator (equivalent to move(:next)).

  ## Parameters

  - `iter_pid` - The iterator process PID

  ## Returns

  - `{:ok, key, value}` - Entry found
  - `:iterator_end` - Iterator exhausted
  - `{:error, reason}` - Error occurred

  """
  @spec next(pid()) :: {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def next(iter_pid) when is_pid(iter_pid) do
    GenServer.call(iter_pid, :next)
  end

  @doc """
  Closes the iterator and releases resources.

  ## Parameters

  - `iter_pid` - The iterator process PID

  ## Returns

  - `:ok`

  """
  @spec close(pid()) :: :ok
  def close(iter_pid) when is_pid(iter_pid) do
    if Process.alive?(iter_pid) do
      GenServer.stop(iter_pid, :normal, 5000)
    end

    :ok
  end

  @doc """
  Collects all remaining entries from the iterator.

  ## Parameters

  - `iter_pid` - The iterator process PID
  - `opts` - Options
    - `max_entries` - Maximum entries to collect (default: unlimited)

  ## Returns

  - `{:ok, [{key, value}]}` - List of entries
  - `{:error, reason}` - Error occurred

  """
  @spec collect(pid(), keyword()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def collect(iter_pid, opts \\ []) when is_pid(iter_pid) do
    max_entries = Keyword.get(opts, :max_entries, :infinity)

    case GenServer.call(iter_pid, :collect) do
      {:ok, entries} when is_list(entries) ->
        limited_entries =
          case max_entries do
            :infinity -> entries
            count -> Enum.take(entries, count)
          end

        {:ok, limited_entries}

      error ->
        error
    end
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init({db_ref, cf_handle, opts}) do
    # Build read options for erlang-rocksdb
    read_opts = build_read_opts(opts)

    # Create the erlang-rocksdb iterator
    case :rocksdb.iterator(db_ref, cf_handle, read_opts) do
      {:ok, iter_ref} ->
        state = %{
          iter_ref: iter_ref,
          db_ref: db_ref,
          cf_handle: cf_handle,
          prefix: Keyword.get(opts, :prefix),
          snapshot: Keyword.get(opts, :snapshot),
          last_seek: nil,
          exhausted: false,
          positioned: false
        }

        # Note: erlang-rocksdb db_ref is a reference, not a PID, so we don't monitor it
        {:ok, Map.put(state, :db_monitor_ref, nil)}

      {:error, reason} ->
        # Stop the GenServer if iterator creation fails
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(
        {:move, action},
        _from,
        %{iter_ref: iter_ref, prefix: prefix, exhausted: exhausted} = state
      ) do
    if exhausted do
      {:reply, :iterator_end, state}
    else
      iter_ref
      |> :rocksdb.iterator_move(action)
      |> reply_for_iterator_result(
        prefix,
        %{state | positioned: true},
        %{state | exhausted: true, positioned: true},
        state
      )
    end
  end

  @impl true
  def handle_call({:seek, seek_key}, _from, state) do
    # Store the seek key to be used on the next iterator_next call
    # This matches the original NIF API where seek returns :ok and next returns the entry
    {:reply, :ok, %{state | last_seek: seek_key, exhausted: false}}
  end

  @impl true
  def handle_call(
        :next,
        _from,
        %{
          iter_ref: iter_ref,
          prefix: prefix,
          last_seek: last_seek,
          exhausted: exhausted,
          positioned: positioned
        } = state
      ) do
    handle_next_call(state, iter_ref, prefix, last_seek, exhausted, positioned)
  end

  @impl true
  def handle_call(
        :collect,
        _from,
        %{iter_ref: iter_ref, prefix: prefix, last_seek: last_seek} = state
      ) do
    # If there's a pending seek, use it as the starting position
    start_key = last_seek || prefix || :first

    entries = collect_with_position(iter_ref, prefix, start_key, [])
    {:reply, {:ok, entries}, %{state | last_seek: nil, exhausted: true}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # Database process died, close iterator
    {:stop, :database_closed, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{iter_ref: iter_ref}) when iter_ref != nil do
    :rocksdb.iterator_close(iter_ref)
    :ok
  end

  def terminate(_reason, _state) do
    :ok
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  defp handle_next_call(state, _iter_ref, _prefix, _last_seek, true, _positioned) do
    {:reply, :iterator_end, state}
  end

  defp handle_next_call(state, iter_ref, prefix, last_seek, false, _positioned)
       when not is_nil(last_seek) do
    iter_ref
    |> :rocksdb.iterator_move(last_seek)
    |> reply_for_iterator_result(
      prefix,
      %{state | last_seek: nil, positioned: true},
      %{state | last_seek: nil, exhausted: true, positioned: true},
      %{state | last_seek: nil}
    )
  end

  defp handle_next_call(state, iter_ref, prefix, nil, false, false) do
    start_key = prefix || :first

    iter_ref
    |> :rocksdb.iterator_move(start_key)
    |> reply_for_iterator_result(
      prefix,
      %{state | positioned: true},
      %{state | exhausted: true, positioned: true},
      state
    )
  end

  defp handle_next_call(state, iter_ref, prefix, nil, false, true) do
    iter_ref
    |> :rocksdb.iterator_move(:next)
    |> reply_for_iterator_result(prefix, state, %{state | exhausted: true}, state)
  end

  defp reply_for_iterator_result(result, prefix, success_state, exhausted_state, error_state) do
    cond do
      match?({:ok, _, _}, result) ->
        reply_for_prefix_boundary(result, prefix, success_state, exhausted_state)

      result in [:iterator_end, {:error, :invalid_iterator}] ->
        {:reply, :iterator_end, exhausted_state}

      true ->
        {:reply, result, error_state}
    end
  end

  defp reply_for_prefix_boundary(
         {:ok, key, _value} = result,
         prefix,
         success_state,
         exhausted_state
       ) do
    if prefix && !has_prefix?(key, prefix) do
      {:reply, :iterator_end, exhausted_state}
    else
      {:reply, result, success_state}
    end
  end

  # Builds read options for erlang-rocksdb iterator
  defp build_read_opts(opts) do
    read_opts = []

    # Fill cache option
    read_opts =
      case Keyword.get(opts, :fill_cache, true) do
        true -> read_opts
        false -> [:fill_cache_false | read_opts]
      end

    # Total order seek option
    read_opts =
      case Keyword.get(opts, :total_order_seek, false) do
        true -> read_opts
        false -> [:total_order_seek_false | read_opts]
      end

    # Prefix same as start option
    read_opts =
      case Keyword.get(opts, :prefix_same_as_start, false) do
        true -> read_opts
        false -> [:prefix_same_as_start_false | read_opts]
      end

    # Snapshot option - pass through to erlang-rocksdb read options
    read_opts =
      case Keyword.get(opts, :snapshot) do
        nil -> read_opts
        snapshot_ref -> [{:snapshot, snapshot_ref} | read_opts]
      end

    read_opts
  end

  # Collects all remaining entries from an iterator with positioning
  defp collect_with_position(iter_ref, prefix, start_key, acc) do
    # First, position the iterator at the start
    case :rocksdb.iterator_move(iter_ref, start_key) do
      {:ok, key, value} ->
        # Check prefix boundary if set
        if prefix && !has_prefix?(key, prefix) do
          # First entry is past prefix boundary
          Enum.reverse(acc)
        else
          # Continue collecting from here
          collect_continuing(iter_ref, prefix, [{key, value} | acc])
        end

      _other ->
        Enum.reverse(acc)
    end
  end

  # Continues collecting entries after initial position
  defp collect_continuing(iter_ref, prefix, acc) do
    case :rocksdb.iterator_move(iter_ref, :next) do
      {:ok, key, value} ->
        # Check prefix boundary if set
        if prefix && !has_prefix?(key, prefix) do
          # Reached end of prefix
          Enum.reverse(acc)
        else
          collect_continuing(iter_ref, prefix, [{key, value} | acc])
        end

      _other ->
        Enum.reverse(acc)
    end
  end

  # Checks if a key has the given prefix, safely handling keys shorter than the prefix
  defp has_prefix?(key, prefix) when is_binary(prefix) and byte_size(prefix) > 0 do
    byte_size(key) >= byte_size(prefix) and
      binary_part(key, 0, byte_size(prefix)) == prefix
  end

  defp has_prefix?(_key, _prefix), do: true
end
