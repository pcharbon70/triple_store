defmodule TripleStore.Dictionary.SequenceCounter do
  @moduledoc """
  Atomic sequence counter for generating unique term IDs.

  Uses `:atomics` for lock-free increments with periodic persistence to RocksDB.
  This ensures high throughput ID generation while maintaining durability.

  ## Design

  The counter maintains separate sequences for each dictionary-allocated type:
  - URI (type 1)
  - BNode (type 2)
  - Literal (type 3)

  Each type has its own sequence to allow independent allocation and potential
  future optimizations (e.g., different flush intervals per type).

  ## Persistence Strategy

  - **Flush interval**: Counter persisted every 1000 allocations per type
  - **Recovery margin**: On startup, add 1000 to persisted value to ensure
    no ID reuse even if crash occurred before last flush
  - **Graceful shutdown**: Checkpoint all counters via `terminate/2`

  ## Usage

  The counter is started with a database reference and manages its own
  persistence. It does NOT need to be in the supervision tree if you
  manage the lifecycle manually with a database handle.

  ```elixir
  # Start counter with database reference
  {:ok, counter} = SequenceCounter.start_link(db: db_ref)

  # Get next ID for a type
  {:ok, id} = SequenceCounter.next_id(counter, :uri)

  # Flush all counters to disk
  :ok = SequenceCounter.flush(counter)
  ```

  ## Error Handling

  - `{:error, :sequence_overflow}` - Sequence exhausted (>576 quadrillion IDs)
  - `{:error, :invalid_type}` - Unknown type requested
  - `{:error, :db_closed}` - Database reference is no longer valid
  """

  use GenServer

  require Logger

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Counter reference (GenServer pid or name)"
  @type counter :: GenServer.server()

  @typedoc "Dictionary-allocated term types"
  @type dict_type :: :uri | :bnode | :literal

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Sequence counter storage keys in str2id column family
  @counter_key_prefix "__seq_counter__"

  # Type indices for atomics array (1-indexed for :atomics)
  @type_indices %{
    uri: 1,
    bnode: 2,
    literal: 3
  }

  # Number of counter types
  @num_types 3

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the sequence counter GenServer.

  ## Options

  - `:db` - Required. Database reference from RocksDB NIF
  - `:name` - Optional. GenServer name for registration

  ## Examples

      {:ok, counter} = SequenceCounter.start_link(db: db_ref)
      {:ok, counter} = SequenceCounter.start_link(db: db_ref, name: MyCounter)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets the next ID for the given dictionary type.

  Atomically increments the counter and returns a fully-encoded term ID
  with the appropriate type tag.

  ## Arguments

  - `counter` - Counter reference (pid or name)
  - `type` - One of `:uri`, `:bnode`, or `:literal`

  ## Returns

  - `{:ok, term_id}` - The next unique ID for this type
  - `{:error, :sequence_overflow}` - Counter exhausted
  - `{:error, :invalid_type}` - Unknown type

  ## Examples

      {:ok, id} = SequenceCounter.next_id(counter, :uri)
      {:uri, seq} = Dictionary.decode_id(id)
  """
  @spec next_id(counter(), dict_type()) :: {:ok, Dictionary.term_id()} | {:error, atom()}
  def next_id(counter, type) when type in [:uri, :bnode, :literal] do
    GenServer.call(counter, {:next_id, type})
  end

  def next_id(_counter, _type), do: {:error, :invalid_type}

  @doc """
  Gets the current sequence value for a type without incrementing.

  Useful for diagnostics and monitoring.

  ## Examples

      {:ok, current} = SequenceCounter.current(counter, :uri)
  """
  @spec current(counter(), dict_type()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def current(counter, type) when type in [:uri, :bnode, :literal] do
    GenServer.call(counter, {:current, type})
  end

  def current(_counter, _type), do: {:error, :invalid_type}

  @doc """
  Forces a flush of all counters to RocksDB.

  Normally counters are flushed automatically every 1000 allocations.
  Use this for graceful shutdown or explicit checkpointing.

  ## Examples

      :ok = SequenceCounter.flush(counter)
  """
  @spec flush(counter()) :: :ok | {:error, term()}
  def flush(counter) do
    GenServer.call(counter, :flush)
  end

  @doc """
  Stops the counter, flushing state to disk.

  ## Examples

      :ok = SequenceCounter.stop(counter)
  """
  @spec stop(counter()) :: :ok
  def stop(counter) do
    GenServer.stop(counter, :normal)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)

    # Create atomics array for the 3 counter types
    # Using signed: false since sequences are always non-negative
    counter_ref = :atomics.new(@num_types, signed: false)

    # Load persisted values and initialize counters
    state = %{
      db: db,
      counter_ref: counter_ref,
      # Track allocations since last flush per type
      allocations_since_flush: %{uri: 0, bnode: 0, literal: 0}
    }

    case load_and_initialize_counters(state) do
      {:ok, state} ->
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:next_id, type}, _from, state) do
    type_index = @type_indices[type]
    type_tag = type_to_tag(type)

    # Atomic increment - returns new value
    new_seq = :atomics.add_get(state.counter_ref, type_index, 1)

    # Check for overflow
    if new_seq > Dictionary.max_sequence() do
      # Roll back the increment
      :atomics.sub(state.counter_ref, type_index, 1)
      {:reply, {:error, :sequence_overflow}, state}
    else
      # Create the full term ID
      term_id = Dictionary.encode_id(type_tag, new_seq)

      # Update allocation count and maybe flush
      state = maybe_flush_counter(state, type, new_seq)

      {:reply, {:ok, term_id}, state}
    end
  end

  @impl true
  def handle_call({:current, type}, _from, state) do
    type_index = @type_indices[type]
    current = :atomics.get(state.counter_ref, type_index)
    {:reply, {:ok, current}, state}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    case flush_all_counters(state) do
      :ok ->
        new_allocations = %{uri: 0, bnode: 0, literal: 0}
        {:reply, :ok, %{state | allocations_since_flush: new_allocations}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Best-effort flush on shutdown
    case flush_all_counters(state) do
      :ok ->
        Logger.debug("Sequence counters flushed successfully on shutdown")

      {:error, reason} ->
        Logger.warning("Failed to flush sequence counters on shutdown: #{inspect(reason)}")
    end

    :ok
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  @spec maybe_flush_counter(map(), dict_type(), non_neg_integer()) :: map()
  defp maybe_flush_counter(state, type, new_seq) do
    new_alloc_count = state.allocations_since_flush[type] + 1
    new_allocations = Map.put(state.allocations_since_flush, type, new_alloc_count)
    state = %{state | allocations_since_flush: new_allocations}

    if new_alloc_count >= Dictionary.flush_interval() do
      do_flush_counter(state, type, new_seq, new_allocations)
    else
      state
    end
  end

  @spec do_flush_counter(map(), dict_type(), non_neg_integer(), map()) :: map()
  defp do_flush_counter(state, type, new_seq, new_allocations) do
    case persist_counter(state.db, type, new_seq) do
      :ok ->
        %{state | allocations_since_flush: Map.put(new_allocations, type, 0)}

      {:error, reason} ->
        Logger.warning("Failed to persist #{type} counter: #{inspect(reason)}, will retry")
        state
    end
  end

  @spec load_and_initialize_counters(map()) :: {:ok, map()} | {:error, term()}
  defp load_and_initialize_counters(state) do
    results =
      for type <- [:uri, :bnode, :literal] do
        case load_counter(state.db, type) do
          {:ok, value} ->
            # Add safety margin to ensure no ID reuse after crash
            initial_value = value + Dictionary.safety_margin()
            type_index = @type_indices[type]
            :atomics.put(state.counter_ref, type_index, initial_value)

            Logger.debug(
              "Initialized #{type} counter: persisted=#{value}, starting=#{initial_value}"
            )

            {:ok, type}

          {:error, reason} ->
            {:error, {type, reason}}
        end
      end

    errors = for {:error, err} <- results, do: err

    if errors == [] do
      {:ok, state}
    else
      {:error, {:counter_init_failed, errors}}
    end
  end

  @spec load_counter(reference(), dict_type()) :: {:ok, non_neg_integer()} | {:error, term()}
  defp load_counter(db, type) do
    key = counter_key(type)

    case NIF.get(db, :str2id, key) do
      {:ok, binary} ->
        <<value::64-big>> = binary
        {:ok, value}

      :not_found ->
        # First time - start at 0
        {:ok, 0}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec persist_counter(reference(), dict_type(), non_neg_integer()) :: :ok | {:error, term()}
  defp persist_counter(db, type, value) do
    key = counter_key(type)
    binary = <<value::64-big>>

    case NIF.put(db, :str2id, key, binary) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec flush_all_counters(map()) :: :ok | {:error, term()}
  defp flush_all_counters(state) do
    results =
      for type <- [:uri, :bnode, :literal] do
        type_index = @type_indices[type]
        current = :atomics.get(state.counter_ref, type_index)
        persist_counter(state.db, type, current)
      end

    errors = for {:error, err} <- results, do: err

    if errors == [] do
      :ok
    else
      {:error, {:flush_failed, errors}}
    end
  end

  @spec counter_key(dict_type()) :: binary()
  defp counter_key(type) do
    "#{@counter_key_prefix}#{type}"
  end

  @spec type_to_tag(dict_type()) :: non_neg_integer()
  defp type_to_tag(:uri), do: Dictionary.type_uri()
  defp type_to_tag(:bnode), do: Dictionary.type_bnode()
  defp type_to_tag(:literal), do: Dictionary.type_literal()
end
