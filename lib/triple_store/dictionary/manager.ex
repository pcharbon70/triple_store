defmodule TripleStore.Dictionary.Manager do
  @moduledoc """
  Dictionary Manager GenServer for serializing term ID creation.

  This GenServer serializes `get_or_create_id` operations to ensure atomic
  create-if-not-exists semantics. Read-only operations (`lookup_id`) go
  directly to RocksDB without serialization for maximum performance.

  ## ETS Read Cache

  The Manager maintains an ETS-based read cache for frequently accessed terms.
  This cache is checked before making a GenServer call, allowing concurrent
  read access without serialization.

  Cache characteristics:
  - Created with `{:read_concurrency, true}` for parallel reads
  - Write-through: populated after each new ID creation
  - Lookup complexity: O(1) average case
  - No eviction policy (memory bounded by unique terms)

  ## Memory Considerations

  The ETS cache grows with the number of unique terms. Each cache entry uses
  approximately 100-200 bytes depending on term size. For example:
  - 1 million terms: ~100-200 MB
  - 10 million terms: ~1-2 GB
  - 100 million terms: ~10-20 GB

  The cache has no eviction policy as memory is bounded by the total number
  of unique terms in the dataset. Consider available memory when loading
  very large datasets.

  ## Usage

  ```elixir
  {:ok, manager} = Manager.start_link(db: db_ref)

  # Read-only lookup (checks cache then RocksDB, no ID creation)
  {:ok, id} = Manager.lookup_id(manager, term)
  :not_found = Manager.lookup_id(manager, unknown_term)

  # Atomic get-or-create (checks cache first, serialized on miss)
  {:ok, id} = Manager.get_or_create_id(manager, term)
  ```
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.SequenceCounter
  alias TripleStore.Dictionary.StringToId

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Manager reference (GenServer pid or name)"
  @type manager :: GenServer.server()

  @typedoc "RDF term (URI, blank node, or literal)"
  @type rdf_term :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()

  # Sentinel value for persistent_term lookups
  @not_found :__manager_not_found__

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the Dictionary Manager GenServer.

  ## Options

  - `:db` - Required. Database reference from RocksDB NIF
  - `:counter` - Optional. External sequence counter reference. If not provided,
    a new counter will be started. Use this when sharing a counter across
    multiple managers (e.g., in ShardedManager).
  - `:cache` - Optional. External ETS cache table reference. If not provided,
    a new ETS table will be created. Use this when sharing a cache across
    multiple managers.
  - `:name` - Optional. GenServer name for registration

  ## Examples

      {:ok, manager} = Manager.start_link(db: db_ref)
      {:ok, manager} = Manager.start_link(db: db_ref, name: MyManager)
      {:ok, manager} = Manager.start_link(db: db_ref, counter: shared_counter)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets an existing ID or creates a new one for a term.

  This operation first checks the ETS read cache for O(1) lookup.
  On cache miss, it falls through to the GenServer for atomic
  create-if-not-exists semantics.

  ## Arguments

  - `manager` - Manager process reference
  - `term` - RDF term to get or create ID for

  ## Returns

  - `{:ok, term_id}` - Existing or newly created ID
  - `{:error, reason}` - On failure
  """
  @spec get_or_create_id(manager(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | {:error, term()}
  def get_or_create_id(manager, term) do
    # Try cache lookup first (lock-free, concurrent reads)
    case cache_lookup(manager, term) do
      {:ok, id} ->
        emit_cache_telemetry(:hit)
        {:ok, id}

      :miss ->
        emit_cache_telemetry(:miss)
        GenServer.call(manager, {:get_or_create_id, term})
    end
  end

  @doc """
  Looks up an existing ID for a term without creating one.

  This is useful for query-only workloads where you don't want to
  create new entries for unknown terms. Checks the ETS cache first,
  then falls back to RocksDB.

  ## Arguments

  - `manager` - Manager process reference
  - `term` - RDF term to look up

  ## Returns

  - `{:ok, term_id}` - If term exists
  - `:not_found` - If term doesn't exist
  - `{:error, reason}` - On failure
  """
  @spec lookup_id(manager(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  def lookup_id(manager, term) do
    # Try cache lookup first
    case cache_lookup(manager, term) do
      {:ok, id} ->
        emit_cache_telemetry(:hit)
        {:ok, id}

      :miss ->
        emit_cache_telemetry(:miss)
        # Fall through to RocksDB lookup via GenServer
        GenServer.call(manager, {:lookup_id, term})
    end
  end

  # Look up term in the ETS cache without GenServer call
  defp cache_lookup(manager, term) do
    cache_key = {:dictionary_cache, manager}

    with {:ok, cache} <- get_cache_table(cache_key),
         {:ok, key} <- StringToId.encode_term(term) do
      case :ets.lookup(cache, key) do
        [{^key, id}] -> {:ok, id}
        [] -> :miss
      end
    else
      _ -> :miss
    end
  end

  defp get_cache_table(cache_key) do
    case :persistent_term.get(cache_key, @not_found) do
      @not_found -> :error
      cache -> {:ok, cache}
    end
  end

  defp emit_cache_telemetry(type) do
    :telemetry.execute(
      [:triple_store, :dictionary, :cache],
      %{count: 1},
      %{type: type}
    )
  end

  @doc """
  Gets or creates IDs for multiple terms atomically.

  Batch version for efficient bulk loading.

  ## Arguments

  - `manager` - Manager process reference
  - `terms` - List of RDF terms

  ## Returns

  - `{:ok, ids}` - List of term IDs in same order as input
  - `{:error, reason}` - On failure
  """
  @spec get_or_create_ids(manager(), [rdf_term()]) ::
          {:ok, [Dictionary.term_id()]} | {:error, term()}
  def get_or_create_ids(manager, terms) do
    GenServer.call(manager, {:get_or_create_ids, terms})
  end

  @doc """
  Gets the database reference from the manager.

  This is useful when you need to perform read-only dictionary lookups
  using the same database the manager was initialized with.

  ## Arguments

  - `manager` - Manager process reference

  ## Returns

  - `{:ok, db}` - Database reference
  """
  @spec get_db(manager()) :: {:ok, reference()}
  def get_db(manager) do
    GenServer.call(manager, :get_db)
  end

  @doc """
  Stops the manager, flushing the sequence counter.
  """
  @spec stop(manager()) :: :ok
  def stop(manager) do
    GenServer.stop(manager, :normal)
  end

  @doc """
  Gets the sequence counter reference from the manager.

  This is useful for backup/restore operations that need to export or
  import counter state.

  ## Arguments

  - `manager` - Manager process reference

  ## Returns

  - `{:ok, counter}` - Sequence counter process reference
  """
  @spec get_counter(manager()) :: {:ok, SequenceCounter.counter()}
  def get_counter(manager) do
    GenServer.call(manager, :get_counter)
  end

  @doc """
  Gets the ETS cache table reference from the manager.

  This is useful for diagnostics and testing.

  ## Arguments

  - `manager` - Manager process reference

  ## Returns

  - `{:ok, cache}` - ETS table reference
  """
  @spec get_cache(manager()) :: {:ok, :ets.tid()}
  def get_cache(manager) do
    GenServer.call(manager, :get_cache)
  end

  @doc """
  Gets cache statistics.

  Returns the current size of the ETS cache.

  ## Arguments

  - `manager` - Manager process reference

  ## Returns

  - `{:ok, stats}` - Map with cache statistics
  """
  @spec cache_stats(manager()) :: {:ok, map()}
  def cache_stats(manager) do
    GenServer.call(manager, :cache_stats)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    external_counter = Keyword.get(opts, :counter)
    external_cache = Keyword.get(opts, :cache)

    # Create or use external ETS cache for lock-free reads
    {cache, owns_cache} = get_or_create_cache(external_cache)

    # Store cache reference in persistent_term for client-side lookup
    cache_key = {:dictionary_cache, self()}
    :persistent_term.put(cache_key, cache)

    # Use external counter if provided, otherwise start a new one
    case get_or_start_counter(db, external_counter) do
      {:ok, counter, owns_counter} ->
        {:ok,
         %{
           db: db,
           counter: counter,
           owns_counter: owns_counter,
           cache: cache,
           owns_cache: owns_cache
         }}

      {:error, reason} ->
        # Clean up cache if counter fails
        if owns_cache, do: :ets.delete(cache)
        safe_persistent_term_erase(cache_key)
        {:stop, reason}
    end
  end

  defp get_or_create_cache(nil) do
    # Create new ETS table with read concurrency for parallel lookups
    cache =
      :ets.new(:dictionary_cache, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, false}
      ])

    {cache, true}
  end

  defp get_or_create_cache(cache) when is_reference(cache) do
    # Use external cache (e.g., shared across shards)
    {cache, false}
  end

  defp get_or_start_counter(_db, counter) when is_pid(counter) do
    # External counter provided - we don't own it
    {:ok, counter, false}
  end

  defp get_or_start_counter(db, nil) do
    # No external counter - start our own
    case SequenceCounter.start_link(db: db) do
      {:ok, counter} -> {:ok, counter, true}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def handle_call({:get_or_create_id, term}, _from, state) do
    start_time = System.monotonic_time()
    result = do_get_or_create_id(state.db, state.counter, state.cache, term)
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:triple_store, :dictionary, :get_or_create],
      %{duration: duration, count: 1},
      %{success: match?({:ok, _}, result), batch: false}
    )

    {:reply, result, state}
  end

  @impl true
  def handle_call({:lookup_id, term}, _from, state) do
    result = do_lookup_id(state.db, state.cache, term)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_or_create_ids, terms}, _from, state) do
    start_time = System.monotonic_time()
    result = do_get_or_create_ids_batch(state.db, state.counter, state.cache, terms)
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:triple_store, :dictionary, :get_or_create],
      %{duration: duration, count: length(terms)},
      %{success: match?({:ok, _}, result), batch: true}
    )

    {:reply, result, state}
  end

  @impl true
  def handle_call(:get_db, _from, state) do
    {:reply, {:ok, state.db}, state}
  end

  @impl true
  def handle_call(:get_counter, _from, state) do
    {:reply, {:ok, state.counter}, state}
  end

  @impl true
  def handle_call(:get_cache, _from, state) do
    {:reply, {:ok, state.cache}, state}
  end

  @impl true
  def handle_call(:cache_stats, _from, state) do
    size = :ets.info(state.cache, :size)
    memory = :ets.info(state.cache, :memory) * :erlang.system_info(:wordsize)
    stats = %{size: size, memory_bytes: memory}
    {:reply, {:ok, stats}, state}
  end

  @impl true
  def handle_info(_msg, state) do
    # Catch-all for unexpected messages to prevent mailbox accumulation
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Clean up persistent_term entry
    cache_key = {:dictionary_cache, self()}
    safe_persistent_term_erase(cache_key)

    # Only delete the cache if we own it
    if state.owns_cache do
      :ets.delete(state.cache)
    end

    # Only stop the counter if we own it (i.e., we created it)
    if state.owns_counter do
      SequenceCounter.stop(state.counter)
    end

    :ok
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  @spec do_get_or_create_id(reference(), SequenceCounter.counter(), :ets.tid(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | {:error, term()}
  defp do_get_or_create_id(db, counter, cache, term) do
    case StringToId.encode_term(term) do
      {:ok, key} ->
        # Check if already exists in RocksDB
        case NIF.get(db, :str2id, key) do
          {:ok, <<id::64-big>>} ->
            # Populate cache for future lookups
            :ets.insert(cache, {key, id})
            {:ok, id}

          :not_found ->
            # Create new ID and populate cache
            create_and_store_id(db, counter, cache, key, term)

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @spec do_lookup_id(reference(), :ets.tid(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  defp do_lookup_id(db, cache, term) do
    with {:ok, key} <- StringToId.encode_term(term) do
      lookup_in_cache_or_db(db, cache, key)
    end
  end

  @spec lookup_in_cache_or_db(reference(), :ets.tid(), binary()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  defp lookup_in_cache_or_db(db, cache, key) do
    case :ets.lookup(cache, key) do
      [{^key, id}] -> {:ok, id}
      [] -> lookup_in_rocksdb(db, cache, key)
    end
  end

  @spec lookup_in_rocksdb(reference(), :ets.tid(), binary()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  defp lookup_in_rocksdb(db, cache, key) do
    case NIF.get(db, :str2id, key) do
      {:ok, <<id::64-big>>} ->
        :ets.insert(cache, {key, id})
        {:ok, id}

      :not_found ->
        :not_found

      {:error, _} = error ->
        error
    end
  end

  @spec create_and_store_id(
          reference(),
          SequenceCounter.counter(),
          :ets.tid(),
          binary(),
          rdf_term()
        ) ::
          {:ok, Dictionary.term_id()} | {:error, term()}
  defp create_and_store_id(db, counter, cache, key, term) do
    term_type = get_term_type(term)

    case SequenceCounter.next_id(counter, term_type) do
      {:ok, id} ->
        id_binary = <<id::64-big>>

        with :ok <- NIF.put(db, :str2id, key, id_binary),
             :ok <- NIF.put(db, :id2str, id_binary, key) do
          # Populate cache after successful storage
          :ets.insert(cache, {key, id})
          {:ok, id}
        end

      {:error, _} = error ->
        error
    end
  end

  # Optimized batch processing with range allocation
  @spec do_get_or_create_ids_batch(reference(), SequenceCounter.counter(), :ets.tid(), [
          rdf_term()
        ]) ::
          {:ok, [Dictionary.term_id()]} | {:error, term()}
  defp do_get_or_create_ids_batch(db, counter, cache, terms) do
    {encoded_terms, encode_errors} = encode_and_lookup_terms(db, cache, terms)

    case encode_errors do
      [first_error | _] ->
        {:error, {:encode_failed, first_error}}

      [] ->
        process_encoded_terms(db, counter, cache, encoded_terms)
    end
  end

  @spec process_encoded_terms(
          reference(),
          SequenceCounter.counter(),
          :ets.tid(),
          [{non_neg_integer(), binary(), rdf_term(), Dictionary.term_id() | nil}]
        ) :: {:ok, [Dictionary.term_id()]} | {:error, term()}
  defp process_encoded_terms(db, counter, cache, encoded_terms) do
    needs_ids = for {idx, key, term, nil} <- encoded_terms, do: {idx, key, term}

    case needs_ids do
      [] ->
        {:ok, for({_idx, _key, _term, id} <- encoded_terms, do: id)}

      _ ->
        create_missing_ids(db, counter, cache, needs_ids, encoded_terms)
    end
  end

  @spec create_missing_ids(
          reference(),
          SequenceCounter.counter(),
          :ets.tid(),
          [{non_neg_integer(), binary(), rdf_term()}],
          [{non_neg_integer(), binary(), rdf_term(), Dictionary.term_id() | nil}]
        ) :: {:ok, [Dictionary.term_id()]} | {:error, term()}
  defp create_missing_ids(db, counter, cache, needs_ids, encoded_terms) do
    # Deduplicate by key - group indices that share the same key
    # This fixes the bug where the same term appearing multiple times in a batch
    # would get different IDs instead of sharing the same ID
    {unique_terms, key_to_indices} = deduplicate_needs_ids(needs_ids)

    with {:ok, type_ranges} <- allocate_ranges_for_unique_terms(counter, unique_terms),
         {:ok, key_to_id} <- assign_and_store_unique_ids(db, cache, unique_terms, type_ranges) do
      # Build id_map from key_to_id and key_to_indices
      id_map =
        Enum.reduce(key_to_indices, %{}, fn {key, indices}, acc ->
          id = Map.fetch!(key_to_id, key)

          Enum.reduce(indices, acc, fn idx, inner_acc ->
            Map.put(inner_acc, idx, id)
          end)
        end)

      ids =
        Enum.map(encoded_terms, fn {idx, _key, _term, existing_id} ->
          existing_id || Map.fetch!(id_map, idx)
        end)

      {:ok, ids}
    end
  end

  # Deduplicate needs_ids by key, returning unique terms and a mapping of key to indices
  @spec deduplicate_needs_ids([{non_neg_integer(), binary(), rdf_term()}]) ::
          {[{binary(), rdf_term()}], %{binary() => [non_neg_integer()]}}
  defp deduplicate_needs_ids(needs_ids) do
    {unique_map, key_to_indices} =
      Enum.reduce(needs_ids, {%{}, %{}}, fn {idx, key, term}, {unique, indices} ->
        new_indices = Map.update(indices, key, [idx], fn existing -> [idx | existing] end)

        case Map.has_key?(unique, key) do
          true -> {unique, new_indices}
          false -> {Map.put(unique, key, term), new_indices}
        end
      end)

    unique_terms = Enum.map(unique_map, fn {key, term} -> {key, term} end)
    {unique_terms, key_to_indices}
  end

  @spec encode_and_lookup_terms(reference(), :ets.tid(), [rdf_term()]) ::
          {[{non_neg_integer(), binary(), rdf_term(), Dictionary.term_id() | nil}],
           [{non_neg_integer(), term()}]}
  defp encode_and_lookup_terms(db, cache, terms) do
    terms
    |> Enum.with_index()
    |> Enum.reduce({[], []}, fn {term, idx}, {acc, errors} ->
      case encode_and_lookup_term(db, cache, term, idx) do
        {:ok, entry} -> {[entry | acc], errors}
        {:error, reason} -> {acc, [{idx, reason} | errors]}
      end
    end)
    |> then(fn {encoded, errors} -> {Enum.reverse(encoded), Enum.reverse(errors)} end)
  end

  @spec encode_and_lookup_term(reference(), :ets.tid(), rdf_term(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), binary(), rdf_term(), Dictionary.term_id() | nil}}
          | {:error, term()}
  defp encode_and_lookup_term(db, cache, term, idx) do
    case StringToId.encode_term(term) do
      {:ok, key} ->
        existing_id = lookup_existing_id(db, cache, key)
        {:ok, {idx, key, term, existing_id}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec lookup_existing_id(reference(), :ets.tid(), binary()) :: Dictionary.term_id() | nil
  defp lookup_existing_id(db, cache, key) do
    case :ets.lookup(cache, key) do
      [{^key, id}] -> id
      [] -> lookup_existing_id_in_db(db, cache, key)
    end
  end

  @spec lookup_existing_id_in_db(reference(), :ets.tid(), binary()) :: Dictionary.term_id() | nil
  defp lookup_existing_id_in_db(db, cache, key) do
    case NIF.get(db, :str2id, key) do
      {:ok, <<id::64-big>>} ->
        :ets.insert(cache, {key, id})
        id

      _ ->
        nil
    end
  end

  # Allocate ID ranges for unique terms (works with [{key, term}])
  @spec allocate_ranges_for_unique_terms(SequenceCounter.counter(), [{binary(), rdf_term()}]) ::
          {:ok, %{atom() => {non_neg_integer(), non_neg_integer()}}} | {:error, term()}
  defp allocate_ranges_for_unique_terms(counter, unique_terms) do
    # Count terms by type
    type_counts =
      unique_terms
      |> Enum.reduce(%{uri: 0, bnode: 0, literal: 0}, fn {_key, term}, acc ->
        type = get_term_type(term)
        Map.update!(acc, type, &(&1 + 1))
      end)

    # Allocate ranges for each type that has terms
    type_counts
    |> Enum.filter(fn {_type, count} -> count > 0 end)
    |> Enum.reduce_while({:ok, %{}}, fn {type, count}, {:ok, ranges} ->
      case SequenceCounter.allocate_range(counter, type, count) do
        {:ok, start_seq} ->
          {:cont, {:ok, Map.put(ranges, type, {start_seq, 0})}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  # Assign IDs from ranges and store in RocksDB for unique terms only
  # Returns a map of key -> id
  @spec assign_and_store_unique_ids(
          reference(),
          :ets.tid(),
          [{binary(), rdf_term()}],
          %{atom() => {non_neg_integer(), non_neg_integer()}}
        ) :: {:ok, %{binary() => Dictionary.term_id()}} | {:error, term()}
  defp assign_and_store_unique_ids(db, cache, unique_terms, type_ranges) do
    # Track current offset for each type
    {results, _final_ranges} =
      Enum.reduce(unique_terms, {%{}, type_ranges}, fn {key, term}, {key_to_id, ranges} ->
        type = get_term_type(term)
        type_tag = type_to_tag(type)
        {start_seq, offset} = Map.fetch!(ranges, type)

        seq = start_seq + offset
        id = Dictionary.encode_id(type_tag, seq)
        id_binary = <<id::64-big>>

        # Store in RocksDB
        :ok = NIF.put(db, :str2id, key, id_binary)
        :ok = NIF.put(db, :id2str, id_binary, key)

        # Populate cache
        :ets.insert(cache, {key, id})

        new_ranges = Map.put(ranges, type, {start_seq, offset + 1})
        {Map.put(key_to_id, key, id), new_ranges}
      end)

    {:ok, results}
  end

  defp type_to_tag(:uri), do: Dictionary.type_uri()
  defp type_to_tag(:bnode), do: Dictionary.type_bnode()
  defp type_to_tag(:literal), do: Dictionary.type_literal()

  @spec get_term_type(rdf_term()) :: :uri | :bnode | :literal
  defp get_term_type(%RDF.IRI{}), do: :uri
  defp get_term_type(%RDF.BlankNode{}), do: :bnode
  defp get_term_type(%RDF.Literal{}), do: :literal

  # Safe helper for erasing persistent_term entries
  @spec safe_persistent_term_erase(term()) :: :ok
  defp safe_persistent_term_erase(key) do
    case :persistent_term.get(key, @not_found) do
      @not_found -> :ok
      _ -> :persistent_term.erase(key)
    end

    :ok
  end
end
