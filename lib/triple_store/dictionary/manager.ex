defmodule TripleStore.Dictionary.Manager do
  @moduledoc """
  Dictionary Manager GenServer for serializing term ID creation.

  This GenServer serializes `get_or_create_id` operations to ensure atomic
  create-if-not-exists semantics. Read-only operations (`lookup_id`) go
  directly to RocksDB without serialization for maximum performance.

  ## Usage

  ```elixir
  {:ok, manager} = Manager.start_link(db: db_ref)

  # Read-only lookup (direct to RocksDB, no serialization)
  {:ok, id} = StringToId.lookup_id(db, term)

  # Atomic get-or-create (serialized through Manager)
  {:ok, id} = Manager.get_or_create_id(manager, term)
  ```
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Batch
  alias TripleStore.Dictionary.SequenceCounter
  alias TripleStore.Dictionary.StringToId

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Manager reference (GenServer pid or name)"
  @type manager :: GenServer.server()

  @typedoc "RDF term (URI, blank node, or literal)"
  @type rdf_term :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the Dictionary Manager GenServer.

  ## Options

  - `:db` - Required. Database reference from RocksDB NIF
  - `:name` - Optional. GenServer name for registration

  ## Examples

      {:ok, manager} = Manager.start_link(db: db_ref)
      {:ok, manager} = Manager.start_link(db: db_ref, name: MyManager)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets an existing ID or creates a new one for a term.

  This operation is serialized through the GenServer to ensure atomic
  create-if-not-exists semantics. Two concurrent calls for the same
  term will return the same ID.

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
    GenServer.call(manager, {:get_or_create_id, term})
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

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)

    # Start the sequence counter
    case SequenceCounter.start_link(db: db) do
      {:ok, counter} ->
        {:ok, %{db: db, counter: counter}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get_or_create_id, term}, _from, state) do
    start_time = System.monotonic_time()
    result = do_get_or_create_id(state.db, state.counter, term)
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:triple_store, :dictionary, :get_or_create],
      %{duration: duration, count: 1},
      %{success: match?({:ok, _}, result), batch: false}
    )

    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_or_create_ids, terms}, _from, state) do
    start_time = System.monotonic_time()
    result = do_get_or_create_ids(state.db, state.counter, terms)
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
  def terminate(_reason, state) do
    SequenceCounter.stop(state.counter)
    :ok
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  @spec do_get_or_create_id(reference(), SequenceCounter.counter(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | {:error, term()}
  defp do_get_or_create_id(db, counter, term) do
    case StringToId.encode_term(term) do
      {:ok, key} ->
        # Check if already exists
        case NIF.get(db, :str2id, key) do
          {:ok, <<id::64-big>>} ->
            {:ok, id}

          :not_found ->
            # Create new ID
            create_and_store_id(db, counter, key, term)

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @spec create_and_store_id(reference(), SequenceCounter.counter(), binary(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | {:error, term()}
  defp create_and_store_id(db, counter, key, term) do
    term_type = get_term_type(term)

    case SequenceCounter.next_id(counter, term_type) do
      {:ok, id} ->
        id_binary = <<id::64-big>>

        with :ok <- NIF.put(db, :str2id, key, id_binary),
             :ok <- NIF.put(db, :id2str, id_binary, key) do
          {:ok, id}
        end

      {:error, _} = error ->
        error
    end
  end

  @spec do_get_or_create_ids(reference(), SequenceCounter.counter(), [rdf_term()]) ::
          {:ok, [Dictionary.term_id()]} | {:error, term()}
  defp do_get_or_create_ids(db, counter, terms) do
    Batch.map_collect_success(terms, &do_get_or_create_id(db, counter, &1))
  end

  @spec get_term_type(rdf_term()) :: :uri | :bnode | :literal
  defp get_term_type(%RDF.IRI{}), do: :uri
  defp get_term_type(%RDF.BlankNode{}), do: :bnode
  defp get_term_type(%RDF.Literal{}), do: :literal
end
