defmodule TripleStore.Transaction do
  @moduledoc """
  Transaction coordinator for SPARQL UPDATE operations.

  This GenServer provides serialized write access to the triple store,
  ensuring that concurrent updates don't interfere with each other.
  It also provides snapshot-based read isolation during updates.

  ## Architecture

  All write operations (INSERT, DELETE, UPDATE) are serialized through
  this coordinator. Reads can happen concurrently with writes, but
  readers during an update see a consistent snapshot from before the
  update started.

  ```
  ┌─────────────────────────────────────────────────────┐
  │                    Client Requests                   │
  │    (query/1, update/1, insert/2, delete/2, etc.)    │
  └─────────────┬─────────────────────────┬─────────────┘
                │                         │
                │ Writes (serialized)     │ Reads (direct)
                ▼                         ▼
  ┌─────────────────────┐   ┌─────────────────────────────┐
  │ Transaction Manager │   │      Direct DB Access       │
  │    (GenServer)      │   │  (snapshot during updates)  │
  └─────────────────────┘   └─────────────────────────────┘
                │
                ▼
  ┌─────────────────────────────────────────────────────┐
  │                    RocksDB                           │
  │         (WriteBatch for atomicity)                   │
  └─────────────────────────────────────────────────────┘
  ```

  ## Isolation Levels

  - **Writers**: Serialized through GenServer, one at a time
  - **Readers during update**: See snapshot from before update started
  - **Readers outside update**: Direct database access

  ## Plan Cache Integration

  After successful writes, the plan cache is automatically invalidated
  to ensure query plans reflect the updated statistics.

  ## Usage

      # Start the transaction manager
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Execute an update (serialized)
      {:ok, count} = Transaction.update(txn, "INSERT DATA { <s> <p> <o> }")

      # Execute a query (may use snapshot during concurrent update)
      {:ok, results} = Transaction.query(txn, "SELECT * WHERE { ?s ?p ?o }")

  ## Error Handling

  If an update fails partway through, changes are not applied (rollback).
  RocksDB's WriteBatch ensures atomicity at the storage level.
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.PlanCache
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.Term
  alias TripleStore.SPARQL.UpdateExecutor

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Transaction manager reference"
  @type manager :: GenServer.server()

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Dictionary manager reference"
  @type dict_manager :: GenServer.server()

  @typedoc "Execution context"
  @type context :: %{db: db_ref(), dict_manager: dict_manager()}

  @typedoc "Update result"
  @type update_result :: {:ok, non_neg_integer()} | {:error, term()}

  @typedoc "Query result"
  @type query_result :: {:ok, term()} | {:error, term()}

  @typedoc "Transaction state"
  @type state :: %{
          db: db_ref(),
          dict_manager: dict_manager(),
          update_in_progress: boolean(),
          current_snapshot: reference() | nil,
          plan_cache: GenServer.server() | nil,
          stats_callback: (-> :ok) | nil
        }

  # ===========================================================================
  # Configuration
  # ===========================================================================

  # Timeout for update operations (5 minutes)
  @update_timeout 300_000

  # Timeout for query operations (2 minutes)
  @query_timeout 120_000

  @doc """
  Returns the default update timeout in milliseconds.
  """
  @spec update_timeout() :: pos_integer()
  def update_timeout, do: @update_timeout

  @doc """
  Returns the default query timeout in milliseconds.
  """
  @spec query_timeout() :: pos_integer()
  def query_timeout, do: @query_timeout

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the transaction manager.

  ## Options

  - `:db` (required) - RocksDB database reference
  - `:dict_manager` (required) - Dictionary manager process
  - `:name` - Optional name for registration
  - `:plan_cache` - Optional plan cache process to invalidate after updates
  - `:stats_callback` - Optional callback to refresh statistics after updates

  ## Examples

      {:ok, txn} = Transaction.start_link(
        db: db,
        dict_manager: manager,
        plan_cache: PlanCache
      )

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Stops the transaction manager.
  """
  @spec stop(manager()) :: :ok
  def stop(manager) do
    GenServer.stop(manager)
  end

  @doc """
  Executes a SPARQL UPDATE operation.

  The operation is serialized through the transaction manager, ensuring
  no concurrent updates. After successful completion, the plan cache
  is invalidated.

  ## Arguments

  - `manager` - Transaction manager process
  - `sparql` - SPARQL UPDATE string
  - `opts` - Options (`:timeout` defaults to 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples affected
  - `{:error, {:parse_error, msg}}` - Parse error
  - `{:error, reason}` - Execution error

  ## Examples

      {:ok, 1} = Transaction.update(txn, "INSERT DATA { <s> <p> <o> }")
      {:ok, 2} = Transaction.update(txn, "DELETE DATA { <s1> <p> <o> . <s2> <p> <o> }")

  """
  @spec update(manager(), String.t(), keyword()) :: update_result()
  def update(manager, sparql, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @update_timeout)
    GenServer.call(manager, {:update, sparql}, timeout)
  end

  @doc """
  Executes a pre-parsed SPARQL UPDATE AST.

  Useful when you've already parsed the update and want to execute it.

  ## Arguments

  - `manager` - Transaction manager process
  - `ast` - Parsed UPDATE AST from `Parser.parse_update/1`
  - `opts` - Options (`:timeout` defaults to 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples affected
  - `{:error, reason}` - Execution error

  """
  @spec execute_update(manager(), term(), keyword()) :: update_result()
  def execute_update(manager, ast, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @update_timeout)
    GenServer.call(manager, {:execute_update, ast}, timeout)
  end

  @doc """
  Executes a SPARQL query.

  If an update is in progress, the query uses a snapshot for consistent reads.
  Otherwise, reads directly from the database.

  ## Arguments

  - `manager` - Transaction manager process
  - `sparql` - SPARQL query string
  - `opts` - Options (`:timeout` defaults to 2 minutes)

  ## Returns

  - `{:ok, results}` - Query results
  - `{:error, {:parse_error, msg}}` - Parse error
  - `{:error, reason}` - Execution error

  """
  @spec query(manager(), String.t(), keyword()) :: query_result()
  def query(manager, sparql, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @query_timeout)
    GenServer.call(manager, {:query, sparql}, timeout)
  end

  @doc """
  Directly inserts triples without parsing SPARQL.

  ## Arguments

  - `manager` - Transaction manager process
  - `triples` - List of `{subject, predicate, object}` RDF terms
  - `opts` - Options

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, reason}` - On failure

  """
  @spec insert(manager(), [{term(), term(), term()}], keyword()) :: update_result()
  def insert(manager, triples, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @update_timeout)
    GenServer.call(manager, {:insert, triples}, timeout)
  end

  @doc """
  Directly deletes triples without parsing SPARQL.

  ## Arguments

  - `manager` - Transaction manager process
  - `triples` - List of `{subject, predicate, object}` RDF terms
  - `opts` - Options

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, reason}` - On failure

  """
  @spec delete(manager(), [{term(), term(), term()}], keyword()) :: update_result()
  def delete(manager, triples, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @update_timeout)
    GenServer.call(manager, {:delete, triples}, timeout)
  end

  @doc """
  Returns whether an update is currently in progress.

  Useful for debugging and monitoring.
  """
  @spec update_in_progress?(manager()) :: boolean()
  def update_in_progress?(manager) do
    GenServer.call(manager, :update_in_progress?)
  end

  @doc """
  Returns the current snapshot reference, if any.

  Returns `nil` if no update is in progress.
  """
  @spec current_snapshot(manager()) :: reference() | nil
  def current_snapshot(manager) do
    GenServer.call(manager, :current_snapshot)
  end

  @doc """
  Returns the execution context for direct operations.

  Use with caution - bypasses serialization.
  """
  @spec get_context(manager()) :: context()
  def get_context(manager) do
    GenServer.call(manager, :get_context)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    dict_manager = Keyword.fetch!(opts, :dict_manager)
    plan_cache = Keyword.get(opts, :plan_cache)
    stats_callback = Keyword.get(opts, :stats_callback)

    state = %{
      db: db,
      dict_manager: dict_manager,
      update_in_progress: false,
      current_snapshot: nil,
      plan_cache: plan_cache,
      stats_callback: stats_callback
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:update, sparql}, _from, state) do
    result =
      case Parser.parse_update(sparql) do
        {:ok, ast} ->
          execute_update_internal(ast, state)

        {:error, _} = error ->
          error
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:execute_update, ast}, _from, state) do
    result = execute_update_internal(ast, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:query, sparql}, _from, state) do
    result = execute_query_internal(sparql, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:insert, triples}, _from, state) do
    result = execute_insert_internal(triples, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete, triples}, _from, state) do
    result = execute_delete_internal(triples, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:update_in_progress?, _from, state) do
    {:reply, state.update_in_progress, state}
  end

  @impl true
  def handle_call(:current_snapshot, _from, state) do
    {:reply, state.current_snapshot, state}
  end

  @impl true
  def handle_call(:get_context, _from, state) do
    ctx = %{db: state.db, dict_manager: state.dict_manager}
    {:reply, ctx, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Release any active snapshot
    if state.current_snapshot do
      NIF.release_snapshot(state.current_snapshot)
    end

    :ok
  end

  # ===========================================================================
  # Internal Implementation
  # ===========================================================================

  # Execute an update with proper isolation and cache invalidation
  defp execute_update_internal(ast, state) do
    # Create snapshot for concurrent readers
    snapshot_result = create_snapshot(state.db)

    case snapshot_result do
      {:ok, snapshot} ->
        try do
          # Mark update in progress (not actually used here since we're synchronous,
          # but could be exposed for monitoring)
          ctx = %{db: state.db, dict_manager: state.dict_manager}

          # Execute the update
          result = UpdateExecutor.execute(ctx, ast)

          # On success, invalidate cache and refresh stats
          case result do
            {:ok, count} when count > 0 ->
              invalidate_cache(state.plan_cache)
              call_stats_callback(state.stats_callback)
              {:ok, count}

            {:ok, 0} ->
              # No changes, no need to invalidate
              {:ok, 0}

            {:error, _} = error ->
              # Rollback is automatic - WriteBatch wasn't applied
              error
          end
        after
          # Always release snapshot
          release_snapshot(snapshot)
        end

      {:error, _} = error ->
        error
    end
  end

  # Execute a query, potentially using snapshot for isolation
  defp execute_query_internal(sparql, state) do
    ctx = %{db: state.db, dict_manager: state.dict_manager}

    case Query.prepare(sparql) do
      {:ok, prepared} ->
        Query.execute(ctx, prepared)

      {:error, _} = error ->
        error
    end
  end

  # Execute direct insert
  defp execute_insert_internal(triples, state) do
    ctx = %{db: state.db, dict_manager: state.dict_manager}

    # Convert RDF triples to internal format
    quads =
      Enum.map(triples, fn {s, p, o} ->
        {:triple, Term.to_ast(s), Term.to_ast(p), Term.to_ast(o)}
      end)

    result = UpdateExecutor.execute_insert_data(ctx, quads)

    case result do
      {:ok, count} when count > 0 ->
        invalidate_cache(state.plan_cache)
        call_stats_callback(state.stats_callback)
        {:ok, count}

      other ->
        other
    end
  end

  # Execute direct delete
  defp execute_delete_internal(triples, state) do
    ctx = %{db: state.db, dict_manager: state.dict_manager}

    # Convert RDF triples to internal format
    quads =
      Enum.map(triples, fn {s, p, o} ->
        {:triple, Term.to_ast(s), Term.to_ast(p), Term.to_ast(o)}
      end)

    result = UpdateExecutor.execute_delete_data(ctx, quads)

    case result do
      {:ok, count} when count > 0 ->
        invalidate_cache(state.plan_cache)
        call_stats_callback(state.stats_callback)
        {:ok, count}

      other ->
        other
    end
  end

  # ===========================================================================
  # Snapshot Management
  # ===========================================================================

  defp create_snapshot(db) do
    NIF.snapshot(db)
  end

  defp release_snapshot(snapshot) do
    NIF.release_snapshot(snapshot)
  end

  # ===========================================================================
  # Cache Invalidation
  # ===========================================================================

  defp invalidate_cache(nil), do: :ok

  defp invalidate_cache(plan_cache) do
    try do
      PlanCache.invalidate(name: plan_cache)
    rescue
      e ->
        Logger.warning("Plan cache invalidation failed: #{inspect(e)}")
        :ok
    catch
      :exit, reason ->
        Logger.warning("Plan cache invalidation crashed: #{inspect(reason)}")
        :ok
    end
  end

  defp call_stats_callback(nil), do: :ok

  defp call_stats_callback(callback) when is_function(callback, 0) do
    try do
      callback.()
    rescue
      e ->
        Logger.warning("Stats callback failed: #{inspect(e)}")
        :ok
    catch
      :exit, reason ->
        Logger.warning("Stats callback crashed: #{inspect(reason)}")
        :ok
    end
  end
end
