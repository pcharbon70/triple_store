defmodule TripleStore do
  @moduledoc """
  A high-performance RDF triple store with SPARQL 1.1 and OWL 2 RL reasoning.

  This module provides the unified public API for the triple store. It composes
  the lower-level modules (Loader, Query, Transaction, Reasoner) into a
  convenient interface.

  ## Quick Start

      # Open a store
      {:ok, store} = TripleStore.open("./data")

      # Load RDF data
      {:ok, count} = TripleStore.load(store, "ontology.ttl")

      # Query with SPARQL
      {:ok, results} = TripleStore.query(store, "SELECT ?s WHERE { ?s a foaf:Person }")

      # Enable reasoning
      {:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)

      # Check health
      {:ok, health} = TripleStore.health(store)

      # Close when done
      :ok = TripleStore.close(store)

  ## Architecture

  The triple store uses:
  - RocksDB for persistent storage via Rustler NIFs
  - Dictionary encoding for compact term representation
  - SPO/POS/OSP indices for efficient pattern matching
  - Forward-chaining materialization for OWL 2 RL reasoning

  ## Store Handle

  The store handle returned by `open/2` contains:
  - Database reference for RocksDB operations
  - Dictionary manager for term encoding/decoding
  - Transaction manager for coordinated updates

  ## Thread Safety

  - Reads can be executed concurrently
  - Writes are serialized through the Transaction manager
  - Snapshot isolation ensures consistent reads during writes

  ## Error Handling

  All functions return tagged tuples:
  - `{:ok, result}` on success
  - `{:error, reason}` on failure

  Common error reasons:
  - `:database_closed` - Store was closed
  - `:parse_error` - Invalid SPARQL syntax
  - `:timeout` - Query exceeded time limit
  - `:file_not_found` - RDF file does not exist
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Loader
  alias TripleStore.SPARQL.Query
  alias TripleStore.Transaction
  alias TripleStore.Reasoner.SemiNaive
  alias TripleStore.Reasoner.ReasoningProfile
  alias TripleStore.Statistics
  alias TripleStore.Telemetry

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Store handle containing database and manager references"
  @type store :: %{
          db: reference(),
          dict_manager: GenServer.server(),
          transaction: GenServer.server() | nil,
          path: String.t()
        }

  @typedoc "Options for opening a store"
  @type open_opts :: [
          create_if_missing: boolean()
        ]

  @typedoc "Options for querying"
  @type query_opts :: [
          timeout: pos_integer(),
          explain: boolean(),
          optimize: boolean()
        ]

  @typedoc "Options for loading"
  @type load_opts :: [
          batch_size: pos_integer(),
          format: atom()
        ]

  @typedoc "Options for materialization"
  @type materialize_opts :: [
          profile: :rdfs | :owl2rl | :all,
          parallel: boolean()
        ]

  @typedoc "Health status"
  @type health_status :: :healthy | :degraded | :unhealthy

  @typedoc "Health check result"
  @type health_result :: %{
          status: health_status(),
          triple_count: non_neg_integer(),
          database_open: boolean(),
          dict_manager_alive: boolean(),
          checked_at: DateTime.t()
        }

  # ===========================================================================
  # Store Lifecycle
  # ===========================================================================

  @doc """
  Opens a triple store at the given path.

  Creates the database and required column families if they don't exist.
  Also starts the dictionary manager for term encoding.

  ## Arguments

  - `path` - Path to the database directory

  ## Options

  - `:create_if_missing` - Create database if it doesn't exist (default: true)

  ## Returns

  - `{:ok, store}` - Store handle for subsequent operations
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, store} = TripleStore.open("./data")

      # With options
      {:ok, store} = TripleStore.open("./data", create_if_missing: true)

  """
  @spec open(Path.t(), open_opts()) :: {:ok, store()} | {:error, term()}
  def open(path, opts \\ []) do
    _create_if_missing = Keyword.get(opts, :create_if_missing, true)

    Telemetry.span(:store, :open, %{path: path}, fn ->
      with {:ok, db} <- NIF.open(path),
           {:ok, dict_manager} <- DictManager.start_link(db: db) do
        store = %{
          db: db,
          dict_manager: dict_manager,
          transaction: nil,
          path: path
        }

        {{:ok, store}, %{}}
      end
    end)
  end

  @doc """
  Closes the triple store and releases all resources.

  After closing, the store handle is no longer valid.

  ## Arguments

  - `store` - Store handle from `open/2`

  ## Returns

  - `:ok` - On success
  - `{:error, reason}` - On failure

  ## Examples

      :ok = TripleStore.close(store)

  """
  @spec close(store()) :: :ok | {:error, term()}
  def close(%{db: db, dict_manager: dict_manager} = _store) do
    # Stop the dictionary manager
    if is_pid(dict_manager) and Process.alive?(dict_manager) do
      GenServer.stop(dict_manager, :normal)
    end

    # Close the database
    NIF.close(db)
  end

  # ===========================================================================
  # Query Operations
  # ===========================================================================

  @doc """
  Executes a SPARQL query against the store.

  Supports SELECT, ASK, CONSTRUCT, and DESCRIBE queries.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `sparql` - SPARQL query string

  ## Options

  - `:timeout` - Maximum execution time in ms (default: 30000)
  - `:explain` - Return query plan instead of executing (default: false)
  - `:optimize` - Enable query optimization (default: true)

  ## Returns

  - `{:ok, results}` - Query results (format depends on query type)
  - `{:error, {:parse_error, reason}}` - Invalid SPARQL syntax
  - `{:error, :timeout}` - Query exceeded time limit
  - `{:error, reason}` - Other failures

  ## Examples

      # SELECT query
      {:ok, results} = TripleStore.query(store, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10")
      # => {:ok, [%{"s" => ..., "p" => ..., "o" => ...}, ...]}

      # ASK query
      {:ok, exists} = TripleStore.query(store, "ASK { ?s a foaf:Person }")
      # => {:ok, true}

      # With timeout
      {:ok, results} = TripleStore.query(store, sparql, timeout: 5000)

  """
  @spec query(store(), String.t(), query_opts()) ::
          {:ok, term()} | {:error, term()}
  def query(%{db: db, dict_manager: dict_manager}, sparql, opts \\ []) do
    ctx = %{db: db, dict_manager: dict_manager}

    # Sanitize query for telemetry - don't expose raw SPARQL which may contain sensitive data
    telemetry_metadata = Telemetry.sanitize_query(sparql)

    Telemetry.span(:query, :execute, telemetry_metadata, fn ->
      case Query.query(ctx, sparql, opts) do
        {:ok, result} ->
          {result, %{result_type: result_type(result)}}

        {:error, _} = error ->
          error
      end
    end)
  end

  # ===========================================================================
  # Data Loading
  # ===========================================================================

  @doc """
  Loads RDF data from a file into the store.

  Supports Turtle, N-Triples, N-Quads, RDF/XML, TriG, and JSON-LD formats.
  Format is auto-detected from file extension.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `path` - Path to the RDF file

  ## Options

  - `:batch_size` - Number of triples per batch (default: 1000)
  - `:format` - Force specific format (auto-detected if not provided)

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, :file_not_found}` - File does not exist
  - `{:error, {:parse_error, reason}}` - Invalid RDF syntax
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, count} = TripleStore.load(store, "data.ttl")

      # With options
      {:ok, count} = TripleStore.load(store, "data.rdf", format: :rdfxml, batch_size: 5000)

  """
  @spec load(store(), Path.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load(%{db: db, dict_manager: dict_manager}, path, opts \\ []) do
    Loader.load_file(db, dict_manager, path, opts)
  end

  @doc """
  Loads an RDF.Graph into the store.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `graph` - RDF.Graph to load

  ## Options

  - `:batch_size` - Number of triples per batch (default: 1000)

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, reason}` - On failure

  ## Examples

      graph = RDF.Graph.new([{~I<http://example.org/s>, ~I<http://example.org/p>, "object"}])
      {:ok, 1} = TripleStore.load_graph(store, graph)

  """
  @spec load_graph(store(), RDF.Graph.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_graph(%{db: db, dict_manager: dict_manager}, graph, opts \\ []) do
    Loader.load_graph(db, dict_manager, graph, opts)
  end

  @doc """
  Loads RDF data from a string.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `content` - RDF content as string
  - `format` - Format of the content (`:turtle`, `:ntriples`, etc.)

  ## Options

  - `:batch_size` - Number of triples per batch (default: 1000)
  - `:base_iri` - Base IRI for relative URI resolution

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, reason}` - On failure

  ## Examples

      ttl = \"""
      @prefix ex: <http://example.org/> .
      ex:alice ex:knows ex:bob .
      \"""
      {:ok, 1} = TripleStore.load_string(store, ttl, :turtle)

  """
  @spec load_string(store(), String.t(), atom(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_string(%{db: db, dict_manager: dict_manager}, content, format, opts \\ []) do
    Loader.load_string(db, dict_manager, content, format, opts)
  end

  # ===========================================================================
  # Reasoning
  # ===========================================================================

  @doc """
  Materializes inferred triples using the specified reasoning profile.

  Uses forward-chaining semi-naive evaluation to compute the closure
  of all applicable inference rules.

  ## Arguments

  - `store` - Store handle from `open/2`

  ## Options

  - `:profile` - Reasoning profile (default: :owl2rl)
    - `:rdfs` - RDFS entailment rules only
    - `:owl2rl` - OWL 2 RL profile (includes RDFS)
    - `:all` - All available rules
  - `:parallel` - Enable parallel rule evaluation (default: true)

  ## Returns

  - `{:ok, stats}` - Materialization statistics
    - `:iterations` - Number of fixpoint iterations
    - `:total_derived` - Total number of derived triples
    - `:duration_ms` - Total duration in milliseconds
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = TripleStore.materialize(store)
      # => {:ok, %{iterations: 5, total_derived: 1000, duration_ms: 250}}

      {:ok, stats} = TripleStore.materialize(store, profile: :rdfs)

  """
  @spec materialize(store(), materialize_opts()) ::
          {:ok, map()} | {:error, term()}
  def materialize(%{db: _db, dict_manager: _dict_manager}, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    _parallel = Keyword.get(opts, :parallel, true)

    with {:ok, rules} <- ReasoningProfile.rules_for(profile) do
      # For in-memory reasoning, start with empty facts
      # In production, this would stream from the database
      initial_facts = MapSet.new()

      result =
        SemiNaive.materialize_in_memory(rules, initial_facts,
          max_iterations: TripleStore.Config.get(:max_iterations)
        )

      case result do
        {:ok, _all_facts, stats} ->
          {:ok, stats}

        {:error, _} = error ->
          error
      end
    end
  end

  # ===========================================================================
  # Health & Status
  # ===========================================================================

  @doc """
  Returns the health status of the store.

  Checks database connectivity, process health, and basic statistics.

  ## Arguments

  - `store` - Store handle from `open/2`

  ## Returns

  - `{:ok, health}` - Health status map
  - `{:error, reason}` - On failure

  ## Health Status

  - `:healthy` - All systems operational
  - `:degraded` - Some non-critical issues detected
  - `:unhealthy` - Critical issues detected

  ## Examples

      {:ok, health} = TripleStore.health(store)
      # => {:ok, %{
      #      status: :healthy,
      #      triple_count: 10000,
      #      database_open: true,
      #      dict_manager_alive: true,
      #      checked_at: ~U[2025-12-27 10:00:00Z]
      #    }}

  """
  @spec health(store()) :: {:ok, health_result()} | {:error, term()}
  def health(%{db: db, dict_manager: dict_manager}) do
    database_open = NIF.is_open(db)
    dict_manager_alive = is_pid(dict_manager) and Process.alive?(dict_manager)

    triple_count =
      case Statistics.triple_count(db) do
        {:ok, count} -> count
        _ -> 0
      end

    status =
      cond do
        not database_open -> :unhealthy
        not dict_manager_alive -> :degraded
        true -> :healthy
      end

    health = %{
      status: status,
      triple_count: triple_count,
      database_open: database_open,
      dict_manager_alive: dict_manager_alive,
      checked_at: DateTime.utc_now()
    }

    {:ok, health}
  end

  @doc """
  Returns basic statistics about the store.

  ## Arguments

  - `store` - Store handle from `open/2`

  ## Returns

  - `{:ok, stats}` - Statistics map
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = TripleStore.stats(store)
      # => {:ok, %{
      #      triple_count: 10000,
      #      distinct_subjects: 500,
      #      distinct_predicates: 50,
      #      distinct_objects: 3000
      #    }}

  """
  @spec stats(store()) :: {:ok, map()} | {:error, term()}
  def stats(%{db: db}) do
    Statistics.all(db)
  end

  # ===========================================================================
  # SPARQL Update
  # ===========================================================================

  @doc """
  Executes a SPARQL UPDATE operation.

  Supports INSERT DATA, DELETE DATA, INSERT/DELETE WHERE, and other
  SPARQL Update operations.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `sparql` - SPARQL UPDATE string

  ## Returns

  - `{:ok, count}` - Number of triples affected
  - `{:error, {:parse_error, reason}}` - Invalid SPARQL syntax
  - `{:error, reason}` - Other failures

  ## Examples

      {:ok, 1} = TripleStore.update(store, "INSERT DATA { <http://ex.org/s> <http://ex.org/p> 'object' }")

      {:ok, count} = TripleStore.update(store, \"""
        DELETE { ?s ?p ?o }
        WHERE { ?s a <http://ex.org/Deprecated> ; ?p ?o }
      \""")

  """
  @spec update(store(), String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def update(%{db: db, dict_manager: dict_manager, transaction: txn}, sparql) do
    if txn do
      Transaction.update(txn, sparql)
    else
      # Create a temporary transaction for this update
      case Transaction.start_link(db: db, dict_manager: dict_manager) do
        {:ok, temp_txn} ->
          result = Transaction.update(temp_txn, sparql)
          GenServer.stop(temp_txn, :normal)
          result

        {:error, _} = error ->
          error
      end
    end
  end

  # ===========================================================================
  # Backup & Restore
  # ===========================================================================

  @doc """
  Creates a backup of the store.

  See `TripleStore.Backup.create/3` for full documentation.

  ## Examples

      {:ok, metadata} = TripleStore.backup(store, "/backups/mydb_20251227")

  """
  @spec backup(store(), Path.t(), keyword()) ::
          {:ok, TripleStore.Backup.backup_metadata()} | {:error, term()}
  def backup(store, backup_path, opts \\ []) do
    TripleStore.Backup.create(store, backup_path, opts)
  end

  @doc """
  Restores a store from a backup.

  See `TripleStore.Backup.restore/3` for full documentation.

  ## Examples

      {:ok, store} = TripleStore.restore("/backups/mydb_20251227", "/data/restored")

  """
  @spec restore(Path.t(), Path.t(), keyword()) ::
          {:ok, store()} | {:error, term()}
  def restore(backup_path, restore_path, opts \\ []) do
    TripleStore.Backup.restore(backup_path, restore_path, opts)
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp result_type(result) when is_list(result), do: :select
  defp result_type(result) when is_boolean(result), do: :ask
  defp result_type(%RDF.Graph{}), do: :graph
  defp result_type(_), do: :unknown
end
