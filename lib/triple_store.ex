defmodule TripleStore do
  @moduledoc """
  A high-performance RDF triple store with SPARQL 1.1 and OWL 2 RL reasoning.

  This module provides the unified public API for the triple store. It composes
  the lower-level modules (Loader, Query, Transaction, Reasoner) into a
  convenient interface.

  ## Quick Start

      # Open a store
      {:ok, store} = TripleStore.open("./data")

      # Load RDF data from file
      {:ok, count} = TripleStore.load(store, "ontology.ttl")

      # Or insert triples directly
      {:ok, 1} = TripleStore.insert(store, {~I<http://ex.org/alice>, ~I<http://ex.org/knows>, ~I<http://ex.org/bob>})

      # Query with SPARQL
      {:ok, results} = TripleStore.query(store, "SELECT ?s WHERE { ?s a foaf:Person }")

      # Update with SPARQL UPDATE
      {:ok, count} = TripleStore.update(store, "INSERT DATA { <http://ex.org/new> <http://ex.org/p> 'value' }")

      # Enable OWL 2 RL reasoning
      {:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)

      # Export data
      {:ok, graph} = TripleStore.export(store, :graph)

      # Backup and restore
      {:ok, metadata} = TripleStore.backup(store, "/backups/mydb")

      # Check health and stats
      {:ok, health} = TripleStore.health(store)
      {:ok, stats} = TripleStore.stats(store)

      # Close when done
      :ok = TripleStore.close(store)

  ## Public API Reference

  ### Store Lifecycle
  - `open/2` - Open or create a triple store
  - `close/1` - Close the store and release resources

  ### Data Loading
  - `load/2` - Load RDF from a file (Turtle, N-Triples, etc.)
  - `load_graph/3` - Load an `RDF.Graph` directly
  - `load_string/4` - Load RDF from a string

  ### Triple Operations
  - `insert/2` - Insert one or more triples
  - `delete/2` - Delete one or more triples

  ### Querying
  - `query/2` - Execute SPARQL SELECT/ASK/CONSTRUCT queries
  - `update/2` - Execute SPARQL UPDATE operations

  ### Data Export
  - `export/2` - Export triples to graph, file, or string

  ### Reasoning
  - `materialize/2` - Compute OWL 2 RL inferences
  - `reasoning_status/1` - Get reasoning subsystem status

  ### Operations
  - `backup/2` - Create a backup of the store
  - `restore/2` - Restore from a backup

  ### Monitoring
  - `health/1` - Get health status
  - `stats/1` - Get store statistics

  ### Bang Variants
  All functions that return `{:ok, result}` or `{:error, reason}` have
  corresponding `!` variants that return the result directly or raise
  `TripleStore.Error`:
  - `open!/2`, `load!/3`, `query!/3`, `update!/2`
  - `insert!/2`, `delete!/2`, `export!/3`
  - `materialize!/2`, `reasoning_status!/1`
  - `health!/1`, `stats!/1`, `backup!/3`, `restore!/3`

  ## Architecture

  The triple store uses:
  - **RocksDB** for persistent storage via Rustler NIFs
  - **Dictionary encoding** for compact term representation (64-bit IDs)
  - **SPO/POS/OSP indices** for O(log n) pattern matching
  - **Forward-chaining materialization** for OWL 2 RL reasoning
  - **Semi-naive evaluation** for efficient fixpoint computation

  ## Store Handle

  The store handle returned by `open/2` is a map containing:
  - `:db` - Database reference for RocksDB operations
  - `:dict_manager` - Dictionary manager PID for term encoding/decoding
  - `:transaction` - Transaction manager (if active)
  - `:path` - Path to the database directory

  ## Thread Safety

  - **Reads** can be executed concurrently from multiple processes
  - **Writes** are serialized through the Transaction manager
  - **Snapshot isolation** ensures consistent reads during writes
  - The store handle can be safely shared between processes

  ## Error Handling

  All functions return tagged tuples:
  - `{:ok, result}` on success
  - `{:error, reason}` on failure

  Common error reasons:
  - `:database_closed` - Store was closed
  - `:parse_error` - Invalid SPARQL syntax
  - `:timeout` - Query exceeded time limit
  - `:file_not_found` - RDF file does not exist
  - `:path_traversal_attempt` - Security: path contains `..`

  ## Supported RDF Formats

  The following formats are supported for loading and exporting:
  - **Turtle** (`.ttl`) - Recommended for human-readable RDF
  - **N-Triples** (`.nt`) - Line-based format for streaming
  - **N-Quads** (`.nq`) - N-Triples with named graphs (default graph only)
  - **TriG** (`.trig`) - Turtle with named graphs (default graph only)
  - **RDF/XML** (`.rdf`) - XML-based format (requires optional dependency)
  - **JSON-LD** (`.jsonld`) - JSON-based format (requires optional dependency)

  ## SPARQL Support

  Full SPARQL 1.1 Query and Update support including:
  - SELECT, ASK, CONSTRUCT, DESCRIBE queries
  - INSERT DATA, DELETE DATA, INSERT/DELETE WHERE updates
  - FILTER, OPTIONAL, UNION, MINUS, VALUES
  - Aggregates (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)
  - Subqueries and property paths

  ## Reasoning Profiles

  Available reasoning profiles for `materialize/2`:
  - `:rdfs` - RDFS entailment rules (subclass, subproperty, domain, range)
  - `:owl2rl` - OWL 2 RL profile (includes RDFS plus OWL rules)
  - `:all` - All available reasoning rules
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Dictionary.ShardedManager
  alias TripleStore.Loader
  alias TripleStore.Reasoner.ReasoningProfile
  alias TripleStore.Reasoner.ReasoningStatus
  alias TripleStore.Reasoner.SemiNaive
  alias TripleStore.SPARQL.Query
  alias TripleStore.Statistics
  alias TripleStore.Telemetry
  alias TripleStore.Transaction

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
          create_if_missing: boolean(),
          dictionary_shards: pos_integer() | nil
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
  - `:dictionary_shards` - Number of dictionary manager shards for parallel
    term encoding. When set to a value > 1, uses `ShardedManager` instead of
    the single `Manager` GenServer. Recommended for bulk loading workloads.
    Default: nil (uses single Manager)

  ## Returns

  - `{:ok, store}` - Store handle for subsequent operations
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, store} = TripleStore.open("./data")

      # With options
      {:ok, store} = TripleStore.open("./data", create_if_missing: true)

      # With sharded dictionary for bulk loading (uses CPU core count shards)
      {:ok, store} = TripleStore.open("./data", dictionary_shards: System.schedulers_online())

  """
  @spec open(Path.t(), open_opts()) :: {:ok, store()} | {:error, term()}
  def open(path, opts \\ []) do
    create_if_missing = Keyword.get(opts, :create_if_missing, true)
    dictionary_shards = Keyword.get(opts, :dictionary_shards)

    with :ok <- validate_path(path) do
      Telemetry.span(:store, :open, %{path: Path.basename(path)}, fn ->
        # Check if path exists when create_if_missing is false
        if not create_if_missing and not File.exists?(path) do
          {{:error, :database_not_found}, %{}}
        else
          with {:ok, db} <- NIF.open(path),
               {:ok, dict_manager} <- start_dict_manager(db, dictionary_shards) do
            store = %{
              db: db,
              dict_manager: dict_manager,
              transaction: nil,
              path: path
            }

            {{:ok, store}, %{}}
          end
        end
      end)
    end
  end

  # Starts either ShardedManager or regular Manager based on shard count
  defp start_dict_manager(db, nil), do: DictManager.start_link(db: db)
  defp start_dict_manager(db, 1), do: DictManager.start_link(db: db)

  defp start_dict_manager(db, shard_count) when is_integer(shard_count) and shard_count > 1 do
    ShardedManager.start_link(db: db, shards: shard_count)
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
    # ShardedManager is a Supervisor, Manager is a GenServer - handle both
    if is_pid(dict_manager) and Process.alive?(dict_manager) do
      # Check if it's a Supervisor by looking at the initial_call in process info
      case Process.info(dict_manager, :dictionary) do
        {:dictionary, dict} ->
          initial_call = Keyword.get(dict, :"$initial_call", nil)

          if initial_call == {:supervisor, Supervisor.Default, 1} do
            # It's a Supervisor (ShardedManager) - use its stop function
            ShardedManager.stop(dict_manager)
          else
            # It's a GenServer (Manager) - use GenServer.stop
            GenServer.stop(dict_manager, :normal)
          end

        nil ->
          # Process may have exited, try GenServer.stop as fallback
          try do
            GenServer.stop(dict_manager, :normal)
          catch
            :exit, _ -> :ok
          end
      end
    end

    # Close the database
    NIF.close(db)
  end

  @doc """
  Closes the triple store, raising on error.

  See `close/1` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      TripleStore.close!(store)

  """
  @spec close!(store()) :: :ok
  def close!(store) do
    case close(store) do
      :ok -> :ok
      {:error, reason} -> raise error_for(reason, :database_io_error)
    end
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
          {{:ok, result}, %{result_type: result_type(result)}}

        {:error, _} = error ->
          {error, %{}}
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
  # Triple Operations
  # ===========================================================================

  @doc """
  Inserts RDF triples into the store.

  Accepts either a single RDF triple or a list of triples. Triples are
  dictionary-encoded and indexed in all three indices (SPO, POS, OSP).

  ## Arguments

  - `store` - Store handle from `open/2`
  - `triples` - A single triple or list of triples

  ## Triple Formats

  Triples can be provided as:
  - 3-tuples: `{subject, predicate, object}`
  - RDF.Description structs
  - RDF.Graph structs

  Where subject, predicate, and object are RDF.ex terms:
  - IRIs: `~I<http://example.org/resource>`
  - Blank nodes: `~B<b1>`
  - Literals: `~L"value"` or `RDF.literal("value", datatype: XSD.string)`

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, reason}` - On failure

  ## Examples

      # Single triple
      {:ok, 1} = TripleStore.insert(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})

      # Multiple triples
      triples = [
        {~I<http://ex.org/s1>, ~I<http://ex.org/p>, ~L"value1"},
        {~I<http://ex.org/s2>, ~I<http://ex.org/p>, ~L"value2"}
      ]
      {:ok, 2} = TripleStore.insert(store, triples)

      # From RDF.Graph
      graph = RDF.Graph.new([{~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"}])
      {:ok, 1} = TripleStore.insert(store, graph)

  """
  @spec insert(store(), RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def insert(%{db: db, dict_manager: dict_manager}, triples) do
    Loader.insert(db, dict_manager, triples)
  end

  @doc """
  Deletes RDF triples from the store.

  Removes triples matching the given patterns from all indices.
  Accepts the same formats as `insert/2`.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `triples` - A single triple or list of triples to delete

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, reason}` - On failure

  ## Examples

      # Delete single triple
      {:ok, 1} = TripleStore.delete(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})

      # Delete multiple triples
      {:ok, count} = TripleStore.delete(store, triples)

  ## Note

  Deleting triples does not automatically update materialized inferences.
  Call `materialize/2` to recompute the closure after significant deletions.

  """
  @spec delete(store(), RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete(%{db: db, dict_manager: dict_manager}, triples) do
    Loader.delete(db, dict_manager, triples)
  end

  # ===========================================================================
  # Data Export
  # ===========================================================================

  @doc """
  Exports triples from the store.

  Supports exporting to RDF.Graph, files, or strings in various RDF formats.

  ## Arguments

  - `store` - Store handle from `open/2`
  - `target` - Export target (see below)

  ## Targets

  - `:graph` - Returns an RDF.Graph with all triples
  - `{:file, path, format}` - Writes to file in specified format
  - `{:string, format}` - Returns serialized string

  ## Formats

  - `:turtle` - Turtle format (.ttl)
  - `:ntriples` - N-Triples format (.nt)
  - `:nquads` - N-Quads format (.nq)

  ## Options

  - `:pattern` - Triple pattern to filter exports (default: all)
  - `:prefixes` - Prefix map for serialization
  - `:base_iri` - Base IRI for relative URIs

  ## Returns

  - `{:ok, RDF.Graph.t()}` - For `:graph` target
  - `{:ok, count}` - For file targets (number of triples exported)
  - `{:ok, string}` - For string targets
  - `{:error, reason}` - On failure

  ## Examples

      # Export as graph
      {:ok, graph} = TripleStore.export(store, :graph)

      # Export to file
      {:ok, count} = TripleStore.export(store, {:file, "data.ttl", :turtle})

      # Export as string
      {:ok, ttl} = TripleStore.export(store, {:string, :turtle})

      # Export with pattern filter
      {:ok, graph} = TripleStore.export(store, :graph, pattern: {:var, {:bound, pred_id}, :var})

  """
  @spec export(store(), :graph | {:file, Path.t(), atom()} | {:string, atom()}, keyword()) ::
          {:ok, RDF.Graph.t() | non_neg_integer() | String.t()} | {:error, term()}
  def export(store, target, opts \\ [])

  def export(%{db: db}, :graph, opts) do
    pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
    graph_opts = Keyword.take(opts, [:name, :base_iri, :prefixes])
    TripleStore.Exporter.export_graph(db, pattern, graph_opts)
  end

  def export(%{db: db}, {:file, path, format}, opts) do
    TripleStore.Exporter.export_file(db, path, format, opts)
  end

  def export(%{db: db}, {:string, format}, opts) do
    TripleStore.Exporter.export_string(db, format, opts)
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
  def materialize(%{db: db, dict_manager: _dict_manager}, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    _parallel = Keyword.get(opts, :parallel, true)

    with {:ok, rules} <- ReasoningProfile.rules_for(profile),
         {:ok, initial_facts} <- load_facts_from_db(db) do
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

  # Load all triples from the database as facts for reasoning
  defp load_facts_from_db(db) do
    case TripleStore.Index.lookup_all(db, {:var, :var, :var}) do
      {:ok, triples} ->
        # Convert internal triple list to MapSet of tuples
        facts = MapSet.new(triples)
        {:ok, facts}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the current reasoning status.

  Provides information about the reasoning subsystem including:
  - Active profile and mode
  - Derived triple counts
  - Materialization history
  - Whether rematerialization is needed

  ## Arguments

  - `store` - Store handle from `open/2`

  ## Returns

  - `{:ok, status}` - Reasoning status map
  - `{:error, reason}` - On failure

  ## Status Fields

  - `:state` - Current state (`:initialized`, `:materialized`, `:stale`, `:error`)
  - `:profile` - Active reasoning profile (`:rdfs`, `:owl2rl`, etc.)
  - `:mode` - Reasoning mode (`:materialized`, `:hybrid`, etc.)
  - `:derived_count` - Number of derived triples
  - `:explicit_count` - Number of explicit triples
  - `:total_count` - Total triple count
  - `:last_materialization` - Timestamp of last materialization
  - `:needs_rematerialization` - Whether rematerialization is needed

  ## Examples

      {:ok, status} = TripleStore.reasoning_status(store)
      # => {:ok, %{
      #      state: :materialized,
      #      profile: :owl2rl,
      #      derived_count: 1500,
      #      explicit_count: 5000,
      #      total_count: 6500,
      #      last_materialization: ~U[2025-12-28 10:00:00Z],
      #      needs_rematerialization: false
      #    }}

  """
  @spec reasoning_status(store()) :: {:ok, map()} | {:error, term()}
  def reasoning_status(%{path: path}) do
    # Use path-based key for status lookup
    key = path_to_status_key(path)

    case ReasoningStatus.load(key) do
      {:ok, status} ->
        summary = ReasoningStatus.summary(status)

        result =
          Map.put(
            summary,
            :needs_rematerialization,
            ReasoningStatus.needs_rematerialization?(status)
          )

        {:ok, result}

      {:error, :not_found} ->
        # No status stored yet - return default
        {:ok,
         %{
           state: :initialized,
           profile: nil,
           mode: nil,
           derived_count: 0,
           explicit_count: 0,
           total_count: 0,
           last_materialization: nil,
           materialization_count: 0,
           last_materialization_stats: nil,
           needs_rematerialization: false,
           error: nil
         }}
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

  @doc """
  Starts scheduled periodic backups for a store.

  Creates a background process that runs backups at regular intervals,
  automatically rotating old backups beyond the specified limit.

  See `TripleStore.ScheduledBackup` for full documentation.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`
  - `backup_dir` - Directory to store backups
  - `opts` - Scheduling options

  ## Options

  - `:interval` - Backup interval in milliseconds (default: 1 hour)
  - `:max_backups` - Maximum backups to keep (default: 5)
  - `:prefix` - Backup name prefix (default: "scheduled")
  - `:run_immediately` - Run first backup immediately (default: false)

  ## Returns

  - `{:ok, pid}` - Scheduler process started
  - `{:error, reason}` - Failed to start scheduler

  ## Examples

      # Start hourly backups, keeping last 24
      {:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/mydb",
        interval: :timer.hours(1),
        max_backups: 24
      )

      # Check scheduler status
      {:ok, status} = TripleStore.ScheduledBackup.status(scheduler)

      # Stop scheduled backups
      :ok = TripleStore.ScheduledBackup.stop(scheduler)

  """
  @spec schedule_backup(store(), Path.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def schedule_backup(store, backup_dir, opts \\ []) do
    TripleStore.ScheduledBackup.start_link(
      Keyword.merge(opts, store: store, backup_dir: backup_dir)
    )
  end

  # ===========================================================================
  # Bang Variants (raise on error)
  # ===========================================================================

  @doc """
  Opens a triple store, raising on error.

  See `open/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      store = TripleStore.open!("./data")

  """
  @spec open!(Path.t(), open_opts()) :: store()
  def open!(path, opts \\ []) do
    unwrap_or_raise!(open(path, opts), :database_open_failed, path: path)
  end

  @doc """
  Loads RDF data from a file, raising on error.

  See `load/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.load!(store, "data.ttl")

  """
  @spec load!(store(), Path.t(), load_opts()) :: non_neg_integer()
  def load!(store, path, opts \\ []) do
    unwrap_or_raise!(load(store, path, opts), :validation_file_not_found, path: path)
  end

  @doc """
  Loads an RDF.Graph, raising on error.

  See `load_graph/3` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.load_graph!(store, graph)

  """
  @spec load_graph!(store(), RDF.Graph.t(), load_opts()) :: non_neg_integer()
  def load_graph!(store, graph, opts \\ []) do
    unwrap_or_raise!(load_graph(store, graph, opts), :validation_invalid_input, [])
  end

  @doc """
  Loads RDF from a string, raising on error.

  See `load_string/4` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.load_string!(store, ttl, :turtle)

  """
  @spec load_string!(store(), String.t(), atom(), load_opts()) :: non_neg_integer()
  def load_string!(store, content, format, opts \\ []) do
    unwrap_or_raise!(load_string(store, content, format, opts), :data_parse_error, [])
  end

  @doc """
  Executes a SPARQL query, raising on error.

  See `query/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      results = TripleStore.query!(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 10")

  """
  @spec query!(store(), String.t(), query_opts()) :: term()
  def query!(store, sparql, opts \\ []) do
    unwrap_or_raise!(query(store, sparql, opts), :query_parse_error, [])
  end

  @doc """
  Executes a SPARQL UPDATE, raising on error.

  See `update/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.update!(store, "INSERT DATA { <s> <p> 'o' }")

  """
  @spec update!(store(), String.t()) :: non_neg_integer()
  def update!(store, sparql) do
    unwrap_or_raise!(update(store, sparql), :query_parse_error, [])
  end

  @doc """
  Inserts triples, raising on error.

  See `insert/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.insert!(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})

  """
  @spec insert!(store(), RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()) ::
          non_neg_integer()
  def insert!(store, triples) do
    unwrap_or_raise!(insert(store, triples), :validation_invalid_input, [])
  end

  @doc """
  Deletes triples, raising on error.

  See `delete/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      count = TripleStore.delete!(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})

  """
  @spec delete!(store(), RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()) ::
          non_neg_integer()
  def delete!(store, triples) do
    unwrap_or_raise!(delete(store, triples), :validation_invalid_input, [])
  end

  @doc """
  Exports triples, raising on error.

  See `export/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      graph = TripleStore.export!(store, :graph)
      count = TripleStore.export!(store, {:file, "data.ttl", :turtle})

  """
  @spec export!(store(), :graph | {:file, Path.t(), atom()} | {:string, atom()}, keyword()) ::
          RDF.Graph.t() | non_neg_integer() | String.t()
  def export!(store, target, opts \\ []) do
    unwrap_or_raise!(export(store, target, opts), :database_io_error, [])
  end

  @doc """
  Materializes inferences, raising on error.

  See `materialize/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      stats = TripleStore.materialize!(store, profile: :owl2rl)

  """
  @spec materialize!(store(), materialize_opts()) :: map()
  def materialize!(store, opts \\ []) do
    unwrap_or_raise!(materialize(store, opts), :reasoning_rule_error, [])
  end

  @doc """
  Gets reasoning status, raising on error.

  See `reasoning_status/1` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      status = TripleStore.reasoning_status!(store)

  """
  @spec reasoning_status!(store()) :: map()
  def reasoning_status!(store) do
    # reasoning_status/1 always returns {:ok, _} so we can unwrap directly
    {:ok, status} = reasoning_status(store)
    status
  end

  @doc """
  Gets health status, raising on error.

  See `health/1` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      health = TripleStore.health!(store)

  """
  @spec health!(store()) :: health_result()
  def health!(store) do
    # health/1 always returns {:ok, _} so we can unwrap directly
    {:ok, health} = health(store)
    health
  end

  @doc """
  Gets store statistics, raising on error.

  See `stats/1` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      stats = TripleStore.stats!(store)

  """
  @spec stats!(store()) :: map()
  def stats!(store) do
    unwrap_or_raise!(stats(store), :system_internal_error, [])
  end

  @doc """
  Creates a backup, raising on error.

  See `backup/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      metadata = TripleStore.backup!(store, "/backups/mydb")

  """
  @spec backup!(store(), Path.t(), keyword()) :: TripleStore.Backup.backup_metadata()
  def backup!(store, backup_path, opts \\ []) do
    unwrap_or_raise!(backup(store, backup_path, opts), :database_io_error, path: backup_path)
  end

  @doc """
  Restores from backup, raising on error.

  See `restore/2` for details.

  ## Raises

  - `TripleStore.Error` on failure

  ## Examples

      store = TripleStore.restore!("/backups/mydb", "./restored")

  """
  @spec restore!(Path.t(), Path.t(), keyword()) :: store()
  def restore!(backup_path, restore_path, opts \\ []) do
    unwrap_or_raise!(restore(backup_path, restore_path, opts), :database_io_error,
      path: backup_path
    )
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp result_type(result) when is_list(result), do: :select
  defp result_type(result) when is_boolean(result), do: :ask
  defp result_type(%RDF.Graph{}), do: :graph
  defp result_type(_), do: :unknown

  # Unwrap an ok/error tuple or raise a structured error.
  # Used by bang variants to reduce boilerplate.
  defp unwrap_or_raise!({:ok, result}, _category, _opts), do: result

  defp unwrap_or_raise!({:error, reason}, category, opts) do
    raise error_for(reason, category, opts)
  end

  # Validate path to prevent path traversal attacks
  defp validate_path(path) when is_binary(path) do
    if String.contains?(path, "..") do
      {:error, :path_traversal_attempt}
    else
      :ok
    end
  end

  # Convert a store path to a binary key for status lookup.
  # Uses ETS-compatible binary keys to avoid atom table exhaustion.
  defp path_to_status_key(path) when is_binary(path) do
    hash = :erlang.phash2(path)
    "triple_store_status_#{hash}"
  end

  # Convert raw error reasons to structured TripleStore.Error exceptions.
  # Delegates to the centralized Error.from_reason/2 for consistency.
  # The default_category is only used if the reason doesn't have its own
  # natural category (e.g., generic atom/tuple errors).
  defp error_for(reason, default_category, opts \\ [])

  defp error_for(%TripleStore.Error{} = error, _default, _opts), do: error

  defp error_for(reason, default_category, opts) do
    # Only pass the category to from_reason for reasons that don't have
    # their own natural category mapping. Known reasons like :timeout,
    # :database_closed, :path_traversal_attempt, etc. have their own
    # categories in Error.from_reason and shouldn't be overridden.
    error_opts =
      if has_natural_category?(reason) do
        maybe_add_details(opts, opts)
      else
        opts
        |> Keyword.put_new(:category, default_category)
        |> maybe_add_details(opts)
      end

    TripleStore.Error.from_reason(reason, error_opts)
  end

  # Reasons that have their own natural category mapping in Error.from_reason
  defp has_natural_category?(:timeout), do: true
  defp has_natural_category?(:database_closed), do: true
  defp has_natural_category?(:file_not_found), do: true
  defp has_natural_category?(:path_traversal_attempt), do: true
  defp has_natural_category?(:database_not_found), do: true
  defp has_natural_category?(:max_iterations_exceeded), do: true
  defp has_natural_category?({:parse_error, _}), do: true
  defp has_natural_category?({:file_not_found, _}), do: true
  defp has_natural_category?({:invalid_format, _}), do: true
  defp has_natural_category?({:io_error, _}), do: true
  defp has_natural_category?(_), do: false

  # Merge any path or other metadata into details
  defp maybe_add_details(error_opts, opts) do
    path = Keyword.get(opts, :path)

    if path do
      Keyword.update(error_opts, :details, %{path: path}, &Map.put(&1, :path, path))
    else
      error_opts
    end
  end
end
