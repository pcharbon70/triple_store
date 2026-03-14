defmodule TripleStore.Loader do
  @moduledoc """
  Bulk loading pipeline for efficient RDF data ingestion.

  Provides high-performance loading of RDF data from graphs and files
  using batched writes and progress reporting via Telemetry.

  ## Features

  - **Batched writes**: Groups triples into configurable batch sizes (default 10,000)
  - **Dynamic batch sizing**: Automatically adjusts batch size based on memory budget
  - **Parallel processing**: Flow-based pipeline with configurable stage count
  - **Sequential fallback**: Uses `Enum.reduce_while` when parallel mode disabled
  - **Progress reporting**: Emits Telemetry events for monitoring
  - **Format support**: Turtle, N-Triples, N-Quads, RDF/XML, TriG, JSON-LD
  - **Named graphs**: Full quad support for loading data into specific graphs
  - **Path validation**: File paths are validated to prevent path traversal attacks
  - **File size limits**: Configurable maximum file size (default 100MB)

  ## Batch Size Configuration

  The batch size controls how many triples are processed together before writing
  to the database. Larger batches reduce NIF round-trip overhead but use more memory.

  | Scenario | Recommended | Memory Usage |
  |----------|-------------|--------------|
  | Low memory (<4GB) | 5,000 | ~360 KB |
  | Standard (4-16GB) | 10,000 | ~720 KB |
  | High memory (>16GB) | 50,000 | ~3.6 MB |
  | Bulk import | 100,000 | ~7.2 MB |

  Memory usage estimate: batch_size × 3 indices × 24 bytes per key ≈ 72 bytes/triple

  ## Quad and Graph Support

  The loader supports both triple and quad formats. For quad-capable stores:

  - **N-Quads (`.nq`)**: Loads all quads including named graphs
  - **TriG (`.trig`)**: Loads all quads including named graphs
  - **Turtle/N-Triples**: Can be loaded to specific named graphs using `:graph` option

  ### Loading to Named Graphs

  For formats without native graph support (Turtle, N-Triples), use the `:graph` option:

      # Load Turtle file to named graph
      {:ok, count} = Loader.load_file(db, manager, "data.ttl",
        graph: RDF.iri("http://example.org/mygraph"))

      # Load to default graph explicitly
      {:ok, count} = Loader.load_file(db, manager, "data.ttl",
        graph: :default)

  ### Graph-Scoped Loading Functions

  For more advanced graph-scoped loading, see:

  - `load_to_graph/5` - Load a file to a specific named graph
  - `load_files_to_graphs/3` - Load multiple files to multiple graphs

  ### Memory Budget Options

  Use the `:memory_budget` option for automatic batch sizing:

  - `:low` - 5,000 triples/batch (for memory-constrained systems)
  - `:medium` - 10,000 triples/batch (default, balanced)
  - `:high` - 50,000 triples/batch (for systems with ample memory)
  - `:auto` - Detects system memory and selects appropriate size

  ## Parallel Loading

  Use the `:parallel` option to enable Flow-based parallel processing:

  - `:parallel` - Enable parallel encoding (default: `true`)
  - `:stages` - Number of parallel encoding stages (default: `System.schedulers_online()`)
  - `:max_demand` - Maximum demand per stage for backpressure (default: 5)

  The parallel pipeline overlaps dictionary encoding (CPU-bound) with index writing
  (I/O-bound), improving throughput on multi-core systems.

  ## Progress Reporting

  Use the `:progress_callback` option to monitor long-running bulk loads:

  - `:progress_callback` - Function called periodically with progress info
  - `:progress_interval` - Call callback every N batches (default: 10, min: 1)

  The callback receives a map with:
  - `triples_loaded` - Number of triples loaded so far
  - `batch_number` - Current batch number (1-indexed)
  - `elapsed_ms` - Elapsed time in milliseconds
  - `rate_per_second` - Current loading rate (triples/second)

  Return `:continue` to proceed or `:halt` to cancel loading.

  **Note**: Progress callbacks are invoked synchronously within the loading pipeline.
  Long-running callbacks will slow down the loading process. If you need to perform
  expensive operations (e.g., database writes, network calls), consider using
  `send/2` to dispatch work to a separate process.

  ### Example

      Loader.load_file(db, manager, "large_file.ttl",
        progress_callback: fn info ->
          IO.puts("Loaded \#{info.triples_loaded} triples (\#{info.rate_per_second}/s)")
          :continue
        end,
        progress_interval: 5
      )

  ## Bulk Loading Mode

  For large data imports, use `:bulk_mode` to optimize for throughput over
  immediate durability:

      {:ok, count} = Loader.load_file(db, manager, "large.ttl", bulk_mode: true)

  Bulk mode enables these optimizations:

  - **Deferred sync**: Uses `sync: false` for writes, avoiding per-batch fsync
  - **Larger batches**: Uses 50,000 triples per batch (vs 10,000 default)
  - **Final sync**: Calls `flush_wal(true)` after load completes for durability

  ### Durability Trade-offs

  With bulk mode enabled:
  - **Process crash**: Data is safe (WAL still written, just not fsync'd)
  - **OS/power failure**: May lose last few batches written before failure

  This is acceptable for bulk imports because:
  1. You can restart the import if it fails
  2. The final sync ensures durability once loading completes
  3. Performance gain is typically 10-50x faster for large imports

  ### Error Handling

  If the final `flush_wal` fails after a successful bulk load:
  - Returns `{:error, {:flush_failed, count, reason}}`
  - `count` indicates the number of triples that were written
  - Data is still in the WAL and will survive process restart
  - Only OS crash or power failure before OS buffer flush can lose data

  ### Dictionary Sync Behavior

  Dictionary encoding operations (term -> ID mappings) are performed before
  index writes. In bulk mode, both dictionary and index writes use the same
  deferred sync settings. The final `flush_wal(true)` call flushes all column
  families, ensuring both dictionary entries and index entries are durable.

  ## High-Volume Bulk Loading

  For bulk loads exceeding 100,000 triples, consider these optimizations:

  1. **Enable bulk mode**: For maximum throughput:

         Loader.load_file(db, manager, "large.ttl", bulk_mode: true)

  2. **Use ShardedManager**: Configure `dictionary_shards` in `TripleStore.open/2`
     to parallelize dictionary encoding across multiple processes:

         {:ok, store} = TripleStore.open(path, dictionary_shards: 8)

  3. **Increase batch size**: Use `:memory_budget` or explicit `:batch_size`:

         Loader.load_file(db, manager, "large.ttl", memory_budget: :high)

  4. **Tune stage count**: Match stages to CPU cores:

         Loader.load_file(db, manager, "large.ttl", stages: 8)

  ## Batch Size Limits

  The minimum batch size is 100 triples. Values below this will be clamped and
  a warning will be logged. This ensures efficient use of RocksDB WriteBatch
  operations. The maximum batch size is 100,000 triples.

  ## Important Limitations

  ### Triple Store Mode

  When using the triple store schema (default), only the default graph is supported.
  Named graphs require the quad store schema (`schema: :quad` option when opening the database).

  ## Telemetry Events

  The loader emits the following telemetry events:

  - `[:triple_store, :loader, :start]` - When loading begins
    - Metadata: `%{source: :graph | :file, path: String.t() | nil}`

  - `[:triple_store, :loader, :batch]` - After each batch is written
    - Measurements: `%{count: integer, duration: integer}`
    - Metadata: `%{batch_number: integer, sync: boolean}`

  - `[:triple_store, :loader, :stop]` - When loading completes
    - Measurements: `%{total_count: integer, duration: integer}`
    - Metadata: `%{source: :graph | :file}`

  - `[:triple_store, :loader, :exception]` - On error
    - Metadata: `%{kind: :error | :exit | :throw, reason: term}`

  ## Usage

      # Load from an RDF.Graph (to default graph)
      {:ok, count} = Loader.load_graph(db, manager, graph)

      # Load from a file (format auto-detected)
      {:ok, count} = Loader.load_file(db, manager, "data.ttl")

      # Load to named graph
      {:ok, count} = Loader.load_file(db, manager, "data.ttl",
        graph: RDF.iri("http://example.org/mygraph"))

      # With custom options
      {:ok, count} = Loader.load_graph(db, manager, graph,
        batch_size: 5000,
        stages: 4
      )
  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.ShardedManager
  alias TripleStore.Index

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Dictionary manager process"
  @type manager :: Manager.manager() | ShardedManager.t()

  @typedoc "Memory budget options for automatic batch sizing"
  @type memory_budget :: :low | :medium | :high | :auto

  @typedoc """
  Progress information passed to progress callbacks.

  Fields:
  - `triples_loaded` - Number of triples loaded so far
  - `batch_number` - Current batch number (1-indexed)
  - `elapsed_ms` - Elapsed time in milliseconds since loading started
  - `rate_per_second` - Current loading rate (triples/second)
  """
  @type progress_info :: %{
          triples_loaded: non_neg_integer(),
          batch_number: pos_integer(),
          elapsed_ms: non_neg_integer(),
          rate_per_second: float()
        }

  @typedoc """
  Progress callback function type.

  Called periodically during loading to report progress.
  Return `:continue` to continue loading, or `:halt` to cancel.
  """
  @type progress_callback :: (progress_info() -> :continue | :halt)

  @typedoc "Loading options"
  @type load_opts :: [
          batch_size: pos_integer(),
          memory_budget: memory_budget(),
          bulk_mode: boolean(),
          parallel: boolean(),
          stages: pos_integer(),
          max_demand: pos_integer(),
          progress_callback: progress_callback() | nil,
          progress_interval: pos_integer(),
          format: atom() | nil,
          base_iri: String.t() | nil,
          max_file_size: pos_integer() | nil,
          graph: RDF.IRI.t() | RDF.BlankNode.t() | :default | nil
        ]

  @typedoc """
  Write options passed to NIF batch operations.

  Controls the sync behavior for RocksDB writes:
  - `sync: true` - Force fsync after each batch (default for single operations)
  - `sync: false` - Defer sync to OS, data still written to WAL
  """
  @type write_opts :: %{sync: boolean()}

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_batch_size 10_000
  @min_batch_size 100
  @max_batch_size 100_000
  # 100MB
  @default_max_file_size 100_000_000

  # Bulk mode defaults
  @bulk_mode_batch_size 50_000

  # Parallel loading defaults
  @default_parallel true
  @default_max_demand 5
  @min_stages 1
  @max_stages 64

  # Progress reporting defaults
  # Report progress every N batches (default: every 10 batches)
  @default_progress_interval 10
  @min_progress_interval 1

  # Max demand limits
  @min_max_demand 1
  @max_max_demand 100

  # Memory budget to batch size mapping
  @memory_budget_sizes %{
    low: 5_000,
    medium: 10_000,
    high: 50_000
  }

  # Memory thresholds for :auto mode (in bytes)
  # < 4GB = low, 4-16GB = medium, > 16GB = high
  @memory_threshold_low 4 * 1024 * 1024 * 1024
  @memory_threshold_high 16 * 1024 * 1024 * 1024

  # Resource limits to prevent DoS
  # Maximum number of triples/quads that can be loaded in a single operation
  @max_triples 1_000_000_000
  # Progress callback timeout in milliseconds (5 seconds)
  @progress_callback_timeout 5000

  # Supported RDF file formats
  @supported_formats %{
    ".ttl" => :turtle,
    ".turtle" => :turtle,
    ".nt" => :ntriples,
    ".ntriples" => :ntriples,
    ".nq" => :nquads,
    ".nquads" => :nquads,
    ".rdf" => :rdfxml,
    ".xml" => :rdfxml,
    ".trig" => :trig,
    ".jsonld" => :jsonld,
    ".json" => :jsonld
  }

  # ===========================================================================
  # Public API - Graph Loading
  # ===========================================================================

  @doc """
  Loads an RDF.Graph or RDF.Dataset into the store.

  Converts all triples in the graph/dataset to internal representation and
  inserts them in batches for efficient storage.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `graph_or_dataset` - RDF.Graph or RDF.Dataset to load

  ## Options

  - `:batch_size` - Number of triples per batch (default: #{@default_batch_size})
  - `:graph` - Target graph for quads (RDF.IRI, RDF.BlankNode, or :default). When loading
    an RDF.Graph, all triples are loaded to the specified graph. When loading an
    RDF.Dataset, this option is ignored and graphs in the dataset are preserved.

  ## Returns

  - `{:ok, count}` - Number of triples/quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback returning `:halt`
  - `{:error, reason}` - On failure
  - `{:error, {:flush_failed, count, reason}}` - Bulk mode sync failed after successful load

  ## Examples

      iex> graph = RDF.Graph.new([{~I<http://ex.org/s>, ~I<http://ex.org/p>, "o"}])
      iex> {:ok, 1} = Loader.load_graph(db, manager, graph)

      iex> # Load to named graph
      iex> Loader.load_graph(db, manager, graph, graph: RDF.iri("http://example.org/g"))

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events.
  """
  @spec load_graph(db_ref(), manager(), RDF.Graph.t() | RDF.Dataset.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}

  def load_graph(db, manager, graph_or_dataset, opts \\ [])

  def load_graph(db, manager, %RDF.Dataset{} = dataset, opts) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :dataset,
      path: nil,
      graph_count: RDF.Dataset.graph_count(dataset)
    }

    with_telemetry(start_metadata, fn ->
      quads = RDF.Dataset.quads(dataset)
      load_quads(db, manager, quads, batch_size, opts)
    end)
  end

  def load_graph(db, manager, %RDF.Graph{} = graph, opts) do
    # Check if :graph option is provided
    graph_opt = Keyword.get(opts, :graph)

    # Check if this is a quad store
    {:ok, is_quad} = ErlangAdapter.is_quad_store?(db)

    batch_size = resolve_batch_size(opts)

    cond do
      # Named graph specified - load to specified named graph
      graph_opt && graph_opt != :default ->
        start_metadata = %{
          source: :graph,
          path: nil,
          triple_count: RDF.Graph.triple_count(graph)
        }

        with_telemetry(start_metadata, fn ->
          triples = RDF.Graph.triples(graph)
          quads = triples_to_quads(triples, graph_opt)
          load_quads(db, manager, quads, batch_size, opts)
        end)

      # Quad store with default graph - load as quads with graph_id 0
      is_quad ->
        start_metadata = %{
          source: :graph,
          path: nil,
          triple_count: RDF.Graph.triple_count(graph)
        }

        with_telemetry(start_metadata, fn ->
          triples = RDF.Graph.triples(graph)
          quads = triples_to_quads(triples, :default)
          load_quads(db, manager, quads, batch_size, opts)
        end)

      # Triple store - use original triple loading logic
      true ->
        start_metadata = %{
          source: :graph,
          path: nil,
          triple_count: RDF.Graph.triple_count(graph)
        }

        with_telemetry(start_metadata, fn ->
          triples = RDF.Graph.triples(graph)
          load_triples(db, manager, triples, batch_size, opts)
        end)
    end
  end

  # ===========================================================================
  # Public API - File Loading
  # ===========================================================================

  @doc """
  Loads an RDF file into the triple store.

  Parses the file using RDF.ex and loads the resulting triples.
  Format is auto-detected from file extension or can be specified.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `path` - Path to the RDF file

  ## Options

  - `:format` - Force specific format (`:turtle`, `:ntriples`, `:rdfxml`, etc.)
  - `:batch_size` - Number of triples per batch (default: #{@default_batch_size})
  - `:max_file_size` - Maximum file size in bytes (default: 100MB)
  - `:graph` - Target graph for quads (RDF.IRI, RDF.BlankNode, or :default). For Turtle/N-Triples,
    all triples are loaded to the specified graph. For N-Quads/TriG with native graphs, this option
    is ignored and graphs in the file are preserved.

  ## Supported Formats

  - Turtle (`.ttl`, `.turtle`)
  - N-Triples (`.nt`, `.ntriples`)
  - N-Quads (`.nq`, `.nquads`)
  - RDF/XML (`.rdf`, `.xml`)
  - TriG (`.trig`)
  - JSON-LD (`.jsonld`, `.json`)

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:halted, count}` - Loading was cancelled by progress callback returning `:halt`
  - `{:error, :file_not_found}` - File does not exist
  - `{:error, :invalid_path}` - Path contains traversal sequences (`..`)
  - `{:error, {:file_too_large, size, max}}` - File exceeds size limit
  - `{:error, {:unsupported_format, ext, [supported: list]}}` - Unknown file format
  - `{:error, {:flush_failed, count, reason}}` - Bulk mode sync failed after successful load
  - `{:error, reason}` - On parse or load failure

  ## Examples

      iex> {:ok, count} = Loader.load_file(db, manager, "data.ttl")

      iex> {:ok, count} = Loader.load_file(db, manager, "data.rdf", format: :rdfxml)

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events.
  """
  @spec load_file(db_ref(), manager(), Path.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_file(db, manager, path, opts \\ []) do
    # Check if :graph option is provided
    graph_opt = Keyword.get(opts, :graph)

    # Check if this is a quad store
    {:ok, is_quad} = ErlangAdapter.is_quad_store?(db)

    cond do
      # Named graph specified - delegate to load_to_graph
      graph_opt && graph_opt != :default ->
        load_to_graph(db, manager, path, graph_opt, opts)

      # Quad store with default graph - load as quads with graph_id 0
      is_quad ->
        batch_size = resolve_batch_size(opts)
        max_file_size = Keyword.get(opts, :max_file_size, @default_max_file_size)

        start_metadata = %{source: :file, path: Path.basename(path)}

        with_telemetry(start_metadata, fn ->
          with {:ok, validated_path} <- validate_file_path(path),
               {:ok, format} <- detect_format(validated_path, opts),
               :ok <- check_file_size(validated_path, max_file_size),
               {:ok, graph} <- parse_file(validated_path, format) do
            triples = RDF.Graph.triples(graph)
            quads = triples_to_quads(triples, :default)
            load_quads(db, manager, quads, batch_size, opts)
          end
        end)

      # Triple store - use original triple loading logic
      true ->
        batch_size = resolve_batch_size(opts)
        max_file_size = Keyword.get(opts, :max_file_size, @default_max_file_size)

        start_metadata = %{source: :file, path: Path.basename(path)}

        with_telemetry(start_metadata, fn ->
          with {:ok, validated_path} <- validate_file_path(path),
               {:ok, format} <- detect_format(validated_path, opts),
               :ok <- check_file_size(validated_path, max_file_size),
               {:ok, graph} <- parse_file(validated_path, format) do
            triples = RDF.Graph.triples(graph)
            load_triples(db, manager, triples, batch_size, opts)
          end
        end)
    end
  end

  @doc """
  Loads RDF data from a string.

  Parses the string content and loads the resulting triples.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `content` - RDF content as string
  - `format` - Format of the content (`:turtle`, `:ntriples`, etc.)

  ## Options

  - `:batch_size` - Number of triples per batch (default: #{@default_batch_size})
  - `:base_iri` - Base IRI for relative URI resolution
  - `:graph` - Target graph for quads (RDF.IRI, RDF.BlankNode, or :default). For Turtle/N-Triples,
    all triples are loaded to the specified graph.

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:halted, count}` - Loading was cancelled by progress callback returning `:halt`
  - `{:error, {:flush_failed, count, reason}}` - Bulk mode sync failed after successful load
  - `{:error, reason}` - On parse or load failure

  ## Examples

      iex> ttl = \"\"\"
      ...> @prefix ex: <http://example.org/> .
      ...> ex:s ex:p "object" .
      ...> \"\"\"
      iex> {:ok, 1} = Loader.load_string(db, manager, ttl, :turtle)
  """
  @spec load_string(db_ref(), manager(), String.t(), atom(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_string(db, manager, content, format, opts \\ []) do
    # Check if :graph option is provided
    graph_opt = Keyword.get(opts, :graph)

    # Check if this is a quad store
    {:ok, is_quad} = ErlangAdapter.is_quad_store?(db)

    cond do
      # Named graph specified - load as quads
      graph_opt && graph_opt != :default ->
        case parse_string(content, format, Keyword.take(opts, [:base_iri])) do
          {:ok, graph} ->
            triples = RDF.Graph.triples(graph)
            quads = triples_to_quads(triples, graph_opt)
            batch_size = resolve_batch_size(opts)
            load_quads(db, manager, quads, batch_size, opts)

          {:error, _} = error ->
            error
        end

      # Quad store with default graph - load as quads with graph_id 0
      is_quad ->
        base_iri = Keyword.get(opts, :base_iri)
        parse_opts = if base_iri, do: [base_iri: base_iri], else: []

        case parse_string(content, format, parse_opts) do
          {:ok, graph} ->
            triples = RDF.Graph.triples(graph)
            quads = triples_to_quads(triples, :default)
            batch_size = resolve_batch_size(opts)
            load_quads(db, manager, quads, batch_size, opts)

          {:error, _} = error ->
            error
        end

      # Triple store - use original triple loading logic
      true ->
        batch_size = resolve_batch_size(opts)
        base_iri = Keyword.get(opts, :base_iri)
        parse_opts = if base_iri, do: [base_iri: base_iri], else: []

        case parse_string(content, format, parse_opts) do
          {:ok, graph} ->
            triples = RDF.Graph.triples(graph)
            load_triples(db, manager, triples, batch_size, opts)

          {:error, _} = error ->
            error
        end
    end
  end

  # ===========================================================================
  # Public API - Streaming
  # ===========================================================================

  @doc """
  Loads triples from a stream.

  Useful for custom data sources or when you need fine-grained control
  over the loading process.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `triple_stream` - Enumerable of RDF triples

  ## Options

  - `:batch_size` - Number of triples per batch (default: #{@default_batch_size})
  - `:graph` - Target graph for quads (RDF.IRI, RDF.BlankNode, or :default). All triples
    are loaded to the specified graph.

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:halted, count}` - Loading was cancelled by progress callback returning `:halt`
  - `{:error, {:flush_failed, count, reason}}` - Bulk mode sync failed after successful load
  - `{:error, reason}` - On failure

  ## Examples

      iex> triples = [
      ...>   {~I<http://ex.org/s1>, ~I<http://ex.org/p>, "o1"},
      ...>   {~I<http://ex.org/s2>, ~I<http://ex.org/p>, "o2"}
      ...> ]
      iex> {:ok, 2} = Loader.load_stream(db, manager, triples)
  """
  @spec load_stream(db_ref(), manager(), Enumerable.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_stream(db, manager, triple_stream, opts \\ []) do
    # Check if :graph option is provided
    graph_opt = Keyword.get(opts, :graph)

    # Check if this is a quad store
    {:ok, is_quad} = ErlangAdapter.is_quad_store?(db)

    batch_size = resolve_batch_size(opts)

    cond do
      # Named graph specified - load as quads
      graph_opt && graph_opt != :default ->
        quads = triples_to_quads(triple_stream, graph_opt)
        load_quads(db, manager, quads, batch_size, opts)

      # Quad store with default graph - load as quads with graph_id 0
      is_quad ->
        quads = triples_to_quads(triple_stream, :default)
        load_quads(db, manager, quads, batch_size, opts)

      # Triple store - use original triple loading logic
      true ->
        load_triples(db, manager, triple_stream, batch_size, opts)
    end
  end

  # ===========================================================================
  # Public API - Insert/Delete
  # ===========================================================================

  @doc """
  Inserts RDF triples into the store.

  Accepts a single triple, a list of triples, an RDF.Description, or
  an RDF.Graph. Triples are dictionary-encoded and indexed.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `input` - Triple(s) to insert

  ## Input Formats

  - Single triple: `{subject, predicate, object}`
  - List of triples: `[{s, p, o}, ...]`
  - RDF.Description: All triples from the description
  - RDF.Graph: All triples from the graph

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, reason}` - On failure

  ## Examples

      # Single triple
      {:ok, 1} = Loader.insert(db, manager, {~I<http://ex.org/s>, ~I<http://ex.org/p>, "value"})

      # Multiple triples
      {:ok, 2} = Loader.insert(db, manager, [
        {~I<http://ex.org/s1>, ~I<http://ex.org/p>, "v1"},
        {~I<http://ex.org/s2>, ~I<http://ex.org/p>, "v2"}
      ])

      # From RDF.Graph
      {:ok, count} = Loader.insert(db, manager, graph)
  """
  @spec insert(
          db_ref(),
          manager(),
          RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()
        ) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def insert(db, manager, input) do
    input
    |> normalize_to_triples()
    |> modify_triples(db, manager, &Index.insert_triples/2)
  end

  @doc """
  Deletes RDF triples from the store.

  Accepts the same input formats as `insert/3`.
  Removes matching triples from all indices.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `input` - Triple(s) to delete

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, reason}` - On failure

  ## Examples

      # Single triple
      {:ok, 1} = Loader.delete(db, manager, {~I<http://ex.org/s>, ~I<http://ex.org/p>, "value"})

      # Multiple triples
      {:ok, count} = Loader.delete(db, manager, triples)
  """
  @spec delete(
          db_ref(),
          manager(),
          RDF.Triple.t() | [RDF.Triple.t()] | RDF.Graph.t() | RDF.Description.t()
        ) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete(db, manager, input) do
    input
    |> normalize_to_triples()
    |> modify_triples(db, manager, &Index.delete_triples/2)
  end

  # Normalize various input formats to a list of triples
  @spec normalize_to_triples(
          RDF.Triple.t()
          | [RDF.Triple.t()]
          | RDF.Graph.t()
          | RDF.Description.t()
        ) :: [RDF.Triple.t()]
  defp normalize_to_triples(%RDF.Graph{} = graph), do: RDF.Graph.triples(graph)
  defp normalize_to_triples(%RDF.Description{} = desc), do: RDF.Description.triples(desc)
  defp normalize_to_triples(triples) when is_list(triples), do: triples
  defp normalize_to_triples({_s, _p, _o} = triple), do: [triple]

  # Generic triple modification (insert or delete)
  @spec modify_triples([RDF.Triple.t()], db_ref(), manager(), (reference(), list() ->
                                                                 :ok | {:error, term()})) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp modify_triples([], _db, _manager, _index_fn), do: {:ok, 0}

  defp modify_triples(triples, db, manager, index_fn) do
    with {:ok, internal_triples} <- Adapter.from_rdf_triples(manager, triples),
         :ok <- index_fn.(db, internal_triples) do
      {:ok, length(internal_triples)}
    end
  end

  # ===========================================================================
  # Public API - Quad Loading (N-Quads Format)
  # ===========================================================================

  @doc """
  Loads an N-Quads file into the quad store.

  Parses the N-Quads file and loads all quads (including named graphs)
  into the store. Unlike `load_file/3`, this function preserves all
  named graphs instead of discarding them.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `path` - Path to the N-Quads file

  ## Options

  - `:batch_size` - Number of quads per batch (default: #{@default_batch_size})
  - `:bulk_mode` - Enable bulk loading optimizations (default: false)
  - `:parallel` - Enable parallel encoding (default: true)
  - `:progress_callback` - Function called periodically with progress info
  - `:progress_interval` - Call callback every N batches (default: 10)

  ## Returns

  - `{:ok, count}` - Number of quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback
  - `{:error, reason}` - On failure

  ## Graph Handling

  - Quads with named graphs are loaded with their graph ID > 0
  - Quads without a graph name (default graph) are loaded with graph ID 0
  - All named graphs in the file are preserved

  ## Examples

      {:ok, 1000} = Loader.load_nquads_file(db, manager, "data.nq")

      # With bulk mode for large files
      {:ok, count} = Loader.load_nquads_file(db, manager, "large.nq",
        batch_size: 50_000,
        bulk_mode: true
      )

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events with `format: :nquads` metadata.
  """
  @spec load_nquads_file(db_ref(), manager(), Path.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_nquads_file(db, manager, path, opts \\ []) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :file,
      path: path,
      format: :nquads
    }

    with_telemetry(start_metadata, fn ->
      case parse_nquads_file_full(path) do
        {:ok, dataset} ->
          quads = RDF.Dataset.quads(dataset)
          load_quads(db, manager, quads, batch_size, opts)

        {:error, _} = error ->
          error
      end
    end)
  end

  @doc """
  Loads N-Quads data from a string into the quad store.

  Parses the N-Quads string and loads all quads (including named graphs)
  into the store.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `content` - N-Quads formatted string

  ## Options

  - `:batch_size` - Number of quads per batch (default: #{@default_batch_size})
  - `:bulk_mode` - Enable bulk loading optimizations (default: false)
  - `:parallel` - Enable parallel encoding (default: true)
  - `:base_iri` - Base IRI for resolving relative IRIs

  ## Returns

  - `{:ok, count}` - Number of quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback
  - `{:error, reason}` - On failure

  ## Examples

      nquads = \"\"\"
      <http://example.org/s> <http://example.org/p> "o" <http://example.org/g> .
      \"\"\"
      {:ok, 1} = Loader.load_nquads_string(db, manager, nquads)
  """
  @spec load_nquads_string(db_ref(), manager(), String.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_nquads_string(db, manager, content, opts \\ []) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :string,
      path: nil,
      format: :nquads
    }

    with_telemetry(start_metadata, fn ->
      parse_opts = [base_iri: Keyword.get(opts, :base_iri)]

      case parse_nquads_string_full(content, parse_opts) do
        {:ok, dataset} ->
          quads = RDF.Dataset.quads(dataset)
          load_quads(db, manager, quads, batch_size, opts)

        {:error, _} = error ->
          error
      end
    end)
  end

  # ===========================================================================
  # Public API - TriG Loading
  # ===========================================================================

  @doc """
  Loads TriG format file into the quad store.

  TriG is a Turtle-like RDF syntax that supports named graphs using
  the GRAPH keyword. This function loads all quads from the TriG file,
  including both named graphs and the default graph.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `path` - Path to TriG file

  ## Options

  - `:batch_size` - Number of quads per batch (default: #{@default_batch_size})
  - `:bulk_mode` - Enable bulk loading optimizations (default: false)
  - `:parallel` - Enable parallel encoding (default: true)
  - `:base_iri` - Base IRI for resolving relative IRIs
  - `:progress_callback` - Callback function for progress updates
  - `:progress_interval` - Report progress every N batches (default: #{@default_progress_interval})

  ## Returns

  - `{:ok, count}` - Number of quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback
  - `{:error, reason}` - On failure

  ## Graph Handling

  - Triples inside `GRAPH <iri> { ... }` blocks are loaded into that named graph
  - Triples outside any GRAPH block are loaded into the default graph (ID 0)
  - Named graph IRIs are automatically registered in the dictionary

  ## Examples

      {:ok, 42} = Loader.load_trig_file(db, manager, "data.trig")

      # With progress callback
      {:ok, count} = Loader.load_trig_file(db, manager, "large.trig",
        progress_callback: fn info -> IO.inspect(info) :continue end
      )

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events with `format: :trig` metadata.
  """
  @spec load_trig_file(db_ref(), manager(), Path.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_trig_file(db, manager, path, opts \\ []) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :file,
      path: path,
      format: :trig
    }

    with_telemetry(start_metadata, fn ->
      case parse_trig_file_full(path) do
        {:ok, dataset} ->
          quads = RDF.Dataset.quads(dataset)
          load_quads(db, manager, quads, batch_size, opts)

        {:error, _} = error ->
          error
      end
    end)
  end

  @doc """
  Loads TriG format data from a string into the quad store.

  Parses the TriG string and loads all quads (including named graphs)
  into the store.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `content` - TriG formatted string

  ## Options

  - `:batch_size` - Number of quads per batch (default: #{@default_batch_size})
  - `:bulk_mode` - Enable bulk loading optimizations (default: false)
  - `:parallel` - Enable parallel encoding (default: true)
  - `:base_iri` - Base IRI for resolving relative IRIs
  - `:progress_callback` - Callback function for progress updates
  - `:progress_interval` - Report progress every N batches (default: #{@default_progress_interval})

  ## Returns

  - `{:ok, count}` - Number of quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback
  - `{:error, reason}` - On failure

  ## Examples

      trig = \"\"\"
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
      }

      ex:s2 ex:p "o2" .
      \"\"\"

      {:ok, 2} = Loader.load_trig_string(db, manager, trig)
  """
  @spec load_trig_string(db_ref(), manager(), String.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_trig_string(db, manager, content, opts \\ []) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :string,
      path: nil,
      format: :trig
    }

    with_telemetry(start_metadata, fn ->
      parse_opts = [base_iri: Keyword.get(opts, :base_iri)]

      case parse_trig_string_full(content, parse_opts) do
        {:ok, dataset} ->
          quads = RDF.Dataset.quads(dataset)
          load_quads(db, manager, quads, batch_size, opts)

        {:error, _} = error ->
          error
      end
    end)
  end

  # ===========================================================================
  # Public API - Graph-Scoped Loading
  # ===========================================================================

  @doc """
  Loads an RDF file into a specific named graph.

  This function loads RDF data (Turtle, N-Triples, etc.) into a specific
  named graph in the quad store. Unlike standard loading which puts data
  in the default graph, this function forces all triples into the specified
  graph.

  This is useful for:
  - Loading Turtle/N-Triples files into named graphs
  - Organizing data from different sources into separate graphs
  - Building multi-dataset stores from single-format files

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `path` - Path to the RDF file
  - `graph` - Target graph (RDF.IRI or RDF.BlankNode)

  ## Options

  - `:format` - Force specific format (`:turtle`, `:ntriples`, etc.)
  - `:batch_size` - Number of quads per batch (default: #{@default_batch_size})
  - `:bulk_mode` - Enable bulk loading optimizations (default: false)
  - `:parallel` - Enable parallel encoding (default: true)
  - `:clear_graph` - Clear target graph before loading (default: false)
  - `:progress_callback` - Callback function for progress updates
  - `:max_file_size` - Maximum file size in bytes (default: 100MB)

  ## Returns

  - `{:ok, count}` - Number of quads loaded
  - `{:halted, count}` - Loading was cancelled by progress callback
  - `{:error, reason}` - On failure

  ## Examples

      # Load Turtle file to named graph
      {:ok, 42} = Loader.load_to_graph(db, manager, "data.ttl",
        RDF.iri("http://example.org/graph1"))

      # Load with graph clearing
      {:ok, count} = Loader.load_to_graph(db, manager, "update.nt",
        RDF.iri("http://example.org/graph1"), clear_graph: true)

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events with `format: :graph_scoped` metadata.
  """
  @spec load_to_graph(
          db_ref(),
          manager(),
          Path.t(),
          RDF.IRI.t() | RDF.BlankNode.t(),
          load_opts()
        ) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  def load_to_graph(db, manager, path, graph, opts \\ []) do
    batch_size = resolve_batch_size(opts)
    max_file_size = Keyword.get(opts, :max_file_size, @default_max_file_size)
    clear_graph? = Keyword.get(opts, :clear_graph, false)

    start_metadata = %{
      source: :file,
      path: Path.basename(path),
      format: :graph_scoped,
      graph: graph
    }

    with_telemetry(start_metadata, fn ->
      with {:ok, validated_path} <- validate_file_path(path),
           {:ok, format} <- detect_format(validated_path, opts),
           :ok <- check_file_size(validated_path, max_file_size) do
        # Clear graph if requested
        if clear_graph? do
          alias TripleStore.QuadOperations
          QuadOperations.delete_graph(db, manager, graph)
        else
          :ok
        end

        # Parse file and convert to quads with target graph
        case parse_file(validated_path, format) do
          {:ok, %RDF.Graph{} = rdf_graph} ->
            triples = RDF.Graph.triples(rdf_graph)
            quads = triples_to_quads(triples, graph)
            load_quads(db, manager, quads, batch_size, opts)

          {:error, _} = error ->
            error
        end
      end
    end)
  end

  @doc """
  Loads multiple RDF files into separate named graphs.

  This function accepts a map where keys are graph terms and values are
  file paths. Each file is loaded into its corresponding graph.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process
  - `graph_files` - Map of `%{graph_term => file_path}`

  ## Options

  - `:parallel` - Enable parallel loading (default: false)
  - `:on_conflict` - Behavior on error: `:continue` (default), `:stop`, or `:abort`
  - `:batch_size` - Number of quads per batch
  - `:bulk_mode` - Enable bulk loading optimizations
  - `:clear_graphs` - Clear target graphs before loading (default: false)
  - `:progress_callback` - Callback with per-file progress

  ## Returns

  - `{:ok, summary}` - Map of `%{graph_term => quad_count}`
  - `{:error, reason}` - On failure (depends on `:on_conflict`)

  ## Examples

      graph_files = %{
        RDF.iri("http://example.org/g1") => "data1.ttl",
        RDF.iri("http://example.org/g2") => "data2.nt"
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files)

  ## Conflict Modes

  - `:continue` - Continue loading other files on error (default)
  - `:stop` - Stop loading on first error, return partial results
  - `:abort` - Abort entire operation, rollback no data (not yet implemented)

  ## Telemetry

  Emits `[:triple_store, :loader, :multi_graph_start]` on start and
  `[:triple_store, :loader, :multi_graph_stop]` on completion.
  """
  @spec load_files_to_graphs(
          db_ref(),
          manager(),
          %{(RDF.IRI.t() | RDF.BlankNode.t()) => Path.t()},
          keyword()
        ) ::
          {:ok, %{(RDF.IRI.t() | RDF.BlankNode.t()) => non_neg_integer()}}
          | {:error, term()}
  def load_files_to_graphs(db, manager, graph_files, opts \\ []) do
    # Validate inputs - return early for empty map
    if map_size(graph_files) == 0 do
      {:ok, %{}}
    else
      do_load_files_to_graphs(db, manager, graph_files, opts)
    end
  end

  # Internal implementation of load_files_to_graphs
  defp do_load_files_to_graphs(db, manager, graph_files, opts) do
    parallel? = Keyword.get(opts, :parallel, false)
    on_conflict = Keyword.get(opts, :on_conflict, :continue)
    clear_graphs? = Keyword.get(opts, :clear_graphs, false)
    progress_callback = Keyword.get(opts, :progress_callback)

    start_metadata = %{
      source: :multi_graph,
      file_count: map_size(graph_files),
      parallel: parallel?
    }

    emit_start_telemetry(start_metadata)

    start_time = System.monotonic_time(:millisecond)

    result =
      if parallel? do
        load_files_to_graphs_parallel(db, manager, graph_files, opts, clear_graphs?)
      else
        load_files_to_graphs_sequential(
          db,
          manager,
          graph_files,
          opts,
          clear_graphs?,
          on_conflict,
          progress_callback,
          start_time
        )
      end

    duration = System.monotonic_time(:millisecond) - start_time

    case result do
      {:ok, summary} ->
        :telemetry.execute(
          [:triple_store, :loader, :multi_graph_stop],
          %{duration: duration, count: map_size(summary)},
          %{status: :ok}
        )

        {:ok, summary}

      {:error, _} = error ->
        :telemetry.execute(
          [:triple_store, :loader, :multi_graph_stop],
          %{duration: duration},
          %{status: :error}
        )

        error
    end
  end

  # ===========================================================================
  # Private - Graph-Scoped Loading Helpers
  # ===========================================================================

  # Emit telemetry start event for multi-graph loading
  defp emit_start_telemetry(metadata) do
    :telemetry.execute(
      [:triple_store, :loader, :multi_graph_start],
      %{system_time: System.system_time()},
      metadata
    )
  end

  # Convert a stream of triples to quads with a specific graph term
  # The graph term is used directly (IRI or BlankNode), and will be
  # converted to graph_id by Adapter.from_rdf_quads during encoding
  # :default is converted to nil which RDF.ex uses for the default graph
  @spec triples_to_quads(Enumerable.t(), RDF.IRI.t() | RDF.BlankNode.t() | :default) ::
          Enumerable.t()
  defp triples_to_quads(triples, :default) do
    Stream.map(triples, fn
      {s, p, o} ->
        RDF.Quad.new(s, p, o, nil)

      rdf_statement ->
        RDF.Quad.new(rdf_statement.subject, rdf_statement.predicate, rdf_statement.object, nil)
    end)
  end

  defp triples_to_quads(triples, graph_term) do
    Stream.map(triples, fn
      {s, p, o} ->
        RDF.Quad.new(s, p, o, graph_term)

      rdf_statement ->
        RDF.Quad.new(
          rdf_statement.subject,
          rdf_statement.predicate,
          rdf_statement.object,
          graph_term
        )
    end)
  end

  # Sequential multi-graph loading
  defp load_files_to_graphs_sequential(
         db,
         manager,
         graph_files,
         opts,
         clear_graphs?,
         on_conflict,
         progress_callback,
         start_time
       ) do
    Enum.reduce_while(graph_files, {:ok, %{}}, fn {graph, path}, {:ok, summary} ->
      # Call progress callback if provided
      callback_result =
        if progress_callback do
          progress_info = %{
            graph: graph,
            file: Path.basename(path),
            loaded_so_far: map_size(summary),
            total_files: map_size(graph_files),
            elapsed_ms: System.monotonic_time(:millisecond) - start_time
          }

          case progress_callback.(progress_info) do
            :continue -> :continue
            :halt -> :halt
          end
        else
          :continue
        end

      # Check if callback requested halt
      if callback_result == :halt do
        {:halt, {:ok, summary}}
      else
        # Load single file to graph
        load_opts =
          opts
          |> Keyword.put(:clear_graph, clear_graphs?)
          |> Keyword.delete(:on_conflict)
          |> Keyword.delete(:progress_callback)

        case load_to_graph(db, manager, path, graph, load_opts) do
          {:ok, count} ->
            {:cont, {:ok, Map.put(summary, graph, count)}}

          {:halted, count} ->
            {:cont, {:ok, Map.put(summary, graph, count)}}

          {:error, reason} ->
            case on_conflict do
              :continue ->
                {:cont, {:ok, Map.put(summary, graph, {:error, reason})}}

              :stop ->
                {:halt, {:ok, Map.put(summary, graph, {:error, reason})}}

              :abort ->
                {:halt, {:error, {:load_error, graph, path, reason}}}
            end
        end
      end
    end)
  end

  # Parallel multi-graph loading using Task.async_stream
  defp load_files_to_graphs_parallel(db, manager, graph_files, opts, clear_graphs?) do
    # Note: Parallel loading requires caution with Dictionary Manager access
    # For now, use Task.async_stream with limited concurrency
    load_opts =
      opts
      |> Keyword.put(:clear_graph, clear_graphs?)
      |> Keyword.delete(:on_conflict)
      |> Keyword.delete(:progress_callback)
      |> Keyword.delete(:parallel)

    graph_files
    |> Task.async_stream(
      fn {graph, path} ->
        case load_to_graph(db, manager, path, graph, load_opts) do
          {:ok, count} -> {graph, count}
          {:halted, count} -> {graph, count}
          {:error, reason} -> {graph, {:error, reason}}
        end
      end,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, %{}}, fn
      {:ok, {graph, result}}, {:ok, summary} ->
        {:cont, {:ok, Map.put(summary, graph, result)}}

      {:exit, reason}, _acc ->
        {:halt, {:error, {:task_exit, reason}}}
    end)
  end

  # ===========================================================================
  # Private - Core Loading Logic
  # ===========================================================================

  @spec load_triples(db_ref(), manager(), Enumerable.t(), pos_integer(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_triples(db, manager, triples, batch_size, opts) do
    parallel? = Keyword.get(opts, :parallel, @default_parallel)
    bulk_mode? = Keyword.get(opts, :bulk_mode, false)
    progress_callback = Keyword.get(opts, :progress_callback)
    progress_interval = validate_progress_interval(Keyword.get(opts, :progress_interval))
    start_time = System.monotonic_time(:millisecond)

    # In bulk mode, use sync: false for writes
    sync? = not bulk_mode?

    progress_opts = %{
      callback: progress_callback,
      interval: progress_interval,
      start_time: start_time
    }

    write_opts = %{
      sync: sync?
    }

    result =
      if parallel? do
        stages = resolve_stages(opts)
        max_demand = validate_max_demand(Keyword.get(opts, :max_demand))

        load_triples_parallel(
          db,
          manager,
          triples,
          batch_size,
          stages,
          max_demand,
          progress_opts,
          write_opts
        )
      else
        load_triples_sequential(db, manager, triples, batch_size, progress_opts, write_opts)
      end

    # In bulk mode, flush WAL after successful load for durability.
    # Note: If flush fails, the data is still in the WAL (written but not fsync'd).
    # On process restart, RocksDB will replay the WAL and recover the data.
    # Only OS crash or power failure before the OS flushes its buffers can lose data.
    case result do
      {:ok, count} when bulk_mode? ->
        case ErlangAdapter.flush_wal(db, true) do
          :ok ->
            {:ok, count}

          {:error, reason} ->
            # Data was written successfully but final sync failed.
            # Data is in the WAL and will survive process restart,
            # but may be lost on OS crash or power failure.
            {:error, {:flush_failed, count, reason}}
        end

      other ->
        other
    end
  end

  # Load quads - similar to load_triples but uses QuadOperations instead of Index
  @spec load_quads(db_ref(), manager(), Enumerable.t(), pos_integer(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_quads(db, manager, quads, batch_size, opts) do
    alias TripleStore.QuadOperations

    parallel? = Keyword.get(opts, :parallel, @default_parallel)
    bulk_mode? = Keyword.get(opts, :bulk_mode, false)
    progress_callback = Keyword.get(opts, :progress_callback)
    progress_interval = validate_progress_interval(Keyword.get(opts, :progress_interval))
    start_time = System.monotonic_time(:millisecond)

    # In bulk mode, use sync: false for writes
    sync? = not bulk_mode?

    progress_opts = %{
      callback: progress_callback,
      interval: progress_interval,
      start_time: start_time
    }

    write_opts = %{
      sync: sync?
    }

    result =
      if parallel? do
        stages = resolve_stages(opts)
        max_demand = validate_max_demand(Keyword.get(opts, :max_demand))

        load_quads_parallel(
          db,
          manager,
          quads,
          batch_size,
          stages,
          max_demand,
          progress_opts,
          write_opts
        )
      else
        load_quads_sequential(db, manager, quads, batch_size, progress_opts, write_opts)
      end

    # In bulk mode, flush WAL after successful load for durability.
    case result do
      {:ok, count} when bulk_mode? ->
        case ErlangAdapter.flush_wal(db, true) do
          :ok ->
            {:ok, count}

          {:error, reason} ->
            {:error, {:flush_failed, count, reason}}
        end

      other ->
        other
    end
  end

  # Validate progress_interval option
  @spec validate_progress_interval(term()) :: pos_integer()
  defp validate_progress_interval(nil), do: @default_progress_interval

  defp validate_progress_interval(interval)
       when is_integer(interval) and interval >= @min_progress_interval do
    interval
  end

  defp validate_progress_interval(interval) when is_integer(interval) do
    Logger.warning(
      "progress_interval #{interval} below minimum #{@min_progress_interval}, using minimum"
    )

    @min_progress_interval
  end

  defp validate_progress_interval(_), do: @default_progress_interval

  # Validate max_demand option
  @spec validate_max_demand(term()) :: pos_integer()
  defp validate_max_demand(nil), do: @default_max_demand

  defp validate_max_demand(demand)
       when is_integer(demand) and demand >= @min_max_demand and demand <= @max_max_demand do
    demand
  end

  defp validate_max_demand(demand) when is_integer(demand) and demand < @min_max_demand do
    Logger.warning("max_demand #{demand} below minimum #{@min_max_demand}, using minimum")
    @min_max_demand
  end

  defp validate_max_demand(demand) when is_integer(demand) and demand > @max_max_demand do
    Logger.warning("max_demand #{demand} above maximum #{@max_max_demand}, using maximum")
    @max_max_demand
  end

  defp validate_max_demand(_), do: @default_max_demand

  # Sequential loading - original implementation
  @spec load_triples_sequential(db_ref(), manager(), Enumerable.t(), pos_integer(), map(), map()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_triples_sequential(db, manager, triples, batch_size, progress_opts, write_opts) do
    triples
    |> Stream.chunk_every(batch_size)
    |> Stream.with_index(1)
    |> Enum.reduce_while({:ok, 0}, fn {batch, batch_number}, {:ok, total} ->
      # Check max triples limit before processing batch
      if total >= @max_triples do
        {:halt, {:error, {:max_triples_exceeded, @max_triples}}}
      else
        batch_start = System.monotonic_time()
        batch_count = length(batch)

        # Don't process batch if it would exceed max triples
        if total + batch_count > @max_triples do
          {:halt, {:error, {:max_triples_exceeded, @max_triples}}}
        else
          case process_batch(db, manager, batch, write_opts) do
            :ok ->
              new_total = total + batch_count
              batch_duration = System.monotonic_time() - batch_start

              emit_batch_telemetry(batch_count, batch_duration, batch_number, write_opts.sync)

              # Check if we should report progress and handle cancellation
              case maybe_report_progress(progress_opts, batch_number, new_total) do
                :continue ->
                  {:cont, {:ok, new_total}}

                :halt ->
                  {:halt, {:halted, new_total}}
              end

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end
      end
    end)
  end

  # Parallel loading - Flow-based pipeline
  # Stage 1: Chunking (from stream)
  # Stage 2: Dictionary encoding (parallel, CPU-bound)
  # Stage 3: Index writing (sequential via partition, I/O-bound)
  @spec load_triples_parallel(
          db_ref(),
          manager(),
          Enumerable.t(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          map(),
          map()
        ) :: {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_triples_parallel(
         db,
         manager,
         triples,
         batch_size,
         stages,
         max_demand,
         progress_opts,
         write_opts
       ) do
    # Use Agent for error tracking (needs to store full error term)
    # Use :atomics for halt flag (lock-free, no message passing overhead)
    {:ok, error_agent} = Agent.start_link(fn -> nil end)
    halt_ref = :atomics.new(1, signed: false)

    try do
      result =
        triples
        |> Stream.chunk_every(batch_size)
        |> Flow.from_enumerable(stages: stages, max_demand: max_demand)
        # Stage 2: Parallel dictionary encoding
        |> Flow.map(fn batch ->
          # Check if halted before encoding (lock-free read)
          if halted?(halt_ref) do
            {:halted, []}
          else
            encode_batch(manager, batch, error_agent)
          end
        end)
        # Stage 3: Sequential writing via single partition
        |> Flow.partition(stages: 1, max_demand: max_demand)
        |> Flow.reduce(fn -> {0, 0} end, fn encoded_batch, {total, batch_num} ->
          write_encoded_batch_with_progress(
            db,
            encoded_batch,
            batch_num + 1,
            error_agent,
            halt_ref,
            total,
            progress_opts,
            write_opts
          )
        end)
        |> Flow.emit(:state)
        |> Enum.to_list()

      # Check for errors or halt that occurred during processing
      cond do
        halted?(halt_ref) ->
          # Halted by progress callback
          total = Enum.reduce(result, 0, fn {count, _batch_num}, acc -> acc + count end)
          {:halted, total}

        error = Agent.get(error_agent, & &1) ->
          error

        true ->
          # Sum up totals from all partitions (should be just one)
          total = Enum.reduce(result, 0, fn {count, _batch_num}, acc -> acc + count end)
          {:ok, total}
      end
    after
      Agent.stop(error_agent)
    end
  end

  # Lock-free halt flag helpers using :atomics
  @spec halted?(reference()) :: boolean()
  defp halted?(halt_ref), do: :atomics.get(halt_ref, 1) == 1

  @spec set_halted(reference()) :: :ok
  defp set_halted(halt_ref) do
    :atomics.put(halt_ref, 1, 1)
    :ok
  end

  # Encode a batch of RDF triples to internal representation
  @spec encode_batch(manager(), [RDF.Triple.t()], pid()) ::
          {:ok, list()} | {:error, term()}
  defp encode_batch(manager, rdf_triples, error_agent) do
    case Adapter.from_rdf_triples(manager, rdf_triples) do
      {:ok, internal_triples} ->
        {:ok, internal_triples}

      {:error, reason} = error ->
        Agent.update(error_agent, fn _ -> error end)
        {:error, reason}
    end
  end

  # Write encoded batch with progress reporting (for parallel loading)
  @spec write_encoded_batch_with_progress(
          db_ref(),
          {:ok, list()} | {:error, term()} | {:halted, list()},
          pos_integer(),
          pid(),
          reference(),
          non_neg_integer(),
          map(),
          map()
        ) :: {non_neg_integer(), pos_integer()}
  defp write_encoded_batch_with_progress(
         _db,
         {:error, _reason},
         batch_num,
         _error_agent,
         _halt_ref,
         total,
         _progress_opts,
         _write_opts
       ) do
    # Skip writing on encoding error, error already recorded
    {total, batch_num}
  end

  defp write_encoded_batch_with_progress(
         _db,
         {:halted, _},
         batch_num,
         _error_agent,
         _halt_ref,
         total,
         _progress_opts,
         _write_opts
       ) do
    # Skip writing when halted
    {total, batch_num}
  end

  defp write_encoded_batch_with_progress(
         db,
         {:ok, internal_triples},
         batch_num,
         error_agent,
         halt_ref,
         total,
         progress_opts,
         write_opts
       ) do
    # Check if already halted (lock-free read)
    if halted?(halt_ref) do
      {total, batch_num}
    else
      batch_start = System.monotonic_time()

      case Index.insert_triples(db, internal_triples, sync: write_opts.sync) do
        :ok ->
          batch_count = length(internal_triples)
          new_total = total + batch_count
          batch_duration = System.monotonic_time() - batch_start

          emit_batch_telemetry(batch_count, batch_duration, batch_num, write_opts.sync)

          # Report progress and handle cancellation
          case maybe_report_progress(progress_opts, batch_num, new_total) do
            :continue ->
              {new_total, batch_num}

            :halt ->
              set_halted(halt_ref)
              {new_total, batch_num}
          end

        {:error, _reason} = error ->
          Agent.update(error_agent, fn _ -> error end)
          {total, batch_num}
      end
    end
  end

  # Telemetry helper for batch events
  @spec emit_batch_telemetry(non_neg_integer(), integer(), pos_integer(), boolean()) :: :ok
  defp emit_batch_telemetry(count, duration, batch_number, sync) do
    :telemetry.execute(
      [:triple_store, :loader, :batch],
      %{count: count, duration: duration},
      %{batch_number: batch_number, sync: sync}
    )
  end

  # Report progress if callback is set and interval is reached
  # Includes timeout protection to prevent slow/hanging callbacks from blocking load
  @spec maybe_report_progress(map(), pos_integer(), non_neg_integer()) :: :continue | :halt
  defp maybe_report_progress(%{callback: nil}, _batch_number, _total), do: :continue

  defp maybe_report_progress(
         %{callback: callback, interval: interval, start_time: start_time},
         batch_number,
         total
       ) do
    # Only report on interval boundaries
    if rem(batch_number, interval) == 0 do
      elapsed_ms = System.monotonic_time(:millisecond) - start_time
      rate = if elapsed_ms > 0, do: total / elapsed_ms * 1000, else: 0.0

      progress_info = %{
        triples_loaded: total,
        batch_number: batch_number,
        elapsed_ms: elapsed_ms,
        rate_per_second: rate
      }

      # Call callback with timeout to prevent blocking
      task = Task.async(fn -> callback.(progress_info) end)

      case Task.yield(task, @progress_callback_timeout) || Task.shutdown(task) do
        {:ok, :halt} -> :halt
        _ -> :continue
      end
    else
      :continue
    end
  end

  @spec process_batch(db_ref(), manager(), [RDF.Triple.t()], map()) :: :ok | {:error, term()}
  defp process_batch(db, manager, rdf_triples, write_opts) do
    case Adapter.from_rdf_triples(manager, rdf_triples) do
      {:ok, internal_triples} ->
        Index.insert_triples(db, internal_triples, sync: write_opts.sync)

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Private - Quad Loading Functions
  # ===========================================================================

  # Sequential quad loading - similar to triple loading but uses QuadOperations
  @spec load_quads_sequential(db_ref(), manager(), Enumerable.t(), pos_integer(), map(), map()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_quads_sequential(db, manager, quads, batch_size, progress_opts, write_opts) do
    alias TripleStore.QuadOperations

    quads
    |> Stream.chunk_every(batch_size)
    |> Stream.with_index(1)
    |> Enum.reduce_while({:ok, 0}, fn {batch, batch_number}, {:ok, total} ->
      # Check max triples limit before processing batch
      if total >= @max_triples do
        {:halt, {:error, {:max_triples_exceeded, @max_triples}}}
      else
        batch_start = System.monotonic_time()
        batch_count = length(batch)

        # Don't process batch if it would exceed max triples
        if total + batch_count > @max_triples do
          {:halt, {:error, {:max_triples_exceeded, @max_triples}}}
        else
          case process_quad_batch(db, manager, batch, write_opts) do
            :ok ->
              new_total = total + batch_count
              batch_duration = System.monotonic_time() - batch_start

              emit_batch_telemetry(batch_count, batch_duration, batch_number, write_opts.sync)

              # Check if we should report progress and handle cancellation
              case maybe_report_quad_progress(progress_opts, batch_number, new_total) do
                :continue ->
                  {:cont, {:ok, new_total}}

                :halt ->
                  {:halt, {:halted, new_total}}
              end

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end
      end
    end)
  end

  # Parallel quad loading - Flow-based pipeline for quads
  @spec load_quads_parallel(
          db_ref(),
          manager(),
          Enumerable.t(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          map(),
          map()
        ) :: {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp load_quads_parallel(
         db,
         manager,
         quads,
         batch_size,
         stages,
         max_demand,
         progress_opts,
         write_opts
       ) do
    alias TripleStore.QuadOperations

    # Use Agent for error tracking
    {:ok, error_agent} = Agent.start_link(fn -> nil end)
    halt_ref = :atomics.new(1, signed: false)

    try do
      result =
        quads
        |> Stream.chunk_every(batch_size)
        |> Flow.from_enumerable(stages: stages, max_demand: max_demand)
        # Stage 2: Parallel dictionary encoding for quads
        |> Flow.map(fn batch ->
          if halted?(halt_ref) do
            {:halted, []}
          else
            encode_quad_batch(manager, batch, error_agent)
          end
        end)
        # Stage 3: Sequential writing via single partition
        |> Flow.partition(stages: 1, max_demand: max_demand)
        |> Flow.reduce(fn -> {0, 0} end, fn encoded_batch, {total, batch_num} ->
          write_encoded_quad_batch_with_progress(
            db,
            encoded_batch,
            batch_num + 1,
            error_agent,
            halt_ref,
            total,
            progress_opts,
            write_opts
          )
        end)
        |> Flow.emit(:state)
        |> Enum.to_list()

      cond do
        halted?(halt_ref) ->
          total = Enum.reduce(result, 0, fn {count, _batch_num}, acc -> acc + count end)
          {:halted, total}

        error = Agent.get(error_agent, & &1) ->
          error

        true ->
          total = Enum.reduce(result, 0, fn {count, _batch_num}, acc -> acc + count end)
          {:ok, total}
      end
    after
      Agent.stop(error_agent)
    end
  end

  # Process a batch of quads for insertion
  @spec process_quad_batch(db_ref(), manager(), [RDF.Quad.t()], map()) :: :ok | {:error, term()}
  defp process_quad_batch(db, manager, rdf_quads, write_opts) do
    alias TripleStore.QuadOperations

    case Adapter.from_rdf_quads(manager, rdf_quads) do
      {:ok, internal_quads} ->
        QuadOperations.insert_quads(db, internal_quads, sync: write_opts.sync)

      {:error, _} = error ->
        error
    end
  end

  # Encode a batch of RDF quads to internal representation
  @spec encode_quad_batch(manager(), [RDF.Quad.t()], pid()) ::
          {:ok, list()} | {:error, term()}
  defp encode_quad_batch(manager, rdf_quads, error_agent) do
    case Adapter.from_rdf_quads(manager, rdf_quads) do
      {:ok, internal_quads} ->
        {:ok, internal_quads}

      {:error, reason} = error ->
        Agent.update(error_agent, fn _ -> error end)
        {:error, reason}
    end
  end

  # Write encoded quad batch with progress reporting
  @spec write_encoded_quad_batch_with_progress(
          db_ref(),
          {:ok, list()} | {:error, term()} | {:halted, list()},
          pos_integer(),
          pid(),
          reference(),
          non_neg_integer(),
          map(),
          map()
        ) :: {non_neg_integer(), pos_integer()}
  defp write_encoded_quad_batch_with_progress(
         _db,
         {:error, _reason},
         batch_num,
         _error_agent,
         _halt_ref,
         total,
         _progress_opts,
         _write_opts
       ) do
    {total, batch_num}
  end

  defp write_encoded_quad_batch_with_progress(
         _db,
         {:halted, _quads},
         batch_num,
         _error_agent,
         _halt_ref,
         total,
         _progress_opts,
         _write_opts
       ) do
    {total, batch_num}
  end

  defp write_encoded_quad_batch_with_progress(
         _db,
         {:ok, []},
         batch_num,
         _error_agent,
         _halt_ref,
         total,
         _progress_opts,
         _write_opts
       ) do
    {total, batch_num}
  end

  defp write_encoded_quad_batch_with_progress(
         db,
         {:ok, internal_quads},
         batch_num,
         _error_agent,
         halt_ref,
         total,
         progress_opts,
         write_opts
       ) do
    batch_start = System.monotonic_time()
    batch_count = length(internal_quads)

    case TripleStore.QuadOperations.insert_quads(db, internal_quads, sync: write_opts.sync) do
      :ok ->
        new_total = total + batch_count
        batch_duration = System.monotonic_time() - batch_start

        emit_batch_telemetry(batch_count, batch_duration, batch_num, write_opts.sync)

        case maybe_report_quad_progress(progress_opts, batch_num, new_total) do
          :continue ->
            {new_total, batch_num}

          :halt ->
            set_halted(halt_ref)
            {new_total, batch_num}
        end

      {:error, _reason} ->
        {total, batch_num}
    end
  end

  # Report progress for quad loading
  # Includes timeout protection to prevent slow/hanging callbacks from blocking load
  @spec maybe_report_quad_progress(map(), pos_integer(), non_neg_integer()) :: :continue | :halt
  defp maybe_report_quad_progress(%{callback: nil}, _batch_number, _total), do: :continue

  defp maybe_report_quad_progress(
         %{callback: callback, interval: interval, start_time: start_time},
         batch_number,
         total
       ) do
    if rem(batch_number, interval) == 0 do
      elapsed_ms = System.monotonic_time(:millisecond) - start_time
      rate = if elapsed_ms > 0, do: total / elapsed_ms * 1000, else: 0.0

      progress_info = %{
        quads_loaded: total,
        batch_number: batch_number,
        elapsed_ms: elapsed_ms,
        rate_per_second: rate
      }

      # Call callback with timeout to prevent blocking
      task = Task.async(fn -> callback.(progress_info) end)

      case Task.yield(task, @progress_callback_timeout) || Task.shutdown(task) do
        {:ok, :halt} -> :halt
        _ -> :continue
      end
    else
      :continue
    end
  end

  # Resolve stage count from options, defaulting to CPU cores
  @spec resolve_stages(keyword()) :: pos_integer()
  defp resolve_stages(opts) do
    case Keyword.get(opts, :stages) do
      nil -> System.schedulers_online()
      n when is_integer(n) and n >= @min_stages and n <= @max_stages -> n
      n when is_integer(n) and n < @min_stages -> @min_stages
      n when is_integer(n) and n > @max_stages -> @max_stages
      _ -> System.schedulers_online()
    end
  end

  # ===========================================================================
  # Private - Telemetry Helper
  # ===========================================================================

  @spec with_telemetry(map(), (-> {:ok, non_neg_integer()}
                                  | {:error, term()}
                                  | {:halted, non_neg_integer()})) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
  defp with_telemetry(start_metadata, func) do
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:triple_store, :loader, :start],
      %{system_time: System.system_time()},
      start_metadata
    )

    try do
      case func.() do
        {:ok, count} ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :loader, :stop],
            %{total_count: count, duration: duration},
            Map.take(start_metadata, [:source, :path])
          )

          {:ok, count}

        {:halted, count} ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :loader, :stop],
            %{total_count: count, duration: duration, halted: true},
            Map.take(start_metadata, [:source, :path])
          )

          {:halted, count}

        {:error, _} = error ->
          error
      end
    rescue
      e ->
        :telemetry.execute(
          [:triple_store, :loader, :exception],
          %{duration: System.monotonic_time() - start_time},
          Map.merge(Map.take(start_metadata, [:source, :path]), %{kind: :error, reason: e})
        )

        reraise e, __STACKTRACE__
    end
  end

  # ===========================================================================
  # Private - Path Validation
  # ===========================================================================

  @spec validate_file_path(Path.t(), [Path.t()] | nil) ::
          {:ok, Path.t()} | {:error, :invalid_path}
  defp validate_file_path(path, allowed_dirs \\ nil) do
    # Check for path traversal in the original path before expansion
    # This catches attempts like "../", "..\\", "%2e%2e", etc.
    if has_path_traversal?(path) do
      {:error, :invalid_path}
    else
      expanded = Path.expand(path)

      # If allowed_dirs is specified, verify the path is within them
      if allowed_dirs != nil and Path.type(expanded) == :absolute do
        if within_allowed_dirs?(expanded, allowed_dirs) do
          {:ok, expanded}
        else
          {:error, :invalid_path}
        end
      else
        # No directory restrictions or relative path - safe to use
        {:ok, expanded}
      end
    end
  rescue
    _ -> {:error, :invalid_path}
  end

  # Check if a path contains path traversal attempts
  # This checks for literal "..", URL-encoded variants, and other bypasses
  defp has_path_traversal?(path) when is_binary(path) do
    # Check for literal dot-dot-slash sequences
    dot_dot_checks = [
      # Literal ".."
      "..",
      # URL encoded ".."
      "%2e%2e",
      # Partially encoded
      "%2e.",
      # Partially encoded
      ".%2e",
      # Windows backslash separator (if on Unix, this is safe check)
      "..\\",
      # Double-encoded "."
      "%252e",
      # Unicode bypass (UTF-8)
      "%c0%ae",
      # Unicode bypass (overlong)
      "%e0%80%af"
    ]

    # Normalize path for checking (lowercase for case-insensitive checks)
    normalized = String.downcase(path)

    Enum.any?(dot_dot_checks, fn pattern ->
      String.contains?(normalized, pattern)
    end)
  end

  # Check if a path is within the list of allowed directories
  defp within_allowed_dirs?(path, allowed_dirs) do
    normalized_path = normalize_path(path)

    Enum.any?(allowed_dirs, fn dir ->
      normalized_allowed = normalize_path(dir)
      # Check if path starts with allowed directory (with trailing slash for proper prefix match)
      String.starts_with?(normalized_path <> "/", normalized_allowed <> "/") or
        normalized_path == normalized_allowed
    end)
  end

  # Normalize a path for comparison
  defp normalize_path(path) do
    path
    |> Path.expand()
    |> String.replace_trailing("/", "")
  end

  @spec check_file_size(Path.t(), pos_integer()) ::
          :ok | {:error, {:file_too_large, non_neg_integer(), pos_integer()}}
  defp check_file_size(path, max_size) do
    case File.stat(path) do
      {:ok, %File.Stat{size: size}} when size > max_size ->
        {:error, {:file_too_large, size, max_size}}

      {:ok, _stat} ->
        :ok

      {:error, :enoent} ->
        # File doesn't exist - let parse_file handle this error
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  # ===========================================================================
  # Private - Format Detection
  # ===========================================================================

  @spec detect_format(Path.t(), keyword()) :: {:ok, atom()} | {:error, term()}
  defp detect_format(path, opts) do
    case Keyword.get(opts, :format) do
      nil ->
        ext = Path.extname(path) |> String.downcase()

        case Map.get(@supported_formats, ext) do
          nil ->
            supported = Map.keys(@supported_formats) |> Enum.sort()
            {:error, {:unsupported_format, ext, [supported: supported]}}

          format ->
            {:ok, format}
        end

      format when is_atom(format) ->
        {:ok, format}
    end
  end

  # ===========================================================================
  # Private - Parsing
  # ===========================================================================

  @spec parse_file(Path.t(), atom()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_file(path, format) do
    if File.exists?(path) do
      case format do
        :turtle -> RDF.Turtle.read_file(path)
        :ntriples -> RDF.NTriples.read_file(path)
        :nquads -> parse_nquads_file(path)
        :rdfxml -> parse_rdfxml_file(path)
        :trig -> parse_trig_file(path)
        :jsonld -> parse_jsonld_file(path)
        _ -> {:error, :unsupported_format}
      end
    else
      {:error, :file_not_found}
    end
  end

  @spec parse_string(String.t(), atom(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_string(content, format, opts) do
    case format do
      :turtle -> RDF.Turtle.read_string(content, opts)
      :ntriples -> RDF.NTriples.read_string(content, opts)
      :nquads -> parse_nquads_string(content, opts)
      :rdfxml -> parse_rdfxml_string(content, opts)
      :trig -> parse_trig_string(content, opts)
      :jsonld -> parse_jsonld_string(content, opts)
      _ -> {:error, :unsupported_format}
    end
  end

  # N-Quads and TriG return Datasets - we extract only the default graph.
  # NOTE: Named graphs are discarded. See moduledoc for details on this limitation.

  @spec parse_nquads_file(Path.t()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_nquads_file(path) do
    RDF.NQuads.read_file(path) |> extract_default_graph()
  end

  @spec parse_nquads_string(String.t(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_nquads_string(content, opts) do
    RDF.NQuads.read_string(content, opts) |> extract_default_graph()
  end

  # Full N-Quads parsing - returns the complete dataset (preserves named graphs)
  @spec parse_nquads_file_full(Path.t()) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  defp parse_nquads_file_full(path) do
    RDF.NQuads.read_file(path)
  end

  @spec parse_nquads_string_full(String.t(), keyword()) ::
          {:ok, RDF.Dataset.t()} | {:error, term()}
  defp parse_nquads_string_full(content, opts) do
    RDF.NQuads.read_string(content, opts)
  end

  # Full TriG parsing - returns the complete dataset (preserves named graphs)
  @spec parse_trig_file_full(Path.t()) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  defp parse_trig_file_full(path) do
    RDF.TriG.read_file(path)
  end

  @spec parse_trig_string_full(String.t(), keyword()) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  defp parse_trig_string_full(content, opts) do
    RDF.TriG.read_string(content, opts)
  end

  @spec parse_trig_file(Path.t()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_trig_file(path) do
    RDF.TriG.read_file(path) |> extract_default_graph()
  end

  @spec parse_trig_string(String.t(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_trig_string(content, opts) do
    RDF.TriG.read_string(content, opts) |> extract_default_graph()
  end

  # Extract the default graph from a dataset parsing result.
  # Named graphs in the dataset are discarded.
  @spec extract_default_graph({:ok, RDF.Dataset.t()} | {:error, term()}) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  defp extract_default_graph({:ok, dataset}) do
    {:ok, RDF.Dataset.default_graph(dataset)}
  end

  defp extract_default_graph(error), do: error

  # RDF/XML parsing - uses optional module helper to avoid compile-time dependency
  @spec parse_rdfxml_file(Path.t()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_rdfxml_file(path) do
    call_optional_module(RDF.XML, :read_file, [path], :rdfxml_not_available)
  end

  @spec parse_rdfxml_string(String.t(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_rdfxml_string(content, opts) do
    call_optional_module(RDF.XML, :read_string, [content, opts], :rdfxml_not_available)
  end

  # JSON-LD parsing - uses optional module helper to avoid compile-time dependency
  @spec parse_jsonld_file(Path.t()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_jsonld_file(path) do
    call_optional_module(JSON.LD, :read_file, [path], :jsonld_not_available)
  end

  @spec parse_jsonld_string(String.t(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  defp parse_jsonld_string(content, opts) do
    call_optional_module(JSON.LD, :read_string, [content, opts], :jsonld_not_available)
  end

  # Helper to call functions on optional modules without compile-time dependency
  # credo:disable-for-next-line Credo.Check.Refactor.Apply
  @spec call_optional_module(module(), atom(), list(), atom()) ::
          {:ok, RDF.Graph.t()} | {:error, atom()}
  defp call_optional_module(module, function, args, error_key) do
    if Code.ensure_loaded?(module) do
      apply(module, function, args)
    else
      {:error, error_key}
    end
  end

  # ===========================================================================
  # Private Functions - Batch Size
  # ===========================================================================

  @spec resolve_batch_size(keyword()) :: pos_integer()
  defp resolve_batch_size(opts) do
    bulk_mode? = Keyword.get(opts, :bulk_mode, false)

    case {Keyword.get(opts, :batch_size), Keyword.get(opts, :memory_budget)} do
      # Explicit batch_size takes precedence
      {size, _} when not is_nil(size) -> validate_batch_size(size)
      # Explicit memory_budget takes precedence over bulk_mode default
      {nil, budget} when not is_nil(budget) -> optimal_batch_size(budget)
      # Bulk mode uses larger default batch size
      {nil, nil} when bulk_mode? -> @bulk_mode_batch_size
      # Standard default
      {nil, nil} -> @default_batch_size
    end
  end

  @spec validate_batch_size(term()) :: pos_integer()
  defp validate_batch_size(size)
       when is_integer(size) and size >= @min_batch_size and size <= @max_batch_size do
    size
  end

  defp validate_batch_size(size) when is_integer(size) and size < @min_batch_size do
    Logger.warning(
      "batch_size #{size} below minimum #{@min_batch_size}, using minimum. " <>
        "Small batch sizes reduce performance due to NIF overhead."
    )

    @min_batch_size
  end

  defp validate_batch_size(size) when is_integer(size) and size > @max_batch_size do
    Logger.warning(
      "batch_size #{size} above maximum #{@max_batch_size}, using maximum. " <>
        "Large batch sizes may cause memory pressure."
    )

    @max_batch_size
  end

  defp validate_batch_size(_), do: @default_batch_size

  @doc """
  Returns the optimal batch size based on memory budget.

  ## Arguments

  - `budget` - Memory budget: `:low`, `:medium`, `:high`, or `:auto`

  ## Returns

  - Batch size appropriate for the given memory budget

  ## Examples

      iex> Loader.optimal_batch_size(:low)
      5000

      iex> Loader.optimal_batch_size(:medium)
      10000

      iex> Loader.optimal_batch_size(:high)
      50000

      iex> Loader.optimal_batch_size(:auto)
      # Returns size based on detected system memory
  """
  @spec optimal_batch_size(memory_budget()) :: pos_integer()
  def optimal_batch_size(:auto) do
    case detect_system_memory() do
      {:ok, memory_bytes} -> batch_size_for_memory(memory_bytes)
      {:error, _} -> @default_batch_size
    end
  end

  def optimal_batch_size(budget) when budget in [:low, :medium, :high] do
    Map.get(@memory_budget_sizes, budget, @default_batch_size)
  end

  def optimal_batch_size(_), do: @default_batch_size

  @spec batch_size_for_memory(non_neg_integer()) :: pos_integer()
  defp batch_size_for_memory(memory_bytes) when memory_bytes < @memory_threshold_low do
    @memory_budget_sizes[:low]
  end

  defp batch_size_for_memory(memory_bytes) when memory_bytes > @memory_threshold_high do
    @memory_budget_sizes[:high]
  end

  defp batch_size_for_memory(_memory_bytes) do
    @memory_budget_sizes[:medium]
  end

  @spec detect_system_memory() :: {:ok, non_neg_integer()} | {:error, :not_available}
  # credo:disable-for-lines:12 Credo.Check.Refactor.Apply
  defp detect_system_memory do
    cond do
      # Try :memsup if available (requires os_mon application)
      Code.ensure_loaded?(:memsup) and function_exported?(:memsup, :get_system_memory_data, 0) ->
        try do
          data = apply(:memsup, :get_system_memory_data, [])
          total = Keyword.get(data, :total_memory, 0)
          {:ok, total}
        rescue
          _ -> read_proc_meminfo()
        end

      # Fall back to /proc/meminfo on Linux
      File.exists?("/proc/meminfo") ->
        read_proc_meminfo()

      true ->
        {:error, :not_available}
    end
  end

  @spec read_proc_meminfo() :: {:ok, non_neg_integer()} | {:error, :not_available}
  defp read_proc_meminfo do
    with {:ok, content} <- File.read("/proc/meminfo"),
         [_, kb_str] <- Regex.run(~r/MemTotal:\s+(\d+)\s+kB/, content) do
      {:ok, String.to_integer(kb_str) * 1024}
    else
      _ -> {:error, :not_available}
    end
  end

  @doc """
  Returns batch size configuration constants.

  Useful for testing and introspection.

  ## Returns

  Map with `:default`, `:min`, and `:max` batch size values.
  """
  @spec batch_size_config() :: %{default: pos_integer(), min: pos_integer(), max: pos_integer()}
  def batch_size_config do
    %{
      default: @default_batch_size,
      min: @min_batch_size,
      max: @max_batch_size
    }
  end
end
