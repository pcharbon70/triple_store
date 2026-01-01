defmodule TripleStore.Loader do
  @moduledoc """
  Bulk loading pipeline for efficient RDF data ingestion.

  Provides high-performance loading of RDF data from graphs and files
  using batched writes and progress reporting via Telemetry.

  ## Features

  - **Batched writes**: Groups triples into configurable batch sizes (default 10,000)
  - **Dynamic batch sizing**: Automatically adjusts batch size based on memory budget
  - **Sequential processing**: Uses `Enum.reduce_while` with batched writes for reliable loading
  - **Progress reporting**: Emits Telemetry events for monitoring
  - **Format support**: Turtle, N-Triples, N-Quads, RDF/XML, TriG, JSON-LD
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

  ### Memory Budget Options

  Use the `:memory_budget` option for automatic batch sizing:

  - `:low` - 5,000 triples/batch (for memory-constrained systems)
  - `:medium` - 10,000 triples/batch (default, balanced)
  - `:high` - 50,000 triples/batch (for systems with ample memory)
  - `:auto` - Detects system memory and selects appropriate size

  ## Important Limitations

  ### Named Graphs Not Supported

  When loading N-Quads (`.nq`) or TriG (`.trig`) files, **only the default graph
  is loaded**. Named graphs are discarded. This is a current architectural
  limitation of the triple store which stores triples, not quads.

  If you need named graph support for provenance tracking or SPARQL named graph
  queries, this will be addressed in Phase 2 (SPARQL Engine) with quad storage.

  ## Telemetry Events

  The loader emits the following telemetry events:

  - `[:triple_store, :loader, :start]` - When loading begins
    - Metadata: `%{source: :graph | :file, path: String.t() | nil}`

  - `[:triple_store, :loader, :batch]` - After each batch is written
    - Measurements: `%{count: integer, duration: integer}`
    - Metadata: `%{batch_number: integer}`

  - `[:triple_store, :loader, :stop]` - When loading completes
    - Measurements: `%{total_count: integer, duration: integer}`
    - Metadata: `%{source: :graph | :file}`

  - `[:triple_store, :loader, :exception]` - On error
    - Metadata: `%{kind: :error | :exit | :throw, reason: term}`

  ## Usage

      # Load from an RDF.Graph
      {:ok, count} = Loader.load_graph(db, manager, graph)

      # Load from a file (format auto-detected)
      {:ok, count} = Loader.load_file(db, manager, "data.ttl")

      # With custom options
      {:ok, count} = Loader.load_graph(db, manager, graph,
        batch_size: 5000,
        stages: 4
      )
  """

  alias TripleStore.Adapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Dictionary manager process"
  @type manager :: Manager.manager()

  @typedoc "Memory budget options for automatic batch sizing"
  @type memory_budget :: :low | :medium | :high | :auto

  @typedoc "Loading options"
  @type load_opts :: [
          batch_size: pos_integer(),
          memory_budget: memory_budget(),
          format: atom() | nil,
          base_iri: String.t() | nil,
          max_file_size: pos_integer() | nil
        ]

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_batch_size 10_000
  @min_batch_size 100
  @max_batch_size 100_000
  # 100MB
  @default_max_file_size 100_000_000

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
  Loads an RDF.Graph into the triple store.

  Converts all triples in the graph to internal representation and
  inserts them in batches for efficient storage.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager process
  - `graph` - RDF.Graph to load

  ## Options

  - `:batch_size` - Number of triples per batch (default: #{@default_batch_size})

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, reason}` - On failure

  ## Examples

      iex> graph = RDF.Graph.new([{~I<http://ex.org/s>, ~I<http://ex.org/p>, "o"}])
      iex> {:ok, 1} = Loader.load_graph(db, manager, graph)

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events.
  """
  @spec load_graph(db_ref(), manager(), RDF.Graph.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_graph(db, manager, %RDF.Graph{} = graph, opts \\ []) do
    batch_size = resolve_batch_size(opts)

    start_metadata = %{
      source: :graph,
      path: nil,
      triple_count: RDF.Graph.triple_count(graph)
    }

    with_telemetry(start_metadata, fn ->
      triples = RDF.Graph.triples(graph)
      load_triples(db, manager, triples, batch_size)
    end)
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

  ## Supported Formats

  - Turtle (`.ttl`, `.turtle`)
  - N-Triples (`.nt`, `.ntriples`)
  - N-Quads (`.nq`, `.nquads`)
  - RDF/XML (`.rdf`, `.xml`)
  - TriG (`.trig`)
  - JSON-LD (`.jsonld`, `.json`)

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, :file_not_found}` - File does not exist
  - `{:error, :invalid_path}` - Path contains traversal sequences (`..`)
  - `{:error, {:file_too_large, size, max}}` - File exceeds size limit
  - `{:error, {:unsupported_format, ext, [supported: list]}}` - Unknown file format
  - `{:error, reason}` - On parse or load failure

  ## Examples

      iex> {:ok, count} = Loader.load_file(db, manager, "data.ttl")

      iex> {:ok, count} = Loader.load_file(db, manager, "data.rdf", format: :rdfxml)

  ## Telemetry

  Emits `[:triple_store, :loader, :start]`, `[:triple_store, :loader, :batch]`,
  and `[:triple_store, :loader, :stop]` events.
  """
  @spec load_file(db_ref(), manager(), Path.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_file(db, manager, path, opts \\ []) do
    batch_size = resolve_batch_size(opts)
    max_file_size = Keyword.get(opts, :max_file_size, @default_max_file_size)

    start_metadata = %{source: :file, path: Path.basename(path)}

    with_telemetry(start_metadata, fn ->
      with {:ok, validated_path} <- validate_file_path(path),
           {:ok, format} <- detect_format(validated_path, opts),
           :ok <- check_file_size(validated_path, max_file_size),
           {:ok, graph} <- parse_file(validated_path, format) do
        triples = RDF.Graph.triples(graph)
        load_triples(db, manager, triples, batch_size)
      end
    end)
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

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, reason}` - On parse or load failure

  ## Examples

      iex> ttl = \"\"\"
      ...> @prefix ex: <http://example.org/> .
      ...> ex:s ex:p "object" .
      ...> \"\"\"
      iex> {:ok, 1} = Loader.load_string(db, manager, ttl, :turtle)
  """
  @spec load_string(db_ref(), manager(), String.t(), atom(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_string(db, manager, content, format, opts \\ []) do
    batch_size = resolve_batch_size(opts)
    base_iri = Keyword.get(opts, :base_iri)

    parse_opts = if base_iri, do: [base_iri: base_iri], else: []

    case parse_string(content, format, parse_opts) do
      {:ok, graph} ->
        triples = RDF.Graph.triples(graph)
        load_triples(db, manager, triples, batch_size)

      {:error, _} = error ->
        error
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

  ## Returns

  - `{:ok, count}` - Number of triples loaded
  - `{:error, reason}` - On failure

  ## Examples

      iex> triples = [
      ...>   {~I<http://ex.org/s1>, ~I<http://ex.org/p>, "o1"},
      ...>   {~I<http://ex.org/s2>, ~I<http://ex.org/p>, "o2"}
      ...> ]
      iex> {:ok, 2} = Loader.load_stream(db, manager, triples)
  """
  @spec load_stream(db_ref(), manager(), Enumerable.t(), load_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def load_stream(db, manager, triple_stream, opts \\ []) do
    batch_size = resolve_batch_size(opts)
    load_triples(db, manager, triple_stream, batch_size)
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
  # Private - Core Loading Logic
  # ===========================================================================

  @spec load_triples(db_ref(), manager(), Enumerable.t(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp load_triples(db, manager, triples, batch_size) do
    triples
    |> Stream.chunk_every(batch_size)
    |> Stream.with_index(1)
    |> Enum.reduce_while({:ok, 0}, fn {batch, batch_number}, {:ok, total} ->
      batch_start = System.monotonic_time()

      case process_batch(db, manager, batch) do
        :ok ->
          batch_count = length(batch)
          batch_duration = System.monotonic_time() - batch_start

          :telemetry.execute(
            [:triple_store, :loader, :batch],
            %{count: batch_count, duration: batch_duration},
            %{batch_number: batch_number}
          )

          {:cont, {:ok, total + batch_count}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @spec process_batch(db_ref(), manager(), [RDF.Triple.t()]) :: :ok | {:error, term()}
  defp process_batch(db, manager, rdf_triples) do
    case Adapter.from_rdf_triples(manager, rdf_triples) do
      {:ok, internal_triples} ->
        Index.insert_triples(db, internal_triples)

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Private - Telemetry Helper
  # ===========================================================================

  @spec with_telemetry(map(), (-> {:ok, non_neg_integer()} | {:error, term()})) ::
          {:ok, non_neg_integer()} | {:error, term()}
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

  @spec validate_file_path(Path.t()) :: {:ok, Path.t()} | {:error, :invalid_path}
  defp validate_file_path(path) do
    # Prevent path traversal attacks
    if String.contains?(path, "..") do
      {:error, :invalid_path}
    else
      {:ok, Path.expand(path)}
    end
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
    case {Keyword.get(opts, :batch_size), Keyword.get(opts, :memory_budget)} do
      {nil, nil} -> @default_batch_size
      {nil, budget} -> optimal_batch_size(budget)
      {size, _} -> validate_batch_size(size)
    end
  end

  @spec validate_batch_size(term()) :: pos_integer()
  defp validate_batch_size(size)
       when is_integer(size) and size >= @min_batch_size and size <= @max_batch_size do
    size
  end

  defp validate_batch_size(size) when is_integer(size) and size < @min_batch_size do
    @min_batch_size
  end

  defp validate_batch_size(size) when is_integer(size) and size > @max_batch_size do
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
