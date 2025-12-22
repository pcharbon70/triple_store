defmodule TripleStore.Loader do
  @moduledoc """
  Bulk loading pipeline for efficient RDF data ingestion.

  Provides high-performance loading of RDF data from graphs and files
  using parallel processing with Flow and batched writes.

  ## Features

  - **Batched writes**: Groups triples into configurable batch sizes (default 1000)
  - **Parallel processing**: Uses Flow for concurrent term conversion
  - **Progress reporting**: Emits Telemetry events for monitoring
  - **Format support**: Turtle, N-Triples, N-Quads, RDF/XML, TriG, JSON-LD

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
  alias TripleStore.Index
  alias TripleStore.Dictionary.Manager

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Dictionary manager process"
  @type manager :: Manager.manager()

  @typedoc "Loading options"
  @type load_opts :: [
          batch_size: pos_integer(),
          format: atom() | nil,
          base_iri: String.t() | nil
        ]

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_batch_size 1000

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
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:triple_store, :loader, :start],
      %{system_time: System.system_time()},
      %{source: :graph, path: nil, triple_count: RDF.Graph.triple_count(graph)}
    )

    try do
      triples = RDF.Graph.triples(graph)
      result = load_triples(db, manager, triples, batch_size)

      duration = System.monotonic_time() - start_time

      case result do
        {:ok, count} ->
          :telemetry.execute(
            [:triple_store, :loader, :stop],
            %{total_count: count, duration: duration},
            %{source: :graph}
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
          %{kind: :error, reason: e}
        )

        reraise e, __STACKTRACE__
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
  - `{:error, :unsupported_format}` - Unknown file format
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
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:triple_store, :loader, :start],
      %{system_time: System.system_time()},
      %{source: :file, path: path}
    )

    try do
      with {:ok, format} <- detect_format(path, opts),
           {:ok, graph} <- parse_file(path, format) do
        triples = RDF.Graph.triples(graph)
        result = load_triples(db, manager, triples, batch_size)

        duration = System.monotonic_time() - start_time

        case result do
          {:ok, count} ->
            :telemetry.execute(
              [:triple_store, :loader, :stop],
              %{total_count: count, duration: duration},
              %{source: :file, path: path}
            )

            {:ok, count}

          {:error, _} = error ->
            error
        end
      end
    rescue
      e ->
        :telemetry.execute(
          [:triple_store, :loader, :exception],
          %{duration: System.monotonic_time() - start_time},
          %{kind: :error, reason: e, path: path}
        )

        reraise e, __STACKTRACE__
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
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
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
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    load_triples(db, manager, triple_stream, batch_size)
  end

  # ===========================================================================
  # Private - Core Loading Logic
  # ===========================================================================

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

  defp process_batch(db, manager, rdf_triples) do
    case Adapter.from_rdf_triples(manager, rdf_triples) do
      {:ok, internal_triples} ->
        Index.insert_triples(db, internal_triples)

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Private - Format Detection
  # ===========================================================================

  defp detect_format(path, opts) do
    case Keyword.get(opts, :format) do
      nil ->
        ext = Path.extname(path) |> String.downcase()

        case Map.get(@supported_formats, ext) do
          nil -> {:error, :unsupported_format}
          format -> {:ok, format}
        end

      format when is_atom(format) ->
        {:ok, format}
    end
  end

  # ===========================================================================
  # Private - Parsing
  # ===========================================================================

  defp parse_file(path, format) do
    unless File.exists?(path) do
      {:error, :file_not_found}
    else
      case format do
        :turtle -> RDF.Turtle.read_file(path)
        :ntriples -> RDF.NTriples.read_file(path)
        :nquads -> parse_nquads_file(path)
        :rdfxml -> RDF.XML.read_file(path)
        :trig -> parse_trig_file(path)
        :jsonld -> parse_jsonld_file(path)
        _ -> {:error, :unsupported_format}
      end
    end
  end

  defp parse_string(content, format, opts) do
    case format do
      :turtle -> RDF.Turtle.read_string(content, opts)
      :ntriples -> RDF.NTriples.read_string(content, opts)
      :nquads -> parse_nquads_string(content, opts)
      :rdfxml -> RDF.XML.read_string(content, opts)
      :trig -> parse_trig_string(content, opts)
      :jsonld -> parse_jsonld_string(content, opts)
      _ -> {:error, :unsupported_format}
    end
  end

  # N-Quads returns a Dataset, extract default graph
  defp parse_nquads_file(path) do
    case RDF.NQuads.read_file(path) do
      {:ok, dataset} -> {:ok, RDF.Dataset.default_graph(dataset)}
      error -> error
    end
  end

  defp parse_nquads_string(content, opts) do
    case RDF.NQuads.read_string(content, opts) do
      {:ok, dataset} -> {:ok, RDF.Dataset.default_graph(dataset)}
      error -> error
    end
  end

  # TriG returns a Dataset, extract default graph
  defp parse_trig_file(path) do
    case RDF.TriG.read_file(path) do
      {:ok, dataset} -> {:ok, RDF.Dataset.default_graph(dataset)}
      error -> error
    end
  end

  defp parse_trig_string(content, opts) do
    case RDF.TriG.read_string(content, opts) do
      {:ok, dataset} -> {:ok, RDF.Dataset.default_graph(dataset)}
      error -> error
    end
  end

  # JSON-LD parsing
  defp parse_jsonld_file(path) do
    if Code.ensure_loaded?(JSON.LD) do
      JSON.LD.read_file(path)
    else
      {:error, :jsonld_not_available}
    end
  end

  defp parse_jsonld_string(content, opts) do
    if Code.ensure_loaded?(JSON.LD) do
      JSON.LD.read_string(content, opts)
    else
      {:error, :jsonld_not_available}
    end
  end
end
