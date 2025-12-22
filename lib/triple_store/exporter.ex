defmodule TripleStore.Exporter do
  @moduledoc """
  Export functions for serializing stored triples to RDF formats.

  Provides functions to export triples from the store back to RDF.Graph
  structures and various file formats.

  ## Features

  - **Full export**: Export all triples as an RDF.Graph
  - **Pattern filtering**: Export only triples matching a pattern
  - **File output**: Write to Turtle, N-Triples, N-Quads, or other formats
  - **Streaming**: Memory-efficient export for large datasets

  ## Usage

      # Export all triples as a graph
      {:ok, graph} = Exporter.export_graph(db)

      # Export with pattern filter (all triples with specific predicate)
      pattern = {:var, {:bound, predicate_id}, :var}
      {:ok, graph} = Exporter.export_graph(db, pattern)

      # Export to file
      {:ok, count} = Exporter.export_file(db, "output.ttl", :turtle)

      # Stream triples for memory-efficient processing
      {:ok, stream} = Exporter.stream_triples(db)
      Enum.each(stream, fn triple -> ... end)
  """

  alias TripleStore.Adapter
  alias TripleStore.Index

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Triple pattern for filtering"
  @type pattern :: Index.pattern()

  @typedoc "RDF serialization format"
  @type format :: :turtle | :ntriples | :nquads | :trig | :rdfxml | :jsonld

  @typedoc "Export options"
  @type export_opts :: [
          name: RDF.IRI.t() | nil,
          base_iri: String.t() | nil,
          prefixes: map() | nil
        ]

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_batch_size 1000

  # Format to file extension mapping
  @format_extensions %{
    turtle: ".ttl",
    ntriples: ".nt",
    nquads: ".nq",
    trig: ".trig",
    rdfxml: ".rdf",
    jsonld: ".jsonld"
  }

  # ===========================================================================
  # Public API - Graph Export
  # ===========================================================================

  @doc """
  Exports all triples from the store as an RDF.Graph.

  Retrieves all triples using the {:var, :var, :var} pattern and
  converts them to an RDF.Graph structure.

  ## Arguments

  - `db` - Database reference

  ## Options

  - `:name` - Graph name (IRI)
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Returns

  - `{:ok, RDF.Graph.t()}` - The exported graph
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, graph} = Exporter.export_graph(db)
      iex> RDF.Graph.triple_count(graph)
      42
  """
  @spec export_graph(db_ref(), export_opts()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  def export_graph(db, opts \\ [])

  def export_graph(db, opts) when is_list(opts) do
    do_export_graph(db, {:var, :var, :var}, opts)
  end

  @doc """
  Exports triples matching a pattern as an RDF.Graph.

  Retrieves triples using the given pattern and converts them
  to an RDF.Graph structure.

  ## Arguments

  - `db` - Database reference
  - `pattern` - Triple pattern for filtering (see `TripleStore.Index`)

  ## Options

  - `:name` - Graph name (IRI)
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Pattern Format

  Each element of the pattern tuple is either:
  - `:var` - Matches any value (variable)
  - `{:bound, term_id}` - Matches specific term ID

  ## Returns

  - `{:ok, RDF.Graph.t()}` - The exported graph
  - `{:error, reason}` - On failure

  ## Examples

      iex> # Export all triples with a specific subject
      iex> pattern = {{:bound, subject_id}, :var, :var}
      iex> {:ok, graph} = Exporter.export_graph(db, pattern)

      iex> # Export all triples with a specific predicate
      iex> pattern = {:var, {:bound, predicate_id}, :var}
      iex> {:ok, graph} = Exporter.export_graph(db, pattern)
  """
  @spec export_graph(db_ref(), pattern(), export_opts()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def export_graph(db, pattern, opts) when is_tuple(pattern) do
    do_export_graph(db, pattern, opts)
  end

  defp do_export_graph(db, pattern, opts) do
    with {:ok, internal_triples} <- Index.lookup_all(db, pattern),
         {:ok, graph} <- Adapter.to_rdf_graph(db, internal_triples, opts) do
      {:ok, graph}
    end
  end

  # ===========================================================================
  # Public API - File Export
  # ===========================================================================

  @doc """
  Exports triples to a file in the specified format.

  Writes all triples from the store to the given file path
  in the specified RDF format.

  ## Arguments

  - `db` - Database reference
  - `path` - Output file path
  - `format` - Output format (`:turtle`, `:ntriples`, `:nquads`, etc.)

  ## Options

  - `:pattern` - Triple pattern for filtering (default: all triples)
  - `:name` - Graph name (IRI)
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Supported Formats

  - `:turtle` - Turtle format (.ttl)
  - `:ntriples` - N-Triples format (.nt)
  - `:nquads` - N-Quads format (.nq)
  - `:trig` - TriG format (.trig)
  - `:rdfxml` - RDF/XML format (.rdf) - requires optional dependency
  - `:jsonld` - JSON-LD format (.jsonld) - requires optional dependency

  ## Returns

  - `{:ok, count}` - Number of triples exported
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Exporter.export_file(db, "output.ttl", :turtle)
      iex> count
      42

      iex> {:ok, count} = Exporter.export_file(db, "output.nt", :ntriples,
      ...>   pattern: {{:bound, subject_id}, :var, :var})
  """
  @spec export_file(db_ref(), Path.t(), format(), export_opts()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def export_file(db, path, format, opts \\ []) do
    pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
    graph_opts = Keyword.take(opts, [:name, :base_iri, :prefixes])

    with {:ok, graph} <- export_graph(db, pattern, graph_opts),
         :ok <- write_graph_to_file(graph, path, format) do
      {:ok, RDF.Graph.triple_count(graph)}
    end
  end

  @doc """
  Exports triples to a string in the specified format.

  Serializes all triples from the store to a string
  in the specified RDF format.

  ## Arguments

  - `db` - Database reference
  - `format` - Output format (`:turtle`, `:ntriples`, `:nquads`, etc.)

  ## Options

  - `:pattern` - Triple pattern for filtering (default: all triples)
  - `:name` - Graph name (IRI)
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Returns

  - `{:ok, content}` - Serialized RDF content
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, ttl} = Exporter.export_string(db, :turtle)
      iex> String.contains?(ttl, "<http://example.org/subject>")
      true
  """
  @spec export_string(db_ref(), format(), export_opts()) ::
          {:ok, String.t()} | {:error, term()}
  def export_string(db, format, opts \\ []) do
    pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
    graph_opts = Keyword.take(opts, [:name, :base_iri, :prefixes])

    with {:ok, graph} <- export_graph(db, pattern, graph_opts) do
      serialize_graph(graph, format)
    end
  end

  # ===========================================================================
  # Public API - Streaming Export
  # ===========================================================================

  @doc """
  Returns a stream of RDF triples from the store.

  Provides a lazy stream of triples for memory-efficient processing
  of large datasets. Triples are converted to RDF.ex format on demand.

  ## Arguments

  - `db` - Database reference

  ## Options

  - `:pattern` - Triple pattern for filtering (default: all triples)
  - `:batch_size` - Number of triples to convert at once (default: #{@default_batch_size})

  ## Returns

  - `{:ok, Stream.t()}` - Stream of RDF triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, stream} = Exporter.stream_triples(db)
      iex> stream |> Enum.take(10)
      [{~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"}, ...]

      iex> # Stream with pattern filter
      iex> {:ok, stream} = Exporter.stream_triples(db,
      ...>   pattern: {{:bound, subject_id}, :var, :var})
  """
  @spec stream_triples(db_ref(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def stream_triples(db, opts \\ []) do
    pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with {:ok, internal_stream} <- Index.lookup(db, pattern) do
      rdf_stream =
        internal_stream
        |> Stream.chunk_every(batch_size)
        |> Stream.flat_map(fn batch ->
          case Adapter.to_rdf_triples(db, batch) do
            {:ok, rdf_triples} ->
              # Filter out :not_found entries
              Enum.filter(rdf_triples, &is_tuple/1)

            {:error, _} ->
              []
          end
        end)

      {:ok, rdf_stream}
    end
  end

  @doc """
  Returns a stream of internal triples from the store.

  Provides raw internal triples (term IDs) without conversion.
  Useful when you need to process IDs directly.

  ## Arguments

  - `db` - Database reference

  ## Options

  - `:pattern` - Triple pattern for filtering (default: all triples)

  ## Returns

  - `{:ok, Stream.t()}` - Stream of internal triples `{s_id, p_id, o_id}`
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, stream} = Exporter.stream_internal_triples(db)
      iex> stream |> Enum.take(10)
      [{1000, 1001, 1002}, ...]
  """
  @spec stream_internal_triples(db_ref(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def stream_internal_triples(db, opts \\ []) do
    pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
    Index.lookup(db, pattern)
  end

  # ===========================================================================
  # Public API - Utilities
  # ===========================================================================

  @doc """
  Returns the count of triples matching a pattern.

  ## Arguments

  - `db` - Database reference
  - `pattern` - Triple pattern (default: all triples)

  ## Returns

  - `{:ok, count}` - Number of matching triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Exporter.count(db)
      iex> count
      42
  """
  @spec count(db_ref(), pattern()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(db, pattern \\ {:var, :var, :var}) do
    Index.count(db, pattern)
  end

  @doc """
  Returns the suggested file extension for a format.

  ## Examples

      iex> Exporter.format_extension(:turtle)
      ".ttl"

      iex> Exporter.format_extension(:ntriples)
      ".nt"
  """
  @spec format_extension(format()) :: String.t()
  def format_extension(format) do
    Map.get(@format_extensions, format, ".rdf")
  end

  # ===========================================================================
  # Private - Serialization
  # ===========================================================================

  defp write_graph_to_file(graph, path, format) do
    case serialize_graph(graph, format) do
      {:ok, content} -> File.write(path, content)
      {:error, _} = error -> error
    end
  end

  defp serialize_graph(graph, :turtle) do
    RDF.Turtle.write_string(graph)
  end

  defp serialize_graph(graph, :ntriples) do
    RDF.NTriples.write_string(graph)
  end

  defp serialize_graph(graph, :nquads) do
    # N-Quads expects a dataset
    dataset = RDF.Dataset.new(graph)
    RDF.NQuads.write_string(dataset)
  end

  defp serialize_graph(graph, :trig) do
    # TriG expects a dataset
    dataset = RDF.Dataset.new(graph)
    RDF.TriG.write_string(dataset)
  end

  defp serialize_graph(graph, :rdfxml) do
    if Code.ensure_loaded?(RDF.XML) do
      RDF.XML.write_string(graph)
    else
      {:error, :rdfxml_not_available}
    end
  end

  defp serialize_graph(graph, :jsonld) do
    if Code.ensure_loaded?(JSON.LD) do
      JSON.LD.write_string(graph)
    else
      {:error, :jsonld_not_available}
    end
  end

  defp serialize_graph(_graph, format) do
    {:error, {:unsupported_format, format}}
  end
end
