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
  - **Telemetry**: Progress monitoring via telemetry events

  ## Telemetry Events

  The exporter emits the following telemetry events:

  - `[:triple_store, :exporter, :start]` - When export begins
    - Metadata: `%{operation: :graph | :file | :string, path: String.t() | nil}`

  - `[:triple_store, :exporter, :stop]` - When export completes
    - Measurements: `%{triple_count: integer, duration: integer}`
    - Metadata: `%{operation: :graph | :file | :string}`

  - `[:triple_store, :exporter, :exception]` - On error
    - Metadata: `%{kind: :error | :exit | :throw, reason: term}`

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

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: reference()

  @typedoc "Triple pattern for filtering"
  @type pattern :: Index.pattern()

  @typedoc "Quad pattern for filtering: {s_pat, p_pat, o_pat, g_pat} where each is :bound or :var"
  @type quad_pattern :: {:bound | :var, :bound | :var, :bound | :var, :bound | :var}

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
    with_telemetry(%{operation: :graph, path: nil}, fn ->
      with {:ok, internal_triples} <- Index.lookup_all(db, pattern) do
        Adapter.to_rdf_graph(db, internal_triples, opts)
      end
    end)
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
    # Validate path to prevent path traversal attacks
    with :ok <- validate_file_path(path) do
      pattern = Keyword.get(opts, :pattern, {:var, :var, :var})
      graph_opts = Keyword.take(opts, [:name, :base_iri, :prefixes])

      with_telemetry(%{operation: :file, path: Path.basename(path), format: format}, fn ->
        # Note: export_graph already has its own telemetry, but this wraps the full file operation
        with {:ok, graph} <- do_export_graph_raw(db, pattern, graph_opts),
             :ok <- write_graph_to_file(graph, path, format) do
          {:ok, RDF.Graph.triple_count(graph)}
        end
      end)
    end
  end

  # Validate file path to prevent path traversal attacks
  # When allowed_dirs is specified, validates that the path is within those directories
  defp validate_file_path(path, allowed_dirs \\ nil) when is_binary(path) do
    # Check for path traversal in the original path before expansion
    # This catches attempts like "../", "..\\", "%2e%2e", etc.
    if has_path_traversal?(path) do
      {:error, :invalid_path}
    else
      path
      |> Path.expand()
      |> validate_expanded_file_path(allowed_dirs)
    end
  rescue
    _ -> {:error, :invalid_path}
  end

  defp validate_expanded_file_path(_path, nil), do: :ok

  defp validate_expanded_file_path(expanded_path, allowed_dirs) do
    expanded_path
    |> Path.dirname()
    |> validate_export_parent(allowed_dirs)
  end

  defp validate_export_parent(parent, allowed_dirs) do
    if Path.type(parent) == :absolute and within_allowed_dirs?(parent, allowed_dirs) do
      :ok
    else
      {:error, :invalid_path}
    end
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

  # ===========================================================================
  # Public API - Quad Export (N-Quads Format)
  # ===========================================================================

  @doc """
  Exports all quads to an N-Quads file.

  Writes all quads (including named graphs) from the quad store
  to the given file path in N-Quads format.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `path` - Output file path

  ## Options

  - `:pattern` - Quad pattern for filtering (default: all quads)
  - `:batch_size` - Number of quads to process at once (default: #{@default_batch_size})

  ## Pattern Format

  Each element of the pattern tuple is either:
  - `:var` - Matches any value (variable)
  - `{:bound, term_id}` - Matches specific term ID

  ## Returns

  - `{:ok, count}` - Number of quads exported
  - `{:error, reason}` - On failure

  ## Graph Handling

  - Quads in the default graph (ID 0) are exported without a graph name
  - Quads in named graphs (ID > 0) are exported with their graph IRI

  ## Examples

      # Export all quads
      {:ok, 1000} = Exporter.export_nquads_file(db, "output.nq")

      # Export only quads from a specific graph
      {:ok, count} = Exporter.export_nquads_file(db, "output.nq",
        pattern: {:var, :var, :var, {:bound, graph_id}}
      )
  """
  @spec export_nquads_file(db_ref(), Path.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def export_nquads_file(db, path, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    # Validate path to prevent path traversal attacks
    with :ok <- validate_file_path(path) do
      pattern = Keyword.get(opts, :pattern, {:var, :var, :var, :var})
      _batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

      with_telemetry(%{operation: :file, path: Path.basename(path), format: :nquads}, fn ->
        # Get all matching quads
        values = extract_bound_values(pattern, opts)

        internal_quads = QuadOperations.lookup_quads(db, pattern, values)

        # Convert to RDF.Quads
        all_rdf_quads =
          case Adapter.to_rdf_quads(db, internal_quads) do
            {:ok, rdf_quads} -> rdf_quads
            _ -> []
          end

        # Write to file
        dataset = RDF.Dataset.new(all_rdf_quads)

        case RDF.NQuads.write_file(dataset, path) do
          :ok -> {:ok, length(all_rdf_quads)}
          {:error, reason} -> {:error, reason}
        end
      end)
    end
  end

  @doc """
  Exports all quads to an N-Quads string.

  Serializes all quads (including named graphs) from the quad store
  to a string in N-Quads format.

  ## Arguments

  - `db` - Database reference (must be a quad store)

  ## Options

  - `:pattern` - Quad pattern for filtering (default: all quads)
  - `:batch_size` - Number of quads to process at once (default: #{@default_batch_size})

  ## Returns

  - `{:ok, content}` - N-Quads formatted string
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, nquads} = Exporter.export_nquads_string(db)
      String.contains?(nquads, "<http://example.org/subject>")
  """
  @spec export_nquads_string(db_ref(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def export_nquads_string(db, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    pattern = Keyword.get(opts, :pattern, {:var, :var, :var, :var})
    _batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with_telemetry(%{operation: :string, path: nil, format: :nquads}, fn ->
      # Get all matching quads
      values = extract_bound_values(pattern, opts)

      internal_quads = QuadOperations.lookup_quads(db, pattern, values)

      # Convert to RDF.Quads
      all_rdf_quads =
        case Adapter.to_rdf_quads(db, internal_quads) do
          {:ok, rdf_quads} -> rdf_quads
          _ -> []
        end

      # Serialize to string
      dataset = RDF.Dataset.new(all_rdf_quads)

      case RDF.NQuads.write_string(dataset, []) do
        {:ok, content} -> {:ok, content}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  # ===========================================================================
  # Public API - TriG Export
  # ===========================================================================

  @doc """
  Exports all quads to a TriG file.

  Writes all quads (including named graphs) from the quad store
  to the given file path in TriG format.

  TriG is a Turtle-like RDF syntax that supports named graphs using
  the GRAPH keyword. This format is more human-readable than N-Quads.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `path` - Output file path

  ## Options

  - `:pattern` - Quad pattern for filtering (default: all quads)
  - `:batch_size` - Number of quads to process at once (default: #{@default_batch_size})
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Pattern Format

  Each element of the pattern tuple is either:
  - `:var` - Matches any value (variable)
  - `:bound` - Matches specific term ID

  For pattern-based filtering, provide the corresponding ID options:
  - `:subject_id` - For bound subject position
  - `:predicate_id` - For bound predicate position
  - `:object_id` - For bound object position
  - `:graph_id` - For bound graph position

  ## Returns

  - `{:ok, count}` - Number of quads exported
  - `{:error, reason}` - On failure

  ## Graph Handling

  - Quads in the default graph (ID 0) are exported outside GRAPH blocks
  - Quads in named graphs (ID > 0) are exported within GRAPH <iri> { ... } blocks

  ## Examples

      # Export all quads
      {:ok, 1000} = Exporter.export_trig_file(db, "output.trig")

      # Export only quads from a specific graph
      {:ok, count} = Exporter.export_trig_file(db, "output.trig",
        pattern: {:var, :var, :var, :bound},
        graph_id: graph_id
      )
  """
  @spec export_trig_file(db_ref(), Path.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def export_trig_file(db, path, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    # Validate path to prevent path traversal attacks
    with :ok <- validate_file_path(path) do
      pattern = Keyword.get(opts, :pattern, {:var, :var, :var, :var})
      _batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

      with_telemetry(%{operation: :file, path: Path.basename(path), format: :trig}, fn ->
        # Get all matching quads
        values = extract_bound_values(pattern, opts)

        internal_quads = QuadOperations.lookup_quads(db, pattern, values)

        # Convert to RDF.Quads
        all_rdf_quads =
          case Adapter.to_rdf_quads(db, internal_quads) do
            {:ok, rdf_quads} -> rdf_quads
            _ -> []
          end

        # Write to file
        dataset = RDF.Dataset.new(all_rdf_quads)

        trig_opts = build_trig_opts(opts)

        case RDF.TriG.write_file(dataset, path, trig_opts) do
          :ok -> {:ok, length(all_rdf_quads)}
          {:error, reason} -> {:error, reason}
        end
      end)
    end
  end

  @doc """
  Exports all quads to a TriG string.

  Serializes all quads (including named graphs) from the quad store
  to a string in TriG format.

  ## Arguments

  - `db` - Database reference (must be a quad store)

  ## Options

  - `:pattern` - Quad pattern for filtering (default: all quads)
  - `:batch_size` - Number of quads to process at once (default: #{@default_batch_size})
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Returns

  - `{:ok, content}` - TriG formatted string
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, trig} = Exporter.export_trig_string(db)
      String.contains?(trig, "GRAPH <http://example.org/mygraph>")
  """
  @spec export_trig_string(db_ref(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def export_trig_string(db, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    pattern = Keyword.get(opts, :pattern, {:var, :var, :var, :var})
    _batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with_telemetry(%{operation: :string, path: nil, format: :trig}, fn ->
      # Get all matching quads
      values = extract_bound_values(pattern, opts)

      internal_quads = QuadOperations.lookup_quads(db, pattern, values)

      # Convert to RDF.Quads
      all_rdf_quads =
        case Adapter.to_rdf_quads(db, internal_quads) do
          {:ok, rdf_quads} -> rdf_quads
          _ -> []
        end

      # Serialize to string
      dataset = RDF.Dataset.new(all_rdf_quads)

      trig_opts = build_trig_opts(opts)

      case RDF.TriG.write_string(dataset, trig_opts) do
        {:ok, content} -> {:ok, content}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  # Raw export without telemetry (for use by export_file to avoid double telemetry)
  defp do_export_graph_raw(db, pattern, opts) do
    with {:ok, internal_triples} <- Index.lookup_all(db, pattern) do
      Adapter.to_rdf_graph(db, internal_triples, opts)
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

    with_telemetry(%{operation: :string, path: nil, format: format}, fn ->
      with {:ok, graph} <- do_export_graph_raw(db, pattern, graph_opts) do
        serialize_graph(graph, format)
      end
    end)
  end

  # ===========================================================================
  # Public API - Graph-Scoped Export
  # ===========================================================================

  @doc """
  Exports all quads from the store as an RDF.Dataset.

  Retrieves all quads (including named graphs) and converts them
  to an RDF.Dataset structure containing all graphs.

  ## Arguments

  - `db` - Database reference (must be a quad store)

  ## Options

  - `:pattern` - Quad pattern for filtering (default: all quads)
  - `:batch_size` - Number of quads to process at once (default: #{@default_batch_size})

  ## Returns

  - `{:ok, RDF.Dataset.t()}` - The exported dataset
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, dataset} = Exporter.export_dataset(db)
      iex> RDF.Dataset.graph_count(dataset)
      3
      iex> {:ok, dataset} = Exporter.export_dataset(db, pattern: {:var, :var, :var, :bound})
  """
  @spec export_dataset(db_ref(), keyword()) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  def export_dataset(db, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    pattern = Keyword.get(opts, :pattern, {:var, :var, :var, :var})
    _batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with_telemetry(%{operation: :dataset, path: nil}, fn ->
      values = extract_bound_values(pattern, opts)
      internal_quads = QuadOperations.lookup_quads(db, pattern, values)

      case Adapter.to_rdf_quads(db, internal_quads) do
        {:ok, rdf_quads} ->
          # Filter out :not_found entries
          valid_quads = Enum.filter(rdf_quads, &is_tuple/1)
          {:ok, RDF.Dataset.new(valid_quads)}

        {:error, _} = error ->
          error
      end
    end)
  end

  @doc """
  Exports specific named graphs as an RDF.Dataset.

  Retrieves all quads from the specified named graphs and converts them
  to an RDF.Dataset structure.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process for term-to-ID conversion
  - `graphs` - List of graph terms (RDF.IRI or RDF.BlankNode) to export

  ## Options

  - `:include_default` - Whether to include default graph (default: false)
  - `:batch_size` - Number of quads to process at once

  ## Returns

  - `{:ok, RDF.Dataset.t()}` - The exported dataset
  - `{:error, reason}` - On failure

  ## Examples

      iex> graphs = [RDF.iri("http://example.org/g1"), RDF.iri("http://example.org/g2")]
      iex> {:ok, dataset} = Exporter.export_graphs(db, manager, graphs)
      iex> RDF.Dataset.graph_count(dataset)
      2
  """
  @spec export_graphs(
          db_ref(),
          TripleStore.Dictionary.manager(),
          [RDF.IRI.t() | RDF.BlankNode.t()],
          keyword()
        ) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  def export_graphs(db, manager, graphs, opts \\ []) do
    include_default = Keyword.get(opts, :include_default, false)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with_telemetry(%{operation: :graphs, path: nil}, fn ->
      graphs
      |> graph_ids_for_terms(manager)
      |> lookup_graph_quads(db)
      |> maybe_include_default_graph(db, include_default)
      |> dataset_from_export_quads(db, batch_size)
    end)
  end

  @doc """
  Exports only the default graph as an RDF.Graph.

  Retrieves all quads from the default graph and converts them
  to an RDF.Graph structure.

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

      iex> {:ok, graph} = Exporter.export_default_graph(db)
      iex> RDF.Graph.triple_count(graph)
      42
  """
  @spec export_default_graph(db_ref(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}
  def export_default_graph(db, opts \\ []) do
    alias TripleStore.Adapter
    alias TripleStore.QuadOperations

    graph_opts = Keyword.take(opts, [:name, :base_iri, :prefixes])

    with_telemetry(%{operation: :default_graph, path: nil}, fn ->
      # Get quads from default graph (ID 0)
      internal_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      # Convert to RDF.Graph (triples only, no graph context)
      with {:ok, rdf_quads} <- Adapter.to_rdf_quads(db, internal_quads) do
        # Filter out :not_found entries and extract triples
        triples =
          rdf_quads
          |> Enum.filter(&is_tuple/1)
          |> Enum.map(fn {s, p, o, _g} -> {s, p, o} end)

        {:ok, RDF.Graph.new(triples, graph_opts)}
      end
    end)
  end

  @doc """
  Exports a single named graph as an RDF.Graph.

  Retrieves all quads from the specified named graph and converts them
  to an RDF.Graph structure.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process for term-to-ID conversion
  - `graph` - Graph term (RDF.IRI or RDF.BlankNode)

  ## Options

  - `:name` - Graph name override (defaults to the graph term)
  - `:base_iri` - Base IRI for the graph
  - `:prefixes` - Prefix mappings for serialization

  ## Returns

  - `{:ok, RDF.Graph.t()}` - The exported graph
  - `{:error, :graph_not_found}` - If graph doesn't exist
  - `{:error, reason}` - On other failures

  ## Examples

      iex> graph = RDF.iri("http://example.org/mygraph")
      iex> {:ok, graph} = Exporter.export_single_graph(db, manager, graph)
      iex> RDF.Graph.triple_count(graph)
      10
  """
  @spec export_single_graph(
          db_ref(),
          TripleStore.Dictionary.manager(),
          RDF.IRI.t() | RDF.BlankNode.t(),
          keyword()
        ) :: {:ok, RDF.Graph.t()} | {:error, term()}
  def export_single_graph(db, manager, graph_term, opts \\ []) do
    alias TripleStore.Adapter

    with_telemetry(%{operation: :single_graph, path: nil}, fn ->
      with {:ok, graph_id} <- Adapter.term_to_id(manager, graph_term),
           :ok <- ensure_graph_exists(db, manager, graph_term),
           {:ok, rdf_quads} <-
             graph_id |> lookup_graph_quads(db) |> then(&Adapter.to_rdf_quads(db, &1)) do
        {:ok, build_exported_graph(rdf_quads, graph_term, opts)}
      end
    end)
  end

  @doc """
  Exports multiple named graphs as an RDF.Dataset.

  Retrieves all quads from the specified named graphs and converts them
  to an RDF.Dataset structure containing all the graphs.

  This is an alias for `export_graphs/4` with clearer naming for the
  multiple graph use case.

  ## Arguments

  - `db` - Database reference (must be a quad store)
  - `manager` - Dictionary manager process for term-to-ID conversion
  - `graphs` - List of graph terms (RDF.IRI or RDF.BlankNode) to export

  ## Options

  - `:include_default` - Whether to include default graph (default: false)
  - `:batch_size` - Number of quads to process at once

  ## Returns

  - `{:ok, RDF.Dataset.t()}` - The exported dataset
  - `{:error, reason}` - On failure

  ## Examples

      iex> graphs = [RDF.iri("http://example.org/g1"), RDF.iri("http://example.org/g2")]
      iex> {:ok, dataset} = Exporter.export_multiple_graphs(db, manager, graphs)
      iex> RDF.Dataset.graph_count(dataset)
      2
  """
  @spec export_multiple_graphs(
          db_ref(),
          TripleStore.Dictionary.manager(),
          [RDF.IRI.t() | RDF.BlankNode.t()],
          keyword()
        ) :: {:ok, RDF.Dataset.t()} | {:error, term()}
  def export_multiple_graphs(db, manager, graphs, opts \\ []) do
    export_graphs(db, manager, graphs, opts)
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

            {:error, reason} ->
              # Log the error rather than silently swallowing it
              Logger.warning("Error converting batch to RDF triples: #{inspect(reason)}")
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
  # Public API - Pattern Convenience Wrappers
  # ===========================================================================

  @doc """
  Exports all triples with a specific subject.

  Convenience wrapper around `export_graph/3` for subject-based filtering.

  ## Arguments

  - `db` - Database reference
  - `subject_id` - The internal term ID for the subject

  ## Options

  Same as `export_graph/3`

  ## Returns

  - `{:ok, RDF.Graph.t()}` - Graph containing matching triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, graph} = Exporter.export_by_subject(db, subject_id)
  """
  @spec export_by_subject(db_ref(), non_neg_integer(), export_opts()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def export_by_subject(db, subject_id, opts \\ []) do
    export_graph(db, {{:bound, subject_id}, :var, :var}, opts)
  end

  @doc """
  Exports all triples with a specific predicate.

  Convenience wrapper around `export_graph/3` for predicate-based filtering.

  ## Arguments

  - `db` - Database reference
  - `predicate_id` - The internal term ID for the predicate

  ## Options

  Same as `export_graph/3`

  ## Returns

  - `{:ok, RDF.Graph.t()}` - Graph containing matching triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, graph} = Exporter.export_by_predicate(db, predicate_id)
  """
  @spec export_by_predicate(db_ref(), non_neg_integer(), export_opts()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def export_by_predicate(db, predicate_id, opts \\ []) do
    export_graph(db, {:var, {:bound, predicate_id}, :var}, opts)
  end

  @doc """
  Exports all triples with a specific object.

  Convenience wrapper around `export_graph/3` for object-based filtering.

  ## Arguments

  - `db` - Database reference
  - `object_id` - The internal term ID for the object

  ## Options

  Same as `export_graph/3`

  ## Returns

  - `{:ok, RDF.Graph.t()}` - Graph containing matching triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, graph} = Exporter.export_by_object(db, object_id)
  """
  @spec export_by_object(db_ref(), non_neg_integer(), export_opts()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def export_by_object(db, object_id, opts \\ []) do
    export_graph(db, {:var, :var, {:bound, object_id}}, opts)
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

  # apply/3 is intentional to avoid compile-time dependency on optional module
  # credo:disable-for-lines:7 Credo.Check.Refactor.Apply
  defp serialize_graph(graph, :rdfxml) do
    if Code.ensure_loaded?(RDF.XML) do
      apply(RDF.XML, :write_string, [graph])
    else
      {:error, :rdfxml_not_available}
    end
  end

  # credo:disable-for-lines:7 Credo.Check.Refactor.Apply
  defp serialize_graph(graph, :jsonld) do
    if Code.ensure_loaded?(JSON.LD) do
      apply(JSON.LD, :write_string, [graph])
    else
      {:error, :jsonld_not_available}
    end
  end

  defp serialize_graph(_graph, format) do
    {:error, {:unsupported_format, format}}
  end

  # ===========================================================================
  # Private - Telemetry Helper
  # ===========================================================================

  # Wraps an export operation with telemetry events.
  # Handles :ok results for graph exports and {:ok, result} for count/string exports.
  defp with_telemetry(metadata, func) do
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:triple_store, :exporter, :start],
      %{system_time: System.system_time()},
      metadata
    )

    try do
      case func.() do
        {:ok, %RDF.Dataset{} = dataset} ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :exporter, :stop],
            %{graph_count: RDF.Dataset.graph_count(dataset), duration: duration},
            Map.take(metadata, [:operation, :path])
          )

          {:ok, dataset}

        {:ok, %RDF.Graph{} = graph} ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :exporter, :stop],
            %{triple_count: RDF.Graph.triple_count(graph), duration: duration},
            Map.take(metadata, [:operation, :path])
          )

          {:ok, graph}

        {:ok, count} when is_integer(count) ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :exporter, :stop],
            %{triple_count: count, duration: duration},
            Map.take(metadata, [:operation, :path])
          )

          {:ok, count}

        {:ok, content} when is_binary(content) ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:triple_store, :exporter, :stop],
            %{byte_size: byte_size(content), duration: duration},
            Map.take(metadata, [:operation, :path])
          )

          {:ok, content}

        {:error, _} = error ->
          error
      end
    rescue
      e ->
        :telemetry.execute(
          [:triple_store, :exporter, :exception],
          %{duration: System.monotonic_time() - start_time},
          Map.merge(Map.take(metadata, [:operation, :path]), %{kind: :error, reason: e})
        )

        reraise e, __STACKTRACE__
    end
  end

  # ===========================================================================
  # Private - Quad Export Helpers
  # ===========================================================================

  # Extracts bound values from opts based on the quad pattern.
  # For positions that are :bound in the pattern, extracts the corresponding
  # value from opts. For :var positions, no value is needed.
  #
  # This is used for N-Quads export where the pattern specifies which
  # positions are bound (e.g., {:var, :var, :var, :bound} for graph-scoped export).
  @spec extract_bound_values(quad_pattern(), keyword()) :: map()
  defp extract_bound_values({s_pat, p_pat, o_pat, g_pat}, opts) do
    [
      {s_pat, :s, :subject_id},
      {p_pat, :p, :predicate_id},
      {o_pat, :o, :object_id},
      {g_pat, :g, :graph_id}
    ]
    |> Enum.reduce(%{}, fn {pattern, key, opt_key}, values ->
      maybe_put_bound_value(values, pattern, key, Keyword.get(opts, opt_key))
    end)
  end

  defp graph_ids_for_terms(graphs, manager) do
    Enum.flat_map(graphs, fn graph_term ->
      case Adapter.term_to_id(manager, graph_term) do
        {:ok, graph_id} -> [graph_id]
        _ -> []
      end
    end)
  end

  defp lookup_graph_quads(graph_ids, db) when is_list(graph_ids) do
    alias TripleStore.QuadOperations

    Enum.flat_map(graph_ids, fn graph_id ->
      lookup_graph_quads(graph_id, db)
    end)
  end

  defp lookup_graph_quads(graph_id, db) do
    alias TripleStore.QuadOperations

    QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})
  end

  defp maybe_include_default_graph(quads, _db, false), do: quads

  defp maybe_include_default_graph(quads, db, true) do
    lookup_graph_quads(0, db) ++ quads
  end

  defp dataset_from_export_quads(quads, db, batch_size) do
    quads
    |> Enum.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, []}, fn batch, {:ok, acc} ->
      append_export_quad_batch(db, batch, acc)
    end)
    |> finalize_dataset_export()
  end

  defp append_export_quad_batch(db, batch, acc) do
    case Adapter.to_rdf_quads(db, batch) do
      {:ok, rdf_quads} ->
        {:cont, {:ok, acc ++ valid_rdf_quads(rdf_quads)}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp finalize_dataset_export({:ok, rdf_quads}), do: {:ok, RDF.Dataset.new(rdf_quads)}
  defp finalize_dataset_export(error), do: error

  defp ensure_graph_exists(db, manager, graph_term) do
    alias TripleStore.QuadOperations

    if QuadOperations.graph_exists?(db, manager, graph_term) do
      :ok
    else
      {:error, :graph_not_found}
    end
  end

  defp build_exported_graph(rdf_quads, graph_term, opts) do
    rdf_quads
    |> graph_triples_from_quads()
    |> then(&RDF.Graph.new(&1, export_single_graph_opts(graph_term, opts)))
  end

  defp graph_triples_from_quads(rdf_quads) do
    Enum.map(valid_rdf_quads(rdf_quads), fn {s, p, o, _g} -> {s, p, o} end)
  end

  defp valid_rdf_quads(rdf_quads) do
    Enum.filter(rdf_quads, &is_tuple/1)
  end

  defp export_single_graph_opts(graph_term, opts) do
    if Keyword.has_key?(opts, :name) do
      Keyword.take(opts, [:name, :base_iri, :prefixes])
    else
      opts
      |> Keyword.take([:base_iri, :prefixes])
      |> Keyword.put(:name, graph_term)
    end
  end

  defp maybe_put_bound_value(values, :bound, _key, nil), do: values
  defp maybe_put_bound_value(values, :bound, key, value), do: Map.put(values, key, value)
  defp maybe_put_bound_value(values, :var, _key, _value), do: values

  # ===========================================================================
  # Helper Functions - TriG Options
  # ===========================================================================

  # Extracts TriG-specific options from the opts keyword list.
  #
  # Returns a keyword list containing only the options that are relevant
  # for TriG serialization (base_iri, prefixes).
  #
  # Arguments:
  # - `opts` - Full options keyword list
  #
  # Returns:
  # - Keyword list with TriG-specific options
  @spec build_trig_opts(keyword()) :: keyword()
  defp build_trig_opts(opts) do
    opts
    |> Keyword.take([:base_iri, :prefixes])
  end
end
