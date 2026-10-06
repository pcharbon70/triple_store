defmodule TripleStore.Benchmark.LDBC.RDFStream do
  @moduledoc """
  Bounded-memory validation and transformation for line-oriented RDF.

  N-Triples and N-Quads statements are parsed one physical line at a time.
  Malformed input returns a tagged line number and is never silently repaired.
  """

  alias TripleStore.Benchmark.Artifact

  @type format :: :ntriples | :nquads
  @type statement :: RDF.Triple.t() | RDF.Quad.t()

  @doc "Reduces parsed statements while preserving bounded input memory."
  @spec reduce(Path.t(), format(), acc, (statement(), acc -> acc)) ::
          {:ok, acc, non_neg_integer()} | {:error, term()}
        when acc: term()
  def reduce(path, format, initial, reducer)
      when format in [:ntriples, :nquads] and is_function(reducer, 2) do
    path
    |> File.stream!(:line)
    |> Stream.with_index(1)
    |> Enum.reduce_while({:ok, initial, 0}, &reduce_line(&1, &2, format, reducer))
  rescue
    error in File.Error -> {:error, {:file_error, error.reason}}
  end

  defp reduce_line({line, line_number}, {:ok, acc, count}, format, reducer) do
    if Artifact.statement_line?(line) do
      reduce_statement(line, line_number, acc, count, format, reducer)
    else
      {:cont, {:ok, acc, count}}
    end
  end

  defp reduce_statement(line, line_number, acc, count, format, reducer) do
    case parse_line(line, format) do
      {:ok, statement} -> {:cont, {:ok, reducer.(statement, acc), count + 1}}
      {:error, reason} -> {:halt, {:error, {:rdf_parse_error, line_number, reason}}}
    end
  end

  @doc "Scans syntax, counts statements, and records graph identities."
  @spec scan(Path.t(), format()) :: {:ok, map()} | {:error, term()}
  def scan(path, format) do
    reducer = fn statement, graphs ->
      case statement do
        {_subject, _predicate, _object, nil} -> MapSet.put(graphs, :default)
        {_subject, _predicate, _object, graph} -> MapSet.put(graphs, to_string(graph))
        {_subject, _predicate, _object} -> MapSet.put(graphs, :default)
      end
    end

    with {:ok, graphs, count} <- reduce(path, format, MapSet.new(), reducer),
         {:ok, checksum} <- Artifact.checksum(path) do
      {:ok,
       %{
         statement_count: count,
         checksum: checksum,
         graphs: graphs |> MapSet.to_list() |> Enum.sort()
       }}
    end
  end

  @doc "Validates and copies bytes unchanged through an atomic partial file."
  @spec validate_and_copy(Path.t(), Path.t(), format()) :: {:ok, map()} | {:error, term()}
  def validate_and_copy(source, destination, format) do
    with {:ok, scan} <- scan(source, format),
         :ok <- Artifact.atomic_copy(source, destination),
         {:ok, copied_checksum} <- Artifact.checksum(destination),
         true <- copied_checksum == scan.checksum do
      {:ok, Map.put(scan, :normalization, :byte_preserving_copy)}
    else
      false -> {:error, :copy_checksum_mismatch}
      {:error, _} = error -> error
    end
  end

  @doc "Parses exactly one N-Triples or N-Quads statement."
  @spec parse_line(String.t(), format()) :: {:ok, statement()} | {:error, term()}
  def parse_line(line, :ntriples) do
    with {:ok, graph} <- RDF.NTriples.read_string(line),
         [triple] <- RDF.Graph.triples(graph) do
      {:ok, triple}
    else
      {:error, reason} ->
        {:error, reason}

      statements when is_list(statements) ->
        {:error, {:expected_one_statement, length(statements)}}
    end
  end

  def parse_line(line, :nquads) do
    with {:ok, dataset} <- RDF.NQuads.read_string(line),
         [quad] <- RDF.Dataset.quads(dataset) do
      {:ok, quad}
    else
      {:error, reason} ->
        {:error, reason}

      statements when is_list(statements) ->
        {:error, {:expected_one_statement, length(statements)}}
    end
  end
end
