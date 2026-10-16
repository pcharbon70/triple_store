defmodule TripleStore.Benchmark.WatDiv.ReferenceAnswers do
  @moduledoc """
  Loads independent WatDiv reference answers in SPARQL Results JSON format.

  Reference directories contain one `.srj` file for each workload query and a
  manifest binding those answers to an exact dataset and workload fingerprint.
  Answers are normalized using the same canonical representation as the
  Wikidata correctness harness, including duplicate preservation.
  """

  alias TripleStore.Benchmark.WatDiv.Workload
  alias TripleStore.Benchmark.Wikidata.AnswerNormalizer

  @type t :: %{
          engine: String.t(),
          engine_version: String.t(),
          dataset_sha256: String.t(),
          workload_sha256: String.t(),
          entries: %{String.t() => map()},
          root: Path.t()
        }

  @doc "Loads and validates a reference-answer directory."
  @spec load(Path.t(), Workload.t(), map()) :: {:ok, t()} | {:error, term()}
  def load(reference_dir, workload, dataset_manifest)
      when is_binary(reference_dir) and is_map(workload) and is_map(dataset_manifest) do
    manifest_path = Path.join(reference_dir, "manifest.json")

    with {:ok, json} <- File.read(manifest_path),
         {:ok, manifest} when is_map(manifest) <- Jason.decode(json),
         :ok <- validate_manifest(manifest),
         :ok <- verify_binding(manifest, workload, dataset_manifest),
         {:ok, entries} <- load_entries(reference_dir, workload.queries) do
      {:ok,
       %{
         engine: manifest["engine"],
         engine_version: manifest["engine_version"],
         dataset_sha256: manifest["dataset_sha256"],
         workload_sha256: manifest["workload_sha256"],
         entries: entries,
         root: Path.expand(reference_dir)
       }}
    else
      {:ok, _other} -> {:error, :invalid_reference_manifest}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_reference_manifest}
    end
  end

  @doc "Validates reference result files and writes their binding manifest."
  @spec write_manifest(Path.t(), Workload.t(), map(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def write_manifest(reference_dir, workload, dataset_manifest, engine, engine_version)
      when is_binary(reference_dir) and is_map(workload) and is_map(dataset_manifest) and
             is_binary(engine) and is_binary(engine_version) do
    manifest = %{
      schema_version: 1,
      engine: engine,
      engine_version: engine_version,
      dataset_sha256: dataset_manifest.dataset_sha256,
      workload_sha256: workload.fingerprint
    }

    with :ok <- validate_manifest(stringify_keys(manifest)),
         {:ok, _entries} <- load_entries(reference_dir, workload.queries),
         :ok <- File.mkdir_p(reference_dir),
         :ok <-
           File.write(
             Path.join(reference_dir, "manifest.json"),
             Jason.encode!(manifest, pretty: true)
           ) do
      {:ok, manifest}
    end
  end

  @doc "Parses one W3C SPARQL Results JSON document into a normalized answer."
  @spec parse_sparql_json(String.t()) :: {:ok, map()} | {:error, term()}
  def parse_sparql_json(json) when is_binary(json) do
    with {:ok, document} when is_map(document) <- Jason.decode(json),
         {:ok, result} <- decode_result(document),
         {:ok, answer} <- AnswerNormalizer.normalize(result, ordering: :unordered) do
      {:ok, answer}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_sparql_results_json}
    end
  end

  defp load_entries(reference_dir, queries) do
    Enum.reduce_while(queries, {:ok, %{}}, fn query, {:ok, entries} ->
      path = Path.join(reference_dir, query.id <> ".srj")

      case load_entry(path, query.id) do
        {:ok, answer} -> {:cont, {:ok, Map.put(entries, query.id, answer)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp load_entry(path, query_id) do
    case File.read(path) do
      {:ok, json} -> parse_entry(json, query_id)
      {:error, :enoent} -> {:error, {:missing_reference_answer, query_id}}
      {:error, reason} -> {:error, {:reference_answer_read_failed, query_id, reason}}
    end
  end

  defp parse_entry(json, query_id) do
    case parse_sparql_json(json) do
      {:ok, answer} -> {:ok, answer}
      {:error, reason} -> {:error, {:invalid_reference_answer, query_id, reason}}
    end
  end

  defp validate_manifest(manifest) do
    cond do
      manifest["schema_version"] != 1 ->
        {:error, :invalid_reference_schema_version}

      not non_empty_string?(manifest["engine"]) ->
        {:error, :missing_reference_engine}

      not non_empty_string?(manifest["engine_version"]) ->
        {:error, :missing_reference_engine_version}

      not sha256?(manifest["dataset_sha256"]) ->
        {:error, :invalid_reference_dataset_sha256}

      not sha256?(manifest["workload_sha256"]) ->
        {:error, :invalid_reference_workload_sha256}

      true ->
        :ok
    end
  end

  defp verify_binding(manifest, workload, dataset_manifest) do
    cond do
      manifest["dataset_sha256"] != dataset_manifest.dataset_sha256 ->
        {:error,
         {:dataset_fingerprint_mismatch, dataset_manifest.dataset_sha256,
          manifest["dataset_sha256"]}}

      manifest["workload_sha256"] != workload.fingerprint ->
        {:error,
         {:workload_fingerprint_mismatch, workload.fingerprint, manifest["workload_sha256"]}}

      true ->
        :ok
    end
  end

  defp decode_result(%{"boolean" => boolean}) when is_boolean(boolean), do: {:ok, boolean}

  defp decode_result(%{"results" => %{"bindings" => bindings}}) when is_list(bindings) do
    bindings
    |> Enum.reduce_while({:ok, []}, fn binding, {:ok, rows} ->
      case decode_binding(binding) do
        {:ok, row} -> {:cont, {:ok, [row | rows]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp decode_result(_document), do: {:error, :invalid_sparql_results_json}

  defp decode_binding(binding) when is_map(binding) do
    Enum.reduce_while(binding, {:ok, %{}}, fn {variable, encoded}, {:ok, row} ->
      case decode_term(encoded) do
        {:ok, term} -> {:cont, {:ok, Map.put(row, variable, term)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp decode_binding(_binding), do: {:error, :invalid_binding}

  defp decode_term(%{"type" => "uri", "value" => value}) when is_binary(value),
    do: {:ok, {:named_node, value}}

  defp decode_term(%{"type" => "bnode", "value" => value}) when is_binary(value),
    do: {:ok, {:blank_node, value}}

  defp decode_term(%{"type" => "literal", "value" => value, "xml:lang" => lang})
       when is_binary(value) and is_binary(lang),
       do: {:ok, {:literal, :lang, value, lang}}

  defp decode_term(%{"type" => "literal", "value" => value, "datatype" => datatype})
       when is_binary(value) and is_binary(datatype),
       do: {:ok, {:literal, :typed, value, datatype}}

  defp decode_term(%{"type" => "typed-literal", "value" => value, "datatype" => datatype})
       when is_binary(value) and is_binary(datatype),
       do: {:ok, {:literal, :typed, value, datatype}}

  defp decode_term(%{"type" => "literal", "value" => value}) when is_binary(value),
    do: {:ok, {:literal, :simple, value}}

  defp decode_term(_encoded), do: {:error, :unsupported_reference_term}

  defp non_empty_string?(value), do: is_binary(value) and value != ""
  defp sha256?("sha256:" <> digest), do: byte_size(digest) == 64
  defp sha256?(_value), do: false

  defp stringify_keys(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end
end
