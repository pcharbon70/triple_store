defmodule TripleStore.Benchmark.WatDiv.Workload do
  @moduledoc """
  Loads query instances produced by the official WatDiv query generator.

  The upstream generator writes instantiated `SELECT` queries consecutively to
  standard output. `split_file/3` converts that stream into one query per file;
  `load_dir/1` then builds a stable, content-addressed workload for execution.
  Unresolved `%variable%` placeholders are rejected because timing template
  text is not a valid WatDiv workload.
  """

  @query_extensions [".rq", ".sparql"]
  @placeholder ~r/%[A-Za-z][A-Za-z0-9_]*%/
  @select_boundary ~r/(?im)(?=^\s*SELECT\b)/

  @type query :: %{id: String.t(), path: Path.t(), sparql: String.t()}
  @type t :: %{
          root: Path.t(),
          queries: [query()],
          query_count: non_neg_integer(),
          fingerprint: String.t()
        }

  @doc "Splits concatenated output from the official WatDiv query generator."
  @spec split_queries(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def split_queries(content) when is_binary(content) do
    queries =
      content
      |> then(&Regex.split(@select_boundary, &1, trim: true))
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    cond do
      queries == [] ->
        {:error, :no_queries}

      Enum.any?(queries, &(not Regex.match?(~r/^SELECT\b/i, &1))) ->
        {:error, :unexpected_generator_output}

      true ->
        {:ok, Enum.map(queries, &ensure_trailing_newline/1)}
    end
  end

  @doc "Splits one upstream output file into stable one-query `.rq` files."
  @spec split_file(Path.t(), Path.t(), String.t()) :: {:ok, [Path.t()]} | {:error, term()}
  def split_file(input_path, output_dir, template_id)
      when is_binary(input_path) and is_binary(output_dir) and is_binary(template_id) do
    with :ok <- validate_template_id(template_id),
         {:ok, content} <- File.read(input_path),
         {:ok, queries} <- split_queries(content),
         :ok <- File.mkdir_p(output_dir) do
      paths =
        queries
        |> Enum.with_index(1)
        |> Enum.map(fn {query, index} ->
          path = Path.join(output_dir, "#{template_id}-#{pad_index(index)}.rq")
          File.write!(path, query)
          path
        end)

      {:ok, paths}
    end
  end

  @doc "Loads a directory containing exactly one instantiated query per file."
  @spec load_dir(Path.t()) :: {:ok, t()} | {:error, term()}
  def load_dir(root) when is_binary(root) do
    root = Path.expand(root)

    with :ok <- ensure_directory(root),
         paths when paths != [] <- query_paths(root),
         {:ok, queries} <- load_queries(paths, root) do
      {:ok,
       %{
         root: root,
         queries: queries,
         query_count: length(queries),
         fingerprint: fingerprint(queries)
       }}
    else
      [] -> {:error, :empty_workload}
      {:error, _reason} = error -> error
    end
  end

  @doc "Computes the stable SHA-256 fingerprint for normalized query files."
  @spec fingerprint([query()]) :: String.t()
  def fingerprint(queries) when is_list(queries) do
    digest =
      queries
      |> Enum.map_join("\n", fn query -> query.id <> "\0" <> query.sparql end)
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    "sha256:#{digest}"
  end

  defp load_queries(paths, root) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, queries} ->
      id = query_id(path, root)

      case load_query(path, id) do
        {:ok, query} -> {:cont, {:ok, [query | queries]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, queries} -> {:ok, Enum.reverse(queries)}
      error -> error
    end
  end

  defp load_query(path, id) do
    with {:ok, content} <- File.read(path),
         :ok <- ensure_no_placeholders(content, id),
         {:ok, queries} <- split_queries(content),
         {:ok, sparql} <- exactly_one_query(queries, id) do
      {:ok, %{id: id, path: path, sparql: sparql}}
    end
  end

  defp ensure_no_placeholders(content, id) do
    if Regex.match?(@placeholder, content),
      do: {:error, {:unresolved_placeholder, id}},
      else: :ok
  end

  defp exactly_one_query([sparql], _id), do: {:ok, sparql}

  defp exactly_one_query(queries, id),
    do: {:error, {:query_file_count, id, length(queries)}}

  defp query_paths(root) do
    root
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&(File.regular?(&1) and Path.extname(&1) in @query_extensions))
    |> Enum.sort()
  end

  defp query_id(path, root) do
    path
    |> Path.relative_to(root)
    |> Path.rootname()
    |> String.replace("\\", "/")
  end

  defp ensure_directory(path) do
    if File.dir?(path), do: :ok, else: {:error, :workload_directory_not_found}
  end

  defp validate_template_id(template_id) do
    if Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/, template_id),
      do: :ok,
      else: {:error, :invalid_template_id}
  end

  defp ensure_trailing_newline(query), do: String.trim(query) <> "\n"
  defp pad_index(index), do: index |> Integer.to_string() |> String.pad_leading(4, "0")
end
