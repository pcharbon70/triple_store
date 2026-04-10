defmodule TripleStore.Benchmark.Wikidata.Normalizer do
  @moduledoc """
  Shared normalization helpers for Wikidata benchmark queries.

  The phase 2 benchmark corpus needs a light-weight normalization layer so
  imported workloads can be made reproducible without mutating the original
  source text by hand for every benchmark tier or execution variant.
  """

  @type limit_policy :: :preserve | {:default, pos_integer()} | {:cap, pos_integer()}
  @type rewrite ::
          :strip_blazegraph_hints
          | {:replace, Regex.t(), String.t()}
          | {:rewrite_label_service, [label_binding()]}
  @type label_binding :: %{subject: String.t(), label: String.t(), language: String.t()}

  @doc """
  Applies the configured normalization rewrites and limit policy.
  """
  @spec normalize(String.t(), keyword()) :: String.t()
  def normalize(sparql, opts \\ []) when is_binary(sparql) and is_list(opts) do
    sparql
    |> apply_rewrites(Keyword.get(opts, :rewrites, []))
    |> apply_limit_policy(Keyword.get(opts, :limit_policy, :preserve))
    |> compact_blank_lines()
    |> String.trim()
  end

  @doc """
  Applies a benchmark execution variant to a normalized SELECT query.
  """
  @spec apply_execution_variant(String.t(), atom()) :: String.t()
  def apply_execution_variant(sparql, :raw), do: sparql

  def apply_execution_variant(sparql, :count_only) when is_binary(sparql) do
    sparql
    |> strip_terminal_modifiers()
    |> then(fn stripped ->
      Regex.replace(
        ~r/\bSELECT\b\s+(?:DISTINCT\s+)?(?:REDUCED\s+)?(.+?)\bWHERE\b/si,
        stripped,
        "SELECT (COUNT(*) AS ?count) WHERE",
        global: false
      )
    end)
    |> String.trim()
  end

  def apply_execution_variant(sparql, :distinct_only) when is_binary(sparql) do
    Regex.replace(~r/\bSELECT\b\s+(?!DISTINCT\b)/i, sparql, "SELECT DISTINCT ", global: false)
  end

  @doc """
  Applies a LIMIT policy without changing the rest of the query text.
  """
  @spec apply_limit_policy(String.t(), limit_policy()) :: String.t()
  def apply_limit_policy(sparql, :preserve), do: sparql

  def apply_limit_policy(sparql, {:default, limit}) when is_integer(limit) and limit > 0 do
    case Regex.run(~r/\bLIMIT\s+\d+\b/i, sparql) do
      nil -> "#{String.trim_trailing(sparql)}\nLIMIT #{limit}"
      _ -> sparql
    end
  end

  def apply_limit_policy(sparql, {:cap, limit}) when is_integer(limit) and limit > 0 do
    case Regex.run(~r/\bLIMIT\s+(\d+)\b/i, sparql, capture: :all_but_first) do
      [existing] ->
        if String.to_integer(existing) > limit do
          Regex.replace(~r/\bLIMIT\s+\d+\b/i, sparql, "LIMIT #{limit}", global: false)
        else
          sparql
        end

      nil ->
        "#{String.trim_trailing(sparql)}\nLIMIT #{limit}"
    end
  end

  @doc """
  Applies a list of normalization rewrites to the query text.
  """
  @spec apply_rewrites(String.t(), [rewrite()]) :: String.t()
  def apply_rewrites(sparql, rewrites) when is_binary(sparql) and is_list(rewrites) do
    Enum.reduce(rewrites, sparql, &apply_rewrite/2)
  end

  defp apply_rewrite(:strip_blazegraph_hints, sparql) do
    sparql
    |> String.split("\n")
    |> Enum.reject(fn line ->
      trimmed = String.trim(line)
      String.starts_with?(trimmed, "PREFIX hint:") or String.contains?(trimmed, "hint:Query")
    end)
    |> Enum.join("\n")
  end

  defp apply_rewrite({:replace, pattern, replacement}, sparql) when is_struct(pattern, Regex) do
    Regex.replace(pattern, sparql, replacement)
  end

  defp apply_rewrite({:rewrite_label_service, bindings}, sparql) when is_list(bindings) do
    sparql =
      Regex.replace(
        ~r/SERVICE\s+wikibase:label\s*\{.*?\}/si,
        sparql,
        build_label_service_rewrite(bindings)
      )

    sparql
    |> maybe_add_rdfs_prefix(bindings)
  end

  defp apply_rewrite(_rewrite, sparql), do: sparql

  defp build_label_service_rewrite(bindings) do
    bindings
    |> Enum.map(fn %{subject: subject, label: label, language: language} ->
      """
      OPTIONAL {
        #{subject} rdfs:label #{label} .
        FILTER(LANG(#{label}) = "#{language}")
      }
      """
      |> String.trim()
    end)
    |> Enum.join("\n")
  end

  defp maybe_add_rdfs_prefix(sparql, []), do: sparql

  defp maybe_add_rdfs_prefix(sparql, _bindings) do
    if String.contains?(sparql, "PREFIX rdfs:") do
      sparql
    else
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n#{sparql}"
    end
  end

  defp strip_terminal_modifiers(sparql) do
    Regex.replace(~r/\n?(ORDER BY|LIMIT|OFFSET)\b.*$/si, sparql, "")
  end

  defp compact_blank_lines(sparql) do
    Regex.replace(~r/\n{3,}/, sparql, "\n\n")
  end
end
