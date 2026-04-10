defmodule TripleStore.Benchmark.Wikidata.AnswerNormalizer do
  @moduledoc """
  Canonical answer normalization for Wikidata benchmark comparisons.

  The normalizer turns raw SPARQL query results into stable, hashable records
  that can be compared across repeated runs and reference baselines. It
  explicitly models:

  - ordering-sensitive versus unordered result sets
  - blank-node handling policies
  - count-only and distinct-only execution variants
  - datatype-relaxed comparison views for divergence classification
  """

  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_int "http://www.w3.org/2001/XMLSchema#int"
  @xsd_long "http://www.w3.org/2001/XMLSchema#long"
  @xsd_short "http://www.w3.org/2001/XMLSchema#short"
  @xsd_byte "http://www.w3.org/2001/XMLSchema#byte"
  @xsd_non_negative_integer "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
  @xsd_non_positive_integer "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
  @xsd_positive_integer "http://www.w3.org/2001/XMLSchema#positiveInteger"
  @xsd_negative_integer "http://www.w3.org/2001/XMLSchema#negativeInteger"
  @xsd_unsigned_int "http://www.w3.org/2001/XMLSchema#unsignedInt"
  @xsd_unsigned_long "http://www.w3.org/2001/XMLSchema#unsignedLong"
  @xsd_unsigned_short "http://www.w3.org/2001/XMLSchema#unsignedShort"
  @xsd_unsigned_byte "http://www.w3.org/2001/XMLSchema#unsignedByte"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"
  @xsd_double "http://www.w3.org/2001/XMLSchema#double"
  @xsd_float "http://www.w3.org/2001/XMLSchema#float"
  @xsd_string "http://www.w3.org/2001/XMLSchema#string"

  @integer_types [
    @xsd_integer,
    @xsd_int,
    @xsd_long,
    @xsd_short,
    @xsd_byte,
    @xsd_non_negative_integer,
    @xsd_non_positive_integer,
    @xsd_positive_integer,
    @xsd_negative_integer,
    @xsd_unsigned_int,
    @xsd_unsigned_long,
    @xsd_unsigned_short,
    @xsd_unsigned_byte
  ]

  @float_types [@xsd_decimal, @xsd_double, @xsd_float]

  @type execution_variant :: :raw | :count_only | :distinct_only
  @type ordering_policy :: :ordered | :unordered
  @type blank_node_policy :: :preserve | :anonymous
  @type result_kind :: :bindings | :boolean | :scalar

  @type normalized_answer :: %{
          schema_version: pos_integer(),
          result_kind: result_kind(),
          execution_variant: execution_variant(),
          ordering: ordering_policy(),
          blank_node_policy: blank_node_policy(),
          row_count: non_neg_integer(),
          distinct_row_count: non_neg_integer(),
          normalized_rows: [String.t()],
          fingerprint: String.t(),
          unordered_fingerprint: String.t(),
          distinct_fingerprint: String.t(),
          datatype_relaxed_fingerprint: String.t(),
          anonymous_blank_node_fingerprint: String.t()
        }

  @type normalize_opts :: [
          execution_variant: execution_variant(),
          ordering: ordering_policy(),
          blank_node_policy: blank_node_policy()
        ]

  @doc """
  Normalizes a query result into a stable, serializable answer record.
  """
  @spec normalize(term(), normalize_opts()) :: {:ok, normalized_answer()} | {:error, term()}
  def normalize(result, opts \\ []) do
    execution_variant = Keyword.get(opts, :execution_variant, :raw)
    ordering = Keyword.get(opts, :ordering, :unordered)
    blank_node_policy = Keyword.get(opts, :blank_node_policy, :anonymous)

    with :ok <- validate_execution_variant(execution_variant),
         :ok <- validate_ordering(ordering),
         :ok <- validate_blank_node_policy(blank_node_policy) do
      {:ok,
       build_answer_record(
         result,
         execution_variant: execution_variant,
         ordering: ordering,
         blank_node_policy: blank_node_policy
       )}
    end
  end

  @doc """
  Returns the canonical hash for a normalized answer.
  """
  @spec fingerprint(normalized_answer()) :: String.t()
  def fingerprint(%{fingerprint: fingerprint}), do: fingerprint

  defp build_answer_record(result, opts) when is_list(result) do
    execution_variant = Keyword.fetch!(opts, :execution_variant)
    ordering = Keyword.fetch!(opts, :ordering)
    blank_node_policy = Keyword.fetch!(opts, :blank_node_policy)

    base_rows =
      Enum.map(
        result,
        &normalize_binding(&1, blank_node_policy: blank_node_policy, relax_datatypes: false)
      )

    normalized_rows =
      base_rows
      |> apply_execution_variant(execution_variant)
      |> apply_ordering(ordering)

    unordered_rows =
      base_rows
      |> apply_execution_variant(execution_variant)
      |> Enum.sort()

    distinct_rows =
      base_rows
      |> Enum.uniq()
      |> Enum.sort()

    datatype_relaxed_rows =
      result
      |> Enum.map(
        &normalize_binding(&1, blank_node_policy: blank_node_policy, relax_datatypes: true)
      )
      |> apply_execution_variant(execution_variant)
      |> Enum.sort()

    anonymous_blank_node_rows =
      result
      |> Enum.map(&normalize_binding(&1, blank_node_policy: :anonymous, relax_datatypes: false))
      |> apply_execution_variant(execution_variant)
      |> Enum.sort()

    %{
      schema_version: 1,
      result_kind: :bindings,
      execution_variant: execution_variant,
      ordering: ordering,
      blank_node_policy: blank_node_policy,
      row_count: length(normalized_rows),
      distinct_row_count: length(Enum.uniq(unordered_rows)),
      normalized_rows: normalized_rows,
      fingerprint: hash_rows(:bindings, normalized_rows),
      unordered_fingerprint: hash_rows(:bindings, unordered_rows),
      distinct_fingerprint: hash_rows(:bindings, distinct_rows),
      datatype_relaxed_fingerprint: hash_rows(:bindings, datatype_relaxed_rows),
      anonymous_blank_node_fingerprint: hash_rows(:bindings, anonymous_blank_node_rows)
    }
  end

  defp build_answer_record(result, opts) when is_boolean(result) do
    execution_variant = Keyword.fetch!(opts, :execution_variant)
    blank_node_policy = Keyword.fetch!(opts, :blank_node_policy)
    normalized_rows = [if(result, do: "true", else: "false")]

    %{
      schema_version: 1,
      result_kind: :boolean,
      execution_variant: execution_variant,
      ordering: :ordered,
      blank_node_policy: blank_node_policy,
      row_count: 1,
      distinct_row_count: 1,
      normalized_rows: normalized_rows,
      fingerprint: hash_rows(:boolean, normalized_rows),
      unordered_fingerprint: hash_rows(:boolean, normalized_rows),
      distinct_fingerprint: hash_rows(:boolean, normalized_rows),
      datatype_relaxed_fingerprint: hash_rows(:boolean, normalized_rows),
      anonymous_blank_node_fingerprint: hash_rows(:boolean, normalized_rows)
    }
  end

  defp build_answer_record(result, opts) do
    execution_variant = Keyword.fetch!(opts, :execution_variant)
    blank_node_policy = Keyword.fetch!(opts, :blank_node_policy)
    normalized_rows = [normalize_scalar(result)]

    %{
      schema_version: 1,
      result_kind: :scalar,
      execution_variant: execution_variant,
      ordering: :ordered,
      blank_node_policy: blank_node_policy,
      row_count: 1,
      distinct_row_count: 1,
      normalized_rows: normalized_rows,
      fingerprint: hash_rows(:scalar, normalized_rows),
      unordered_fingerprint: hash_rows(:scalar, normalized_rows),
      distinct_fingerprint: hash_rows(:scalar, normalized_rows),
      datatype_relaxed_fingerprint: hash_rows(:scalar, normalized_rows),
      anonymous_blank_node_fingerprint: hash_rows(:scalar, normalized_rows)
    }
  end

  defp apply_execution_variant(rows, :distinct_only), do: Enum.uniq(rows)
  defp apply_execution_variant(rows, _variant), do: rows

  defp apply_ordering(rows, :ordered), do: rows
  defp apply_ordering(rows, :unordered), do: Enum.sort(rows)

  defp normalize_binding(binding, opts) when is_map(binding) do
    binding
    |> Enum.sort_by(fn {var, _value} -> to_string(var) end)
    |> Enum.map(fn {var, value} ->
      [to_string(var), normalize_term(value, opts)]
    end)
    |> Jason.encode!()
  end

  defp normalize_binding(other, opts) do
    Jason.encode!([["value", normalize_term(other, opts)]])
  end

  defp normalize_term({:named_node, iri}, _opts), do: Jason.encode!(["iri", iri])

  defp normalize_term({:blank_node, id}, opts) do
    if Keyword.get(opts, :blank_node_policy) == :anonymous do
      Jason.encode!(["blank_node", "_:blank"])
    else
      Jason.encode!(["blank_node", id])
    end
  end

  defp normalize_term({:literal, :simple, value}, _opts),
    do: Jason.encode!(["literal", "simple", value])

  defp normalize_term({:literal, :lang, value, lang}, _opts),
    do: Jason.encode!(["literal", "lang", value, lang])

  defp normalize_term({:literal, :typed, value, datatype}, opts) do
    literal_tag = if opts[:relax_datatypes], do: "relaxed", else: "typed"

    Jason.encode!([
      "literal",
      literal_tag,
      canonicalize_literal(value, datatype, opts),
      maybe_datatype(datatype, opts)
    ])
  end

  defp normalize_term({:literal, {:typed, datatype}, value}, opts) do
    normalize_term({:literal, :typed, value, datatype}, opts)
  end

  defp normalize_term({:literal, {:lang, lang}, value}, _opts),
    do: Jason.encode!(["literal", "lang", value, lang])

  defp normalize_term(other, _opts), do: Jason.encode!(["term", inspect(other)])

  defp normalize_scalar(value) do
    value
    |> inspect()
    |> then(&Jason.encode!(["scalar", &1]))
  end

  defp canonicalize_literal(value, datatype, opts) do
    cond do
      opts[:relax_datatypes] and datatype in @integer_types ->
        canonicalize_integer(value)

      opts[:relax_datatypes] and datatype in @float_types ->
        canonicalize_float(value)

      opts[:relax_datatypes] and datatype == @xsd_boolean ->
        canonicalize_boolean(value)

      datatype == @xsd_string ->
        value

      true ->
        value
    end
  end

  defp maybe_datatype(datatype, opts) do
    if Keyword.get(opts, :relax_datatypes) do
      "relaxed"
    else
      datatype
    end
  end

  defp canonicalize_integer(value) when is_binary(value) do
    normalized = String.trim(value)

    case Integer.parse(normalized) do
      {int, ""} -> Integer.to_string(int)
      _ -> normalized
    end
  end

  defp canonicalize_integer(value), do: to_string(value)

  defp canonicalize_float(value) when is_binary(value) do
    normalized = String.trim(value)

    case Float.parse(normalized) do
      {float, ""} -> :erlang.float_to_binary(float, [:short])
      _ -> normalized
    end
  end

  defp canonicalize_float(value), do: to_string(value)

  defp canonicalize_boolean(value) when is_binary(value) do
    case String.downcase(String.trim(value)) do
      "1" -> "true"
      "true" -> "true"
      "0" -> "false"
      "false" -> "false"
      other -> other
    end
  end

  defp canonicalize_boolean(value), do: to_string(value)

  defp hash_rows(kind, rows) do
    rows
    |> then(&Jason.encode!([kind, &1]))
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp validate_execution_variant(variant) when variant in [:raw, :count_only, :distinct_only],
    do: :ok

  defp validate_execution_variant(_variant), do: {:error, :invalid_execution_variant}

  defp validate_ordering(:ordered), do: :ok
  defp validate_ordering(:unordered), do: :ok
  defp validate_ordering(_ordering), do: {:error, :invalid_ordering_policy}

  defp validate_blank_node_policy(:preserve), do: :ok
  defp validate_blank_node_policy(:anonymous), do: :ok
  defp validate_blank_node_policy(_policy), do: {:error, :invalid_blank_node_policy}
end
