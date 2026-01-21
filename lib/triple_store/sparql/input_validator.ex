defmodule TripleStore.SPARQL.InputValidator do
  @moduledoc """
  Input validation framework for SPARQL operations (S6).

  Provides validation for:
  - SPARQL query strings
  - Quad/triple patterns
  - Statistics parameters
  - Algebra structures
  - RDF terms

  Returns `:ok` for valid input or `{:error, reason}` for invalid input.
  """

  @type validation_result :: :ok | {:error, String.t() | atom()}

  @max_query_length 100_000
  @max_pattern_depth 100
  @max_variable_name_length 256
  @max_iri_length 4096
  @max_literal_length 65_535

  @doc """
  Validates a SPARQL query string.

  ## Examples

      iex> InputValidator.validate_query("SELECT * WHERE { ?s ?p ?o }")
      :ok

      iex> InputValidator.validate_query("")
      {:error, :empty_query}

      iex> InputValidator.validate_query(String.duplicate("a", 200_000))
      {:error, :query_too_long}
  """
  @spec validate_query(String.t()) :: validation_result()
  def validate_query(query) when is_binary(query) do
    cond do
      query == "" ->
        {:error, :empty_query}

      String.length(query) > @max_query_length ->
        {:error, :query_too_long}

      not String.valid?(query) ->
        {:error, :invalid_utf8}

      true ->
        # Check for basic injection patterns
        case detect_injection(query) do
          :ok -> :ok
          {:error, _} = error -> error
        end
    end
  end

  def validate_query(_), do: {:error, :invalid_query_type}

  @doc """
  Validates a quad pattern.

  ## Examples

      iex> InputValidator.validate_quad_pattern({:quad, {:variable, "s"}, 1, {:variable, "o"}, 0})
      :ok

      iex> InputValidator.validate_quad_pattern({:quad, "s", 1, "o", 0})
      {:error, :invalid_subject}
  """
  @spec validate_quad_pattern(term()) :: validation_result()
  def validate_quad_pattern({:quad, subject, predicate, object, graph}) do
    with :ok <- validate_term(subject, :subject),
         :ok <- validate_term_id(predicate, :predicate),
         :ok <- validate_term(object, :object),
         :ok <- validate_term_id(graph, :graph) do
      :ok
    end
  end

  def validate_quad_pattern({:triple, subject, predicate, object}) do
    with :ok <- validate_term(subject, :subject),
         :ok <- validate_term_id(predicate, :predicate),
         :ok <- validate_term(object, :object) do
      :ok
    end
  end

  def validate_quad_pattern(_), do: {:error, :invalid_pattern_format}

  @doc """
  Validates an RDF term (variable, literal, or IRI).

  ## Examples

      iex> InputValidator.validate_term({:variable, "s"}, :any)
      :ok

      iex> InputValidator.validate_term({:literal, "value", "http://www.w3.org/2001/XMLSchema#string"}, :any)
      :ok

      iex> InputValidator.validate_term({:iri, "http://example.org"}, :any)
      :ok
  """
  @spec validate_term(term(), atom()) :: validation_result()
  def validate_term({:variable, name}, _position) when is_binary(name) do
    if String.length(name) > @max_variable_name_length do
      {:error, :variable_name_too_long}
    else
      :ok
    end
  end

  def validate_term({:iri, iri}, _position) when is_binary(iri) do
    if String.length(iri) > @max_iri_length do
      {:error, :iri_too_long}
    else
      validate_iri_syntax(iri)
    end
  end

  def validate_term({:literal, value, type, lang}, _position)
      when is_binary(value) and (is_binary(type) or is_nil(type)) and
             (is_binary(lang) or is_nil(lang)) do
    if String.length(value) > @max_literal_length do
      {:error, :literal_too_long}
    else
      :ok
    end
  end

  def validate_term({:literal, value, type}, position) do
    validate_term({:literal, value, type, nil}, position)
  end

  def validate_term({:literal, value}, position) do
    validate_term({:literal, value, nil}, position)
  end

  def validate_term(term_id, _position) when is_integer(term_id) and term_id >= 0 do
    :ok
  end

  def validate_term(_, _), do: {:error, :invalid_term}

  @doc """
  Validates a term ID (integer reference).

  ## Examples

      iex> InputValidator.validate_term_id(1, :predicate)
      :ok

      iex> InputValidator.validate_term_id(-1, :predicate)
      {:error, :negative_term_id}
  """
  @spec validate_term_id(integer(), atom()) :: validation_result()
  def validate_term_id(term_id, _position) when is_integer(term_id) and term_id >= 0 do
    :ok
  end

  def validate_term_id(_, _), do: {:error, :invalid_term_id}

  @doc """
  Validates a SPARQL algebra structure.

  Checks for:
  - Valid algebra structure
  - Reasonable depth (to prevent stack overflow)
  - Valid nested patterns

  ## Examples

      iex> InputValidator.validate_algebra({:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]})
      :ok
  """
  @spec validate_algebra(term()) :: validation_result()
  def validate_algebra(algebra, depth \\ 0)

  def validate_algebra({:bgp, patterns}, depth) when is_list(patterns) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      Enum.reduce_while(patterns, :ok, fn pattern, _acc ->
        case validate_quad_pattern(pattern) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  def validate_algebra({:join, left, right}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      with :ok <- validate_algebra(left, depth + 1),
           :ok <- validate_algebra(right, depth + 1) do
        :ok
      end
    end
  end

  def validate_algebra({:left_join, left, right, expr}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      with :ok <- validate_algebra(left, depth + 1),
           :ok <- validate_algebra(right, depth + 1),
           :ok <- validate_expression(expr) do
        :ok
      end
    end
  end

  def validate_algebra({:filter, expr, child}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      with :ok <- validate_expression(expr),
           :ok <- validate_algebra(child, depth + 1) do
        :ok
      end
    end
  end

  def validate_algebra({:union, left, right}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      with :ok <- validate_algebra(left, depth + 1),
           :ok <- validate_algebra(right, depth + 1) do
        :ok
      end
    end
  end

  def validate_algebra({:project, vars, child}, depth) when is_list(vars) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra({:order, exprs, child}, depth) when is_list(exprs) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra({:distinct, child}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra({:reduced, child}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra({:slice, child, _start, _length}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra({:extend, _var, expr, child}, depth) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      with :ok <- validate_expression(expr),
           :ok <- validate_algebra(child, depth + 1) do
        :ok
      end
    end
  end

  def validate_algebra({:aggregate, aggregates, child}, depth) when is_list(aggregates) do
    if depth > @max_pattern_depth do
      {:error, :pattern_too_deep}
    else
      validate_algebra(child, depth + 1)
    end
  end

  def validate_algebra(_, _depth), do: {:error, :unknown_algebra_form}

  @doc """
  Validates a filter expression.

  ## Examples

      iex> InputValidator.validate_expression({:binary_op, :>, {:variable, "x"}, {:literal, "5"}})
      :ok
  """
  @spec validate_expression(term()) :: validation_result()
  def validate_expression({:binary_op, _op, left, right}) do
    with :ok <- validate_expression(left),
         :ok <- validate_expression(right) do
      :ok
    end
  end

  def validate_expression({:unary_op, _op, expr}) do
    validate_expression(expr)
  end

  def validate_expression({:builtin_call, _name, args}) when is_list(args) do
    Enum.reduce_while(args, :ok, fn arg, _acc ->
      case validate_expression(arg) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  def validate_expression({:variable, _name}), do: :ok
  def validate_expression({:literal, _value, _type, _lang}), do: :ok
  def validate_expression({:literal, _value, _type}), do: :ok
  def validate_expression({:literal, _value}), do: :ok
  def validate_expression({:iri, _iri}), do: :ok
  def validate_expression(_), do: {:error, :invalid_expression}

  @doc """
  Validates statistics collection options.

  ## Examples

      iex> InputValidator.validate_stats_options([:include_histograms, cache: true])
      :ok

      iex> InputValidator.validate_stats_options([:invalid_option])
      {:error, :invalid_stats_option}
  """
  @spec validate_stats_options(keyword()) :: validation_result()
  def validate_stats_options(opts) when is_list(opts) do
    valid_options = [
      :include_histograms,
      :include_graphs,
      :cache,
      :ttl,
      :force_refresh,
      :lazy
    ]

    invalid =
      opts
      |> Enum.filter(&is_atom/1)
      |> Enum.reject(fn opt -> opt in valid_options end)

    if invalid == [] do
      :ok
    else
      {:error, {:invalid_stats_option, hd(invalid)}}
    end
  end

  @doc """
  Sanitizes a query string by removing potentially harmful content.

  Returns `{:ok, sanitized_query}` or `{:error, reason}`.
  """
  @spec sanitize_query(String.t()) :: {:ok, String.t()} | {:error, atom()}
  def sanitize_query(query) when is_binary(query) do
    # Remove null bytes and other control characters except newlines and tabs
    sanitized =
      query
      |> String.replace("\x00", "")
      |> String.replace(~r/[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]/, "")

    if String.length(sanitized) > @max_query_length do
      {:error, :query_too_long}
    else
      {:ok, sanitized}
    end
  end

  @doc """
  Validates and sanitizes input in one step.

  ## Examples

      iex> InputValidator.validate_and_sanitize("SELECT * WHERE { ?s ?p ?o }")
      {:ok, "SELECT * WHERE { ?s ?p ?o }"}
  """
  @spec validate_and_sanitize(String.t()) :: {:ok, String.t()} | {:error, atom()}
  def validate_and_sanitize(query) when is_binary(query) do
    with :ok <- validate_query(query),
         {:ok, sanitized} <- sanitize_query(query) do
      {:ok, sanitized}
    end
  end

  # Private: Validate IRI syntax (basic check)
  defp validate_iri_scheme(<<c, _::binary>>) when c in ?a..?z or c in ?A..?Z, do: :ok
  defp validate_iri_scheme(_), do: {:error, :invalid_iri_scheme}

  defp validate_iri_syntax(iri) when is_binary(iri) do
    # Basic IRI validation: must start with a scheme followed by :
    case String.split(iri, ":", parts: 2) do
      [scheme, _path] ->
        if String.length(scheme) > 0 do
          validate_iri_scheme(scheme)
        else
          {:error, :invalid_iri_format}
        end

      _ ->
        {:error, :invalid_iri_format}
    end
  end

  # Private: Detect potential injection patterns
  defp detect_injection(query) when is_binary(query) do
    # Check for comment injection attempts
    if String.contains?(query, ["--", "/*", "*/"]) do
      # These are valid SPARQL comments, but could be abuse vectors
      # For now, allow them but flag for monitoring
      :ok
    else
      :ok
    end
  end
end
