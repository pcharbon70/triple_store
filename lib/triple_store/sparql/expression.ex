defmodule TripleStore.SPARQL.Expression do
  @moduledoc """
  SPARQL Expression evaluation module.

  This module provides functions for evaluating SPARQL expressions from parsed AST.
  Expressions appear in FILTER clauses, BIND expressions, and SELECT projections.

  ## Expression Types

  The module handles the following expression categories:

  - **Arithmetic**: +, -, *, /, unary minus
  - **Comparison**: =, !=, <, >, <=, >=
  - **Logical**: &&, ||, !
  - **Built-in functions**: STR, LANG, DATATYPE, BOUND, IF, COALESCE, etc.
  - **Aggregates**: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE

  ## Evaluation

  Expressions are evaluated against a solution (variable bindings map):

      iex> bindings = %{"x" => {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}}
      iex> expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}}
      iex> Expression.evaluate(expr, bindings)
      {:ok, {:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}}

  ## Error Handling

  Expressions return `:error` for type errors or unbound variables:

      iex> Expression.evaluate({:variable, "unbound"}, %{})
      :error

  """

  # Common datatype IRIs
  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"
  @xsd_float "http://www.w3.org/2001/XMLSchema#float"
  @xsd_double "http://www.w3.org/2001/XMLSchema#double"
  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_string "http://www.w3.org/2001/XMLSchema#string"
  @xsd_date_time "http://www.w3.org/2001/XMLSchema#dateTime"

  # Security limits
  @regex_timeout_ms 1000
  @max_regex_pattern_length 1000

  @type rdf_term ::
          {:variable, String.t()}
          | {:named_node, String.t()}
          | {:blank_node, String.t()}
          | {:literal, :simple, String.t()}
          | {:literal, :lang, String.t(), String.t()}
          | {:literal, :typed, String.t(), String.t()}

  @type expression :: tuple()
  @type bindings :: %{String.t() => rdf_term()}
  @type eval_result :: {:ok, rdf_term()} | :error

  # ===========================================================================
  # Main Evaluation Entry Point
  # ===========================================================================

  @doc """
  Evaluates a SPARQL expression against variable bindings.

  ## Arguments
  - `expr` - The expression AST node
  - `bindings` - Map of variable names to RDF terms

  ## Returns
  - `{:ok, term}` on success
  - `:error` on type errors, unbound variables, or other evaluation failures

  ## Examples

      iex> Expression.evaluate({:variable, "x"}, %{"x" => {:literal, :simple, "hello"}})
      {:ok, {:literal, :simple, "hello"}}

      iex> Expression.evaluate({:add, {:literal, :typed, "1", @xsd_integer}, {:literal, :typed, "2", @xsd_integer}}, %{})
      {:ok, {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}}

  """
  @spec evaluate(expression(), bindings()) :: eval_result()
  def evaluate(expr, bindings \\ %{})

  # ===========================================================================
  # Variables and Literals
  # ===========================================================================

  def evaluate({:variable, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> :error
      value -> {:ok, value}
    end
  end

  def evaluate({:named_node, _} = term, _bindings), do: {:ok, term}
  def evaluate({:blank_node, _} = term, _bindings), do: {:ok, term}
  def evaluate({:literal, _, _} = term, _bindings), do: {:ok, term}
  def evaluate({:literal, _, _, _} = term, _bindings), do: {:ok, term}

  # ===========================================================================
  # Arithmetic Expressions
  # ===========================================================================

  def evaluate({:add, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_num, l_type} <- to_numeric(l),
         {:ok, r_num, r_type} <- to_numeric(r) do
      result_type = promote_numeric_type(l_type, r_type)
      {:ok, make_numeric(l_num + r_num, result_type)}
    else
      _ -> :error
    end
  end

  def evaluate({:subtract, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_num, l_type} <- to_numeric(l),
         {:ok, r_num, r_type} <- to_numeric(r) do
      result_type = promote_numeric_type(l_type, r_type)
      {:ok, make_numeric(l_num - r_num, result_type)}
    else
      _ -> :error
    end
  end

  def evaluate({:multiply, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_num, l_type} <- to_numeric(l),
         {:ok, r_num, r_type} <- to_numeric(r) do
      result_type = promote_numeric_type(l_type, r_type)
      {:ok, make_numeric(l_num * r_num, result_type)}
    else
      _ -> :error
    end
  end

  def evaluate({:divide, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_num, l_type} <- to_numeric(l),
         {:ok, r_num, r_type} <- to_numeric(r) do
      if r_num == 0 do
        :error
      else
        result_type = promote_numeric_type(l_type, r_type)
        # Division always produces decimal/double
        promoted = if result_type == :integer, do: :decimal, else: result_type
        {:ok, make_numeric(l_num / r_num, promoted)}
      end
    else
      _ -> :error
    end
  end

  def evaluate({:unary_minus, expr}, bindings) do
    with {:ok, val} <- evaluate(expr, bindings),
         {:ok, num, type} <- to_numeric(val) do
      {:ok, make_numeric(-num, type)}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Comparison Expressions
  # ===========================================================================

  def evaluate({:equal, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings) do
      {:ok, make_boolean(rdf_equal?(l, r))}
    else
      _ -> :error
    end
  end

  def evaluate({:greater, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, cmp} <- rdf_compare(l, r) do
      {:ok, make_boolean(cmp == :gt)}
    else
      _ -> :error
    end
  end

  def evaluate({:less, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, cmp} <- rdf_compare(l, r) do
      {:ok, make_boolean(cmp == :lt)}
    else
      _ -> :error
    end
  end

  def evaluate({:greater_or_equal, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, cmp} <- rdf_compare(l, r) do
      {:ok, make_boolean(cmp in [:gt, :eq])}
    else
      _ -> :error
    end
  end

  def evaluate({:less_or_equal, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, cmp} <- rdf_compare(l, r) do
      {:ok, make_boolean(cmp in [:lt, :eq])}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Logical Expressions
  # ===========================================================================

  def evaluate({:and, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_bool} <- effective_boolean_value(l),
         {:ok, r_bool} <- effective_boolean_value(r) do
      {:ok, make_boolean(l_bool and r_bool)}
    else
      _ -> :error
    end
  end

  def evaluate({:or, left, right}, bindings) do
    with {:ok, l} <- evaluate(left, bindings),
         {:ok, r} <- evaluate(right, bindings),
         {:ok, l_bool} <- effective_boolean_value(l),
         {:ok, r_bool} <- effective_boolean_value(r) do
      {:ok, make_boolean(l_bool or r_bool)}
    else
      _ -> :error
    end
  end

  def evaluate({:not, expr}, bindings) do
    with {:ok, val} <- evaluate(expr, bindings),
         {:ok, bool} <- effective_boolean_value(val) do
      {:ok, make_boolean(not bool)}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Built-in Functions: Term Accessors
  # ===========================================================================

  def evaluate({:bound, {:variable, name}}, bindings) do
    {:ok, make_boolean(Map.has_key?(bindings, name))}
  end

  def evaluate({:function_call, "STR", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      {:ok, {:literal, :simple, term_to_string(val)}}
    end
  end

  def evaluate({:function_call, "LANG", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      case val do
        {:literal, :lang, _, lang} -> {:ok, {:literal, :simple, lang}}
        {:literal, _, _} -> {:ok, {:literal, :simple, ""}}
        {:literal, _, _, _} -> {:ok, {:literal, :simple, ""}}
        _ -> :error
      end
    end
  end

  def evaluate({:function_call, "DATATYPE", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      case val do
        {:literal, :simple, _} ->
          {:ok, {:named_node, @xsd_string}}

        {:literal, :typed, _, dt} ->
          {:ok, {:named_node, dt}}

        {:literal, :lang, _, _} ->
          {:ok, {:named_node, "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"}}

        _ ->
          :error
      end
    end
  end

  def evaluate({:function_call, "IRI", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      case val do
        {:named_node, _} = iri -> {:ok, iri}
        {:literal, :simple, s} -> {:ok, {:named_node, s}}
        {:literal, :typed, s, @xsd_string} -> {:ok, {:named_node, s}}
        _ -> :error
      end
    end
  end

  def evaluate({:function_call, "URI", args}, bindings) do
    evaluate({:function_call, "IRI", args}, bindings)
  end

  def evaluate({:function_call, "BNODE", []}, _bindings) do
    id = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    {:ok, {:blank_node, "b#{id}"}}
  end

  def evaluate({:function_call, "BNODE", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      case val do
        {:literal, :simple, s} -> {:ok, {:blank_node, "b#{s}"}}
        {:literal, :typed, s, @xsd_string} -> {:ok, {:blank_node, "b#{s}"}}
        _ -> :error
      end
    end
  end

  # ===========================================================================
  # Built-in Functions: Type Checking
  # ===========================================================================

  def evaluate({:function_call, "ISIRI", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      {:ok, make_boolean(match?({:named_node, _}, val))}
    end
  end

  def evaluate({:function_call, "ISURI", args}, bindings) do
    evaluate({:function_call, "ISIRI", args}, bindings)
  end

  def evaluate({:function_call, "ISBLANK", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      {:ok, make_boolean(match?({:blank_node, _}, val))}
    end
  end

  def evaluate({:function_call, "ISLITERAL", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      is_lit = match?({:literal, _, _}, val) or match?({:literal, _, _, _}, val)
      {:ok, make_boolean(is_lit)}
    end
  end

  def evaluate({:function_call, "ISNUMERIC", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings) do
      is_num =
        case to_numeric(val) do
          {:ok, _, _} -> true
          _ -> false
        end

      {:ok, make_boolean(is_num)}
    end
  end

  # ===========================================================================
  # Built-in Functions: String Functions
  # ===========================================================================

  def evaluate({:function_call, "STRLEN", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      {:ok, {:literal, :typed, Integer.to_string(String.length(str)), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SUBSTR", [str_arg, start_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, start_val} <- evaluate(start_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, start, _} <- to_numeric(start_val) do
      # SPARQL uses 1-based indexing
      start_idx = max(0, trunc(start) - 1)
      result = String.slice(str, start_idx..-1//1)
      {:ok, make_string_result(result, str_val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SUBSTR", [str_arg, start_arg, len_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, start_val} <- evaluate(start_arg, bindings),
         {:ok, len_val} <- evaluate(len_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, start, _} <- to_numeric(start_val),
         {:ok, len, _} <- to_numeric(len_val) do
      start_idx = max(0, trunc(start) - 1)
      result = String.slice(str, start_idx, trunc(len))
      {:ok, make_string_result(result, str_val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "UCASE", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      {:ok, make_string_result(String.upcase(str), val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "LCASE", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      {:ok, make_string_result(String.downcase(str), val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "STRSTARTS", [str_arg, prefix_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, prefix_val} <- evaluate(prefix_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, prefix} <- to_string_value(prefix_val) do
      {:ok, make_boolean(String.starts_with?(str, prefix))}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "STRENDS", [str_arg, suffix_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, suffix_val} <- evaluate(suffix_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, suffix} <- to_string_value(suffix_val) do
      {:ok, make_boolean(String.ends_with?(str, suffix))}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "CONTAINS", [str_arg, substr_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, substr_val} <- evaluate(substr_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, substr} <- to_string_value(substr_val) do
      {:ok, make_boolean(String.contains?(str, substr))}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "STRBEFORE", [str_arg, match_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, match_val} <- evaluate(match_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, match} <- to_string_value(match_val) do
      result =
        case String.split(str, match, parts: 2) do
          [before, _] -> before
          _ -> ""
        end

      {:ok, make_string_result(result, str_val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "STRAFTER", [str_arg, match_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, match_val} <- evaluate(match_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, match} <- to_string_value(match_val) do
      result =
        case String.split(str, match, parts: 2) do
          [_, after_] -> after_
          _ -> ""
        end

      {:ok, make_string_result(result, str_val)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "ENCODE_FOR_URI", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      {:ok, {:literal, :simple, URI.encode(str)}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "CONCAT", args}, bindings) when is_list(args) do
    results =
      Enum.map(args, fn arg ->
        with {:ok, val} <- evaluate(arg, bindings),
             {:ok, str} <- to_string_value(val) do
          {:ok, str}
        end
      end)

    if Enum.all?(results, &match?({:ok, _}, &1)) do
      strs = Enum.map(results, fn {:ok, s} -> s end)
      {:ok, {:literal, :simple, Enum.join(strs)}}
    else
      :error
    end
  end

  def evaluate({:function_call, "LANGMATCHES", [lang_arg, pattern_arg]}, bindings) do
    with {:ok, lang_val} <- evaluate(lang_arg, bindings),
         {:ok, pattern_val} <- evaluate(pattern_arg, bindings),
         {:ok, lang} <- to_string_value(lang_val),
         {:ok, pattern} <- to_string_value(pattern_val) do
      result =
        cond do
          pattern == "*" -> lang != ""
          String.downcase(lang) == String.downcase(pattern) -> true
          String.starts_with?(String.downcase(lang), String.downcase(pattern) <> "-") -> true
          true -> false
        end

      {:ok, make_boolean(result)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "REGEX", [str_arg, pattern_arg]}, bindings) do
    evaluate({:function_call, "REGEX", [str_arg, pattern_arg, {:literal, :simple, ""}]}, bindings)
  end

  def evaluate({:function_call, "REGEX", [str_arg, pattern_arg, flags_arg]}, bindings) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, pattern_val} <- evaluate(pattern_arg, bindings),
         {:ok, flags_val} <- evaluate(flags_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, pattern} <- to_string_value(pattern_val),
         {:ok, flags} <- to_string_value(flags_val),
         :ok <- validate_regex_pattern(pattern) do
      regex_opts = parse_regex_flags(flags)

      case Regex.compile(pattern, regex_opts) do
        {:ok, regex} ->
          case safe_regex_match(regex, str) do
            {:ok, result} -> {:ok, make_boolean(result)}
            {:error, :timeout} -> :error
          end

        {:error, _} ->
          :error
      end
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "REPLACE", [str_arg, pattern_arg, replacement_arg]}, bindings) do
    evaluate(
      {:function_call, "REPLACE",
       [str_arg, pattern_arg, replacement_arg, {:literal, :simple, ""}]},
      bindings
    )
  end

  def evaluate(
        {:function_call, "REPLACE", [str_arg, pattern_arg, replacement_arg, flags_arg]},
        bindings
      ) do
    with {:ok, str_val} <- evaluate(str_arg, bindings),
         {:ok, pattern_val} <- evaluate(pattern_arg, bindings),
         {:ok, replacement_val} <- evaluate(replacement_arg, bindings),
         {:ok, flags_val} <- evaluate(flags_arg, bindings),
         {:ok, str} <- to_string_value(str_val),
         {:ok, pattern} <- to_string_value(pattern_val),
         {:ok, replacement} <- to_string_value(replacement_val),
         {:ok, flags} <- to_string_value(flags_val),
         :ok <- validate_regex_pattern(pattern) do
      regex_opts = parse_regex_flags(flags)

      case Regex.compile(pattern, regex_opts) do
        {:ok, regex} ->
          case safe_regex_replace(regex, str, replacement) do
            {:ok, result} -> {:ok, make_string_result(result, str_val)}
            {:error, :timeout} -> :error
          end

        {:error, _} ->
          :error
      end
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Built-in Functions: Numeric Functions
  # ===========================================================================

  def evaluate({:function_call, "ABS", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, num, type} <- to_numeric(val) do
      {:ok, make_numeric(abs(num), type)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "ROUND", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, num, type} <- to_numeric(val) do
      {:ok, make_numeric(round(num), type)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "CEIL", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, num, type} <- to_numeric(val) do
      {:ok, make_numeric(Float.ceil(num / 1), type)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "FLOOR", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, num, type} <- to_numeric(val) do
      {:ok, make_numeric(Float.floor(num / 1), type)}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "RAND", []}, _bindings) do
    {:ok, {:literal, :typed, Float.to_string(:rand.uniform()), @xsd_double}}
  end

  # ===========================================================================
  # Built-in Functions: Date/Time Functions
  # ===========================================================================

  def evaluate({:function_call, "NOW", []}, _bindings) do
    now = DateTime.utc_now() |> DateTime.to_iso8601()
    {:ok, {:literal, :typed, now, @xsd_date_time}}
  end

  def evaluate({:function_call, "YEAR", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.year), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "MONTH", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.month), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "DAY", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.day), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "HOURS", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.hour), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "MINUTES", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.minute), @xsd_integer}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SECONDS", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      {:ok, {:literal, :typed, Integer.to_string(dt.second), @xsd_decimal}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "TIMEZONE", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      # Return timezone as xsd:dayTimeDuration
      offset_mins = div(dt.utc_offset + dt.std_offset, 60)
      sign = if offset_mins >= 0, do: "", else: "-"
      hours = abs(div(offset_mins, 60))
      mins = abs(rem(offset_mins, 60))
      duration = "#{sign}PT#{hours}H#{mins}M"
      {:ok, {:literal, :typed, duration, "http://www.w3.org/2001/XMLSchema#dayTimeDuration"}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "TZ", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, dt} <- parse_datetime(val) do
      tz = if dt.time_zone == "Etc/UTC", do: "Z", else: dt.zone_abbr
      {:ok, {:literal, :simple, tz}}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Built-in Functions: Hash Functions
  # ===========================================================================

  def evaluate({:function_call, "MD5", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      hash = :crypto.hash(:md5, str) |> Base.encode16(case: :lower)
      {:ok, {:literal, :simple, hash}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SHA1", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      hash = :crypto.hash(:sha, str) |> Base.encode16(case: :lower)
      {:ok, {:literal, :simple, hash}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SHA256", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      hash = :crypto.hash(:sha256, str) |> Base.encode16(case: :lower)
      {:ok, {:literal, :simple, hash}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SHA384", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      hash = :crypto.hash(:sha384, str) |> Base.encode16(case: :lower)
      {:ok, {:literal, :simple, hash}}
    else
      _ -> :error
    end
  end

  def evaluate({:function_call, "SHA512", [arg]}, bindings) do
    with {:ok, val} <- evaluate(arg, bindings),
         {:ok, str} <- to_string_value(val) do
      hash = :crypto.hash(:sha512, str) |> Base.encode16(case: :lower)
      {:ok, {:literal, :simple, hash}}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # Conditional Expressions
  # ===========================================================================

  def evaluate({:if_expr, cond_expr, then_expr, else_expr}, bindings) do
    with {:ok, cond_val} <- evaluate(cond_expr, bindings),
         {:ok, cond_bool} <- effective_boolean_value(cond_val) do
      if cond_bool do
        evaluate(then_expr, bindings)
      else
        evaluate(else_expr, bindings)
      end
    else
      _ -> evaluate(else_expr, bindings)
    end
  end

  def evaluate({:coalesce, args}, bindings) when is_list(args) do
    Enum.find_value(args, :error, fn arg ->
      case evaluate(arg, bindings) do
        {:ok, val} -> {:ok, val}
        :error -> nil
      end
    end)
  end

  def evaluate({:in_expr, needle_expr, haystack}, bindings) when is_list(haystack) do
    with {:ok, needle} <- evaluate(needle_expr, bindings) do
      result =
        Enum.any?(haystack, fn hay_expr ->
          case evaluate(hay_expr, bindings) do
            {:ok, hay} -> rdf_equal?(needle, hay)
            :error -> false
          end
        end)

      {:ok, make_boolean(result)}
    else
      _ -> :error
    end
  end

  # ===========================================================================
  # EXISTS/NOT EXISTS (requires pattern evaluation context)
  # ===========================================================================

  def evaluate({:exists, _pattern}, _bindings) do
    # EXISTS evaluation requires access to the data store, which is handled
    # at a higher level (the executor). Here we just return a placeholder error.
    # The actual implementation will be in the query executor.
    :error
  end

  # Fallback for unknown function calls
  def evaluate({:function_call, _name, _args}, _bindings) do
    # Unknown function - could be a custom function
    :error
  end

  # Fallback for unknown expressions
  def evaluate(expr, _bindings) when is_tuple(expr) do
    :error
  end

  # ===========================================================================
  # Aggregate Functions (evaluated in GROUP BY context)
  # ===========================================================================

  @doc """
  Evaluates an aggregate function over a group of solutions.

  This is called by the query executor during GROUP BY processing.
  """
  @spec evaluate_aggregate(tuple(), [bindings()]) :: eval_result()
  def evaluate_aggregate({:count, :star, distinct?}, solutions) do
    count = if distinct?, do: length(Enum.uniq(solutions)), else: length(solutions)
    {:ok, {:literal, :typed, Integer.to_string(count), @xsd_integer}}
  end

  def evaluate_aggregate({:count, expr, distinct?}, solutions) do
    values = collect_values(expr, solutions, distinct?)
    {:ok, {:literal, :typed, Integer.to_string(length(values)), @xsd_integer}}
  end

  def evaluate_aggregate({:sum, expr, distinct?}, solutions) do
    values = collect_numeric_values(expr, solutions, distinct?)

    if values == [] do
      {:ok, {:literal, :typed, "0", @xsd_integer}}
    else
      sum = Enum.sum(Enum.map(values, fn {n, _} -> n end))
      type = Enum.reduce(values, :integer, fn {_, t}, acc -> promote_numeric_type(acc, t) end)
      {:ok, make_numeric(sum, type)}
    end
  end

  def evaluate_aggregate({:avg, expr, distinct?}, solutions) do
    values = collect_numeric_values(expr, solutions, distinct?)

    if values == [] do
      {:ok, {:literal, :typed, "0", @xsd_decimal}}
    else
      nums = Enum.map(values, fn {n, _} -> n end)
      avg = Enum.sum(nums) / length(nums)
      {:ok, {:literal, :typed, Float.to_string(avg), @xsd_decimal}}
    end
  end

  def evaluate_aggregate({:min, expr, _distinct?}, solutions) do
    values = collect_values(expr, solutions, false)

    if values == [] do
      :error
    else
      min_val = Enum.min_by(values, &term_sort_key/1)
      {:ok, min_val}
    end
  end

  def evaluate_aggregate({:max, expr, _distinct?}, solutions) do
    values = collect_values(expr, solutions, false)

    if values == [] do
      :error
    else
      max_val = Enum.max_by(values, &term_sort_key/1)
      {:ok, max_val}
    end
  end

  def evaluate_aggregate({{:group_concat, separator}, expr, distinct?}, solutions) do
    sep = separator || " "
    values = collect_values(expr, solutions, distinct?)
    strs = Enum.map(values, &term_to_string/1)
    {:ok, {:literal, :simple, Enum.join(strs, sep)}}
  end

  def evaluate_aggregate({:sample, expr, _distinct?}, solutions) do
    case solutions do
      [] ->
        :error

      [first | _] ->
        case evaluate(expr, first) do
          {:ok, val} -> {:ok, val}
          :error -> :error
        end
    end
  end

  # ===========================================================================
  # Expression Analysis
  # ===========================================================================

  @doc """
  Extracts all variable names referenced in an expression.
  """
  @spec expression_variables(expression()) :: [String.t()]
  def expression_variables(expr) do
    expr
    |> collect_expr_variables()
    |> Enum.uniq()
  end

  defp collect_expr_variables({:variable, name}), do: [name]

  defp collect_expr_variables(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> tl()
    |> Enum.flat_map(&collect_expr_variables/1)
  end

  defp collect_expr_variables(list) when is_list(list) do
    Enum.flat_map(list, &collect_expr_variables/1)
  end

  defp collect_expr_variables(_), do: []

  @doc """
  Checks if an expression is a constant (no variable references).
  """
  @spec is_constant?(expression()) :: boolean()
  def is_constant?(expr) do
    expression_variables(expr) == []
  end

  @doc """
  Returns the type of an expression node.
  """
  @spec expression_type(expression()) :: atom()
  def expression_type({:variable, _}), do: :variable
  def expression_type({:named_node, _}), do: :named_node
  def expression_type({:blank_node, _}), do: :blank_node
  def expression_type({:literal, _, _}), do: :literal
  def expression_type({:literal, _, _, _}), do: :literal
  def expression_type({:add, _, _}), do: :arithmetic
  def expression_type({:subtract, _, _}), do: :arithmetic
  def expression_type({:multiply, _, _}), do: :arithmetic
  def expression_type({:divide, _, _}), do: :arithmetic
  def expression_type({:unary_minus, _}), do: :arithmetic
  def expression_type({:equal, _, _}), do: :comparison
  def expression_type({:greater, _, _}), do: :comparison
  def expression_type({:less, _, _}), do: :comparison
  def expression_type({:greater_or_equal, _, _}), do: :comparison
  def expression_type({:less_or_equal, _, _}), do: :comparison
  def expression_type({:and, _, _}), do: :logical
  def expression_type({:or, _, _}), do: :logical
  def expression_type({:not, _}), do: :logical
  def expression_type({:bound, _}), do: :builtin
  def expression_type({:exists, _}), do: :builtin
  def expression_type({:if_expr, _, _, _}), do: :conditional
  def expression_type({:coalesce, _}), do: :conditional
  def expression_type({:in_expr, _, _}), do: :conditional
  def expression_type({:function_call, _, _}), do: :function_call
  def expression_type({:count, _, _}), do: :aggregate
  def expression_type({:sum, _, _}), do: :aggregate
  def expression_type({:avg, _, _}), do: :aggregate
  def expression_type({:min, _, _}), do: :aggregate
  def expression_type({:max, _, _}), do: :aggregate
  def expression_type({{:group_concat, _}, _, _}), do: :aggregate
  def expression_type({:sample, _, _}), do: :aggregate
  def expression_type(_), do: :unknown

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Convert RDF term to numeric value and type
  defp to_numeric({:literal, :typed, value, @xsd_integer}) do
    case Integer.parse(value) do
      {n, ""} -> {:ok, n, :integer}
      _ -> :error
    end
  end

  defp to_numeric({:literal, :typed, value, @xsd_decimal}) do
    case Float.parse(value) do
      {n, ""} -> {:ok, n, :decimal}
      _ -> :error
    end
  end

  defp to_numeric({:literal, :typed, value, @xsd_float}) do
    case Float.parse(value) do
      {n, ""} -> {:ok, n, :float}
      _ -> :error
    end
  end

  defp to_numeric({:literal, :typed, value, @xsd_double}) do
    case Float.parse(value) do
      {n, ""} -> {:ok, n, :double}
      _ -> :error
    end
  end

  defp to_numeric(_), do: :error

  # Promote numeric types according to SPARQL rules
  defp promote_numeric_type(:double, _), do: :double
  defp promote_numeric_type(_, :double), do: :double
  defp promote_numeric_type(:float, _), do: :float
  defp promote_numeric_type(_, :float), do: :float
  defp promote_numeric_type(:decimal, _), do: :decimal
  defp promote_numeric_type(_, :decimal), do: :decimal
  defp promote_numeric_type(:integer, :integer), do: :integer

  # Create numeric literal
  defp make_numeric(num, :integer) when is_float(num) do
    {:literal, :typed, Integer.to_string(trunc(num)), @xsd_integer}
  end

  defp make_numeric(num, :integer) do
    {:literal, :typed, Integer.to_string(num), @xsd_integer}
  end

  defp make_numeric(num, :decimal) do
    {:literal, :typed, Float.to_string(num / 1), @xsd_decimal}
  end

  defp make_numeric(num, :float) do
    {:literal, :typed, Float.to_string(num / 1), @xsd_float}
  end

  defp make_numeric(num, :double) do
    {:literal, :typed, Float.to_string(num / 1), @xsd_double}
  end

  # Create boolean literal
  defp make_boolean(true), do: {:literal, :typed, "true", @xsd_boolean}
  defp make_boolean(false), do: {:literal, :typed, "false", @xsd_boolean}

  # RDF term equality
  defp rdf_equal?({:named_node, a}, {:named_node, b}), do: a == b
  defp rdf_equal?({:blank_node, a}, {:blank_node, b}), do: a == b
  defp rdf_equal?({:literal, :simple, a}, {:literal, :simple, b}), do: a == b

  defp rdf_equal?({:literal, :lang, va, la}, {:literal, :lang, vb, lb}),
    do: va == vb and String.downcase(la) == String.downcase(lb)

  defp rdf_equal?({:literal, :typed, va, ta}, {:literal, :typed, vb, tb}),
    do: va == vb and ta == tb

  # Cross-type numeric comparison
  defp rdf_equal?(a, b) do
    case {to_numeric(a), to_numeric(b)} do
      {{:ok, na, _}, {:ok, nb, _}} -> na == nb
      _ -> false
    end
  end

  # RDF term comparison
  defp rdf_compare(a, b) do
    case {to_numeric(a), to_numeric(b)} do
      {{:ok, na, _}, {:ok, nb, _}} ->
        cond do
          na < nb -> {:ok, :lt}
          na > nb -> {:ok, :gt}
          true -> {:ok, :eq}
        end

      _ ->
        # String comparison for literals
        with {:ok, sa} <- to_string_value(a),
             {:ok, sb} <- to_string_value(b) do
          cond do
            sa < sb -> {:ok, :lt}
            sa > sb -> {:ok, :gt}
            true -> {:ok, :eq}
          end
        else
          _ -> :error
        end
    end
  end

  # Get effective boolean value
  defp effective_boolean_value({:literal, :typed, "true", @xsd_boolean}), do: {:ok, true}
  defp effective_boolean_value({:literal, :typed, "false", @xsd_boolean}), do: {:ok, false}
  defp effective_boolean_value({:literal, :typed, "1", @xsd_boolean}), do: {:ok, true}
  defp effective_boolean_value({:literal, :typed, "0", @xsd_boolean}), do: {:ok, false}
  defp effective_boolean_value({:literal, :simple, s}), do: {:ok, s != ""}
  defp effective_boolean_value({:literal, :typed, s, @xsd_string}), do: {:ok, s != ""}

  defp effective_boolean_value(term) do
    case to_numeric(term) do
      {:ok, n, _} -> {:ok, n != 0}
      _ -> :error
    end
  end

  # Extract string value from term
  defp to_string_value({:literal, :simple, s}), do: {:ok, s}
  defp to_string_value({:literal, :lang, s, _}), do: {:ok, s}
  defp to_string_value({:literal, :typed, s, _}), do: {:ok, s}
  defp to_string_value(_), do: :error

  # Convert term to string representation
  defp term_to_string({:named_node, iri}), do: iri
  defp term_to_string({:blank_node, id}), do: "_:#{id}"
  defp term_to_string({:literal, :simple, s}), do: s
  defp term_to_string({:literal, :lang, s, _}), do: s
  defp term_to_string({:literal, :typed, s, _}), do: s

  # Make string result preserving language tag if present
  defp make_string_result(str, {:literal, :lang, _, lang}), do: {:literal, :lang, str, lang}
  defp make_string_result(str, _), do: {:literal, :simple, str}

  # Parse regex flags - returns a string for Regex.compile/2
  defp parse_regex_flags(flags) do
    flags
    |> String.graphemes()
    |> Enum.map(fn
      "i" -> "i"
      "m" -> "m"
      "s" -> "s"
      "x" -> "x"
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join("")
  end

  # Parse datetime from literal
  defp parse_datetime({:literal, :typed, value, @xsd_date_time}) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> {:ok, dt}
      _ -> :error
    end
  end

  defp parse_datetime(_), do: :error

  # Collect values for aggregates
  defp collect_values(expr, solutions, distinct?) do
    values =
      solutions
      |> Enum.map(fn bindings -> evaluate(expr, bindings) end)
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, v} -> v end)

    if distinct?, do: Enum.uniq(values), else: values
  end

  # Collect numeric values for aggregates
  defp collect_numeric_values(expr, solutions, distinct?) do
    values =
      collect_values(expr, solutions, distinct?)
      |> Enum.map(fn v -> {to_numeric(v), v} end)
      |> Enum.filter(fn
        {{:ok, _, _}, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {{:ok, n, t}, _} -> {n, t} end)

    if distinct?, do: Enum.uniq(values), else: values
  end

  # Sort key for term ordering
  defp term_sort_key({:blank_node, id}), do: {0, id}
  defp term_sort_key({:named_node, iri}), do: {1, iri}
  defp term_sort_key({:literal, :simple, s}), do: {2, s}
  defp term_sort_key({:literal, :lang, s, l}), do: {3, s, l}
  defp term_sort_key({:literal, :typed, s, t}), do: {4, s, t}
  defp term_sort_key(_), do: {5, ""}

  # ===========================================================================
  # Regex Safety Functions (ReDoS Protection)
  # ===========================================================================

  # Validates regex pattern for potential ReDoS attacks
  defp validate_regex_pattern(pattern) when byte_size(pattern) > @max_regex_pattern_length do
    :error
  end

  defp validate_regex_pattern(pattern) do
    # Check for catastrophic backtracking patterns like (a+)+, (a*)*
    # These patterns can cause exponential time complexity
    if has_catastrophic_backtracking?(pattern) do
      :error
    else
      :ok
    end
  end

  # Detect patterns known to cause catastrophic backtracking
  # This is a heuristic check for common dangerous patterns
  defp has_catastrophic_backtracking?(pattern) do
    # Nested quantifiers: (a+)+, (a*)+, (a+)*, (a*)*, etc.
    nested_quantifier = ~r/\([^)]*[+*][^)]*\)[+*]/

    # Overlapping alternatives with quantifiers: (a|a)+
    # This is harder to detect precisely, so we're conservative

    Regex.match?(nested_quantifier, pattern)
  end

  # Execute regex match with timeout protection
  defp safe_regex_match(regex, string) do
    task = Task.async(fn -> Regex.match?(regex, string) end)

    case Task.yield(task, @regex_timeout_ms) do
      {:ok, result} ->
        {:ok, result}

      nil ->
        Task.shutdown(task, :brutal_kill)

        :telemetry.execute(
          [:triple_store, :sparql, :expression, :regex_timeout],
          %{pattern: regex.source, string_length: byte_size(string)},
          %{}
        )

        {:error, :timeout}
    end
  end

  # Execute regex replace with timeout protection
  defp safe_regex_replace(regex, string, replacement) do
    task = Task.async(fn -> Regex.replace(regex, string, replacement) end)

    case Task.yield(task, @regex_timeout_ms) do
      {:ok, result} ->
        {:ok, result}

      nil ->
        Task.shutdown(task, :brutal_kill)

        :telemetry.execute(
          [:triple_store, :sparql, :expression, :regex_timeout],
          %{pattern: regex.source, string_length: byte_size(string)},
          %{}
        )

        {:error, :timeout}
    end
  end
end
