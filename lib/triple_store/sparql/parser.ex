defmodule TripleStore.SPARQL.Parser do
  @moduledoc """
  SPARQL query and update parser using spargebra NIF.

  This module provides functions to parse SPARQL query and update strings into an
  Elixir-native AST representation. The parser supports all SPARQL 1.1
  query forms (SELECT, CONSTRUCT, ASK, DESCRIBE) and update operations
  (INSERT DATA, DELETE DATA, DELETE/INSERT WHERE, LOAD, CLEAR, CREATE, DROP).

  ## Usage

      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?name WHERE { ?s foaf:name ?name }")
      {:ok, ast} = TripleStore.SPARQL.Parser.parse_update("INSERT DATA { <s> <p> <o> }")

  ## AST Structure

  The AST is returned as nested Elixir tuples and lists representing:

  - **Query types**: `:select`, `:construct`, `:ask`, `:describe`
  - **Update type**: `:update` with list of operations
  - **Pattern types**: `:bgp`, `:join`, `:left_join`, `:union`, `:filter`, etc.
  - **Term types**: `:variable`, `:named_node`, `:blank_node`, `:literal`
  - **Expression types**: `:and`, `:or`, `:equal`, `:less`, `:greater`, etc.
  - **Update operations**: `:insert_data`, `:delete_data`, `:delete_insert`, `:load`, `:clear`, `:create`, `:drop`

  ## Examples

      # Parse a simple SELECT query
      iex> {:ok, {:select, props}} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      iex> is_list(props)
      true

      # Parse with FILTER
      iex> {:ok, _ast} = TripleStore.SPARQL.Parser.parse("SELECT ?age WHERE { ?s :age ?age . FILTER(?age > 30) }")

      # Parse error handling
      iex> {:error, {:parse_error, msg}} = TripleStore.SPARQL.Parser.parse("INVALID QUERY")
      iex> is_binary(msg)
      true

      # Parse UPDATE operations
      iex> {:ok, {:update, _}} = TripleStore.SPARQL.Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")

  """

  alias TripleStore.SPARQL.Parser.NIF

  # ===========================================================================
  # Error Types
  # ===========================================================================

  @typedoc """
  Structured parse error with position information.

  Fields:
  - `message` - Human-readable error description
  - `line` - Line number (1-indexed) where error occurred
  - `column` - Column number (1-indexed) where error occurred
  - `raw_message` - Original error message from parser
  - `hint` - Optional suggestion for fixing the error
  """
  @type parse_error :: %{
          message: String.t(),
          line: pos_integer() | nil,
          column: pos_integer() | nil,
          raw_message: String.t(),
          hint: String.t() | nil
        }

  @doc """
  Parses a SPARQL query string into an AST.

  ## Arguments
  - `sparql` - The SPARQL query string to parse

  ## Returns
  - `{:ok, ast}` on success
  - `{:error, {:parse_error, message}}` on parse failure

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT * WHERE { ?s ?p ?o }")
      iex> elem(ast, 0)
      :select

  """
  @spec parse(String.t()) :: {:ok, term()} | {:error, {:parse_error, String.t()}}
  def parse(sparql) when is_binary(sparql) do
    NIF.parse_query(sparql)
  end

  @doc """
  Parses a SPARQL query string, raising on error.

  ## Arguments
  - `sparql` - The SPARQL query string to parse

  ## Returns
  - The parsed AST on success

  ## Raises
  - `ArgumentError` on parse failure

  ## Examples

      iex> ast = TripleStore.SPARQL.Parser.parse!("SELECT ?s WHERE { ?s ?p ?o }")
      iex> elem(ast, 0)
      :select

  """
  @spec parse!(String.t()) :: term()
  def parse!(sparql) when is_binary(sparql) do
    case parse(sparql) do
      {:ok, ast} -> ast
      {:error, {:parse_error, message}} -> raise ArgumentError, "SPARQL parse error: #{message}"
    end
  end

  @doc """
  Parses a SPARQL UPDATE string into an AST.

  ## Arguments
  - `sparql` - The SPARQL UPDATE string to parse

  ## Returns
  - `{:ok, ast}` on success
  - `{:error, {:parse_error, message}}` on parse failure

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      iex> elem(ast, 0)
      :update

  """
  @spec parse_update(String.t()) :: {:ok, term()} | {:error, {:parse_error, String.t()}}
  def parse_update(sparql) when is_binary(sparql) do
    NIF.parse_update(sparql)
  end

  @doc """
  Parses a SPARQL UPDATE string, raising on error.

  ## Arguments
  - `sparql` - The SPARQL UPDATE string to parse

  ## Returns
  - The parsed AST on success

  ## Raises
  - `ArgumentError` on parse failure

  ## Examples

      iex> ast = TripleStore.SPARQL.Parser.parse_update!("CLEAR ALL")
      iex> elem(ast, 0)
      :update

  """
  @spec parse_update!(String.t()) :: term()
  def parse_update!(sparql) when is_binary(sparql) do
    case parse_update(sparql) do
      {:ok, ast} -> ast
      {:error, {:parse_error, message}} -> raise ArgumentError, "SPARQL UPDATE parse error: #{message}"
    end
  end

  # ===========================================================================
  # Enhanced Error Handling
  # ===========================================================================

  @doc """
  Parses a SPARQL query string with detailed error information.

  Returns structured error information including line/column position and
  helpful hints for common mistakes.

  ## Arguments
  - `sparql` - The SPARQL query string to parse

  ## Returns
  - `{:ok, ast}` on success
  - `{:error, error_details}` on parse failure, where `error_details` is a map containing:
    - `:message` - Human-readable error description
    - `:line` - Line number (1-indexed) where error occurred
    - `:column` - Column number (1-indexed) where error occurred
    - `:raw_message` - Original error message from parser
    - `:hint` - Optional suggestion for fixing the error

  ## Examples

      iex> {:ok, _ast} = TripleStore.SPARQL.Parser.parse_with_details("SELECT ?s WHERE { ?s ?p ?o }")

      iex> {:error, error} = TripleStore.SPARQL.Parser.parse_with_details("SELECT ?s WHERE")
      iex> is_integer(error.line) and is_integer(error.column)
      true

  """
  @spec parse_with_details(String.t()) :: {:ok, term()} | {:error, parse_error()}
  def parse_with_details(sparql) when is_binary(sparql) do
    case NIF.parse_query(sparql) do
      {:ok, ast} ->
        {:ok, ast}

      {:error, {:parse_error, raw_message}} ->
        {:error, build_error_details(raw_message, sparql, :query)}
    end
  end

  @doc """
  Parses a SPARQL UPDATE string with detailed error information.

  Returns structured error information including line/column position and
  helpful hints for common mistakes.

  ## Arguments
  - `sparql` - The SPARQL UPDATE string to parse

  ## Returns
  - `{:ok, ast}` on success
  - `{:error, error_details}` on parse failure, where `error_details` is a map containing:
    - `:message` - Human-readable error description
    - `:line` - Line number (1-indexed) where error occurred
    - `:column` - Column number (1-indexed) where error occurred
    - `:raw_message` - Original error message from parser
    - `:hint` - Optional suggestion for fixing the error

  ## Examples

      iex> {:ok, _ast} = TripleStore.SPARQL.Parser.parse_update_with_details("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")

      iex> {:error, error} = TripleStore.SPARQL.Parser.parse_update_with_details("INSERT DATA")
      iex> is_integer(error.line) and is_integer(error.column)
      true

  """
  @spec parse_update_with_details(String.t()) :: {:ok, term()} | {:error, parse_error()}
  def parse_update_with_details(sparql) when is_binary(sparql) do
    case NIF.parse_update(sparql) do
      {:ok, ast} ->
        {:ok, ast}

      {:error, {:parse_error, raw_message}} ->
        {:error, build_error_details(raw_message, sparql, :update)}
    end
  end

  @doc """
  Formats a parse error into a human-readable string with position context.

  ## Arguments
  - `error` - A parse error map from `parse_with_details/1` or `parse_update_with_details/1`
  - `sparql` - The original SPARQL string (for context display)

  ## Returns
  A formatted error string with:
  - Error message
  - Line and column position
  - The problematic line with a caret pointing to the error location
  - Optional hint for fixing the error

  ## Examples

      iex> {:error, error} = TripleStore.SPARQL.Parser.parse_with_details("SELECT ?s WHERE")
      iex> formatted = TripleStore.SPARQL.Parser.format_error(error, "SELECT ?s WHERE")
      iex> String.contains?(formatted, "line 1")
      true

  """
  @spec format_error(parse_error(), String.t()) :: String.t()
  def format_error(error, sparql) when is_map(error) and is_binary(sparql) do
    lines = String.split(sparql, "\n")

    position_str =
      case {error.line, error.column} do
        {line, col} when is_integer(line) and is_integer(col) ->
          "at line #{line}, column #{col}"

        {line, _} when is_integer(line) ->
          "at line #{line}"

        _ ->
          "at unknown position"
      end

    context =
      if error.line && error.line > 0 && error.line <= length(lines) do
        line_content = Enum.at(lines, error.line - 1)
        pointer =
          if error.column && error.column > 0 do
            String.duplicate(" ", error.column - 1) <> "^"
          else
            ""
          end

        """

            #{line_content}
            #{pointer}
        """
      else
        ""
      end

    hint_str =
      if error.hint do
        "\n\nHint: #{error.hint}"
      else
        ""
      end

    "Parse error #{position_str}: #{error.message}#{context}#{hint_str}"
  end

  # Builds detailed error information from a raw error message
  @doc false
  @spec build_error_details(String.t(), String.t(), :query | :update) :: parse_error()
  def build_error_details(raw_message, sparql, query_type) do
    {line, column} = extract_position(raw_message)
    expected = extract_expected(raw_message)
    # Pass raw_message to build_message so it can detect things like "Prefix not found"
    message = build_message(expected, raw_message, query_type)
    hint = generate_hint(raw_message, sparql, query_type)

    %{
      message: message,
      line: line,
      column: column,
      raw_message: raw_message,
      hint: hint
    }
  end

  # Extracts line and column from error message format "error at LINE:COLUMN: ..."
  defp extract_position(raw_message) do
    case Regex.run(~r/error at (\d+):(\d+):/, raw_message) do
      [_, line_str, col_str] ->
        {String.to_integer(line_str), String.to_integer(col_str)}

      _ ->
        {nil, nil}
    end
  end

  # Extracts the "expected" part from error message (handles multiline)
  defp extract_expected(raw_message) do
    # Use dotall mode (s) so . matches newlines too
    case Regex.run(~r/expected (.+)/s, raw_message, capture: :all_but_first) do
      [expected] -> String.trim(expected)
      _ -> nil
    end
  end

  # Builds a human-readable message from expected tokens
  defp build_message(nil, _raw_message, _query_type), do: "Syntax error in query"

  defp build_message(expected, raw_message, query_type) do
    cond do
      # Check for prefix not found first (can be inside "one of")
      String.contains?(raw_message, "Prefix not found") ->
        "Undefined prefix. The prefix used in this query has not been declared"

      # Check for unbound variable in SELECT (GROUP BY scoping error)
      String.contains?(raw_message, "variable that is unbound") ->
        "Variable scoping error. A variable in SELECT is not bound by the query pattern or GROUP BY"

      String.contains?(expected, "CONSTRUCT") ->
        "Invalid query form. Expected SELECT, CONSTRUCT, ASK, or DESCRIBE"

      String.contains?(expected, "INSERT") or String.contains?(expected, "DELETE") ->
        "Invalid update form. Expected INSERT DATA, DELETE DATA, INSERT/DELETE WHERE, LOAD, CLEAR, CREATE, or DROP"

      String.contains?(expected, "\"{\"") and String.contains?(expected, "DISTINCT") ->
        "Missing WHERE clause or opening brace '{'."

      String.contains?(expected, "\"}\"") ->
        "Unclosed brace. Expected '}' to close the pattern block"

      String.contains?(expected, "\".\"") or String.contains?(expected, "\";\"") ->
        "Invalid triple pattern. Expected '.' to separate triples or complete the pattern"

      String.contains?(expected, "LATERAL") or String.contains?(expected, "SERVICE") ->
        "Incomplete triple pattern. A triple requires subject, predicate, and object"

      String.contains?(expected, "one of") ->
        simplify_expected(expected, query_type)

      true ->
        "Expected #{expected}"
    end
  end

  # Simplifies complex "expected one of" messages
  defp simplify_expected(expected, _query_type) do
    # Extract just the readable tokens, ignoring unicode ranges
    tokens =
      expected
      |> String.replace(~r/\[.*?\]/, "")
      |> String.replace(~r/\\u\{[^}]+\}/, "")
      |> String.replace("one of ", "")
      |> String.split(~r/[,\s]+/)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "'")))
      |> Enum.uniq()
      |> Enum.take(5)

    if Enum.empty?(tokens) do
      "Syntax error - unexpected token"
    else
      "Expected one of: #{Enum.join(tokens, ", ")}"
    end
  end

  # Generates helpful hints based on common mistakes
  defp generate_hint(raw_message, sparql, query_type) do
    cond do
      # Undefined prefix - extract the prefix name from the query
      String.contains?(raw_message, "Prefix not found") ->
        extract_undefined_prefix_hint(sparql)

      # Unbound variable scoping error (GROUP BY)
      String.contains?(raw_message, "variable that is unbound") ->
        if String.contains?(String.upcase(sparql), "GROUP BY") do
          "When using GROUP BY, all SELECT variables must be either:\n" <>
            "  1. Listed in the GROUP BY clause, or\n" <>
            "  2. Used inside an aggregate function (COUNT, SUM, AVG, etc.)"
        else
          "When using aggregate functions, add a GROUP BY clause or wrap all other variables in aggregate functions"
        end

      # Missing WHERE keyword
      String.contains?(raw_message, "expected") and
          String.contains?(raw_message, "\"{\"") and
          not String.contains?(String.upcase(sparql), "WHERE") and
          query_type == :query ->
        "Did you forget the WHERE keyword? Example: SELECT ?s WHERE { ?s ?p ?o }"

      # Unclosed brace
      String.contains?(raw_message, "\"}\"") ->
        count_braces = fn s ->
          opens = s |> String.graphemes() |> Enum.count(&(&1 == "{"))
          closes = s |> String.graphemes() |> Enum.count(&(&1 == "}"))
          {opens, closes}
        end

        {opens, closes} = count_braces.(sparql)

        if opens > closes do
          "You have #{opens} opening braces but only #{closes} closing braces"
        else
          "Check that all braces are properly matched"
        end

      # Incomplete triple - missing object
      String.contains?(raw_message, "LATERAL") or String.contains?(raw_message, "SERVICE") ->
        "Triple patterns require exactly three parts: subject predicate object. Did you forget the object?"

      # Missing separator between triples
      String.contains?(raw_message, "\".\"") ->
        "Use '.' to separate triple patterns, or ';' for patterns sharing the same subject"

      # Invalid IRI
      String.contains?(raw_message, "<") or String.contains?(raw_message, ">") ->
        "IRIs must be enclosed in angle brackets: <http://example.org/resource>"

      # Variables must start with ? or $
      Regex.match?(~r/\b[a-z][a-z0-9]*\s+[a-z]/i, sparql) and
          not String.contains?(sparql, "?") and
          not String.contains?(sparql, "$") ->
        "Variables must start with '?' or '$'. Example: ?name, $value"

      true ->
        nil
    end
  end

  # Extracts undefined prefix and suggests declaration
  defp extract_undefined_prefix_hint(sparql) do
    # Look for prefix:localname patterns that aren't declared
    declared_prefixes =
      Regex.scan(~r/PREFIX\s+(\w+):/i, sparql)
      |> Enum.map(fn [_, prefix] -> prefix end)
      |> MapSet.new()

    used_prefixes =
      Regex.scan(~r/\b(\w+):[a-zA-Z]/, sparql)
      |> Enum.map(fn [_, prefix] -> prefix end)
      |> Enum.reject(&(&1 in ["http", "https", "urn", "mailto"]))
      |> MapSet.new()

    undefined = MapSet.difference(used_prefixes, declared_prefixes)

    case MapSet.to_list(undefined) do
      [prefix] ->
        "Add 'PREFIX #{prefix}: <URI>' at the start of your query. Common prefixes:\n" <>
          "  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" <>
          "  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" <>
          "  PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" <>
          "  PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"

      [_ | _] = prefixes ->
        "Add PREFIX declarations for: #{Enum.join(prefixes, ", ")}"

      [] ->
        "Declare the prefix with PREFIX before using it"
    end
  end

  @doc """
  Checks if the NIF is loaded and operational.

  ## Returns
  - `true` if the NIF is loaded
  - `false` otherwise

  ## Examples

      iex> TripleStore.SPARQL.Parser.nif_loaded?()
      true

  """
  @spec nif_loaded?() :: boolean()
  def nif_loaded? do
    try do
      NIF.nif_loaded() == "sparql_parser_nif"
    rescue
      _ -> false
    end
  end

  # ===========================================================================
  # AST Pattern Matching Helpers
  # ===========================================================================

  @doc """
  Extracts the query type from a parsed AST.

  ## Arguments
  - `ast` - The parsed AST

  ## Returns
  - `:select`, `:construct`, `:ask`, or `:describe`

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      iex> TripleStore.SPARQL.Parser.query_type(ast)
      :select

  """
  @spec query_type(term()) :: :select | :construct | :ask | :describe
  def query_type({type, _props}) when type in [:select, :construct, :ask, :describe], do: type

  @doc """
  Checks if the AST represents a SELECT query.
  """
  @spec select?(term()) :: boolean()
  def select?({:select, _}), do: true
  def select?(_), do: false

  @doc """
  Checks if the AST represents a CONSTRUCT query.
  """
  @spec construct?(term()) :: boolean()
  def construct?({:construct, _}), do: true
  def construct?(_), do: false

  @doc """
  Checks if the AST represents an ASK query.
  """
  @spec ask?(term()) :: boolean()
  def ask?({:ask, _}), do: true
  def ask?(_), do: false

  @doc """
  Checks if the AST represents a DESCRIBE query.
  """
  @spec describe?(term()) :: boolean()
  def describe?({:describe, _}), do: true
  def describe?(_), do: false

  @doc """
  Checks if the AST represents an UPDATE operation.
  """
  @spec update?(term()) :: boolean()
  def update?({:update, _}), do: true
  def update?(_), do: false

  # ===========================================================================
  # UPDATE AST Helpers
  # ===========================================================================

  @doc """
  Extracts operations from a parsed UPDATE AST.

  ## Arguments
  - `ast` - The parsed UPDATE AST

  ## Returns
  - A list of update operations

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      iex> ops = TripleStore.SPARQL.Parser.get_operations(ast)
      iex> length(ops) == 1
      true

  """
  @spec get_operations(term()) :: [term()]
  def get_operations({:update, props}) when is_list(props) do
    props
    |> Enum.find(fn {key, _} -> key == "operations" end)
    |> case do
      {"operations", ops} -> ops
      nil -> []
    end
  end

  def get_operations(_), do: []

  @doc """
  Extracts the pattern from a parsed query AST.

  ## Arguments
  - `ast` - The parsed AST

  ## Returns
  - The pattern subtree

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      iex> pattern = TripleStore.SPARQL.Parser.get_pattern(ast)
      iex> is_tuple(pattern)
      true

  """
  @spec get_pattern(term()) :: term()
  def get_pattern({_type, props}) when is_list(props) do
    props
    |> Enum.find(fn {key, _} -> key == "pattern" end)
    |> case do
      {"pattern", pattern} -> pattern
      nil -> nil
    end
  end

  @doc """
  Extracts variables from a pattern.

  Recursively walks the pattern AST and collects all variable names.

  ## Arguments
  - `pattern` - The pattern subtree from the AST

  ## Returns
  - A list of variable names (strings)

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s ?p WHERE { ?s ?p ?o }")
      iex> pattern = TripleStore.SPARQL.Parser.get_pattern(ast)
      iex> vars = TripleStore.SPARQL.Parser.extract_variables(pattern)
      iex> "s" in vars and "p" in vars and "o" in vars
      true

  """
  @spec extract_variables(term()) :: [String.t()]
  def extract_variables(pattern) do
    pattern
    |> do_extract_variables()
    |> Enum.uniq()
  end

  defp do_extract_variables({:variable, name}) when is_binary(name), do: [name]
  defp do_extract_variables({:bgp, triples}) when is_list(triples) do
    Enum.flat_map(triples, &do_extract_variables/1)
  end
  defp do_extract_variables({:triple, s, p, o}) do
    do_extract_variables(s) ++ do_extract_variables(p) ++ do_extract_variables(o)
  end
  defp do_extract_variables({:join, left, right}) do
    do_extract_variables(left) ++ do_extract_variables(right)
  end
  defp do_extract_variables({:left_join, left, right, _expr}) do
    do_extract_variables(left) ++ do_extract_variables(right)
  end
  defp do_extract_variables({:union, left, right}) do
    do_extract_variables(left) ++ do_extract_variables(right)
  end
  defp do_extract_variables({:filter, _expr, inner}) do
    do_extract_variables(inner)
  end
  defp do_extract_variables({:project, inner, vars}) when is_list(vars) do
    var_names = Enum.flat_map(vars, &do_extract_variables/1)
    inner_vars = do_extract_variables(inner)
    var_names ++ inner_vars
  end
  defp do_extract_variables({:distinct, inner}), do: do_extract_variables(inner)
  defp do_extract_variables({:reduced, inner}), do: do_extract_variables(inner)
  defp do_extract_variables({:slice, inner, _start, _length}), do: do_extract_variables(inner)
  defp do_extract_variables({:order_by, inner, _exprs}), do: do_extract_variables(inner)
  defp do_extract_variables({:extend, inner, var, _expr}) do
    do_extract_variables(inner) ++ do_extract_variables(var)
  end
  defp do_extract_variables({:group, inner, vars, _aggs}) do
    do_extract_variables(inner) ++ Enum.flat_map(vars, &do_extract_variables/1)
  end
  defp do_extract_variables(_), do: []

  @doc """
  Extracts all triple patterns from a BGP.

  ## Arguments
  - `pattern` - The pattern subtree from the AST

  ## Returns
  - A list of triple patterns

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT * WHERE { ?s ?p ?o . ?s :name ?name }")
      iex> pattern = TripleStore.SPARQL.Parser.get_pattern(ast)
      iex> triples = TripleStore.SPARQL.Parser.extract_bgp_triples(pattern)
      iex> length(triples) >= 1
      true

  """
  @spec extract_bgp_triples(term()) :: [term()]
  def extract_bgp_triples(pattern) do
    pattern
    |> do_extract_bgp_triples()
    |> List.flatten()
  end

  defp do_extract_bgp_triples({:bgp, triples}) when is_list(triples), do: triples
  defp do_extract_bgp_triples({:join, left, right}) do
    [do_extract_bgp_triples(left), do_extract_bgp_triples(right)]
  end
  defp do_extract_bgp_triples({:left_join, left, right, _expr}) do
    [do_extract_bgp_triples(left), do_extract_bgp_triples(right)]
  end
  defp do_extract_bgp_triples({:union, left, right}) do
    [do_extract_bgp_triples(left), do_extract_bgp_triples(right)]
  end
  defp do_extract_bgp_triples({:filter, _expr, inner}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:project, inner, _vars}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:distinct, inner}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:reduced, inner}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:slice, inner, _start, _length}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:order_by, inner, _exprs}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:extend, inner, _var, _expr}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples({:group, inner, _vars, _aggs}), do: do_extract_bgp_triples(inner)
  defp do_extract_bgp_triples(_), do: []
end
