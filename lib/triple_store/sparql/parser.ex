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
