defmodule TripleStore.SPARQL.Algebra do
  @moduledoc """
  SPARQL Algebra representation and operations.

  This module defines the algebra node types that represent SPARQL query operations
  after parsing. The algebra forms the intermediate representation between parsing
  and query execution, enabling optimization passes and structured traversal.

  ## Algebra Node Types

  The SPARQL algebra is represented using tuples with an atom tag as the first element:

  - **Basic patterns**: `:bgp`, `:triple`
  - **Joins**: `:join`, `:left_join` (OPTIONAL), `:minus`
  - **Set operations**: `:union`
  - **Filtering**: `:filter`
  - **Extension**: `:extend` (BIND), `:group` (GROUP BY)
  - **Projection**: `:project`
  - **Modifiers**: `:distinct`, `:reduced`, `:order_by`, `:slice`

  ## Examples

      # Create a BGP (Basic Graph Pattern) with one triple
      bgp = Algebra.bgp([
        Algebra.triple({:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"})
      ])

      # Create a filter over a BGP
      filtered = Algebra.filter(
        {:greater, {:variable, "age"}, {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}},
        bgp
      )

      # Create a projection
      result = Algebra.project(filtered, [{:variable, "s"}, {:variable, "name"}])

  ## Validation

  The module provides validation functions to ensure algebra trees are well-formed:

      :ok = Algebra.validate(algebra_node)
      {:error, reason} = Algebra.validate(invalid_node)

  """

  # ===========================================================================
  # Type Definitions
  # ===========================================================================

  @typedoc """
  An RDF term in a triple pattern.
  """
  @type rdf_term ::
          {:variable, String.t()}
          | {:named_node, String.t()}
          | {:blank_node, String.t()}
          | {:literal, :simple, String.t()}
          | {:literal, :lang, String.t(), String.t()}
          | {:literal, :typed, String.t(), String.t()}

  @typedoc """
  A triple pattern with subject, predicate, and object terms.
  """
  @type triple :: {:triple, rdf_term(), rdf_term(), rdf_term()}

  @typedoc """
  A list of variable projections.
  """
  @type projection :: [rdf_term()]

  @typedoc """
  An expression node for FILTERs and BINDs.
  """
  @type expression :: tuple()

  @typedoc """
  An order specification: ascending or descending on an expression.
  """
  @type order_condition :: {:asc, expression()} | {:desc, expression()}

  @typedoc """
  Aggregate function types.
  """
  @type aggregate ::
          {:count, expression() | :star, boolean()}
          | {:sum, expression(), boolean()}
          | {:avg, expression(), boolean()}
          | {:min, expression(), boolean()}
          | {:max, expression(), boolean()}
          | {:group_concat, expression(), boolean(), String.t() | nil}
          | {:sample, expression(), boolean()}

  @typedoc """
  All algebra node types.
  """
  @type t ::
          {:bgp, [triple()]}
          | {:join, t(), t()}
          | {:left_join, t(), t(), expression() | nil}
          | {:minus, t(), t()}
          | {:union, t(), t()}
          | {:filter, expression(), t()}
          | {:extend, t(), rdf_term(), expression()}
          | {:group, t(), [rdf_term()], [{rdf_term(), aggregate()}]}
          | {:project, t(), projection()}
          | {:distinct, t()}
          | {:reduced, t()}
          | {:order_by, t(), [order_condition()]}
          | {:slice, t(), non_neg_integer(), non_neg_integer() | :infinity}
          | {:values, [rdf_term()], [[rdf_term() | :undef]]}
          | {:service, rdf_term(), t(), boolean()}
          | {:graph, rdf_term(), t()}
          | {:path, rdf_term(), property_path(), rdf_term()}

  @typedoc """
  Property path expression types.
  """
  @type property_path ::
          {:link, String.t()}
          | {:reverse, property_path()}
          | {:sequence, property_path(), property_path()}
          | {:alternative, property_path(), property_path()}
          | {:zero_or_more, property_path()}
          | {:one_or_more, property_path()}
          | {:zero_or_one, property_path()}
          | {:negated_property_set, [String.t()]}

  @typedoc """
  A compiled query with query type, algebra pattern, and metadata.
  """
  @type compiled_query :: %{
          type: :select | :construct | :ask | :describe,
          pattern: t(),
          dataset: term() | nil,
          base_iri: String.t() | nil,
          template: [triple()] | nil
        }

  # ===========================================================================
  # AST Compilation
  # ===========================================================================

  @doc """
  Compiles a parsed AST into a compiled query structure.

  The parser already produces algebra nodes, so this function primarily
  extracts and normalizes the query structure, validates the algebra,
  and returns a structured result.

  ## Arguments
  - `ast` - The parsed AST from `Parser.parse/1` or `Parser.parse!/1`

  ## Returns
  - `{:ok, compiled_query}` on success
  - `{:error, reason}` on validation failure

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      iex> {:ok, compiled} = TripleStore.SPARQL.Algebra.from_ast(ast)
      iex> compiled.type
      :select

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("ASK WHERE { ?s ?p ?o }")
      iex> {:ok, compiled} = TripleStore.SPARQL.Algebra.from_ast(ast)
      iex> compiled.type
      :ask

  """
  @spec from_ast(tuple()) :: {:ok, compiled_query()} | {:error, String.t()}
  def from_ast({:select, props}) when is_list(props) do
    compile_query(:select, props)
  end

  def from_ast({:construct, props}) when is_list(props) do
    compile_query(:construct, props)
  end

  def from_ast({:ask, props}) when is_list(props) do
    compile_query(:ask, props)
  end

  def from_ast({:describe, props}) when is_list(props) do
    compile_query(:describe, props)
  end

  def from_ast(ast) do
    {:error, "Invalid AST: expected {:select|:construct|:ask|:describe, props}, got: #{inspect(ast)}"}
  end

  @doc """
  Compiles a parsed AST into a compiled query, raising on error.

  ## Examples

      iex> ast = TripleStore.SPARQL.Parser.parse!("SELECT ?s WHERE { ?s ?p ?o }")
      iex> compiled = TripleStore.SPARQL.Algebra.from_ast!(ast)
      iex> compiled.type
      :select

  """
  @spec from_ast!(tuple()) :: compiled_query()
  def from_ast!(ast) do
    case from_ast(ast) do
      {:ok, compiled} -> compiled
      {:error, reason} -> raise ArgumentError, "AST compilation failed: #{reason}"
    end
  end

  # Compiles a query from its properties
  defp compile_query(type, props) do
    pattern = get_prop(props, "pattern")
    dataset = get_prop(props, "dataset")
    base_iri = get_prop(props, "base_iri")
    template = get_prop(props, "template")

    if is_nil(pattern) do
      {:error, "Missing pattern in #{type} query"}
    else
      case validate(pattern) do
        :ok ->
          {:ok,
           %{
             type: type,
             pattern: pattern,
             dataset: dataset,
             base_iri: base_iri,
             template: template
           }}

        {:error, reason} ->
          {:error, "Invalid algebra pattern: #{reason}"}
      end
    end
  end

  defp get_prop(props, key) do
    Enum.find_value(props, fn
      {^key, value} -> value
      _ -> nil
    end)
  end

  @doc """
  Extracts the algebra pattern from a compiled query or raw AST.

  This is a convenience function for getting just the pattern without
  the full compiled query structure.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> Algebra.node_type(pattern)
      :project

  """
  @spec extract_pattern(tuple() | compiled_query()) :: {:ok, t()} | {:error, String.t()}
  def extract_pattern(%{pattern: pattern}), do: {:ok, pattern}

  def extract_pattern({type, props}) when type in [:select, :construct, :ask, :describe] do
    case get_prop(props, "pattern") do
      nil -> {:error, "No pattern found in query"}
      pattern -> {:ok, pattern}
    end
  end

  def extract_pattern(_), do: {:error, "Invalid query structure"}

  @doc """
  Extracts variables that will appear in the result of a SELECT query.

  For SELECT queries, this returns the projected variables.
  For other query types, returns an empty list.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s ?name WHERE { ?s ?p ?name }")
      iex> TripleStore.SPARQL.Algebra.result_variables(ast)
      [{:variable, "s"}, {:variable, "name"}]

  """
  @spec result_variables(tuple() | compiled_query()) :: [rdf_term()]
  def result_variables(%{type: :select, pattern: pattern}) do
    extract_projection_vars(pattern)
  end

  def result_variables({:select, props}) do
    case get_prop(props, "pattern") do
      nil -> []
      pattern -> extract_projection_vars(pattern)
    end
  end

  def result_variables(_), do: []

  # Extracts projection variables from the outermost project/distinct/reduced/slice node
  defp extract_projection_vars({:project, _inner, vars}) do
    Enum.map(vars, fn {:variable, name} -> {:variable, name} end)
  end

  defp extract_projection_vars({:distinct, inner}), do: extract_projection_vars(inner)
  defp extract_projection_vars({:reduced, inner}), do: extract_projection_vars(inner)
  defp extract_projection_vars({:slice, inner, _, _}), do: extract_projection_vars(inner)
  defp extract_projection_vars({:order_by, inner, _}), do: extract_projection_vars(inner)
  defp extract_projection_vars(_), do: []

  @doc """
  Returns the innermost BGP patterns from an algebra tree.

  Useful for analyzing query selectivity or extracting triple patterns
  for index selection.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s <http://a> ?o OPTIONAL { ?s <http://b> ?o2 } }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> bgps = TripleStore.SPARQL.Algebra.collect_bgps(pattern)
      iex> length(bgps)
      2

  """
  @spec collect_bgps(t()) :: [t()]
  def collect_bgps(pattern) do
    fold(pattern, [], fn
      {:bgp, _} = bgp, acc -> [bgp | acc]
      _, acc -> acc
    end)
    |> Enum.reverse()
  end

  @doc """
  Counts the total number of triple patterns in an algebra tree.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s <http://a> ?o . ?s <http://b> ?o2 }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> TripleStore.SPARQL.Algebra.triple_count(pattern)
      2

  """
  @spec triple_count(t()) :: non_neg_integer()
  def triple_count(pattern) do
    pattern
    |> collect_bgps()
    |> Enum.reduce(0, fn {:bgp, triples}, acc -> acc + length(triples) end)
  end

  @doc """
  Checks if an algebra tree contains any OPTIONAL (left join) patterns.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o OPTIONAL { ?s ?p2 ?o2 } }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> TripleStore.SPARQL.Algebra.has_optional?(pattern)
      true

  """
  @spec has_optional?(t()) :: boolean()
  def has_optional?(pattern) do
    fold(pattern, false, fn
      {:left_join, _, _, _}, _acc -> true
      _, acc -> acc
    end)
  end

  @doc """
  Checks if an algebra tree contains any UNION patterns.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { { ?s ?p ?o } UNION { ?s ?p2 ?o2 } }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> TripleStore.SPARQL.Algebra.has_union?(pattern)
      true

  """
  @spec has_union?(t()) :: boolean()
  def has_union?(pattern) do
    fold(pattern, false, fn
      {:union, _, _}, _acc -> true
      _, acc -> acc
    end)
  end

  @doc """
  Checks if an algebra tree contains any FILTER expressions.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o FILTER(?o > 10) }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> TripleStore.SPARQL.Algebra.has_filter?(pattern)
      true

  """
  @spec has_filter?(t()) :: boolean()
  def has_filter?(pattern) do
    fold(pattern, false, fn
      {:filter, _, _}, _acc -> true
      _, acc -> acc
    end)
  end

  @doc """
  Checks if an algebra tree contains aggregation (GROUP BY).

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s ?p ?o } GROUP BY ?s")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> TripleStore.SPARQL.Algebra.has_aggregation?(pattern)
      true

  """
  @spec has_aggregation?(t()) :: boolean()
  def has_aggregation?(pattern) do
    fold(pattern, false, fn
      {:group, _, _, _}, _acc -> true
      _, acc -> acc
    end)
  end

  @doc """
  Extracts all FILTER expressions from an algebra tree.

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o FILTER(?o > 10) FILTER(?s != ?o) }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> filters = TripleStore.SPARQL.Algebra.collect_filters(pattern)
      iex> length(filters)
      2

  """
  @spec collect_filters(t()) :: [expression()]
  def collect_filters(pattern) do
    fold(pattern, [], fn
      {:filter, expr, _}, acc -> [expr | acc]
      _, acc -> acc
    end)
    |> Enum.reverse()
  end

  # ===========================================================================
  # Node Types - Basic Patterns
  # ===========================================================================

  @doc """
  Returns the set of all algebra node types.
  """
  @spec node_types() :: [atom()]
  def node_types do
    [
      :bgp,
      :join,
      :left_join,
      :minus,
      :union,
      :filter,
      :extend,
      :group,
      :project,
      :distinct,
      :reduced,
      :order_by,
      :slice,
      :values,
      :service,
      :graph,
      :path
    ]
  end

  @doc """
  Creates a Basic Graph Pattern (BGP) node.

  A BGP contains a list of triple patterns that must all match for a solution.

  ## Examples

      iex> Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}

      iex> Algebra.bgp([])
      {:bgp, []}

  """
  @spec bgp([triple()]) :: {:bgp, [triple()]}
  def bgp(patterns) when is_list(patterns) do
    {:bgp, patterns}
  end

  @doc """
  Creates a triple pattern.

  ## Examples

      iex> Algebra.triple({:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"})
      {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}

  """
  @spec triple(rdf_term(), rdf_term(), rdf_term()) :: triple()
  def triple(subject, predicate, object) do
    {:triple, subject, predicate, object}
  end

  # ===========================================================================
  # Node Types - Joins
  # ===========================================================================

  @doc """
  Creates an inner join node.

  Produces solutions that are compatible across both operands.

  ## Examples

      iex> left = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p1"}, {:variable, "o1"}}])
      iex> right = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])
      iex> {:join, ^left, ^right} = Algebra.join(left, right)

  """
  @spec join(t(), t()) :: {:join, t(), t()}
  def join(left, right) do
    {:join, left, right}
  end

  @doc """
  Creates a left outer join node (OPTIONAL).

  Produces all solutions from the left operand, extended with compatible
  solutions from the right operand where they exist.

  ## Arguments
  - `left` - The required pattern
  - `right` - The optional pattern
  - `filter` - Optional filter expression applied to matched rows (nil if none)

  ## Examples

      iex> required = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> optional = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])
      iex> {:left_join, ^required, ^optional, nil} = Algebra.left_join(required, optional)

  """
  @spec left_join(t(), t(), expression() | nil) :: {:left_join, t(), t(), expression() | nil}
  def left_join(left, right, filter \\ nil) do
    {:left_join, left, right, filter}
  end

  @doc """
  Creates a MINUS node.

  Produces solutions from the left operand that are NOT compatible with
  any solution from the right operand.

  ## Examples

      iex> pattern = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> exclude = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/deleted"}, {:literal, :simple, "true"}}])
      iex> {:minus, ^pattern, ^exclude} = Algebra.minus(pattern, exclude)

  """
  @spec minus(t(), t()) :: {:minus, t(), t()}
  def minus(left, right) do
    {:minus, left, right}
  end

  # ===========================================================================
  # Node Types - Set Operations
  # ===========================================================================

  @doc """
  Creates a UNION node.

  Produces the multiset union of solutions from both operands.

  ## Examples

      iex> left = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/a"}, {:variable, "o"}}])
      iex> right = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/b"}, {:variable, "o"}}])
      iex> {:union, ^left, ^right} = Algebra.union(left, right)

  """
  @spec union(t(), t()) :: {:union, t(), t()}
  def union(left, right) do
    {:union, left, right}
  end

  # ===========================================================================
  # Node Types - Filtering
  # ===========================================================================

  @doc """
  Creates a FILTER node.

  Filters solutions based on an expression that must evaluate to true.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}}])
      iex> expr = {:greater, {:variable, "age"}, {:literal, :typed, "18", "http://www.w3.org/2001/XMLSchema#integer"}}
      iex> {:filter, ^expr, ^bgp} = Algebra.filter(expr, bgp)

  """
  @spec filter(expression(), t()) :: {:filter, expression(), t()}
  def filter(expression, pattern) do
    {:filter, expression, pattern}
  end

  # ===========================================================================
  # Node Types - Extension
  # ===========================================================================

  @doc """
  Creates an EXTEND node (BIND).

  Extends solutions by binding a new variable to an expression value.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}}])
      iex> expr = {:multiply, {:variable, "age"}, {:literal, :typed, "12", "http://www.w3.org/2001/XMLSchema#integer"}}
      iex> {:extend, ^bgp, {:variable, "months"}, ^expr} = Algebra.extend(bgp, {:variable, "months"}, expr)

  """
  @spec extend(t(), rdf_term(), expression()) :: {:extend, t(), rdf_term(), expression()}
  def extend(pattern, variable, expression) do
    {:extend, pattern, variable, expression}
  end

  @doc """
  Creates a GROUP node for GROUP BY aggregation.

  Groups solutions by key variables and computes aggregate expressions.

  ## Arguments
  - `pattern` - The pattern to group
  - `group_vars` - Variables to group by (keyword list with :variable keys)
  - `aggregates` - List of `{variable, aggregate_expr}` tuples

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/type"}, {:variable, "type"}}])
      iex> group_vars = [variable: "s"]
      iex> aggregates = [{{:variable, "count"}, {:count, {:variable, "type"}, false}}]
      iex> {:group, ^bgp, ^group_vars, ^aggregates} = Algebra.group(bgp, group_vars, aggregates)

  """
  @spec group(t(), keyword(), [{rdf_term(), aggregate()}]) ::
          {:group, t(), keyword(), [{rdf_term(), aggregate()}]}
  def group(pattern, group_vars, aggregates) do
    {:group, pattern, group_vars, aggregates}
  end

  # ===========================================================================
  # Node Types - Projection
  # ===========================================================================

  @doc """
  Creates a PROJECT node for variable projection.

  Selects only specified variables from the solutions.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> vars = [variable: "s", variable: "p"]
      iex> {:project, ^bgp, ^vars} = Algebra.project(bgp, vars)

  """
  @spec project(t(), projection()) :: {:project, t(), projection()}
  def project(pattern, variables) do
    {:project, pattern, variables}
  end

  # ===========================================================================
  # Node Types - Duplicate Modifiers
  # ===========================================================================

  @doc """
  Creates a DISTINCT node.

  Removes duplicate solutions from the result set.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> project = Algebra.project(bgp, [variable: "s"])
      iex> {:distinct, ^project} = Algebra.distinct(project)

  """
  @spec distinct(t()) :: {:distinct, t()}
  def distinct(pattern) do
    {:distinct, pattern}
  end

  @doc """
  Creates a REDUCED node.

  Allows duplicate removal at the discretion of the query engine
  (may remove some duplicates for efficiency).

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> project = Algebra.project(bgp, [variable: "s"])
      iex> {:reduced, ^project} = Algebra.reduced(project)

  """
  @spec reduced(t()) :: {:reduced, t()}
  def reduced(pattern) do
    {:reduced, pattern}
  end

  # ===========================================================================
  # Node Types - Solution Modifiers
  # ===========================================================================

  @doc """
  Creates an ORDER BY node.

  Orders solutions by the specified expressions with direction.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}])
      iex> conditions = [asc: {:variable, "name"}]
      iex> {:order_by, ^bgp, ^conditions} = Algebra.order_by(bgp, conditions)

  """
  @spec order_by(t(), [order_condition()]) :: {:order_by, t(), [order_condition()]}
  def order_by(pattern, conditions) do
    {:order_by, pattern, conditions}
  end

  @doc """
  Creates a SLICE node for OFFSET/LIMIT.

  Selects a range of solutions starting at offset with a maximum count.

  ## Arguments
  - `pattern` - The pattern to slice
  - `offset` - Number of solutions to skip (0 for no skip)
  - `limit` - Maximum number of solutions to return (`:infinity` for no limit)

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> {:slice, ^bgp, 10, 5} = Algebra.slice(bgp, 10, 5)

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> {:slice, ^bgp, 0, 100} = Algebra.slice(bgp, 0, 100)

  """
  @spec slice(t(), non_neg_integer(), non_neg_integer() | :infinity) ::
          {:slice, t(), non_neg_integer(), non_neg_integer() | :infinity}
  def slice(pattern, offset, limit) when is_integer(offset) and offset >= 0 do
    {:slice, pattern, offset, limit}
  end

  # ===========================================================================
  # Node Types - Values/Inline Data
  # ===========================================================================

  @doc """
  Creates a VALUES node for inline data.

  Provides a fixed set of solution bindings.

  ## Examples

      iex> vars = [{:variable, "x"}, {:variable, "y"}]
      iex> data = [[{:literal, :simple, "1"}, {:literal, :simple, "2"}], [{:literal, :simple, "3"}, :undef]]
      iex> {:values, ^vars, ^data} = Algebra.values(vars, data)

  """
  @spec values([rdf_term()], [[rdf_term() | :undef]]) :: {:values, [rdf_term()], [[rdf_term() | :undef]]}
  def values(variables, data) do
    {:values, variables, data}
  end

  # ===========================================================================
  # Node Types - Named Graph and Service
  # ===========================================================================

  @doc """
  Creates a GRAPH node for querying a named graph.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> graph_name = {:named_node, "http://example.org/graph1"}
      iex> {:graph, ^graph_name, ^bgp} = Algebra.graph(graph_name, bgp)

  """
  @spec graph(rdf_term(), t()) :: {:graph, rdf_term(), t()}
  def graph(graph_term, pattern) do
    {:graph, graph_term, pattern}
  end

  @doc """
  Creates a SERVICE node for federated queries.

  ## Arguments
  - `endpoint` - The service endpoint IRI or variable
  - `pattern` - The pattern to evaluate at the service
  - `silent` - Whether to silently ignore service failures

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> endpoint = {:named_node, "http://dbpedia.org/sparql"}
      iex> {:service, ^endpoint, ^bgp, false} = Algebra.service(endpoint, bgp, false)

  """
  @spec service(rdf_term(), t(), boolean()) :: {:service, rdf_term(), t(), boolean()}
  def service(endpoint, pattern, silent \\ false) do
    {:service, endpoint, pattern, silent}
  end

  # ===========================================================================
  # Node Types - Property Paths
  # ===========================================================================

  @doc """
  Creates a property path pattern.

  ## Examples

      iex> path = {:one_or_more, {:link, "http://example.org/knows"}}
      iex> {:path, {:variable, "s"}, ^path, {:variable, "o"}} = Algebra.path({:variable, "s"}, path, {:variable, "o"})

  """
  @spec path(rdf_term(), property_path(), rdf_term()) :: {:path, rdf_term(), property_path(), rdf_term()}
  def path(subject, property_path, object) do
    {:path, subject, property_path, object}
  end

  # ===========================================================================
  # Node Analysis
  # ===========================================================================

  @doc """
  Returns the type of an algebra node.

  ## Examples

      iex> Algebra.node_type({:bgp, []})
      :bgp

      iex> Algebra.node_type({:filter, {:equal, {:variable, "x"}, {:literal, :simple, "1"}}, {:bgp, []}})
      :filter

  """
  @spec node_type(t()) :: atom()
  def node_type(node) when is_tuple(node) do
    elem(node, 0)
  end

  @doc """
  Checks if a node is of a specific type.

  ## Examples

      iex> Algebra.is_type?({:bgp, []}, :bgp)
      true

      iex> Algebra.is_type?({:filter, nil, {:bgp, []}}, :bgp)
      false

  """
  @spec is_type?(t(), atom()) :: boolean()
  def is_type?(node, type) when is_tuple(node) and is_atom(type) do
    node_type(node) == type
  end

  @doc """
  Extracts all variables referenced in an algebra node.

  Returns a list of unique variable terms found in the pattern.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> vars = Algebra.variables(bgp)
      iex> Enum.sort(vars)
      [{:variable, "o"}, {:variable, "p"}, {:variable, "s"}]

  """
  @spec variables(t()) :: [rdf_term()]
  def variables(node) do
    node
    |> collect_variables()
    |> Enum.uniq()
  end

  defp collect_variables({:bgp, triples}) do
    Enum.flat_map(triples, &collect_variables/1)
  end

  defp collect_variables({:triple, s, p, o}) do
    Enum.filter([s, p, o], &variable?/1)
  end

  defp collect_variables({:join, left, right}) do
    collect_variables(left) ++ collect_variables(right)
  end

  defp collect_variables({:left_join, left, right, filter}) do
    vars = collect_variables(left) ++ collect_variables(right)
    if filter, do: vars ++ collect_expr_variables(filter), else: vars
  end

  defp collect_variables({:minus, left, right}) do
    collect_variables(left) ++ collect_variables(right)
  end

  defp collect_variables({:union, left, right}) do
    collect_variables(left) ++ collect_variables(right)
  end

  defp collect_variables({:filter, expr, pattern}) do
    collect_variables(pattern) ++ collect_expr_variables(expr)
  end

  defp collect_variables({:extend, pattern, var, expr}) do
    [var | collect_variables(pattern)] ++ collect_expr_variables(expr)
  end

  defp collect_variables({:group, pattern, group_vars, aggregates}) do
    vars = collect_variables(pattern)
    group_var_terms = Enum.map(group_vars, fn {:variable, name} -> {:variable, name} end)

    agg_vars =
      Enum.flat_map(aggregates, fn {var, _agg} ->
        [var]
      end)

    vars ++ group_var_terms ++ agg_vars
  end

  defp collect_variables({:project, pattern, projection}) do
    project_vars = Enum.map(projection, fn {:variable, name} -> {:variable, name} end)
    collect_variables(pattern) ++ project_vars
  end

  defp collect_variables({:distinct, pattern}), do: collect_variables(pattern)
  defp collect_variables({:reduced, pattern}), do: collect_variables(pattern)

  defp collect_variables({:order_by, pattern, conditions}) do
    order_vars = Enum.flat_map(conditions, fn {_dir, expr} -> collect_expr_variables(expr) end)
    collect_variables(pattern) ++ order_vars
  end

  defp collect_variables({:slice, pattern, _offset, _limit}), do: collect_variables(pattern)

  defp collect_variables({:values, variables, _data}) do
    variables
  end

  defp collect_variables({:graph, graph_term, pattern}) do
    vars = collect_variables(pattern)
    if variable?(graph_term), do: [graph_term | vars], else: vars
  end

  defp collect_variables({:service, endpoint, pattern, _silent}) do
    vars = collect_variables(pattern)
    if variable?(endpoint), do: [endpoint | vars], else: vars
  end

  defp collect_variables({:path, subject, _path, object}) do
    Enum.filter([subject, object], &variable?/1)
  end

  defp collect_variables(_), do: []

  defp collect_expr_variables({:variable, _} = var), do: [var]
  defp collect_expr_variables(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> tl()
    |> Enum.flat_map(&collect_expr_variables/1)
  end
  defp collect_expr_variables(_), do: []

  defp variable?({:variable, _}), do: true
  defp variable?(_), do: false

  @doc """
  Returns the child nodes of an algebra node.

  ## Examples

      iex> left = Algebra.bgp([])
      iex> right = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> join = Algebra.join(left, right)
      iex> Algebra.children(join)
      [{:bgp, []}, {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}]

  """
  @spec children(t()) :: [t()]
  def children({:bgp, _}), do: []
  def children({:join, left, right}), do: [left, right]
  def children({:left_join, left, right, _}), do: [left, right]
  def children({:minus, left, right}), do: [left, right]
  def children({:union, left, right}), do: [left, right]
  def children({:filter, _expr, pattern}), do: [pattern]
  def children({:extend, pattern, _var, _expr}), do: [pattern]
  def children({:group, pattern, _vars, _aggs}), do: [pattern]
  def children({:project, pattern, _vars}), do: [pattern]
  def children({:distinct, pattern}), do: [pattern]
  def children({:reduced, pattern}), do: [pattern]
  def children({:order_by, pattern, _conds}), do: [pattern]
  def children({:slice, pattern, _offset, _limit}), do: [pattern]
  def children({:values, _vars, _data}), do: []
  def children({:graph, _term, pattern}), do: [pattern]
  def children({:service, _endpoint, pattern, _silent}), do: [pattern]
  def children({:path, _s, _path, _o}), do: []

  @doc """
  Transforms an algebra tree using a transformation function.

  The function is applied to each node bottom-up (children first, then parent).

  ## Examples

      # Count all nodes
      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      iex> count = Algebra.fold(filter, 0, fn _node, acc -> acc + 1 end)
      iex> count
      2

  """
  @spec fold(t(), acc, (t(), acc -> acc)) :: acc when acc: term()
  def fold(node, acc, fun) do
    children_acc =
      node
      |> children()
      |> Enum.reduce(acc, fn child, a -> fold(child, a, fun) end)

    fun.(node, children_acc)
  end

  @doc """
  Maps a transformation function over an algebra tree.

  The function is applied bottom-up (children first, then parent).
  Each node is replaced by the function's return value.

  ## Examples

      # Replace all BGPs with empty BGPs
      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      iex> result = Algebra.map(filter, fn
      ...>   {:bgp, _} -> {:bgp, []}
      ...>   node -> node
      ...> end)
      iex> result
      {:filter, {:bound, {:variable, "s"}}, {:bgp, []}}

  """
  @spec map(t(), (t() -> t())) :: t()
  def map(node, fun) do
    mapped_children =
      node
      |> children()
      |> Enum.map(fn child -> map(child, fun) end)

    rebuilt = rebuild_with_children(node, mapped_children)
    fun.(rebuilt)
  end

  defp rebuild_with_children({:bgp, triples}, []), do: {:bgp, triples}
  defp rebuild_with_children({:join, _, _}, [left, right]), do: {:join, left, right}

  defp rebuild_with_children({:left_join, _, _, filter}, [left, right]),
    do: {:left_join, left, right, filter}

  defp rebuild_with_children({:minus, _, _}, [left, right]), do: {:minus, left, right}
  defp rebuild_with_children({:union, _, _}, [left, right]), do: {:union, left, right}
  defp rebuild_with_children({:filter, expr, _}, [pattern]), do: {:filter, expr, pattern}

  defp rebuild_with_children({:extend, _, var, expr}, [pattern]),
    do: {:extend, pattern, var, expr}

  defp rebuild_with_children({:group, _, vars, aggs}, [pattern]), do: {:group, pattern, vars, aggs}
  defp rebuild_with_children({:project, _, vars}, [pattern]), do: {:project, pattern, vars}
  defp rebuild_with_children({:distinct, _}, [pattern]), do: {:distinct, pattern}
  defp rebuild_with_children({:reduced, _}, [pattern]), do: {:reduced, pattern}
  defp rebuild_with_children({:order_by, _, conds}, [pattern]), do: {:order_by, pattern, conds}

  defp rebuild_with_children({:slice, _, offset, limit}, [pattern]),
    do: {:slice, pattern, offset, limit}

  defp rebuild_with_children({:values, vars, data}, []), do: {:values, vars, data}

  defp rebuild_with_children({:graph, term, _}, [pattern]),
    do: {:graph, term, pattern}

  defp rebuild_with_children({:service, endpoint, _, silent}, [pattern]),
    do: {:service, endpoint, pattern, silent}

  defp rebuild_with_children({:path, s, path, o}, []), do: {:path, s, path, o}

  # ===========================================================================
  # Validation
  # ===========================================================================

  @doc """
  Validates an algebra node structure.

  Returns `:ok` if the node is well-formed, or `{:error, reason}` otherwise.

  ## Examples

      iex> Algebra.validate({:bgp, []})
      :ok

      iex> Algebra.validate({:bgp, "not a list"})
      {:error, "BGP patterns must be a list"}

  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate({:bgp, patterns}) when is_list(patterns) do
    validate_all(patterns, &validate_triple/1)
  end

  def validate({:bgp, _}), do: {:error, "BGP patterns must be a list"}

  def validate({:join, left, right}), do: validate_binary(left, right)
  def validate({:left_join, left, right, _filter}), do: validate_binary(left, right)
  def validate({:minus, left, right}), do: validate_binary(left, right)
  def validate({:union, left, right}), do: validate_binary(left, right)

  def validate({:filter, _expr, pattern}), do: validate(pattern)
  def validate({:extend, pattern, _var, _expr}), do: validate(pattern)
  def validate({:group, pattern, _vars, _aggs}), do: validate(pattern)
  def validate({:project, pattern, _vars}), do: validate(pattern)
  def validate({:distinct, pattern}), do: validate(pattern)
  def validate({:reduced, pattern}), do: validate(pattern)
  def validate({:order_by, pattern, _conds}), do: validate(pattern)

  def validate({:slice, pattern, offset, limit})
      when is_integer(offset) and offset >= 0 and
             (is_integer(limit) and limit >= 0 or limit == :infinity) do
    validate(pattern)
  end

  def validate({:slice, _, _, _}), do: {:error, "Invalid slice offset/limit"}

  def validate({:values, vars, data}) when is_list(vars) and is_list(data), do: :ok
  def validate({:values, _, _}), do: {:error, "VALUES variables and data must be lists"}

  def validate({:graph, _term, pattern}), do: validate(pattern)
  def validate({:service, _endpoint, pattern, silent}) when is_boolean(silent), do: validate(pattern)
  def validate({:service, _, _, _}), do: {:error, "SERVICE silent flag must be boolean"}

  def validate({:path, _s, _path, _o}), do: :ok

  def validate(node) when is_tuple(node) do
    {:error, "Unknown algebra node type: #{inspect(elem(node, 0))}"}
  end

  def validate(_), do: {:error, "Algebra node must be a tuple"}

  defp validate_binary(left, right) do
    case validate(left) do
      :ok -> validate(right)
      error -> error
    end
  end

  defp validate_triple({:triple, s, p, o}) do
    case validate_term(s) do
      :ok ->
        case validate_term(p) do
          :ok -> validate_term(o)
          error -> error
        end

      error ->
        error
    end
  end

  defp validate_triple(other), do: {:error, "Invalid triple: #{inspect(other)}"}

  defp validate_term({:variable, name}) when is_binary(name), do: :ok
  defp validate_term({:named_node, iri}) when is_binary(iri), do: :ok
  defp validate_term({:blank_node, id}) when is_binary(id), do: :ok
  defp validate_term({:literal, :simple, value}) when is_binary(value), do: :ok
  defp validate_term({:literal, :lang, value, lang}) when is_binary(value) and is_binary(lang), do: :ok
  defp validate_term({:literal, :typed, value, dt}) when is_binary(value) and is_binary(dt), do: :ok
  defp validate_term(term), do: {:error, "Invalid term: #{inspect(term)}"}

  defp validate_all([], _validator), do: :ok

  defp validate_all([head | tail], validator) do
    case validator.(head) do
      :ok -> validate_all(tail, validator)
      error -> error
    end
  end

  # ===========================================================================
  # Pretty Printing
  # ===========================================================================

  @doc """
  Returns a human-readable string representation of an algebra node.

  ## Examples

      iex> bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      iex> Algebra.to_string(bgp)
      "BGP([?s ?p ?o])"

  """
  @spec to_string(t()) :: String.t()
  def to_string(node) do
    format(node, 0)
  end

  defp format({:bgp, triples}, _indent) do
    patterns = Enum.map_join(triples, ", ", &format_triple/1)
    "BGP([#{patterns}])"
  end

  defp format({:join, left, right}, indent) do
    "Join(\n#{indent(indent + 2)}#{format(left, indent + 2)},\n#{indent(indent + 2)}#{format(right, indent + 2)})"
  end

  defp format({:left_join, left, right, nil}, indent) do
    "LeftJoin(\n#{indent(indent + 2)}#{format(left, indent + 2)},\n#{indent(indent + 2)}#{format(right, indent + 2)})"
  end

  defp format({:left_join, left, right, filter}, indent) do
    "LeftJoin(\n#{indent(indent + 2)}#{format(left, indent + 2)},\n#{indent(indent + 2)}#{format(right, indent + 2)},\n#{indent(indent + 2)}#{inspect(filter)})"
  end

  defp format({:minus, left, right}, indent) do
    "Minus(\n#{indent(indent + 2)}#{format(left, indent + 2)},\n#{indent(indent + 2)}#{format(right, indent + 2)})"
  end

  defp format({:union, left, right}, indent) do
    "Union(\n#{indent(indent + 2)}#{format(left, indent + 2)},\n#{indent(indent + 2)}#{format(right, indent + 2)})"
  end

  defp format({:filter, expr, pattern}, indent) do
    "Filter(#{inspect(expr)},\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:extend, pattern, var, expr}, indent) do
    "Extend(#{format_term(var)} = #{inspect(expr)},\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:group, pattern, vars, aggs}, indent) do
    var_str = Enum.map_join(vars, ", ", fn {:variable, n} -> "?#{n}" end)
    agg_str = Enum.map_join(aggs, ", ", fn {v, a} -> "#{format_term(v)} = #{inspect(a)}" end)
    "Group([#{var_str}], [#{agg_str}],\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:project, pattern, vars}, indent) do
    var_str = Enum.map_join(vars, ", ", fn {:variable, n} -> "?#{n}" end)
    "Project([#{var_str}],\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:distinct, pattern}, indent) do
    "Distinct(\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:reduced, pattern}, indent) do
    "Reduced(\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:order_by, pattern, conds}, indent) do
    cond_str = Enum.map_join(conds, ", ", fn {dir, expr} -> "#{dir}(#{inspect(expr)})" end)
    "OrderBy([#{cond_str}],\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:slice, pattern, offset, limit}, indent) do
    "Slice(#{offset}, #{limit},\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:values, vars, _data}, _indent) do
    var_str = Enum.map_join(vars, ", ", &format_term/1)
    "Values([#{var_str}], ...)"
  end

  defp format({:graph, term, pattern}, indent) do
    "Graph(#{format_term(term)},\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:service, endpoint, pattern, silent}, indent) do
    silent_str = if silent, do: " SILENT", else: ""
    "Service#{silent_str}(#{format_term(endpoint)},\n#{indent(indent + 2)}#{format(pattern, indent + 2)})"
  end

  defp format({:path, s, path, o}, _indent) do
    "Path(#{format_term(s)}, #{inspect(path)}, #{format_term(o)})"
  end

  defp format_triple({:triple, s, p, o}) do
    "#{format_term(s)} #{format_term(p)} #{format_term(o)}"
  end

  defp format_term({:variable, name}), do: "?#{name}"
  defp format_term({:named_node, iri}), do: "<#{iri}>"
  defp format_term({:blank_node, id}), do: "_:#{id}"
  defp format_term({:literal, :simple, value}), do: "\"#{value}\""
  defp format_term({:literal, :lang, value, lang}), do: "\"#{value}\"@#{lang}"
  defp format_term({:literal, :typed, value, dt}), do: "\"#{value}\"^^<#{dt}>"
  defp format_term(other), do: inspect(other)

  defp indent(n), do: String.duplicate(" ", n)
end
