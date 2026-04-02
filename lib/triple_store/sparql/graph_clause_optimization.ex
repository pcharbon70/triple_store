defmodule TripleStore.SPARQL.GraphClauseOptimization do
  @moduledoc """
  GRAPH clause optimization for SPARQL queries.

  This module optimizes SPARQL GRAPH clauses by:
  1. Detecting static vs variable GRAPH clauses
  2. Applying appropriate execution strategies based on pattern complexity
  3. Using graph-prefixed iterators for static graphs when beneficial

  ## Strategies

  ### Static Graph (GRAPH <iri> { ... })

  - If pattern has 1 variable: Use graph-prefixed iterator (single iterator with graph bound)
  - If pattern has 2+ variables: Consider multi-iterator with graph as bound component

  ### Variable Graph (GRAPH ?g { ... })

  - If pattern has 3 other variables: Use 4-iterator join for full enumeration
  - If pattern has fewer variables: Use sequential graph iteration

  ## Examples

      # Static graph with single variable - use graph-prefixed iterator
      GRAPH <http://example.org/graph1> { ?s a :Type }
      => Strategy: :graph_prefixed_single_iterator

      # Variable graph with 3 other variables - use multi-iterator
      GRAPH ?g { ?s ?p ?o }
      => Strategy: :four_iterator_enumeration
  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @type graph_clause :: :static | :variable
  @type strategy :: :graph_prefixed_single_iterator | :four_iterator_enumeration
                 | :sequential_graph_iteration | :direct_lookup
                 | :multi_iterator_with_bound_graph
  @type analysis :: %{
                 graph_type: graph_clause(),
                 recommended_strategy: strategy(),
                 reason: String.t(),
                 optimization_hints: keyword()
               }

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Minimum number of variables (excluding graph) to consider multi-iterator
  @multi_iterator_threshold 2

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Analyzes a GRAPH clause and determines optimal execution strategy.

  Takes the pattern inside the GRAPH clause and the graph term (either a
  specific IRI or a variable), then returns analysis with the recommended
  execution strategy.

  ## Parameters

  - `pattern` - The SPARQL algebra pattern inside the GRAPH clause
  - `graph_term` - Either `{:named_node, iri}` for static graphs or `{:variable, name}` for variable graphs
  - `stats` - Optional statistics map for cardinality estimation

  ## Returns

  An analysis map with:
  - `:graph_type` - `:static` or `:variable`
  - `:recommended_strategy` - The optimal execution strategy
  - `:reason` - Human-readable explanation
  - `:optimization_hints` - Hints for the executor

  ## Examples

      iex> analyze_graph_clause({:bgp, [...]}, {:named_node, "http://example.org/g"})
      %{
        graph_type: :static,
        recommended_strategy: :graph_prefixed_single_iterator,
        reason: "Static graph with 1 variable - use graph-prefixed iterator",
        optimization_hints: [bind_graph: true]
      }

      iex> analyze_graph_clause({:bgp, [...]}, {:variable, "g"})
      %{
        graph_type: :variable,
        recommended_strategy: :four_iterator_enumeration,
        reason: "Variable graph with 3 other variables - use 4-iterator join",
        optimization_hints: [use_multi_iterator: true]
      }
  """
  @spec analyze_graph_clause(term(), term(), map()) :: analysis()
  def analyze_graph_clause(pattern, graph_term, _stats \\ %{}) do
    graph_type = determine_graph_type(graph_term)
    variable_count = count_variables_in_pattern(pattern)

    cond do
      # Fully-bound pattern - direct lookup
      variable_count == 0 ->
        %{
          graph_type: graph_type,
          recommended_strategy: :direct_lookup,
          reason: "Pattern is fully-bound - use direct lookup",
          optimization_hints: [use_direct_lookup: true]
        }

      # Static graph with single variable - graph-prefixed iterator
      graph_type == :static and variable_count == 1 ->
        %{
          graph_type: :static,
          recommended_strategy: :graph_prefixed_single_iterator,
          reason: "Static graph with single variable - use graph-prefixed iterator",
          optimization_hints: [bind_graph: true, reduce_iterator_count: true]
        }

      # Static graph with 2+ variables - multi-iterator with bound graph
      graph_type == :static and variable_count >= @multi_iterator_threshold ->
        %{
          graph_type: :static,
          recommended_strategy: :multi_iterator_with_bound_graph,
          reason: "Static graph with #{variable_count} variables - use multi-iterator with graph bound",
          optimization_hints: [use_multi_iterator: true, bind_graph: true]
        }

      # Variable graph with 3 other variables - 4-iterator enumeration
      graph_type == :variable and variable_count >= 3 ->
        %{
          graph_type: :variable,
          recommended_strategy: :four_iterator_enumeration,
          reason: "Variable graph with #{variable_count} other variables - use 4-iterator join for full enumeration",
          optimization_hints: [use_multi_iterator: true, enumerate_all_graphs: true]
        }

      # Variable graph with fewer variables - sequential graph iteration
      graph_type == :variable ->
        %{
          graph_type: :variable,
          recommended_strategy: :sequential_graph_iteration,
          reason: "Variable graph with #{variable_count} other variables - use sequential graph iteration",
          optimization_hints: [use_sequential_iteration: true]
        }

      # Default - sequential iteration
      true ->
        %{
          graph_type: graph_type,
          recommended_strategy: :sequential_graph_iteration,
          reason: "Use default sequential graph iteration",
          optimization_hints: []
        }
    end
  end

  @doc """
  Returns true if the GRAPH clause should use multi-iterator execution.

  ## Examples

      iex> should_use_multi_iterator_for_graph?({:variable, "g"}, {:bgp, [...]})
      true

      iex> should_use_multi_iterator_for_graph?({:named_node, "http://example.org/g"}, {:bgp, [...]})
      false
  """
  @spec should_use_multi_iterator_for_graph?(term(), term()) :: boolean()
  def should_use_multi_iterator_for_graph?(graph_term, pattern) do
    analysis = analyze_graph_clause(pattern, graph_term)
    analysis.recommended_strategy in [:four_iterator_enumeration, :multi_iterator_with_bound_graph]
  end

  @doc """
  Applies optimization hints to a pattern for graph execution.

  This function takes a pattern and optimization hints, returning a modified
  pattern with optimizations applied (e.g., graph term pre-bound).

  ## Parameters

  - `pattern` - The original SPARQL pattern
  - `hints` - Optimization hints from analysis

  ## Returns

  A tuple of `{optimized_pattern, graph_binding}` where:
  - `optimized_pattern` - The pattern with optimizations applied
  - `graph_binding` - Pre-bound graph variable if applicable

  ## Examples

      iex> apply_optimization_hints({:bgp, [...]}, [bind_graph: true])
      {{:bgp, [...]}, %{g: graph_id}}
  """
  @spec apply_optimization_hints(term(), keyword()) :: {term(), map() | nil}
  def apply_optimization_hints(pattern, hints) do
    graph_binding =
      if Keyword.get(hints, :bind_graph, false) do
        # Graph should be pre-bound - extract from hints or return empty map
        Keyword.get(hints, :graph_binding, %{})
      else
        nil
      end

    {pattern, graph_binding}
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  # Determine if graph term is static (IRI) or variable
  defp determine_graph_type({:variable, _name}), do: :variable
  defp determine_graph_type({:named_node, _iri}), do: :static
  defp determine_graph_type(_other), do: :static

  # Count variables in a pattern (excluding graph variable)
  defp count_variables_in_pattern(pattern) do
    case pattern do
      {:bgp, triple_patterns} ->
        Enum.reduce(triple_patterns, 0, fn {:triple, s, p, o}, acc ->
          acc + count_component_variables([s, p, o])
        end)

      {:quad, s, p, o, _g} ->
        # Count all non-graph variables (s, p, o only)
        count_component_variables([s, p, o])

      _ ->
        0
    end
  end

  # Count variables in a list of components
  defp count_component_variables(components) do
    Enum.count(components, &is_variable?/1)
  end

  # Check if a term is a variable
  defp is_variable?({:variable, _name}), do: true
  defp is_variable?(_), do: false
end
