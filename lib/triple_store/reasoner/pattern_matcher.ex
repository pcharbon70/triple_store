defmodule TripleStore.Reasoner.PatternMatcher do
  @moduledoc """
  Shared pattern matching utilities for reasoning operations.

  This module provides pattern matching functions used across the reasoning
  subsystem, including delta computation, semi-naive evaluation, and derived
  fact storage.

  ## Pattern Formats

  Two pattern formats are supported:

  1. **Rule patterns**: Used in rule definitions and delta computation
     - Variables: `{:var, name}` where name is an atom
     - Constants: Any other term

  2. **Index patterns**: Used in database lookups
     - Variables: `:var`
     - Bound values: `{:bound, value}`

  ## Quad Pattern Support

  For quad store reasoning, the module also supports quad patterns with a graph
  component. Quad patterns extend triple patterns with:
  - Graph variables: `{:var, name}` for matching any graph
  - Bound graphs: `{:bound, graph_id}` for specific graph IDs
  - `:default` - Default graph (ID = 0)
  - `:all` - All graphs (for global reasoning)

  ## Usage Examples

      # Match a triple against a rule pattern
      PatternMatcher.matches_triple?({a, b, c}, {:pattern, [s, p, o]})

      # Match a quad against a quad pattern
      PatternMatcher.matches_quad?({g, a, b, c}, {:quad_pattern, [g_pat, s, p, o]})

      # Check if a term matches a pattern element
      PatternMatcher.matches_term?(some_value, {:var, :x})  # true
      PatternMatcher.matches_term?(some_value, some_value)   # true
      PatternMatcher.matches_term?(some_value, other_value)  # false
  """

  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A rule term - either a variable or a constant"
  @type rule_term :: Rule.rule_term()

  @typedoc "A triple pattern for matching"
  @type pattern :: {:pattern, [rule_term()]}

  @typedoc "A quad pattern for graph-aware matching"
  @type quad_pattern :: {:quad_pattern, list()}

  @typedoc "A ground triple"
  @type triple :: {term(), term(), term()}

  @typedoc "A ground quad (graph_id, subject, predicate, object)"
  @type quad :: {term(), term(), term(), term()}

  @typedoc "An index pattern element"
  @type index_element :: :var | {:bound, term()}

  @typedoc "An index pattern for database lookups"
  @type index_pattern :: {index_element(), index_element(), index_element()}

  @typedoc "A quad index pattern for database lookups"
  @type quad_index_pattern :: {index_element(), index_element(), index_element(), index_element()}

  @typedoc "Graph term in a quad pattern"
  @type graph_term :: {:var, atom()} | {:bound, non_neg_integer()} | :default | :all

  # ============================================================================
  # Rule Pattern Matching
  # ============================================================================

  @doc """
  Checks if a term matches a pattern element using rule pattern format.

  Variables (`{:var, _}`) match any term. All other patterns require exact match.

  ## Examples

      iex> PatternMatcher.matches_term?(:foo, {:var, :x})
      true

      iex> PatternMatcher.matches_term?(:foo, :foo)
      true

      iex> PatternMatcher.matches_term?(:foo, :bar)
      false
  """
  @spec matches_term?(term(), rule_term()) :: boolean()
  def matches_term?(_fact_term, {:var, _}), do: true
  def matches_term?(fact_term, {:const, value}), do: fact_term == value
  def matches_term?(fact_term, pattern_term), do: fact_term == pattern_term

  @doc """
  Checks if a triple matches a rule pattern.

  ## Examples

      iex> pattern = {:pattern, [{:var, :s}, {:iri, "p"}, {:var, :o}]}
      iex> PatternMatcher.matches_triple?({:a, {:iri, "p"}, :b}, pattern)
      true

      iex> PatternMatcher.matches_triple?({:a, {:iri, "q"}, :b}, pattern)
      false
  """
  @spec matches_triple?(triple(), pattern()) :: boolean()
  def matches_triple?({fs, fp, fo}, {:pattern, [ps, pp, po]}) do
    matches_term?(fs, ps) and matches_term?(fp, pp) and matches_term?(fo, po)
  end

  @doc """
  Filters a set of facts to those matching a rule pattern.

  ## Examples

      iex> facts = MapSet.new([{:a, :p, :b}, {:c, :p, :d}, {:e, :q, :f}])
      iex> pattern = {:pattern, [{:var, :s}, :p, {:var, :o}]}
      iex> PatternMatcher.filter_matching(facts, pattern)
      [{:a, :p, :b}, {:c, :p, :d}]
  """
  @spec filter_matching(Enumerable.t(), pattern()) :: [triple()]
  def filter_matching(facts, {:pattern, [ps, pp, po]} = _pattern) do
    Enum.filter(facts, fn {fs, fp, fo} ->
      matches_term?(fs, ps) and matches_term?(fp, pp) and matches_term?(fo, po)
    end)
  end

  # ============================================================================
  # Quad Pattern Matching
  # ============================================================================

  @doc """
  Checks if a graph term matches a quad pattern graph element.

  Graph patterns can be:
  - `{:var, name}` - Matches any graph ID
  - `{:bound, graph_id}` - Matches only the specific graph ID
  - `:default` - Matches graph ID 0 (default graph)
  - `:all` - Matches any graph (used for global reasoning)

  ## Examples

      iex> PatternMatcher.matches_graph_term?(1, {:var, :g})
      true

      iex> PatternMatcher.matches_graph_term?(1, {:bound, 1})
      true

      iex> PatternMatcher.matches_graph_term?(1, {:bound, 2})
      false

      iex> PatternMatcher.matches_graph_term?(0, :default)
      true

      iex> PatternMatcher.matches_graph_term?(5, :default)
      false

      iex> PatternMatcher.matches_graph_term?(1, :all)
      true
  """
  @spec matches_graph_term?(non_neg_integer(), graph_term()) :: boolean()
  def matches_graph_term?(_graph_id, {:var, _name}), do: true
  def matches_graph_term?(graph_id, {:bound, graph_id}), do: true
  def matches_graph_term?(_graph_id, {:bound, _other_id}), do: false
  def matches_graph_term?(0, :default), do: true
  def matches_graph_term?(_graph_id, :default), do: false
  def matches_graph_term?(_graph_id, :all), do: true

  @doc """
  Checks if a quad matches a quad pattern.

  ## Examples

      iex> pattern = {:quad_pattern, [{:var, :g}, {:var, :s}, :p, {:var, :o}]}
      iex> PatternMatcher.matches_quad?({1, :a, :p, :b}, pattern)
      true

      iex> PatternMatcher.matches_quad?({2, :a, :q, :b}, pattern)
      false

      iex> pattern = {:quad_pattern, [{:bound, 1}, {:var, :s}, :p, {:var, :o}]}
      iex> PatternMatcher.matches_quad?({1, :a, :p, :b}, pattern)
      true

      iex> PatternMatcher.matches_quad?({2, :a, :p, :b}, pattern)
      false
  """
  @spec matches_quad?(quad(), quad_pattern()) :: boolean()
  def matches_quad?({fg, fs, fp, fo}, {:quad_pattern, [pg, ps, pp, po]}) do
    matches_graph_term?(fg, pg) and
      matches_term?(fs, ps) and
      matches_term?(fp, pp) and
      matches_term?(fo, po)
  end

  @doc """
  Filters a set of quads to those matching a quad pattern.

  ## Examples

      iex> quads = [{1, :a, :p, :b}, {2, :c, :p, :d}, {1, :e, :q, :f}]
      iex> pattern = {:quad_pattern, [{:bound, 1}, {:var, :s}, :p, {:var, :o}]}
      iex> PatternMatcher.filter_matching_quads(quads, pattern)
      [{1, :a, :p, :b}]
  """
  @spec filter_matching_quads(Enumerable.t(), quad_pattern()) :: [quad()]
  def filter_matching_quads(quads, {:quad_pattern, [pg, ps, pp, po]} = _pattern) do
    Enum.filter(quads, fn {fg, fs, fp, fo} ->
      matches_graph_term?(fg, pg) and
        matches_term?(fs, ps) and
        matches_term?(fp, pp) and
        matches_term?(fo, po)
    end)
  end

  @doc """
  Checks if a quad matches an index pattern.

  ## Examples

      iex> pattern = {:bound, :var, {:bound, :p}, :var}
      iex> PatternMatcher.matches_quad_index_pattern?({1, :a, :p, :b}, pattern)
      true

      iex> PatternMatcher.matches_quad_index_pattern?({1, :a, :q, :b}, pattern)
      false
  """
  @spec matches_quad_index_pattern?(quad(), quad_index_pattern()) :: boolean()
  def matches_quad_index_pattern?({g, s, p, o}, {g_pat, s_pat, p_pat, o_pat}) do
    matches_index_element?(g, g_pat) and
      matches_index_element?(s, s_pat) and
      matches_index_element?(p, p_pat) and
      matches_index_element?(o, o_pat)
  end

  # ============================================================================
  # Index Pattern Matching
  # ============================================================================

  @doc """
  Checks if a term matches an index pattern element.

  The `:var` element matches any term. `{:bound, value}` requires exact match.

  ## Examples

      iex> PatternMatcher.matches_index_element?(:foo, :var)
      true

      iex> PatternMatcher.matches_index_element?(:foo, {:bound, :foo})
      true

      iex> PatternMatcher.matches_index_element?(:foo, {:bound, :bar})
      false
  """
  @spec matches_index_element?(term(), index_element()) :: boolean()
  def matches_index_element?(_value, :var), do: true
  def matches_index_element?(value, {:bound, expected}), do: value == expected

  @doc """
  Checks if a triple matches an index pattern.

  ## Examples

      iex> pattern = {:var, {:bound, :p}, :var}
      iex> PatternMatcher.matches_index_pattern?({:a, :p, :b}, pattern)
      true

      iex> PatternMatcher.matches_index_pattern?({:a, :q, :b}, pattern)
      false
  """
  @spec matches_index_pattern?(triple(), index_pattern()) :: boolean()
  def matches_index_pattern?({s, p, o}, {s_pat, p_pat, o_pat}) do
    matches_index_element?(s, s_pat) and
      matches_index_element?(p, p_pat) and
      matches_index_element?(o, o_pat)
  end

  # ============================================================================
  # Pattern Conversion
  # ============================================================================

  @doc """
  Converts a rule pattern to an index pattern.

  Rule patterns use `{:var, name}` for variables and `{:const, value}` or
  raw values for constants. Index patterns use `:var` and `{:bound, value}`.

  ## Examples

      iex> PatternMatcher.rule_to_index_pattern({:pattern, [{:var, :s}, {:iri, "p"}, {:var, :o}]})
      {:var, {:bound, {:iri, "p"}}, :var}
  """
  @spec rule_to_index_pattern(pattern()) :: index_pattern()
  def rule_to_index_pattern({:pattern, [s, p, o]}) do
    {convert_to_index(s), convert_to_index(p), convert_to_index(o)}
  end

  @doc """
  Converts an index pattern to a rule pattern.

  ## Examples

      iex> PatternMatcher.index_to_rule_pattern({:var, {:bound, :p}, :var})
      {:pattern, [{:var, :_s}, :p, {:var, :_o}]}
  """
  @spec index_to_rule_pattern(index_pattern()) :: pattern()
  def index_to_rule_pattern({s, p, o}) do
    {:pattern, [convert_from_index(s, :s), convert_from_index(p, :p), convert_from_index(o, :o)]}
  end

  @doc """
  Converts a quad rule pattern to a quad index pattern.

  ## Examples

      iex> PatternMatcher.quad_to_index_pattern({:quad_pattern, [{:var, :g}, {:var, :s}, :p, {:var, :o}]})
      {:var, :var, {:bound, :p}, :var}

      iex> PatternMatcher.quad_to_index_pattern({:quad_pattern, [{:bound, 1}, {:var, :s}, :p, {:var, :o}]})
      {{:bound, 1}, :var, {:bound, :p}, :var}
  """
  @spec quad_to_index_pattern(quad_pattern()) :: quad_index_pattern()
  def quad_to_index_pattern({:quad_pattern, [g, s, p, o]}) do
    {convert_graph_to_index(g), convert_to_index(s), convert_to_index(p), convert_to_index(o)}
  end

  @doc """
  Converts a quad index pattern to a quad rule pattern.

  ## Examples

      iex> PatternMatcher.index_to_quad_pattern({:var, :var, {:bound, :p}, :var})
      {:quad_pattern, [{:var, :_g}, {:var, :_s}, :p, {:var, :_o}]}
  """
  @spec index_to_quad_pattern(quad_index_pattern()) :: quad_pattern()
  def index_to_quad_pattern({g, s, p, o}) do
    {:quad_pattern,
     [convert_graph_from_index(g, :g), convert_from_index(s, :s), convert_from_index(p, :p),
      convert_from_index(o, :o)]}
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp convert_to_index({:var, _name}), do: :var
  defp convert_to_index({:const, value}), do: {:bound, value}
  defp convert_to_index(:var), do: :var
  defp convert_to_index({:bound, _} = bound), do: bound
  defp convert_to_index(value), do: {:bound, value}

  defp convert_from_index(:var, name), do: {:var, :"_#{name}"}
  defp convert_from_index({:bound, value}, _name), do: value

  # Graph term conversion for quad patterns
  defp convert_graph_to_index({:var, _name}), do: :var
  defp convert_graph_to_index({:bound, _graph_id} = bound), do: bound
  defp convert_graph_to_index(:default), do: {:bound, 0}
  defp convert_graph_to_index(:all), do: :var

  defp convert_graph_from_index(:var, name), do: {:var, :"_#{name}"}
  defp convert_graph_from_index({:bound, 0}, _name), do: :default
  defp convert_graph_from_index({:bound, graph_id}, _name), do: {:bound, graph_id}

  # ============================================================================
  # Unification and Binding Operations
  # ============================================================================

  @typedoc "Variable bindings map"
  @type bindings :: %{atom() => term()}

  @doc """
  Unifies a concrete term with a pattern term, returning updated bindings.

  - If the pattern is a variable and unbound, binds it to the concrete term
  - If the pattern is a variable and already bound, checks for consistency
  - If the pattern is a constant, checks for equality

  ## Examples

      iex> PatternMatcher.unify_term(:foo, {:var, :x}, %{})
      {:ok, %{x: :foo}}

      iex> PatternMatcher.unify_term(:foo, {:var, :x}, %{x: :foo})
      {:ok, %{x: :foo}}

      iex> PatternMatcher.unify_term(:foo, {:var, :x}, %{x: :bar})
      :no_match

      iex> PatternMatcher.unify_term(:foo, :foo, %{})
      {:ok, %{}}

      iex> PatternMatcher.unify_term(:foo, :bar, %{})
      :no_match
  """
  @spec unify_term(term(), rule_term(), bindings()) :: {:ok, bindings()} | :no_match
  def unify_term(concrete, {:var, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> {:ok, Map.put(bindings, name, concrete)}
      ^concrete -> {:ok, bindings}
      _other -> :no_match
    end
  end

  def unify_term(concrete, pattern, bindings) when concrete == pattern do
    {:ok, bindings}
  end

  def unify_term(_concrete, _pattern, _bindings) do
    :no_match
  end

  @doc """
  Matches a triple against a rule head pattern, returning bindings.

  ## Examples

      iex> PatternMatcher.match_rule_head({:a, :p, :b}, {:pattern, [{:var, :s}, :p, {:var, :o}]})
      {:ok, %{s: :a, o: :b}}

      iex> PatternMatcher.match_rule_head({:a, :q, :b}, {:pattern, [{:var, :s}, :p, {:var, :o}]})
      :no_match
  """
  @spec match_rule_head(triple(), pattern()) :: {:ok, bindings()} | :no_match
  def match_rule_head({s, p, o}, {:pattern, [hs, hp, ho]}) do
    with {:ok, b1} <- unify_term(s, hs, %{}),
         {:ok, b2} <- unify_term(p, hp, b1),
         {:ok, b3} <- unify_term(o, ho, b2) do
      {:ok, b3}
    else
      :no_match -> :no_match
    end
  end

  @doc """
  Substitutes a pattern term with its bound value if available.

  Returns the bound value if the variable is bound, otherwise returns the
  original pattern term.

  ## Examples

      iex> PatternMatcher.substitute_if_bound({:var, :x}, %{x: :foo})
      :foo

      iex> PatternMatcher.substitute_if_bound({:var, :x}, %{})
      {:var, :x}

      iex> PatternMatcher.substitute_if_bound(:constant, %{})
      :constant
  """
  @spec substitute_if_bound(rule_term(), bindings()) :: term()
  def substitute_if_bound({:var, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> {:var, name}
      value -> value
    end
  end

  def substitute_if_bound(term, _bindings), do: term

  @doc """
  Attempts to extend bindings by binding a pattern term to a value.

  Returns the updated bindings if successful, or nil if there's a conflict.

  ## Examples

      iex> PatternMatcher.maybe_bind(%{}, {:var, :x}, :foo)
      %{x: :foo}

      iex> PatternMatcher.maybe_bind(%{x: :foo}, {:var, :x}, :foo)
      %{x: :foo}

      iex> PatternMatcher.maybe_bind(%{x: :foo}, {:var, :x}, :bar)
      nil

      iex> PatternMatcher.maybe_bind(%{}, :foo, :foo)
      %{}

      iex> PatternMatcher.maybe_bind(%{}, :foo, :bar)
      nil
  """
  @spec maybe_bind(bindings(), rule_term(), term()) :: bindings() | nil
  def maybe_bind(bindings, {:var, name}, value) do
    case Map.get(bindings, name) do
      nil -> Map.put(bindings, name, value)
      ^value -> bindings
      _other -> nil
    end
  end

  def maybe_bind(bindings, pattern, value) do
    if pattern == value, do: bindings, else: nil
  end

  # ============================================================================
  # Quad Unification Functions
  # ============================================================================

  @doc """
  Unifies a concrete graph term with a graph pattern term.

  Similar to unify_term/3 but specifically handles graph terms which can be:
  - `{:var, name}` - Variable (binds to any graph ID)
  - `{:bound, graph_id}` - Specific graph ID (must match)
  - `:default` - Default graph (ID = 0)
  - `:all` - Matches any graph (doesn't bind)

  ## Examples

      iex> PatternMatcher.unify_graph_term(1, {:var, :g}, %{})
      {:ok, %{g: 1}}

      iex> PatternMatcher.unify_graph_term(0, :default, %{})
      {:ok, %{}}

      iex> PatternMatcher.unify_graph_term(1, {:bound, 1}, %{})
      {:ok, %{}}

      iex> PatternMatcher.unify_graph_term(1, {:bound, 2}, %{})
      :no_match
  """
  @spec unify_graph_term(non_neg_integer(), graph_term(), bindings()) :: {:ok, bindings()} | :no_match
  def unify_graph_term(graph_id, {:var, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> {:ok, Map.put(bindings, name, graph_id)}
      ^graph_id -> {:ok, bindings}
      _other -> :no_match
    end
  end

  def unify_graph_term(0, :default, bindings), do: {:ok, bindings}
  def unify_graph_term(_graph_id, :default, _bindings), do: :no_match
  def unify_graph_term(graph_id, {:bound, graph_id}, bindings), do: {:ok, bindings}
  def unify_graph_term(_graph_id, {:bound, _other}, _bindings), do: :no_match
  def unify_graph_term(_graph_id, :all, bindings), do: {:ok, bindings}

  @doc """
  Matches a quad against a quad pattern, returning bindings.

  ## Examples

      iex> PatternMatcher.match_quad_pattern({1, :a, :p, :b}, {:quad_pattern, [{:var, :g}, {:var, :s}, :p, {:var, :o}]})
      {:ok, %{g: 1, s: :a, o: :b}}

      iex> PatternMatcher.match_quad_pattern({1, :a, :q, :b}, {:quad_pattern, [{:var, :g}, {:var, :s}, :p, {:var, :o}]})
      :no_match
  """
  @spec match_quad_pattern(quad(), quad_pattern()) :: {:ok, bindings()} | :no_match
  def match_quad_pattern({g, s, p, o}, {:quad_pattern, [pg, ps, pp, po]}) do
    with {:ok, b1} <- unify_graph_term(g, pg, %{}),
         {:ok, b2} <- unify_term(s, ps, b1),
         {:ok, b3} <- unify_term(p, pp, b2),
         {:ok, b4} <- unify_term(o, po, b3) do
      {:ok, b4}
    else
      :no_match -> :no_match
    end
  end

  @doc """
  Matches a quad head pattern for rule evaluation, returning bindings.

  This is similar to match_quad_pattern/2 but specifically for rule heads
  where the graph component may have special handling.
  """
  @spec match_quad_head(quad(), quad_pattern()) :: {:ok, bindings()} | :no_match
  def match_quad_head(quad, quad_pattern), do: match_quad_pattern(quad, quad_pattern)
end
