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

  ## Usage Examples

      # Match a triple against a rule pattern
      PatternMatcher.matches_triple?({a, b, c}, {:pattern, [s, p, o]})

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

  @typedoc "A ground triple"
  @type triple :: {term(), term(), term()}

  @typedoc "An index pattern element"
  @type index_element :: :var | {:bound, term()}

  @typedoc "An index pattern for database lookups"
  @type index_pattern :: {index_element(), index_element(), index_element()}

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
end
