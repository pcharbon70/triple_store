defmodule TripleStore.SPARQL.QuadPatternRecognition do
  @moduledoc """
  Pattern recognition module for detecting when quad patterns benefit from multi-iterator joins.

  This module analyzes SPARQL quad patterns and determines whether they should
  use the multi-iterator QuadLeapfrog approach or the single-iterator approach
  based on pattern complexity and cost estimates.

  ## Decision Criteria

  A quad pattern will use multi-iterator joins when:
  1. Has 2 or more unbound variables (benefits from coordinated iteration)
  2. Estimated cardinality is above threshold (justifies coordination overhead)
  3. Pattern is compatible with multi-iterator execution

  Otherwise, use the existing single-iterator approach.

  ## Examples

      # Simple pattern with 1 variable - use single iterator
      {:quad, {:variable, "s"}, 42, 35, 0}
      => :use_single_iterator

      # Complex pattern with 4 variables - use multi-iterator
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      => :use_multi_iterator
  """

  alias TripleStore.SPARQL.{Cardinality, QuadCardinality}

  # ===========================================================================
  # Types
  # ===========================================================================

  @type quad_pattern :: {:quad, term(), term(), term(), term()}
  @type component :: {:bound, term()} | {:variable, String.t()}
  @type decision :: :use_single_iterator | :use_multi_iterator
  @type analysis :: %{
                 decision: decision(),
                 reason: String.t(),
                 variable_count: non_neg_integer(),
                 estimated_cardinality: float(),
                 recommended_approach: atom()
               }

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Minimum number of unbound variables to consider multi-iterator
  @multi_iterator_threshold 2

  # Maximum cardinality for single-iterator approach (above this, consider multi-iterator)
  @max_single_iterator_cardinality 100_000

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Analyzes a quad pattern and determines the optimal execution strategy.

  Returns an analysis map with:
  - `:decision` - `:use_single_iterator` or `:use_multi_iterator`
  - `:reason` - Human-readable explanation
  - `:variable_count` - Number of unbound variables
  - `:estimated_cardinality` - Estimated result cardinality
  - `:recommended_approach` - Specific approach to use

  ## Examples

      iex> analyze_quad_pattern({:quad, {:variable, "s"}, 42, 35, 0}, %{})
      %{
        decision: :use_single_iterator,
        reason: "Pattern has only 1 unbound variable",
        variable_count: 1,
        estimated_cardinality: 1.0,
        recommended_approach: :direct_lookup
      }

      iex> analyze_quad_pattern({:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}, stats)
      %{
        decision: :use_multi_iterator,
        reason: "Pattern has 4 unbound variables and benefits from coordinated iteration",
        variable_count: 4,
        estimated_cardinality: estimated,
        recommended_approach: :quad_leapfrog
      }
  """
  @spec analyze_quad_pattern(quad_pattern(), Cardinality.stats()) :: analysis()
  def analyze_quad_pattern(pattern, stats \\ %{})

  def analyze_quad_pattern({:quad, s, p, o, g} = pattern, stats) do
    components = [s, p, o, g]
    variable_count = count_variables(components)

    # Estimate cardinality for this pattern
    estimated_card = estimate_cardinality(pattern, stats)

    # Make decision based on variable count and cardinality
    decision =
      cond do
        # Fully-bound pattern - use direct lookup
        variable_count == 0 ->
          %{
            decision: :use_single_iterator,
            reason: "Pattern is fully-bound - use direct lookup",
            variable_count: 0,
            estimated_cardinality: 1.0,
            recommended_approach: :direct_lookup
          }

        # Single variable - use single iterator with prefix scan
        variable_count == 1 ->
          %{
            decision: :use_single_iterator,
            reason: "Pattern has only 1 unbound variable - single iterator is optimal",
            variable_count: 1,
            estimated_cardinality: estimated_card,
            recommended_approach: :prefix_scan
          }

        # Multiple variables with high cardinality - use multi-iterator
        variable_count >= @multi_iterator_threshold and
          estimated_card > @max_single_iterator_cardinality ->
          %{
            decision: :use_multi_iterator,
            reason:
              "Pattern has #{variable_count} unbound variables with high cardinality (#{ trunc_float(estimated_card) }) - multi-iterator leapfrog is optimal",
            variable_count: variable_count,
            estimated_cardinality: estimated_card,
            recommended_approach: :quad_leapfrog
          }

        # Multiple variables but low cardinality - could use either
        variable_count >= @multi_iterator_threshold ->
          %{
            decision: :use_multi_iterator,
            reason:
              "Pattern has #{variable_count} unbound variables - multi-iterator provides worst-case optimal performance",
            variable_count: variable_count,
            estimated_cardinality: estimated_card,
            recommended_approach: :quad_leapfrog
          }

        # Default to single iterator
        true ->
          %{
            decision: :use_single_iterator,
            reason: "Pattern complexity doesn't justify multi-iterator overhead",
            variable_count: variable_count,
            estimated_cardinality: estimated_card,
            recommended_approach: :prefix_scan
          }
      end

    decision
  end

  @doc """
  Returns true if the pattern should use multi-iterator execution.

  This is a convenience function that returns a boolean decision.

  ## Examples

      iex> should_use_multi_iterator?({:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}})
      true

      iex> should_use_multi_iterator?({:quad, {:variable, "s"}, 42, 35, 0})
      false
  """
  @spec should_use_multi_iterator?(quad_pattern()) :: boolean()
  def should_use_multi_iterator?(pattern) do
    should_use_multi_iterator?(pattern, %{})
  end

  @spec should_use_multi_iterator?(quad_pattern(), Cardinality.stats()) :: boolean()
  def should_use_multi_iterator?(pattern, stats) do
    analyze_quad_pattern(pattern, stats).decision == :use_multi_iterator
  end

  @doc """
  Counts the number of unbound variables in a quad pattern.

  ## Examples

      iex> count_variables({:quad, {:variable, "s"}, 42, {:variable, "o"}, 0})
      2
  """
  @spec count_variables([term()]) :: non_neg_integer()
  def count_variables(components) when is_list(components) do
    Enum.count(components, fn
      {:variable, _} -> true
      _ -> false
    end)
  end

  @doc """
  Translates a SPARQL quad pattern to a QuadLeapfrog-compatible pattern.

  This converts SPARQL term representations to the format expected by
  QuadLeapfrog, handling:
  - Variables → {:variable, name}
  - Bound values → {:bound, id}
  - Default graph handling

  ## Examples

      iex> translate_to_leapfrog_pattern({:quad, {:variable, "s"}, 42, {:variable, "o"}, 0})
      {:quad, {:variable, "s"}, {:bound, 42}, {:variable, "o"}, {:bound, 0}}
  """
  @spec translate_to_leapfrog_pattern(quad_pattern()) :: quad_pattern()
  def translate_to_leapfrog_pattern({:quad, s, p, o, g}) do
    {:quad, translate_component(s), translate_component(p), translate_component(o),
     translate_component(g)}
  end

  @doc """
  Extracts variable names from a quad pattern.

  Returns a list of variable names in order [s, p, o, g].

  ## Examples

      iex> extract_variables({:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}})
      ["s", nil, "o", "g"]
  """
  @spec extract_variables(quad_pattern()) :: [String.t() | nil]
  def extract_variables({:quad, s, p, o, g}) do
    [
      var_name(s),
      var_name(p),
      var_name(o),
      var_name(g)
    ]
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  # Estimate cardinality for a quad pattern
  defp estimate_cardinality(pattern, stats) do
    case QuadCardinality.estimate_pattern(pattern, stats) do
      {:ok, card} -> card
      _ -> 1_000.0  # Default estimate when stats unavailable
    end
  rescue
    _ -> 1_000.0  # Fallback estimate
  end

  # Truncate float to integer for display
  defp trunc_float(float) when is_float(float), do: Kernel.trunc(float)
  defp trunc_float(int), do: int

  # Translate a single component to Leapfrog format
  defp translate_component({:variable, _name} = var), do: var
  defp translate_component({:bound, _} = bound), do: bound
  defp translate_component(value) when is_integer(value), do: {:bound, value}
  defp translate_component(value) when is_atom(value), do: {:bound, value}
  defp translate_component(_other), do: {:bound, nil}

  # Extract variable name from component, or nil if bound
  defp var_name({:variable, name}), do: name
  defp var_name(_), do: nil

  # Truncate float to integer for display (private helper)
  # defp trunc(float) when is_float(float), do: trunc(float)
  # defp trunc(int), do: int
end
