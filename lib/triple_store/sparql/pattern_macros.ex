defmodule TripleStore.SPARQL.PatternMacros do
  @moduledoc """
  Pattern matching macros and helpers for SPARQL triple/quad patterns (S17).

  This module provides macros and helper functions to reduce repetitive pattern
  matching code across SPARQL modules. It consolidates common operations like:

  - Deconstructing triple and quad patterns
  - Extracting variables from patterns
  - Counting bound terms
  - Type checking patterns

  ## Examples

      # Use defpattern to define a function for both triple and quad patterns
      import TripleStore.SPARQL.PatternMacros

      defpattern my_func(pattern) do
        # pattern is matched as either {:triple, s, p, o} or {:quad, s, p, o, g}
        # terms is [s, p, o] or [s, p, o, g]
        terms
      end

  """

  # ===========================================================================
  # Macros
  # ===========================================================================

  @doc """
  Guard expression to check if a value is a triple pattern.
  Use in function heads: `def foo(arg) when is_triple_pattern(arg)`

  """
  defmacro is_triple_pattern(arg) do
    quote do
      is_tuple(unquote(arg)) and tuple_size(unquote(arg)) == 4 and
        elem(unquote(arg), 0) == :triple
    end
  end

  @doc """
  Guard expression to check if a value is a quad pattern.
  Use in function heads: `def foo(arg) when is_quad_pattern(arg)`

  """
  defmacro is_quad_pattern(arg) do
    quote do
      is_tuple(unquote(arg)) and tuple_size(unquote(arg)) == 5 and
        elem(unquote(arg), 0) == :quad
    end
  end

  @doc """
  Guard expression to check if a value is a triple or quad pattern.
  Use in function heads: `def foo(arg) when is_pattern(arg)`

  """
  defmacro is_pattern(arg) do
    quote do
      is_tuple(unquote(arg)) and
        ((tuple_size(unquote(arg)) == 4 and elem(unquote(arg), 0) == :triple) or
           (tuple_size(unquote(arg)) == 5 and elem(unquote(arg), 0) == :quad))
    end
  end

  @doc """
  Checks if a term is a variable pattern: `{:variable, name}`

  """
  defmacro is_variable_term(arg) do
    quote do
      is_tuple(unquote(arg)) and tuple_size(unquote(arg)) == 2 and
        elem(unquote(arg), 0) == :variable
    end
  end

  @doc """
  Checks if a term is a constant (not a variable).

  """
  defmacro is_constant_term(arg) do
    quote do
      not is_tuple(unquote(arg)) or
        (tuple_size(unquote(arg)) == 2 and elem(unquote(arg), 0) != :variable) or
        (tuple_size(unquote(arg)) > 2 and elem(unquote(arg), 0) != :variable)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  @doc """
  Extracts all terms from a pattern as a list.

  ## Returns

  - `[subject, predicate, object]` for triple patterns
  - `[subject, predicate, object, graph]` for quad patterns
  - `[]` for non-patterns

  """
  def pattern_terms({:triple, s, p, o}), do: [s, p, o]
  def pattern_terms({:quad, s, p, o, g}), do: [s, p, o, g]
  def pattern_terms(_), do: []

  @doc """
  Gets the pattern type: `:triple`, `:quad`, or `:unknown`.

  """
  def pattern_type({:triple, _, _, _}), do: :triple
  def pattern_type({:quad, _, _, _, _}), do: :quad
  def pattern_type(_), do: :unknown

  @doc """
  Gets the arity (number of elements) of a pattern.

  ## Returns

  - `3` for triple patterns (subject, predicate, object)
  - `4` for quad patterns (subject, predicate, object, graph)
  - `0` for non-patterns

  """
  def pattern_arity({:triple, _, _, _}), do: 3
  def pattern_arity({:quad, _, _, _, _}), do: 4
  def pattern_arity(_), do: 0

  @doc """
  Extracts variable names from a pattern term.

  Returns the variable name if the term is `{:variable, name}`, `nil` otherwise.

  """
  def extract_var_name({:variable, name}), do: name
  def extract_var_name(_), do: nil

  @doc """
  Extracts all variable names from a pattern.

  Returns a list of unique variable names found in the pattern.

  ## Examples

      iex> PatternMacros.pattern_variables({:triple, {:variable, "s"}, 10, {:variable, "o"}})
      ["s", "o"]

      iex> PatternMacros.pattern_variables({:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}})
      ["s", "o", "g"]

  """
  def pattern_variables({:triple, s, p, o}) do
    [s, p, o]
    |> Enum.map(&extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  def pattern_variables({:quad, s, p, o, g}) do
    [s, p, o, g]
    |> Enum.map(&extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  def pattern_variables(_), do: []

  @doc """
  Extracts all variable names from a pattern as a MapSet.

  ## Examples

      iex> PatternMacros.pattern_variable_set({:triple, {:variable, "s"}, 10, {:variable, "o"}})
      MapSet.new(["s", "o"])

  """
  def pattern_variable_set({:triple, s, p, o}) do
    [s, p, o]
    |> Enum.map(&extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  def pattern_variable_set({:quad, s, p, o, g}) do
    [s, p, o, g]
    |> Enum.map(&extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  def pattern_variable_set(_), do: MapSet.new()

  @doc """
  Counts the number of bound (non-variable) terms in a pattern.

  ## Examples

      iex> PatternMacros.bound_term_count({:triple, {:variable, "s"}, 10, {:variable, "o"}})
      1

      iex> PatternMacros.bound_term_count({:quad, 1, 2, 3, 4})
      4

  """
  def bound_term_count({:triple, s, p, o}) do
    count_bound([s, p, o])
  end

  def bound_term_count({:quad, s, p, o, g}) do
    count_bound([s, p, o, g])
  end

  def bound_term_count(_), do: 0

  defp count_bound(terms) do
    Enum.count(terms, fn
      {:variable, _} -> false
      _ -> true
    end)
  end

  @doc """
  Checks if a pattern contains a specific variable.

  ## Examples

      iex> PatternMacros.has_variable?({:triple, {:variable, "s"}, 10, {:variable, "o"}}, "s")
      true

      iex> PatternMacros.has_variable?({:triple, {:variable, "s"}, 10, {:variable, "o"}}, "g")
      false

  """
  def has_variable?({:triple, s, p, o}, var_name) do
    extract_var_name(s) == var_name or
      extract_var_name(p) == var_name or
      extract_var_name(o) == var_name
  end

  def has_variable?({:quad, s, p, o, g}, var_name) do
    extract_var_name(s) == var_name or
      extract_var_name(p) == var_name or
      extract_var_name(o) == var_name or
      extract_var_name(g) == var_name
  end

  def has_variable?(_, _), do: false

  @doc """
  Gets the position of a variable in a pattern.

  ## Returns

  - `:subject`, `:predicate`, `:object`, or `:graph` if found
  - `nil` if not found

  ## Examples

      iex> PatternMacros.variable_position({:triple, {:variable, "s"}, 10, {:variable, "o"}}, "s")
      :subject

      iex> PatternMacros.variable_position({:triple, {:variable, "s"}, 10, {:variable, "o"}}, "o")
      :object

      iex> PatternMacros.variable_position({:triple, {:variable, "s"}, 10, {:variable, "o"}}, "g")
      nil

  """
  def variable_position({:triple, s, p, o}, var_name) do
    cond do
      extract_var_name(s) == var_name -> :subject
      extract_var_name(p) == var_name -> :predicate
      extract_var_name(o) == var_name -> :object
      true -> nil
    end
  end

  def variable_position({:quad, s, p, o, g}, var_name) do
    cond do
      extract_var_name(s) == var_name -> :subject
      extract_var_name(p) == var_name -> :predicate
      extract_var_name(o) == var_name -> :object
      extract_var_name(g) == var_name -> :graph
      true -> nil
    end
  end

  def variable_position(_, _), do: nil

  @doc """
  Checks if a term is bound given a set of bound variables.

  A term is considered bound if it's a constant or a variable in the bound set.

  """
  def term_bound?({:variable, name}, bound_vars) when is_map(bound_vars) do
    case Map.get(bound_vars, :__struct__) do
      MapSet -> MapSet.member?(bound_vars, name)
      _ -> Map.has_key?(bound_vars, name)
    end
  end

  def term_bound?({:variable, _}, _), do: false
  def term_bound?(_, _), do: true

  @doc """
  Converts a triple pattern to a quad pattern with the given graph.

  ## Examples

      iex> PatternMacros.to_quad({:triple, s, p, o}, :default)
      {:quad, s, p, o, :default}

  """
  def to_quad(pattern, graph \\ :default)

  def to_quad({:triple, s, p, o}, graph) do
    {:quad, s, p, o, graph}
  end

  def to_quad({:quad, _, _, _, _} = quad, _graph), do: quad

  @doc """
  Extracts the graph component from a pattern.

  Returns `:default_graph` for triple patterns, or the graph term for quad patterns.

  """
  def extract_graph({:triple, _, _, _}), do: :default_graph
  def extract_graph({:quad, _, _, _, g}), do: g
  def extract_graph(_), do: nil

  @doc """
  Creates a triple pattern from components.

  """
  def triple(s, p, o), do: {:triple, s, p, o}

  @doc """
  Creates a quad pattern from components.

  """
  def quad(s, p, o, g), do: {:quad, s, p, o, g}
end
