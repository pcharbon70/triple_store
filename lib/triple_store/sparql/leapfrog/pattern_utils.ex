defmodule TripleStore.SPARQL.Leapfrog.PatternUtils do
  @moduledoc """
  Shared utility functions for working with SPARQL triple patterns.

  This module consolidates helper functions that are used across the
  Leapfrog Triejoin implementation modules to avoid code duplication.
  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "A triple pattern from the SPARQL algebra"
  @type triple_pattern :: {:triple, term(), term(), term()}

  @typedoc "Variable name"
  @type variable :: String.t()

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Extracts the variable name from a term, if it's a variable.

  ## Arguments

  - `term` - A term from a triple pattern

  ## Returns

  - The variable name (String) if the term is a variable
  - `nil` otherwise

  ## Examples

      iex> PatternUtils.extract_var_name({:variable, "x"})
      "x"

      iex> PatternUtils.extract_var_name({:named_node, "http://example.org/foo"})
      nil

      iex> PatternUtils.extract_var_name(42)
      nil

  """
  @spec extract_var_name(term()) :: String.t() | nil
  def extract_var_name({:variable, name}), do: name
  def extract_var_name(_), do: nil

  @doc """
  Checks if a triple pattern contains a specific variable.

  ## Arguments

  - `pattern` - A triple pattern
  - `var_name` - The variable name to look for

  ## Returns

  - `true` if the pattern contains the variable
  - `false` otherwise

  ## Examples

      iex> pattern = {:triple, {:variable, "x"}, 10, {:variable, "y"}}
      iex> PatternUtils.pattern_contains_variable?(pattern, "x")
      true

      iex> PatternUtils.pattern_contains_variable?(pattern, "z")
      false

  """
  @spec pattern_contains_variable?(triple_pattern(), variable()) :: boolean()
  def pattern_contains_variable?({:triple, s, p, o}, var_name) do
    extract_var_name(s) == var_name or
      extract_var_name(p) == var_name or
      extract_var_name(o) == var_name
  end

  def pattern_contains_variable?(_, _), do: false

  @doc """
  Gets the position of a variable in a triple pattern.

  ## Arguments

  - `pattern` - A triple pattern
  - `var_name` - The variable name to find

  ## Returns

  - `:subject`, `:predicate`, or `:object` if found
  - `nil` if not found

  ## Examples

      iex> pattern = {:triple, {:variable, "x"}, 10, {:variable, "y"}}
      iex> PatternUtils.variable_position(pattern, "x")
      :subject

      iex> PatternUtils.variable_position(pattern, "y")
      :object

      iex> PatternUtils.variable_position(pattern, "z")
      nil

  """
  @spec variable_position(triple_pattern(), variable()) :: :subject | :predicate | :object | nil
  def variable_position({:triple, s, p, o}, var_name) do
    cond do
      extract_var_name(s) == var_name -> :subject
      extract_var_name(p) == var_name -> :predicate
      extract_var_name(o) == var_name -> :object
      true -> nil
    end
  end

  @doc """
  Extracts all variable names from a triple pattern.

  ## Arguments

  - `pattern` - A triple pattern

  ## Returns

  List of variable names found in the pattern.

  ## Examples

      iex> pattern = {:triple, {:variable, "x"}, 10, {:variable, "y"}}
      iex> PatternUtils.pattern_variables(pattern)
      ["x", "y"]

  """
  @spec pattern_variables(triple_pattern()) :: [variable()]
  def pattern_variables({:triple, s, p, o}) do
    [extract_var_name(s), extract_var_name(p), extract_var_name(o)]
    |> Enum.reject(&is_nil/1)
  end

  def pattern_variables(_), do: []

  @doc """
  Checks if a term is a constant (not a variable).

  ## Arguments

  - `term` - A term from a triple pattern

  ## Returns

  - `true` if the term is a constant
  - `false` if the term is a variable

  """
  @spec is_constant?(term()) :: boolean()
  def is_constant?({:variable, _}), do: false
  def is_constant?(_), do: true

  @doc """
  Checks if a term is bound (either a constant or a bound variable).

  ## Arguments

  - `term` - A term from a triple pattern
  - `bound_vars` - MapSet of bound variable names

  ## Returns

  - `true` if the term is bound
  - `false` otherwise

  """
  @spec is_bound_or_const?(term(), MapSet.t(variable())) :: boolean()
  def is_bound_or_const?({:variable, name}, bound_vars), do: MapSet.member?(bound_vars, name)
  def is_bound_or_const?({:named_node, _}, _), do: true
  def is_bound_or_const?({:literal, _, _}, _), do: true
  def is_bound_or_const?({:literal, _, _, _}, _), do: true
  def is_bound_or_const?({:blank_node, _}, _), do: true
  def is_bound_or_const?(id, _) when is_integer(id), do: true
  def is_bound_or_const?(_, _), do: false

  @doc """
  Gets the value of a term (constant or from bindings).

  ## Arguments

  - `term` - A term from a triple pattern
  - `bindings` - Map of variable name to value

  ## Returns

  - The integer value if it's a constant or bound variable
  - `nil` if the term is an unbound variable

  """
  @spec get_term_value(term(), %{variable() => non_neg_integer()}) :: non_neg_integer() | nil
  def get_term_value({:variable, name}, bindings), do: Map.get(bindings, name)
  def get_term_value(id, _bindings) when is_integer(id), do: id
  def get_term_value(_, _), do: nil
end
