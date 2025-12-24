defmodule TripleStore.SPARQL.Leapfrog.MultiLevel do
  @moduledoc """
  Multi-level Leapfrog Triejoin for complete query execution.

  This module orchestrates the full Leapfrog Triejoin algorithm across multiple
  variables. It processes variables one at a time according to the Variable
  Elimination Order (VEO), using Leapfrog joins at each level to find valid
  bindings, then descending to the next level.

  ## Algorithm Overview

  1. Compute optimal variable ordering (VEO)
  2. For each variable in order:
     a. Create iterators for patterns containing that variable
     b. Use Leapfrog to find intersection (common values)
     c. For each value found, bind the variable and descend
  3. When all variables are bound, emit the complete binding
  4. Backtrack to find more solutions

  ## Usage

      patterns = [
        {:triple, {:variable, "x"}, {:named_node, "knows"}, {:variable, "y"}},
        {:triple, {:variable, "y"}, {:named_node, "age"}, {:variable, "z"}}
      ]

      {:ok, executor} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(executor) |> Enum.to_list()
      # => [%{"x" => 1, "y" => 2, "z" => 25}, ...]

  ## Design Notes

  The algorithm maintains a stack of levels (deepest first), each containing:
  - The variable being processed
  - The Leapfrog join for that variable's iterators
  - The current binding for that variable

  This enables efficient backtracking without recreating iterators.
  """

  alias TripleStore.SPARQL.Leapfrog.{TrieIterator, Leapfrog, VariableOrdering}

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Triple pattern from SPARQL algebra"
  @type triple_pattern :: {:triple, term(), term(), term()}

  @typedoc "Variable binding map"
  @type bindings :: %{String.t() => non_neg_integer()}

  @typedoc """
  A level in the multi-level iteration stack.

  - `:variable` - Variable name being processed at this level
  - `:leapfrog` - Leapfrog join for this variable's iterators
  - `:value` - Current bound value for this variable
  - `:level_idx` - Index in var_order (0 = first variable)
  """
  @type level :: %{
          variable: String.t(),
          leapfrog: Leapfrog.t() | nil,
          value: non_neg_integer() | nil,
          level_idx: non_neg_integer()
        }

  @typedoc """
  The MultiLevel executor struct.

  - `:db` - Database reference
  - `:patterns` - Original triple patterns
  - `:var_order` - Variable elimination order
  - `:levels` - Stack of levels (deepest/latest first)
  - `:current_bindings` - Current variable bindings
  - `:exhausted` - Whether all solutions have been enumerated
  - `:initialized` - Whether we've started iteration
  """
  @type t :: %__MODULE__{
          db: reference(),
          patterns: [triple_pattern()],
          var_order: [String.t()],
          levels: [level()],
          current_bindings: bindings(),
          exhausted: boolean(),
          initialized: boolean()
        }

  @enforce_keys [:db, :patterns, :var_order]
  defstruct [
    :db,
    :patterns,
    :var_order,
    levels: [],
    current_bindings: %{},
    exhausted: false,
    initialized: false
  ]

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Creates a new multi-level executor for the given patterns.

  ## Arguments

  - `db` - Database reference
  - `patterns` - List of triple patterns
  - `stats` - Statistics for variable ordering (optional)

  ## Returns

  - `{:ok, executor}` on success
  - `{:error, reason}` on failure

  """
  @spec new(reference(), [triple_pattern()], map()) :: {:ok, t()} | {:error, term()}
  def new(db, patterns, stats \\ %{}) do
    case VariableOrdering.compute(patterns, stats) do
      {:ok, []} ->
        # No variables - this is a ground pattern check
        {:ok, %__MODULE__{db: db, patterns: patterns, var_order: [], exhausted: true}}

      {:ok, var_order} ->
        executor = %__MODULE__{
          db: db,
          patterns: patterns,
          var_order: var_order
        }

        {:ok, executor}
    end
  end

  @doc """
  Returns a stream of all bindings that satisfy the patterns.

  ## Arguments

  - `executor` - The multi-level executor

  ## Returns

  A Stream of binding maps.

  """
  @spec stream(t()) :: Enumerable.t()
  def stream(%__MODULE__{exhausted: true}), do: []
  def stream(%__MODULE__{var_order: []}), do: []

  def stream(%__MODULE__{} = executor) do
    Stream.unfold(executor, fn exec ->
      case next_binding(exec) do
        {:ok, bindings, new_exec} ->
          {bindings, new_exec}

        :exhausted ->
          nil
      end
    end)
  end

  @doc """
  Finds the next binding that satisfies all patterns.

  ## Arguments

  - `executor` - The multi-level executor

  ## Returns

  - `{:ok, bindings, new_executor}` if a binding was found
  - `:exhausted` if no more bindings exist

  """
  @spec next_binding(t()) :: {:ok, bindings(), t()} | :exhausted
  def next_binding(%__MODULE__{exhausted: true}), do: :exhausted

  def next_binding(%__MODULE__{initialized: false} = exec) do
    # First call - start descending from level 0
    exec = %{exec | initialized: true}
    find_next_solution(exec, :descend, 0)
  end

  def next_binding(%__MODULE__{levels: []} = _exec) do
    # No levels and already initialized = exhausted
    :exhausted
  end

  def next_binding(%__MODULE__{levels: [top | _]} = exec) do
    # We have a previous solution - advance from the deepest level
    find_next_solution(exec, :advance, top.level_idx)
  end

  @doc """
  Closes all open iterators and releases resources.

  ## Arguments

  - `executor` - The multi-level executor

  ## Returns

  - `:ok`

  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{levels: levels}) do
    Enum.each(levels, fn level ->
      if level.leapfrog do
        Leapfrog.close(level.leapfrog)
      end
    end)

    :ok
  end

  # ===========================================================================
  # Private: Core Algorithm
  # ===========================================================================

  # Main loop: find the next complete solution
  defp find_next_solution(%__MODULE__{var_order: var_order} = exec, :descend, level_idx)
       when level_idx >= length(var_order) do
    # All variables bound - return this solution
    {:ok, exec.current_bindings, exec}
  end

  defp find_next_solution(exec, :descend, level_idx) do
    # Try to descend to level_idx
    case enter_level(exec, level_idx) do
      {:ok, new_exec} ->
        # Successfully entered this level, continue to next
        find_next_solution(new_exec, :descend, level_idx + 1)

      :no_match ->
        # No values at this level - backtrack
        find_next_solution(exec, :backtrack, level_idx - 1)
    end
  end

  defp find_next_solution(_exec, :backtrack, level_idx) when level_idx < 0 do
    # Backtracked past the first level - exhausted
    :exhausted
  end

  defp find_next_solution(exec, :backtrack, level_idx) do
    # Try to advance at this level
    case advance_at_level(exec, level_idx) do
      {:ok, new_exec} ->
        # Successfully advanced, continue descending
        find_next_solution(new_exec, :descend, level_idx + 1)

      :exhausted ->
        # No more values at this level, backtrack further
        new_exec = pop_level(exec, level_idx)
        find_next_solution(new_exec, :backtrack, level_idx - 1)
    end
  end

  defp find_next_solution(exec, :advance, level_idx) do
    # Advance at the current level (after returning a solution)
    case advance_at_level(exec, level_idx) do
      {:ok, new_exec} ->
        # Advanced successfully, continue descending
        find_next_solution(new_exec, :descend, level_idx + 1)

      :exhausted ->
        # No more values, backtrack
        new_exec = pop_level(exec, level_idx)
        find_next_solution(new_exec, :backtrack, level_idx - 1)
    end
  end

  # ===========================================================================
  # Private: Level Operations
  # ===========================================================================

  # Enter a new level (create iterators and find first value)
  defp enter_level(exec, level_idx) do
    variable = Enum.at(exec.var_order, level_idx)

    # Find patterns containing this variable
    containing_patterns =
      exec.patterns
      |> Enum.filter(&pattern_contains_variable?(&1, variable))

    # Create iterators
    case create_iterators_for_variable(exec.db, containing_patterns, variable, exec.current_bindings) do
      {:ok, iterators} when iterators != [] ->
        case Leapfrog.new(iterators) do
          {:ok, lf} ->
            case Leapfrog.search(lf) do
              {:ok, lf} ->
                {:ok, value} = Leapfrog.current(lf)

                new_level = %{
                  variable: variable,
                  leapfrog: lf,
                  value: value,
                  level_idx: level_idx
                }

                new_bindings = Map.put(exec.current_bindings, variable, value)
                new_exec = %{exec | levels: [new_level | exec.levels], current_bindings: new_bindings}

                {:ok, new_exec}

              {:exhausted, lf} ->
                Leapfrog.close(lf)
                :no_match
            end

          {:exhausted, lf} ->
            Leapfrog.close(lf)
            :no_match
        end

      {:ok, []} ->
        :no_match

      {:error, _} ->
        :no_match
    end
  end

  # Advance at a level (find next value in leapfrog)
  defp advance_at_level(%__MODULE__{levels: levels} = exec, level_idx) do
    # Find the level at this index
    case Enum.find(levels, &(&1.level_idx == level_idx)) do
      nil ->
        :exhausted

      level ->
        case Leapfrog.next(level.leapfrog) do
          {:ok, lf} ->
            {:ok, value} = Leapfrog.current(lf)

            # Update the level
            updated_level = %{level | leapfrog: lf, value: value}
            new_levels = replace_level(levels, level_idx, updated_level)
            new_bindings = Map.put(exec.current_bindings, level.variable, value)

            # Remove any deeper levels (their iterators are now invalid)
            new_levels = Enum.filter(new_levels, &(&1.level_idx <= level_idx))

            # Clear bindings for removed levels
            removed_vars =
              levels
              |> Enum.filter(&(&1.level_idx > level_idx))
              |> Enum.map(& &1.variable)

            new_bindings = Map.drop(new_bindings, removed_vars)

            {:ok, %{exec | levels: new_levels, current_bindings: new_bindings}}

          {:exhausted, _lf} ->
            :exhausted
        end
    end
  end

  # Pop a level from the stack
  defp pop_level(%__MODULE__{levels: levels} = exec, level_idx) do
    case Enum.find(levels, &(&1.level_idx == level_idx)) do
      nil ->
        exec

      level ->
        # Close the leapfrog
        if level.leapfrog, do: Leapfrog.close(level.leapfrog)

        # Remove this level and all deeper levels
        new_levels = Enum.filter(levels, &(&1.level_idx < level_idx))

        # Remove bindings for this and deeper levels
        removed_vars =
          levels
          |> Enum.filter(&(&1.level_idx >= level_idx))
          |> Enum.map(& &1.variable)

        new_bindings = Map.drop(exec.current_bindings, removed_vars)

        %{exec | levels: new_levels, current_bindings: new_bindings}
    end
  end

  # Replace a level in the list
  defp replace_level(levels, level_idx, new_level) do
    Enum.map(levels, fn level ->
      if level.level_idx == level_idx, do: new_level, else: level
    end)
  end

  # ===========================================================================
  # Private: Iterator Creation
  # ===========================================================================

  # Create iterators for a variable across all its patterns
  defp create_iterators_for_variable(db, patterns, target_var, bindings) do
    results =
      patterns
      |> Enum.map(fn pattern ->
        {cf, prefix, level} = choose_index_and_prefix(pattern, target_var, bindings)
        TrieIterator.new(db, cf, prefix, level)
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        iterators = Enum.map(results, fn {:ok, iter} -> iter end)
        {:ok, iterators}

      error ->
        error
    end
  end

  # Choose the best index and build prefix for a pattern
  defp choose_index_and_prefix({:triple, s, p, o}, target_var, bindings) do
    s_val = get_term_value(s, bindings)
    p_val = get_term_value(p, bindings)
    o_val = get_term_value(o, bindings)

    s_var = extract_var_name(s)
    p_var = extract_var_name(p)
    o_var = extract_var_name(o)

    target_pos =
      cond do
        s_var == target_var -> :s
        p_var == target_var -> :p
        o_var == target_var -> :o
      end

    # Choose index based on what's bound and target position
    # IMPORTANT: level must equal byte_size(prefix) / 8 for TrieIterator to work correctly
    # Index key structures:
    # - SPO: subject (0) | predicate (1) | object (2)
    # - POS: predicate (0) | object (1) | subject (2)
    # - OSP: object (0) | subject (1) | predicate (2)
    case {target_pos, s_val, p_val, o_val} do
      # Target is Subject
      {:s, _, p, o} when p != nil and o != nil ->
        # P and O bound: POS with (P,O) prefix -> level 2 (S)
        {:pos, <<p::64-big, o::64-big>>, 2}

      {:s, _, p, _} when p != nil ->
        # Only P bound: no perfect index exists
        # Use SPO and scan all subjects (will include many non-matching)
        # This is suboptimal but correct - the Leapfrog will filter
        {:spo, <<>>, 0}

      {:s, _, _, o} when o != nil ->
        # Only O bound: OSP with O prefix -> level 1 (S)
        {:osp, <<o::64-big>>, 1}

      {:s, _, _, _} ->
        # Nothing bound: SPO scan -> level 0 (S)
        {:spo, <<>>, 0}

      # Target is Predicate
      {:p, s, _, o} when s != nil and o != nil ->
        # S and O bound: use SPO with S prefix -> level 1 (P)
        # (OSP would give P at level 2 with O prefix, but we'd need O,S prefix)
        {:spo, <<s::64-big>>, 1}

      {:p, s, _, _} when s != nil ->
        # Only S bound: SPO with S prefix -> level 1 (P)
        {:spo, <<s::64-big>>, 1}

      {:p, _, _, o} when o != nil ->
        # Only O bound: no perfect index
        # Use POS and scan all predicates
        {:pos, <<>>, 0}

      {:p, _, _, _} ->
        # Nothing bound: POS scan -> level 0 (P)
        {:pos, <<>>, 0}

      # Target is Object
      {:o, s, p, _} when s != nil and p != nil ->
        # S and P bound: SPO with (S,P) prefix -> level 2 (O)
        {:spo, <<s::64-big, p::64-big>>, 2}

      {:o, s, _, _} when s != nil ->
        # Only S bound: no perfect index for O
        # Use OSP and scan all objects
        {:osp, <<>>, 0}

      {:o, _, p, _} when p != nil ->
        # Only P bound: POS with P prefix -> level 1 (O)
        {:pos, <<p::64-big>>, 1}

      {:o, _, _, _} ->
        # Nothing bound: OSP scan -> level 0 (O)
        {:osp, <<>>, 0}
    end
  end

  # Get the value of a term (constant or from bindings)
  defp get_term_value({:variable, name}, bindings), do: Map.get(bindings, name)
  defp get_term_value(id, _bindings) when is_integer(id), do: id
  defp get_term_value(_, _), do: nil

  # ===========================================================================
  # Private: Pattern Helpers
  # ===========================================================================

  defp pattern_contains_variable?({:triple, s, p, o}, var_name) do
    extract_var_name(s) == var_name or
      extract_var_name(p) == var_name or
      extract_var_name(o) == var_name
  end

  defp extract_var_name({:variable, name}), do: name
  defp extract_var_name(_), do: nil
end
