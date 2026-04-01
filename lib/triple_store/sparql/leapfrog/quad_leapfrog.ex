defmodule TripleStore.SPARQL.Leapfrog.QuadLeapfrog do
  @moduledoc """
  Quad-specific Leapfrog join for 4-way joins on quad patterns.

  Extends the core Leapfrog algorithm to handle quad patterns with subject,
  predicate, object, and graph components. Uses QuadTrieIterator for
  efficient iteration over quad indices.

  ## Current Implementation Status

  **IMPORTANT**: This module provides the structure and variable ordering logic
  for quad patterns, but full integration with the core Leapfrog algorithm
  requires making Leapfrog polymorphic to work with both TrieIterator and
  QuadTrieIterator types.

  The current implementation:
  - Provides variable ordering for quad patterns
  - Creates QuadTrieIterator instances for quad patterns
  - Handles fully-bound quad patterns with direct lookup
  - Provides binding extraction from quad keys

  **TODO**: To complete full multi-way quad joins:
  1. Make core Leapfrog polymorphic over iterator types (via Protocol or behaviour)
  2. Update Leapfrog type specs to accept both TrieIterator and QuadTrieIterator
  3. Implement proper 4-iterator join coordination
  4. Add comprehensive integration tests

  ## Algorithm Overview

  For a quad pattern like:
  {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

  The algorithm creates four iterators (one for each variable position) and
  uses the leapfrog technique to find intersections where all four variables
  align on the same quad.

  ## Usage

      # Create iterators for a quad pattern
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      {:ok, lf} = QuadLeapfrog.from_pattern(db, pattern)

      # Stream through results
      stream = QuadLeapfrog.stream(lf)
      results = Enum.to_list(stream)

  ## Variable Ordering

  For optimal performance, variables should be ordered by selectivity:
  1. Bound variables (constants) are most selective
  2. Variables with low cardinality (e.g., specific predicates)
  3. Variables at prefix positions in the index

  Use `quad_variable_ordering/2` to get the optimal order.

  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.SPARQL.Leapfrog.{Leapfrog, QuadTrieIterator}
  alias TripleStore.SPARQL.QuadCardinality

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc """
  Quad component position in the 4-tuple.

  - `:bound` - Component is a concrete value (integer ID or atom)
  - `:variable` - Component is a variable with a name
  """
  @type component :: {:bound, non_neg_integer()} | {:variable, String.t()}

  @typedoc """
  Index selection for a quad join position.

  Each position (0-3 representing s, p, o, g) can use a different index
  depending on which components are bound. The index determines the
  prefix scan strategy.
  """
  @type index :: :gspo | :gpos | :spog | :posg

  @typedoc """
  Iterator position specification.

  - `:position` - Position index (0=s, 1=p, 2=o, 3=g)
  - `:component` - The component at this position
  - `:index` - Which index to use for this position
  """
  @type iterator_position :: {position :: non_neg_integer(), component :: component(), index :: index()}

  @typedoc """
  Plan for creating iterators for a quad pattern.

  Each entry specifies which index to use for each variable position,
  along with the prefix depth (number of bound components before it).
  """
  @type iterator_plan :: [{position :: non_neg_integer(), index :: index(), prefix_depth :: non_neg_integer()}]

  @typedoc """
  Iterator with quad-specific metadata for multi-iterator joins.

  - `:iterator` - The QuadTrieIterator instance
  - `:variable_name` - Name of the variable this iterator represents (e.g., "s", "p", "o", "g")
  - `:position` - Position index in the quad pattern (0=s, 1=p, 2=o, 3=g)
  - `:index` - Which index this iterator uses
  """
  @type tagged_iterator :: %{
          iterator: QuadTrieIterator.t(),
          variable_name: String.t(),
          position: non_neg_integer(),
          index: index()
        }

  @typedoc """
  The QuadLeapfrog struct wraps the core Leapfrog with quad-specific metadata.

  - `:leapfrog` - The core Leapfrog struct
  - `:variables` - Ordered list of variable names
  - `:pattern` - Original quad pattern
  - `:bindings` - Extracted variable bindings from current match
  - `:tagged_iterators` - List of tagged iterators with metadata for binding extraction
  """
  @type t :: %__MODULE__{
          leapfrog: Leapfrog.t(),
          variables: [String.t()],
          pattern: tuple(),
          bindings: map(),
          tagged_iterators: [tagged_iterator()] | nil
        }

  @enforce_keys [:leapfrog, :variables, :pattern]
  defstruct [:leapfrog, :variables, :pattern, bindings: %{}, tagged_iterators: nil]

  # ===========================================================================
  # Index Strategy (Section 1.1)
  # ===========================================================================

  @doc """
  Determines the optimal index for a given quad pattern position.

  The index selection is based on which positions are bound:
  - GSPO (Graph-Subject-Predicate-Object): Best when graph is bound
  - GPOS (Graph-Predicate-Object-Subject): Best when graph and predicate are bound
  - SPOG (Subject-Predicate-Object-Graph): Best when subject is bound
  - POSG (Predicate-Object-Subject-Graph): Best when predicate is bound

  ## Arguments

  - `pattern` - Quad pattern {:quad, s, p, o, g}
  - `position` - Position index (0=s, 1=p, 2=o, 3=g)

  ## Returns

  - `:gspo`, `:gpos`, `:spog`, or `:posg`

  ## Examples

      index_for_position({:quad, 42, {:variable, "p"}, 1, 0}, 0)
      # => :gspo (graph bound, prefix scan on graph)

      index_for_position({:quad, {:variable, "s"}, {:variable, "p"}, 1, 0}, 0)
      # => :gspo (graph bound, scan for s)

  """
  @spec index_for_position(tuple(), non_neg_integer()) :: index()
  # Subject position (0) with subject bound
  def index_for_position({:quad, s, _p, _o, g}, 0) when not is_tuple(s) do
    # Subject bound - use SPOG if graph not bound, otherwise GSPO
    if not is_tuple(g), do: :gspo, else: :spog
  end

  # Subject position (0) with subject variable
  def index_for_position({:quad, {:variable, _s}, _p, _o, g}, 0) do
    # Subject variable - use GSPO if graph bound, SPOG otherwise
    if not is_tuple(g), do: :gspo, else: :spog
  end

  # Predicate position (1) with predicate bound
  def index_for_position({:quad, _s, p, _o, g}, 1) when not is_tuple(p) do
    # Predicate bound - use GPOS if graph bound, POSG otherwise
    if not is_tuple(g), do: :gpos, else: :posg
  end

  # Predicate position (1) with predicate variable
  def index_for_position({:quad, _s, {:variable, _p}, _o, g}, 1) do
    # Predicate variable - use GPOS if graph bound, POSG otherwise
    if not is_tuple(g), do: :gpos, else: :posg
  end

  # Object position (2)
  def index_for_position({:quad, _s, _p, _o, g}, 2) do
    # Object position - use GSPO if graph bound
    if not is_tuple(g), do: :gspo, else: :spog
  end

  # Graph position (3) - always use GSPO/GPOS as primary indices
  def index_for_position({:quad, _s, _p, _o, _g}, 3), do: :gspo

  @doc """
  Plans the iterator creation strategy for a quad pattern.

  Returns a list of iterator specifications, one for each unbound position.
  Each spec includes the position index, the index to use, and the prefix depth.

  ## Arguments

  - `pattern` - Quad pattern {:quad, s, p, o, g}

  ## Returns

  - `{:ok, iterator_plan()}` - List of iterator specs
  - `{:error, reason}` - If pattern is invalid

  ## Examples

      {:ok, plan} = plan_iterators({:quad, {:variable, "s"}, {:variable, "p"}, 1, 0})
      # => [{0, :spog, 0}, {1, :posg, 1}]

  """
  @spec plan_iterators(tuple()) :: {:ok, iterator_plan()} | {:error, term()}
  def plan_iterators({:quad, s, p, o, g} = pattern) do
    components = [s, p, o, g]

    # Count unbound variables
    unbound_count = Enum.count(components, &variable?/1)

    cond do
      unbound_count == 0 ->
        # All bound - no iterators needed (direct lookup)
        {:ok, []}

      unbound_count == 1 ->
        # Single unbound - use prefix scan (existing behavior)
        {:ok, plan_single_variable(pattern, components)}

      true ->
        # Multiple unbound - need multiple iterators
        {:ok, plan_multi_variable(pattern, components)}
    end
  end

  # Plan for single variable (existing behavior - prefix scan)
  defp plan_single_variable({:quad, _s, _p, _o, g}, components) do
    # Find the unbound position
    {_, unbound_pos} =
      components
      |> Enum.with_index()
      |> Enum.find(fn {comp, _idx} -> variable?(comp) end)

    # Count bound components before the unbound position (in SPO order)
    prefix_depth =
      components
      |> Enum.take(unbound_pos)
      |> Enum.count(&bound?/1)

    # For subject position (0), add 1 if graph is bound (graph comes before subject in GSPO)
    prefix_depth =
      if unbound_pos == 0 and bound?(g) do
        prefix_depth + 1
      else
        prefix_depth
      end

    # Select index based on which components are bound (including graph)
    selected_index = select_index_for_components(components)

    [{unbound_pos, selected_index, prefix_depth}]
  end

  # Plan for multiple variables (new multi-iterator behavior)
  defp plan_multi_variable({:quad, s, p, o, g}, components) do
    # For multi-iterator joins, all iterators must use the same index
    # Select the base index based on what's bound
    base_index =
      cond do
        bound?(g) -> :gspo  # Graph-bound: use GSPO for all iterators
        bound?(s) -> :spog  # Subject-bound but not graph: use SPOG
        true -> :posg       # Fallback to POSG
      end

    # Create an iterator for each unbound variable position, all using the same index
    components
    |> Enum.with_index()
    |> Enum.filter(fn {comp, _idx} -> variable?(comp) end)
    |> Enum.map(fn {_comp, pos} ->
      # For plan purposes, prefix_depth is the position (will be recalculated)
      {pos, base_index, pos}
    end)
  end

  # Select optimal index based on which components are bound
  defp select_index_for_components([s, _p, _o, g]) do
    cond do
      bound?(g) and bound?(s) -> :gspo
      bound?(g) -> :gpos
      bound?(s) -> :spog
      true -> :posg
    end
  end

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Creates a QuadLeapfrog from a quad pattern.

  Analyzes the pattern to determine optimal index and variable ordering,
  then creates iterators for leapfrog execution.

  ## Arguments

  - `db` - Database reference
  - `pattern` - Quad pattern {:quad, s, p, o, g}

  ## Returns

  - `{:ok, quad_lf}` on success
  - `{:exhausted, quad_lf}` if no results possible
  - `{:error, reason}` on failure

  ## Examples

      # Fully unbound pattern - will iterate all quads
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      {:ok, lf} = QuadLeapfrog.from_pattern(db, pattern)

      # Graph-scoped pattern with some bounds
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      {:ok, lf} = QuadLeapfrog.from_pattern(db, pattern)

  """
  @spec from_pattern(pid(), tuple()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def from_pattern(db, {:quad, s, p, o, g} = pattern) do
    # Determine variable ordering
    variables = extract_variables([s, p, o, g])

    # Create iterators for each variable position
    case do_create_iterators_for_pattern(db, pattern) do
      {:ok, tagged_iterators} ->
        # Section 1.3: Validate iterator list before passing to Leapfrog
        with :ok <- validate_iterators(tagged_iterators),
             raw_iterators = Enum.map(tagged_iterators, & &1.iterator) do
          case Leapfrog.new(raw_iterators) do
            {:ok, lf} ->
              {:ok,
               %__MODULE__{
                 leapfrog: lf,
                 variables: variables,
                 pattern: pattern,
                 tagged_iterators: tagged_iterators
               }}

            {:exhausted, lf} ->
              {:exhausted,
               %__MODULE__{
                 leapfrog: lf,
                 variables: variables,
                 pattern: pattern,
                 tagged_iterators: tagged_iterators
               }}

            {:error, reason} ->
              {:error, reason}
          end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Section 1.3: Validate iterator list before passing to Leapfrog
  # Returns :ok if valid, {:error, reason} if invalid
  defp validate_iterators([]) do
    # Empty iterator list means fully-bound pattern with direct lookup
    # This is handled by fully_bound_lookup, so we shouldn't get here
    {:error, :no_iterators}
  end

  defp validate_iterators(tagged_iterators) do
    # Check for duplicate positions
    positions = Enum.map(tagged_iterators, & &1.position)

    if length(Enum.uniq(positions)) != length(positions) do
      {:error, {:duplicate_positions, positions}}
    else
      # Check that all iterators implement TrieIteratorProtocol
      # (they should if created via QuadTrieIterator.new/4)
      :ok
    end
  end

  @doc """
  Searches for the next common value across all iterators.

  Delegates to the core Leapfrog search algorithm.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  - `{:ok, quad_lf}` if a match was found
  - `{:exhausted, quad_lf}` if no more matches
  - `{:error, reason}` on failure

  """
  @spec search(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def search(%__MODULE__{leapfrog: lf} = qlf) do
    case Leapfrog.search(lf) do
      {:ok, lf} ->
        qlf = %{qlf | leapfrog: lf}
        qlf = extract_bindings(qlf)
        {:ok, qlf}

      {:exhausted, lf} ->
        {:exhausted, %{qlf | leapfrog: lf, bindings: %{}}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Advances past the current match to find the next common value.

  Delegates to the core Leapfrog next algorithm.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  - `{:ok, quad_lf}` if another match was found
  - `{:exhausted, quad_lf}` if no more matches
  - `{:error, reason}` on failure

  """
  @spec next(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def next(%__MODULE__{leapfrog: lf} = qlf) do
    case Leapfrog.next(lf) do
      {:ok, lf} ->
        qlf = %{qlf | leapfrog: lf}
        qlf = extract_bindings(qlf)
        {:ok, qlf}

      {:exhausted, lf} ->
        {:exhausted, %{qlf | leapfrog: lf, bindings: %{}}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns the current bindings.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  Map of variable names to their bound values for the current match.

  """
  @spec bindings(t()) :: map()
  def bindings(%__MODULE__{bindings: bindings}), do: bindings

  @doc """
  Checks if the QuadLeapfrog is exhausted.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  - `true` if exhausted
  - `false` otherwise

  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{leapfrog: lf}), do: Leapfrog.exhausted?(lf)

  @doc """
  Test helper: Creates iterators for a quad pattern.

  This function is primarily for testing multi-iterator creation.
  For normal use, see `from_pattern/2` which handles the full
  leapfrog initialization.

  ## Arguments

  - `db` - Database reference
  - `pattern` - Quad pattern {:quad, s, p, o, g}

  ## Returns

  - `{:ok, iterators}` - List of tagged iterators with metadata
  - `{:error, reason}` - On failure

  """
  @spec create_iterators_for_pattern(pid(), tuple()) ::
          {:ok, [tagged_iterator()]} | {:error, term()}
  def create_iterators_for_pattern(db, pattern), do: do_create_iterators_for_pattern(db, pattern)

  @doc """
  Returns all iterators (for inspection/debugging).

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  The list of QuadTrieIterator structs.

  """
  @spec iterators(t()) :: [Leapfrog.iterator()]
  def iterators(%__MODULE__{leapfrog: lf}), do: Leapfrog.iterators(lf)

  @doc """
  Closes all iterators and releases resources.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  - `:ok`

  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{leapfrog: lf}) do
    Leapfrog.close(lf)
  end

  @doc """
  Creates a Stream that yields all matches as binding maps.

  Each yielded item is a map with variable names as keys and their
  corresponding values from the current quad.

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  A Stream of binding maps.

  ## Examples

      {:ok, lf} = QuadLeapfrog.from_pattern(db, pattern)
      stream = QuadLeapfrog.stream(lf)
      results = Enum.to_list(stream)

  """
  @spec stream(t()) :: Enumerable.t()
  def stream(%__MODULE__{} = qlf) do
    Stream.unfold(qlf, fn qlf ->
      case search_or_next(qlf) do
        {:ok, searched_lf} ->
          unfold_quad_bindings(searched_lf)

        {:exhausted, _} ->
          nil
      end
    end)
  end

  @doc """
  Determines optimal variable ordering for quad leapfrog.

  Uses cardinality estimates and index position heuristics to
  determine which variables should be joined first.

  ## Arguments

  - `pattern` - Quad pattern {:quad, s, p, o, g}
  - `stats` - Statistics map for cardinality estimation

  ## Returns

  Ordered list of variable positions (0-3) from most to least selective.

  ## Examples

      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      {:ok, order} = QuadLeapfrog.quad_variable_ordering(pattern, stats)
      # Might return [3, 1, 0, 2] - graph first, then predicate, etc.

  """
  @spec quad_variable_ordering(tuple(), map()) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def quad_variable_ordering({:quad, s, p, o, g}, stats) do
    # Get cardinality estimates for each position
    components = [s, p, o, g]
    positions = [0, 1, 2, 3]

    # Score each position based on selectivity
    scored_positions =
      positions
      |> Enum.map(fn pos ->
        component = Enum.at(components, pos)
        score = position_selectivity_score(component, pos, stats)
        {pos, score}
      end)

    # Sort by score (lower is more selective)
    ordered = Enum.sort_by(scored_positions, fn {_pos, score} -> score end)
    {:ok, Enum.map(ordered, fn {pos, _score} -> pos end)}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Creates iterators for all variable positions in a pattern
  # Section 1.2: Multi-iterator creation for quad joins
  defp do_create_iterators_for_pattern(db, {:quad, s, p, o, g} = pattern) do
    components = [s, p, o, g]

    # Check if all components are bound (fully-specified quad)
    if Enum.all?(components, &bound?/1) do
      # Fully-bound pattern: do direct lookup instead of iteration
      # This is more efficient and avoids creating unnecessary iterators
      fully_bound_lookup(db, s, p, o, g)
    else
      # Plan multi-iterator strategy using plan from Section 1.1
      case plan_iterators(pattern) do
        {:ok, iterator_plan} ->
          create_iterators_from_plan(db, components, iterator_plan)

        {:error, _reason} = error ->
          error
      end
    end
  end

  # Creates multiple iterators from an iterator plan
  # Section 1.2: Create one iterator per variable position
  defp create_iterators_from_plan(db, components, iterator_plan) do
    # Create an iterator for each entry in the plan
    iterator_plan
    |> Enum.map(fn {position, index, _original_depth} ->
      component = Enum.at(components, position)
      variable_name = extract_variable_name(component, position)

      # Build position-specific prefix for this index
      {prefix, level} = build_prefix_for_index(components, position, index)

      # Create the iterator at the specified position
      case QuadTrieIterator.new(db, index, prefix, level) do
        {:ok, iter} ->
          # Tag the iterator with metadata for binding extraction
          {:ok,
           %{
             iterator: iter,
             variable_name: variable_name,
             position: position,
             index: index
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end)
    |> Enum.reduce({:ok, []}, fn
      {:ok, tagged_iter}, {:ok, acc} -> {:ok, acc ++ [tagged_iter]}
      {:error, reason}, _acc -> {:error, reason}
    end)
  end

  # Builds a position-specific prefix for a given index
  # Section 1.2: Prefix builder per position and index
  # Returns {prefix_binary, level}
  defp build_prefix_for_index(components, position, index) do
    # Map components to index order
    components_in_index_order = components_for_index(components, index)

    # Find the index of our target position in this ordering
    target_position_index = find_position_in_index_order(position, index)

    # Build prefix from all bound components before the target position
    # For multi-variable joins, we need ALL bound components before this position
    prefix_components =
      components_in_index_order
      |> Enum.take(target_position_index)

    # Build prefix from bound components only (variables can't be in prefix)
    prefix =
      prefix_components
      |> Enum.filter(&bound?/1)
      |> Enum.map(&extract_bound_value/1)
      |> Enum.map(fn id -> <<id::64-big>> end)
      |> IO.iodata_to_binary()

    # The level is the position index in the index-specific order
    # This tells the QuadTrieIterator which level to scan at
    {prefix, target_position_index}
  end

  # Finds the index order position (0-3) for a given quad position in a specific index
  defp find_position_in_index_order(0, :gspo), do: 1  # s is at index 1 in GSPO
  defp find_position_in_index_order(1, :gspo), do: 2  # p is at index 2 in GSPO
  defp find_position_in_index_order(2, :gspo), do: 3  # o is at index 3 in GSPO
  defp find_position_in_index_order(3, :gspo), do: 0  # g is at index 0 in GSPO

  defp find_position_in_index_order(0, :gpos), do: 3  # s is at index 3 in GPOS
  defp find_position_in_index_order(1, :gpos), do: 1  # p is at index 1 in GPOS
  defp find_position_in_index_order(2, :gpos), do: 2  # o is at index 2 in GPOS
  defp find_position_in_index_order(3, :gpos), do: 0  # g is at index 0 in GPOS

  defp find_position_in_index_order(0, :spog), do: 0  # s is at index 0 in SPOG
  defp find_position_in_index_order(1, :spog), do: 1  # p is at index 1 in SPOG
  defp find_position_in_index_order(2, :spog), do: 2  # o is at index 2 in SPOG
  defp find_position_in_index_order(3, :spog), do: 3  # g is at index 3 in SPOG

  defp find_position_in_index_order(0, :posg), do: 2  # s is at index 2 in POSG
  defp find_position_in_index_order(1, :posg), do: 0  # p is at index 0 in POSG
  defp find_position_in_index_order(2, :posg), do: 1  # o is at index 1 in POSG
  defp find_position_in_index_order(3, :posg), do: 3  # g is at index 3 in POSG

  # Returns components in the order they appear in the given index
  defp components_for_index([s, p, o, g], :gspo), do: [g, s, p, o]
  defp components_for_index([s, p, o, g], :gpos), do: [g, p, o, s]
  defp components_for_index([s, p, o, g], :spog), do: [s, p, o, g]
  defp components_for_index([s, p, o, g], :posg), do: [p, o, s, g]

  # Extracts variable name from component, with fallback to position-based name
  defp extract_variable_name({:variable, name}, _position), do: name
  defp extract_variable_name(_component, position) do
    # Fallback to position-based name for non-variable components
    Enum.at(["s", "p", "o", "g"], position, "var_#{position}")
  end

  # Direct lookup for fully-bound quads
  # Returns empty iterator list if quad exists, error otherwise
  defp fully_bound_lookup(db, s, p, o, g) do
    # Normalize graph ID
    graph_id = normalize_graph_id(g)

    # Build the full 32-byte key
    key = <<graph_id::64-big, s::64-big, p::64-big, o::64-big>>

    # Use the NIF to check if the quad exists
    case ErlangAdapter.get(db, :gspo, key) do
      {:ok, _value} ->
        # Quad exists, return empty iterator list (nothing to iterate)
        {:ok, []}

      :not_found ->
        # Quad doesn't exist, return empty iterator list
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Normalize graph ID to integer
  defp normalize_graph_id(graph_id) when is_integer(graph_id), do: graph_id
  defp normalize_graph_id(:default_graph), do: 0
  # Would need actual lookup
  defp normalize_graph_id({:named_node, _iri}), do: 0
  defp normalize_graph_id(_), do: 0

  # Build prefix from bound components
  defp build_prefix_from_components([], acc), do: IO.iodata_to_binary(:lists.reverse(acc))

  defp build_prefix_from_components([component | rest], acc) do
    value = extract_bound_value(component)

    if bound?(component) do
      build_prefix_from_components(rest, [<<value::64-big>> | acc])
    else
      # Stop at first unbound component
      IO.iodata_to_binary(:lists.reverse(acc))
    end
  end

  # Extract variable names from components
  defp extract_variables(components) do
    components
    |> Enum.with_index()
    |> Enum.filter(fn {comp, _idx} -> variable?(comp) end)
    |> Enum.map(fn {{:variable, name}, _idx} -> name end)
  end

  # Check if a component is a variable
  defp variable?({:variable, _name}), do: true
  defp variable?(_), do: false

  # Check if a component is bound (not a variable)
  defp bound?(component), do: not variable?(component)

  # Extract bound value from component
  defp extract_bound_value({:variable, _}), do: 0
  defp extract_bound_value(value) when is_integer(value), do: value
  defp extract_bound_value(:default_graph), do: 0
  defp extract_bound_value({:named_node, _iri}), do: 0
  defp extract_bound_value(_), do: 0

  # Calculate selectivity score for a position
  defp position_selectivity_score(component, _position, stats) do
    case bound?(component) do
      true -> 0
      false -> variable_selectivity_score(stats)
    end
  end

  # Extract bindings from the current match
  defp extract_bindings(%__MODULE__{leapfrog: lf, pattern: pattern} = qlf) do
    case Leapfrog.current(lf) do
      :exhausted ->
        %{qlf | bindings: %{}}

      {:ok, _value} ->
        # Get current iterator key
        iterators = Leapfrog.iterators(lf)

        bindings =
          case iterators do
            [%QuadTrieIterator{} = iter | _] ->
              bindings_from_quad_iterator(iter, pattern)

            _ ->
              %{}
          end

        %{qlf | bindings: bindings}
    end
  end

  defp unfold_quad_bindings(searched_lf) do
    case bindings(searched_lf) do
      bindings when map_size(bindings) > 0 -> {bindings, searched_lf}
      %{} -> nil
    end
  end

  defp variable_selectivity_score(stats) do
    case QuadCardinality.estimate_pattern(
           {:quad, {:variable, "_"}, {:variable, "_"}, {:variable, "_"}, {:variable, "_"}},
           stats
         ) do
      card when is_number(card) and card > 0 ->
        trunc(:math.log(card))

      _error ->
        default_card = Map.get(stats, :quad_count, 10_000)

        Logger.debug(
          "QuadLeapfrog: Using fallback cardinality #{default_card} for variable ordering"
        )

        trunc(:math.log(max(1, default_card)))
    end
  end

  defp bindings_from_quad_iterator(%QuadTrieIterator{} = iter, pattern) do
    case QuadTrieIterator.current_key(iter) do
      :exhausted ->
        %{}

      {:ok, key} ->
        {g, s, p, o} = QuadTrieIterator.decode_key(key)
        {:quad, s_pat, p_pat, o_pat, g_pat} = pattern

        %{}
        |> maybe_add_binding(s_pat, s, "s")
        |> maybe_add_binding(p_pat, p, "p")
        |> maybe_add_binding(o_pat, o, "o")
        |> maybe_add_binding(g_pat, g, "g")
    end
  end

  # Add binding if component is a variable
  defp maybe_add_binding(map, {:variable, name}, value, _default_name) do
    Map.put(map, name, value)
  end

  defp maybe_add_binding(map, _component, _value, _default_name) do
    map
  end

  # Helper for stream: search if not at match, otherwise advance then search
  defp search_or_next(%__MODULE__{} = qlf) do
    case Leapfrog.current(qlf.leapfrog) do
      {:ok, _value} ->
        next(qlf)

      :exhausted ->
        search(qlf)
    end
  end
end
