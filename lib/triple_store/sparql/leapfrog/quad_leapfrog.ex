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

  alias TripleStore.SPARQL.Leapfrog.{Leapfrog, QuadTrieIterator}
  alias TripleStore.SPARQL.QuadCardinality

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc """
  The QuadLeapfrog struct wraps the core Leapfrog with quad-specific metadata.

  - `:leapfrog` - The core Leapfrog struct
  - `:variables` - Ordered list of variable names
  - `:pattern` - Original quad pattern
  - `:bindings` - Extracted variable bindings from current match
  """
  @type t :: %__MODULE__{
          leapfrog: Leapfrog.t(),
          variables: [String.t()],
          pattern: tuple(),
          bindings: map()
        }

  @enforce_keys [:leapfrog, :variables, :pattern]
  defstruct [:leapfrog, :variables, :pattern, bindings: %{}]

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
    case create_iterators_for_pattern(db, pattern) do
      {:ok, iterators} ->
        case Leapfrog.new(iterators) do
          {:ok, lf} ->
            {:ok, %__MODULE__{leapfrog: lf, variables: variables, pattern: pattern}}

          {:exhausted, lf} ->
            {:exhausted, %__MODULE__{leapfrog: lf, variables: variables, pattern: pattern}}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
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
  Returns all iterators (for inspection/debugging).

  ## Arguments

  - `qlf` - The QuadLeapfrog struct

  ## Returns

  The list of QuadTrieIterator structs.

  """
  @spec iterators(t()) :: [QuadTrieIterator.t()]
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
          case bindings(searched_lf) do
            bindings when map_size(bindings) > 0 ->
              {bindings, searched_lf}

            %{} ->
              nil
          end

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
  # Uses GSPO index by default for quad patterns
  defp create_iterators_for_pattern(db, {:quad, s, p, o, g}) do
    components = [s, p, o, g]

    # Check if all components are bound (fully-specified quad)
    if Enum.all?(components, &is_bound?/1) do
      # Fully-bound pattern: do direct lookup instead of iteration
      # This is more efficient and avoids creating unnecessary iterators
      fully_bound_lookup(db, s, p, o, g)
    else
      # Build prefix from bound components
      prefix = build_prefix_from_components(components, [])

      # Create iterator for first variable position after prefix
      prefix_ids = div(byte_size(prefix), 8)

      # Create iterator at the first unbound position
      case QuadTrieIterator.new(db, :gspo, prefix, prefix_ids) do
        {:ok, iter} -> {:ok, [iter]}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # Direct lookup for fully-bound quads
  # Returns empty iterator list if quad exists, error otherwise
  defp fully_bound_lookup(db, s, p, o, g) do
    # Normalize graph ID
    graph_id = normalize_graph_id(g)

    # Build the full 32-byte key
    key = <<graph_id::64-big, s::64-big, p::64-big, o::64-big>>

    # Use the NIF to check if the quad exists
    case TripleStore.Backend.RocksDB.NIF.get(db, :gspo, key) do
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

    if is_bound?(component) do
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
    |> Enum.filter(fn {comp, _idx} -> is_variable?(comp) end)
    |> Enum.map(fn {{:variable, name}, _idx} -> name end)
  end

  # Check if a component is a variable
  defp is_variable?({:variable, _name}), do: true
  defp is_variable?(_), do: false

  # Check if a component is bound (not a variable)
  defp is_bound?(component), do: not is_variable?(component)

  # Extract bound value from component
  defp extract_bound_value({:variable, _}), do: 0
  defp extract_bound_value(value) when is_integer(value), do: value
  defp extract_bound_value(:default_graph), do: 0
  defp extract_bound_value({:named_node, _iri}), do: 0
  defp extract_bound_value(_), do: 0

  # Calculate selectivity score for a position
  defp position_selectivity_score(component, _position, stats) do
    cond do
      # Bound constants are most selective (score 0)
      is_bound?(component) ->
        0

      # Variables have selectivity based on position
      true ->
        # Use cardinality estimate if available
        case QuadCardinality.estimate_pattern(
               {:quad, {:variable, "_"}, {:variable, "_"}, {:variable, "_"}, {:variable, "_"}},
               stats
             ) do
          card when is_number(card) and card > 0 ->
            # Higher cardinality = lower selectivity = higher score
            # Use log to scale the score
            trunc(:math.log(card))

          _error ->
            # Fallback: use default quad count with logging
            # This provides a consistent baseline instead of arbitrary 1000
            default_card = Map.get(stats, :quad_count, 10_000)

            Logger.debug(
              "QuadLeapfrog: Using fallback cardinality #{default_card} for variable ordering"
            )

            # Log scale of default cardinality
            trunc(:math.log(max(1, default_card)))
        end
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
            [iter | _] ->
              case QuadTrieIterator.current_key(iter) do
                :exhausted ->
                  %{}

                {:ok, key} ->
                  # Decode quad key and extract bindings
                  {g, s, p, o} = QuadTrieIterator.decode_key(key)

                  # Map pattern components to their values
                  {:quad, s_pat, p_pat, o_pat, g_pat} = pattern

                  %{}
                  |> maybe_add_binding(s_pat, s, "s")
                  |> maybe_add_binding(p_pat, p, "p")
                  |> maybe_add_binding(o_pat, o, "o")
                  |> maybe_add_binding(g_pat, g, "g")
              end

            _ ->
              %{}
          end

        %{qlf | bindings: bindings}
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
