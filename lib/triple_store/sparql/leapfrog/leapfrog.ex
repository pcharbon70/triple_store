defmodule TripleStore.SPARQL.Leapfrog.Leapfrog do
  @moduledoc """
  Core Leapfrog join algorithm for worst-case optimal multi-way joins.

  Leapfrog Triejoin finds the intersection of multiple sorted iterators by
  "leapfrogging" - repeatedly advancing the iterator with the smallest value
  to catch up to the largest value until all iterators align on a common value.

  ## Algorithm Overview

  Given k sorted iterators, the algorithm:
  1. Sorts iterators by their current value
  2. The iterator at position 0 (smallest) seeks to the value at position k-1 (largest)
  3. This may cause the formerly-smallest to become the largest
  4. Repeat until all iterators have the same value (found match) or one exhausts

  ## Usage

      # Create iterators for different triple patterns
      {:ok, iter1} = TrieIterator.new(db, :pos, <<pred1::64-big, obj1::64-big>>, 2)
      {:ok, iter2} = TrieIterator.new(db, :pos, <<pred2::64-big, obj2::64-big>>, 2)

      # Initialize leapfrog
      {:ok, lf} = Leapfrog.new([iter1, iter2])

      # Find common values
      {:ok, lf} = Leapfrog.search(lf)
      {:ok, value} = Leapfrog.current(lf)

      # Advance to next common value
      {:ok, lf} = Leapfrog.next(lf)

  ## Performance

  The algorithm is worst-case optimal: for k iterators each with at most n values,
  finding all common values takes O(k * n * log(n)) time in the worst case.
  In practice, it often performs much better due to the seek operation skipping
  large ranges of non-matching values.

  ## Design Notes

  The Leapfrog struct maintains iterators sorted by their current value.
  After each search or next operation, iterators are re-sorted to maintain
  the invariant that index 0 has the smallest value.
  """

  alias TripleStore.SPARQL.Leapfrog.TrieIterator

  # ===========================================================================
  # Types
  # ===========================================================================

  # Default maximum iterations to prevent DoS attacks
  @default_max_iterations 1_000_000

  @typedoc """
  The Leapfrog struct.

  - `:iterators` - List of TrieIterators sorted by current value
  - `:current_value` - The common value all iterators are at, or nil
  - `:exhausted` - Whether any iterator is exhausted (no more common values)
  - `:at_match` - Whether currently positioned at a match
  - `:iteration_count` - Number of search iterations performed
  - `:max_iterations` - Maximum allowed iterations (DoS protection)
  """
  @type t :: %__MODULE__{
          iterators: [TrieIterator.t()],
          current_value: non_neg_integer() | nil,
          exhausted: boolean(),
          at_match: boolean(),
          iteration_count: non_neg_integer(),
          max_iterations: non_neg_integer()
        }

  @enforce_keys [:iterators]
  defstruct [
    :iterators,
    :current_value,
    exhausted: false,
    at_match: false,
    iteration_count: 0,
    max_iterations: @default_max_iterations
  ]

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Creates a new Leapfrog join from a list of iterators.

  The iterators should all be positioned at their first value (or exhausted).
  This function initializes the Leapfrog state but does NOT search for the
  first common value - call `search/1` after initialization.

  ## Arguments

  - `iterators` - List of TrieIterator structs (must have at least 1)
  - `opts` - Options keyword list:
    - `:max_iterations` - Maximum search iterations (default: 1,000,000)

  ## Returns

  - `{:ok, leapfrog}` on success
  - `{:exhausted, leapfrog}` if any iterator is already exhausted
  - `{:error, reason}` on failure

  ## Examples

      {:ok, lf} = Leapfrog.new([iter1, iter2, iter3])
      {:ok, lf} = Leapfrog.search(lf)

      # With custom iteration limit
      {:ok, lf} = Leapfrog.new([iter1, iter2], max_iterations: 10_000)

  """
  @spec new([TrieIterator.t()], keyword()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def new(iterators, opts \\ [])

  def new([], _opts) do
    {:error, :empty_iterator_list}
  end

  def new(iterators, opts) when is_list(iterators) do
    max_iterations = Keyword.get(opts, :max_iterations, @default_max_iterations)

    # Check if any iterator is exhausted
    if Enum.any?(iterators, &TrieIterator.exhausted?/1) do
      {:exhausted,
       %__MODULE__{iterators: iterators, exhausted: true, max_iterations: max_iterations}}
    else
      # Sort iterators by current value
      sorted = sort_iterators(iterators)
      {:ok, %__MODULE__{iterators: sorted, max_iterations: max_iterations}}
    end
  end

  @doc """
  Searches for the next common value across all iterators.

  This implements the core leapfrog algorithm: repeatedly advance the
  iterator with the smallest value until all iterators agree.

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  - `{:ok, leapfrog}` if a common value was found
  - `{:exhausted, leapfrog}` if no more common values exist
  - `{:error, :max_iterations_exceeded}` if iteration limit reached (DoS protection)
  - `{:error, reason}` on other failures

  ## Examples

      {:ok, lf} = Leapfrog.search(lf)
      {:ok, value} = Leapfrog.current(lf)

  """
  @spec search(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def search(%__MODULE__{exhausted: true} = lf) do
    {:exhausted, lf}
  end

  def search(%__MODULE__{iteration_count: count, max_iterations: max})
      when count >= max do
    {:error, :max_iterations_exceeded}
  end

  def search(%__MODULE__{iterators: [single]} = lf) do
    # Single iterator case - always matches itself
    case TrieIterator.current(single) do
      {:ok, value} ->
        {:ok, %{lf | current_value: value, at_match: true}}

      :exhausted ->
        {:exhausted, %{lf | exhausted: true}}
    end
  end

  def search(%__MODULE__{iterators: iterators} = lf) do
    do_search(iterators, lf)
  end

  @doc """
  Advances past the current match to find the next common value.

  This advances the first iterator (which has the current common value)
  to its next value, then calls search to find the next intersection.

  ## Arguments

  - `lf` - The Leapfrog struct (must be at a match)

  ## Returns

  - `{:ok, leapfrog}` if another common value was found
  - `{:exhausted, leapfrog}` if no more common values exist
  - `{:error, reason}` on failure

  ## Examples

      {:ok, lf} = Leapfrog.next(lf)
      case Leapfrog.current(lf) do
        {:ok, value} -> # process next value
        :exhausted -> # done
      end

  """
  @spec next(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def next(%__MODULE__{exhausted: true} = lf) do
    {:exhausted, lf}
  end

  def next(%__MODULE__{at_match: false} = lf) do
    # Not at a match, just search
    search(lf)
  end

  def next(%__MODULE__{iterators: [first | rest]} = lf) do
    # Advance the first iterator past the current value
    case TrieIterator.next(first) do
      {:ok, advanced} ->
        # Re-sort and search for next common value
        new_iterators = sort_iterators([advanced | rest])
        search(%{lf | iterators: new_iterators, at_match: false, current_value: nil})

      {:exhausted, _exhausted} ->
        {:exhausted, %{lf | exhausted: true}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns the current common value.

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  - `{:ok, value}` if at a match
  - `:exhausted` if exhausted or not at a match

  """
  @spec current(t()) :: {:ok, non_neg_integer()} | :exhausted
  def current(%__MODULE__{exhausted: true}), do: :exhausted
  def current(%__MODULE__{at_match: false}), do: :exhausted
  def current(%__MODULE__{current_value: nil}), do: :exhausted
  def current(%__MODULE__{current_value: value}), do: {:ok, value}

  @doc """
  Checks if the Leapfrog is exhausted.

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  - `true` if exhausted
  - `false` otherwise

  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{exhausted: true}), do: true
  def exhausted?(%__MODULE__{}), do: false

  @doc """
  Returns all iterators (for inspection/debugging).

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  The list of iterators.

  """
  @spec iterators(t()) :: [TrieIterator.t()]
  def iterators(%__MODULE__{iterators: iters}), do: iters

  @doc """
  Closes all iterators and releases resources.

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  - `:ok`

  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{iterators: iterators}) do
    Enum.each(iterators, &TrieIterator.close/1)
    :ok
  end

  @doc """
  Creates a Stream that yields all common values.

  This is a convenient way to iterate through all matches without
  manual search/next calls.

  ## Arguments

  - `lf` - The Leapfrog struct

  ## Returns

  A Stream of common values.

  ## Examples

      {:ok, lf} = Leapfrog.new([iter1, iter2])
      values = Leapfrog.stream(lf) |> Enum.to_list()

  """
  @spec stream(t()) :: Enumerable.t()
  def stream(%__MODULE__{} = lf) do
    Stream.unfold(lf, fn lf ->
      case search_or_next(lf) do
        {:ok, searched_lf} ->
          case current(searched_lf) do
            {:ok, value} ->
              # Mark that we need to advance next time
              {value, %{searched_lf | at_match: true}}

            :exhausted ->
              nil
          end

        {:exhausted, _} ->
          nil
      end
    end)
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Sort iterators by their current value (ascending)
  defp sort_iterators(iterators) do
    Enum.sort_by(iterators, fn iter ->
      case TrieIterator.current(iter) do
        {:ok, value} -> value
        :exhausted -> :infinity
      end
    end)
  end

  # Get current value of an iterator, returning infinity for exhausted
  defp get_value(iter) do
    case TrieIterator.current(iter) do
      {:ok, value} -> value
      :exhausted -> :infinity
    end
  end

  # Core leapfrog search algorithm
  defp do_search(iterators, lf) do
    # Check iteration limit
    if lf.iteration_count >= lf.max_iterations do
      {:error, :max_iterations_exceeded}
    else
      # Increment iteration count
      lf = %{lf | iteration_count: lf.iteration_count + 1}

      # Get min and max values
      [first | _] = iterators
      last = List.last(iterators)

      min_val = get_value(first)
      max_val = get_value(last)

      cond do
        min_val == :infinity or max_val == :infinity ->
          # Some iterator exhausted
          {:exhausted, %{lf | exhausted: true}}

        min_val == max_val ->
          # All iterators at same value - found a match!
          {:ok, %{lf | iterators: iterators, current_value: min_val, at_match: true}}

        true ->
          # min < max: seek min iterator to max value
          case TrieIterator.seek(first, max_val) do
            {:ok, advanced} ->
              # Re-sort and continue searching
              new_iterators = sort_iterators([advanced | tl(iterators)])
              do_search(new_iterators, lf)

            {:exhausted, _} ->
              {:exhausted, %{lf | exhausted: true}}

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  # Helper for stream: search if not at match, otherwise advance then search
  defp search_or_next(%__MODULE__{at_match: true} = lf) do
    next(lf)
  end

  defp search_or_next(%__MODULE__{} = lf) do
    search(lf)
  end
end
