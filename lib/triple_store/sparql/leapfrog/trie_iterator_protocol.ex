defprotocol TripleStore.SPARQL.Leapfrog.TrieIteratorProtocol do
  @moduledoc """
  Protocol for trie iterators used in Leapfrog joins.

  This protocol defines the common interface that both TrieIterator and
  QuadTrieIterator must implement to work with the Leapfrog algorithm.

  The protocol enables polymorphic iterator handling, allowing the core
  Leapfrog algorithm to work with both 24-byte (triple) and 32-byte (quad)
  keys without type-specific code.

  ## Required Functions

  Implementations must provide:
  - `current/1` - Returns current value or :exhausted
  - `seek/2` - Seeks iterator to target value
  - `next/1` - Advances to next distinct value
  - `exhausted?/1` - Checks if iterator is exhausted

  ## Example

      defimpl TrieIteratorProtocol, for: TripleStore.SPARQL.Leapfrog.TrieIterator do
        def current(iter), do: TrieIterator.current(iter)
        def seek(iter, target), do: TrieIterator.seek(iter, target)
        def next(iter), do: TrieIterator.next(iter)
        def exhausted?(iter), do: TrieIterator.exhausted?(iter)
      end

  """

  @doc """
  Returns the current value at the iterator's configured level.

  ## Returns

  - `{:ok, value}` if positioned at a valid entry
  - `:exhausted` if the iterator is exhausted

  """
  @spec current(t()) :: {:ok, non_neg_integer()} | :exhausted
  def current(iterator)

  @doc """
  Seeks the iterator to a target value.

  After seeking, the iterator will be positioned at the first entry where
  the value at the configured level is >= target.

  ## Returns

  - `{:ok, iterator}` if positioned at a valid entry
  - `{:exhausted, iterator}` if no entries >= target exist

  """
  @spec seek(t(), non_neg_integer()) :: {:ok, t()} | {:exhausted, t()}
  def seek(iterator, target)

  @doc """
  Advances the iterator to the next distinct value at the configured level.

  ## Returns

  - `{:ok, iterator}` if advanced to next value
  - `{:exhausted, iterator}` if no more values

  """
  @spec next(t()) :: {:ok, t()} | {:exhausted, t()}
  def next(iterator)

  @doc """
  Checks if the iterator is exhausted.

  ## Returns

  - `true` if exhausted
  - `false` if positioned at a valid entry

  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(iterator)

  @doc """
  Closes the iterator and releases resources.

  ## Returns

  - `:ok`

  """
  @spec close(t()) :: :ok
  def close(iterator)
end
