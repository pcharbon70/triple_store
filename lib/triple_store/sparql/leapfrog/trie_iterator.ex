defmodule TripleStore.SPARQL.Leapfrog.TrieIterator do
  @moduledoc """
  Trie iterator abstraction over RocksDB prefix scans for Leapfrog Triejoin.

  This module provides a stateful iterator interface that enables efficient
  variable-by-variable iteration required by the Leapfrog Triejoin algorithm.
  Unlike standard iterators that return full key-value pairs, the TrieIterator
  returns values at a specific "level" (position) within the key.

  ## Leapfrog Triejoin Background

  Leapfrog Triejoin is a worst-case optimal join algorithm that processes
  multi-way joins by iterating over sorted lists of values and "leapfrogging"
  to find common values. It requires iterators that can:

  1. Report their current value
  2. Seek to a target value (or the next value >= target)
  3. Advance to the next value
  4. Report when exhausted

  ## Key Structure

  Keys in the SPO/POS/OSP indices are 24-byte binaries with three 64-bit
  big-endian integers. The iterator extracts values at a specific level:

  - Level 0: First 8 bytes (e.g., Subject in SPO)
  - Level 1: Bytes 8-16 (e.g., Predicate in SPO)
  - Level 2: Bytes 16-24 (e.g., Object in SPO)

  ## Example

      # Create iterator over SPO index at level 0 (subjects)
      {:ok, iter} = TrieIterator.new(db, :spo, <<>>)

      # Seek to subject >= 100
      {:ok, iter} = TrieIterator.seek(iter, 100)

      # Get current value
      {:ok, 100} = TrieIterator.current(iter)

      # Advance to next distinct value
      {:ok, iter} = TrieIterator.next(iter)
      {:ok, 150} = TrieIterator.current(iter)

      # Check if exhausted
      false = TrieIterator.exhausted?(iter)

  ## Design Notes

  The iterator maintains its state immutably - each operation returns a new
  iterator struct. The underlying RocksDB iterator handle is mutable, but
  is managed through the NIF layer.
  """

  alias TripleStore.Backend.RocksDB.NIF

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc """
  The TrieIterator struct.

  - `:db` - Database reference
  - `:cf` - Column family (:spo, :pos, or :osp)
  - `:prefix` - Binary prefix to iterate within
  - `:level` - Which position in the key to extract (0, 1, or 2)
  - `:iter_ref` - RocksDB iterator reference
  - `:current_key` - Current full key or nil if exhausted
  - `:current_value` - Current extracted value at level, or nil if exhausted
  - `:exhausted` - Whether the iterator is exhausted
  """
  @type t :: %__MODULE__{
          db: reference(),
          cf: :spo | :pos | :osp,
          prefix: binary(),
          level: 0 | 1 | 2,
          iter_ref: reference() | nil,
          current_key: binary() | nil,
          current_value: non_neg_integer() | nil,
          exhausted: boolean()
        }

  @enforce_keys [:db, :cf, :prefix, :level]
  defstruct [
    :db,
    :cf,
    :prefix,
    :level,
    :iter_ref,
    :current_key,
    :current_value,
    exhausted: false
  ]

  # Size of each ID in bytes
  @id_size 8

  # Maximum 64-bit unsigned integer value
  @max_uint64 0xFFFFFFFFFFFFFFFF

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Creates a new trie iterator.

  ## Arguments

  - `db` - Database reference
  - `cf` - Column family (:spo, :pos, or :osp)
  - `prefix` - Binary prefix to constrain iteration
  - `level` - Which position to extract values from (0, 1, or 2)

  ## Returns

  - `{:ok, iterator}` on success
  - `{:error, reason}` on failure

  ## Examples

      # Iterate over all subjects in SPO
      {:ok, iter} = TrieIterator.new(db, :spo, <<>>, 0)

      # Iterate over predicates for a specific subject
      {:ok, iter} = TrieIterator.new(db, :spo, <<subject_id::64-big>>, 1)

      # Iterate over objects for a specific subject-predicate pair
      {:ok, iter} = TrieIterator.new(db, :spo, <<s_id::64-big, p_id::64-big>>, 2)

  """
  @spec new(reference(), :spo | :pos | :osp, binary(), 0 | 1 | 2) ::
          {:ok, t()} | {:error, term()}
  def new(db, cf, prefix, level) when cf in [:spo, :pos, :osp] and level in [0, 1, 2] do
    case NIF.prefix_iterator(db, cf, prefix) do
      {:ok, iter_ref} ->
        iter = %__MODULE__{
          db: db,
          cf: cf,
          prefix: prefix,
          level: level,
          iter_ref: iter_ref
        }

        # Position at first entry and extract current value
        case advance_to_first(iter) do
          {:ok, positioned_iter} -> {:ok, positioned_iter}
          {:exhausted, exhausted_iter} -> {:ok, exhausted_iter}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Seeks the iterator to a target value.

  After seeking, the iterator will be positioned at the first entry where
  the value at the configured level is >= target.

  ## Arguments

  - `iter` - The iterator
  - `target` - The target value to seek to

  ## Returns

  - `{:ok, iterator}` if positioned at a valid entry
  - `{:exhausted, iterator}` if no entries >= target exist
  - `{:error, reason}` on failure

  ## Examples

      {:ok, iter} = TrieIterator.seek(iter, 100)
      {:ok, 100} = TrieIterator.current(iter)

  """
  @spec seek(t(), non_neg_integer()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def seek(%__MODULE__{exhausted: true} = iter, _target) do
    {:exhausted, iter}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def seek(%__MODULE__{} = iter, target) when is_integer(target) and target >= 0 do
    # Build the seek key by appending target at the correct level
    seek_key = build_seek_key(iter.prefix, iter.level, target)

    case NIF.iterator_seek(iter.iter_ref, seek_key) do
      :ok ->
        # After seeking, get the current entry
        case NIF.iterator_next(iter.iter_ref) do
          {:ok, key, _value} ->
            if String.starts_with?(key, iter.prefix) do
              value = extract_value_at_level(key, iter.level)
              {:ok, %{iter | current_key: key, current_value: value, exhausted: false}}
            else
              # Seek went past the prefix boundary
              {:exhausted, %{iter | current_key: nil, current_value: nil, exhausted: true}}
            end

          :iterator_end ->
            {:exhausted, %{iter | current_key: nil, current_value: nil, exhausted: true}}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Advances the iterator to the next distinct value at the configured level.

  This skips over entries that have the same value at the current level,
  which is essential for Leapfrog Triejoin to enumerate distinct values.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `{:ok, iterator}` if advanced to next value
  - `{:exhausted, iterator}` if no more values
  - `{:error, reason}` on failure

  ## Examples

      # If current value is 100, next() advances to the first entry with value > 100
      {:ok, iter} = TrieIterator.next(iter)
      {:ok, 150} = TrieIterator.current(iter)

  """
  @spec next(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def next(%__MODULE__{exhausted: true} = iter) do
    {:exhausted, iter}
  end

  def next(%__MODULE__{current_value: nil} = iter) do
    {:exhausted, %{iter | exhausted: true}}
  end

  def next(%__MODULE__{current_value: @max_uint64} = iter) do
    # At maximum 64-bit value, cannot advance further (overflow protection)
    {:exhausted, %{iter | current_key: nil, current_value: nil, exhausted: true}}
  end

  def next(%__MODULE__{} = iter) do
    # To skip to the next distinct value, we seek to current_value + 1
    next_target = iter.current_value + 1
    seek(iter, next_target)
  end

  @doc """
  Returns the current value at the iterator's configured level.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `{:ok, value}` if positioned at a valid entry
  - `:exhausted` if the iterator is exhausted

  ## Examples

      {:ok, 100} = TrieIterator.current(iter)

  """
  @spec current(t()) :: {:ok, non_neg_integer()} | :exhausted
  def current(%__MODULE__{exhausted: true}), do: :exhausted
  def current(%__MODULE__{current_value: nil}), do: :exhausted
  def current(%__MODULE__{current_value: value}), do: {:ok, value}

  @doc """
  Returns the current full key if available.

  This is useful for extracting all values from the current entry,
  not just the value at the configured level.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `{:ok, key}` if positioned at a valid entry
  - `:exhausted` if the iterator is exhausted

  """
  @spec current_key(t()) :: {:ok, binary()} | :exhausted
  def current_key(%__MODULE__{exhausted: true}), do: :exhausted
  def current_key(%__MODULE__{current_key: nil}), do: :exhausted
  def current_key(%__MODULE__{current_key: key}), do: {:ok, key}

  @doc """
  Checks if the iterator is exhausted.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `true` if exhausted
  - `false` if positioned at a valid entry

  ## Examples

      false = TrieIterator.exhausted?(iter)
      {:ok, iter} = TrieIterator.next(iter)
      true = TrieIterator.exhausted?(iter)  # After last entry

  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{exhausted: true}), do: true
  def exhausted?(%__MODULE__{}), do: false

  @doc """
  Closes the iterator and releases resources.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `:ok`

  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{iter_ref: nil}), do: :ok

  def close(%__MODULE__{iter_ref: iter_ref}) do
    NIF.iterator_close(iter_ref)
    :ok
  end

  @doc """
  Extracts a specific ID from a 24-byte key.

  ## Arguments

  - `key` - The 24-byte index key
  - `level` - Which position (0, 1, or 2)

  ## Returns

  The 64-bit integer at the specified position.

  """
  @spec extract_value_at_level(binary(), 0 | 1 | 2) :: non_neg_integer()
  def extract_value_at_level(key, level) when byte_size(key) >= 24 and level in [0, 1, 2] do
    offset = level * @id_size
    <<_::binary-size(offset), value::64-big, _::binary>> = key
    value
  end

  @doc """
  Decodes a 24-byte key into its three component IDs.

  ## Arguments

  - `key` - The 24-byte index key

  ## Returns

  A tuple of three integers {first, second, third}.

  """
  @spec decode_key(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def decode_key(<<first::64-big, second::64-big, third::64-big>>) do
    {first, second, third}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Advances to the first entry and extracts the value
  defp advance_to_first(iter) do
    case NIF.iterator_next(iter.iter_ref) do
      {:ok, key, _value} ->
        if String.starts_with?(key, iter.prefix) do
          value = extract_value_at_level(key, iter.level)
          {:ok, %{iter | current_key: key, current_value: value}}
        else
          {:exhausted, %{iter | current_key: nil, current_value: nil, exhausted: true}}
        end

      :iterator_end ->
        {:exhausted, %{iter | current_key: nil, current_value: nil, exhausted: true}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Builds a seek key by extending the prefix with the target at the given level
  defp build_seek_key(prefix, level, target) do
    # The prefix length tells us how many complete IDs are already bound
    prefix_ids = div(byte_size(prefix), @id_size)

    cond do
      level == prefix_ids ->
        # Target is the next position after prefix - simple append
        prefix <> <<target::64-big>>

      level > prefix_ids ->
        # Level is beyond prefix - need to pad intermediate levels with 0
        # This happens when we want to iterate at level 2 but only have level 0 prefix
        # E.g., POS index with P-only prefix, seeking subject at level 2
        # Pad with zeros for skipped levels, then add target
        padding_levels = level - prefix_ids
        padding = :binary.copy(<<0::64-big>>, padding_levels)
        prefix <> padding <> <<target::64-big>>

      true ->
        # level < prefix_ids - shouldn't happen, but handle gracefully
        prefix <> <<target::64-big>>
    end
  end
end
