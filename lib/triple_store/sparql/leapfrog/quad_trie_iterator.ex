defmodule TripleStore.SPARQL.Leapfrog.QuadTrieIterator do
  @moduledoc """
  Quad trie iterator for 32-byte quad keys in Leapfrog Triejoin.

  Extends the TrieIterator concept to handle quad patterns with 4 components
  (subject, predicate, object, graph). Quad keys are 32 bytes with four 64-bit
  big-endian integers.

  ## Key Structure

  Quad keys in the GSPO/GPOS/SPOG/POSG indices are 32-byte binaries:

  - GSPO: Graph(8) | Subject(8) | Predicate(8) | Object(8)
  - GPOS: Graph(8) | Predicate(8) | Object(8) | Subject(8)
  - SPOG: Subject(8) | Predicate(8) | Object(8) | Graph(8)
  - POSG: Predicate(8) | Object(8) | Subject(8) | Graph(8)

  The iterator extracts values at a specific "level" (position) within the key:
  - Level 0: First 8 bytes
  - Level 1: Bytes 8-16
  - Level 2: Bytes 16-24
  - Level 3: Bytes 24-32

  ## Example

      # Create iterator over GSPO index at level 0 (graphs)
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Seek to graph >= 100
      {:ok, iter} = QuadTrieIterator.seek(iter, 100)

      # Get current value
      {:ok, 100} = QuadTrieIterator.current(iter)

      # Advance to next distinct value
      {:ok, iter} = QuadTrieIterator.next(iter)

  ## Design Notes

  The core Leapfrog algorithm works with any iterator that provides:
  - current/1: Returns current value or :exhausted
  - seek/2: Seeks to target value
  - next/1: Advances to next distinct value
  - exhausted?/1: Checks if exhausted

  This makes QuadTrieIterator compatible with the existing Leapfrog
  algorithm for 4-way joins on quad patterns.
  """

  alias TripleStore.Backend.RocksDB.NIF

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc """
  The QuadTrieIterator struct.

  - `:db` - Database reference (adapter PID)
  - `:cf` - Column family (:gspo, :gpos, :spog, or :posg)
  - `:prefix` - Binary prefix to iterate within
  - `:level` - Which position in the key to extract (0, 1, 2, or 3)
  - `:iter_ref` - RocksDB iterator reference (iterator PID)
  - `:current_key` - Current full key or nil if exhausted
  - `:current_value` - Current extracted value at level, or nil if exhausted
  - `:exhausted` - Whether the iterator is exhausted
  """
  @type t :: %__MODULE__{
          db: pid(),
          cf: :gspo | :gpos | :spog | :posg,
          prefix: binary(),
          level: 0 | 1 | 2 | 3,
          iter_ref: pid() | nil,
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
  Creates a new quad trie iterator.

  ## Arguments

  - `db` - Database reference
  - `cf` - Column family (:gspo, :gpos, :spog, or :posg)
  - `prefix` - Binary prefix to constrain iteration
  - `level` - Which position to extract values from (0, 1, 2, or 3)

  ## Returns

  - `{:ok, iterator}` on success
  - `{:error, reason}` on failure

  ## Examples

      # Iterate over all graphs in GSPO
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Iterate over subjects for a specific graph
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<graph_id::64-big>>, 1)

      # Iterate over predicates for a specific graph-subject pair
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<g_id::64-big, s_id::64-big>>, 2)

      # Iterate over objects for a specific graph-subject-predicate triple
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<g_id::64-big, s_id::64-big, p_id::64-big>>, 3)

  """
  @spec new(pid(), :gspo | :gpos | :spog | :posg, binary(), 0 | 1 | 2 | 3) ::
          {:ok, t()} | {:error, term()}
  def new(db, cf, prefix, level) when cf in [:gspo, :gpos, :spog, :posg] and level in [0, 1, 2, 3] do
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

  """
  @spec seek(t(), non_neg_integer()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def seek(%__MODULE__{exhausted: true} = iter, _target) do
    {:exhausted, iter}
  end

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

  """
  @spec current(t()) :: {:ok, non_neg_integer()} | :exhausted
  def current(%__MODULE__{exhausted: true}), do: :exhausted
  def current(%__MODULE__{current_value: nil}), do: :exhausted
  def current(%__MODULE__{current_value: value}), do: {:ok, value}

  @doc """
  Returns the current full key if available.

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
  Extracts a specific ID from a 32-byte quad key.

  ## Arguments

  - `key` - The 32-byte index key
  - `level` - Which position (0, 1, 2, or 3)

  ## Returns

  The 64-bit integer at the specified position.

  ## Examples

      iex> QuadTrieIterator.extract_value_at_level(<<g::64, s::64, p::64, o::64>>, 1)
      s

  """
  @spec extract_value_at_level(binary(), 0 | 1 | 2 | 3) :: non_neg_integer()
  def extract_value_at_level(key, level) when byte_size(key) >= 32 and level in [0, 1, 2, 3] do
    offset = level * @id_size
    <<_::binary-size(offset), value::64-big, _::binary>> = key
    value
  end

  @doc """
  Decodes a 32-byte quad key into its four component IDs.

  ## Arguments

  - `key` - The 32-byte index key

  ## Returns

  A tuple of four integers {first, second, third, fourth}.

  ## Examples

      iex> QuadTrieIterator.decode_key(<<g::64, s::64, p::64, o::64>>)
      {g, s, p, o}

  """
  @spec decode_key(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def decode_key(<<first::64-big, second::64-big, third::64-big, fourth::64-big>>) do
    {first, second, third, fourth}
  end

  @doc """
  Extracts all four values from the current key as a map.

  Returns a map with position keys (:pos0, :pos1, :pos2, :pos3) and values.

  ## Arguments

  - `iter` - The iterator

  ## Returns

  - `{:ok, map}` with all four values if positioned at a valid entry
  - `:exhausted` if exhausted

  """
  @spec extract_binding(t()) :: {:ok, map()} | :exhausted
  def extract_binding(%__MODULE__{exhausted: true}), do: :exhausted

  def extract_binding(%__MODULE__{current_key: nil}), do: :exhausted

  def extract_binding(%__MODULE__{current_key: key}) do
    values = decode_key(key)
    {:ok, %{pos0: elem(values, 0), pos1: elem(values, 1), pos2: elem(values, 2), pos3: elem(values, 3)}}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  @doc """
  Advances the iterator to the first entry and extracts the value.

  ## Returns

  - `{:ok, iterator}` if positioned at a valid entry
  - `{:exhausted, iterator}` if no entries exist
  - `{:error, reason}` on failure

  """
  @spec advance_to_first(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
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

  @doc """
  Builds a seek key by extending the prefix with the target at the given level.

  The seek key is used to position the iterator at the first entry where
  the value at the specified level is >= target.

  """
  @spec build_seek_key(binary(), 0 | 1 | 2 | 3, non_neg_integer()) :: binary()
  defp build_seek_key(prefix, level, target) do
    # The prefix length tells us how many complete IDs are already bound
    prefix_ids = div(byte_size(prefix), @id_size)

    cond do
      level == prefix_ids ->
        # Target is the next position after prefix - simple append
        prefix <> <<target::64-big>>

      level > prefix_ids ->
        # Level is beyond prefix - need to pad intermediate levels with 0
        padding_levels = level - prefix_ids
        padding = :binary.copy(<<0::64-big>>, padding_levels)
        prefix <> padding <> <<target::64-big>>

      true ->
        # level < prefix_ids - shouldn't happen, but handle gracefully
        prefix <> <<target::64-big>>
    end
  end
end

# Protocol implementation for polymorphic Leapfrog support
defimpl TripleStore.SPARQL.Leapfrog.TrieIteratorProtocol, for: TripleStore.SPARQL.Leapfrog.QuadTrieIterator do
  def current(iter), do: TripleStore.SPARQL.Leapfrog.QuadTrieIterator.current(iter)
  def seek(iter, target), do: TripleStore.SPARQL.Leapfrog.QuadTrieIterator.seek(iter, target)
  def next(iter), do: TripleStore.SPARQL.Leapfrog.QuadTrieIterator.next(iter)
  def exhausted?(iter), do: TripleStore.SPARQL.Leapfrog.QuadTrieIterator.exhausted?(iter)
  def close(iter), do: TripleStore.SPARQL.Leapfrog.QuadTrieIterator.close(iter)
end
