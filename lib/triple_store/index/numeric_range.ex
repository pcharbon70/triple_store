defmodule TripleStore.Index.NumericRange do
  @moduledoc """
  Numeric range index for efficient range queries on numeric predicates.

  This module implements a secondary index that enables O(k) range queries on numeric
  values, where k is the number of matching results. This is critical for BSBM Q7
  which filters offers by price range.

  ## Key Format

  The index stores entries in the `numeric_range` column family with keys in the format:

      <<predicate_id::64-big, sortable_value::64-big, subject_id::64-big>>

  Where `sortable_value` is the float converted to sortable bytes using IEEE 754
  sign-magnitude to two's complement conversion.

  ## Usage

      # Register a predicate for numeric range indexing
      {:ok, _} = NumericRange.create_range_index(db, predicate_uri)

      # Query a range
      {:ok, results} = NumericRange.range_query(db, predicate_uri, 50.0, 500.0)

  ## Float Ordering

  IEEE 754 floats don't sort lexicographically as raw bytes. This module converts
  floats to sortable bytes by:

  1. For positive floats: flip the sign bit (0 -> 1)
  2. For negative floats: flip all bits

  This ensures proper lexicographic ordering while preserving the ability to decode
  back to the original float value.
  """

  import Bitwise

  alias TripleStore.Backend.RocksDB.NIF

  @type db_ref :: NIF.db_ref()
  @type predicate_id :: non_neg_integer()
  @type subject_id :: non_neg_integer()

  # Column family for numeric range index
  @cf :numeric_range

  # ETS table for registered range predicates
  @range_predicates_table :numeric_range_predicates

  @doc """
  Converts a float to sortable bytes for lexicographic ordering.

  IEEE 754 floats don't sort correctly as raw bytes because:
  - Positive floats need their sign bit flipped to sort after negative ones
  - Negative floats need all bits flipped to reverse their order

  This function applies the necessary transformations to ensure floats sort
  correctly when stored as big-endian bytes.

  ## Examples

      iex> NumericRange.float_to_sortable_bytes(0.0) < NumericRange.float_to_sortable_bytes(1.0)
      true

      iex> NumericRange.float_to_sortable_bytes(-1.0) < NumericRange.float_to_sortable_bytes(0.0)
      true

      iex> NumericRange.float_to_sortable_bytes(-100.0) < NumericRange.float_to_sortable_bytes(-1.0)
      true

  """
  @spec float_to_sortable_bytes(float()) :: <<_::64>>
  def float_to_sortable_bytes(value) when is_float(value) do
    <<bits::64-unsigned-big>> = <<value::64-float-big>>

    sorted_bits =
      if (bits >>> 63) == 1 do
        # Negative float: flip all bits
        bxor(bits, 0xFFFFFFFFFFFFFFFF)
      else
        # Positive float (including +0.0): flip sign bit
        bxor(bits, 0x8000000000000000)
      end

    <<sorted_bits::64-unsigned-big>>
  end

  def float_to_sortable_bytes(value) when is_integer(value) do
    float_to_sortable_bytes(value * 1.0)
  end

  @doc """
  Converts sortable bytes back to the original float value.

  This is the inverse of `float_to_sortable_bytes/1`.

  ## Examples

      iex> value = 123.456
      iex> value == NumericRange.sortable_bytes_to_float(NumericRange.float_to_sortable_bytes(value))
      true

  """
  @spec sortable_bytes_to_float(<<_::64>>) :: float()
  def sortable_bytes_to_float(<<sorted_bits::64-unsigned-big>>) do
    original_bits =
      if (sorted_bits >>> 63) == 1 do
        # Was positive: flip sign bit back
        bxor(sorted_bits, 0x8000000000000000)
      else
        # Was negative: flip all bits back
        bxor(sorted_bits, 0xFFFFFFFFFFFFFFFF)
      end

    <<value::64-float-big>> = <<original_bits::64-unsigned-big>>
    value
  end

  @doc """
  Initializes the numeric range index system.

  Creates the ETS table for tracking registered predicates. Should be called
  during application startup. Safe to call concurrently - uses atomic table
  creation to avoid race conditions.
  """
  @spec init() :: :ok
  def init do
    alias TripleStore.ETSHelper
    ETSHelper.ensure_table!(@range_predicates_table, [:set, :public, :named_table, read_concurrency: true])
  end

  @doc """
  Registers a predicate for numeric range indexing.

  After registration, all triples with this predicate and numeric objects will be
  indexed for range queries. The predicate must be encoded to an ID before calling.

  ## Arguments
  - `db` - The database reference
  - `predicate_id` - The dictionary-encoded predicate ID

  ## Returns
  - `{:ok, predicate_id}` on success
  - `{:error, reason}` on failure
  """
  @spec create_range_index(db_ref(), predicate_id()) :: {:ok, predicate_id()} | {:error, term()}
  def create_range_index(_db, predicate_id) when is_integer(predicate_id) do
    init()
    :ets.insert(@range_predicates_table, {predicate_id, true})
    {:ok, predicate_id}
  end

  @doc """
  Checks if a predicate has a numeric range index.

  ## Arguments
  - `predicate_id` - The dictionary-encoded predicate ID

  ## Returns
  - `true` if the predicate is indexed
  - `false` otherwise
  """
  @spec has_range_index?(predicate_id()) :: boolean()
  def has_range_index?(predicate_id) when is_integer(predicate_id) do
    case :ets.whereis(@range_predicates_table) do
      :undefined -> false
      _table -> :ets.member(@range_predicates_table, predicate_id)
    end
  end

  @doc """
  Lists all predicates with numeric range indices.

  ## Returns
  - List of predicate IDs that have range indices
  """
  @spec list_range_predicates() :: [predicate_id()]
  def list_range_predicates do
    case :ets.whereis(@range_predicates_table) do
      :undefined ->
        []

      _table ->
        @range_predicates_table
        |> :ets.tab2list()
        |> Enum.map(fn {id, _} -> id end)
    end
  end

  @doc """
  Queries the range index for subjects with values in the given range.

  ## Arguments
  - `db` - The database reference
  - `predicate_id` - The dictionary-encoded predicate ID
  - `min` - Minimum value (inclusive), or `:unbounded` for no lower bound
  - `max` - Maximum value (inclusive), or `:unbounded` for no upper bound

  ## Returns
  - `{:ok, [{subject_id, value}, ...]}` with matching subjects and their values
  - `{:error, reason}` on failure

  ## Examples

      {:ok, results} = NumericRange.range_query(db, price_predicate_id, 50.0, 500.0)
      # Returns offers with prices between 50 and 500

  """
  @spec range_query(db_ref(), predicate_id(), float() | :unbounded, float() | :unbounded) ::
          {:ok, [{subject_id(), float()}]} | {:error, term()}
  def range_query(db, predicate_id, min, max) do
    # Build the prefix for this predicate
    prefix = <<predicate_id::64-unsigned-big>>

    # Get the full key bounds
    min_key = build_range_key(predicate_id, min, :min)
    max_key = build_range_key(predicate_id, max, :max)

    # Use prefix iterator starting from min_key
    case NIF.prefix_iterator(db, @cf, prefix) do
      {:ok, iter} ->
        # Seek to the minimum key if bounded
        case min do
          :unbounded -> :ok
          _ -> NIF.iterator_seek(iter, min_key)
        end

        # Collect results up to max_key
        results = collect_range_results(iter, max_key, max)
        NIF.iterator_close(iter)
        {:ok, results}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Builds a range key for the given predicate and value
  # Keys are 24 bytes: predicate_id(8) + sortable_value(8) + subject_id(8)
  defp build_range_key(predicate_id, :unbounded, :min) do
    # Start from the beginning of this predicate's range
    <<predicate_id::64-unsigned-big, 0::64, 0::64>>
  end

  defp build_range_key(predicate_id, :unbounded, :max) do
    # End at the end of this predicate's range
    <<predicate_id::64-unsigned-big, 0xFFFFFFFFFFFFFFFF::64, 0xFFFFFFFFFFFFFFFF::64>>
  end

  defp build_range_key(predicate_id, value, :min) when is_float(value) or is_integer(value) do
    sortable = float_to_sortable_bytes(value * 1.0)
    # Min key: use 0 for subject_id to include all subjects at this value
    <<predicate_id::64-unsigned-big>> <> sortable <> <<0::64>>
  end

  defp build_range_key(predicate_id, value, :max) when is_float(value) or is_integer(value) do
    sortable = float_to_sortable_bytes(value * 1.0)
    # Max key: use max subject_id to include all subjects at this value
    <<predicate_id::64-unsigned-big>> <> sortable <> <<0xFFFFFFFFFFFFFFFF::64>>
  end

  # Collects results from iterator up to max_key
  defp collect_range_results(iter, max_key, max_value) do
    collect_range_results(iter, max_key, max_value, [])
  end

  defp collect_range_results(iter, max_key, max_value, acc) do
    case NIF.iterator_next(iter) do
      {:ok, key, _value} ->
        # Check if we've passed the max key
        if key > max_key do
          Enum.reverse(acc)
        else
          case parse_range_key(key) do
            {:ok, _predicate_id, float_value, subject_id} ->
              # Double-check value is within range (for :unbounded max)
              if max_value == :unbounded or float_value <= max_value do
                collect_range_results(iter, max_key, max_value, [{subject_id, float_value} | acc])
              else
                Enum.reverse(acc)
              end

            :error ->
              # Skip malformed keys
              collect_range_results(iter, max_key, max_value, acc)
          end
        end

      :iterator_end ->
        Enum.reverse(acc)

      {:error, _} ->
        Enum.reverse(acc)
    end
  end

  # Parses a range key back into its components
  defp parse_range_key(
         <<predicate_id::64-unsigned-big, sortable::binary-size(8), subject_id::64-unsigned-big>>
       ) do
    float_value = sortable_bytes_to_float(sortable)
    {:ok, predicate_id, float_value, subject_id}
  end

  defp parse_range_key(_), do: :error

  @doc """
  Indexes a single triple's numeric value.

  Call this when inserting a triple with a registered numeric predicate.

  ## Arguments
  - `db` - The database reference
  - `predicate_id` - The dictionary-encoded predicate ID
  - `subject_id` - The dictionary-encoded subject ID
  - `value` - The numeric value to index

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec index_value(db_ref(), predicate_id(), subject_id(), float()) :: :ok | {:error, term()}
  def index_value(db, predicate_id, subject_id, value) when is_float(value) or is_integer(value) do
    key = build_index_key(predicate_id, value * 1.0, subject_id)
    # Value is empty - all data is in the key
    NIF.put(db, @cf, key, <<>>)
  end

  @doc """
  Removes a numeric value from the index.

  Call this when deleting a triple with a registered numeric predicate.

  ## Arguments
  - `db` - The database reference
  - `predicate_id` - The dictionary-encoded predicate ID
  - `subject_id` - The dictionary-encoded subject ID
  - `value` - The numeric value to remove

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec delete_value(db_ref(), predicate_id(), subject_id(), float()) :: :ok | {:error, term()}
  def delete_value(db, predicate_id, subject_id, value)
      when is_float(value) or is_integer(value) do
    key = build_index_key(predicate_id, value * 1.0, subject_id)
    NIF.delete(db, @cf, key)
  end

  @doc """
  Builds index operations for a batch write.

  Returns a list of `{:put, :numeric_range, key, value}` tuples for use with
  `NIF.write_batch/3` or `NIF.mixed_batch/3`.

  ## Arguments
  - `predicate_id` - The dictionary-encoded predicate ID
  - `subject_id` - The dictionary-encoded subject ID
  - `value` - The numeric value to index

  ## Returns
  - `{:put, :numeric_range, key, <<>>}` operation tuple
  """
  @spec build_index_operation(predicate_id(), subject_id(), float()) ::
          {:put, :numeric_range, binary(), binary()}
  def build_index_operation(predicate_id, subject_id, value)
      when is_float(value) or is_integer(value) do
    key = build_index_key(predicate_id, value * 1.0, subject_id)
    {:put, @cf, key, <<>>}
  end

  @doc """
  Builds a delete operation for batch removal from the index.

  Returns a `{:delete, :numeric_range, key}` tuple for use with
  `NIF.delete_batch/3` or `NIF.mixed_batch/3`.

  ## Arguments
  - `predicate_id` - The dictionary-encoded predicate ID
  - `subject_id` - The dictionary-encoded subject ID
  - `value` - The numeric value to remove

  ## Returns
  - `{:delete, :numeric_range, key}` operation tuple
  """
  @spec build_delete_operation(predicate_id(), subject_id(), float()) ::
          {:delete, :numeric_range, binary()}
  def build_delete_operation(predicate_id, subject_id, value)
      when is_float(value) or is_integer(value) do
    key = build_index_key(predicate_id, value * 1.0, subject_id)
    {:delete, @cf, key}
  end

  # Builds the full index key
  defp build_index_key(predicate_id, value, subject_id) do
    sortable = float_to_sortable_bytes(value)
    <<predicate_id::64-unsigned-big>> <> sortable <> <<subject_id::64-unsigned-big>>
  end
end
