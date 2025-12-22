defmodule TripleStore.Statistics do
  @moduledoc """
  Basic statistics collection for the triple store.

  Provides functions to compute statistics about the stored triples,
  including counts and cardinality estimates. These statistics are used
  by the query optimizer to estimate result sizes and select efficient
  query plans.

  ## Features

  - **Triple counts**: Total count and per-predicate counts
  - **Distinct counts**: Approximate counts of distinct subjects, predicates, objects
  - **Efficient implementation**: Uses index structure for fast counting

  ## Usage

      # Get total triple count
      {:ok, count} = Statistics.triple_count(db)

      # Get count for a specific predicate
      {:ok, count} = Statistics.predicate_count(db, predicate_id)

      # Get approximate distinct counts
      {:ok, count} = Statistics.distinct_subjects(db)
      {:ok, count} = Statistics.distinct_predicates(db)
      {:ok, count} = Statistics.distinct_objects(db)

  ## Performance Notes

  - `triple_count/1` uses the SPO index for full scan
  - `predicate_count/2` uses the POS index for efficient counting
  - `distinct_*` functions scan their respective primary indices
  - For large datasets, consider caching results (see Task 1.6.2)
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "64-bit term ID"
  @type term_id :: non_neg_integer()

  # ===========================================================================
  # Public API - Triple Counts
  # ===========================================================================

  @doc """
  Returns the total number of triples in the store.

  Counts all entries in the SPO index to get the total triple count.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Total number of triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.triple_count(db)
      iex> count
      42

  ## Performance

  This function performs a full scan of the SPO index and counts entries.
  For large datasets, consider using the Statistics Cache (Task 1.6.2).
  """
  @spec triple_count(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def triple_count(db) do
    Index.count(db, {:var, :var, :var})
  end

  @doc """
  Returns the number of triples with a specific predicate.

  Uses the POS index for efficient predicate-based counting.

  ## Arguments

  - `db` - Database reference
  - `predicate_id` - The term ID of the predicate to count

  ## Returns

  - `{:ok, count}` - Number of triples with this predicate
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.predicate_count(db, rdf_type_id)
      iex> count
      1000

  ## Performance

  This function uses the POS index prefix scan for efficient counting
  of all triples with the given predicate.
  """
  @spec predicate_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def predicate_count(db, predicate_id) when is_integer(predicate_id) and predicate_id >= 0 do
    Index.count(db, {:var, {:bound, predicate_id}, :var})
  end

  # ===========================================================================
  # Public API - Distinct Counts
  # ===========================================================================

  @doc """
  Returns the approximate count of distinct subjects.

  Scans the SPO index and counts unique subject IDs. This provides
  an exact count but may be slow for very large datasets.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct subjects
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.distinct_subjects(db)
      iex> count
      500

  ## Implementation

  Uses the SPO index, iterating and tracking when the subject ID changes.
  Each unique 8-byte prefix in the SPO index represents a distinct subject.
  """
  @spec distinct_subjects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_subjects(db) do
    count_distinct_by_position(db, :spo, 0)
  end

  @doc """
  Returns the exact count of distinct predicates.

  Since predicates are typically a small set (schema properties),
  this provides an exact count efficiently.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct predicates
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.distinct_predicates(db)
      iex> count
      25

  ## Implementation

  Uses the POS index, iterating and tracking when the predicate ID changes.
  Each unique 8-byte prefix in the POS index represents a distinct predicate.
  """
  @spec distinct_predicates(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_predicates(db) do
    count_distinct_by_position(db, :pos, 0)
  end

  @doc """
  Returns the approximate count of distinct objects.

  Scans the OSP index and counts unique object IDs. This provides
  an exact count but may be slow for very large datasets.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct objects
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.distinct_objects(db)
      iex> count
      800

  ## Implementation

  Uses the OSP index, iterating and tracking when the object ID changes.
  Each unique 8-byte prefix in the OSP index represents a distinct object.
  """
  @spec distinct_objects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_objects(db) do
    count_distinct_by_position(db, :osp, 0)
  end

  # ===========================================================================
  # Public API - Bulk Statistics
  # ===========================================================================

  @doc """
  Returns all basic statistics in a single call.

  Computes all statistics together for convenience. For cached access,
  use the Statistics GenServer (Task 1.6.2).

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats_map}` - Map with all statistics
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, stats} = Statistics.all(db)
      iex> stats
      %{
        triple_count: 1000,
        distinct_subjects: 300,
        distinct_predicates: 15,
        distinct_objects: 500
      }
  """
  @spec all(db_ref()) :: {:ok, map()} | {:error, term()}
  def all(db) do
    with {:ok, tc} <- triple_count(db),
         {:ok, ds} <- distinct_subjects(db),
         {:ok, dp} <- distinct_predicates(db),
         {:ok, do_} <- distinct_objects(db) do
      {:ok,
       %{
         triple_count: tc,
         distinct_subjects: ds,
         distinct_predicates: dp,
         distinct_objects: do_
       }}
    end
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Count distinct values at the first position of an index.
  # For SPO, position 0 gives distinct subjects.
  # For POS, position 0 gives distinct predicates.
  # For OSP, position 0 gives distinct objects.
  @spec count_distinct_by_position(db_ref(), :spo | :pos | :osp, non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp count_distinct_by_position(db, cf, _position) do
    # Use prefix_stream with empty prefix for full scan
    case NIF.prefix_stream(db, cf, <<>>) do
      {:ok, stream} ->
        count =
          stream
          |> Stream.map(fn {key, _value} -> extract_first_id(key) end)
          |> Stream.dedup()
          |> Enum.count()

        {:ok, count}

      {:error, _} = error ->
        error
    end
  end

  # Extract the first 8-byte ID from a 24-byte index key
  @spec extract_first_id(binary()) :: non_neg_integer()
  defp extract_first_id(<<first_id::64-big, _rest::binary>>) do
    first_id
  end
end
