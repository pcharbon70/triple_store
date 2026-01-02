defmodule TripleStore.Statistics do
  @moduledoc """
  Statistics collection for cost-based query optimization.

  Provides functions to compute and cache statistics about the stored triples,
  enabling the query optimizer to accurately estimate result sizes and select
  efficient query plans.

  ## Features

  - **Triple counts**: Total count and per-predicate counts
  - **Distinct counts**: Counts of distinct subjects, predicates, objects
  - **Predicate histogram**: Per-predicate triple counts for selectivity estimation
  - **Numeric histograms**: Equi-width histograms for range selectivity estimation
  - **Persistence**: Statistics stored in RocksDB for fast reload
  - **Telemetry**: Collection timing and metrics

  ## Usage

      # Collect all statistics (full scan)
      {:ok, stats} = Statistics.collect(db)

      # Get cached statistics (fast)
      {:ok, stats} = Statistics.get(db)

      # Refresh statistics
      :ok = Statistics.refresh(db)

      # Estimate range selectivity
      selectivity = Statistics.estimate_range_selectivity(stats, pred_id, 10.0, 100.0)

  ## Statistics Structure

      %{
        triple_count: 10000,
        distinct_subjects: 1000,
        distinct_predicates: 50,
        distinct_objects: 2000,
        predicate_histogram: %{42 => 500, 43 => 1500},
        numeric_histograms: %{
          price_id => %{min: 0.0, max: 1000.0, buckets: [...]}
        },
        collected_at: ~U[2026-01-02 12:00:00Z],
        version: 1
      }
  """

  import Bitwise

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "64-bit term ID"
  @type term_id :: non_neg_integer()

  @typedoc "Histogram for numeric values"
  @type numeric_histogram :: %{
          min: float(),
          max: float(),
          bucket_count: pos_integer(),
          buckets: [non_neg_integer()],
          total_count: non_neg_integer()
        }

  @typedoc "Complete statistics map"
  @type stats :: %{
          triple_count: non_neg_integer(),
          distinct_subjects: non_neg_integer(),
          distinct_predicates: non_neg_integer(),
          distinct_objects: non_neg_integer(),
          predicate_histogram: %{term_id() => non_neg_integer()},
          numeric_histograms: %{term_id() => numeric_histogram()},
          collected_at: DateTime.t(),
          version: pos_integer()
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Statistics version for forward compatibility
  @stats_version 1

  # Default histogram bucket count
  @default_bucket_count 100

  # Key prefix for persisted statistics in id2str column family
  # Uses a reserved prefix that can't conflict with term IDs (type tag 0)
  @stats_key_prefix <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>

  # Type tags for identifying inline-encoded numeric values
  @type_integer 0b0100
  @type_decimal 0b0101
  @type_datetime 0b0110

  # ===========================================================================
  # Public API - Collection
  # ===========================================================================

  @doc """
  Collects all statistics from the database.

  Performs a full scan of indices to gather:
  - Total triple count
  - Distinct subject/predicate/object counts
  - Per-predicate cardinalities (predicate histogram)
  - Numeric histograms for inline-encoded predicates

  ## Arguments

  - `db` - Database reference
  - `opts` - Options:
    - `:bucket_count` - Number of histogram buckets (default: 100)
    - `:build_histograms` - Whether to build numeric histograms (default: true)

  ## Returns

  - `{:ok, stats}` - Complete statistics map
  - `{:error, reason}` - On failure

  ## Performance

  This performs multiple index scans. For large datasets:
  - SPO scan for triple count and distinct subjects
  - POS scan for predicate histogram and distinct predicates
  - OSP scan for distinct objects
  - Optional per-predicate scans for numeric histograms
  """
  @spec collect(db_ref(), keyword()) :: {:ok, stats()} | {:error, term()}
  def collect(db, opts \\ []) do
    start_time = System.monotonic_time()
    bucket_count = Keyword.get(opts, :bucket_count, @default_bucket_count)
    build_histograms? = Keyword.get(opts, :build_histograms, true)

    with {:ok, triple_count} <- triple_count(db),
         {:ok, distinct_subjects} <- distinct_subjects(db),
         {:ok, distinct_predicates} <- distinct_predicates(db),
         {:ok, distinct_objects} <- distinct_objects(db),
         {:ok, predicate_histogram} <- build_predicate_histogram(db),
         {:ok, numeric_histograms} <-
           maybe_build_numeric_histograms(db, predicate_histogram, bucket_count, build_histograms?) do
      stats = %{
        triple_count: triple_count,
        distinct_subjects: distinct_subjects,
        distinct_predicates: distinct_predicates,
        distinct_objects: distinct_objects,
        predicate_histogram: predicate_histogram,
        numeric_histograms: numeric_histograms,
        collected_at: DateTime.utc_now(),
        version: @stats_version
      }

      duration = System.monotonic_time() - start_time

      :telemetry.execute(
        [:triple_store, :statistics, :collect],
        %{duration: duration, triple_count: triple_count, predicate_count: map_size(predicate_histogram)},
        %{}
      )

      {:ok, stats}
    end
  end

  @doc """
  Persists statistics to the database.

  Stores the statistics in the id2str column family using a reserved key.
  Statistics can be reloaded on restart using `load/1`.

  ## Arguments

  - `db` - Database reference
  - `stats` - Statistics map to persist

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec save(db_ref(), stats()) :: :ok | {:error, term()}
  def save(db, stats) do
    encoded = :erlang.term_to_binary(stats, [:compressed])

    case NIF.put(db, :id2str, @stats_key_prefix, encoded) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Loads persisted statistics from the database.

  Reads statistics that were previously saved with `save/2`.
  Returns `{:ok, nil}` if no statistics have been saved.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats}` - Previously saved statistics
  - `{:ok, nil}` - No statistics saved
  - `{:error, reason}` - On failure
  """
  @spec load(db_ref()) :: {:ok, stats() | nil} | {:error, term()}
  def load(db) do
    case NIF.get(db, :id2str, @stats_key_prefix) do
      {:ok, encoded} when is_binary(encoded) ->
        stats = :erlang.binary_to_term(encoded)
        {:ok, stats}

      :not_found ->
        {:ok, nil}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets statistics, loading from persistence if available.

  First attempts to load persisted statistics. If not available,
  collects fresh statistics and persists them.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats}` - Statistics map
  - `{:error, reason}` - On failure
  """
  @spec get(db_ref()) :: {:ok, stats()} | {:error, term()}
  def get(db) do
    case load(db) do
      {:ok, nil} ->
        with {:ok, stats} <- collect(db),
             :ok <- save(db, stats) do
          {:ok, stats}
        end

      {:ok, stats} ->
        {:ok, stats}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Refreshes statistics by collecting fresh data.

  Collects new statistics and persists them, replacing any
  previously stored statistics.

  ## Arguments

  - `db` - Database reference
  - `opts` - Collection options (see `collect/2`)

  ## Returns

  - `{:ok, stats}` - Fresh statistics
  - `{:error, reason}` - On failure
  """
  @spec refresh(db_ref(), keyword()) :: {:ok, stats()} | {:error, term()}
  def refresh(db, opts \\ []) do
    with {:ok, stats} <- collect(db, opts),
         :ok <- save(db, stats) do
      {:ok, stats}
    end
  end

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
  """
  @spec predicate_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def predicate_count(db, predicate_id) when is_integer(predicate_id) and predicate_id >= 0 do
    Index.count(db, {:var, {:bound, predicate_id}, :var})
  end

  # ===========================================================================
  # Public API - Distinct Counts
  # ===========================================================================

  @doc """
  Returns the count of distinct subjects.

  Scans the SPO index and counts unique subject IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct subjects
  - `{:error, reason}` - On failure
  """
  @spec distinct_subjects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_subjects(db) do
    count_distinct_by_position(db, :spo)
  end

  @doc """
  Returns the count of distinct predicates.

  Scans the POS index and counts unique predicate IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct predicates
  - `{:error, reason}` - On failure
  """
  @spec distinct_predicates(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_predicates(db) do
    count_distinct_by_position(db, :pos)
  end

  @doc """
  Returns the count of distinct objects.

  Scans the OSP index and counts unique object IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct objects
  - `{:error, reason}` - On failure
  """
  @spec distinct_objects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_objects(db) do
    count_distinct_by_position(db, :osp)
  end

  # ===========================================================================
  # Public API - Predicate Histogram
  # ===========================================================================

  @doc """
  Builds a histogram of triple counts per predicate.

  Scans the POS index and counts triples for each unique predicate.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, histogram}` - Map of predicate_id => count
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, histogram} = Statistics.build_predicate_histogram(db)
      iex> histogram
      %{42 => 500, 43 => 1500, 44 => 300}
  """
  @spec build_predicate_histogram(db_ref()) :: {:ok, %{term_id() => non_neg_integer()}} | {:error, term()}
  def build_predicate_histogram(db) do
    case NIF.prefix_stream(db, :pos, <<>>) do
      {:ok, stream} ->
        histogram =
          stream
          |> Stream.map(fn {key, _value} -> extract_first_id(key) end)
          |> Enum.reduce(%{}, fn predicate_id, acc ->
            Map.update(acc, predicate_id, 1, &(&1 + 1))
          end)

        {:ok, histogram}

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Public API - Numeric Histograms
  # ===========================================================================

  @doc """
  Builds a numeric histogram for a specific predicate.

  Scans all values for the predicate and builds an equi-width histogram
  of the numeric values. Only works for predicates with inline-encoded
  numeric values (integers, decimals, datetimes).

  ## Arguments

  - `db` - Database reference
  - `predicate_id` - The predicate to build histogram for
  - `bucket_count` - Number of histogram buckets (default: 100)

  ## Returns

  - `{:ok, histogram}` - Numeric histogram map
  - `{:ok, nil}` - Predicate has no numeric values
  - `{:error, reason}` - On failure

  ## Histogram Structure

      %{
        min: 0.0,
        max: 1000.0,
        bucket_count: 100,
        buckets: [45, 67, 89, ...],  # 100 counts
        total_count: 5000
      }
  """
  @spec build_numeric_histogram(db_ref(), term_id(), pos_integer()) ::
          {:ok, numeric_histogram() | nil} | {:error, term()}
  def build_numeric_histogram(db, predicate_id, bucket_count \\ @default_bucket_count) do
    prefix = <<predicate_id::64-big>>

    case NIF.prefix_stream(db, :pos, prefix) do
      {:ok, stream} ->
        # First pass: collect all numeric values to find min/max
        numeric_values =
          stream
          |> Stream.map(fn {key, _value} -> extract_second_id(key) end)
          |> Stream.filter(&is_numeric_id?/1)
          |> Stream.map(&decode_numeric_value/1)
          |> Enum.to_list()

        if numeric_values == [] do
          {:ok, nil}
        else
          build_histogram_from_values(numeric_values, bucket_count)
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Estimates the selectivity of a range query using histogram.

  Returns the estimated fraction of values that fall within the given range.

  ## Arguments

  - `stats` - Statistics map containing numeric_histograms
  - `predicate_id` - The predicate to estimate for
  - `min_value` - Minimum value of range (inclusive)
  - `max_value` - Maximum value of range (inclusive)

  ## Returns

  - Selectivity as a float between 0.0 and 1.0
  - 1.0 if no histogram available for the predicate

  ## Examples

      iex> Statistics.estimate_range_selectivity(stats, price_id, 10.0, 100.0)
      0.35  # Approximately 35% of values fall in this range
  """
  @spec estimate_range_selectivity(stats(), term_id(), number(), number()) :: float()
  def estimate_range_selectivity(stats, predicate_id, min_value, max_value) do
    case get_in(stats, [:numeric_histograms, predicate_id]) do
      nil ->
        # No histogram available, assume uniform distribution
        1.0

      histogram ->
        estimate_range_from_histogram(histogram, min_value, max_value)
    end
  end

  # ===========================================================================
  # Public API - Bulk Statistics
  # ===========================================================================

  @doc """
  Returns all basic statistics in a single call.

  Computes basic counts together for convenience. For full statistics
  including histograms, use `collect/1`.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats_map}` - Map with basic statistics
  - `{:error, reason}` - On failure
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
  # Private Helpers - Distinct Counting
  # ===========================================================================

  @spec count_distinct_by_position(db_ref(), :spo | :pos | :osp) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp count_distinct_by_position(db, cf) do
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

  # Extract the second 8-byte ID from a 24-byte index key
  @spec extract_second_id(binary()) :: non_neg_integer()
  defp extract_second_id(<<_first::64, second_id::64-big, _rest::binary>>) do
    second_id
  end

  # ===========================================================================
  # Private Helpers - Numeric Histograms
  # ===========================================================================

  @spec maybe_build_numeric_histograms(db_ref(), map(), pos_integer(), boolean()) ::
          {:ok, map()} | {:error, term()}
  defp maybe_build_numeric_histograms(_db, _predicate_histogram, _bucket_count, false) do
    {:ok, %{}}
  end

  defp maybe_build_numeric_histograms(db, predicate_histogram, bucket_count, true) do
    # Build histograms for predicates that have numeric values
    # We sample the first value to check if a predicate has numeric objects
    results =
      predicate_histogram
      |> Map.keys()
      |> Enum.reduce_while({:ok, %{}}, fn predicate_id, {:ok, acc} ->
        case build_numeric_histogram(db, predicate_id, bucket_count) do
          {:ok, nil} ->
            {:cont, {:ok, acc}}

          {:ok, histogram} ->
            {:cont, {:ok, Map.put(acc, predicate_id, histogram)}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)

    results
  end

  @spec is_numeric_id?(non_neg_integer()) :: boolean()
  defp is_numeric_id?(id) do
    type_tag = id >>> 60

    type_tag == @type_integer or type_tag == @type_decimal or type_tag == @type_datetime
  end

  @spec decode_numeric_value(non_neg_integer()) :: float()
  defp decode_numeric_value(id) do
    type_tag = id >>> 60

    case type_tag do
      @type_integer ->
        decode_inline_integer(id)

      @type_decimal ->
        decode_inline_decimal(id)

      @type_datetime ->
        # DateTime encoded as milliseconds since epoch
        millis = id &&& ((1 <<< 60) - 1)
        millis / 1000.0

      _ ->
        0.0
    end
  end

  # Decode inline integer (two's complement in 60 bits)
  @spec decode_inline_integer(non_neg_integer()) :: float()
  defp decode_inline_integer(id) do
    # Extract 60-bit value
    value = id &&& ((1 <<< 60) - 1)

    # Check sign bit (bit 59)
    if (value &&& (1 <<< 59)) != 0 do
      # Negative: extend sign
      -(((~~~value) &&& ((1 <<< 59) - 1)) + 1) * 1.0
    else
      value * 1.0
    end
  end

  # Decode inline decimal
  @spec decode_inline_decimal(non_neg_integer()) :: float()
  defp decode_inline_decimal(id) do
    # Bit layout: [type:4][sign:1][exponent:11][mantissa:48]
    payload = id &&& ((1 <<< 60) - 1)

    sign = (payload >>> 59) &&& 1
    exponent = (payload >>> 48) &&& 0x7FF
    mantissa = payload &&& ((1 <<< 48) - 1)

    # Unbias exponent
    exp = exponent - 1023

    # Reconstruct value
    value = mantissa * :math.pow(10, exp - 48)

    if sign == 1, do: -value, else: value
  end

  @spec build_histogram_from_values([float()], pos_integer()) :: {:ok, numeric_histogram()}
  defp build_histogram_from_values(values, bucket_count) do
    min_val = Enum.min(values)
    max_val = Enum.max(values)

    # Handle case where all values are the same
    {min_val, max_val, range} =
      if min_val == max_val do
        {min_val - 0.5, max_val + 0.5, 1.0}
      else
        {min_val, max_val, max_val - min_val}
      end

    bucket_width = range / bucket_count

    # Initialize buckets
    buckets = List.duplicate(0, bucket_count)

    # Count values into buckets
    buckets =
      Enum.reduce(values, buckets, fn value, acc ->
        bucket_idx = trunc((value - min_val) / bucket_width)
        # Clamp to valid range (handle edge case of max value)
        bucket_idx = min(max(bucket_idx, 0), bucket_count - 1)

        List.update_at(acc, bucket_idx, &(&1 + 1))
      end)

    histogram = %{
      min: min_val,
      max: max_val,
      bucket_count: bucket_count,
      buckets: buckets,
      total_count: length(values)
    }

    {:ok, histogram}
  end

  @spec estimate_range_from_histogram(numeric_histogram(), number(), number()) :: float()
  defp estimate_range_from_histogram(histogram, min_value, max_value) do
    %{min: hist_min, max: hist_max, bucket_count: bucket_count, buckets: buckets, total_count: total} = histogram

    if total == 0 do
      1.0
    else
      range = hist_max - hist_min
      bucket_width = range / bucket_count

      # Clamp query range to histogram range
      query_min = max(min_value, hist_min)
      query_max = min(max_value, hist_max)

      if query_min >= query_max do
        0.0
      else
        # Calculate bucket indices
        start_bucket = trunc((query_min - hist_min) / bucket_width)
        end_bucket = trunc((query_max - hist_min) / bucket_width)

        start_bucket = min(max(start_bucket, 0), bucket_count - 1)
        end_bucket = min(max(end_bucket, 0), bucket_count - 1)

        # Sum counts in range (with fractional bucket handling)
        count =
          start_bucket..end_bucket
          |> Enum.reduce(0.0, fn bucket_idx, acc ->
            bucket_count_val = Enum.at(buckets, bucket_idx, 0)

            # Calculate fraction of bucket in range
            bucket_start = hist_min + bucket_idx * bucket_width
            bucket_end = bucket_start + bucket_width

            overlap_start = max(query_min, bucket_start)
            overlap_end = min(query_max, bucket_end)
            overlap_fraction = (overlap_end - overlap_start) / bucket_width

            acc + bucket_count_val * overlap_fraction
          end)

        # Return selectivity as fraction of total
        min(count / total, 1.0)
      end
    end
  end
end
