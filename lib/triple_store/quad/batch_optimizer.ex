defmodule TripleStore.Quad.BatchOptimizer do
  @moduledoc """
  Write batch optimization for quad store operations.

  This module optimizes quad insert operations by grouping quads efficiently
  for RocksDB WriteBatch operations. Quad operations write to 4 indices (GSPO,
  GPOS, SPOG, POSG), so each quad results in 4 writes.

  ## Optimization Strategy

  1. **Graph-local grouping**: Groups quads by graph ID to improve locality
  2. **Adaptive batch sizing**: Adjusts batch size based on quad size
  3. **WriteBatch estimation**: Calculates optimal WriteBatch size (1K-50K operations)

  ## Performance Characteristics

  - Each quad write = 4 index writes (GSPO, GPOS, SPOG, POSG)
  - Target batch size: 5K-20K quads (20K-80K index writes)
  - Maximum batch size: 50K operations (12.5K quads)
  - Graph-local grouping improves RocksDB block cache utilization

  ## Usage

      # Group quads for optimal batch processing
      batches = BatchOptimizer.group_quads_for_batch(quads)

      # Process each batch
      Enum.each(batches, fn batch ->
        TripleStore.QuadOperations.insert_quads(db, batch)
      end)

      # Group quads by graph for graph-local processing
      graph_groups = BatchOptimizer.group_quads_by_graph(quads)

      # Process each graph group
      Enum.each(graph_groups, fn {graph_id, graph_quads} ->
        # All quads in graph_quads belong to the same graph
        TripleStore.QuadOperations.insert_quads(db, graph_quads)
      end)

  """

  # ===========================================================================
  # Constants
  # ===========================================================================

  # WriteBatch size constraints (in number of operations)
  # Each quad = 4 operations (one per index)
  @max_batch_operations 50_000
  @target_batch_operations 20_000
  @min_batch_operations 1_000

  # Convert operations to quads (each quad = 4 operations)
  @max_quads_per_batch div(@max_batch_operations, 4)
  @target_quads_per_batch div(@target_batch_operations, 4)
  @min_quads_per_batch div(@min_batch_operations, 4)

  # Default batch size for graph grouping
  @default_graph_batch_size 5_000

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Quad as {subject, predicate, object, graph}"
  @type quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Batch optimization options"
  @type options :: [
          {:target_size, pos_integer()}
          | {:max_size, pos_integer()}
          | {:batch_size, pos_integer()}
          | {:preserve_order, boolean()}
        ]

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Groups quads into optimal batches for WriteBatch operations.

  This function splits a list of quads into batches that are optimized for
  RocksDB WriteBatch performance. Each batch will contain approximately
  `target_size` quads, with a maximum of `max_size` quads.

  ## Parameters

  - `quads` - List of quads to batch
  - `opts` - Optional parameters:
    - `:target_size` - Target quads per batch (default: 5,000)
    - `:max_size` - Maximum quads per batch (default: 12,500)
    - `:preserve_order` - Whether to preserve input order (default: false)

  ## Returns

  List of batches, where each batch is a list of quads.

  ## Examples

      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]
      iex> batches = BatchOptimizer.group_quads_for_batch(quads, target_size: 2)
      iex> length(batches)
      2

  """
  @spec group_quads_for_batch([quad()], options()) :: [[quad()]]
  def group_quads_for_batch(quads, opts \\ []) do
    target_size = Keyword.get(opts, :target_size, @target_quads_per_batch)
    max_size = Keyword.get(opts, :max_size, @max_quads_per_batch)
    preserve_order = Keyword.get(opts, :preserve_order, false)

    quads_per_batch = normalize_batch_size(target_size, @min_quads_per_batch, @max_quads_per_batch)
    max_quads_per_batch = normalize_batch_size(max_size, @min_quads_per_batch, @max_quads_per_batch)

    if preserve_order do
      # Simple chunking preserving order
      Enum.chunk_every(quads, quads_per_batch)
    else
      # Optimize for graph locality
      group_by_graph_and_chunk(quads, quads_per_batch, max_quads_per_batch)
    end
  end

  @doc """
  Groups quads by graph ID for graph-local processing.

  Graph-local grouping improves write performance by:
  1. Improving block cache locality (all writes to same graph region)
  2. Reducing write amplification (contiguous key ranges)
  3. Enabling graph-level optimizations

  ## Parameters

  - `quads` - List of quads to group
  - `opts` - Optional parameters:
    - `:batch_size` - Quads per batch within each graph (default: 5,000)

  ## Returns

  Map of graph_id to list of batches, where each batch is a list of quads.

  ## Examples

      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]
      iex> groups = BatchOptimizer.group_quads_by_graph(quads, batch_size: 1000)
      iex> Map.keys(groups) |> sort()
      [0, 1]

  """
  @spec group_quads_by_graph([quad()], options()) :: %{non_neg_integer() => [[quad()]]}
  def group_quads_by_graph(quads, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_graph_batch_size)

    quads
    |> Enum.group_by(fn {_s, _p, _o, g} -> g end)
    |> Enum.map(fn {graph_id, graph_quads} ->
      batches = Enum.chunk_every(graph_quads, batch_size)
      {graph_id, batches}
    end)
    |> Map.new()
  end

  @doc """
  Estimates the number of WriteBatch operations for a given number of quads.

  Since each quad write results in 4 index writes (GSPO, GPOS, SPOG, POSG),
  this function calculates the total number of RocksDB operations.

  ## Parameters

  - `quad_count` - Number of quads to be written

  ## Returns

  Total number of WriteBatch operations (quad_count * 4).

  ## Examples

      iex> BatchOptimizer.estimate_operations(1000)
      4000

  """
  @spec estimate_operations(non_neg_integer()) :: pos_integer()
  def estimate_operations(quad_count) when is_integer(quad_count) and quad_count >= 0 do
    quad_count * 4
  end

  @doc """
  Calculates the optimal batch size for a given total number of quads.

  Returns a batch size that minimizes the number of batches while keeping
  each batch within the optimal size range.

  ## Parameters

  - `total_quads` - Total number of quads to write

  ## Returns

  Recommended batch size (number of quads per batch).

  ## Examples

      iex> BatchOptimizer.calculate_optimal_batch_size(100_000)
      5000

  """
  @spec calculate_optimal_batch_size(non_neg_integer()) :: pos_integer()
  def calculate_optimal_batch_size(total_quads) when is_integer(total_quads) and total_quads > 0 do
    # Calculate target batch size to get ~10-20 batches total
    target_batches = 20
    target_size = ceil(total_quads / target_batches)

    # Clamp to valid range
    normalize_batch_size(target_size, @min_quads_per_batch, @max_quads_per_batch)
  end

  @doc """
  Returns the recommended batch size constraints.

  ## Returns

  Map with:
  - `:min` - Minimum quads per batch
  - `:target` - Target quads per batch
  - `:max` - Maximum quads per batch

  """
  @spec batch_size_constraints() :: %{
          min: pos_integer(),
          target: pos_integer(),
          max: pos_integer()
        }
  def batch_size_constraints do
    %{
      min: @min_quads_per_batch,
      target: @target_quads_per_batch,
      max: @max_quads_per_batch
    }
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  # Normalizes batch size to be within valid range
  defp normalize_batch_size(size, min_size, _max_size) when size < min_size, do: min_size
  defp normalize_batch_size(size, _min_size, max_size) when size > max_size, do: max_size
  defp normalize_batch_size(size, _min_size, _max_size), do: size

  # Groups quads by graph and then chunks into batches
  # This improves locality by keeping quads from the same graph together
  defp group_by_graph_and_chunk(quads, target_per_batch, max_per_batch) do
    quads
    |> Enum.group_by(fn {_s, _p, _o, g} -> g end)
    |> Enum.flat_map(fn {_graph_id, graph_quads} ->
      # Chunk within each graph for locality
      Enum.chunk_every(graph_quads, target_per_batch)
      |> Enum.flat_map(fn chunk ->
        # If chunk is too large, split it further
        if length(chunk) > max_per_batch do
          Enum.chunk_every(chunk, max_per_batch)
        else
          [chunk]
        end
      end)
    end)
  end
end
