defmodule TripleStore.Constants do
  @moduledoc """
  Centralized constants for the TripleStore application (S18).

  This module contains shared constants used across multiple modules
  to avoid duplication and make configuration easier.
  """

  # ===========================================================================
  # Timeouts and Time Limits
  # ===========================================================================

  @doc "Default timeout for query execution in milliseconds (30 seconds)"
  def default_query_timeout, do: 30_000

  @doc "Default timeout for leapfrog operations in milliseconds"
  def default_leapfrog_timeout, do: 30_000

  # ===========================================================================
  # Sizes and Limits
  # ===========================================================================

  @doc "Default batch size for streaming operations"
  def default_batch_size, do: 1_000

  @doc "Default maximum cache entries for query cache"
  def default_max_cache_entries, do: 10_000

  @doc "Default maximum iterations for leapfrog join algorithm"
  def default_max_iterations, do: 1_000_000

  @doc "Default threshold for parallelizing operations (cost threshold)"
  def default_parallel_threshold, do: 1_000

  # ===========================================================================
  # Cache Configuration
  # ===========================================================================

  @doc "Default TTL for cached query results in milliseconds (5 minutes)"
  def default_cache_ttl, do: 300_000

  @doc "Default plan cache size"
  def default_plan_cache_size, do: 1_000

  # ===========================================================================
  # Graph and Storage Defaults
  # ===========================================================================

  @doc "Default graph ID for the default named graph"
  def default_graph_id, do: 0

  @doc "Default statistics for cost estimation when real stats unavailable"
  def default_stats do
    %{
      triple_count: 10_000,
      quad_count: 10_000,
      distinct_subjects: 1_000,
      distinct_predicates: 100,
      distinct_objects: 2_000,
      total_graphs: 1
    }
  end

  # ===========================================================================
  # Cost Model Weights
  # ===========================================================================

  @doc "Default cost weights for query optimization"
  def default_cost_weights do
    %{
      comparison_cost: 1,
      hash_cost: 50,
      hash_probe_cost: 5,
      index_seek_cost: 10,
      sequential_read_cost: 1,
      memory_weight: 1,
      leapfrog_seek_cost: 10,
      leapfrog_comparison_cost: 1,
      hash_join_threshold: 100
    }
  end
end
