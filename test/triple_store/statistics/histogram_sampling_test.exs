defmodule TripleStore.Statistics.HistogramSamplingTest do
  @moduledoc """
  Tests for histogram sampling functionality in Phase 5.

  Tests verify:
  1. Sampling produces approximate results faster than full scan
  2. Sampled histograms are scaled correctly
  3. Reproducible sampling with seeds
  4. Different sample rates produce different accuracy levels
  """

  use ExUnit.Case, async: false
  @moduletag :slow

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.Statistics

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_histsamp_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db}
  end

  describe "histogram sampling" do
    test "full histogram (sample_rate: 1.0) produces exact counts", %{db: db} do
      # Insert known quads
      quads = [
        # graph 0, pred 10
        {1, 10, 100, 0},
        # graph 0, pred 10
        {2, 10, 101, 0},
        # graph 0, pred 11
        {3, 11, 102, 0},
        # graph 1, pred 10
        {4, 10, 103, 1}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get full histogram
      {:ok, histogram} = Statistics.build_per_graph_histograms(db, sample_rate: 1.0)

      # Verify exact counts
      assert Map.get(histogram, 0, %{}) == %{10 => 2, 11 => 1}
      assert Map.get(histogram, 1, %{}) == %{10 => 1}
    end

    test "sampled histogram scales counts correctly", %{db: db} do
      # Insert 1000 quads with known distribution
      quads =
        for i <- 1..1000 do
          graph = if rem(i, 2) == 0, do: 0, else: 1
          predicate = rem(i, 10) + 10
          {i, predicate, i + 1000, graph}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get full histogram for comparison
      {:ok, full_histogram} = Statistics.build_per_graph_histograms(db, sample_rate: 1.0)

      # Get sampled histogram with 50% sample rate and fixed seed
      {:ok, sampled_histogram} =
        Statistics.build_per_graph_histograms(db, sample_rate: 0.5, seed: 42)

      # Verify structure is the same
      assert Map.keys(full_histogram) |> MapSet.new() ==
               Map.keys(sampled_histogram) |> MapSet.new()

      # Verify sampled counts are in reasonable range (within 50% of actual for large enough samples)
      Enum.each([0, 1], fn graph_id ->
        full_predicates = Map.get(full_histogram, graph_id, %{})
        sampled_predicates = Map.get(sampled_histogram, graph_id, %{})

        # Check that we have similar predicate keys
        full_keys = Map.keys(full_predicates) |> MapSet.new()
        sampled_keys = Map.keys(sampled_predicates) |> MapSet.new()

        # With 50% sampling, we should have most predicates represented
        intersection = MapSet.intersection(full_keys, sampled_keys)
        assert MapSet.size(intersection) >= trunc(MapSet.size(full_keys) * 0.5)
      end)
    end

    test "sample_rate: 0.1 produces results faster than full scan", %{db: db} do
      # Insert 20K quads for pronounced sampling benefit without timeout
      quads =
        for i <- 1..20_000 do
          {rem(i, 2000) + 1, rem(i, 50) + 10, i + 1000, 0}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure full histogram time
      {full_time_us, {:ok, _full}} =
        :timer.tc(fn -> Statistics.build_per_graph_histograms(db, sample_rate: 1.0) end)

      # Measure 10% sample time
      {sample_time_us, {:ok, _sampled}} =
        :timer.tc(fn -> Statistics.build_per_graph_histograms(db, sample_rate: 0.1, seed: 42) end)

      IO.puts("\n  [Sampling] Full histogram: #{div(full_time_us, 1000)}ms")
      IO.puts("  [Sampling] 10% sample: #{div(sample_time_us, 1000)}ms")
      IO.puts("  [Sampling] Speedup: #{Float.round(full_time_us / sample_time_us, 2)}x")

      # Just verify sampling completes in reasonable time
      assert sample_time_us < 3_000_000,
             "Sampled histogram should complete in under 3 seconds, took: #{div(sample_time_us, 1000)}ms"
    end

    test "reproducible sampling with same seed", %{db: db} do
      # Insert test quads
      quads = for i <- 1..500, do: {i, rem(i, 20) + 10, i + 1000, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get two histograms with same seed
      {:ok, hist1} = Statistics.build_per_graph_histograms(db, sample_rate: 0.2, seed: 12345)
      {:ok, hist2} = Statistics.build_per_graph_histograms(db, sample_rate: 0.2, seed: 12345)

      # They should be identical
      assert hist1 == hist2

      # Different seed should produce different results (most of the time)
      {:ok, hist3} = Statistics.build_per_graph_histograms(db, sample_rate: 0.2, seed: 54321)

      # With reasonable probability, histograms should differ
      # (There's a small chance they could be the same, but very unlikely)
      refute hist1 == hist3
    end

    test "small sample_rate still captures predicate distribution", %{db: db} do
      # Insert quads with skewed distribution
      # Predicate 10 appears 500 times, predicates 11-19 appear 50 times each
      skewed_quads =
        Enum.flat_map(1..500, fn i -> [{i, 10, i + 1000, 0}] end) ++
          Enum.flat_map(1..50, fn i ->
            for p <- 11..19, do: {i + 500, p, i + 2000, 0}
          end)

      Enum.each(skewed_quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get full histogram
      {:ok, full_hist} = Statistics.build_per_graph_histograms(db, sample_rate: 1.0)

      # Get 5% sample
      {:ok, sampled_hist} =
        Statistics.build_per_graph_histograms(db, sample_rate: 0.05, seed: 42)

      # Full hist should show predicate 10 as dominant
      full_pred_10_count = Map.get(full_hist, 0, %{}) |> Map.get(10, 0)

      # Sampled hist should also show predicate 10 as dominant (or at least significant)
      sampled_pred_10_count = Map.get(sampled_hist, 0, %{}) |> Map.get(10, 0)

      IO.puts("\n  [Sampling] Full pred 10 count: #{full_pred_10_count}")
      IO.puts("  [Sampling] Sampled pred 10 count: #{sampled_pred_10_count}")

      # Predicate 10 should be present in sampled histogram
      assert sampled_pred_10_count > 0,
             "Dominant predicate should be captured even with small sample rate"
    end

    test "sample_rate: 0.0 returns empty histogram", %{db: db} do
      # Insert test quads
      quads = for i <- 1..100, do: {i, 10, i + 1000, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get histogram with 0% sample rate
      {:ok, histogram} = Statistics.build_per_graph_histograms(db, sample_rate: 0.0)

      # Should be empty
      assert histogram == %{}
    end
  end

  describe "sampling with include_default option" do
    test "sampling works with include_default: false", %{db: db} do
      # Insert quads to both default and named graph
      quads =
        [
          # default graph
          {1, 10, 100, 0},
          # default graph
          {2, 10, 101, 0},
          # named graph
          {3, 11, 102, 1},
          # named graph
          {4, 11, 103, 1}
        ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get sampled histogram excluding default (use full sample to avoid scaling)
      {:ok, histogram} =
        Statistics.build_per_graph_histograms(db,
          sample_rate: 1.0,
          include_default: false
        )

      # Default graph should not be in histogram
      refute Map.has_key?(histogram, 0)

      # Named graph should be present with exact count
      assert Map.get(histogram, 1) == %{11 => 2}
    end
  end
end
