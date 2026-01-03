defmodule TripleStore.StatisticsTest do
  @moduledoc """
  Tests for Statistics Collection (Phase 3.1).

  Verifies:
  - triple_count/1 returns accurate count
  - predicate_count/2 returns accurate per-predicate count
  - distinct_subjects/1 returns accurate distinct count
  - distinct_predicates/1 returns accurate distinct count
  - distinct_objects/1 returns accurate distinct count
  - all/1 returns all statistics
  - collect/1 returns comprehensive statistics with histograms
  - build_predicate_histogram/1 builds accurate predicate histogram
  - build_numeric_histogram/3 builds accurate numeric histograms
  - estimate_range_selectivity/4 estimates range selectivity
  - save/2 and load/1 persist and reload statistics
  - get/1 returns cached or fresh statistics
  """

  use ExUnit.Case, async: false

  import Bitwise

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Statistics

  @test_db_base "/tmp/triple_store_statistics_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # triple_count/1 Tests
  # ===========================================================================

  describe "triple_count/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.triple_count(db)
    end

    test "returns 1 for single triple", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 1001, 1002})
      assert {:ok, 1} = Statistics.triple_count(db)
    end

    test "returns correct count for multiple triples", %{db: db} do
      triples = [
        {1000, 1001, 1002},
        {1000, 1001, 1003},
        {1000, 1004, 1005},
        {1006, 1001, 1002},
        {1007, 1008, 1009}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 5} = Statistics.triple_count(db)
    end

    test "count remains consistent after insert and delete", %{db: db} do
      triples = [{1000, 1001, 1002}, {1003, 1004, 1005}, {1006, 1007, 1008}]
      :ok = Index.insert_triples(db, triples)
      assert {:ok, 3} = Statistics.triple_count(db)

      :ok = Index.delete_triple(db, {1003, 1004, 1005})
      assert {:ok, 2} = Statistics.triple_count(db)
    end
  end

  # ===========================================================================
  # predicate_count/2 Tests
  # ===========================================================================

  describe "predicate_count/2" do
    test "returns 0 for predicate with no triples", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 1001, 1002})
      assert {:ok, 0} = Statistics.predicate_count(db, 9999)
    end

    test "returns correct count for predicate", %{db: db} do
      triples = [
        # predicate 100
        {1000, 100, 2000},
        # predicate 100
        {1001, 100, 2001},
        # predicate 100
        {1002, 100, 2002},
        # predicate 200
        {1003, 200, 2003},
        # predicate 200
        {1004, 200, 2004}
      ]

      :ok = Index.insert_triples(db, triples)

      assert {:ok, 3} = Statistics.predicate_count(db, 100)
      assert {:ok, 2} = Statistics.predicate_count(db, 200)
    end

    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.predicate_count(db, 100)
    end
  end

  # ===========================================================================
  # distinct_subjects/1 Tests
  # ===========================================================================

  describe "distinct_subjects/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_subjects(db)
    end

    test "returns 1 for single subject", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1000, 102, 2002}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_subjects(db)
    end

    test "returns correct count for multiple subjects", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002},
        # Same subject 1000
        {1000, 101, 2003},
        # Same subject 1001
        {1001, 102, 2004}
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct subjects: 1000, 1001, 1002
      assert {:ok, 3} = Statistics.distinct_subjects(db)
    end
  end

  # ===========================================================================
  # distinct_predicates/1 Tests
  # ===========================================================================

  describe "distinct_predicates/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_predicates(db)
    end

    test "returns 1 for single predicate", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_predicates(db)
    end

    test "returns correct count for multiple predicates", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2001},
        {1002, 102, 2002},
        # Same predicate 100
        {1003, 100, 2003},
        # Same predicate 101
        {1004, 101, 2004}
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct predicates: 100, 101, 102
      assert {:ok, 3} = Statistics.distinct_predicates(db)
    end
  end

  # ===========================================================================
  # distinct_objects/1 Tests
  # ===========================================================================

  describe "distinct_objects/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_objects(db)
    end

    test "returns 1 for single object", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2000},
        {1002, 102, 2000}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_objects(db)
    end

    test "returns correct count for multiple objects", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2001},
        {1002, 102, 2002},
        # Same object 2000
        {1003, 103, 2000},
        # Same object 2001
        {1004, 104, 2001}
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct objects: 2000, 2001, 2002
      assert {:ok, 3} = Statistics.distinct_objects(db)
    end
  end

  # ===========================================================================
  # all/1 Tests
  # ===========================================================================

  describe "all/1" do
    test "returns all zeros for empty store", %{db: db} do
      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 0
      assert stats.distinct_subjects == 0
      assert stats.distinct_predicates == 0
      assert stats.distinct_objects == 0
    end

    test "returns correct statistics", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1001, 100, 2000},
        {1001, 102, 2002},
        {1002, 100, 2001}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 5
      # 1000, 1001, 1002
      assert stats.distinct_subjects == 3
      # 100, 101, 102
      assert stats.distinct_predicates == 3
      # 2000, 2001, 2002
      assert stats.distinct_objects == 3
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles large term IDs", %{db: db} do
      large_id = 0xFFFFFFFFFFFFFFFF
      :ok = Index.insert_triple(db, {large_id, 100, 200})

      assert {:ok, 1} = Statistics.triple_count(db)
      assert {:ok, 1} = Statistics.distinct_subjects(db)
    end

    test "handles many triples efficiently", %{db: db} do
      # Insert 100 triples
      triples =
        for i <- 1..100 do
          {1000 + rem(i, 10), 100 + rem(i, 5), 2000 + i}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 100
      # 1000-1009
      assert stats.distinct_subjects == 10
      # 100-104
      assert stats.distinct_predicates == 5
      # All unique
      assert stats.distinct_objects == 100
    end
  end

  # ===========================================================================
  # collect/1 Tests (Phase 3.1)
  # ===========================================================================

  describe "collect/1" do
    test "returns complete statistics for empty store", %{db: db} do
      {:ok, stats} = Statistics.collect(db)

      assert stats.triple_count == 0
      assert stats.distinct_subjects == 0
      assert stats.distinct_predicates == 0
      assert stats.distinct_objects == 0
      assert stats.predicate_histogram == %{}
      assert stats.numeric_histograms == %{}
      assert stats.version == 1
      assert %DateTime{} = stats.collected_at
    end

    test "returns complete statistics with data", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1001, 100, 2000},
        {1001, 102, 2002},
        {1002, 100, 2001}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.collect(db)

      assert stats.triple_count == 5
      assert stats.distinct_subjects == 3
      assert stats.distinct_predicates == 3
      assert stats.distinct_objects == 3

      # Predicate histogram should have accurate counts
      assert stats.predicate_histogram[100] == 3
      assert stats.predicate_histogram[101] == 1
      assert stats.predicate_histogram[102] == 1
    end

    test "respects build_histograms option", %{db: db} do
      # Create an inline integer ID (type 4, value 42)
      int_id = 0b0100 <<< 60 ||| 42

      triples = [
        {1000, 100, int_id},
        {1001, 100, int_id}
      ]

      :ok = Index.insert_triples(db, triples)

      # With histograms disabled
      {:ok, stats} = Statistics.collect(db, build_histograms: false)
      assert stats.numeric_histograms == %{}

      # With histograms enabled (default)
      {:ok, stats} = Statistics.collect(db)
      # Should have a histogram for predicate 100 with numeric objects
      assert is_map(stats.numeric_histograms)
    end
  end

  # ===========================================================================
  # build_predicate_histogram/1 Tests
  # ===========================================================================

  describe "build_predicate_histogram/1" do
    test "returns empty map for empty store", %{db: db} do
      {:ok, histogram} = Statistics.build_predicate_histogram(db)
      assert histogram == %{}
    end

    test "returns accurate counts per predicate", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002},
        {1003, 200, 2003},
        {1004, 200, 2004},
        {1005, 300, 2005}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_predicate_histogram(db)

      assert histogram[100] == 3
      assert histogram[200] == 2
      assert histogram[300] == 1
      assert map_size(histogram) == 3
    end
  end

  # ===========================================================================
  # build_numeric_histogram/3 Tests
  # ===========================================================================

  describe "build_numeric_histogram/3" do
    test "returns nil for predicate with no numeric values", %{db: db} do
      # Non-numeric object (URI type)
      uri_id = 0b0001 <<< 60 ||| 42

      triples = [
        {1000, 100, uri_id},
        {1001, 100, uri_id}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, result} = Statistics.build_numeric_histogram(db, 100)
      assert result == nil
    end

    test "builds histogram for integer values", %{db: db} do
      # Create inline integer IDs (type 4)
      make_int_id = fn n -> 0b0100 <<< 60 ||| n end

      triples =
        for i <- 1..100 do
          {1000 + i, 100, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 100, 10)

      assert histogram != nil
      assert histogram.min == 1.0
      assert histogram.max == 100.0
      assert histogram.bucket_count == 10
      assert histogram.total_count == 100
      assert length(histogram.buckets) == 10
      assert Enum.sum(histogram.buckets) == 100
    end

    test "handles single value", %{db: db} do
      int_id = 0b0100 <<< 60 ||| 42

      :ok = Index.insert_triple(db, {1000, 100, int_id})

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 100, 10)

      assert histogram != nil
      assert histogram.total_count == 1
    end
  end

  # ===========================================================================
  # estimate_range_selectivity/4 Tests
  # ===========================================================================

  describe "estimate_range_selectivity/4" do
    test "returns 1.0 when no histogram available" do
      stats = %{numeric_histograms: %{}}

      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 100.0)
      assert selectivity == 1.0
    end

    test "estimates selectivity from histogram" do
      # Create a histogram with uniform distribution (includes bucket_width)
      histogram = %{
        min: 0.0,
        max: 100.0,
        bucket_count: 10,
        bucket_width: 10.0,
        buckets: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        total_count: 100
      }

      stats = %{numeric_histograms: %{100 => histogram}}

      # Full range should have selectivity ~1.0
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 100.0)
      assert_in_delta selectivity, 1.0, 0.01

      # Half range should have selectivity ~0.5
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 50.0)
      assert_in_delta selectivity, 0.5, 0.1

      # Small range should have low selectivity
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 10.0)
      assert selectivity < 0.2
    end

    test "handles range outside histogram bounds" do
      histogram = %{
        min: 10.0,
        max: 90.0,
        bucket_count: 10,
        bucket_width: 8.0,
        buckets: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        total_count: 100
      }

      stats = %{numeric_histograms: %{100 => histogram}}

      # Range completely outside histogram
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 5.0)
      assert selectivity == 0.0

      # Range partially overlapping
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 0.0, 50.0)
      assert selectivity > 0.0 and selectivity < 1.0
    end
  end

  # ===========================================================================
  # save/2 and load/1 Tests
  # ===========================================================================

  describe "save/2 and load/1" do
    test "persists and reloads statistics", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2001}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.collect(db)
      :ok = Statistics.save(db, stats)

      {:ok, loaded} = Statistics.load(db)

      assert loaded.triple_count == stats.triple_count
      assert loaded.distinct_subjects == stats.distinct_subjects
      assert loaded.predicate_histogram == stats.predicate_histogram
    end

    test "load returns nil when nothing saved", %{db: db} do
      {:ok, nil} = Statistics.load(db)
    end

    @tag :skip_db_close
    test "statistics persist across simulated restart" do
      # Use a separate path to avoid conflict with setup's db
      test_path = "/tmp/triple_store_stats_persist_test_#{:erlang.unique_integer([:positive])}"
      {:ok, db} = NIF.open(test_path)

      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.collect(db)
      :ok = Statistics.save(db, stats)

      # Close the db - need to wait for RocksDB to fully release lock
      :ok = NIF.close(db)

      # Force garbage collection to ensure resource is released
      :erlang.garbage_collect()
      Process.sleep(100)

      {:ok, db2} = NIF.open(test_path)

      {:ok, loaded} = Statistics.load(db2)
      assert loaded.triple_count == 2
      assert loaded.predicate_histogram[100] == 2

      NIF.close(db2)
      :erlang.garbage_collect()
      Process.sleep(50)
      File.rm_rf(test_path)
    end
  end

  # ===========================================================================
  # get/1 Tests
  # ===========================================================================

  describe "get/1" do
    test "collects and saves when no persisted stats", %{db: db} do
      triples = [{1000, 100, 2000}]
      :ok = Index.insert_triples(db, triples)

      # First call should collect and save
      {:ok, stats} = Statistics.get(db)
      assert stats.triple_count == 1

      # Should now be persisted
      {:ok, loaded} = Statistics.load(db)
      assert loaded.triple_count == 1
    end

    test "returns persisted stats when available", %{db: db} do
      # Manually save stats
      stats = %{
        triple_count: 999,
        distinct_subjects: 100,
        distinct_predicates: 10,
        distinct_objects: 200,
        predicate_histogram: %{},
        numeric_histograms: %{},
        collected_at: DateTime.utc_now(),
        version: 1
      }

      :ok = Statistics.save(db, stats)

      # Should return saved stats (not actual)
      {:ok, loaded} = Statistics.get(db)
      assert loaded.triple_count == 999
    end
  end

  # ===========================================================================
  # refresh/1 Tests
  # ===========================================================================

  describe "refresh/1" do
    test "collects fresh statistics and saves", %{db: db} do
      # Save old stats
      old_stats = %{
        triple_count: 0,
        distinct_subjects: 0,
        distinct_predicates: 0,
        distinct_objects: 0,
        predicate_histogram: %{},
        numeric_histograms: %{},
        collected_at: DateTime.utc_now(),
        version: 1
      }

      :ok = Statistics.save(db, old_stats)

      # Add data
      triples = [{1000, 100, 2000}, {1001, 100, 2001}]
      :ok = Index.insert_triples(db, triples)

      # Refresh should update
      {:ok, stats} = Statistics.refresh(db)
      assert stats.triple_count == 2

      # Should be persisted
      {:ok, loaded} = Statistics.load(db)
      assert loaded.triple_count == 2
    end
  end

  # ===========================================================================
  # Custom bucket_count Option Tests (S11)
  # ===========================================================================

  describe "bucket_count option" do
    test "builds histogram with custom bucket count", %{db: db} do
      # Create inline integer IDs (type 4)
      make_int_id = fn n -> 0b0100 <<< 60 ||| n end

      triples =
        for i <- 1..100 do
          {1000 + i, 100, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      # Test with custom bucket count of 5
      {:ok, histogram} = Statistics.build_numeric_histogram(db, 100, 5)

      assert histogram != nil
      assert histogram.bucket_count == 5
      assert length(histogram.buckets) == 5
      assert Enum.sum(histogram.buckets) == 100
    end

    test "collect respects bucket_count option", %{db: db} do
      # Create inline integer IDs
      make_int_id = fn n -> 0b0100 <<< 60 ||| n end

      triples =
        for i <- 1..50 do
          {1000 + i, 100, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.collect(db, bucket_count: 20)

      # Check that numeric histogram was built with 20 buckets
      histogram = Map.get(stats.numeric_histograms, 100)
      assert histogram != nil
      assert histogram.bucket_count == 20
      assert length(histogram.buckets) == 20
    end
  end

  # ===========================================================================
  # Decimal and DateTime Histogram Tests (C9)
  # ===========================================================================

  describe "decimal histogram" do
    test "builds histogram for decimal values", %{db: db} do
      # Create inline decimal IDs (type 5)
      # Simplified: using type tag 5 with a simple encoding
      # Decimal encoding: [type:4][sign:1][exponent:11][mantissa:48]
      # For testing, we'll use values that can be encoded
      make_decimal_id = fn value ->
        # Use Dictionary's encoding
        {:ok, id} = TripleStore.Dictionary.encode_decimal(Decimal.new(value))
        id
      end

      triples =
        for i <- 1..10 do
          value = "#{i * 10}.5"
          {1000 + i, 200, make_decimal_id.(value)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 200, 5)

      assert histogram != nil
      assert histogram.total_count == 10
      assert Enum.sum(histogram.buckets) == 10
    end
  end

  describe "datetime histogram" do
    test "builds histogram for datetime values", %{db: db} do
      # Create inline datetime IDs (type 6)
      make_datetime_id = fn days_offset ->
        dt = DateTime.add(~U[2024-01-01 00:00:00Z], days_offset * 86400, :second)
        {:ok, id} = TripleStore.Dictionary.encode_datetime(dt)
        id
      end

      triples =
        for i <- 1..10 do
          {1000 + i, 300, make_datetime_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 300, 5)

      assert histogram != nil
      assert histogram.total_count == 10
      assert Enum.sum(histogram.buckets) == 10
    end
  end

  describe "negative integer histogram" do
    test "builds histogram for negative integers", %{db: db} do
      make_int_id = fn n ->
        {:ok, id} = TripleStore.Dictionary.encode_integer(n)
        id
      end

      triples =
        for i <- -10..-1 do
          {1000 + abs(i), 400, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 400, 5)

      assert histogram != nil
      assert histogram.min < 0.0
      assert histogram.total_count == 10
      assert Enum.sum(histogram.buckets) == 10
    end
  end

  # ===========================================================================
  # Error Handling Tests (C8)
  # ===========================================================================

  describe "error handling" do
    test "load returns error for invalid stats structure", %{db: db} do
      # Save invalid structure directly
      invalid_data = :erlang.term_to_binary(%{foo: :bar}, [:compressed])
      :ok = NIF.put(db, :id2str, <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>, invalid_data)

      # Load should detect invalid structure
      assert {:error, :invalid_stats_structure} = Statistics.load(db)
    end

    test "load handles missing required keys", %{db: db} do
      # Save partial structure
      partial = %{triple_count: 100}
      partial_data = :erlang.term_to_binary(partial, [:compressed])
      :ok = NIF.put(db, :id2str, <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>, partial_data)

      assert {:error, :invalid_stats_structure} = Statistics.load(db)
    end
  end

  # ===========================================================================
  # Statistics Structure Validation Tests (C7)
  # ===========================================================================

  describe "stats structure validation" do
    test "load validates all required keys present", %{db: db} do
      # Create valid stats
      valid_stats = %{
        triple_count: 10,
        distinct_subjects: 5,
        distinct_predicates: 2,
        distinct_objects: 8,
        predicate_histogram: %{},
        numeric_histograms: %{},
        collected_at: DateTime.utc_now(),
        version: 1
      }

      :ok = Statistics.save(db, valid_stats)
      assert {:ok, loaded} = Statistics.load(db)
      assert loaded.triple_count == 10
    end
  end

  # ===========================================================================
  # Version Migration Tests (S13)
  # ===========================================================================

  describe "version migration" do
    test "migrate_stats_if_needed passes through current version stats" do
      stats = %{
        triple_count: 100,
        distinct_subjects: 10,
        distinct_predicates: 5,
        distinct_objects: 20,
        predicate_histogram: %{},
        numeric_histograms: %{},
        collected_at: DateTime.utc_now(),
        version: 1
      }

      migrated = Statistics.migrate_stats_if_needed(stats)
      assert migrated == stats
    end

    test "migrate_stats_if_needed adds bucket_width to old histograms" do
      # Stats without bucket_width in histogram
      old_histogram = %{
        min: 0.0,
        max: 100.0,
        bucket_count: 10,
        buckets: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        total_count: 100
      }

      stats = %{
        triple_count: 100,
        distinct_subjects: 10,
        distinct_predicates: 5,
        distinct_objects: 20,
        predicate_histogram: %{},
        numeric_histograms: %{42 => old_histogram},
        collected_at: DateTime.utc_now(),
        version: 0
      }

      migrated = Statistics.migrate_stats_if_needed(stats)

      assert migrated.version == 1
      assert Map.has_key?(migrated.numeric_histograms[42], :bucket_width)
      assert migrated.numeric_histograms[42].bucket_width == 10.0
    end
  end

  # ===========================================================================
  # Histogram bucket_width Tests (S4)
  # ===========================================================================

  describe "histogram bucket_width" do
    test "histogram includes bucket_width field", %{db: db} do
      make_int_id = fn n -> 0b0100 <<< 60 ||| n end

      triples =
        for i <- 1..100 do
          {1000 + i, 100, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, histogram} = Statistics.build_numeric_histogram(db, 100, 10)

      assert Map.has_key?(histogram, :bucket_width)
      assert histogram.bucket_width > 0
      # With values 1-100 in 10 buckets, bucket width should be ~10
      assert_in_delta histogram.bucket_width, 9.9, 0.5
    end

    test "estimate_range_selectivity uses bucket_width from histogram", %{db: db} do
      make_int_id = fn n -> 0b0100 <<< 60 ||| n end

      triples =
        for i <- 1..100 do
          {1000 + i, 100, make_int_id.(i)}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.collect(db, bucket_count: 10)

      # Should work without recalculating bucket_width
      selectivity = Statistics.estimate_range_selectivity(stats, 100, 1.0, 50.0)
      assert selectivity > 0.0 and selectivity < 1.0
    end
  end
end
