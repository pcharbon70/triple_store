defmodule TripleStore.Index.NumericRangeTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index.NumericRange

  @moduletag :integration

  setup do
    # Create a unique test database
    path = "/tmp/triple_store_numeric_range_test_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(path)
    NumericRange.init()

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf!(path)
    end)

    {:ok, db: db, path: path}
  end

  describe "float_to_sortable_bytes/1" do
    test "positive floats sort correctly" do
      bytes_1 = NumericRange.float_to_sortable_bytes(1.0)
      bytes_10 = NumericRange.float_to_sortable_bytes(10.0)
      bytes_100 = NumericRange.float_to_sortable_bytes(100.0)
      bytes_1000 = NumericRange.float_to_sortable_bytes(1000.0)

      assert bytes_1 < bytes_10
      assert bytes_10 < bytes_100
      assert bytes_100 < bytes_1000
    end

    test "negative floats sort correctly" do
      bytes_neg1000 = NumericRange.float_to_sortable_bytes(-1000.0)
      bytes_neg100 = NumericRange.float_to_sortable_bytes(-100.0)
      bytes_neg10 = NumericRange.float_to_sortable_bytes(-10.0)
      bytes_neg1 = NumericRange.float_to_sortable_bytes(-1.0)

      assert bytes_neg1000 < bytes_neg100
      assert bytes_neg100 < bytes_neg10
      assert bytes_neg10 < bytes_neg1
    end

    test "negative floats sort before positive floats" do
      bytes_neg1 = NumericRange.float_to_sortable_bytes(-1.0)
      bytes_0 = NumericRange.float_to_sortable_bytes(0.0)
      bytes_1 = NumericRange.float_to_sortable_bytes(1.0)

      assert bytes_neg1 < bytes_0
      assert bytes_0 < bytes_1
    end

    test "zero sorts in the middle" do
      bytes_neg_small = NumericRange.float_to_sortable_bytes(-0.001)
      bytes_0 = NumericRange.float_to_sortable_bytes(0.0)
      bytes_small = NumericRange.float_to_sortable_bytes(0.001)

      assert bytes_neg_small < bytes_0
      assert bytes_0 < bytes_small
    end

    test "handles integers by converting to float" do
      bytes_int = NumericRange.float_to_sortable_bytes(42)
      bytes_float = NumericRange.float_to_sortable_bytes(42.0)

      assert bytes_int == bytes_float
    end

    test "preserves precision for decimal values" do
      values = [0.1, 0.123456789, 1.5, 99.99, 123.456789012345]

      for value <- values do
        bytes = NumericRange.float_to_sortable_bytes(value)
        recovered = NumericRange.sortable_bytes_to_float(bytes)
        assert recovered == value, "Failed for value #{value}"
      end
    end
  end

  describe "sortable_bytes_to_float/1" do
    test "roundtrip for positive values" do
      values = [0.0, 0.5, 1.0, 10.0, 100.0, 1000.0, 1_000_000.0]

      for value <- values do
        bytes = NumericRange.float_to_sortable_bytes(value)
        assert NumericRange.sortable_bytes_to_float(bytes) == value
      end
    end

    test "roundtrip for negative values" do
      values = [-0.5, -1.0, -10.0, -100.0, -1000.0, -1_000_000.0]

      for value <- values do
        bytes = NumericRange.float_to_sortable_bytes(value)
        assert NumericRange.sortable_bytes_to_float(bytes) == value
      end
    end

    test "roundtrip for edge values" do
      # Very small and very large floats
      values = [1.0e-100, 1.0e100, -1.0e-100, -1.0e100]

      for value <- values do
        bytes = NumericRange.float_to_sortable_bytes(value)
        assert NumericRange.sortable_bytes_to_float(bytes) == value
      end
    end
  end

  describe "create_range_index/2 and has_range_index?/1" do
    test "registers predicate for range indexing", %{db: db} do
      predicate_id = 12345

      refute NumericRange.has_range_index?(predicate_id)
      assert {:ok, ^predicate_id} = NumericRange.create_range_index(db, predicate_id)
      assert NumericRange.has_range_index?(predicate_id)
    end

    test "lists registered predicates", %{db: db} do
      pred1 = 100
      pred2 = 200
      pred3 = 300

      NumericRange.create_range_index(db, pred1)
      NumericRange.create_range_index(db, pred2)
      NumericRange.create_range_index(db, pred3)

      predicates = NumericRange.list_range_predicates()
      assert pred1 in predicates
      assert pred2 in predicates
      assert pred3 in predicates
    end
  end

  describe "index_value/4 and range_query/4" do
    test "indexes and retrieves values correctly", %{db: db} do
      predicate_id = 1000
      NumericRange.create_range_index(db, predicate_id)

      # Index some values
      :ok = NumericRange.index_value(db, predicate_id, 1, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, 100.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 200.0)
      :ok = NumericRange.index_value(db, predicate_id, 4, 500.0)

      # Query full range
      {:ok, results} = NumericRange.range_query(db, predicate_id, 0.0, 1000.0)
      assert length(results) == 4
    end

    test "range query with bounds filters correctly", %{db: db} do
      predicate_id = 1001
      NumericRange.create_range_index(db, predicate_id)

      # Index values: 10, 50, 100, 200, 500
      :ok = NumericRange.index_value(db, predicate_id, 1, 10.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 100.0)
      :ok = NumericRange.index_value(db, predicate_id, 4, 200.0)
      :ok = NumericRange.index_value(db, predicate_id, 5, 500.0)

      # Query range [50, 200]
      {:ok, results} = NumericRange.range_query(db, predicate_id, 50.0, 200.0)
      subject_ids = Enum.map(results, fn {id, _} -> id end)

      assert 2 in subject_ids
      assert 3 in subject_ids
      assert 4 in subject_ids
      refute 1 in subject_ids
      refute 5 in subject_ids
    end

    test "range query with unbounded min", %{db: db} do
      predicate_id = 1002
      NumericRange.create_range_index(db, predicate_id)

      :ok = NumericRange.index_value(db, predicate_id, 1, 10.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 100.0)

      # Query from -infinity to 50
      {:ok, results} = NumericRange.range_query(db, predicate_id, :unbounded, 50.0)
      subject_ids = Enum.map(results, fn {id, _} -> id end)

      assert 1 in subject_ids
      assert 2 in subject_ids
      refute 3 in subject_ids
    end

    test "range query with unbounded max", %{db: db} do
      predicate_id = 1003
      NumericRange.create_range_index(db, predicate_id)

      :ok = NumericRange.index_value(db, predicate_id, 1, 10.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 100.0)

      # Query from 50 to +infinity
      {:ok, results} = NumericRange.range_query(db, predicate_id, 50.0, :unbounded)
      subject_ids = Enum.map(results, fn {id, _} -> id end)

      refute 1 in subject_ids
      assert 2 in subject_ids
      assert 3 in subject_ids
    end

    test "range query returns correct values", %{db: db} do
      predicate_id = 1004
      NumericRange.create_range_index(db, predicate_id)

      :ok = NumericRange.index_value(db, predicate_id, 1, 99.99)
      :ok = NumericRange.index_value(db, predicate_id, 2, 199.99)

      {:ok, results} = NumericRange.range_query(db, predicate_id, 0.0, 300.0)

      result_map = Map.new(results)
      assert result_map[1] == 99.99
      assert result_map[2] == 199.99
    end

    test "handles negative values in range", %{db: db} do
      predicate_id = 1005
      NumericRange.create_range_index(db, predicate_id)

      :ok = NumericRange.index_value(db, predicate_id, 1, -100.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, -50.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 0.0)
      :ok = NumericRange.index_value(db, predicate_id, 4, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 5, 100.0)

      # Query range [-50, 50]
      {:ok, results} = NumericRange.range_query(db, predicate_id, -50.0, 50.0)
      subject_ids = Enum.map(results, fn {id, _} -> id end)

      refute 1 in subject_ids
      assert 2 in subject_ids
      assert 3 in subject_ids
      assert 4 in subject_ids
      refute 5 in subject_ids
    end
  end

  describe "delete_value/4" do
    test "removes value from index", %{db: db} do
      predicate_id = 2000
      NumericRange.create_range_index(db, predicate_id)

      :ok = NumericRange.index_value(db, predicate_id, 1, 50.0)
      :ok = NumericRange.index_value(db, predicate_id, 2, 100.0)
      :ok = NumericRange.index_value(db, predicate_id, 3, 150.0)

      # Verify all three are present
      {:ok, results} = NumericRange.range_query(db, predicate_id, 0.0, 200.0)
      assert length(results) == 3

      # Delete one
      :ok = NumericRange.delete_value(db, predicate_id, 2, 100.0)

      # Verify only two remain
      {:ok, results} = NumericRange.range_query(db, predicate_id, 0.0, 200.0)
      assert length(results) == 2
      subject_ids = Enum.map(results, fn {id, _} -> id end)
      refute 2 in subject_ids
    end
  end

  describe "build_index_operation/3 and build_delete_operation/3" do
    test "builds correct put operation" do
      {op_type, cf, key, value} = NumericRange.build_index_operation(1000, 42, 99.99)

      assert op_type == :put
      assert cf == :numeric_range
      assert is_binary(key)
      assert value == <<>>

      # Key should be 24 bytes: predicate(8) + sortable_value(8) + subject(8)
      assert byte_size(key) == 24
    end

    test "builds correct delete operation" do
      {op_type, cf, key} = NumericRange.build_delete_operation(1000, 42, 99.99)

      assert op_type == :delete
      assert cf == :numeric_range
      assert is_binary(key)
      assert byte_size(key) == 24
    end

    test "operations can be used with mixed_batch", %{db: db} do
      predicate_id = 3000
      NumericRange.create_range_index(db, predicate_id)

      # Build batch operations
      ops = [
        NumericRange.build_index_operation(predicate_id, 1, 10.0),
        NumericRange.build_index_operation(predicate_id, 2, 20.0),
        NumericRange.build_index_operation(predicate_id, 3, 30.0)
      ]

      :ok = NIF.mixed_batch(db, ops, true)

      # Verify values were indexed
      {:ok, results} = NumericRange.range_query(db, predicate_id, 0.0, 50.0)
      assert length(results) == 3
    end
  end

  describe "sorting stress test" do
    test "many values sort correctly", %{db: db} do
      predicate_id = 4000
      NumericRange.create_range_index(db, predicate_id)

      # Generate 1000 random values
      :rand.seed(:exsss, {1, 2, 3})
      values = for i <- 1..1000, do: {i, :rand.uniform() * 1000 - 500}

      # Index all values
      for {id, value} <- values do
        :ok = NumericRange.index_value(db, predicate_id, id, value)
      end

      # Query all
      {:ok, results} = NumericRange.range_query(db, predicate_id, -500.0, 500.0)
      assert length(results) == 1000

      # Verify results are sorted by value
      result_values = Enum.map(results, fn {_, v} -> v end)
      assert result_values == Enum.sort(result_values)
    end
  end
end
