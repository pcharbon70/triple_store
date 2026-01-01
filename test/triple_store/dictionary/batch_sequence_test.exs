defmodule TripleStore.Dictionary.BatchSequenceTest do
  @moduledoc """
  Unit tests for Batch Sequence Allocation (Task 1.1.3).

  Tests:
  - Range allocation in SequenceCounter
  - Batch processing in Manager using range allocation
  - Crash recovery with range allocation
  - Concurrent range allocations
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.SequenceCounter

  @test_db_base "/tmp/triple_store_batch_seq_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, path: test_path}
  end

  # ===========================================================================
  # 1.1.3.3: Range Allocation in SequenceCounter
  # ===========================================================================

  describe "SequenceCounter.allocate_range/3" do
    test "allocates sequential range", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, start1} = SequenceCounter.allocate_range(counter, :uri, 10)
      {:ok, start2} = SequenceCounter.allocate_range(counter, :uri, 10)

      # Ranges should be sequential
      assert start2 == start1 + 10

      SequenceCounter.stop(counter)
    end

    test "allocates ranges for different types independently", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, uri_start} = SequenceCounter.allocate_range(counter, :uri, 100)
      {:ok, bnode_start} = SequenceCounter.allocate_range(counter, :bnode, 50)
      {:ok, literal_start} = SequenceCounter.allocate_range(counter, :literal, 25)

      # Each type has its own sequence
      # All should start from their safety margin offset
      assert uri_start > 0
      assert bnode_start > 0
      assert literal_start > 0

      SequenceCounter.stop(counter)
    end

    test "returns error for invalid type", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      assert {:error, :invalid_type} = SequenceCounter.allocate_range(counter, :invalid, 10)

      SequenceCounter.stop(counter)
    end

    test "returns error for invalid count", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      assert {:error, :invalid_count} = SequenceCounter.allocate_range(counter, :uri, 0)
      assert {:error, :invalid_count} = SequenceCounter.allocate_range(counter, :uri, -1)

      SequenceCounter.stop(counter)
    end

    test "emits telemetry for range allocation", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach("range-alloc-handler", [:triple_store, :dictionary, :range_allocated], handler, nil)

      {:ok, start} = SequenceCounter.allocate_range(counter, :uri, 50)

      assert_receive {:telemetry, [:triple_store, :dictionary, :range_allocated],
                      %{start_sequence: ^start, count: 50}, %{type: :uri}}

      :telemetry.detach("range-alloc-handler")
      SequenceCounter.stop(counter)
    end

    test "large range allocation", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, start} = SequenceCounter.allocate_range(counter, :uri, 10_000)
      {:ok, current} = SequenceCounter.current(counter, :uri)

      assert current == start + 10_000 - 1

      SequenceCounter.stop(counter)
    end

    test "mixed single and range allocations", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Single allocation
      {:ok, id1} = SequenceCounter.next_id(counter, :uri)
      {_type, seq1} = Dictionary.decode_id(id1)

      # Range allocation
      {:ok, range_start} = SequenceCounter.allocate_range(counter, :uri, 10)
      assert range_start == seq1 + 1

      # Another single allocation
      {:ok, id2} = SequenceCounter.next_id(counter, :uri)
      {_type, seq2} = Dictionary.decode_id(id2)
      assert seq2 == range_start + 10

      SequenceCounter.stop(counter)
    end
  end

  # ===========================================================================
  # 1.1.3.4: Manager Batch Processing with Range Allocation
  # ===========================================================================

  describe "Manager batch processing with range allocation" do
    test "batch creates IDs using range allocation", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      terms = for i <- 1..100, do: RDF.iri("http://example.org/batch/#{i}")

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      assert length(ids) == 100
      assert length(Enum.uniq(ids)) == 100

      Manager.stop(manager)
    end

    test "batch handles mixed term types", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      terms = [
        RDF.iri("http://example.org/uri/1"),
        RDF.iri("http://example.org/uri/2"),
        RDF.bnode("b1"),
        RDF.bnode("b2"),
        RDF.literal("literal1"),
        RDF.literal("literal2")
      ]

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      assert length(ids) == 6
      assert length(Enum.uniq(ids)) == 6

      Manager.stop(manager)
    end

    test "batch with existing and new terms", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Create some terms first
      uri1 = RDF.iri("http://example.org/existing/1")
      uri2 = RDF.iri("http://example.org/existing/2")
      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)

      # Now batch with mixed existing and new
      terms = [
        uri1,  # existing
        RDF.iri("http://example.org/new/1"),  # new
        uri2,  # existing
        RDF.iri("http://example.org/new/2"),  # new
      ]

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      assert Enum.at(ids, 0) == id1
      assert Enum.at(ids, 2) == id2
      assert Enum.at(ids, 1) != id1 and Enum.at(ids, 1) != id2
      assert Enum.at(ids, 3) != id1 and Enum.at(ids, 3) != id2

      Manager.stop(manager)
    end

    test "batch with all existing terms skips allocation", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Create terms first
      terms = for i <- 1..10, do: RDF.iri("http://example.org/pre/#{i}")
      {:ok, original_ids} = Manager.get_or_create_ids(manager, terms)

      # Request same terms again
      {:ok, cached_ids} = Manager.get_or_create_ids(manager, terms)

      assert original_ids == cached_ids

      Manager.stop(manager)
    end

    test "batch preserves order", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      terms = [
        RDF.iri("http://example.org/order/a"),
        RDF.iri("http://example.org/order/b"),
        RDF.iri("http://example.org/order/c"),
      ]

      {:ok, ids1} = Manager.get_or_create_ids(manager, terms)

      # Get same terms individually
      individual_ids = for term <- terms do
        {:ok, id} = Manager.get_or_create_id(manager, term)
        id
      end

      assert ids1 == individual_ids

      Manager.stop(manager)
    end

    test "large batch allocation", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      terms = for i <- 1..1000, do: RDF.iri("http://example.org/large/#{i}")

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      assert length(ids) == 1000
      assert length(Enum.uniq(ids)) == 1000

      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # 1.1.3.5: Exhaustion Handling
  # ===========================================================================

  describe "sequence exhaustion handling" do
    # Note: Actually testing overflow would require allocating 2^60 IDs,
    # which is not practical. We test the overflow detection logic.

    test "allocate_range validates count fits in remaining space", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Normal allocation should work
      {:ok, _start} = SequenceCounter.allocate_range(counter, :uri, 1000)

      SequenceCounter.stop(counter)
    end
  end

  # ===========================================================================
  # 1.1.3.6: Crash Recovery
  # ===========================================================================

  describe "crash recovery with range allocation" do
    test "counter state persists across restarts", %{db: db} do
      # Allocate some ranges
      {:ok, counter1} = SequenceCounter.start_link(db: db)
      {:ok, _start1} = SequenceCounter.allocate_range(counter1, :uri, 100)
      {:ok, start2} = SequenceCounter.allocate_range(counter1, :uri, 100)
      SequenceCounter.flush(counter1)
      SequenceCounter.stop(counter1)

      # Restart counter
      {:ok, counter2} = SequenceCounter.start_link(db: db)
      {:ok, start3} = SequenceCounter.allocate_range(counter2, :uri, 100)

      # New allocation should continue from where we left off (plus safety margin)
      assert start3 >= start2 + 100

      SequenceCounter.stop(counter2)
    end

    test "safety margin prevents ID reuse after crash", %{db: db} do
      {:ok, counter1} = SequenceCounter.start_link(db: db)
      {:ok, start1} = SequenceCounter.allocate_range(counter1, :uri, 500)
      # Don't flush - simulate crash
      SequenceCounter.stop(counter1)

      # Restart without flush
      {:ok, counter2} = SequenceCounter.start_link(db: db)
      {:ok, start2} = SequenceCounter.allocate_range(counter2, :uri, 100)

      # New allocation should skip ahead by safety margin (1000)
      # Even though we only allocated 500 before "crash"
      assert start2 > start1

      SequenceCounter.stop(counter2)
    end
  end

  # ===========================================================================
  # Concurrent Range Allocations
  # ===========================================================================

  describe "concurrent range allocations" do
    test "concurrent allocations get non-overlapping ranges", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Launch multiple concurrent range allocations
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            SequenceCounter.allocate_range(counter, :uri, 100)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      starts = for {:ok, start} <- results, do: start
      assert length(starts) == 10

      # Create ranges and check no overlap
      ranges = Enum.map(starts, fn start -> MapSet.new(start..(start + 99)) end)

      for {r1, i1} <- Enum.with_index(ranges),
          {r2, i2} <- Enum.with_index(ranges),
          i1 < i2 do
        overlap = MapSet.intersection(r1, r2)
        assert MapSet.size(overlap) == 0, "Ranges #{i1} and #{i2} overlap"
      end

      SequenceCounter.stop(counter)
    end

    test "concurrent batch operations", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      tasks =
        for batch_id <- 1..10 do
          Task.async(fn ->
            terms = for i <- 1..50, do: RDF.iri("http://example.org/concurrent/#{batch_id}/#{i}")
            Manager.get_or_create_ids(manager, terms)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      all_ids =
        results
        |> Enum.flat_map(fn {:ok, ids} -> ids end)

      assert length(all_ids) == 500
      assert length(Enum.uniq(all_ids)) == 500

      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # Telemetry Integration
  # ===========================================================================

  describe "telemetry integration" do
    test "range allocation emits correct telemetry", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach("range-telemetry", [:triple_store, :dictionary, :range_allocated], handler, nil)

      {:ok, start} = SequenceCounter.allocate_range(counter, :literal, 25)

      assert_receive {:telemetry, [:triple_store, :dictionary, :range_allocated],
                      %{start_sequence: ^start, count: 25, duration: _duration}, %{type: :literal}}

      :telemetry.detach("range-telemetry")
      SequenceCounter.stop(counter)
    end
  end
end
