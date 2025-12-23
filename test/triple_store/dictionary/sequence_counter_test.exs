defmodule TripleStore.Dictionary.SequenceCounterTest do
  @moduledoc """
  Tests for the SequenceCounter GenServer (Task 1.3.2).

  Covers:
  - Counter initialization from RocksDB
  - Atomic ID generation for each type
  - Periodic persistence
  - Recovery with safety margin
  - Concurrent access
  - Overflow protection
  """
  use TripleStore.PooledDbCase

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.SequenceCounter

  describe "start_link/1" do
    test "starts counter with database reference", %{db: db} do
      assert {:ok, counter} = SequenceCounter.start_link(db: db)
      assert Process.alive?(counter)
      SequenceCounter.stop(counter)
    end

    test "starts counter with name registration", %{db: db} do
      assert {:ok, counter} = SequenceCounter.start_link(db: db, name: TestCounter)
      assert Process.whereis(TestCounter) == counter
      SequenceCounter.stop(counter)
    end

    test "initializes counters with safety margin on fresh database", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Fresh database should start at safety_margin (1000)
      {:ok, current_uri} = SequenceCounter.current(counter, :uri)
      {:ok, current_bnode} = SequenceCounter.current(counter, :bnode)
      {:ok, current_literal} = SequenceCounter.current(counter, :literal)

      assert current_uri == Dictionary.safety_margin()
      assert current_bnode == Dictionary.safety_margin()
      assert current_literal == Dictionary.safety_margin()

      SequenceCounter.stop(counter)
    end
  end

  describe "next_id/2" do
    test "returns valid URI ID", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, id} = SequenceCounter.next_id(counter, :uri)
      assert Dictionary.term_type(id) == :uri

      SequenceCounter.stop(counter)
    end

    test "returns valid BNode ID", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, id} = SequenceCounter.next_id(counter, :bnode)
      assert Dictionary.term_type(id) == :bnode

      SequenceCounter.stop(counter)
    end

    test "returns valid Literal ID", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, id} = SequenceCounter.next_id(counter, :literal)
      assert Dictionary.term_type(id) == :literal

      SequenceCounter.stop(counter)
    end

    test "returns monotonically increasing IDs", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      ids =
        for _ <- 1..100 do
          {:ok, id} = SequenceCounter.next_id(counter, :uri)
          id
        end

      # All IDs should be unique
      assert length(Enum.uniq(ids)) == 100

      # Extract sequences and verify they're increasing
      sequences = Enum.map(ids, fn id -> elem(Dictionary.decode_id(id), 1) end)
      assert sequences == Enum.sort(sequences)

      SequenceCounter.stop(counter)
    end

    test "returns error for invalid type", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      assert {:error, :invalid_type} = SequenceCounter.next_id(counter, :invalid)
      assert {:error, :invalid_type} = SequenceCounter.next_id(counter, :integer)

      SequenceCounter.stop(counter)
    end

    test "each type has independent sequence", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Get first ID of each type
      {:ok, uri_1} = SequenceCounter.next_id(counter, :uri)
      {:ok, bnode_1} = SequenceCounter.next_id(counter, :bnode)
      {:ok, literal_1} = SequenceCounter.next_id(counter, :literal)

      # All should have the same sequence number (safety_margin + 1)
      {_, uri_seq} = Dictionary.decode_id(uri_1)
      {_, bnode_seq} = Dictionary.decode_id(bnode_1)
      {_, literal_seq} = Dictionary.decode_id(literal_1)

      expected_first_seq = Dictionary.safety_margin() + 1
      assert uri_seq == expected_first_seq
      assert bnode_seq == expected_first_seq
      assert literal_seq == expected_first_seq

      SequenceCounter.stop(counter)
    end
  end

  describe "current/2" do
    test "returns current sequence without incrementing", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, before} = SequenceCounter.current(counter, :uri)
      {:ok, before2} = SequenceCounter.current(counter, :uri)

      assert before == before2

      {:ok, _id} = SequenceCounter.next_id(counter, :uri)

      {:ok, after_next} = SequenceCounter.current(counter, :uri)
      assert after_next == before + 1

      SequenceCounter.stop(counter)
    end

    test "returns error for invalid type", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)
      assert {:error, :invalid_type} = SequenceCounter.current(counter, :invalid)
      SequenceCounter.stop(counter)
    end
  end

  describe "flush/1" do
    test "persists all counters to RocksDB", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Generate some IDs
      for _ <- 1..10, do: SequenceCounter.next_id(counter, :uri)
      for _ <- 1..20, do: SequenceCounter.next_id(counter, :bnode)
      for _ <- 1..30, do: SequenceCounter.next_id(counter, :literal)

      # Get current values
      {:ok, uri_val} = SequenceCounter.current(counter, :uri)
      {:ok, bnode_val} = SequenceCounter.current(counter, :bnode)
      {:ok, literal_val} = SequenceCounter.current(counter, :literal)

      # Flush
      assert :ok = SequenceCounter.flush(counter)

      # Stop counter
      SequenceCounter.stop(counter)

      # Verify persisted values in RocksDB
      {:ok, uri_bin} = NIF.get(db, :str2id, "__seq_counter__uri")
      {:ok, bnode_bin} = NIF.get(db, :str2id, "__seq_counter__bnode")
      {:ok, literal_bin} = NIF.get(db, :str2id, "__seq_counter__literal")

      <<uri_persisted::64-big>> = uri_bin
      <<bnode_persisted::64-big>> = bnode_bin
      <<literal_persisted::64-big>> = literal_bin

      assert uri_persisted == uri_val
      assert bnode_persisted == bnode_val
      assert literal_persisted == literal_val
    end
  end

  describe "recovery with safety margin" do
    test "adds safety margin on restart", %{db: db} do
      # Start first counter and generate IDs
      {:ok, counter1} = SequenceCounter.start_link(db: db)

      for _ <- 1..50, do: SequenceCounter.next_id(counter1, :uri)
      {:ok, uri_before_stop} = SequenceCounter.current(counter1, :uri)

      # Flush and stop
      SequenceCounter.flush(counter1)
      SequenceCounter.stop(counter1)

      # Start new counter with same database
      {:ok, counter2} = SequenceCounter.start_link(db: db)

      {:ok, uri_after_restart} = SequenceCounter.current(counter2, :uri)

      # Should be persisted value + safety margin
      assert uri_after_restart == uri_before_stop + Dictionary.safety_margin()

      SequenceCounter.stop(counter2)
    end

    test "handles crash recovery without ID reuse", %{db: db} do
      # Start counter and generate IDs - flush manually to simulate normal operation
      {:ok, counter1} = SequenceCounter.start_link(db: db)

      # Generate some IDs
      for _ <- 1..100 do
        {:ok, _id} = SequenceCounter.next_id(counter1, :uri)
      end

      # Flush to simulate periodic persistence that would happen in production
      :ok = SequenceCounter.flush(counter1)

      # Generate more IDs after flush (these won't be persisted before crash)
      generated_after_flush =
        for _ <- 1..100 do
          {:ok, id} = SequenceCounter.next_id(counter1, :uri)
          id
        end

      # Simulate crash - unlink first so test doesn't die, then kill
      Process.unlink(counter1)
      Process.exit(counter1, :kill)
      Process.sleep(10)

      # Restart counter
      {:ok, counter2} = SequenceCounter.start_link(db: db)

      # Generate more IDs
      new_ids =
        for _ <- 1..10 do
          {:ok, id} = SequenceCounter.next_id(counter2, :uri)
          id
        end

      # No overlap should exist due to safety margin
      overlap = MapSet.intersection(MapSet.new(generated_after_flush), MapSet.new(new_ids))
      assert MapSet.size(overlap) == 0

      # New sequences should be higher than old ones
      old_max =
        generated_after_flush |> Enum.map(&elem(Dictionary.decode_id(&1), 1)) |> Enum.max()

      new_min = new_ids |> Enum.map(&elem(Dictionary.decode_id(&1), 1)) |> Enum.min()
      assert new_min > old_max

      SequenceCounter.stop(counter2)
    end
  end

  describe "periodic persistence" do
    test "auto-flushes after flush_interval allocations", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Get initial persisted value (should be 0 for fresh db)
      initial_persisted =
        case NIF.get(db, :str2id, "__seq_counter__uri") do
          {:ok, bin} ->
            <<val::64-big>> = bin
            val

          :not_found ->
            0
        end

      # Generate exactly flush_interval IDs
      for _ <- 1..Dictionary.flush_interval() do
        SequenceCounter.next_id(counter, :uri)
      end

      # Should have auto-flushed
      {:ok, bin} = NIF.get(db, :str2id, "__seq_counter__uri")
      <<persisted::64-big>> = bin

      # Persisted value should be updated
      assert persisted > initial_persisted

      SequenceCounter.stop(counter)
    end
  end

  describe "concurrent access" do
    test "handles concurrent ID generation safely", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Spawn many concurrent tasks
      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            for _ <- 1..10 do
              {:ok, id} = SequenceCounter.next_id(counter, :uri)
              id
            end
          end)
        end

      all_ids = tasks |> Task.await_many() |> List.flatten()

      # All IDs should be unique
      assert length(all_ids) == 1000
      assert length(Enum.uniq(all_ids)) == 1000

      SequenceCounter.stop(counter)
    end

    test "handles concurrent access to different types", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      tasks =
        for type <- [:uri, :bnode, :literal], _ <- 1..50 do
          Task.async(fn ->
            {:ok, id} = SequenceCounter.next_id(counter, type)
            {type, id}
          end)
        end

      results = Task.await_many(tasks)

      # Group by type
      by_type = Enum.group_by(results, &elem(&1, 0), &elem(&1, 1))

      # Each type should have 50 unique IDs
      for type <- [:uri, :bnode, :literal] do
        ids = by_type[type]
        assert length(ids) == 50
        assert length(Enum.uniq(ids)) == 50
      end

      SequenceCounter.stop(counter)
    end
  end

  describe "stop/1" do
    test "flushes counters on graceful stop", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Generate IDs (less than flush_interval so no auto-flush)
      for _ <- 1..100, do: SequenceCounter.next_id(counter, :uri)
      {:ok, final_val} = SequenceCounter.current(counter, :uri)

      # Stop (should flush)
      :ok = SequenceCounter.stop(counter)

      # Verify flushed
      {:ok, bin} = NIF.get(db, :str2id, "__seq_counter__uri")
      <<persisted::64-big>> = bin
      assert persisted == final_val
    end
  end

  describe "edge cases" do
    test "handles first ID generation correctly", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      {:ok, first_id} = SequenceCounter.next_id(counter, :uri)
      {type, seq} = Dictionary.decode_id(first_id)

      assert type == :uri
      # First ID should be safety_margin + 1
      assert seq == Dictionary.safety_margin() + 1

      SequenceCounter.stop(counter)
    end

    test "handles large batch generation", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Generate 5000 IDs (triggers multiple auto-flushes)
      ids =
        for _ <- 1..5000 do
          {:ok, id} = SequenceCounter.next_id(counter, :uri)
          id
        end

      assert length(Enum.uniq(ids)) == 5000

      SequenceCounter.stop(counter)
    end
  end

  describe "overflow protection" do
    test "returns error when sequence counter approaches max", %{db: db} do
      # Pre-seed the counter to near max value
      near_max = Dictionary.max_sequence() - 1
      key = "__seq_counter__uri"
      :ok = NIF.put(db, :str2id, key, <<near_max::64-big>>)

      {:ok, counter} = SequenceCounter.start_link(db: db)

      # First ID should work (at safety_margin above near_max, so will overflow)
      # The counter starts at near_max + safety_margin which is > max_sequence
      # Therefore next_id should fail immediately
      result = SequenceCounter.next_id(counter, :uri)

      assert result == {:error, :sequence_overflow}

      SequenceCounter.stop(counter)
    end

    test "rolls back counter on overflow attempt", %{db: db} do
      # Pre-seed to exactly max - safety_margin - 1 so first ID works but second fails
      start_val = Dictionary.max_sequence() - Dictionary.safety_margin() - 1
      key = "__seq_counter__uri"
      :ok = NIF.put(db, :str2id, key, <<start_val::64-big>>)

      {:ok, counter} = SequenceCounter.start_link(db: db)

      # First ID should succeed (counter is at start_val + safety_margin = max - 1)
      {:ok, _id} = SequenceCounter.next_id(counter, :uri)

      # Second ID should fail (would be at max_sequence + 1)
      assert {:error, :sequence_overflow} = SequenceCounter.next_id(counter, :uri)

      # Verify counter was rolled back - current should be at max_sequence
      {:ok, current} = SequenceCounter.current(counter, :uri)
      assert current == Dictionary.max_sequence()

      SequenceCounter.stop(counter)
    end
  end
end
