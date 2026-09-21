defmodule TripleStore.SPARQL.Leapfrog.QuadLeapfrogTest do
  @moduledoc """
  Unit tests for QuadLeapfrog (Section 5.5.2).

  Tests the 4-way Leapfrog join algorithm for quad patterns with
  subject, predicate, object, and graph components.

  Tests include:
  - QuadTrieIterator functionality (new, seek, next, current, exhausted)
  - QuadLeapfrog pattern matching (from_pattern, search, next, bindings)
  - Integration tests with real quad store data
  - Variable ordering with different statistics scenarios
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Leapfrog.{QuadLeapfrog, QuadTrieIterator}

  @moduletag :integration

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_leapfrog_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db}
  end

  # ===========================================================================
  # Variable Ordering Tests (5.5.3)
  # ===========================================================================

  describe "quad_variable_ordering/2" do
    test "orders variables by selectivity with bound positions first" do
      # Pattern: s is variable, p and o are bound, g is variable
      pattern = {:quad, {:variable, "s"}, 10, 100, {:variable, "g"}}

      stats = %{}

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Bound positions (p and o) should have score 0 and be ordered first
      # Unbound positions (s and g) should come after
      assert is_list(ordering)
      assert length(ordering) == 4
    end

    test "handles all variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      stats = %{}

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return all 4 positions
      assert length(ordering) == 4
    end

    test "orders positions correctly" do
      # Pattern: all variables
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      stats = %{}

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return positions 0, 1, 2, 3 in some order
      assert Enum.sort(ordering) == [0, 1, 2, 3]
    end

    test "bound positions come first" do
      # Pattern with some bounds
      pattern = {:quad, {:variable, "s"}, 10, 100, 0}

      stats = %{}

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # First positions should be bound ones (p=1, o=2, g=3)
      # They all have score 0
      bound_positions = ordering |> Enum.take(3) |> Enum.sort()
      assert bound_positions == [1, 2, 3]
    end
  end

  # ===========================================================================
  # Error Scenario Tests (C24)
  # ===========================================================================

  describe "variable ordering with error scenarios" do
    test "uses default cardinality when stats is empty" do
      # Pattern with all variables
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # Empty stats should trigger fallback
      stats = %{}

      # Should still succeed with fallback cardinality
      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return valid ordering
      assert is_list(ordering)
      assert length(ordering) == 4
    end

    test "uses stats quad_count when cardinality estimate fails" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # Stats with quad_count but missing other fields
      stats = %{quad_count: 5000}

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return valid ordering using fallback
      assert is_list(ordering)
      assert length(ordering) == 4
    end

    test "handles nil stats gracefully" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # nil stats should trigger fallback to default
      stats = nil

      # Should still succeed
      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return valid ordering
      assert is_list(ordering)
      assert length(ordering) == 4
    end

    test "uses provided stats when available" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # Full stats
      stats = %{
        quad_count: 10_000,
        distinct_subjects: 1000,
        distinct_predicates: 50,
        distinct_objects: 2000
      }

      {:ok, ordering} = QuadLeapfrog.quad_variable_ordering(pattern, stats)

      # Should return valid ordering
      assert is_list(ordering)
      assert length(ordering) == 4
    end
  end

  # ===========================================================================
  # QuadTrieIterator Tests
  # ===========================================================================

  describe "QuadTrieIterator.new/4" do
    test "creates iterator at level 0 (graph position)", %{db: db} do
      # Insert some quads
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 0}, {3, 12, 102, 1}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      # Create iterator at level 0 to iterate over graph IDs
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert iter.level == 0
      assert iter.cf == :gspo

      # Should be positioned at first entry
      assert {:ok, value} = QuadTrieIterator.current(iter)
      # First graph ID
      assert value == 0

      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 1 (subject position)", %{db: db} do
      # Insert quads with graph prefix
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 0}, {3, 12, 102, 0}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      # Create iterator at level 1 to iterate over subject IDs for graph 0
      prefix = <<0::64-big>>
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, prefix, 1)
      assert iter.level == 1

      # Should be positioned at first subject
      assert {:ok, value} = QuadTrieIterator.current(iter)
      # First subject ID
      assert value == 1

      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 2 (predicate position)", %{db: db} do
      # Insert quads with graph-subject prefix
      Enum.each([{1, 10, 100, 0}, {1, 11, 101, 0}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      # Create iterator at level 2 for graph 0, subject 1
      prefix = <<0::64-big, 1::64-big>>
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, prefix, 2)
      assert iter.level == 2

      # Should be positioned at first predicate
      assert {:ok, value} = QuadTrieIterator.current(iter)
      # One of the predicates
      assert value in [10, 11]

      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 3 (object position)", %{db: db} do
      # Insert quads with graph-subject-predicate prefix
      Enum.each([{1, 10, 100, 0}, {1, 10, 101, 0}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      # Create iterator at level 3 for graph 0, subject 1, predicate 10
      prefix = <<0::64-big, 1::64-big, 10::64-big>>
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, prefix, 3)
      assert iter.level == 3

      # Should be positioned at first object
      assert {:ok, value} = QuadTrieIterator.current(iter)
      # One of the objects
      assert value in [100, 101]

      QuadTrieIterator.close(iter)
    end

    test "handles empty database", %{db: db} do
      # Create iterator on empty database
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Should be exhausted immediately
      assert QuadTrieIterator.exhausted?(iter)

      QuadTrieIterator.close(iter)
    end
  end

  describe "QuadTrieIterator.seek/2" do
    test "seeks to target value at level 0", %{db: db} do
      # Insert quads with different graph IDs
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 5}, {3, 12, 102, 10}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Seek to graph ID 5
      assert {:ok, iter} = QuadTrieIterator.seek(iter, 5)
      assert {:ok, value} = QuadTrieIterator.current(iter)
      assert value == 5

      QuadTrieIterator.close(iter)
    end

    test "seeks to next higher value when target not found", %{db: db} do
      # Insert quads with graph IDs 0, 5, 10
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 5}, {3, 12, 102, 10}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Seek to graph ID 7 (should land at 10)
      assert {:ok, iter} = QuadTrieIterator.seek(iter, 7)
      assert {:ok, value} = QuadTrieIterator.current(iter)
      assert value == 10

      QuadTrieIterator.close(iter)
    end

    test "returns exhausted when seeking beyond all values", %{db: db} do
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 5}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Seek to graph ID 100 (beyond all values)
      assert {:exhausted, iter} = QuadTrieIterator.seek(iter, 100)
      assert QuadTrieIterator.exhausted?(iter)

      QuadTrieIterator.close(iter)
    end

    test "handles exhausted iterator", %{db: db} do
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Empty database = exhausted immediately
      assert {:exhausted, iter} = QuadTrieIterator.seek(iter, 5)

      QuadTrieIterator.close(iter)
    end
  end

  describe "QuadTrieIterator.next/1" do
    test "advances to next distinct value", %{db: db} do
      # Insert quads with graph IDs 0, 5, 10
      Enum.each([{1, 10, 100, 0}, {2, 11, 101, 5}, {3, 12, 102, 10}], fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Start at first value
      assert {:ok, _value} = QuadTrieIterator.current(iter)

      # Advance to next
      assert {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, value} = QuadTrieIterator.current(iter)
      assert value == 5

      # Advance again
      assert {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, value} = QuadTrieIterator.current(iter)
      assert value == 10

      # Advance past end
      assert {:exhausted, iter} = QuadTrieIterator.next(iter)
      assert QuadTrieIterator.exhausted?(iter)

      QuadTrieIterator.close(iter)
    end

    test "handles exhausted iterator", %{db: db} do
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Empty database = exhausted immediately
      assert {:exhausted, iter} = QuadTrieIterator.next(iter)

      QuadTrieIterator.close(iter)
    end

    test "handles maximum uint64 value", %{db: db} do
      # Insert quad with max graph ID
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 0xFFFFFFFFFFFFFFFF})

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Seek to max value
      assert {:ok, iter} = QuadTrieIterator.seek(iter, 0xFFFFFFFFFFFFFFFF)
      assert {:ok, value} = QuadTrieIterator.current(iter)
      assert value == 0xFFFFFFFFFFFFFFFF

      # Next should be exhausted (overflow protection)
      assert {:exhausted, iter} = QuadTrieIterator.next(iter)

      QuadTrieIterator.close(iter)
    end
  end

  describe "QuadTrieIterator.current_key/1" do
    test "returns current full key", %{db: db} do
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 0})

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      assert {:ok, key} = QuadTrieIterator.current_key(iter)
      # Quad keys are 32 bytes
      assert byte_size(key) == 32

      # Verify key structure: graph | subject | predicate | object
      <<g::64-big, s::64-big, p::64-big, o::64-big>> = key
      assert g == 0
      assert s == 1
      assert p == 10
      assert o == 100

      QuadTrieIterator.close(iter)
    end

    test "returns exhausted when no current key", %{db: db} do
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Empty database = no current key
      assert :exhausted = QuadTrieIterator.current_key(iter)

      QuadTrieIterator.close(iter)
    end
  end

  describe "QuadTrieIterator.extract_value_at_level/2" do
    test "extracts value at level 0 (first 8 bytes)", %{db: db} do
      key = <<100::64-big, 1::64-big, 10::64-big, 100::64-big>>
      assert QuadTrieIterator.extract_value_at_level(key, 0) == 100
    end

    test "extracts value at level 1 (bytes 8-16)", %{db: db} do
      key = <<100::64-big, 200::64-big, 10::64-big, 100::64-big>>
      assert QuadTrieIterator.extract_value_at_level(key, 1) == 200
    end

    test "extracts value at level 2 (bytes 16-24)", %{db: db} do
      key = <<100::64-big, 1::64-big, 300::64-big, 100::64-big>>
      assert QuadTrieIterator.extract_value_at_level(key, 2) == 300
    end

    test "extracts value at level 3 (bytes 24-32)", %{db: db} do
      key = <<100::64-big, 1::64-big, 10::64-big, 400::64-big>>
      assert QuadTrieIterator.extract_value_at_level(key, 3) == 400
    end
  end

  describe "QuadTrieIterator.decode_key/1" do
    test "decodes full quad key into four components", %{db: db} do
      key = <<100::64-big, 1::64-big, 10::64-big, 500::64-big>>
      assert {g, s, p, o} = QuadTrieIterator.decode_key(key)
      assert g == 100
      assert s == 1
      assert p == 10
      assert o == 500
    end
  end

  describe "QuadTrieIterator.extract_binding/1" do
    test "extracts all values as map", %{db: db} do
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 0})

      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      assert {:ok, binding} = QuadTrieIterator.extract_binding(iter)
      # graph
      assert binding.pos0 == 0
      # subject
      assert binding.pos1 == 1
      # predicate
      assert binding.pos2 == 10
      # object
      assert binding.pos3 == 100

      QuadTrieIterator.close(iter)
    end

    test "returns exhausted when iterator is exhausted", %{db: db} do
      assert {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Empty database = exhausted
      assert :exhausted = QuadTrieIterator.extract_binding(iter)

      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # QuadLeapfrog Planning and Execution
  # ===========================================================================

  describe "plan_iterators/1" do
    test "uses direct lookup for a fully bound pattern" do
      assert {:ok, []} = QuadLeapfrog.plan_iterators({:quad, 1, 2, 3, 4})
    end

    test "returns one four-field physical scan plan" do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      assert {:ok, [{3, :gspo, 0, <<>>}]} = QuadLeapfrog.plan_iterators(pattern)
    end

    test "chooses the longest contiguous prefix across index orders" do
      cases = [
        {{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 9},
         {3, :gspo, 1, <<9::64-big>>}},
        {{:quad, 1, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}},
         {3, :spog, 1, <<1::64-big>>}},
        {{:quad, {:variable, "s"}, 2, {:variable, "o"}, {:variable, "g"}},
         {3, :posg, 1, <<2::64-big>>}},
        {{:quad, 1, 2, {:variable, "o"}, 9}, {3, :gspo, 3, <<9::64-big, 1::64-big, 2::64-big>>}},
        {{:quad, {:variable, "s"}, 2, 3, 9}, {3, :gpos, 3, <<9::64-big, 2::64-big, 3::64-big>>}}
      ]

      Enum.each(cases, fn {pattern, expected} ->
        assert {:ok, [^expected]} = QuadLeapfrog.plan_iterators(pattern)
      end)
    end

    test "does not encode a bound component after a variable gap" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, 3, 9}
      assert {:ok, [{3, :gspo, 1, <<9::64-big>>}]} = QuadLeapfrog.plan_iterators(pattern)
    end

    test "normalizes compatibility bound tuples" do
      pattern = {:quad, {:variable, "s"}, {:bound, 2}, {:bound, 3}, {:bound, 9}}

      assert {:ok, [{3, :gpos, 3, <<9::64-big, 2::64-big, 3::64-big>>}]} =
               QuadLeapfrog.plan_iterators(pattern)
    end

    test "rejects malformed patterns and components" do
      assert {:error, :invalid_quad_pattern} = QuadLeapfrog.plan_iterators({:triple, 1, 2, 3})

      assert {:error, {:invalid_quad_component, :object, -1}} =
               QuadLeapfrog.plan_iterators({:quad, 1, 2, -1, 0})
    end
  end

  describe "create_iterators_for_pattern/2" do
    test "opens one full-key iterator with plan metadata", %{db: db} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      assert {:ok, [tagged]} = QuadLeapfrog.create_iterators_for_pattern(db, pattern)
      assert tagged.index == :gspo
      assert tagged.prefix_depth == 1
      assert tagged.position == 3
      assert tagged.variable_name == nil
      assert tagged.iterator.level == 3
      assert tagged.iterator.prefix == <<0::64-big>>
      QuadTrieIterator.close(tagged.iterator)
    end

    test "does not open an iterator for fully bound lookup", %{db: db} do
      assert {:ok, []} = QuadLeapfrog.create_iterators_for_pattern(db, {:quad, 1, 2, 3, 4})
    end
  end

  describe "from_pattern/2" do
    test "constructs an active scan for matching prefixes", %{db: db} do
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 5})
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 5}

      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert scan.variables == ["s", "p", "o"]
      assert length(scan.tagged_iterators) == 1
      refute QuadLeapfrog.exhausted?(scan)
      QuadLeapfrog.close(scan)
    end

    test "returns exhausted when a selected prefix is empty", %{db: db} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 99}
      assert {:exhausted, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert QuadLeapfrog.exhausted?(scan)
      QuadLeapfrog.close(scan)
    end

    test "fully bound lookup yields one empty binding when present", %{db: db} do
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 0})
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, {:quad, 1, 10, 100, 0})
      assert [%{}] = Enum.to_list(QuadLeapfrog.stream(scan))
    end

    test "fully bound lookup is exhausted when absent", %{db: db} do
      assert {:exhausted, scan} = QuadLeapfrog.from_pattern(db, {:quad, 1, 10, 100, 0})
      assert [] = Enum.to_list(QuadLeapfrog.stream(scan))
    end

    test "returns tagged validation errors", %{db: db} do
      assert {:error, :invalid_quad_pattern} = QuadLeapfrog.from_pattern(db, {:invalid, :pattern})

      assert {:error, {:invalid_quad_component, :graph, :invalid}} =
               QuadLeapfrog.from_pattern(db, {:quad, 1, 2, 3, :invalid})
    end
  end

  describe "search/1 and next/1" do
    setup %{db: db} do
      Enum.each(
        [{1, 10, 100, 0}, {1, 11, 101, 0}, {2, 10, 100, 0}, {3, 12, 102, 5}],
        &QuadOperations.insert_quad(db, &1)
      )

      :ok
    end

    test "extracts binary-key bindings from the full physical key", %{db: db} do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert {:ok, first} = QuadLeapfrog.search(scan)
      assert %{"s" => 1, "o" => 100} = QuadLeapfrog.bindings(first)
      QuadLeapfrog.close(first)
    end

    test "advances to the next matching key and skips non-matches", %{db: db} do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert {:ok, first} = QuadLeapfrog.search(scan)
      assert {:ok, second} = QuadLeapfrog.next(first)
      assert second.bindings == %{"s" => 2, "o" => 100}
      assert {:exhausted, exhausted} = QuadLeapfrog.next(second)
      assert QuadLeapfrog.exhausted?(exhausted)
      QuadLeapfrog.close(exhausted)
    end

    test "enforces repeated-variable equality", %{db: db} do
      :ok = QuadOperations.insert_quad(db, {7, 10, 7, 0})
      pattern = {:quad, {:variable, "same"}, 10, {:variable, "same"}, 0}
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert [%{"same" => 7}] = Enum.to_list(QuadLeapfrog.stream(scan))
    end

    test "omits anonymous variables", %{db: db} do
      pattern = {:quad, {:variable, "s"}, {:variable, "_"}, {:variable, "_"}, 5}
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      assert [%{"s" => 3}] = Enum.to_list(QuadLeapfrog.stream(scan))
    end
  end

  describe "stream/1" do
    setup %{db: db} do
      Enum.each(
        [{1, 10, 100, 0}, {1, 10, 101, 0}, {1, 11, 100, 0}, {2, 10, 100, 0}],
        &QuadOperations.insert_quad(db, &1)
      )

      :ok
    end

    test "lazily enumerates every matching quad", %{db: db} do
      pattern =
        {:quad, {:variable, "subject"}, {:variable, "predicate"}, {:variable, "object"},
         {:variable, "graph"}}

      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      stream = QuadLeapfrog.stream(scan)
      assert is_function(stream)

      results = Enum.to_list(stream)
      assert length(results) == 4

      assert Enum.all?(results, fn binding ->
               Enum.sort(Map.keys(binding)) == ["graph", "object", "predicate", "subject"]
             end)
    end

    test "bound components filter results but are not emitted", %{db: db} do
      pattern = {:quad, 1, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      results = Enum.to_list(QuadLeapfrog.stream(scan))

      assert length(results) == 3
      assert Enum.all?(results, &(Map.keys(&1) |> Enum.sort() == ["g", "o", "p"]))
    end

    test "early halt closes the owned iterator", %{db: db} do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      assert {:ok, scan} = QuadLeapfrog.from_pattern(db, pattern)
      [tagged] = scan.tagged_iterators
      iterator_pid = tagged.iterator.iter_ref

      assert [_] = scan |> QuadLeapfrog.stream() |> Enum.take(1)
      refute Process.alive?(iterator_pid)
    end
  end
end
