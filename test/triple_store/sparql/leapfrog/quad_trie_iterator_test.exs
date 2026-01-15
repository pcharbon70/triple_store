defmodule TripleStore.SPARQL.Leapfrog.QuadTrieIteratorTest do
  @moduledoc """
  Unit tests for QuadTrieIterator (Section 5.5.1).

  Tests the 32-byte quad trie iterator for Leapfrog joins with quad patterns.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Leapfrog.QuadTrieIterator

  @moduletag :integration

  @test_db_base "/tmp/quad_trie_iterator_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path, schema: :quad)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, path: test_path}
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp insert_quad(db, s, p, o, g), do: QuadOperations.insert_quad(db, {s, p, o, g})

  defp encode_quad_key(g, s, p, o), do: <<g::64-big, s::64-big, p::64-big, o::64-big>>

  # ===========================================================================
  # Basic Iterator Creation Tests (5.5.1.1)
  # ===========================================================================

  describe "new/4" do
    test "creates iterator on empty database", %{db: db} do
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert QuadTrieIterator.exhausted?(iter)
      assert QuadTrieIterator.current(iter) == :exhausted
      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 0 (graphs) with data", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 2, 20, 200, 1)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      refute QuadTrieIterator.exhausted?(iter)
      assert {:ok, 0} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 1 (subjects) with graph prefix", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 2, 20, 200, 0)

      # Iterate over subjects for graph=0
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<0::64-big>>, 1)
      refute QuadTrieIterator.exhausted?(iter)
      assert {:ok, 1} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 2 (predicates) with graph-subject prefix", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 1, 20, 200, 0)

      # Iterate over predicates for graph=0, subject=1
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<0::64-big, 1::64-big>>, 2)
      refute QuadTrieIterator.exhausted?(iter)
      assert {:ok, 10} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "creates iterator at level 3 (objects) with graph-subject-predicate prefix", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 1, 10, 101, 0)

      # Iterate over objects for graph=0, subject=1, predicate=10
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<0::64-big, 1::64-big, 10::64-big>>, 3)
      refute QuadTrieIterator.exhausted?(iter)
      assert {:ok, 100} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "returns exhausted when prefix has no matches", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)

      # Graph 999 doesn't exist
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<999::64-big>>, 1)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # All Four Indices Support Tests (5.5.1.2)
  # ===========================================================================

  describe "all four quad indices" do
    test "works with GSPO index", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert {:ok, 0} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "works with GPOS index", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)

      # GPOS: Graph | Predicate | Object | Subject
      {:ok, iter} = QuadTrieIterator.new(db, :gpos, <<>>, 0)
      assert {:ok, 0} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "works with SPOG index", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)

      # SPOG: Subject | Predicate | Object | Graph
      {:ok, iter} = QuadTrieIterator.new(db, :spog, <<>>, 0)
      assert {:ok, 1} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "works with POSG index", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)

      # POSG: Predicate | Object | Subject | Graph
      {:ok, iter} = QuadTrieIterator.new(db, :posg, <<>>, 0)
      assert {:ok, 10} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Seek Tests (5.5.1.3)
  # ===========================================================================

  describe "seek/2" do
    test "seeks to exact value", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 5, 50, 500, 0)
      insert_quad(db, 10, 100, 1000, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      {:ok, iter} = QuadTrieIterator.seek(iter, 5)
      assert {:ok, 5} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "seeks to next value when exact not present", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 10, 100, 1000, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      # Seek to 5, should land on 10
      {:ok, iter} = QuadTrieIterator.seek(iter, 5)
      assert {:ok, 10} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "returns exhausted when seek past all values", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 5, 50, 500, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      {:exhausted, iter} = QuadTrieIterator.seek(iter, 100)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end

    test "seek on exhausted iterator returns exhausted", %{db: db} do
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert QuadTrieIterator.exhausted?(iter)
      {:exhausted, iter} = QuadTrieIterator.seek(iter, 5)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end

    test "seeks within prefix boundary", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 1, 20, 200, 0)
      insert_quad(db, 2, 10, 100, 0)

      # Seek subjects for graph=0
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<0::64-big>>, 1)
      {:ok, iter} = QuadTrieIterator.seek(iter, 2)
      assert {:ok, 2} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Next Tests (5.5.1.4)
  # ===========================================================================

  describe "next/1" do
    test "advances to next distinct value", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 2, 20, 200, 0)
      insert_quad(db, 3, 30, 300, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, 1} = QuadTrieIterator.current(iter)

      {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, 2} = QuadTrieIterator.current(iter)

      {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, 3} = QuadTrieIterator.current(iter)

      {:exhausted, iter} = QuadTrieIterator.next(iter)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end

    test "skips duplicate values at level", %{db: db} do
      # Subject 1 has multiple predicates - next should skip all of them
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 1, 20, 200, 0)
      insert_quad(db, 1, 30, 300, 0)
      insert_quad(db, 2, 40, 400, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, 1} = QuadTrieIterator.current(iter)

      # Next should skip all subject=1 entries and go to subject=2
      {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, 2} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "next on exhausted iterator returns exhausted", %{db: db} do
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      {:exhausted, iter} = QuadTrieIterator.next(iter)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Binding Extraction Tests (5.5.1.5)
  # ===========================================================================

  describe "binding extraction" do
    test "extract_value_at_level extracts from quad key" do
      key = encode_quad_key(1, 2, 3, 4)

      assert QuadTrieIterator.extract_value_at_level(key, 0) == 1
      assert QuadTrieIterator.extract_value_at_level(key, 1) == 2
      assert QuadTrieIterator.extract_value_at_level(key, 2) == 3
      assert QuadTrieIterator.extract_value_at_level(key, 3) == 4
    end

    test "decode_key decodes 32-byte key into four values" do
      key = encode_quad_key(10, 20, 30, 40)
      assert {10, 20, 30, 40} = QuadTrieIterator.decode_key(key)
    end

    test "extract_binding returns map of all four values", %{db: db} do
      insert_quad(db, 1, 2, 3, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      {:ok, binding} = QuadTrieIterator.extract_binding(iter)

      assert binding.pos0 == 0
      assert binding.pos1 == 1
      assert binding.pos2 == 2
      assert binding.pos3 == 3

      QuadTrieIterator.close(iter)
    end

    test "extract_binding returns exhausted when iterator is exhausted", %{db: db} do
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert QuadTrieIterator.extract_binding(iter) == :exhausted
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Current and Current Key Tests
  # ===========================================================================

  describe "current/1 and current_key/1" do
    test "current returns value at configured level", %{db: db} do
      insert_quad(db, 100, 200, 300, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, 100} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "current_key returns full 32-byte key", %{db: db} do
      insert_quad(db, 100, 200, 300, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, key} = QuadTrieIterator.current_key(iter)
      assert byte_size(key) == 32
      assert {0, 100, 200, 300} = QuadTrieIterator.decode_key(key)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Close Tests
  # ===========================================================================

  describe "close/1" do
    test "closes iterator and returns :ok", %{db: db} do
      insert_quad(db, 1, 2, 3, 0)
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)
      assert :ok = QuadTrieIterator.close(iter)
    end

    test "closing nil iter_ref returns :ok", %{db: db} do
      iter = %QuadTrieIterator{db: db, cf: :gspo, prefix: <<>>, level: 0, iter_ref: nil}
      assert :ok = QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Integration Tests - Multi-Graph Scenarios
  # ===========================================================================

  describe "multi-graph scenarios" do
    test "iterates across multiple graphs", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 2, 20, 200, 1)
      insert_quad(db, 3, 30, 300, 2)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 0)

      # Collect all graphs
      graphs =
        Stream.unfold(iter, fn iter ->
          case QuadTrieIterator.current(iter) do
            {:ok, value} ->
              case QuadTrieIterator.next(iter) do
                {:ok, next_iter} -> {value, next_iter}
                {:exhausted, next_iter} -> {value, next_iter}
              end

            :exhausted ->
              nil
          end
        end)
        |> Enum.to_list()

      assert graphs == [0, 1, 2]
      QuadTrieIterator.close(iter)
    end

    test "graph-scoped iteration with prefix", %{db: db} do
      insert_quad(db, 1, 10, 100, 0)
      insert_quad(db, 2, 20, 200, 0)
      insert_quad(db, 3, 30, 300, 1)

      # Iterate only within graph 0
      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<0::64-big>>, 1)

      subjects =
        Stream.unfold(iter, fn iter ->
          case QuadTrieIterator.current(iter) do
            {:ok, value} ->
              case QuadTrieIterator.next(iter) do
                {:ok, next_iter} -> {value, next_iter}
                {:exhausted, next_iter} -> {value, next_iter}
              end

            :exhausted ->
              nil
          end
        end)
        |> Enum.to_list()

      # Should only have subjects from graph 0
      assert subjects == [1, 2]
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles single quad entry", %{db: db} do
      insert_quad(db, 42, 43, 44, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, 42} = QuadTrieIterator.current(iter)
      {:exhausted, iter} = QuadTrieIterator.next(iter)
      assert QuadTrieIterator.exhausted?(iter)
      QuadTrieIterator.close(iter)
    end

    test "handles consecutive IDs", %{db: db} do
      for i <- 1..10, do: insert_quad(db, i, i + 10, i + 100, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)

      values =
        Stream.unfold(iter, fn iter ->
          case QuadTrieIterator.current(iter) do
            {:ok, value} ->
              case QuadTrieIterator.next(iter) do
                {:ok, next_iter} -> {value, next_iter}
                {:exhausted, next_iter} -> {value, next_iter}
              end

            :exhausted ->
              nil
          end
        end)
        |> Enum.to_list()

      assert values == Enum.to_list(1..10)
      QuadTrieIterator.close(iter)
    end

    test "seek to 0", %{db: db} do
      insert_quad(db, 0, 1, 2, 0)
      insert_quad(db, 5, 6, 7, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      {:ok, iter} = QuadTrieIterator.seek(iter, 0)
      assert {:ok, 0} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end

    test "large gap between IDs", %{db: db} do
      insert_quad(db, 1, 1, 1, 0)
      insert_quad(db, 1_000_000, 1, 1, 0)

      {:ok, iter} = QuadTrieIterator.new(db, :gspo, <<>>, 1)
      assert {:ok, 1} = QuadTrieIterator.current(iter)

      {:ok, iter} = QuadTrieIterator.next(iter)
      assert {:ok, 1_000_000} = QuadTrieIterator.current(iter)
      QuadTrieIterator.close(iter)
    end
  end

  # ===========================================================================
  # Security Tests - Overflow Protection
  # ===========================================================================

  describe "integer overflow protection" do
    test "next returns exhausted at max value" do
      # Create a fake iterator struct at max value
      max_uint64 = 0xFFFFFFFFFFFFFFFF

      iter = %QuadTrieIterator{
        db: nil,
        cf: :gspo,
        prefix: <<>>,
        level: 0,
        iter_ref: nil,
        current_key: <<max_uint64::64-big, 0::64-big, 0::64-big, 0::64-big>>,
        current_value: max_uint64,
        exhausted: false
      }

      # Should return exhausted instead of overflowing
      {:exhausted, result_iter} = QuadTrieIterator.next(iter)
      assert result_iter.exhausted == true
      assert result_iter.current_value == nil
    end

    test "next handles nil current_value gracefully" do
      # Create a fake iterator struct with nil current_value
      # This can happen in edge cases where iterator is created but not yet positioned
      iter = %QuadTrieIterator{
        db: nil,
        cf: :gspo,
        prefix: <<>>,
        level: 0,
        iter_ref: nil,
        current_key: nil,
        current_value: nil,
        exhausted: false
      }

      # Should return exhausted without crashing
      {:exhausted, result_iter} = QuadTrieIterator.next(iter)
      assert result_iter.exhausted == true
      assert result_iter.current_value == nil
      assert result_iter.current_key == nil
    end
  end
end
