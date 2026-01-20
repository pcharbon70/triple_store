defmodule TripleStore.Reasoner.PropertiesTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.GraphProvenance
  alias TripleStore.Reasoner.GraphHelpers

  # Number of test iterations for property tests
  @property_iterations 100

  # ============================================================================
# GraphProvenance Properties
# ============================================================================

  describe "GraphProvenance properties" do
    test "merge is associative" do
      Enum.each(1..@property_iterations, fn _ ->
        quads1 = generate_quads(Enum.random(0..5))
        quads2 = generate_quads(Enum.random(0..5))
        quads3 = generate_quads(Enum.random(0..5))

        tracker1 = build_tracker(quads1, [1])
        tracker2 = build_tracker(quads2, [2])
        tracker3 = build_tracker(quads3, [3])

        left_assoc =
          tracker1
          |> GraphProvenance.merge(tracker2)
          |> GraphProvenance.merge(tracker3)

        right_assoc =
          tracker2
          |> GraphProvenance.merge(tracker3)
          |> GraphProvenance.merge(tracker1)

        assert GraphProvenance.count(left_assoc) == GraphProvenance.count(right_assoc)
      end)
    end

    test "new() is identity for merge" do
      Enum.each(1..@property_iterations, fn _ ->
        quads = generate_quads(Enum.random(0..5))
        tracker = build_tracker(quads, [1])
        empty = GraphProvenance.new()

        merged_left = GraphProvenance.merge(tracker, empty)
        merged_right = GraphProvenance.merge(empty, tracker)

        assert GraphProvenance.count(merged_left) == GraphProvenance.count(tracker)
        assert GraphProvenance.count(merged_right) == GraphProvenance.count(tracker)
      end)
    end

    test "count returns actual number of tracked quads" do
      Enum.each(1..@property_iterations, fn _ ->
        quads = generate_quads(Enum.random(0..10))
        tracker = build_tracker(quads, [1, 2])

        assert GraphProvenance.count(tracker) == length(Enum.uniq(quads))
      end)
    end

    test "add_source and remove_quad maintain count" do
      Enum.each(1..@property_iterations, fn _ ->
        quad = {Enum.random(0..100), Enum.random(0..100), Enum.random(0..100), Enum.random(0..100)}
        tracker = GraphProvenance.new()

        initial_count = GraphProvenance.count(tracker)

        tracker = GraphProvenance.add_source(tracker, quad, [1, 2])
        assert GraphProvenance.count(tracker) == initial_count + 1

        tracker = GraphProvenance.remove_quad(tracker, quad)
        assert GraphProvenance.count(tracker) == initial_count
      end)
    end
  end

  # ============================================================================
# GraphHelpers Properties
  # ============================================================================

  describe "GraphHelpers properties" do
    test "graph_id/2 with default always returns valid result" do
      Enum.each(1..@property_iterations, fn _ ->
        default = Enum.random(0..1_000_000)
        result = GraphHelpers.graph_id([], default: default)

        assert {:ok, ^default} = result
      end)
    end

    test "graph_id!/1 returns valid ID when present" do
      Enum.each(1..@property_iterations, fn _ ->
        id = Enum.random(0..1_000_000)
        opts = [graph_id: id]
        result = GraphHelpers.graph_id!(opts)

        assert is_integer(result)
        assert result >= 0
      end)
    end

    test "graph_id/1 returns :error for negative IDs" do
      Enum.each(1..@property_iterations, fn _ ->
        negative_int = Enum.random(-1_000_000..-1)
        opts = [graph_id: negative_int]

        assert :error = GraphHelpers.graph_id(opts)
      end)
    end

    test "valid_graph_id? accepts non-negative integers" do
      Enum.each(1..@property_iterations, fn _ ->
        id = Enum.random(0..1_000_000)
        assert :ok = GraphHelpers.valid_graph_id?(id)
      end)
    end

    test "valid_graph_id? rejects negative integers" do
      Enum.each(1..@property_iterations, fn _ ->
        negative_int = Enum.random(-1_000_000..-1)
        assert :error = GraphHelpers.valid_graph_id?(negative_int)
      end)
    end

    test "is_graph_ref? matches valid_graph_id?" do
      Enum.each(1..@property_iterations, fn _ ->
        id = Enum.random(0..1_000_000)
        assert GraphHelpers.is_graph_ref?(id) == true

        negative_int = Enum.random(-1_000_000..-1)
        assert GraphHelpers.is_graph_ref?(negative_int) == false
      end)
    end

    test "tbox_graph_id returns :shared when no default specified" do
      Enum.each(1..@property_iterations, fn _ ->
        result = GraphHelpers.tbox_graph_id([], nil)
        assert {:ok, :shared} = result
      end)
    end
  end

  # ============================================================================
# Helper Generators and Functions
  # ============================================================================

  defp generate_quads(count) do
    for _ <- 1..count do
      {Enum.random(0..100), Enum.random(0..100), Enum.random(0..100), Enum.random(0..100)}
    end
  end

  defp build_tracker(quads, sources) do
    Enum.reduce(quads, GraphProvenance.new(), fn quad, acc ->
      GraphProvenance.add_source(acc, quad, sources)
    end)
  end
end
