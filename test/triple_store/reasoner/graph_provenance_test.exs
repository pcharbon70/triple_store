defmodule TripleStore.Reasoner.GraphProvenanceTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.GraphProvenance

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp quad(g, s, p, o), do: {g, s, p, o}

  # ============================================================================
  # Tests: new/0
  # ============================================================================

  describe "new/0" do
    test "creates an empty provenance tracker" do
      tracker = GraphProvenance.new()

      assert tracker.tracking == %{}
      assert tracker.count == 0
      assert GraphProvenance.empty?(tracker)
    end
  end

  # ============================================================================
  # Tests: add_source/3
  # ============================================================================

  describe "add_source/3" do
    test "adds source graphs for a quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1, 0])

      assert tracker.count == 1
      assert GraphProvenance.depends_on?(tracker, q, 1)
      assert GraphProvenance.depends_on?(tracker, q, 0)
    end

    test "merges sources for existing quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1])
      tracker = GraphProvenance.add_source(tracker, q, [0, 2])

      sources = GraphProvenance.get_sources(tracker, q)

      assert sources == MapSet.new([1, 0, 2])
    end

    test "handles multiple quads" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(2, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q1, [1, 0])
        |> GraphProvenance.add_source(q2, [2])

      assert tracker.count == 2
      assert GraphProvenance.depends_on?(tracker, q1, 0)
      assert GraphProvenance.depends_on?(tracker, q2, 2)
      refute GraphProvenance.depends_on?(tracker, q2, 0)
    end
  end

  # ============================================================================
  # Tests: depends_on?/3
  # ============================================================================

  describe "depends_on?/3" do
    test "returns true when quad depends on graph" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1, 0])

      assert GraphProvenance.depends_on?(tracker, q, 1)
      assert GraphProvenance.depends_on?(tracker, q, 0)
    end

    test "returns false when quad does not depend on graph" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1])

      refute GraphProvenance.depends_on?(tracker, q, 0)
      refute GraphProvenance.depends_on?(tracker, q, 2)
    end

    test "returns false for unknown quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      refute GraphProvenance.depends_on?(tracker, q, 1)
    end
  end

  # ============================================================================
  # Tests: get_sources/2
  # ============================================================================

  describe "get_sources/2" do
    test "returns source graphs for tracked quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1, 0, 2])

      sources = GraphProvenance.get_sources(tracker, q)

      assert sources == MapSet.new([1, 0, 2])
    end

    test "returns nil for untracked quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      assert GraphProvenance.get_sources(tracker, q) == nil
    end
  end

  # ============================================================================
  # Tests: remove_quad/2
  # ============================================================================

  describe "remove_quad/2" do
    test "removes tracked quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1, 0])
      assert tracker.count == 1

      tracker = GraphProvenance.remove_quad(tracker, q)

      assert tracker.count == 0
      assert GraphProvenance.get_sources(tracker, q) == nil
    end

    test "handles removing non-existent quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.remove_quad(tracker, q)

      assert tracker.count == 0
    end

    test "decrements count correctly" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(2, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q1, [1])
        |> GraphProvenance.add_source(q2, [2])

      assert tracker.count == 2

      tracker = GraphProvenance.remove_quad(tracker, q1)

      assert tracker.count == 1
    end
  end

  # ============================================================================
  # Tests: find_dependent_quads/2
  # ============================================================================

  describe "find_dependent_quads/2" do
    test "finds all quads depending on a graph" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(1, 100, 201, 301)
      q3 = quad(2, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q1, [1, 0])
        |> GraphProvenance.add_source(q2, [1, 0])
        |> GraphProvenance.add_source(q3, [2, 0])

      dependent_on_1 = GraphProvenance.find_dependent_quads(tracker, 1)

      assert length(dependent_on_1) == 2
      assert q1 in dependent_on_1
      assert q2 in dependent_on_1
      refute q3 in dependent_on_1
    end

    test "returns empty list when no quads depend on graph" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [2])

      dependent = GraphProvenance.find_dependent_quads(tracker, 1)

      assert dependent == []
    end

    test "handles empty tracker" do
      tracker = GraphProvenance.new()

      dependent = GraphProvenance.find_dependent_quads(tracker, 1)

      assert dependent == []
    end
  end

  # ============================================================================
  # Tests: merge/2
  # ============================================================================

  describe "merge/2" do
    test "merges two trackers" do
      tracker1 = GraphProvenance.new()
      tracker2 = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(2, 100, 200, 300)

      tracker1 = GraphProvenance.add_source(tracker1, q1, [1])
      tracker2 = GraphProvenance.add_source(tracker2, q2, [2])

      merged = GraphProvenance.merge(tracker1, tracker2)

      assert merged.count == 2
      assert GraphProvenance.depends_on?(merged, q1, 1)
      assert GraphProvenance.depends_on?(merged, q2, 2)
    end

    test "unions sources for same quad in both trackers" do
      tracker1 = GraphProvenance.new()
      tracker2 = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker1 = GraphProvenance.add_source(tracker1, q, [1])
      tracker2 = GraphProvenance.add_source(tracker2, q, [2])

      merged = GraphProvenance.merge(tracker1, tracker2)

      sources = GraphProvenance.get_sources(merged, q)

      assert sources == MapSet.new([1, 2])
    end
  end

  # ============================================================================
  # Tests: count/1
  # ============================================================================

  describe "count/1" do
    test "returns zero for new tracker" do
      tracker = GraphProvenance.new()

      assert GraphProvenance.count(tracker) == 0
    end

    test "returns number of tracked quads" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(2, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q1, [1])
        |> GraphProvenance.add_source(q2, [2])

      assert GraphProvenance.count(tracker) == 2
    end

    test "does not double count same quad" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q, [1])
        |> GraphProvenance.add_source(q, [2])

      assert GraphProvenance.count(tracker) == 1
    end
  end

  # ============================================================================
  # Tests: empty?/1
  # ============================================================================

  describe "empty?/1" do
    test "returns true for new tracker" do
      tracker = GraphProvenance.new()

      assert GraphProvenance.empty?(tracker)
    end

    test "returns false when quads are tracked" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1])

      refute GraphProvenance.empty?(tracker)
    end

    test "returns true after clearing" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1])
      refute GraphProvenance.empty?(tracker)

      tracker = GraphProvenance.clear(tracker)
      assert GraphProvenance.empty?(tracker)
    end
  end

  # ============================================================================
  # Tests: clear/1
  # ============================================================================

  describe "clear/1" do
    test "removes all tracked quads" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(2, 100, 200, 300)

      tracker =
        tracker
        |> GraphProvenance.add_source(q1, [1])
        |> GraphProvenance.add_source(q2, [2])

      assert tracker.count == 2

      tracker = GraphProvenance.clear(tracker)

      assert tracker.count == 0
      assert tracker.tracking == %{}
    end
  end

  # ============================================================================
  # Tests: detect_cross_graph_deps/2
  # ============================================================================

  describe "detect_cross_graph_deps/2" do
    test "detects quads with external dependencies" do
      tracker = GraphProvenance.new()
      q1 = quad(1, 100, 200, 300)
      q2 = quad(1, 101, 201, 301)

      tracker =
        tracker
        # Depends on TBox (0)
        |> GraphProvenance.add_source(q1, [1, 0])
        # Only depends on itself
        |> GraphProvenance.add_source(q2, [1])

      deps = GraphProvenance.detect_cross_graph_deps(tracker, 1)

      assert Map.has_key?(deps, q1)
      refute Map.has_key?(deps, q2)
      assert deps[q1] == [0]
    end

    test "returns empty map when no cross-graph dependencies" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1])

      deps = GraphProvenance.detect_cross_graph_deps(tracker, 1)

      assert deps == %{}
    end

    test "detects multiple external dependencies" do
      tracker = GraphProvenance.new()
      q = quad(1, 100, 200, 300)

      tracker = GraphProvenance.add_source(tracker, q, [1, 0, 2, 3])

      deps = GraphProvenance.detect_cross_graph_deps(tracker, 1)

      assert deps[q] == [0, 2, 3]
    end
  end
end
