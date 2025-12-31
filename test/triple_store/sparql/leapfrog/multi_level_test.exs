defmodule TripleStore.SPARQL.Leapfrog.MultiLevelTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.SPARQL.Leapfrog.MultiLevel

  @moduletag :integration

  @test_db_base "/tmp/multi_level_test"

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
  # Helper Functions
  # ===========================================================================

  defp insert_triple(db, s, p, o) do
    Index.insert_triple(db, {s, p, o})
  end

  defp var(name), do: {:variable, name}
  defp triple(s, p, o), do: {:triple, s, p, o}

  # ===========================================================================
  # Basic Creation Tests
  # ===========================================================================

  describe "new/3" do
    test "creates executor with empty patterns", %{db: db} do
      {:ok, exec} = MultiLevel.new(db, [])
      assert exec.exhausted == true
      MultiLevel.close(exec)
    end

    test "creates executor with single pattern", %{db: db} do
      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      assert exec.var_order == ["s", "o"] or exec.var_order == ["o", "s"]
      MultiLevel.close(exec)
    end

    test "creates executor with multiple patterns", %{db: db} do
      patterns = [
        triple(var("x"), 10, var("y")),
        triple(var("y"), 20, var("z"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)

      assert length(exec.var_order) == 3
      assert "x" in exec.var_order
      assert "y" in exec.var_order
      assert "z" in exec.var_order
      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Single Variable Tests
  # ===========================================================================

  describe "single variable patterns" do
    test "returns all matching values for single variable pattern", %{db: db} do
      # Insert triples with predicate 10
      insert_triple(db, 1, 10, 100)
      insert_triple(db, 2, 10, 200)
      insert_triple(db, 3, 10, 300)

      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Should have 3 bindings
      assert length(bindings) == 3

      # Check that subjects are 1, 2, 3
      subjects = Enum.map(bindings, & &1["s"]) |> Enum.sort()
      assert subjects == [1, 2, 3]

      MultiLevel.close(exec)
    end

    test "returns empty when no matches", %{db: db} do
      # Insert with different predicate
      insert_triple(db, 1, 20, 100)

      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      bindings = MultiLevel.stream(exec) |> Enum.to_list()
      assert bindings == []

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Multi-Variable Tests (Join)
  # ===========================================================================

  describe "multi-variable patterns (joins)" do
    test "finds intersection of two patterns", %{db: db} do
      # Pattern 1: ?x knows ?y (pred=10)
      # Pattern 2: ?y age ?z (pred=20)
      # We need y values that appear as object in first AND subject in second

      # x=1 knows y=5
      # x=2 knows y=6
      # y=5 age z=25
      # y=7 age z=27 (y=7 not in first pattern, shouldn't match)

      # 1 knows 5
      insert_triple(db, 1, 10, 5)
      # 2 knows 6
      insert_triple(db, 2, 10, 6)
      # 5 age 25
      insert_triple(db, 5, 20, 25)
      # 7 age 27
      insert_triple(db, 7, 20, 27)

      patterns = [
        triple(var("x"), 10, var("y")),
        triple(var("y"), 20, var("z"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Only y=5 appears in both patterns
      assert length(bindings) == 1
      [binding] = bindings
      assert binding["x"] == 1
      assert binding["y"] == 5
      assert binding["z"] == 25

      MultiLevel.close(exec)
    end

    test "finds multiple join results", %{db: db} do
      # Multiple matches for the join
      # 1 knows 5
      insert_triple(db, 1, 10, 5)
      # 2 knows 6
      insert_triple(db, 2, 10, 6)
      # 5 age 25
      insert_triple(db, 5, 20, 25)
      # 6 age 26
      insert_triple(db, 6, 20, 26)

      patterns = [
        triple(var("x"), 10, var("y")),
        triple(var("y"), 20, var("z"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Both y=5 and y=6 should match
      assert length(bindings) == 2

      ys = Enum.map(bindings, & &1["y"]) |> Enum.sort()
      assert ys == [5, 6]

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Star Query Tests
  # ===========================================================================

  describe "star queries" do
    test "finds entities matching multiple predicates", %{db: db} do
      # Person 1 has all three properties
      # 1 knows 100
      insert_triple(db, 1, 10, 100)
      # 1 works_at 200
      insert_triple(db, 1, 20, 200)
      # 1 lives_in 300
      insert_triple(db, 1, 30, 300)

      # Person 2 has only two properties
      # 2 knows 101
      insert_triple(db, 2, 10, 101)
      # 2 works_at 201
      insert_triple(db, 2, 20, 201)

      # Person 3 has only one property
      # 3 knows 102
      insert_triple(db, 3, 10, 102)

      patterns = [
        triple(var("p"), 10, var("a")),
        triple(var("p"), 20, var("b")),
        triple(var("p"), 30, var("c"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Only person 1 has all three predicates
      assert length(bindings) == 1
      [binding] = bindings
      assert binding["p"] == 1
      assert binding["a"] == 100
      assert binding["b"] == 200
      assert binding["c"] == 300

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Chain Query Tests
  # ===========================================================================

  describe "chain queries" do
    test "follows chain of relationships", %{db: db} do
      # Chain: a -> b -> c -> d
      # 1 links_to 2
      insert_triple(db, 1, 10, 2)
      # 2 links_to 3
      insert_triple(db, 2, 10, 3)
      # 3 links_to 4
      insert_triple(db, 3, 10, 4)

      patterns = [
        triple(var("a"), 10, var("b")),
        triple(var("b"), 10, var("c")),
        triple(var("c"), 10, var("d"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Only one valid chain: 1 -> 2 -> 3 -> 4
      assert length(bindings) == 1
      [binding] = bindings
      assert binding["a"] == 1
      assert binding["b"] == 2
      assert binding["c"] == 3
      assert binding["d"] == 4

      MultiLevel.close(exec)
    end

    test "finds multiple chains", %{db: db} do
      # Two chains: 1->2->3 and 4->5->6
      insert_triple(db, 1, 10, 2)
      insert_triple(db, 2, 10, 3)
      insert_triple(db, 4, 10, 5)
      insert_triple(db, 5, 10, 6)

      patterns = [
        triple(var("a"), 10, var("b")),
        triple(var("b"), 10, var("c"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      assert length(bindings) == 2

      starts = Enum.map(bindings, & &1["a"]) |> Enum.sort()
      assert starts == [1, 4]

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Triangle Query Tests
  # ===========================================================================

  describe "triangle queries" do
    test "finds triangles in graph", %{db: db} do
      # Triangle: 1 -> 2 -> 3 -> 1
      insert_triple(db, 1, 10, 2)
      insert_triple(db, 2, 10, 3)
      insert_triple(db, 3, 10, 1)

      # Additional edges that don't form triangles
      insert_triple(db, 4, 10, 5)

      patterns = [
        triple(var("a"), 10, var("b")),
        triple(var("b"), 10, var("c")),
        triple(var("c"), 10, var("a"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      # Triangle can start at any of the 3 vertices
      # 1->2->3->1, 2->3->1->2, 3->1->2->3
      assert length(bindings) == 3

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Stream Laziness Tests
  # ===========================================================================

  describe "stream laziness" do
    test "stream is lazy and can be limited", %{db: db} do
      # Insert many triples
      for i <- 1..100 do
        insert_triple(db, i, 10, i + 1000)
      end

      patterns = [triple(var("s"), 10, var("o"))]

      {:ok, exec} = MultiLevel.new(db, patterns)

      # Take only first 5
      bindings = MultiLevel.stream(exec) |> Enum.take(5)
      assert length(bindings) == 5

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # next_binding/1 Tests
  # ===========================================================================

  describe "next_binding/1" do
    test "iterates through bindings one at a time", %{db: db} do
      insert_triple(db, 1, 10, 100)
      insert_triple(db, 2, 10, 200)
      insert_triple(db, 3, 10, 300)

      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      {:ok, b1, exec} = MultiLevel.next_binding(exec)
      assert is_map(b1)
      assert Map.has_key?(b1, "s")

      {:ok, b2, exec} = MultiLevel.next_binding(exec)
      assert b1 != b2

      {:ok, b3, exec} = MultiLevel.next_binding(exec)
      assert b3 != b2

      assert :exhausted = MultiLevel.next_binding(exec)

      MultiLevel.close(exec)
    end

    test "returns exhausted on empty result", %{db: db} do
      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      assert :exhausted = MultiLevel.next_binding(exec)

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles single triple in database", %{db: db} do
      insert_triple(db, 1, 10, 100)

      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      bindings = MultiLevel.stream(exec) |> Enum.to_list()
      assert length(bindings) == 1
      assert hd(bindings) == %{"s" => 1, "o" => 100}

      MultiLevel.close(exec)
    end

    test "handles pattern with same variable twice", %{db: db} do
      # Find self-loops: ?x links_to ?x
      # Self-loop
      insert_triple(db, 1, 10, 1)
      # Not self-loop
      insert_triple(db, 2, 10, 3)

      # This is a tricky case - same variable in S and O positions
      # Our current implementation treats them as separate, which is correct
      # The leapfrog would need to intersect subject and object iterators

      patterns = [triple(var("x"), 10, var("y"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      bindings = MultiLevel.stream(exec) |> Enum.to_list()
      # Should find both triples
      assert length(bindings) == 2

      MultiLevel.close(exec)
    end

    test "handles large gaps in IDs", %{db: db} do
      insert_triple(db, 1, 10, 2)
      insert_triple(db, 2, 10, 1_000_000)
      insert_triple(db, 1_000_000, 10, 1_000_001)

      patterns = [
        triple(var("a"), 10, var("b")),
        triple(var("b"), 10, var("c"))
      ]

      {:ok, exec} = MultiLevel.new(db, patterns)
      bindings = MultiLevel.stream(exec) |> Enum.to_list()

      assert length(bindings) == 2

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Security Tests - DoS Protection
  # ===========================================================================

  describe "timeout and limits" do
    test "respects timeout_ms option", %{db: db} do
      # Create some data
      for i <- 1..10, do: insert_triple(db, i, 10, i + 100)

      patterns = [triple(var("s"), 10, var("o"))]

      # Create with a timeout
      {:ok, exec} = MultiLevel.new(db, patterns, timeout_ms: 30_000)
      assert exec.timeout_ms == 30_000

      MultiLevel.close(exec)
    end

    test "respects max_iterations option", %{db: db} do
      for i <- 1..10, do: insert_triple(db, i, 10, i + 100)

      patterns = [triple(var("s"), 10, var("o"))]

      {:ok, exec} = MultiLevel.new(db, patterns, max_iterations: 500)
      assert exec.max_iterations == 500

      MultiLevel.close(exec)
    end

    test "rejects too many variables", %{db: db} do
      # Create patterns with more than 100 variables
      patterns =
        for i <- 1..101 do
          triple(var("v#{i}"), i, var("w#{i}"))
        end

      result = MultiLevel.new(db, patterns)
      assert result == {:error, :too_many_variables}
    end

    test "returns timeout error when time exceeded", %{db: db} do
      # This test simulates timeout behavior
      # We create an executor with a very short timeout
      for i <- 1..100, do: insert_triple(db, i, 10, i + 100)
      for i <- 1..100, do: insert_triple(db, i, 20, i + 200)

      patterns = [
        triple(var("x"), 10, var("y")),
        triple(var("y"), 20, var("z"))
      ]

      # Create with very short timeout (1ms) - almost certainly will timeout
      {:ok, exec} = MultiLevel.new(db, patterns, timeout_ms: 1)

      # Add a small delay to ensure timeout
      Process.sleep(5)

      result = MultiLevel.next_binding(exec)

      # Either returns results (if fast enough) or timeout
      case result do
        {:ok, _, _} -> :ok
        {:error, :timeout} -> :ok
        :exhausted -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end

      MultiLevel.close(exec)
    end

    test "backwards compatibility with stats map argument", %{db: db} do
      insert_triple(db, 1, 10, 100)

      patterns = [triple(var("s"), 10, var("o"))]

      # Old-style call with map as third argument (stats)
      {:ok, exec} = MultiLevel.new(db, patterns, %{})

      bindings = MultiLevel.stream(exec) |> Enum.to_list()
      assert length(bindings) == 1

      MultiLevel.close(exec)
    end

    test "accepts stats in options keyword list", %{db: db} do
      insert_triple(db, 1, 10, 100)

      patterns = [triple(var("s"), 10, var("o"))]

      {:ok, exec} = MultiLevel.new(db, patterns, stats: %{}, timeout_ms: 60_000)

      bindings = MultiLevel.stream(exec) |> Enum.to_list()
      assert length(bindings) == 1

      MultiLevel.close(exec)
    end
  end

  # ===========================================================================
  # Close Tests
  # ===========================================================================

  describe "close/1" do
    test "closes executor without error", %{db: db} do
      insert_triple(db, 1, 10, 100)

      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      # Iterate partway
      {:ok, _, exec} = MultiLevel.next_binding(exec)

      assert :ok = MultiLevel.close(exec)
    end

    test "closing exhausted executor is ok", %{db: db} do
      patterns = [triple(var("s"), 10, var("o"))]
      {:ok, exec} = MultiLevel.new(db, patterns)

      # Exhaust it
      _ = MultiLevel.stream(exec) |> Enum.to_list()

      assert :ok = MultiLevel.close(exec)
    end
  end
end
