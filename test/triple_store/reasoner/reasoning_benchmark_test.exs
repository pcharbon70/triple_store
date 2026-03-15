defmodule TripleStore.Reasoner.ReasoningBenchmarkTest do
  @moduledoc """
  Performance benchmarks for Phase 7: Reasoning with Named Graphs.

  These benchmarks measure:
  1. GraphProvenance operations (merge, add_source, remove_quad)
  2. PatternMatcher performance for pattern matching
  3. SemiNaive materialization at various scales
  4. GraphScopedReasoner reasoning performance
  5. Cross-graph reasoning overhead

  Run with: mix test --include benchmark test/triple_store/reasoner/reasoning_benchmark_test.exs
  """

  use ExUnit.Case, async: false

  @moduletag :benchmark
  @moduletag timeout: 300_000

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.{GraphProvenance, PatternMatcher, Rule, SemiNaive}

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_reasoning_bench_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager}
  end

  # ===========================================================================
  # Benchmark 1: GraphProvenance Operations
  # ===========================================================================

  describe "GraphProvenance operations" do
    test "add_source scales linearly", %{db: db} do
      tracker = GraphProvenance.new()

      # Measure adding 10K sources
      {time_us, _tracker} =
        :timer.tc(fn ->
          Enum.reduce(1..10_000, tracker, fn i, acc ->
            quad = {rem(i, 1000), rem(i, 50), i, 0}
            GraphProvenance.add_source(acc, quad, [rem(i, 10)])
          end)
        end)

      IO.puts(
        "\n  [Benchmark] GraphProvenance.add_source 10K operations: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      IO.puts("  [Benchmark] Throughput: #{Float.round(10_000_000.0 / time_us, 2)} ops/sec")

      # Should complete in under 500ms
      assert time_us < 500_000
    end

    test "merge performance for large trackers", %{db: db} do
      # Create two trackers with 5K quads each
      tracker1 =
        Enum.reduce(1..5_000, GraphProvenance.new(), fn i, acc ->
          quad = {rem(i, 1000), rem(i, 50), i, 0}
          GraphProvenance.add_source(acc, quad, [1])
        end)

      tracker2 =
        Enum.reduce(1..5_000, GraphProvenance.new(), fn i, acc ->
          quad = {rem(i, 1000), rem(i, 50), i + 5000, 1}
          GraphProvenance.add_source(acc, quad, [2])
        end)

      # Measure merge time
      {time_us, _merged} = :timer.tc(fn -> GraphProvenance.merge(tracker1, tracker2) end)

      IO.puts(
        "\n  [Benchmark] GraphProvenance.merge 5K + 5K quads: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      # Should complete in under 200ms
      assert time_us < 200_000
    end

    test "remove_quad performance", %{db: db} do
      # Build a tracker with 1K quads
      tracker =
        Enum.reduce(1..1000, GraphProvenance.new(), fn i, acc ->
          quad = {rem(i, 100), rem(i, 20), i, 0}
          GraphProvenance.add_source(acc, quad, [1, 2])
        end)

      # Measure removal time for 100 quads
      quads_to_remove = for i <- 1..100, do: {i, rem(i, 20), i, 0}

      {time_us, _tracker} =
        :timer.tc(fn ->
          Enum.reduce(quads_to_remove, tracker, fn quad, acc ->
            GraphProvenance.remove_quad(acc, quad)
          end)
        end)

      IO.puts("\n  [Benchmark] GraphProvenance.remove_quad 100 operations: #{time_us}μs")

      # Should complete in under 50ms
      assert time_us < 50_000
    end

    test "count is O(1)", %{db: db} do
      # Build a tracker with many quads
      tracker =
        Enum.reduce(1..10_000, GraphProvenance.new(), fn i, acc ->
          quad = {rem(i, 1000), rem(i, 50), i, 0}
          GraphProvenance.add_source(acc, quad, [1])
        end)

      # Measure count time (should be very fast as it's just a map size lookup)
      times = for _ <- 1..1000, do: elem(:timer.tc(fn -> GraphProvenance.count(tracker) end), 0)

      avg_us = Enum.sum(times) / length(times)

      IO.puts("\n  [Benchmark] GraphProvenance.count average: #{Float.round(avg_us, 2)}μs")

      # Average should be under 10μs (it's just map_size)
      assert avg_us < 10
    end
  end

  # ===========================================================================
  # Benchmark 2: PatternMatcher Performance
  # ===========================================================================

  describe "PatternMatcher performance" do
    test "matches_term? is fast", %{db: db} do
      # Test various pattern types
      patterns = [
        {:var, "s"},
        {:const, 42},
        42
      ]

      terms = [1, 42, 100]

      # Measure 100K pattern matches
      {time_us, _} =
        :timer.tc(fn ->
          for _ <- 1..100_000, pattern <- patterns, term <- terms do
            PatternMatcher.matches_term?(term, pattern)
          end
        end)

      ops = 100_000 * length(patterns) * length(terms)
      IO.puts("\n  [Benchmark] PatternMatcher.matches_term? #{div(ops, 1000)}K ops: #{time_us}μs")
      IO.puts("  [Benchmark] Throughput: #{Float.round(ops * 1_000_000.0 / time_us, 2)} ops/sec")

      # Should complete in under 1 second
      assert time_us < 1_000_000
    end

    test "matches_triple? scales with pattern complexity", %{db: db} do
      triple = {1, 10, 100}

      patterns = [
        {:pattern, [{:var, "s"}, {:var, "p"}, {:var, "o"}]},
        {:pattern, [{:const, 1}, {:var, "p"}, {:var, "o"}]},
        {:pattern, [{:const, 1}, {:const, 10}, {:var, "o"}]},
        {:pattern, [{:const, 1}, {:const, 10}, {:const, 100}]}
      ]

      times =
        Enum.map(patterns, fn pattern ->
          # Measure 10K matches per pattern
          {time_us, _} =
            :timer.tc(fn ->
              for _ <- 1..10_000, do: PatternMatcher.matches_triple?(triple, pattern)
            end)

          time_us
        end)

      IO.puts("\n  [Benchmark] PatternMatcher.matches_triple? per pattern complexity:")

      Enum.zip([:all_vars, :one_bound, :two_bound, :all_bound], times)
      |> Enum.each(fn {complexity, time_us} ->
        IO.puts("    #{complexity}: #{time_us}μs")
      end)

      # Even the most complex pattern should be fast
      max_time = Enum.max(times)
      assert max_time < 100_000, "Pattern matching took #{max_time}μs, expected < 100ms"
    end

    test "substitute_if_bound performance", %{db: db} do
      bindings = %{"s" => {:bound, 1}, "p" => {:bound, 10}}
      pattern = {:var, "s"}

      # Measure 100K substitutions
      {time_us, _} =
        :timer.tc(fn ->
          for _ <- 1..100_000, do: PatternMatcher.substitute_if_bound(pattern, bindings)
        end)

      IO.puts("\n  [Benchmark] PatternMatcher.substitute_if_bound 100K ops: #{time_us}μs")
      IO.puts("  [Benchmark] Throughput: #{Float.round(100_000_000.0 / time_us, 2)} ops/sec")

      # Should be very fast (< 100ms for 100K ops)
      assert time_us < 100_000
    end
  end

  # ===========================================================================
  # Benchmark 3: Rule Operations
  # ===========================================================================

  describe "Rule operations" do
    test "could_derive? performance", %{db: db} do
      # Create a sample rule
      # Rule.new/3: (name, body, head) where body is a list of patterns
      rule =
        Rule.new(
          :test_rule,
          [
            {:pattern, [{:var, "s"}, {:const, 10}, {:var, "o"}]},
            {:pattern, [{:var, "s"}, {:const, 15}, {:var, "o"}]}
          ],
          {:pattern, [{:var, "s"}, {:const, 20}, {:var, "o"}]}
        )

      triples = [
        {1, 10, 100},
        {1, 20, 100},
        {5, 20, 50},
        {1, 30, 100}
      ]

      # Measure 100K could_derive? checks
      {time_us, _} =
        :timer.tc(fn ->
          for _ <- 1..25_000, triple <- triples do
            Rule.could_derive?(rule, triple)
          end
        end)

      IO.puts("\n  [Benchmark] Rule.could_derive? 100K checks: #{time_us}μs")
      IO.puts("  [Benchmark] Throughput: #{Float.round(100_000_000.0 / time_us, 2)} checks/sec")

      # Should complete in under 1 second
      assert time_us < 1_000_000
    end
  end

  # ===========================================================================
  # Benchmark 4: SemiNaive Materialization
  # ===========================================================================

  describe "SemiNaive materialization" do
    test "materialization with simple transitive rule", %{db: db} do
      # Insert a simple chain: 1 -> 2 -> 3 -> ... -> 100
      quads =
        for i <- 1..99 do
          {i, 1, i + 1, 0}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Create transitive rule: ?s rel ?o, ?o rel ?t -> ?s rel ?t
      rule =
        Rule.new(
          :transitive,
          [
            {:pattern, [{:var, "s"}, 1, {:var, "o"}]},
            {:pattern, [{:var, "o"}, 1, {:var, "t"}]}
          ],
          {:pattern, [{:var, "s"}, 2, {:var, "t"}]}
        )

      # Create lookup function
      lookup_fn = fn
        {:pattern, [{:var, _}, {:var, _}, {:var, _}]} ->
          {:ok, MapSet.new(Enum.map(quads, fn {g, s, p, o} -> {s, p, o} end))}

        {:pattern, [{:const, s}, {:var, _}, {:var, _}]} ->
          matches =
            quads
            |> Enum.filter(fn {_, gs, _, _} -> gs == s end)
            |> Enum.map(fn {_, s, p, o} -> {s, p, o} end)
            |> MapSet.new()

          {:ok, matches}

        _ ->
          {:ok, MapSet.new()}
      end

      store_fn = fn _derived -> :ok end

      # Measure materialization time
      {time_us, {:ok, stats}} =
        :timer.tc(fn ->
          SemiNaive.materialize(lookup_fn, store_fn, [rule], MapSet.new(),
            parallel: false,
            max_iterations: 100
          )
        end)

      IO.puts(
        "\n  [Benchmark] SemiNaive materialization (99 facts, 1 rule): #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      IO.puts("  [Benchmark] Iterations: #{stats.iterations}")
      IO.puts("  [Benchmark] Derived: #{stats.total_derived}")

      # Should complete in under 1 second
      assert time_us < 1_000_000
    end

    test "materialization with multiple rules", %{db: db} do
      # Create more complex scenario with multiple rules
      quads =
        for i <- 1..50 do
          [{i, 1, i + 1, 0}, {i, 3, i * 2, 0}]
        end
        |> List.flatten()

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      rules = [
        Rule.new(
          :rule1,
          [
            {:pattern, [{:var, "s"}, 1, {:var, "o"}]},
            {:pattern, [{:var, "o"}, 1, {:var, "t"}]}
          ],
          {:pattern, [{:var, "s"}, 2, {:var, "t"}]}
        ),
        Rule.new(
          :rule2,
          [
            {:pattern, [{:var, "s"}, 2, {:var, "o"}]},
            {:pattern, [{:var, "o"}, 2, {:var, "t"}]}
          ],
          {:pattern, [{:var, "s"}, 4, {:var, "t"}]}
        )
      ]

      all_facts = MapSet.new(Enum.map(quads, fn {_, s, p, o} -> {s, p, o} end))

      lookup_fn = fn
        {:pattern, [{:var, _}, {:var, _}, {:var, _}]} -> {:ok, all_facts}
        _ -> {:ok, MapSet.new()}
      end

      store_fn = fn _derived -> :ok end

      # Measure materialization time
      {time_us, {:ok, stats}} =
        :timer.tc(fn ->
          SemiNaive.materialize(lookup_fn, store_fn, rules, all_facts,
            parallel: false,
            max_iterations: 50
          )
        end)

      IO.puts(
        "\n  [Benchmark] SemiNaive materialization (100 facts, 2 rules): #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      IO.puts("  [Benchmark] Iterations: #{stats.iterations}")
      IO.puts("  [Benchmark] Derived: #{stats.total_derived}")

      # Should complete in under 2 seconds
      assert time_us < 2_000_000
    end
  end

  # ===========================================================================
  # Benchmark 5: Graph Helpers
  # ===========================================================================

  describe "GraphHelpers performance" do
    alias TripleStore.Reasoner.GraphHelpers

    test "graph_id extraction is fast", %{db: db} do
      opts = [graph_id: 42, scope: :local, parallel: true]

      # Measure 100K graph_id extractions
      {time_us, _} =
        :timer.tc(fn ->
          for _ <- 1..100_000, do: GraphHelpers.graph_id!(opts)
        end)

      IO.puts("\n  [Benchmark] GraphHelpers.graph_id! 100K ops: #{time_us}μs")
      IO.puts("  [Benchmark] Throughput: #{Float.round(100_000_000.0 / time_us, 2)} ops/sec")

      # Should be very fast (< 50ms for 100K ops)
      assert time_us < 50_000
    end

    test "valid_graph_id? performance" do
      ids = Enum.to_list(0..1000) ++ Enum.to_list(-1..-100//-1)

      # Measure 1M validations
      {time_us, _} =
        :timer.tc(fn ->
          for _ <- 1..1000, id <- ids do
            GraphHelpers.valid_graph_id?(id)
          end
        end)

      ops = 1000 * length(ids)
      IO.puts("\n  [Benchmark] GraphHelpers.valid_graph_id? #{div(ops, 1000)}K ops: #{time_us}μs")

      # Should complete in under 500ms
      assert time_us < 500_000
    end
  end

  # ===========================================================================
  # Benchmark 6: DerivedStore Operations
  # ===========================================================================

  describe "DerivedStore operations" do
    test "encode/decode derived key performance", %{db: _db} do
      quads = for i <- 1..10_000, do: {rem(i, 100), rem(i, 50), i, rem(i, 10)}

      # Measure encoding time (using QuadIndex.gspo_key)
      {encode_time_us, encoded} =
        :timer.tc(fn ->
          Enum.map(quads, fn {g, s, p, o} -> QuadIndex.gspo_key(g, s, p, o) end)
        end)

      # Measure decoding time
      {decode_time_us, _} =
        :timer.tc(fn ->
          Enum.map(encoded, fn key -> QuadIndex.decode_gspo_key(key) end)
        end)

      IO.puts("\n  [Benchmark] QuadIndex.gspo_key 10K ops: #{encode_time_us}μs")
      IO.puts("  [Benchmark] QuadIndex.decode_gspo_key 10K ops: #{decode_time_us}μs")

      # Combined should be under 200ms
      assert encode_time_us + decode_time_us < 200_000
    end
  end
end
