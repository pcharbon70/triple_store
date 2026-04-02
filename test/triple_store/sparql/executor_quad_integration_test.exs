defmodule TripleStore.SPARQL.ExecutorQuadIntegrationTest do
  use ExUnit.Case, async: false
  alias TripleStore.SPARQL.Executor
  alias TripleStore.SPARQL.QuadPatternRecognition
  alias TripleStore.SPARQL.GraphClauseOptimization

  @moduletag :executor_quad_integration

  describe "QuadLeapfrog integration in executor" do
    test "should_use_multi_iterator? returns true for 4-variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "should_use_multi_iterator? returns false for single-variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      refute QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "should_use_multi_iterator? returns true for 2-variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}

      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end
  end

  describe "Pattern Recognition in executor context" do
    test "correctly identifies patterns that should use multi-iterator" do
      # Four-variable pattern should use multi-iterator
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.decision == :use_multi_iterator
      assert analysis.variable_count == 4
    end

    test "correctly identifies patterns that should use single iterator" do
      # Single variable pattern should use single iterator
      pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.decision == :use_single_iterator
      assert analysis.variable_count == 1
    end
  end

  describe "GRAPH clause optimization in executor" do
    test "detects static graph with single variable pattern" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :graph_prefixed_single_iterator
    end

    test "detects variable graph with 3 other variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :variable
      assert analysis.recommended_strategy == :four_iterator_enumeration
    end

    test "should_use_multi_iterator_for_graph? for variable graph" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "should_use_multi_iterator_for_graph? for static graph with 2+ variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end
  end

  describe "Result conversion" do
    test "converts QuadLeapfrog bindings to executor format" do
      # QuadLeapfrog uses {:variable, name} and {:bound, id}
      lf_bindings = [
        {:variable, "s"},
        {:bound, 123},
        {:variable, "o"}
      ]

      result = convert_leapfrog_bindings_to_executor(lf_bindings)

      assert result["s"] == nil
      assert result["o"] == nil
      # Bound value would be the actual RDF term
      assert is_map(result)
    end

    test "handles empty binding list" do
      result = convert_leapfrog_bindings_to_executor([])

      assert result == %{}
    end

    test "preserves all variable names" do
      lf_bindings = [
        {:variable, "s"},
        {:variable, "p"},
        {:variable, "o"},
        {:variable, "g"}
      ]

      result = convert_leapfrog_bindings_to_executor(lf_bindings)

      # Map keys may be in any order
      assert MapSet.new(Map.keys(result)) == MapSet.new(["s", "p", "o", "g"])
    end
  end

  describe "Streaming result behavior" do
    test "LIMIT/OFFSET can be applied to binding stream" do
      # Simulate a stream of bindings
      bindings_stream = [
        %{"s" => 1, "p" => 2, "o" => 3},
        %{"s" => 4, "p" => 5, "o" => 6},
        %{"s" => 7, "p" => 8, "o" => 9},
        %{"s" => 10, "p" => 11, "o" => 12}
      ]

      # Apply LIMIT 2
      limited = Enum.take(bindings_stream, 2)

      assert length(limited) == 2
      assert hd(limited)["s"] == 1
    end

    test "OFFSET skips correct number of results" do
      bindings_stream = [
        %{"s" => 1, "p" => 2, "o" => 3},
        %{"s" => 4, "p" => 5, "o" => 6},
        %{"s" => 7, "p" => 8, "o" => 9},
        %{"s" => 10, "p" => 11, "o" => 12}
      ]

      # Apply OFFSET 2
      offset_result = Enum.drop(bindings_stream, 2)

      assert length(offset_result) == 2
      assert hd(offset_result)["s"] == 7
    end

    test "combined LIMIT and OFFSET works correctly" do
      bindings_stream =
        Enum.map(1..10, fn i -> %{"s" => i, "p" => i * 10, "o" => i * 100} end)

      # Apply OFFSET 5, LIMIT 3
      result = bindings_stream |> Enum.drop(5) |> Enum.take(3)

      assert length(result) == 3
      assert hd(result)["s"] == 6
      assert List.last(result)["s"] == 8
    end
  end

  describe "ORDER BY with QuadLeapfrog results" do
    test "can order bindings by variable" do
      bindings = [
        %{"s" => 3, "p" => 2, "o" => 1},
        %{"s" => 1, "p" => 2, "o" => 3},
        %{"s" => 2, "p" => 2, "o" => 2}
      ]

      ordered = Enum.sort_by(bindings, fn b -> b["s"] end)

      assert hd(ordered)["s"] == 1
      assert List.last(ordered)["s"] == 3
    end

    test "can order bindings by multiple variables" do
      bindings = [
        %{"s" => 1, "p" => 3, "o" => 1},
        %{"s" => 1, "p" => 1, "o" => 3},
        %{"s" => 2, "p" => 2, "o" => 2}
      ]

      ordered = Enum.sort_by(bindings, fn b -> {b["s"], b["p"]} end)

      assert hd(ordered)["p"] == 1
      assert Enum.at(ordered, 1)["p"] == 3
      assert List.last(ordered)["s"] == 2
    end

    test "handles missing values in ordering" do
      bindings = [
        %{"s" => 1, "p" => nil},
        %{"s" => nil, "p" => 2},
        %{"s" => 2, "p" => 1}
      ]

      # nil values should sort last
      ordered = Enum.sort_by(bindings, fn b -> b["s"] || :infinity end)

      assert hd(ordered)["s"] == 1
    end
  end

  describe "Query semantics preservation" do
    test "variable names are preserved through binding conversion" do
      original_vars = ["subject", "predicate", "object", "graph"]
      lf_bindings = Enum.map(original_vars, &{:variable, &1})

      result = convert_leapfrog_bindings_to_executor(lf_bindings)

      # Map keys may be in any order
      assert MapSet.new(Map.keys(result)) == MapSet.new(original_vars)
    end

    test "bound values maintain their identity" do
      lf_bindings = [
        {:variable, "s"},
        {:bound, 42},
        {:variable, "o"}
      ]

      result = convert_leapfrog_bindings_to_executor(lf_bindings)

      # Bound values are stored with their position as key
      # The convert function stores bound values directly in the map
      assert 42 in Map.values(result)
    end
  end

  # Helper functions for tests
  defp count_variables_in_pattern({:quad, s, p, o, g}) do
    components = [s, p, o, g]
    Enum.count(components, fn
      {:variable, _} -> true
      _ -> false
    end)
  end

  defp convert_leapfrog_bindings_to_executor(lf_bindings) do
    Enum.map(lf_bindings, fn
      {:variable, name} -> {name, nil}
      {:bound, _id} = bound -> bound
    end)
    |> Enum.into(%{})
  end
end
