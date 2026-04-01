defmodule TripleStore.SPARQL.QuadPatternRecognitionTest do
  use ExUnit.Case, async: true
  alias TripleStore.SPARQL.QuadPatternRecognition

  @moduletag :quad_pattern_recognition

  describe "analyze_quad_pattern/2" do
    test "fully-bound pattern recommends direct lookup" do
      pattern = {:quad, {:bound, 1}, {:bound, 2}, {:bound, 3}, {:bound, 4}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.decision == :use_single_iterator
      assert analysis.recommended_approach == :direct_lookup
      assert analysis.variable_count == 0
      assert analysis.estimated_cardinality == 1.0
    end

    test "single variable pattern recommends prefix scan" do
      pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.decision == :use_single_iterator
      assert analysis.recommended_approach == :prefix_scan
      assert analysis.variable_count == 1
    end

    test "two variable pattern with high cardinality recommends multi-iterator" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}
      stats = %{total_quads: 1_000_000}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern, stats)

      assert analysis.decision == :use_multi_iterator
      assert analysis.recommended_approach == :quad_leapfrog
      assert analysis.variable_count == 2
    end

    test "four variable pattern recommends multi-iterator" do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:variable, "g"}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.decision == :use_multi_iterator
      assert analysis.recommended_approach == :quad_leapfrog
      assert analysis.variable_count == 4
    end

    test "pattern with stats uses cardinality for decision" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}
      low_stats = %{total_quads: 100}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern, low_stats)

      # Even with low stats, multi-iterator is recommended for 2+ variables
      # due to worst-case optimal guarantees
      assert analysis.decision == :use_multi_iterator
    end
  end

  describe "should_use_multi_iterator?/2" do
    test "returns false for fully-bound pattern" do
      pattern = {:quad, {:bound, 1}, {:bound, 2}, {:bound, 3}, {:bound, 4}}

      refute QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "returns false for single variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      refute QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "returns true for two variable pattern" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}

      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "returns true for four variable pattern" do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:variable, "g"}}

      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end
  end

  describe "count_variables/1" do
    test "counts variables in list of components" do
      components = [
        {:variable, "s"},
        {:bound, 42},
        {:variable, "o"},
        {:bound, 0}
      ]

      assert QuadPatternRecognition.count_variables(components) == 2
    end

    test "returns 0 for no variables" do
      components = [{:bound, 1}, {:bound, 2}, {:bound, 3}, {:bound, 4}]

      assert QuadPatternRecognition.count_variables(components) == 0
    end

    test "returns 4 for all variables" do
      components = [
        {:variable, "s"},
        {:variable, "p"},
        {:variable, "o"},
        {:variable, "g"}
      ]

      assert QuadPatternRecognition.count_variables(components) == 4
    end
  end

  describe "translate_to_leapfrog_pattern/1" do
    test "preserves variable components" do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:variable, "g"}}

      result = QuadPatternRecognition.translate_to_leapfrog_pattern(pattern)

      assert result == pattern
    end

    test "converts integer values to bound components" do
      pattern = {:quad, {:variable, "s"}, 42, 35, 0}

      result = QuadPatternRecognition.translate_to_leapfrog_pattern(pattern)

      assert result ==
               {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}
    end

    test "converts atom values to bound components" do
      pattern = {:quad, :default_graph, 42, 35, 0}

      result = QuadPatternRecognition.translate_to_leapfrog_pattern(pattern)

      assert result ==
               {:quad, {:bound, :default_graph}, {:bound, 42}, {:bound, 35},
                {:bound, 0}}
    end
  end

  describe "extract_variables/1" do
    test "extracts variable names from pattern" do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}

      result = QuadPatternRecognition.extract_variables(pattern)

      assert result == ["s", nil, "o", "g"]
    end

    test "returns all nils for fully-bound pattern" do
      pattern = {:quad, 1, 2, 3, 4}

      result = QuadPatternRecognition.extract_variables(pattern)

      assert result == [nil, nil, nil, nil]
    end

    test "returns all variable names for all-variable pattern" do
      pattern =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:variable, "g"}}

      result = QuadPatternRecognition.extract_variables(pattern)

      assert result == ["s", "p", "o", "g"]
    end
  end
end
