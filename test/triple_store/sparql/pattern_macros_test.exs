defmodule TripleStore.SPARQL.PatternMacrosTest do
  @moduledoc """
  Tests for PatternMacros module (S17).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.PatternMacros

  describe "pattern_terms/1" do
    test "extracts terms from triple pattern" do
      s = {:variable, "s"}
      p = 10
      o = {:variable, "o"}

      assert PatternMacros.pattern_terms({:triple, s, p, o}) == [s, p, o]
    end

    test "extracts terms from quad pattern" do
      s = {:variable, "s"}
      p = 10
      o = {:variable, "o"}
      g = {:variable, "g"}

      assert PatternMacros.pattern_terms({:quad, s, p, o, g}) == [s, p, o, g]
    end

    test "returns empty list for non-pattern" do
      assert PatternMacros.pattern_terms(:not_a_pattern) == []
    end

    test "returns empty list for malformed pattern" do
      assert PatternMacros.pattern_terms({:wrong, 1, 2}) == []
    end
  end

  describe "pattern_type/1" do
    test "returns :triple for triple patterns" do
      assert PatternMacros.pattern_type({:triple, 1, 2, 3}) == :triple
    end

    test "returns :quad for quad patterns" do
      assert PatternMacros.pattern_type({:quad, 1, 2, 3, 4}) == :quad
    end

    test "returns :unknown for non-patterns" do
      assert PatternMacros.pattern_type(:not_a_pattern) == :unknown
    end
  end

  describe "pattern_arity/1" do
    test "returns 3 for triple patterns" do
      assert PatternMacros.pattern_arity({:triple, 1, 2, 3}) == 3
    end

    test "returns 4 for quad patterns" do
      assert PatternMacros.pattern_arity({:quad, 1, 2, 3, 4}) == 4
    end

    test "returns 0 for non-patterns" do
      assert PatternMacros.pattern_arity(:not_a_pattern) == 0
    end
  end

  describe "extract_var_name/1" do
    test "extracts variable name from variable term" do
      assert PatternMacros.extract_var_name({:variable, "x"}) == "x"
    end

    test "returns nil for constant terms" do
      assert PatternMacros.extract_var_name(42) == nil
      assert PatternMacros.extract_var_name({:literal, "value"}) == nil
      assert PatternMacros.extract_var_name({:named_node, "http://example.org"}) == nil
    end
  end

  describe "pattern_variables/1" do
    test "extracts variables from triple pattern" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.pattern_variables(pattern) == ["s", "o"]
    end

    test "extracts variables from quad pattern" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}}
      assert PatternMacros.pattern_variables(pattern) == ["s", "o", "g"]
    end

    test "returns empty list when no variables" do
      pattern = {:triple, 1, 2, 3}
      assert PatternMacros.pattern_variables(pattern) == []
    end

    test "removes duplicate variables" do
      pattern = {:triple, {:variable, "x"}, {:variable, "x"}, {:variable, "x"}}
      assert PatternMacros.pattern_variables(pattern) == ["x"]
    end

    test "returns empty list for non-pattern" do
      assert PatternMacros.pattern_variables(:not_a_pattern) == []
    end
  end

  describe "pattern_variable_set/1" do
    test "returns MapSet of variables from triple pattern" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      result = PatternMacros.pattern_variable_set(pattern)
      assert MapSet.equal?(result, MapSet.new(["s", "o"]))
    end

    test "returns MapSet of variables from quad pattern" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}}
      result = PatternMacros.pattern_variable_set(pattern)
      assert MapSet.equal?(result, MapSet.new(["s", "o", "g"]))
    end

    test "returns empty MapSet when no variables" do
      pattern = {:triple, 1, 2, 3}
      result = PatternMacros.pattern_variable_set(pattern)
      assert MapSet.equal?(result, MapSet.new())
    end
  end

  describe "bound_term_count/1" do
    test "counts bound terms in triple pattern" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.bound_term_count(pattern) == 1
    end

    test "counts bound terms in quad pattern" do
      pattern = {:quad, {:variable, "s"}, 10, 20, {:variable, "g"}}
      assert PatternMacros.bound_term_count(pattern) == 2
    end

    test "counts all bound when no variables" do
      assert PatternMacros.bound_term_count({:triple, 1, 2, 3}) == 3
      assert PatternMacros.bound_term_count({:quad, 1, 2, 3, 4}) == 4
    end

    test "returns 0 for non-pattern" do
      assert PatternMacros.bound_term_count(:not_a_pattern) == 0
    end
  end

  describe "has_variable?/2" do
    test "returns true when variable is in triple pattern" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.has_variable?(pattern, "s")
      assert PatternMacros.has_variable?(pattern, "o")
    end

    test "returns false when variable is not in triple pattern" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      refute PatternMacros.has_variable?(pattern, "g")
      refute PatternMacros.has_variable?(pattern, "x")
    end

    test "returns true when variable is in quad pattern" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}}
      assert PatternMacros.has_variable?(pattern, "s")
      assert PatternMacros.has_variable?(pattern, "g")
    end

    test "returns false when variable is not in quad pattern" do
      pattern = {:quad, {:variable, "s"}, 10, 20, {:variable, "g"}}
      refute PatternMacros.has_variable?(pattern, "x")
    end

    test "returns false for non-pattern" do
      refute PatternMacros.has_variable?(:not_a_pattern, "x")
    end
  end

  describe "variable_position/2" do
    test "returns subject position for variable in subject" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.variable_position(pattern, "s") == :subject
    end

    test "returns predicate position for variable in predicate" do
      pattern = {:triple, 10, {:variable, "p"}, {:variable, "o"}}
      assert PatternMacros.variable_position(pattern, "p") == :predicate
    end

    test "returns object position for variable in object" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.variable_position(pattern, "o") == :object
    end

    test "returns graph position for variable in graph" do
      pattern = {:quad, 10, 20, 30, {:variable, "g"}}
      assert PatternMacros.variable_position(pattern, "g") == :graph
    end

    test "returns nil when variable not found" do
      pattern = {:triple, {:variable, "s"}, 10, {:variable, "o"}}
      assert PatternMacros.variable_position(pattern, "x") == nil
    end
  end

  describe "term_bound?/2" do
    test "returns true for constant term" do
      assert PatternMacros.term_bound?(42, %{})
      assert PatternMacros.term_bound?({:literal, "value"}, MapSet.new())
    end

    test "returns true for variable in bound map" do
      assert PatternMacros.term_bound?({:variable, "x"}, %{"x" => 1})
    end

    test "returns true for variable in bound set" do
      assert PatternMacros.term_bound?({:variable, "x"}, MapSet.new(["x"]))
    end

    test "returns false for unbound variable" do
      refute PatternMacros.term_bound?({:variable, "x"}, %{})
      refute PatternMacros.term_bound?({:variable, "x"}, MapSet.new(["y"]))
    end
  end

  describe "to_quad/2" do
    test "converts triple to quad with default graph" do
      triple = {:triple, 1, 2, 3}
      assert PatternMacros.to_quad(triple) == {:quad, 1, 2, 3, :default}
    end

    test "converts triple to quad with custom graph" do
      triple = {:triple, 1, 2, 3}
      assert PatternMacros.to_quad(triple, :custom) == {:quad, 1, 2, 3, :custom}
    end

    test "returns quad unchanged" do
      quad = {:quad, 1, 2, 3, 4}
      assert PatternMacros.to_quad(quad) == quad
      assert PatternMacros.to_quad(quad, :custom) == quad
    end
  end

  describe "extract_graph/1" do
    test "returns default_graph for triple pattern" do
      assert PatternMacros.extract_graph({:triple, 1, 2, 3}) == :default_graph
    end

    test "returns graph term for quad pattern" do
      assert PatternMacros.extract_graph({:quad, 1, 2, 3, :custom}) == :custom
    end

    test "returns nil for non-pattern" do
      assert PatternMacros.extract_graph(:not_a_pattern) == nil
    end
  end

  describe "triple/3 and quad/4" do
    test "creates a triple pattern" do
      assert PatternMacros.triple(1, 2, 3) == {:triple, 1, 2, 3}
    end

    test "creates a quad pattern" do
      assert PatternMacros.quad(1, 2, 3, 4) == {:quad, 1, 2, 3, 4}
    end
  end

  describe "guard macros" do
    test "is_triple_pattern/1 works in guards" do
      require TripleStore.SPARQL.PatternMacros

      test_fun = fn
        arg when PatternMacros.is_triple_pattern(arg) -> :triple
        _ -> :other
      end

      assert test_fun.({:triple, 1, 2, 3}) == :triple
      assert test_fun.({:quad, 1, 2, 3, 4}) == :other
    end

    test "is_quad_pattern/1 works in guards" do
      require TripleStore.SPARQL.PatternMacros

      test_fun = fn
        arg when PatternMacros.is_quad_pattern(arg) -> :quad
        _ -> :other
      end

      assert test_fun.({:quad, 1, 2, 3, 4}) == :quad
      assert test_fun.({:triple, 1, 2, 3}) == :other
    end

    test "is_pattern/1 works in guards" do
      require TripleStore.SPARQL.PatternMacros

      test_fun = fn
        arg when PatternMacros.is_pattern(arg) -> :pattern
        _ -> :other
      end

      assert test_fun.({:triple, 1, 2, 3}) == :pattern
      assert test_fun.({:quad, 1, 2, 3, 4}) == :pattern
      assert test_fun.({:other, 1, 2}) == :other
    end

    test "is_variable_term/1 works in guards" do
      require TripleStore.SPARQL.PatternMacros

      test_fun = fn
        arg when PatternMacros.is_variable_term(arg) -> :variable
        _ -> :constant
      end

      assert test_fun.({:variable, "x"}) == :variable
      assert test_fun.(42) == :constant
    end
  end
end
