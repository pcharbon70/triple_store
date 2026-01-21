defmodule TripleStore.SPARQL.QuadPatternTest do
  @moduledoc """
  Unit tests for quad pattern representation and conversion (Section 3.1).

  Tests the pattern type definitions, guard functions, and pattern conversion
  functions that form the foundation for quad query execution.
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Executor

  # ===========================================================================
  # Pattern Type Tests (3.1.1)
  # ===========================================================================

  describe "is_quad_pattern?/1" do
    test "returns true for quad patterns" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      assert Executor.is_quad_pattern?(quad)
    end

    test "returns true for quad patterns with bound values" do
      quad =
        {:quad, {:named_node, "http://example.org/Alice"},
         {:named_node, "http://example.org/name"}, {:literal, :simple, "Alice"},
         {:named_node, "http://example.org/graph1"}}

      assert Executor.is_quad_pattern?(quad)
    end

    test "returns true for quad patterns with default graph" do
      quad =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}

      assert Executor.is_quad_pattern?(quad)
    end

    test "returns false for triple patterns" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      refute Executor.is_quad_pattern?(triple)
    end

    test "returns false for other values" do
      refute Executor.is_quad_pattern?(nil)
      refute Executor.is_quad_pattern?(:some_atom)
      refute Executor.is_quad_pattern?({:other, 1, 2, 3, 4})
      refute Executor.is_quad_pattern?({1, 2, 3})
    end
  end

  describe "is_triple_pattern?/1" do
    test "returns true for triple patterns" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      assert Executor.is_triple_pattern?(triple)
    end

    test "returns true for triple patterns with bound values" do
      triple =
        {:triple, {:named_node, "http://example.org/Alice"},
         {:named_node, "http://example.org/name"}, {:literal, :simple, "Alice"}}

      assert Executor.is_triple_pattern?(triple)
    end

    test "returns false for quad patterns" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      refute Executor.is_triple_pattern?(quad)
    end

    test "returns false for other values" do
      refute Executor.is_triple_pattern?(nil)
      refute Executor.is_triple_pattern?(:some_atom)
      refute Executor.is_triple_pattern?({:other, 1, 2, 3})
      refute Executor.is_triple_pattern?({1, 2, 3, 4})
    end
  end

  # ===========================================================================
  # Pattern Conversion Tests (3.1.2)
  # ===========================================================================

  describe "triple_pattern_to_quad/2" do
    test "converts triple to quad with default graph (:default)" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      quad = Executor.triple_pattern_to_quad(triple, :default)

      assert quad == {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
    end

    test "converts triple to quad with default graph (:default_graph)" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      quad = Executor.triple_pattern_to_quad(triple, :default_graph)

      assert quad == {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
    end

    test "converts triple to quad with named graph IRI" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      graph_iri = {:named_node, "http://example.org/graph1"}
      quad = Executor.triple_pattern_to_quad(triple, graph_iri)

      assert quad ==
               {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
                {:named_node, "http://example.org/graph1"}}
    end

    test "converts triple to quad with graph variable" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      graph_var = {:variable, "g"}
      quad = Executor.triple_pattern_to_quad(triple, graph_var)

      assert quad ==
               {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
    end

    test "converts triple to quad with nil graph context (unbound)" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      quad = Executor.triple_pattern_to_quad(triple, nil)

      # Should add a default graph variable
      assert quad ==
               {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
                {:variable, "_graph"}}
    end

    test "preserves bound values in conversion" do
      triple =
        {:triple, {:named_node, "http://example.org/Alice"},
         {:named_node, "http://example.org/name"}, {:literal, :simple, "Alice"}}

      quad = Executor.triple_pattern_to_quad(triple, :default)

      assert quad ==
               {:quad, {:named_node, "http://example.org/Alice"},
                {:named_node, "http://example.org/name"}, {:literal, :simple, "Alice"},
                :default_graph}
    end

    test "preserves mixed bound/unbound values in conversion" do
      triple =
        {:triple, {:named_node, "http://example.org/Alice"}, {:variable, "p"}, {:variable, "o"}}

      quad = Executor.triple_pattern_to_quad(triple, {:variable, "g"})

      assert quad ==
               {:quad, {:named_node, "http://example.org/Alice"}, {:variable, "p"},
                {:variable, "o"}, {:variable, "g"}}
    end
  end

  # ===========================================================================
  # Binding Helper Tests (3.1.3)
  # ===========================================================================

  describe "binding_has_graph?/2" do
    test "returns true when graph variable is bound (string var name)" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/graph1"}
      }

      assert Executor.binding_has_graph?(binding, "g")
    end

    test "returns true when graph variable is bound (tuple var)" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/graph1"}
      }

      assert Executor.binding_has_graph?(binding, {:variable, "g"})
    end

    test "returns false when graph variable is not bound" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "p" => {:named_node, "http://example.org/name"}
      }

      refute Executor.binding_has_graph?(binding, "g")
    end

    test "returns false when binding is empty" do
      refute Executor.binding_has_graph?(%{}, "g")
    end

    test "returns false for non-existent variable" do
      binding = %{"x" => {:named_node, "http://example.org/x"}}
      refute Executor.binding_has_graph?(binding, "y")
    end
  end

  describe "extract_graph_from_binding/2" do
    test "returns bound graph term when variable is present" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/graph1"}
      }

      assert Executor.extract_graph_from_binding(binding, {:variable, "g"}) ==
               {:bound, {:named_node, "http://example.org/graph1"}}
    end

    test "returns :not_bound when graph variable is missing" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "p" => {:named_node, "http://example.org/name"}
      }

      assert Executor.extract_graph_from_binding(binding, {:variable, "g"}) == :not_bound
    end

    test "returns :not_bound for empty binding" do
      assert Executor.extract_graph_from_binding(%{}, {:variable, "g"}) == :not_bound
    end

    test "handles literal values in binding" do
      binding = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:literal, :simple, "graph1"}
      }

      assert Executor.extract_graph_from_binding(binding, {:variable, "g"}) ==
               {:bound, {:literal, :simple, "graph1"}}
    end
  end

  describe "default_graph_id/0" do
    test "returns 0 as the default graph ID" do
      assert Executor.default_graph_id() == 0
    end

    test "matches QuadIndex default graph ID" do
      assert Executor.default_graph_id() == TripleStore.QuadIndex.default_graph_id()
    end
  end
end
