defmodule TripleStore.SPARQL.SolutionModifierTest do
  @moduledoc """
  Unit tests for Solution Modifier Adaptation (Section 3.5).

  Tests the handling of graph variables in SPARQL solution modifiers:
  - SELECT projection with graph variable
  - GROUP BY with graph variable
  - ORDER BY with graph variable
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Executor

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp create_binding(graph_iri) do
    base = %{"s" => {:named_node, "http://example.org/Alice"}}
    Map.put(base, "g", graph_iri)
  end

  defp to_stream(list) do
    Stream.iterate(list, fn
      [_ | t] -> t
      [] -> []
    end)
    |> Stream.take_while(fn
      [] -> false
      _ -> true
    end)
    |> Stream.flat_map(fn
      [h | _] -> [h]
      [] -> []
    end)
  end

  # ===========================================================================
  # Projection with Graph Tests (3.5.1)
  # ===========================================================================

  describe "project with graph variable" do
    test "includes graph variable when projected" do
      # Test that project preserves graph variable in binding
      binding = create_binding({:named_node, "http://example.org/graph1"})
      stream = to_stream([binding])

      # Project should keep the graph variable
      # project/2 expects a list of variable names
      var_names = ["s", "g"]
      result_stream = Executor.project(stream, var_names)

      results = Enum.to_list(result_stream)
      assert length(results) == 1
      assert Map.has_key?(hd(results), "g")
    end

    test "excludes graph variable when not projected" do
      # Test that project can exclude graph variable
      binding = create_binding({:named_node, "http://example.org/graph1"})
      stream = to_stream([binding])

      # Project only subject, not graph
      var_names = ["s"]
      result_stream = Executor.project(stream, var_names)

      results = Enum.to_list(result_stream)
      assert length(results) == 1
      refute Map.has_key?(hd(results), "g")
    end

    test "handles SELECT * with graph variable" do
      # SELECT * means project all variables including graph
      binding = create_binding({:named_node, "http://example.org/graph1"})
      stream = to_stream([binding])

      # Simulate SELECT * - all variables
      var_names = ["s", "g"]
      result_stream = Executor.project(stream, var_names)

      results = Enum.to_list(result_stream)
      assert length(results) == 1
      assert Map.has_key?(hd(results), "g")
      assert Map.has_key?(hd(results), "s")
    end
  end

  # ===========================================================================
  # GROUP BY with Graph Tests (3.5.2)
  # ===========================================================================

  describe "group_by with graph variable" do
    test "groups by graph IRI" do
      # Create bindings with different graphs
      binding1 = create_binding({:named_node, "http://example.org/graph1"})
      binding2 = create_binding({:named_node, "http://example.org/graph1"})
      binding3 = create_binding({:named_node, "http://example.org/graph2"})

      stream = to_stream([binding1, binding2, binding3])

      # Group by graph variable
      # group_by/3 expects list of {:variable, name} tuples
      group_vars = [{:variable, "g"}]
      aggregates = []

      result_stream = Executor.group_by(stream, group_vars, aggregates)
      results = Enum.to_list(result_stream)

      # Should have 2 groups (graph1 and graph2)
      assert length(results) == 2
    end

    test "groups by graph and other variable" do
      # Create bindings with different graphs and subjects
      b1 = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/g1"}
      }

      b2 = %{
        "s" => {:named_node, "http://example.org/Bob"},
        "g" => {:named_node, "http://example.org/g1"}
      }

      b3 = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/g2"}
      }

      stream = to_stream([b1, b2, b3])

      # Group by both graph and subject
      group_vars = [{:variable, "g"}, {:variable, "s"}]
      aggregates = []

      result_stream = Executor.group_by(stream, group_vars, aggregates)
      results = Enum.to_list(result_stream)

      # Should have 3 unique (g, s) combinations
      assert length(results) == 3
    end

    test "COUNT aggregates per graph group" do
      binding1 = create_binding({:named_node, "http://example.org/graph1"})
      binding2 = create_binding({:named_node, "http://example.org/graph1"})
      binding3 = create_binding({:named_node, "http://example.org/graph2"})

      stream = to_stream([binding1, binding2, binding3])

      # Group by graph with COUNT aggregate
      # Aggregate format: {{:variable, result_var}, {:aggregate_type, expr, distinct?}}
      group_vars = [{:variable, "g"}]
      aggregates = [{{:variable, "count"}, {:count, :star, false}}]

      result_stream = Executor.group_by(stream, group_vars, aggregates)
      results = Enum.to_list(result_stream)

      assert length(results) == 2

      # Verify counts: graph1 should have 2, graph2 should have 1
      counts = Enum.map(results, fn r -> Map.get(r, "count") end) |> Enum.sort()

      assert counts == [
               {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"},
               {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
             ]
    end
  end

  # ===========================================================================
  # ORDER BY with Graph Tests (3.5.3)
  # ===========================================================================

  describe "order_by with graph variable" do
    test "sorts by graph IRI" do
      # Create bindings with different graphs
      binding1 = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/Z"}
      }

      binding2 = %{
        "s" => {:named_node, "http://example.org/Bob"},
        "g" => {:named_node, "http://example.org/A"}
      }

      binding3 = %{
        "s" => {:named_node, "http://example.org/Carol"},
        "g" => {:named_node, "http://example.org/M"}
      }

      stream = to_stream([binding1, binding2, binding3])

      # Order by graph ascending
      # order_by/2 expects list of {var_name, direction} tuples
      comparators = [{"g", :asc}]
      result_stream = Executor.order_by(stream, comparators)
      results = Enum.to_list(result_stream)

      # Should be ordered: A, M, Z
      assert [
               %{"g" => {:named_node, "http://example.org/A"}},
               %{"g" => {:named_node, "http://example.org/M"}},
               %{"g" => {:named_node, "http://example.org/Z"}}
             ] = results
    end

    test "sorts by graph descending" do
      binding1 = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/Z"}
      }

      binding2 = %{
        "s" => {:named_node, "http://example.org/Bob"},
        "g" => {:named_node, "http://example.org/A"}
      }

      binding3 = %{
        "s" => {:named_node, "http://example.org/Carol"},
        "g" => {:named_node, "http://example.org/M"}
      }

      stream = to_stream([binding1, binding2, binding3])

      # Order by graph descending
      comparators = [{"g", :desc}]
      result_stream = Executor.order_by(stream, comparators)
      results = Enum.to_list(result_stream)

      # Should be ordered: Z, M, A
      assert [
               %{"g" => {:named_node, "http://example.org/Z"}},
               %{"g" => {:named_node, "http://example.org/M"}},
               %{"g" => {:named_node, "http://example.org/A"}}
             ] = results
    end

    test "sorts by graph then subject" do
      binding1 = %{
        "s" => {:named_node, "http://example.org/Bob"},
        "g" => {:named_node, "http://example.org/A"}
      }

      binding2 = %{
        "s" => {:named_node, "http://example.org/Alice"},
        "g" => {:named_node, "http://example.org/A"}
      }

      binding3 = %{
        "s" => {:named_node, "http://example.org/Carol"},
        "g" => {:named_node, "http://example.org/B"}
      }

      stream = to_stream([binding1, binding2, binding3])

      # Order by graph first, then subject
      comparators = [
        {"g", :asc},
        {"s", :asc}
      ]

      result_stream = Executor.order_by(stream, comparators)
      results = Enum.to_list(result_stream)

      # Alice/A, Bob/A, Carol/B
      assert [
               %{
                 "s" => {:named_node, "http://example.org/Alice"},
                 "g" => {:named_node, "http://example.org/A"}
               },
               %{
                 "s" => {:named_node, "http://example.org/Bob"},
                 "g" => {:named_node, "http://example.org/A"}
               },
               %{
                 "s" => {:named_node, "http://example.org/Carol"},
                 "g" => {:named_node, "http://example.org/B"}
               }
             ] = results
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "full query with graph solution modifiers" do
    test "project + group + order with graph variable" do
      # Simulate a complete query flow:
      # SELECT ?g (COUNT(?s) AS ?count)
      # WHERE { GRAPH ?g { ?s a ex:Person } }
      # GROUP BY ?g
      # ORDER BY ?g

      # Create input bindings with different graphs
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice1"},
          "g" => {:named_node, "http://example.org/g1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Alice2"},
          "g" => {:named_node, "http://example.org/g1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob1"},
          "g" => {:named_node, "http://example.org/g2"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob2"},
          "g" => {:named_node, "http://example.org/g2"}
        },
        %{
          "s" => {:named_node, "http://example.org/Carol"},
          "g" => {:named_node, "http://example.org/g1"}
        }
      ]

      stream = to_stream(bindings)

      # 1. Group by graph
      group_vars = [{:variable, "g"}]
      aggregates = [{{:variable, "count"}, {:count, :star, false}}]
      grouped = Executor.group_by(stream, group_vars, aggregates)

      # 2. Order by graph
      comparators = [{"g", :asc}]
      ordered = Executor.order_by(grouped, comparators)

      # 3. Project to get final results
      var_names = ["g", "count"]
      projected = Executor.project(ordered, var_names)

      results = Enum.to_list(projected)

      # Should have 2 groups (g1 and g2), ordered by graph IRI
      assert length(results) == 2

      # g1 should come before g2 (lexicographic)
      [first, second] = results
      assert Map.get(first, "g") == {:named_node, "http://example.org/g1"}

      assert Map.get(first, "count") ==
               {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(second, "g") == {:named_node, "http://example.org/g2"}

      assert Map.get(second, "count") ==
               {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end
  end
end
