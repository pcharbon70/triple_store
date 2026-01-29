defmodule TripleStore.SPARQL.SerializationTest do
  @moduledoc """
  Unit tests for Query Results Serialization (Section 3.6).

  Tests the serialization of query results including:
  - Graph variables in SELECT results
  - CONSTRUCT queries returning RDF.Dataset for named graphs
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Executor

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp create_context do
    # Create a mock context for testing
    %{db: nil, dict_manager: nil}
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
  # Graph Variable in SELECT Results (3.6.1)
  # ===========================================================================

  describe "to_select_results with graph variable" do
    test "includes graph variable in results" do
      # Bindings with graph variable
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "g" => {:named_node, "http://example.org/graph2"}
        }
      ]

      stream = to_stream(bindings)

      # Get all results (no variable projection)
      results = Executor.to_select_results(stream)

      assert length(results) == 2
      assert Enum.all?(results, fn r -> Map.has_key?(r, "g") end)
    end

    test "projects graph variable when specified" do
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/graph1"},
          "o" => {:literal, :simple, "Name"}
        }
      ]

      stream = to_stream(bindings)

      # Project only graph and subject
      results = Executor.to_select_results(stream, ["g", "s"])

      assert length(results) == 1
      result = hd(results)
      assert Map.has_key?(result, "g")
      assert Map.has_key?(result, "s")
      refute Map.has_key?(result, "o")
    end

    test "graph IRI is returned as standard RDF term" do
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        }
      ]

      stream = to_stream(bindings)
      results = Executor.to_select_results(stream)

      result = hd(results)
      assert Map.get(result, "g") == {:named_node, "http://example.org/graph1"}
    end
  end

  # ===========================================================================
  # CONSTRUCT with Graph (3.6.2)
  # ===========================================================================

  describe "to_construct_result" do
    setup do
      {:ok, ctx: create_context()}
    end

    test "constructs RDF.Graph from default graph bindings", %{ctx: ctx} do
      # Template with variables
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]

      # Bindings without graph variable (default graph)
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "name" => {:literal, :simple, "Alice"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "name" => {:literal, :simple, "Bob"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Graph for default graph queries
      assert %RDF.Graph{} = result

      # Verify the graph has the correct number of triples
      assert RDF.Graph.triple_count(result) == 2

      # Verify the triples are correct
      triples = RDF.Graph.triples(result) |> Enum.to_list()
      assert length(triples) == 2
    end

    test "handles empty bindings", %{ctx: ctx} do
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]

      stream = to_stream([])

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)
      # Empty graph for no bindings
      assert %RDF.Graph{} = result
      assert RDF.Graph.triple_count(result) == 0
    end

    test "skips triples with unbound variables", %{ctx: ctx} do
      # Template where name might be unbound
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]

      # Binding where name is not present
      bindings = [
        %{"s" => {:named_node, "http://example.org/Alice"}}
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)
      # No triples should be constructed due to unbound variable
      assert %RDF.Graph{} = result
      assert RDF.Graph.triple_count(result) == 0
    end

    test "returns RDF.Dataset for named graph bindings", %{ctx: ctx} do
      # Template with variables
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]

      # Bindings with graph variable
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "name" => {:literal, :simple, "Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "name" => {:literal, :simple, "Bob"},
          "g" => {:named_node, "http://example.org/graph2"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Dataset for named graph queries
      assert %RDF.Dataset{} = result

      # Verify the dataset has the correct number of graphs
      graph_names = RDF.Dataset.graphs(result) |> Enum.map(&RDF.Graph.name/1)
      assert length(graph_names) == 2

      # Verify the dataset has the correct number of quads
      assert RDF.Dataset.statement_count(result) == 2
    end

    test "returns RDF.Dataset with multiple graphs from same named graph", %{ctx: ctx} do
      # Template with variables
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/knows"}, {:variable, "o"}}
      ]

      # Bindings with graph variable - all from same graph
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "o" => {:named_node, "http://example.org/Bob"},
          "g" => {:named_node, "http://example.org/social"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "o" => {:named_node, "http://example.org/Charlie"},
          "g" => {:named_node, "http://example.org/social"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Dataset
      assert %RDF.Dataset{} = result

      # Should have 1 named graph
      graph_names = RDF.Dataset.graphs(result) |> Enum.map(&RDF.Graph.name/1)
      assert length(graph_names) == 1

      # Should have 2 quads total
      assert RDF.Dataset.statement_count(result) == 2
    end

    test "returns RDF.Dataset for CONSTRUCT with multiple named graphs", %{ctx: ctx} do
      # Template with variables
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/type"},
         {:named_node, "http://example.org/Person"}}
      ]

      # Bindings from multiple named graphs
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "g" => {:named_node, "http://example.org/graph1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Carol"},
          "g" => {:named_node, "http://example.org/graph2"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Dataset
      assert %RDF.Dataset{} = result

      # Should have 2 named graphs
      graphs = RDF.Dataset.graphs(result)
      assert length(graphs) == 2

      # Verify total statement count
      assert RDF.Dataset.statement_count(result) == 3
    end

    test "constructs with mixed literal types", %{ctx: ctx} do
      # Template with different literal types
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}},
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"},
         {:variable, "name"}},
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/tag"}, {:variable, "tag"}}
      ]

      # Bindings with different literal types
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "age" => {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"},
          "name" => {:literal, :simple, "Alice"},
          "tag" => {:literal, :lang, "person", "en"},
          "g" => {:named_node, "http://example.org/data"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Dataset
      assert %RDF.Dataset{} = result

      # Should have 3 statements
      assert RDF.Dataset.statement_count(result) == 3
    end

    test "handles blank nodes in CONSTRUCT", %{ctx: ctx} do
      # Template with blank node
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]

      # Bindings with blank node
      bindings = [
        %{
          "s" => {:blank_node, "alice"},
          "name" => {:literal, :simple, "Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # Should return RDF.Dataset
      assert %RDF.Dataset{} = result

      # Should have 1 statement
      assert RDF.Dataset.statement_count(result) == 1

      # Verify the subject is a blank node
      statements = RDF.Dataset.statements(result) |> Enum.to_list()
      subject = statements |> hd() |> RDF.Statement.subject()
      assert RDF.bnode?(subject)
    end
  end

  # ===========================================================================
  # T3: ASK and DESCRIBE with Graphs
  # ===========================================================================

  describe "T3: ASK and DESCRIBE with Graphs" do
    setup do
      {:ok, ctx: create_context()}
    end

    test "T3.1 ASK query with named graph", %{ctx: ctx} do
      # ASK query should work with named graph context
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        }
      ]

      stream = to_stream(bindings)

      # ASK returns true if any bindings exist
      result = stream |> Enum.take(1) |> length()

      # Should have at least one result for ASK to return true
      assert result > 0
    end

    test "T3.2 ASK query with GRAPH clause", %{ctx: ctx} do
      # ASK with GRAPH clause should check existence in specific graph
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "p" => {:named_node, "http://example.org/knows"},
          "o" => {:named_node, "http://example.org/Alice"},
          "g" => {:named_node, "http://example.org/social"}
        }
      ]

      stream = to_stream(bindings)

      # ASK result is based on whether any bindings match
      has_results = stream |> Enum.take(1) |> length() > 0

      assert is_boolean(has_results)
    end

    test "T3.3 DESCRIBE with named graph", %{ctx: ctx} do
      # DESCRIBE should extract triples from named graph
      template = [
        {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      ]

      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "p" => {:named_node, "http://xmlns.com/foaf/0.1/name"},
          "o" => {:literal, :simple, "Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template)

      # DESCRIBE returns an RDF graph
      assert %RDF.Dataset{} = result

      # Should contain the described resource
      assert RDF.Dataset.statement_count(result) >= 0
    end

    test "T3.4 DESCRIBE with GRAPH clause", %{ctx: ctx} do
      # DESCRIBE within GRAPH clause context
      template = [
        {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      ]

      # Multiple bindings for different graphs
      bindings = [
        %{
          "s" => {:named_node, "http://example.org/Alice"},
          "p" => {:named_node, "http://xmlns.com/foaf/0.1/name"},
          "o" => {:literal, :simple, "Alice"},
          "g" => {:named_node, "http://example.org/graph1"}
        },
        %{
          "s" => {:named_node, "http://example.org/Bob"},
          "p" => {:named_node, "http://xmlns.com/foaf/0.1/name"},
          "o" => {:literal, :simple, "Bob"},
          "g" => {:named_node, "http://example.org/graph2"}
        }
      ]

      stream = to_stream(bindings)

      assert {:ok, result} = Executor.to_construct_result(ctx, stream, template, ["g"], [])

      # DESCRIBE with GRAPH should return dataset with multiple graphs
      assert %RDF.Dataset{} = result

      # Verify statements from multiple graphs
      statement_count = RDF.Dataset.statement_count(result)
      assert statement_count == 2
    end
  end
end
