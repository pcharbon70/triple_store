defmodule TripleStore.Benchmark.LUBMTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LUBM

  @moduletag :benchmark

  describe "generate/2" do
    test "generates graph for scale 1" do
      graph = LUBM.generate(1)

      assert %RDF.Graph{} = graph
      triple_count = RDF.Graph.triple_count(graph)

      # Should generate significant number of triples
      assert triple_count > 10_000
      assert triple_count < 200_000
    end

    test "generates more triples with higher scale" do
      graph1 = LUBM.generate(1)
      graph2 = LUBM.generate(2)

      count1 = RDF.Graph.triple_count(graph1)
      count2 = RDF.Graph.triple_count(graph2)

      # Scale 2 should have roughly twice as many triples
      assert count2 > count1
      assert count2 > count1 * 1.5
    end

    test "generates deterministic output with same seed" do
      graph1 = LUBM.generate(1, seed: 12345)
      graph2 = LUBM.generate(1, seed: 12345)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      assert MapSet.equal?(triples1, triples2)
    end

    test "generates different output with different seeds" do
      graph1 = LUBM.generate(1, seed: 11111)
      graph2 = LUBM.generate(1, seed: 22222)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      # Should have some overlap (same schema) but not be identical
      refute MapSet.equal?(triples1, triples2)
    end

    test "generates expected entity types" do
      graph = LUBM.generate(1)
      triples = RDF.Graph.triples(graph)

      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      type_triples = Enum.filter(triples, fn {_, p, _} -> p == rdf_type end)

      types = Enum.map(type_triples, fn {_, _, o} -> to_string(o) end)

      assert Enum.any?(types, &String.contains?(&1, "University"))
      assert Enum.any?(types, &String.contains?(&1, "Department"))
      assert Enum.any?(types, &String.contains?(&1, "Professor"))
      assert Enum.any?(types, &String.contains?(&1, "Student"))
      assert Enum.any?(types, &String.contains?(&1, "Course"))
      assert Enum.any?(types, &String.contains?(&1, "Publication"))
    end
  end

  describe "stream/2" do
    test "generates triples as stream" do
      stream = LUBM.stream(1)

      triples = Enum.take(stream, 100)

      assert length(triples) == 100
      assert Enum.all?(triples, fn {s, p, o} -> is_struct(s) and is_struct(p) end)
    end

    test "stream generates same triples as generate with same seed" do
      graph = LUBM.generate(1, seed: 99999)
      stream = LUBM.stream(1, seed: 99999)

      graph_triples = MapSet.new(RDF.Graph.triples(graph))
      stream_triples = MapSet.new(Enum.to_list(stream))

      assert MapSet.equal?(graph_triples, stream_triples)
    end
  end

  describe "estimate_triple_count/1" do
    test "returns reasonable estimate" do
      estimate = LUBM.estimate_triple_count(1)

      # Estimate should be in a reasonable range
      assert estimate > 20_000
      assert estimate < 200_000
    end

    test "estimate scales linearly" do
      est1 = LUBM.estimate_triple_count(1)
      est5 = LUBM.estimate_triple_count(5)

      # Should be roughly 5x
      assert est5 > est1 * 4
      assert est5 < est1 * 6
    end
  end

  describe "namespace/0" do
    test "returns LUBM ontology namespace" do
      ns = LUBM.namespace()

      assert is_binary(ns)
      assert String.contains?(ns, "lehigh.edu")
      assert String.contains?(ns, "univ-bench")
    end
  end

  describe "RDF validity" do
    test "all triples have valid IRI subjects" do
      graph = LUBM.generate(1, seed: 42)

      for {s, _p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = s
        assert String.starts_with?(to_string(s), "http://")
      end
    end

    test "all triples have valid IRI predicates" do
      graph = LUBM.generate(1, seed: 42)

      for {_s, p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = p
        assert String.starts_with?(to_string(p), "http://")
      end
    end

    test "all objects are valid RDF terms" do
      graph = LUBM.generate(1, seed: 42)

      for {_s, _p, o} <- RDF.Graph.triples(graph) do
        assert is_struct(o, RDF.IRI) or is_struct(o, RDF.Literal)
      end
    end

    test "no blank nodes in generated data" do
      graph = LUBM.generate(1, seed: 42)

      for {s, _p, o} <- RDF.Graph.triples(graph) do
        refute is_struct(s, RDF.BlankNode)
        refute is_struct(o, RDF.BlankNode)
      end
    end

    test "all URIs are well-formed" do
      graph = LUBM.generate(1, seed: 42)

      for {s, p, o} <- RDF.Graph.triples(graph) do
        assert valid_uri?(to_string(s))
        assert valid_uri?(to_string(p))

        if is_struct(o, RDF.IRI) do
          assert valid_uri?(to_string(o))
        end
      end
    end

    test "rdf:type triples use correct namespace" do
      graph = LUBM.generate(1, seed: 42)
      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

      type_triples = Enum.filter(RDF.Graph.triples(graph), fn {_, p, _} -> p == rdf_type end)

      # Should have type declarations
      assert length(type_triples) > 0

      # All type objects should be LUBM classes
      for {_, _, o} <- type_triples do
        assert is_struct(o, RDF.IRI)
        uri = to_string(o)
        assert String.contains?(uri, "lehigh.edu") or String.contains?(uri, "University")
      end
    end

    test "literals have appropriate datatypes" do
      graph = LUBM.generate(1, seed: 42)

      literals =
        RDF.Graph.triples(graph)
        |> Enum.map(fn {_, _, o} -> o end)
        |> Enum.filter(&is_struct(&1, RDF.Literal))

      # Should have some literals (names, emails, etc.)
      assert length(literals) > 0

      # Check that literals have values
      for lit <- literals do
        assert RDF.Literal.value(lit) != nil
      end
    end
  end

  # Helper for URI validation
  defp valid_uri?(uri) do
    String.starts_with?(uri, "http://") or String.starts_with?(uri, "https://")
  end
end
