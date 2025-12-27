defmodule TripleStore.Benchmark.BSBMTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.BSBM

  @moduletag :benchmark

  describe "generate/2" do
    test "generates graph for 100 products" do
      graph = BSBM.generate(100)

      assert %RDF.Graph{} = graph
      triple_count = RDF.Graph.triple_count(graph)

      # Should generate significant number of triples
      assert triple_count > 5_000
      assert triple_count < 100_000
    end

    test "generates more triples with more products" do
      graph1 = BSBM.generate(100)
      graph2 = BSBM.generate(200)

      count1 = RDF.Graph.triple_count(graph1)
      count2 = RDF.Graph.triple_count(graph2)

      # More products = more triples
      assert count2 > count1
      assert count2 > count1 * 1.5
    end

    test "generates deterministic output with same seed" do
      graph1 = BSBM.generate(100, seed: 12345)
      graph2 = BSBM.generate(100, seed: 12345)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      assert MapSet.equal?(triples1, triples2)
    end

    test "generates different output with different seeds" do
      graph1 = BSBM.generate(100, seed: 11111)
      graph2 = BSBM.generate(100, seed: 22222)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      # Should have some overlap (same schema) but not be identical
      refute MapSet.equal?(triples1, triples2)
    end

    test "generates expected entity types" do
      graph = BSBM.generate(50)
      triples = RDF.Graph.triples(graph)

      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      type_triples = Enum.filter(triples, fn {_, p, _} -> p == rdf_type end)

      types = Enum.map(type_triples, fn {_, _, o} -> to_string(o) end)

      assert Enum.any?(types, &String.contains?(&1, "Product"))
      assert Enum.any?(types, &String.contains?(&1, "Producer"))
      assert Enum.any?(types, &String.contains?(&1, "Vendor"))
      assert Enum.any?(types, &String.contains?(&1, "Offer"))
      assert Enum.any?(types, &String.contains?(&1, "Review"))
    end

    test "generates products with correct relationships" do
      graph = BSBM.generate(10)
      triples = RDF.Graph.triples(graph)

      bsbm_producer = RDF.iri("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer")
      producer_triples = Enum.filter(triples, fn {_, p, _} -> p == bsbm_producer end)

      # Each product should have a producer
      assert length(producer_triples) >= 10
    end
  end

  describe "stream/2" do
    test "generates triples as stream" do
      stream = BSBM.stream(50)

      triples = Enum.take(stream, 100)

      assert length(triples) == 100
      assert Enum.all?(triples, fn {s, p, _o} -> is_struct(s) and is_struct(p) end)
    end

    test "stream generates same triples as generate with same seed" do
      graph = BSBM.generate(50, seed: 99999)
      stream = BSBM.stream(50, seed: 99999)

      graph_triples = MapSet.new(RDF.Graph.triples(graph))
      stream_triples = MapSet.new(Enum.to_list(stream))

      assert MapSet.equal?(graph_triples, stream_triples)
    end
  end

  describe "estimate_triple_count/1" do
    test "returns reasonable estimate" do
      estimate = BSBM.estimate_triple_count(100)

      assert estimate > 10_000
      assert estimate < 200_000
    end

    test "estimate scales roughly linearly" do
      est100 = BSBM.estimate_triple_count(100)
      est500 = BSBM.estimate_triple_count(500)

      # Should be roughly 5x
      assert est500 > est100 * 3
      assert est500 < est100 * 7
    end
  end

  describe "namespace/0" do
    test "returns BSBM vocabulary namespace" do
      ns = BSBM.namespace()

      assert is_binary(ns)
      assert String.contains?(ns, "bsbm")
      assert String.contains?(ns, "vocabulary")
    end
  end
end
