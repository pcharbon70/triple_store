defmodule TripleStore.Benchmark.WatDivTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.WatDiv

  @moduletag :benchmark

  describe "generate/2" do
    test "generates graph for scale 1" do
      graph = WatDiv.generate(1)

      assert %RDF.Graph{} = graph
      triple_count = RDF.Graph.triple_count(graph)

      # Should generate approximately 40K triples
      assert triple_count > 30_000
      assert triple_count < 100_000
    end

    test "generates more triples with larger scale" do
      graph1 = WatDiv.generate(1)
      graph2 = WatDiv.generate(2)

      count1 = RDF.Graph.triple_count(graph1)
      count2 = RDF.Graph.triple_count(graph2)

      # Scale 2 should have more triples than scale 1
      assert count2 > count1
      assert count2 > count1 * 1.5
    end

    test "generates deterministic output with same seed" do
      graph1 = WatDiv.generate(1, seed: 12_345)
      graph2 = WatDiv.generate(1, seed: 12_345)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      assert MapSet.equal?(triples1, triples2)
    end

    test "generates different output with different seeds" do
      graph1 = WatDiv.generate(1, seed: 11_111)
      graph2 = WatDiv.generate(1, seed: 22_222)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      # Should have some overlap (same schema) but not be identical
      refute MapSet.equal?(triples1, triples2)
    end

    test "generates expected entity types" do
      graph = WatDiv.generate(1)
      triples = RDF.Graph.triples(graph)

      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      type_triples = Enum.filter(triples, fn {_, p, _} -> p == rdf_type end)

      types = Enum.map(type_triples, fn {_, _, o} -> to_string(o) end)

      # WatDiv entity types
      assert Enum.any?(types, &String.contains?(&1, "User"))
      assert Enum.any?(types, &String.contains?(&1, "Product"))
      assert Enum.any?(types, fn t -> String.contains?(t, "Offer") or String.contains?(t, "Offer") end)
      assert Enum.any?(types, &String.contains?(&1, "Retailer"))
      assert Enum.any?(types, &String.contains?(&1, "Genre"))
    end

    test "generates users with social relationships" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      wsdbm_ns = "http://db.uwaterloo.ca/~galuc/wsdbm/"

      # Check for friendship relationships (uses friendOf property)
      friendships =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), wsdbm_ns <> "friendOf")
        end)

      # Should have some friendships
      assert length(friendships) > 0

      # Check for likes relationships
      likes =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), wsdbm_ns <> "likes")
        end)

      # Should have some likes
      assert length(likes) > 0
    end

    test "generates products with offers" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      wsdbm_ns = "http://db.uwaterloo.ca/~galuc/wsdbm/"

      # Check for offer entities
      offer_triples =
        Enum.filter(triples, fn {s, _, _} ->
          String.contains?(to_string(s), wsdbm_ns <> "Offer")
        end)

      # Should have offer entities
      assert length(offer_triples) > 0

      # Check for product-offer relationships
      product_offer_triples =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), wsdbm_ns <> "product")
        end)

      # Should have product relationships
      assert length(product_offer_triples) > 0
    end

    test "heterogeneous structure - not all entities have same attributes" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      sorg_ns = "http://schema.org/"
      wsdbm_ns = "http://db.uwaterloo.ca/~galuc/wsdbm/"

      # Get all users
      user_type = RDF.iri(sorg_ns <> "User")
      users =
        triples
        |> Enum.filter(fn {s, p, o} ->
          p == RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") and o == user_type
        end)
        |> Enum.map(fn {s, _, _} -> s end)
        |> Enum.take(10)

      # Check that not all users have all attributes
      # (some may have homepage, some may not, etc.)
      homepage_attr =
        Enum.any?(users, fn user ->
          not Enum.any?(
            triples,
            fn {s, p, _} ->
              s == user and String.contains?(to_string(p), "homepage")
            end
          )
        end)

      # At least some users should not have certain attributes
      assert homepage_attr or length(users) == 0
    end
  end

  describe "stream/2" do
    test "generates triples as stream" do
      stream = WatDiv.stream(1)

      triples = Enum.take(stream, 100)

      assert length(triples) == 100
      assert Enum.all?(triples, fn {s, p, _o} -> is_struct(s) and is_struct(p) end)
    end

    test "stream generates same triples as generate with same seed" do
      graph = WatDiv.generate(1, seed: 99_999)
      stream = WatDiv.stream(1, seed: 99_999)

      graph_triples = MapSet.new(RDF.Graph.triples(graph))
      stream_triples = MapSet.new(Enum.to_list(stream))

      assert MapSet.equal?(graph_triples, stream_triples)
    end

    test "stream is memory efficient for large scale" do
      # Stream scale 10 without materializing full graph
      stream = WatDiv.stream(10)

      # Take first 10000 triples from stream
      triples = Enum.take(stream, 10_000)

      assert length(triples) == 10_000
    end
  end

  describe "estimate_triple_count/1" do
    test "returns reasonable estimate for scale 1" do
      estimate = WatDiv.estimate_triple_count(1)

      # Should be approximately 40K
      assert estimate > 20_000
      assert estimate < 100_000
    end

    test "estimate scales roughly linearly" do
      est1 = WatDiv.estimate_triple_count(1)
      est10 = WatDiv.estimate_triple_count(10)

      # Should be roughly 10x
      assert est10 > est1 * 5
      assert est10 < est1 * 15
    end

    test "estimate is close to actual count" do
      estimate = WatDiv.estimate_triple_count(1)
      graph = WatDiv.generate(1)
      actual = RDF.Graph.triple_count(graph)

      # Estimate should be within 3x of actual (WatDiv has variable structure)
      ratio = estimate / actual
      assert ratio > 0.33
      assert ratio < 3.0
    end
  end

  describe "namespace/0" do
    test "returns WatDiv vocabulary namespace" do
      ns = WatDiv.namespace()

      assert is_binary(ns)
      assert String.contains?(ns, "uwaterloo.ca")
      assert String.contains?(ns, "wsdbm")
    end
  end

  describe "RDF validity" do
    test "all triples have valid IRI subjects" do
      graph = WatDiv.generate(1, seed: 42)

      for {s, _p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = s
        assert String.starts_with?(to_string(s), "http://")
      end
    end

    test "all triples have valid IRI predicates" do
      graph = WatDiv.generate(1, seed: 42)

      for {_s, p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = p
        assert String.starts_with?(to_string(p), "http://")
      end
    end

    test "all objects are valid RDF terms" do
      graph = WatDiv.generate(1, seed: 42)

      for {_s, _p, o} <- RDF.Graph.triples(graph) do
        assert is_struct(o, RDF.IRI) or is_struct(o, RDF.Literal)
      end
    end

    test "no blank nodes in generated data" do
      graph = WatDiv.generate(1, seed: 42)

      for {s, _p, o} <- RDF.Graph.triples(graph) do
        refute is_struct(s, RDF.BlankNode)
        refute is_struct(o, RDF.BlankNode)
      end
    end

    test "all URIs are well-formed" do
      graph = WatDiv.generate(1, seed: 42)

      for {s, p, o} <- RDF.Graph.triples(graph) do
        assert valid_uri?(to_string(s))
        assert valid_uri?(to_string(p))

        if is_struct(o, RDF.IRI) do
          assert valid_uri?(to_string(o))
        end
      end
    end

    test "rdf:type triples use correct namespace" do
      graph = WatDiv.generate(1, seed: 42)
      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

      type_triples = Enum.filter(RDF.Graph.triples(graph), fn {_, p, _} -> p == rdf_type end)

      # Should have type declarations
      assert type_triples != []

      # All type objects should be valid IRIs
      for {_, _, o} <- type_triples do
        assert is_struct(o, RDF.IRI)
        uri = to_string(o)
        assert String.starts_with?(uri, "http://")
      end
    end

    test "literals have appropriate datatypes" do
      graph = WatDiv.generate(1, seed: 42)

      literals =
        RDF.Graph.triples(graph)
        |> Enum.map(fn {_, _, o} -> o end)
        |> Enum.filter(&is_struct(&1, RDF.Literal))

      # Should have many literals (labels, titles, prices, etc.)
      assert length(literals) > 1000

      # Check that literals have values
      for lit <- literals do
        assert RDF.Literal.value(lit) != nil
      end
    end

    test "numeric properties have numeric values" do
      graph = WatDiv.generate(1, seed: 42)

      # Check price values (sorg:price is used in offers, not priceCurrency)
      sorg_ns = "http://schema.org/"

      price_triples =
        Enum.filter(RDF.Graph.triples(graph), fn {_s, p, _o} ->
          pred_str = to_string(p)
          # Match sorg:price but not sorg:priceCurrency
          String.contains?(pred_str, sorg_ns <> "price") and
            not String.contains?(pred_str, "priceCurrency")
        end)

      # Should have at least one price
      assert length(price_triples) > 0

      # Check that prices are numeric
      for {_s, _p, o} <- price_triples do
        assert is_struct(o, RDF.Literal)
        value = RDF.Literal.value(o)
        assert is_number(value)
        assert value > 0
      end
    end
  end

  describe "WatDiv-specific features" do
    test "generates products with genres" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      sorg_ns = "http://schema.org/"

      # Check for genre relationships
      genre_triples =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), sorg_ns <> "genre")
        end)

      # Should have genre assignments
      assert length(genre_triples) > 0
    end

    test "generates purchase relationships" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      wsdbm_ns = "http://db.uwaterloo.ca/~galuc/wsdbm/"

      # Check for purchase relationships
      purchase_triples =
        Enum.filter(triples, fn {s, _p, _} ->
          String.contains?(to_string(s), wsdbm_ns <> "Purchase")
        end)

      # Should have purchase entities
      assert length(purchase_triples) > 0
    end

    test "generates multiple entity categories" do
      graph = WatDiv.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

      type_counts =
        triples
        |> Enum.filter(fn {_, p, _} -> p == rdf_type end)
        |> Enum.map(fn {_, _, o} -> to_string(o) end)
        |> Enum.frequencies()

      # Should have multiple entity types
      assert map_size(type_counts) > 5
    end
  end

  # Helper for URI validation
  defp valid_uri?(uri) do
    String.starts_with?(uri, "http://") or String.starts_with?(uri, "https://")
  end
end
