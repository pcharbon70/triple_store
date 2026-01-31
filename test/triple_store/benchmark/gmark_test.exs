defmodule TripleStore.Benchmark.GMarkTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.GMark

  @moduletag :benchmark

  describe "generate/2" do
    test "generates graph for scale 1" do
      graph = GMark.generate(1)

      assert %RDF.Graph{} = graph
      triple_count = RDF.Graph.triple_count(graph)

      # Should generate approximately 10K triples
      assert triple_count > 5_000
      assert triple_count < 50_000
    end

    test "generates more triples with larger scale" do
      graph1 = GMark.generate(1)
      graph2 = GMark.generate(2)

      count1 = RDF.Graph.triple_count(graph1)
      count2 = RDF.Graph.triple_count(graph2)

      # Scale 2 should have more triples than scale 1
      assert count2 > count1
      assert count2 > count1 * 1.5
    end

    test "generates deterministic output with same seed" do
      graph1 = GMark.generate(1, seed: 12_345)
      graph2 = GMark.generate(1, seed: 12_345)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      assert MapSet.equal?(triples1, triples2)
    end

    test "generates different output with different seeds" do
      graph1 = GMark.generate(1, seed: 11_111)
      graph2 = GMark.generate(1, seed: 22_222)

      triples1 = MapSet.new(RDF.Graph.triples(graph1))
      triples2 = MapSet.new(RDF.Graph.triples(graph2))

      # Should have some overlap (same schema) but not be identical
      refute MapSet.equal?(triples1, triples2)
    end

    test "generates expected entity types" do
      graph = GMark.generate(1)
      triples = RDF.Graph.triples(graph)

      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      type_triples = Enum.filter(triples, fn {_, p, _} -> p == rdf_type end)

      types = Enum.map(type_triples, fn {_, _, o} -> to_string(o) end)

      # gMark Bib entity types
      assert Enum.any?(types, &String.contains?(&1, "Researcher"))
      assert Enum.any?(types, &String.contains?(&1, "Paper"))
      assert Enum.any?(types, &String.contains?(&1, "Journal"))
      assert Enum.any?(types, &String.contains?(&1, "Conference"))
      assert Enum.any?(types, &String.contains?(&1, "City"))
    end

    test "generates papers with authorship relationships" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"

      # Check for authors relationships
      authors_edges =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), gmark_ns <> "authors")
        end)

      # Should have some authorship edges
      refute Enum.empty?(authors_edges)
    end

    test "generates papers with conference relationships" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"

      # Check for publishedIn relationships
      published_in_edges =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), gmark_ns <> "publishedIn")
        end)

      # Should have publishedIn edges
      refute Enum.empty?(published_in_edges)
    end

    test "generates some papers with journal extensions" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"

      # Check for extendedTo relationships
      extended_to_edges =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), gmark_ns <> "extendedTo")
        end)

      # Should have some extendedTo edges (about 50% of papers)
      refute Enum.empty?(extended_to_edges)
    end

    test "generates conferences with city relationships" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"

      # Check for heldIn relationships
      held_in_edges =
        Enum.filter(triples, fn {_, p, _} ->
          String.contains?(to_string(p), gmark_ns <> "heldIn")
        end)

      # Should have heldIn edges
      refute Enum.empty?(held_in_edges)
    end

    test "cities have fixed count regardless of scale" do
      graph1 = GMark.generate(1, seed: 42)
      graph2 = GMark.generate(10, seed: 42)

      triples1 = RDF.Graph.triples(graph1)
      triples2 = RDF.Graph.triples(graph2)

      gmark_ns = "http://gmark.example.org/"
      city_type = RDF.iri(gmark_ns <> "City")
      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

      cities1 =
        Enum.count(triples1, fn {_s, p, o} ->
          p == rdf_type and o == city_type
        end)

      cities2 =
        Enum.count(triples2, fn {_s, p, o} ->
          p == rdf_type and o == city_type
        end)

      # Cities should have same count (fixed, doesn't scale)
      assert cities1 == cities2
    end
  end

  describe "stream/2" do
    test "generates triples as stream" do
      stream = GMark.stream(1)

      triples = Enum.take(stream, 100)

      assert length(triples) == 100
      assert Enum.all?(triples, fn {s, p, _o} -> is_struct(s) and is_struct(p) end)
    end

    test "stream generates same triples as generate with same seed" do
      graph = GMark.generate(1, seed: 99_999)
      stream = GMark.stream(1, seed: 99_999)

      graph_triples = MapSet.new(RDF.Graph.triples(graph))
      stream_triples = MapSet.new(Enum.to_list(stream))

      assert MapSet.equal?(graph_triples, stream_triples)
    end

    test "stream is memory efficient for large scale" do
      # Stream scale 10 without materializing full graph
      stream = GMark.stream(10)

      # Take first 10000 triples from stream
      triples = Enum.take(stream, 10_000)

      assert length(triples) == 10_000
    end
  end

  describe "estimate_triple_count/1" do
    test "returns reasonable estimate for scale 1" do
      estimate = GMark.estimate_triple_count(1)

      # Should be approximately 10K
      assert estimate > 5_000
      assert estimate < 50_000
    end

    test "estimate scales roughly linearly" do
      est1 = GMark.estimate_triple_count(1)
      est10 = GMark.estimate_triple_count(10)

      # Should be roughly 10x
      assert est10 > est1 * 5
      assert est10 < est1 * 15
    end

    test "estimate is close to actual count" do
      estimate = GMark.estimate_triple_count(1)
      graph = GMark.generate(1, seed: 42)
      actual = RDF.Graph.triple_count(graph)

      # Estimate should be within reasonable range of actual
      ratio = estimate / actual
      assert ratio > 0.3
      assert ratio < 3.0
    end
  end

  describe "namespace/0" do
    test "returns gMark vocabulary namespace" do
      ns = GMark.namespace()

      assert is_binary(ns)
      assert String.contains?(ns, "gmark.example.org")
    end
  end

  describe "RDF validity" do
    test "all triples have valid IRI subjects" do
      graph = GMark.generate(1, seed: 42)

      for {s, _p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = s
        assert String.starts_with?(to_string(s), "http://")
      end
    end

    test "all triples have valid IRI predicates" do
      graph = GMark.generate(1, seed: 42)

      for {_s, p, _o} <- RDF.Graph.triples(graph) do
        assert %RDF.IRI{} = p
        assert String.starts_with?(to_string(p), "http://")
      end
    end

    test "all objects are valid RDF terms" do
      graph = GMark.generate(1, seed: 42)

      for {_s, _p, o} <- RDF.Graph.triples(graph) do
        assert is_struct(o, RDF.IRI) or is_struct(o, RDF.Literal)
      end
    end

    test "no blank nodes in generated data" do
      graph = GMark.generate(1, seed: 42)

      for {s, _p, o} <- RDF.Graph.triples(graph) do
        refute is_struct(s, RDF.BlankNode)
        refute is_struct(o, RDF.BlankNode)
      end
    end

    test "all URIs are well-formed" do
      graph = GMark.generate(1, seed: 42)

      for {s, p, o} <- RDF.Graph.triples(graph) do
        assert valid_uri?(to_string(s))
        assert valid_uri?(to_string(p))

        if is_struct(o, RDF.IRI) do
          assert valid_uri?(to_string(o))
        end
      end
    end

    test "rdf:type triples use correct namespace" do
      graph = GMark.generate(1, seed: 42)
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

    test "literals have appropriate values" do
      graph = GMark.generate(1, seed: 42)

      literals =
        RDF.Graph.triples(graph)
        |> Enum.map(fn {_, _, o} -> o end)
        |> Enum.filter(&is_struct(&1, RDF.Literal))

      # Should have many literals (names, titles, etc.)
      assert length(literals) > 1000

      # Check that literals have values
      for lit <- literals do
        assert RDF.Literal.value(lit) != nil
      end
    end

    test "integer properties have integer values" do
      graph = GMark.generate(1, seed: 42)

      gmark_ns = "http://gmark.example.org/"

      # Check ID properties
      id_triples =
        Enum.filter(RDF.Graph.triples(graph), fn {_s, p, _o} ->
          pred_str = to_string(p)
          String.contains?(pred_str, gmark_ns) and String.contains?(pred_str, "Id")
        end)

      # Should have some ID properties
      refute Enum.empty?(id_triples)

      # Check that IDs are integers
      for {_s, _p, o} <- id_triples do
        assert is_struct(o, RDF.Literal)
        value = RDF.Literal.value(o)
        assert is_integer(value)
        assert value > 0
      end
    end
  end

  describe "node type distribution" do
    test "has expected node type proportions at scale 1" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"
      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

      # Count each node type
      researcher_type = RDF.iri(gmark_ns <> "Researcher")
      paper_type = RDF.iri(gmark_ns <> "Paper")
      journal_type = RDF.iri(gmark_ns <> "Journal")
      conference_type = RDF.iri(gmark_ns <> "Conference")
      city_type = RDF.iri(gmark_ns <> "City")

      researchers =
        Enum.count(triples, fn {_, p, o} -> p == rdf_type and o == researcher_type end)

      papers = Enum.count(triples, fn {_, p, o} -> p == rdf_type and o == paper_type end)
      journals = Enum.count(triples, fn {_, p, o} -> p == rdf_type and o == journal_type end)

      conferences =
        Enum.count(triples, fn {_, p, o} -> p == rdf_type and o == conference_type end)

      cities = Enum.count(triples, fn {_, p, o} -> p == rdf_type and o == city_type end)

      # Check proportions (approximately)
      # At scale 1: 1000 researchers, 600 papers, 200 journals, 200 conferences, 100 cities
      assert researchers > 900 and researchers < 1100
      assert papers > 500 and papers < 700
      assert journals > 150 and journals < 250
      assert conferences > 150 and conferences < 250
      assert cities > 90 and cities < 110
    end
  end

  describe "gMark-specific features" do
    test "generates authorship edges between papers and researchers" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"
      authors_pred = RDF.iri(gmark_ns <> "authors")

      authorships = Enum.filter(triples, fn {_, p, _} -> p == authors_pred end)

      # Should have authorship edges (approximately papers * avg_authors)
      assert length(authorships) > 1000
      assert length(authorships) < 5000
    end

    test "each paper has at least one author" do
      graph = GMark.generate(1, seed: 42)
      triples = RDF.Graph.triples(graph)

      gmark_ns = "http://gmark.example.org/"
      rdf_type = RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      paper_type = RDF.iri(gmark_ns <> "Paper")
      authors_pred = RDF.iri(gmark_ns <> "authors")

      # Get all papers
      papers =
        Enum.filter(triples, fn {_, p, o} ->
          p == rdf_type and o == paper_type
        end)
        |> Enum.map(fn {s, _, _} -> s end)

      # Check each paper has at least one author
      Enum.each(papers, fn paper ->
        authors =
          Enum.count(triples, fn {s, p, _} ->
            s == paper and p == authors_pred
          end)

        assert authors >= 1
      end)
    end
  end

  # Helper for URI validation
  defp valid_uri?(uri) do
    String.starts_with?(uri, "http://") or String.starts_with?(uri, "https://")
  end
end
