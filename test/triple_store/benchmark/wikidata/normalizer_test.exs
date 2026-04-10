defmodule TripleStore.Benchmark.Wikidata.NormalizerTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.Normalizer
  alias TripleStore.SPARQL.Parser

  test "caps existing LIMIT clauses when the tier policy is smaller" do
    sparql = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1000"

    assert Normalizer.apply_limit_policy(sparql, {:cap, 25}) ==
             "SELECT ?s WHERE { ?s ?p ?o } LIMIT 25"
  end

  test "adds a default LIMIT when the query has none" do
    sparql = "SELECT ?s WHERE { ?s ?p ?o }"

    assert Normalizer.apply_limit_policy(sparql, {:default, 50}) ==
             "SELECT ?s WHERE { ?s ?p ?o }\nLIMIT 50"
  end

  test "strips Blazegraph-specific hints and keeps the query parseable" do
    sparql = """
    PREFIX hint: <http://www.bigdata.com/queryHints#>

    SELECT ?s WHERE {
      hint:Query hint:optimizer "None" .
      ?s ?p ?o .
    }
    LIMIT 10
    """

    normalized = Normalizer.normalize(sparql, rewrites: [:strip_blazegraph_hints])

    refute String.contains?(normalized, "hint:Query")
    refute String.contains?(normalized, "PREFIX hint:")
    assert {:ok, _ast} = Parser.parse(normalized)
  end

  test "rewrites label service blocks into explicit label triples" do
    sparql = """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX bd: <http://www.bigdata.com/rdf#>

    SELECT ?person ?personLabel WHERE {
      ?person ?p wd:Q5 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    """

    normalized =
      Normalizer.normalize(sparql,
        rewrites: [
          {:rewrite_label_service, [%{subject: "?person", label: "?personLabel", language: "en"}]}
        ]
      )

    refute String.contains?(normalized, "SERVICE wikibase:label")
    assert String.contains?(normalized, "rdfs:label")
  end

  test "builds count-only and distinct-only execution variants" do
    sparql = "SELECT ?person WHERE { ?person ?p ?o } ORDER BY ?person LIMIT 100"

    assert Normalizer.apply_execution_variant(sparql, :count_only) ==
             "SELECT (COUNT(*) AS ?count) WHERE { ?person ?p ?o }"

    assert Normalizer.apply_execution_variant(
             "SELECT ?person WHERE { ?person ?p ?o }",
             :distinct_only
           ) ==
             "SELECT DISTINCT ?person WHERE { ?person ?p ?o }"
  end
end
