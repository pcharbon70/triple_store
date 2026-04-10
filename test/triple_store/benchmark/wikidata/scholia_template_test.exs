defmodule TripleStore.Benchmark.Wikidata.ScholiaTemplateTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.ScholiaTemplate
  alias TripleStore.SPARQL.Parser

  test "normalizes template rewrites and validates template metadata" do
    assert {:ok, template} =
             ScholiaTemplate.new(
               template_id: "scholia-sample-template",
               class_id: :human,
               class_label: "Human",
               name: "Sample template",
               description: "A simple Scholia-style template",
               shape: :path_bgp,
               raw_sparql: """
               PREFIX wikibase: <http://wikiba.se/ontology#>
               PREFIX bd: <http://www.bigdata.com/rdf#>

               SELECT ?work ?workLabel WHERE {
                 ?work ?p ?o .
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
               }
               LIMIT %limit%
               """,
               params: [:entity, :class, :language, :limit],
               defaults: %{language: "en", limit: "25"},
               feature_tags: [:template, :label_service],
               complexity: :medium,
               stress_points: [:fanout],
               rewrites: [
                 {:rewrite_label_service,
                  [%{subject: "?work", label: "?workLabel", language: "en"}]}
               ],
               variant_support: [:raw, :count_only],
               source: %{kind: :scholia_template, location: "scholia:test:sample"}
             )

    refute String.contains?(template.sparql, "SERVICE wikibase:label")
    assert String.contains?(template.sparql, "rdfs:label")
    assert {:ok, _ast} = Parser.parse(String.replace(template.sparql, "%limit%", "25"))
  end
end
