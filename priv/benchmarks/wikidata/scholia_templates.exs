[
  %{
    template_id: "scholia-author-works",
    class_id: :human,
    class_label: "Human",
    name: "Author works",
    description: "Find works authored by the selected entity.",
    shape: :path_bgp,
    raw_sparql: """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX bd: <http://www.bigdata.com/rdf#>

    SELECT ?work ?workLabel WHERE {
      ?work wdt:P50 <%entity%> .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],%language%". }
    }
    LIMIT %limit%
    """,
    params: [:entity, :class, :language, :limit],
    defaults: %{language: "en", limit: "200"},
    feature_tags: [:template, :label_service, :entity_anchor, :join],
    complexity: :medium,
    stress_points: [:fanout, :label_lookup],
    rewrites: [
      {:rewrite_label_service, [%{subject: "?work", label: "?workLabel", language: "en"}]}
    ],
    variant_support: [:raw, :count_only, :distinct_only],
    source: %{
      kind: :scholia_template,
      location: "scholia:author:works",
      label: "Scholia author works"
    }
  },
  %{
    template_id: "scholia-work-citations",
    class_id: :scholarly_article,
    class_label: "Scholarly Article",
    name: "Article citations",
    description: "Find works citing the selected article.",
    shape: :path_bgp,
    raw_sparql: """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX hint: <http://www.bigdata.com/queryHints#>

    SELECT ?citing ?citingLabel WHERE {
      hint:Query hint:optimizer "None" .
      ?citing wdt:P2860 <%entity%> .
      OPTIONAL {
        ?citing rdfs:label ?citingLabel .
        FILTER(LANG(?citingLabel) = "%language%")
      }
    }
    LIMIT %limit%
    """,
    params: [:entity, :class, :language, :limit],
    defaults: %{language: "en", limit: "100"},
    feature_tags: [:template, :citation, :optional, :entity_anchor],
    complexity: :complex,
    stress_points: [:optimizer_sensitivity, :join_selectivity],
    rewrites: [:strip_blazegraph_hints],
    variant_support: [:raw, :count_only],
    source: %{
      kind: :scholia_template,
      location: "scholia:work:citations",
      label: "Scholia work citations"
    }
  },
  %{
    template_id: "scholia-organization-members",
    class_id: :organization,
    class_label: "Organization",
    name: "Organization members",
    description: "Find people affiliated with the selected organization.",
    shape: :multi_bgp,
    raw_sparql: """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX bd: <http://www.bigdata.com/rdf#>

    SELECT ?person ?personLabel WHERE {
      ?person wdt:P31 <%class%> ;
              wdt:P108 <%entity%> .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],%language%". }
    }
    LIMIT %limit%
    """,
    params: [:entity, :class, :language, :limit],
    defaults: %{
      language: "en",
      limit: "150",
      class: "http://www.wikidata.org/entity/Q5"
    },
    feature_tags: [:template, :label_service, :entity_anchor, :organization_affiliation],
    complexity: :medium,
    stress_points: [:fanout, :star_join],
    rewrites: [
      {:rewrite_label_service, [%{subject: "?person", label: "?personLabel", language: "en"}]}
    ],
    variant_support: [:raw, :distinct_only],
    source: %{
      kind: :scholia_template,
      location: "scholia:organization:members",
      label: "Scholia organization members"
    }
  },
  %{
    template_id: "scholia-field-members",
    class_id: :field_of_work,
    class_label: "Field Of Work",
    name: "Researchers in field",
    description: "Find researchers associated with the selected field of work.",
    shape: :optional,
    raw_sparql: """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?person ?personLabel WHERE {
      ?person wdt:P31 <%class%> ;
              wdt:P101 <%entity%> .
      OPTIONAL {
        ?person rdfs:label ?personLabel .
        FILTER(LANG(?personLabel) = "%language%")
      }
    }
    LIMIT %limit%
    """,
    params: [:entity, :class, :language, :limit],
    defaults: %{
      language: "en",
      limit: "100",
      class: "http://www.wikidata.org/entity/Q5"
    },
    feature_tags: [:template, :optional, :entity_anchor, :field_of_work],
    complexity: :medium,
    stress_points: [:optional_join],
    rewrites: [],
    variant_support: [:raw, :count_only, :distinct_only],
    source: %{
      kind: :scholia_template,
      location: "scholia:field:members",
      label: "Scholia field members"
    }
  }
]
