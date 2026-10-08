[
  %{
    id: "ldbc/spb/aggregation-01@v2.0.2",
    suite: :spb,
    profile_id: "spb-smoke-v2.0.2",
    catalog_id: "ldbc/spb/aggregation-01@v2.0.2",
    upstream_id: "query1",
    kind: :read,
    parameter_schema: [],
    result_schema: [%{name: "solutions", type: :rdf_term}],
    ordering: %{mode: :unordered},
    limit: nil,
    timeout_class: :standard,
    tags: [:representative, :smoke],
    strategy: {:sparql, "SELECT ?solutions WHERE { ?solutions ?p ?o } LIMIT 10"},
    source: %{
      source_id: "spb-2.0.2",
      path: "datasets_and_queries/sparql/advanced/aggregation_standard/query1.txt",
      checksum: "ce6323c0936306729408233dc70d26f2389b34c6"
    },
    transformation_version: "triplestore-spb-smoke-v1"
  },
  %{
    id: "ldbc/snb-bi/read-01@v1.0.3",
    suite: :snb_bi,
    profile_id: "snb-bi-smoke-v1.0.3",
    catalog_id: "ldbc/snb-bi/read-01@v1.0.3",
    upstream_id: "BI 1",
    kind: :read,
    parameter_schema: [%{name: "datetime", type: :timestamp, required: true}],
    result_schema: [%{name: "message", type: :rdf_term}],
    ordering: %{mode: :unordered},
    limit: 10,
    timeout_class: :standard,
    tags: [:representative, :smoke],
    strategy: {:sparql, "SELECT ?message WHERE { ?message ?p $datetime } LIMIT 10"},
    source: %{
      source_id: "snb-specification-2.2.4",
      path: "snb-bi/query-1.md",
      checksum: "5f7956e07a214373c363b371a3b88bc83ddcd118"
    },
    transformation_version: "triplestore-snb-bi-smoke-v1"
  },
  %{
    id: "ldbc/snb-interactive/complex-read-01@v1.2.0",
    suite: :snb_interactive,
    profile_id: "snb-interactive-smoke-v1.2.0",
    catalog_id: "ldbc/snb-interactive/complex-read-01@v1.2.0",
    upstream_id: "Interactive Complex Read 1",
    kind: :read,
    parameter_schema: [
      %{name: "personId", type: :id, required: true},
      %{name: "firstName", type: :string, required: true}
    ],
    result_schema: [%{name: "otherPerson", type: :rdf_term}],
    ordering: %{mode: :ordered, keys: [{"otherPerson", :asc}]},
    limit: 20,
    timeout_class: :standard,
    tags: [:representative, :smoke],
    strategy:
      {:sparql,
       "SELECT ?otherPerson WHERE { ?otherPerson ?p $firstName . FILTER($personId >= 0) } ORDER BY ?otherPerson LIMIT 20"},
    source: %{
      source_id: "snb-interactive-v1-driver-1.2.0",
      path: "src/main/java/com/ldbc/driver/workloads/ldbc/snb/interactive/LdbcQuery1.java",
      checksum: "4cd13735f964406ad34f34ccd5bef4d6e6c284d0"
    },
    transformation_version: "triplestore-snb-interactive-smoke-v1"
  }
]
