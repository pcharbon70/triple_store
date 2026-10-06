%{
  generator_source_id: "ldbc-spb-implementation-v2.0.2",
  generator_pin: "ce6323c0936306729408233dc70d26f2389b34c6",
  license: "Apache-2.0",
  notice: "NOTICE.txt",
  reference_datasets: [
    "datasets_and_queries/datasets/UK-Parliament-Identifiers-People-8.ttl",
    "datasets_and_queries/datasets/english-football-competitions-1.ttl",
    "datasets_and_queries/datasets/english-football-teams-2.ttl",
    "datasets_and_queries/datasets/formula1-competitions-8.ttl",
    "datasets_and_queries/datasets/formula1-teams-3.ttl",
    "datasets_and_queries/datasets/international-football-competitions-3.ttl",
    "datasets_and_queries/datasets/international-football-teams-2.ttl",
    "datasets_and_queries/datasets/scottish-football-competitions-1.ttl",
    "datasets_and_queries/datasets/scottish-football-teams-2.ttl"
  ],
  ontologies: [
    "datasets_and_queries/ontologies"
  ],
  rule_configuration: "datasets_and_queries/RdfsRules-optimized-spb.pie",
  generator_definitions: [
    "datasets_and_queries/definitions.properties-basic",
    "datasets_and_queries/definitions.properties-advanced"
  ],
  scale_settings: %{
    smoke: %{dataset_size: 7, seed: 42, format: "N-Quads"},
    external: %{format: "N-Quads", generated_triples_per_file: 5_000_000}
  },
  editorial_inputs: [
    "datasets_and_queries/sparql/editorial/insert.txt",
    "datasets_and_queries/sparql/editorial/update.txt",
    "datasets_and_queries/sparql/editorial/delete.txt"
  ],
  optional_inputs: %{
    text: ["datasets_and_queries/datasets/entities_prefLabels.zip"],
    geospatial: [
      "datasets_and_queries/datasets/geonames_europe.zip",
      "datasets_and_queries/datasets/geonames_sameAs_links.zip"
    ]
  },
  graphs: %{
    ontology: "urn:ldbc:spb:graph:ontology",
    reference: "urn:ldbc:spb:graph:reference",
    creative_works: "urn:ldbc:spb:graph:creative-works"
  }
}
