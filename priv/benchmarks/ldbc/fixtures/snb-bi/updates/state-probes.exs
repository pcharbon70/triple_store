base = "https://ldbcouncil.org/snb/"
entity = &"#{base}entity/#{&1}/#{&2}"
predicate = &"#{base}ontology/#{&1}"

%{
  schema_version: 1,
  mapping_version: "snb-rdf-v1",
  checkpoints: %{
    initial: %{
      batch_sequence: nil,
      counts: %{
        entities: 8,
        graphs: 1,
        properties: 39,
        relationships: 3,
        statements: 62
      },
      identities: %{
        present_entities: [
          entity.("Forum", "20"),
          entity.("Organisation", "7001"),
          entity.("Organisation", "7002"),
          entity.("Person", "1"),
          entity.("Person", "2"),
          entity.("Person", "1002"),
          entity.("Place", "9001"),
          entity.("Tag", "8001")
        ],
        absent_entities: [entity.("Person", "1001")],
        present_relationships: [
          {entity.("Person", "1"), predicate.("knows"), entity.("Person", "2"),
           "#{base}graph/snb_bi/initial"}
        ]
      },
      facets: %{
        entities: %{
          "Forum" => 1,
          "Organisation" => 2,
          "Person" => 3,
          "Place" => 1,
          "Tag" => 1
        },
        graphs: %{"#{base}graph/snb_bi/initial" => 62},
        properties: %{"firstName" => 3, "name" => 4, "title" => 1},
        relationships: %{"hasMember" => 2, "knows" => 1}
      },
      state_checksum: "sha256:4681fa0b234d737fa2bfef89215295f433382bf6349973784d48138dae6cc246"
    },
    after_update_batch: %{
      batch_sequence: %{first: 1, last: 16},
      counts: %{
        entities: 8,
        graphs: 1,
        properties: 39,
        relationships: 3,
        statements: 62
      },
      identities: %{
        absent_entities: [entity.("Person", "1001")],
        present_relationships: [
          {entity.("Person", "1"), predicate.("knows"), entity.("Person", "2"),
           "#{base}graph/snb_bi/initial"}
        ]
      },
      facets: %{
        entities: %{
          "Forum" => 1,
          "Organisation" => 2,
          "Person" => 3,
          "Place" => 1,
          "Tag" => 1
        },
        graphs: %{"#{base}graph/snb_bi/initial" => 62},
        properties: %{"firstName" => 3, "name" => 4, "title" => 1},
        relationships: %{"hasMember" => 2, "knows" => 1}
      },
      state_checksum: "sha256:4681fa0b234d737fa2bfef89215295f433382bf6349973784d48138dae6cc246"
    }
  }
}
