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
        entities: 3,
        graphs: 1,
        properties: 24,
        relationships: 3,
        statements: 42
      },
      identities: %{
        present_entities: [
          entity.("Forum", "20"),
          entity.("Person", "1"),
          entity.("Person", "2")
        ],
        absent_entities: [entity.("Person", "3")],
        present_relationships: [
          {entity.("Person", "1"), predicate.("knows"), entity.("Person", "2"),
           "#{base}graph/snb_bi/initial"}
        ]
      },
      facets: %{
        entities: %{"Forum" => 1, "Person" => 2},
        graphs: %{"#{base}graph/snb_bi/initial" => 42},
        properties: %{"firstName" => 2, "title" => 1},
        relationships: %{"hasMember" => 2, "knows" => 1}
      },
      state_checksum: "sha256:4e699f53e569540a0c9be3d5524fa6763a02863d5467c44ac7c1c0a72e5d0385"
    },
    after_person_batch: %{
      batch_sequence: %{first: 1, last: 1},
      counts: %{
        entities: 4,
        graphs: 2,
        properties: 34,
        relationships: 4,
        statements: 54
      },
      identities: %{
        present_entities: [entity.("Person", "3")],
        present_relationships: [
          {entity.("Person", "3"), predicate.("isLocatedIn"), entity.("Place", "0"),
           "#{base}graph/snb_bi/updates"}
        ]
      },
      facets: %{
        entities: %{"Forum" => 1, "Person" => 3},
        graphs: %{
          "#{base}graph/snb_bi/initial" => 42,
          "#{base}graph/snb_bi/updates" => 12
        },
        properties: %{"firstName" => 3, "title" => 1},
        relationships: %{"hasMember" => 2, "isLocatedIn" => 1, "knows" => 1}
      },
      state_checksum: "sha256:d9a77ba2a8cde0a72d0be2abcddd0be578fdcad2910a50978176fa9c5460d73d"
    }
  }
}
