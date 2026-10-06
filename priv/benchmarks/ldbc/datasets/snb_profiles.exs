%{
  snb_bi_smoke: %{
    id: "snb-bi-smoke",
    suite: :snb_bi,
    profile: :snb_bi_smoke,
    scale_factor: "smoke",
    delimiter: "|",
    generator_kind: :spark,
    generator_source_id: "snb-bi-datagen-0.5.1",
    generator_pin: "2459f4e45834c78902a50511fc64a05c48dd4029",
    generator_settings: %{mode: "bi", serializer: "csv", epoch_millis: true},
    mode: "bi",
    serializer: "csv",
    seed: 42,
    license: "Apache-2.0",
    entities: [
      %{path: "entities/person.csv", type: "Person", id_column: "id"},
      %{path: "entities/forum.csv", type: "Forum", id_column: "id"}
    ],
    relationships: [
      %{
        path: "relationships/forum_hasMember_person.csv",
        type: "hasMember",
        from_type: "Forum",
        from_column: "Forum.id",
        to_type: "Person",
        to_column: "Person.id",
        properties: ["joinDate"]
      },
      %{
        path: "relationships/person_knows_person.csv",
        type: "knows",
        from_type: "Person",
        from_column: "Person1.id",
        to_type: "Person",
        to_column: "Person2.id",
        properties: ["creationDate"]
      }
    ],
    updates: [
      %{
        path: "updates/person-batch.csv",
        role: :bi_update_batch,
        entity_type: "Person",
        id_column: "id",
        sequence_column: "sequence",
        operation_column: "operation"
      }
    ],
    parameters: [%{path: "parameters/bi-read-1.csv"}]
  },
  snb_interactive_smoke: %{
    id: "snb-interactive-smoke",
    suite: :snb_interactive,
    profile: :snb_interactive_smoke,
    scale_factor: "smoke",
    delimiter: "|",
    generator_kind: :hadoop,
    generator_source_id: "snb-interactive-v1-datagen-1.0.0",
    generator_pin: "37d35f40f5023fcf1afd3b6d0984f71c202f4bca",
    generator_settings: %{mode: "interactive", serializer: "csv_basic", update_partitions: 1},
    mode: "interactive",
    serializer: "csv_basic",
    seed: 42,
    license: "Apache-2.0",
    entities: [
      %{path: "entities/person.csv", type: "Person", id_column: "id"},
      %{path: "entities/post.csv", type: "Post", id_column: "id"}
    ],
    relationships: [
      %{
        path: "relationships/post_hasCreator_person.csv",
        type: "hasCreator",
        from_type: "Post",
        from_column: "Post.id",
        to_type: "Person",
        to_column: "Person.id",
        properties: []
      },
      %{
        path: "relationships/person_knows_person.csv",
        type: "knows",
        from_type: "Person",
        from_column: "Person1.id",
        to_type: "Person",
        to_column: "Person2.id",
        properties: ["creationDate"]
      }
    ],
    updates: [
      %{
        path: "updates/person-stream.csv",
        role: :interactive_update_stream,
        entity_type: "Person",
        id_column: "id",
        sequence_column: "sequence",
        operation_column: "operation"
      }
    ],
    parameters: [%{path: "parameters/interactive-complex-1.csv"}]
  }
}
