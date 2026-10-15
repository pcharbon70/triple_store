field_types = %{
  "personId" => :identifier,
  "postId" => :identifier,
  "commentId" => :identifier,
  "forumId" => :identifier,
  "moderatorId" => :identifier,
  "authorPersonId" => :identifier,
  "countryId" => :identifier,
  "person1Id" => :identifier,
  "person2Id" => :identifier,
  "replyToPostId" => :identifier,
  "replyToCommentId" => :identifier,
  "cityId" => :identifier,
  "personFirstName" => :string,
  "personLastName" => :string,
  "gender" => :string,
  "birthday" => :date,
  "creationDate" => :timestamp,
  "locationIP" => :string,
  "browserUsed" => :string,
  "forumTitle" => :string,
  "imageFile" => :string,
  "language" => :string,
  "content" => :string,
  "length" => :integer,
  "languages" => {:list, :string},
  "emails" => {:list, :string},
  "tagIds" => {:list, :identifier},
  "studyAt" => {:list, {:pair, "organisationId", "classYear"}},
  "workAt" => {:list, {:pair, "organisationId", "workFrom"}}
}

schema = fn id, operation, identity, required, optional, dependencies, effect ->
  fields = required ++ optional

  %{
    id: id,
    operation: operation,
    identity: identity,
    required_fields: required,
    optional_fields: optional,
    field_types: Map.take(field_types, fields),
    dependencies: dependencies,
    effect: effect
  }
end

record_types = [
  schema.(
    "insert-01",
    :insert,
    ["personId"],
    ~w(personId personFirstName personLastName gender birthday creationDate locationIP browserUsed cityId languages emails tagIds studyAt workAt),
    [],
    %{
      "cityId" => "Place",
      "tagIds" => "Tag",
      "studyAt" => "Organisation",
      "workAt" => "Organisation"
    },
    {:entity, "Person"}
  ),
  schema.(
    "insert-02",
    :insert,
    ["personId", "postId"],
    ~w(personId postId creationDate),
    [],
    %{"personId" => "Person", "postId" => "Post"},
    {:relationship, "likes", "Person", "Post"}
  ),
  schema.(
    "insert-03",
    :insert,
    ["personId", "commentId"],
    ~w(personId commentId creationDate),
    [],
    %{"personId" => "Person", "commentId" => "Comment"},
    {:relationship, "likes", "Person", "Comment"}
  ),
  schema.(
    "insert-04",
    :insert,
    ["forumId"],
    ~w(forumId forumTitle creationDate moderatorId tagIds),
    [],
    %{"moderatorId" => "Person", "tagIds" => "Tag"},
    {:entity, "Forum"}
  ),
  schema.(
    "insert-05",
    :insert,
    ["forumId", "personId"],
    ~w(forumId personId creationDate),
    [],
    %{"forumId" => "Forum", "personId" => "Person"},
    {:relationship, "hasMember", "Forum", "Person"}
  ),
  schema.(
    "insert-06",
    :insert,
    ["postId"],
    ~w(postId imageFile creationDate locationIP browserUsed language content length authorPersonId forumId countryId tagIds),
    [],
    %{
      "authorPersonId" => "Person",
      "forumId" => "Forum",
      "countryId" => "Place",
      "tagIds" => "Tag"
    },
    {:entity, "Post"}
  ),
  schema.(
    "insert-07",
    :insert,
    ["commentId"],
    ~w(commentId creationDate locationIP browserUsed content length authorPersonId countryId tagIds),
    ~w(replyToPostId replyToCommentId),
    %{
      "authorPersonId" => "Person",
      "countryId" => "Place",
      "tagIds" => "Tag",
      "replyToPostId" => "Post",
      "replyToCommentId" => "Comment"
    },
    {:entity, "Comment"}
  ),
  schema.(
    "insert-08",
    :insert,
    ["person1Id", "person2Id"],
    ~w(person1Id person2Id creationDate),
    [],
    %{"person1Id" => "Person", "person2Id" => "Person"},
    {:relationship, "knows", "Person", "Person"}
  ),
  schema.("delete-01", :delete, ["personId"], ~w(personId), [], %{}, {:deep_entity, "Person"}),
  schema.(
    "delete-02",
    :delete,
    ["personId", "postId"],
    ~w(personId postId),
    [],
    %{},
    {:relationship, "likes", "Person", "Post"}
  ),
  schema.(
    "delete-03",
    :delete,
    ["personId", "commentId"],
    ~w(personId commentId),
    [],
    %{},
    {:relationship, "likes", "Person", "Comment"}
  ),
  schema.("delete-04", :delete, ["forumId"], ~w(forumId), [], %{}, {:deep_entity, "Forum"}),
  schema.(
    "delete-05",
    :delete,
    ["forumId", "personId"],
    ~w(forumId personId),
    [],
    %{},
    {:relationship, "hasMember", "Forum", "Person"}
  ),
  schema.("delete-06", :delete, ["postId"], ~w(postId), [], %{}, {:deep_entity, "Post"}),
  schema.("delete-07", :delete, ["commentId"], ~w(commentId), [], %{}, {:deep_entity, "Comment"}),
  schema.(
    "delete-08",
    :delete,
    ["person1Id", "person2Id"],
    ~w(person1Id person2Id),
    [],
    %{},
    {:relationship, "knows", "Person", "Person"}
  )
]

fixtures = [
  %{
    type: "insert-01",
    fields: %{
      "personId" => "1001",
      "personFirstName" => "Ada",
      "personLastName" => "Lovelace",
      "gender" => "female",
      "birthday" => "1815-12-10",
      "creationDate" => 1_262_304_000_000,
      "locationIP" => "192.0.2.1",
      "browserUsed" => "Firefox",
      "cityId" => "9001",
      "languages" => ["en"],
      "emails" => ["ada@example.test"],
      "tagIds" => ["8001"],
      "studyAt" => [%{"organisationId" => "7001", "classYear" => 1835}],
      "workAt" => [%{"organisationId" => "7002", "workFrom" => 1840}]
    }
  },
  %{
    type: "insert-02",
    fields: %{"personId" => "1001", "postId" => "6001", "creationDate" => 1_262_304_001_000}
  },
  %{
    type: "insert-03",
    fields: %{"personId" => "1001", "commentId" => "6101", "creationDate" => 1_262_304_002_000}
  },
  %{
    type: "insert-04",
    fields: %{
      "forumId" => "5001",
      "forumTitle" => "Analytical Engines",
      "creationDate" => 1_262_304_003_000,
      "moderatorId" => "1001",
      "tagIds" => ["8001"]
    }
  },
  %{
    type: "insert-05",
    fields: %{"forumId" => "5001", "personId" => "1001", "creationDate" => 1_262_304_004_000}
  },
  %{
    type: "insert-06",
    fields: %{
      "postId" => "6001",
      "imageFile" => "",
      "creationDate" => 1_262_304_005_000,
      "locationIP" => "192.0.2.1",
      "browserUsed" => "Firefox",
      "language" => "en",
      "content" => "post",
      "length" => 4,
      "authorPersonId" => "1001",
      "forumId" => "5001",
      "countryId" => "9001",
      "tagIds" => ["8001"]
    }
  },
  %{
    type: "insert-07",
    fields: %{
      "commentId" => "6101",
      "creationDate" => 1_262_304_006_000,
      "locationIP" => "192.0.2.1",
      "browserUsed" => "Firefox",
      "content" => "reply",
      "length" => 5,
      "authorPersonId" => "1001",
      "countryId" => "9001",
      "replyToPostId" => "6001",
      "tagIds" => ["8001"]
    }
  },
  %{
    type: "insert-08",
    fields: %{"person1Id" => "1001", "person2Id" => "1002", "creationDate" => 1_262_304_007_000}
  },
  %{type: "delete-01", fields: %{"personId" => "1001"}},
  %{type: "delete-02", fields: %{"personId" => "1001", "postId" => "6001"}},
  %{type: "delete-03", fields: %{"personId" => "1001", "commentId" => "6101"}},
  %{type: "delete-04", fields: %{"forumId" => "5001"}},
  %{type: "delete-05", fields: %{"forumId" => "5001", "personId" => "1001"}},
  %{type: "delete-06", fields: %{"postId" => "6001"}},
  %{type: "delete-07", fields: %{"commentId" => "6101"}},
  %{type: "delete-08", fields: %{"person1Id" => "1001", "person2Id" => "1002"}}
]

%{
  schema_version: 1,
  catalog_id: "ldbc-snb-bi-updates-v1.0.3",
  operation_catalog_version: "v1.0.3",
  generator: %{
    source_id: "snb-bi-datagen-0.5.1",
    version: "v0.5.1",
    pin: "2459f4e45834c78902a50511fc64a05c48dd4029"
  },
  mapping_version: "snb-rdf-v1",
  record_types: record_types,
  fixture_records: fixtures
}
