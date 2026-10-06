# Generated from the pinned upstream operation metadata; edit through a source-pin review.
%{
  id: "ldbc-snb-bi-v1.0.3",
  benchmark: :snb_bi,
  version: "v1.0.3",
  source_id: "snb-specification-2.2.4",
  expected_operation_ids: [
    "ldbc/snb-bi/read-01@v1.0.3",
    "ldbc/snb-bi/read-02@v1.0.3",
    "ldbc/snb-bi/read-03@v1.0.3",
    "ldbc/snb-bi/read-04@v1.0.3",
    "ldbc/snb-bi/read-05@v1.0.3",
    "ldbc/snb-bi/read-06@v1.0.3",
    "ldbc/snb-bi/read-07@v1.0.3",
    "ldbc/snb-bi/read-08@v1.0.3",
    "ldbc/snb-bi/read-09@v1.0.3",
    "ldbc/snb-bi/read-10@v1.0.3",
    "ldbc/snb-bi/read-11@v1.0.3",
    "ldbc/snb-bi/read-12@v1.0.3",
    "ldbc/snb-bi/read-13@v1.0.3",
    "ldbc/snb-bi/read-14@v1.0.3",
    "ldbc/snb-bi/read-15@v1.0.3",
    "ldbc/snb-bi/read-16@v1.0.3",
    "ldbc/snb-bi/read-17@v1.0.3",
    "ldbc/snb-bi/read-18@v1.0.3",
    "ldbc/snb-bi/read-19@v1.0.3",
    "ldbc/snb-bi/read-20@v1.0.3",
    "ldbc/snb-bi/update-batch@v1.0.3"
  ],
  operations: [
    %{
      id: "ldbc/snb-bi/read-01@v1.0.3",
      upstream_id: "BI 1",
      family: :read,
      availability: :mandatory,
      title: "Posting summary",
      parameters: [
        %{
          name: "datetime",
          type: "DateTime"
        }
      ],
      result: [
        %{
          name: "year",
          type: "32-bit Integer"
        },
        %{
          name: "isComment",
          type: "Boolean",
          category: "meta"
        },
        %{
          name: "lengthCategory",
          type: "32-bit Integer",
          category: "calculated"
        },
        %{
          name: "messageCount",
          type: "64-bit Integer",
          category: "aggregated"
        },
        %{
          name: "averageMessageLength",
          type: "32-bit Float",
          category: "aggregated"
        },
        %{
          name: "sumMessageLength",
          type: "64-bit Integer",
          category: "aggregated"
        },
        %{
          name: "percentageOfMessages",
          type: "32-bit Float",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "year",
          direction: "desc"
        },
        %{
          name: "isComment",
          direction: "asc"
        },
        %{
          name: "lengthCategory",
          direction: "asc"
        }
      ],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "3.2",
        "4.1",
        "4.2",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-01.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-02@v1.0.3",
      upstream_id: "BI 2",
      family: :read,
      availability: :mandatory,
      title: "Tag evolution",
      parameters: [
        %{
          name: "date",
          type: "Date"
        },
        %{
          name: "tagClass",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "tag.name",
          type: "Long String"
        },
        %{
          name: "countWindow1",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "countWindow2",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "diff",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "diff",
          direction: "desc"
        },
        %{
          name: "tag.name",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "a",
        "b",
        "m"
      ],
      choke_points: [
        "2.4",
        "3.1",
        "3.2",
        "4.1",
        "4.2",
        "4.3",
        "5.3",
        "6.1",
        "8.2",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-02.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-03@v1.0.3",
      upstream_id: "BI 3",
      family: :read,
      availability: :mandatory,
      title: "Popular topics in a country",
      parameters: [
        %{
          name: "tagClass",
          type: "Long String"
        },
        %{
          name: "country",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "forum.id",
          type: "ID"
        },
        %{
          name: "forum.title",
          type: "Long String"
        },
        %{
          name: "forum.creationDate",
          type: "DateTime"
        },
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "messageCount",
          category: "aggregated",
          type: "32-bit Integer"
        }
      ],
      ordering: [
        %{
          name: "messageCount",
          direction: "desc"
        },
        %{
          name: "forum.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "1.1",
        "1.2",
        "1.3",
        "2.1",
        "2.2",
        "2.4",
        "3.3",
        "8.2"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-03.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-04@v1.0.3",
      upstream_id: "BI 4",
      family: :read,
      availability: :mandatory,
      title: "Top message creators by country",
      parameters: [
        %{
          name: "date",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "person.firstName",
          type: "String"
        },
        %{
          name: "person.lastName",
          type: "String"
        },
        %{
          name: "person.creationDate",
          type: "DateTime"
        },
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "messageCount",
          direction: "desc"
        },
        %{
          name: "person.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "1.3",
        "2.1",
        "2.2",
        "2.3",
        "2.4",
        "3.3",
        "5.3",
        "6.1",
        "8.2",
        "8.4"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-04.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-05@v1.0.3",
      upstream_id: "BI 5",
      family: :read,
      availability: :mandatory,
      title: "Most active posters of a given topic",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "replyCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "likeCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "score",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "score",
          direction: "desc"
        },
        %{
          name: "person.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "2.3",
        "2.6",
        "8.2"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-05.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-06@v1.0.3",
      upstream_id: "BI 6",
      family: :read,
      availability: :mandatory,
      title: "Most authoritative users on a given topic",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "authorityScore",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "authorityScore",
          direction: "desc"
        },
        %{
          name: "person1.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "2.3",
        "2.6",
        "3.3",
        "6.1",
        "8.2"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-06.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-07@v1.0.3",
      upstream_id: "BI 7",
      family: :read,
      availability: :mandatory,
      title: "Related topics",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "relatedTag.name",
          type: "Long String"
        },
        %{
          name: "count",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "count",
          direction: "desc"
        },
        %{
          name: "relatedTag.name",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.4",
        "3.3",
        "5.2",
        "8.1"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-07.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-08@v1.0.3",
      upstream_id: "BI 8",
      family: :read,
      availability: :mandatory,
      title: "Central person for a tag",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        },
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "endDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "score",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "friendsScore",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "score + friendsScore",
          direction: "desc"
        },
        %{
          name: "person.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "a",
        "b",
        "m"
      ],
      choke_points: [
        "1.2",
        "2.1",
        "2.3",
        "3.2",
        "5.3",
        "8.2",
        "8.4",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-08.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-09@v1.0.3",
      upstream_id: "BI 9",
      family: :read,
      availability: :mandatory,
      title: "Top thread initiators",
      parameters: [
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "endDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "person.firstName",
          type: "String"
        },
        %{
          name: "person.lastName",
          type: "String"
        },
        %{
          name: "threadCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "messageCount",
          direction: "desc"
        },
        %{
          name: "person.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "2.2",
        "2.3",
        "2.6",
        "3.2",
        "7.2",
        "7.3",
        "7.4",
        "8.1",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-09.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-10@v1.0.3",
      upstream_id: "BI 10",
      family: :read,
      availability: :mandatory,
      title: "Experts in social circle",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "country",
          type: "String"
        },
        %{
          name: "tagClass",
          type: "Long String"
        },
        %{
          name: "minPathDistance",
          type: "32-bit Integer"
        },
        %{
          name: "maxPathDistance",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "expertCandidatePerson.id",
          type: "ID"
        },
        %{
          name: "tag.name",
          type: "Long String"
        },
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "messageCount",
          direction: "desc"
        },
        %{
          name: "tag.name",
          direction: "asc"
        },
        %{
          name: "expertCandidatePerson.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "a",
        "b"
      ],
      choke_points: [
        "1.2",
        "1.3",
        "2.3",
        "2.4",
        "2.6",
        "3.3",
        "5.3",
        "7.1",
        "7.2",
        "7.3",
        "8.1",
        "8.6"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-10.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-11@v1.0.3",
      upstream_id: "BI 11",
      family: :read,
      availability: :mandatory,
      title: "Friend triangles",
      parameters: [
        %{
          name: "country",
          type: "Long String"
        },
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "endDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "count",
          type: "64-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [
        "2.3",
        "2.5",
        "3.2"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-11.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-12@v1.0.3",
      upstream_id: "BI 12",
      family: :read,
      availability: :mandatory,
      title: "How many persons have a given number of messages",
      parameters: [
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "lengthThreshold",
          type: "32-bit Integer"
        },
        %{
          name: "languages",
          type: "\\{String\\}"
        }
      ],
      result: [
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "personCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "personCount",
          direction: "desc"
        },
        %{
          name: "messageCount",
          direction: "desc"
        }
      ],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [
        "1.1",
        "1.2",
        "1.4",
        "2.6",
        "3.2",
        "4.2",
        "4.3",
        "8.1",
        "8.2",
        "8.3",
        "8.4",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-12.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-13@v1.0.3",
      upstream_id: "BI 13",
      family: :read,
      availability: :mandatory,
      title: "Zombies in a country",
      parameters: [
        %{
          name: "country",
          type: "Long String"
        },
        %{
          name: "endDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "zombie.id",
          type: "ID"
        },
        %{
          name: "zombieLikeCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "totalLikeCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "zombieScore",
          type: "32-bit Float",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "zombieScore",
          direction: "desc"
        },
        %{
          name: "zombie.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "default"
      ],
      choke_points: [
        "1.2",
        "2.1",
        "2.3",
        "2.4",
        "2.6",
        "3.2",
        "3.3",
        "4.2",
        "5.1",
        "5.3",
        "8.2",
        "8.4",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-13.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-14@v1.0.3",
      upstream_id: "BI 14",
      family: :read,
      availability: :mandatory,
      title: "International dialog",
      parameters: [
        %{
          name: "country1",
          type: "Long String"
        },
        %{
          name: "country2",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "person2.id",
          type: "ID"
        },
        %{
          name: "city1.name",
          type: "Long String"
        },
        %{
          name: "score",
          type: "32-bit Integer",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "score",
          direction: "desc"
        },
        %{
          name: "person1.id",
          direction: "asc"
        },
        %{
          name: "person2.id",
          direction: "asc"
        }
      ],
      limit: 100,
      variants: [
        "a",
        "b"
      ],
      choke_points: [
        "1.3",
        "1.4",
        "2.1",
        "3.1",
        "3.3",
        "5.1",
        "5.2",
        "5.3",
        "8.3",
        "8.4"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-14.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-15@v1.0.3",
      upstream_id: "BI 15",
      family: :read,
      availability: :mandatory,
      title: "Trusted connection paths through forums created in a given timeframe",
      parameters: [
        %{
          name: "person1Id",
          type: "ID"
        },
        %{
          name: "person2Id",
          type: "ID"
        },
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "endDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "weight",
          category: "calculated",
          type: "32-bit Float"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "a",
        "b"
      ],
      choke_points: [
        "1.2",
        "2.1",
        "2.2",
        "2.4",
        "3.3",
        "5.1",
        "5.3",
        "7.2",
        "7.3",
        "7.6",
        "7.7",
        "8.1",
        "8.2",
        "8.3",
        "8.4",
        "8.5",
        "8.6"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-15.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-16@v1.0.3",
      upstream_id: "BI 16",
      family: :read,
      availability: :mandatory,
      title: "Fake news detection",
      parameters: [
        %{
          name: "tagA",
          type: "Long String"
        },
        %{
          name: "dateA",
          type: "Date"
        },
        %{
          name: "tagB",
          type: "Long String"
        },
        %{
          name: "dateB",
          type: "Date"
        },
        %{
          name: "maxKnowsLimit",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "person.id",
          type: "ID"
        },
        %{
          name: "messageCountA",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "messageCountB",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "messageCountA + \\mbox{messageCountB}",
          direction: "desc"
        },
        %{
          name: "person.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "a",
        "b"
      ],
      choke_points: [
        "5.3",
        "8.4",
        "8.5"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-16.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-17@v1.0.3",
      upstream_id: "BI 17",
      family: :read,
      availability: :mandatory,
      title: "Information propagation analysis",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        },
        %{
          name: "delta",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "messageCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "messageCount",
          direction: "desc"
        },
        %{
          name: "person1.id",
          direction: "asc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [
        "2.1",
        "2.3",
        "2.5",
        "2.6",
        "8.1"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-17.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-18@v1.0.3",
      upstream_id: "BI 18",
      family: :read,
      availability: :mandatory,
      title: "Friend recommendation",
      parameters: [
        %{
          name: "tag",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "person2.id",
          type: "ID"
        },
        %{
          name: "mutualFriendCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "mutualFriendCount",
          direction: "desc"
        },
        %{
          name: "person1.id",
          direction: "asc"
        },
        %{
          name: "person2.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "2.5",
        "2.6",
        "8.1"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-18.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-19@v1.0.3",
      upstream_id: "BI 19",
      family: :read,
      availability: :mandatory,
      title: "Interaction path between cities",
      parameters: [
        %{
          name: "city1Id",
          type: "ID"
        },
        %{
          name: "city2Id",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "person2.id",
          type: "ID"
        },
        %{
          name: "totalWeight",
          type: "32-bit Integer",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "person1.id",
          direction: "asc"
        },
        %{
          name: "person2.id",
          direction: "asc"
        }
      ],
      limit: nil,
      variants: [
        "a",
        "b"
      ],
      choke_points: [
        "3.3",
        "7.6",
        "7.7",
        "8.4",
        "8.6"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-19.yaml"
    },
    %{
      id: "ldbc/snb-bi/read-20@v1.0.3",
      upstream_id: "BI 20",
      family: :read,
      availability: :mandatory,
      title: "Recruitment",
      parameters: [
        %{
          name: "company",
          type: "Long String"
        },
        %{
          name: "person2Id",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "person1.id",
          type: "ID"
        },
        %{
          name: "totalWeight",
          type: "32-bit Integer",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "totalWeight",
          direction: "asc"
        },
        %{
          name: "person1.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "a",
        "b",
        "m"
      ],
      choke_points: [
        "3.3",
        "7.6",
        "7.7",
        "7.8",
        "8.4",
        "8.6"
      ],
      frequency: %{
        kind: :official_parameter_sequence
      },
      dependencies: [],
      source_path: "query-specifications/bi-read-20.yaml"
    },
    %{
      id: "ldbc/snb-bi/update-batch@v1.0.3",
      upstream_id: "BI update batch",
      family: :update_batch,
      availability: :mandatory,
      title: "Ordered insert and delete microbatch",
      parameters: [
        %{
          name: "batchSequence",
          type: "64-bit Integer"
        },
        %{
          name: "inserts",
          type: "ordered insert records"
        },
        %{
          name: "deletes",
          type: "ordered delete records"
        }
      ],
      result: [
        %{
          name: "appliedBatch",
          type: "Boolean"
        }
      ],
      ordering: :stream_defined,
      limit: nil,
      variants: [
        "insert-01..08",
        "delete-01..08"
      ],
      choke_points: [
        "9.1",
        "9.2",
        "9.3",
        "9.4",
        "9.5"
      ],
      frequency: %{
        kind: :official_update_stream
      },
      dependencies: [
        :preceding_batch
      ],
      source_path: "query-specifications/{insert,delete}-01..08.yaml"
    }
  ]
}
