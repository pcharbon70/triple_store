# Generated from the pinned upstream operation metadata; edit through a source-pin review.
%{
  id: "ldbc-snb-interactive-v1.2.0",
  benchmark: :snb_interactive,
  version: "v1.2.0",
  source_id: "snb-specification-2.2.4",
  expected_operation_ids: [
    "ldbc/snb-interactive/complex-read-01@v1.2.0",
    "ldbc/snb-interactive/complex-read-02@v1.2.0",
    "ldbc/snb-interactive/complex-read-03@v1.2.0",
    "ldbc/snb-interactive/complex-read-04@v1.2.0",
    "ldbc/snb-interactive/complex-read-05@v1.2.0",
    "ldbc/snb-interactive/complex-read-06@v1.2.0",
    "ldbc/snb-interactive/complex-read-07@v1.2.0",
    "ldbc/snb-interactive/complex-read-08@v1.2.0",
    "ldbc/snb-interactive/complex-read-09@v1.2.0",
    "ldbc/snb-interactive/complex-read-10@v1.2.0",
    "ldbc/snb-interactive/complex-read-11@v1.2.0",
    "ldbc/snb-interactive/complex-read-12@v1.2.0",
    "ldbc/snb-interactive/complex-read-13@v1.2.0",
    "ldbc/snb-interactive/complex-read-14@v1.2.0",
    "ldbc/snb-interactive/short-read-01@v1.2.0",
    "ldbc/snb-interactive/short-read-02@v1.2.0",
    "ldbc/snb-interactive/short-read-03@v1.2.0",
    "ldbc/snb-interactive/short-read-04@v1.2.0",
    "ldbc/snb-interactive/short-read-05@v1.2.0",
    "ldbc/snb-interactive/short-read-06@v1.2.0",
    "ldbc/snb-interactive/short-read-07@v1.2.0",
    "ldbc/snb-interactive/insert-01@v1.2.0",
    "ldbc/snb-interactive/insert-02@v1.2.0",
    "ldbc/snb-interactive/insert-03@v1.2.0",
    "ldbc/snb-interactive/insert-04@v1.2.0",
    "ldbc/snb-interactive/insert-05@v1.2.0",
    "ldbc/snb-interactive/insert-06@v1.2.0",
    "ldbc/snb-interactive/insert-07@v1.2.0",
    "ldbc/snb-interactive/insert-08@v1.2.0"
  ],
  operations: [
    %{
      id: "ldbc/snb-interactive/complex-read-01@v1.2.0",
      upstream_id: "Interactive Complex Read 1",
      family: :complex_read,
      availability: :mandatory,
      title: "Transitive friends with a certain name",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "firstName",
          type: "String"
        }
      ],
      result: [
        %{
          name: "otherPerson.id",
          type: "ID"
        },
        %{
          name: "otherPerson.lastName",
          type: "String"
        },
        %{
          name: "distanceFromPerson",
          type: "32-bit Integer",
          category: "calculated"
        },
        %{
          name: "otherPerson.birthday",
          type: "Date"
        },
        %{
          name: "otherPerson.creationDate",
          type: "DateTime"
        },
        %{
          name: "otherPerson.gender",
          type: "String"
        },
        %{
          name: "otherPerson.browserUsed",
          type: "String"
        },
        %{
          name: "otherPerson.locationIP",
          type: "String"
        },
        %{
          name: "otherPerson.email",
          type: "\\{Long String\\}"
        },
        %{
          name: "otherPerson.speaks",
          type: "\\{String\\}"
        },
        %{
          name: "locationCity.name",
          type: "String"
        },
        %{
          name: "universities",
          type: "\\{\\<String, 32-bit Integer, String>\\}",
          category: "aggregated"
        },
        %{
          name: "companies",
          type: "\\{\\<String, 32-bit Integer, String>\\}",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "distanceFromPerson",
          direction: "asc"
        },
        %{
          name: "otherPerson.lastName",
          direction: "asc"
        },
        %{
          name: "otherPerson.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "2.1",
        "5.3",
        "8.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-01.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-02@v1.2.0",
      upstream_id: "Interactive Complex Read 2",
      family: :complex_read,
      availability: :mandatory,
      title: "Recent messages by your friends",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "maxDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "friend.id",
          type: "ID"
        },
        %{
          name: "friend.firstName",
          type: "String"
        },
        %{
          name: "friend.lastName",
          type: "String"
        },
        %{
          name: "message.id",
          type: "ID"
        },
        %{
          name: "message.content or message.imageFile (for photos)",
          type: "Text"
        },
        %{
          name: "message.creationDate",
          type: "DateTime"
        }
      ],
      ordering: [
        %{
          name: "message.creationDate",
          direction: "desc"
        },
        %{
          name: "message.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "1.1",
        "2.2",
        "2.3",
        "3.2",
        "8.5"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-02.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-03@v1.2.0",
      upstream_id: "Interactive Complex Read 3",
      family: :complex_read,
      availability: :mandatory,
      title: "Friends and friends of friends that have been to given countries",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "countryXName",
          type: "String"
        },
        %{
          name: "countryYName",
          type: "String"
        },
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "durationDays",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "otherPerson.id",
          type: "ID"
        },
        %{
          name: "otherPerson.firstName",
          type: "String"
        },
        %{
          name: "otherPerson.lastName",
          type: "String"
        },
        %{
          name: "xCount",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "yCount",
          type: "32-bit Integer",
          category: "aggregated"
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
          name: "otherPerson.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "2.1",
        "3.1",
        "5.1",
        "8.2",
        "8.5"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-03.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-04@v1.2.0",
      upstream_id: "Interactive Complex Read 4",
      family: :complex_read,
      availability: :mandatory,
      title: "New topics",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "startDate",
          type: "Date"
        },
        %{
          name: "durationDays",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "tag.name",
          type: "Long String"
        },
        %{
          name: "postCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "postCount",
          direction: "desc"
        },
        %{
          name: "tag.name",
          direction: "asc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [
        "2.3",
        "8.2",
        "8.5"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-04.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-05@v1.2.0",
      upstream_id: "Interactive Complex Read 5",
      family: :complex_read,
      availability: :mandatory,
      title: "New groups",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "minDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "forum.title",
          type: "Long String"
        },
        %{
          name: "postCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "postCount",
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
        "2.3",
        "3.3",
        "8.2",
        "8.5"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-05.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-06@v1.2.0",
      upstream_id: "Interactive Complex Read 6",
      family: :complex_read,
      availability: :mandatory,
      title: "Tag co-occurrence",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "tagName",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "otherTag.name",
          type: "Long String"
        },
        %{
          name: "postCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "postCount",
          direction: "desc"
        },
        %{
          name: "otherTag.name",
          direction: "asc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [
        "5.1",
        "8.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-06.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-07@v1.2.0",
      upstream_id: "Interactive Complex Read 7",
      family: :complex_read,
      availability: :mandatory,
      title: "Recent likers",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "friend.id",
          type: "ID"
        },
        %{
          name: "friend.firstName",
          type: "String"
        },
        %{
          name: "friend.lastName",
          type: "String"
        },
        %{
          name: "likes.creationDate",
          type: "DateTime"
        },
        %{
          name: "message.id",
          type: "ID"
        },
        %{
          name: "message.content or message.imageFile (for photos)",
          type: "Text"
        },
        %{
          name: "minutesLatency",
          type: "32-bit Integer",
          category: "calculated"
        },
        %{
          name: "isNew",
          type: "Boolean",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "likes.creationDate",
          direction: "desc"
        },
        %{
          name: "friend.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "2.2",
        "2.3",
        "3.3",
        "5.1",
        "8.1",
        "8.3"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-07.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-08@v1.2.0",
      upstream_id: "Interactive Complex Read 8",
      family: :complex_read,
      availability: :mandatory,
      title: "Recent replies",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "commentAuthor.id",
          type: "ID"
        },
        %{
          name: "commentAuthor.firstName",
          type: "String"
        },
        %{
          name: "commentAuthor.lastName",
          type: "String"
        },
        %{
          name: "comment.creationDate",
          type: "DateTime"
        },
        %{
          name: "comment.id",
          type: "ID"
        },
        %{
          name: "comment.content",
          type: "Text"
        }
      ],
      ordering: [
        %{
          name: "comment.creationDate",
          direction: "desc"
        },
        %{
          name: "comment.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "2.4",
        "3.3",
        "5.3"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-08.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-09@v1.2.0",
      upstream_id: "Interactive Complex Read 9",
      family: :complex_read,
      availability: :mandatory,
      title: "Recent messages by friends or friends of friends",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "maxDate",
          type: "Date"
        }
      ],
      result: [
        %{
          name: "otherPerson.id",
          type: "ID"
        },
        %{
          name: "otherPerson.firstName",
          type: "String"
        },
        %{
          name: "otherPerson.lastName",
          type: "String"
        },
        %{
          name: "message.id",
          type: "ID"
        },
        %{
          name: "message.content or message.imageFile (for photos)",
          type: "Text"
        },
        %{
          name: "message.creationDate",
          type: "DateTime"
        }
      ],
      ordering: [
        %{
          name: "message.creationDate",
          direction: "desc"
        },
        %{
          name: "message.id",
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
        "2.2",
        "2.3",
        "3.2",
        "3.3",
        "8.5"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-09.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-10@v1.2.0",
      upstream_id: "Interactive Complex Read 10",
      family: :complex_read,
      availability: :mandatory,
      title: "Friend recommendation",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "month",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "foaf.id",
          type: "ID"
        },
        %{
          name: "foaf.firstName",
          type: "String"
        },
        %{
          name: "foaf.lastName",
          type: "String"
        },
        %{
          name: "commonInterestScore",
          type: "32-bit Integer",
          category: "aggregated"
        },
        %{
          name: "foaf.gender",
          type: "String"
        },
        %{
          name: "city.name",
          type: "String"
        }
      ],
      ordering: [
        %{
          name: "commonInterestScore",
          direction: "desc"
        },
        %{
          name: "foaf.id",
          direction: "asc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [
        "2.3",
        "3.3",
        "4.1",
        "4.2",
        "5.1",
        "5.2",
        "6.1",
        "7.1",
        "8.6"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-10.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-11@v1.2.0",
      upstream_id: "Interactive Complex Read 11",
      family: :complex_read,
      availability: :mandatory,
      title: "Job referral",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "countryName",
          type: "String"
        },
        %{
          name: "workFromYear",
          type: "32-bit Integer"
        }
      ],
      result: [
        %{
          name: "otherPerson.id",
          type: "ID"
        },
        %{
          name: "otherPerson.firstName",
          type: "String"
        },
        %{
          name: "otherPerson.lastName",
          type: "String"
        },
        %{
          name: "company.name",
          type: "String"
        },
        %{
          name: "workAt.workFrom",
          type: "32-bit Integer"
        }
      ],
      ordering: [
        %{
          name: "workAt.workFrom",
          direction: "asc"
        },
        %{
          name: "otherPerson.id",
          direction: "asc"
        },
        %{
          name: "company.name",
          direction: "desc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [
        "1.3",
        "2.3",
        "2.4",
        "3.3",
        "4.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-11.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-12@v1.2.0",
      upstream_id: "Interactive Complex Read 12",
      family: :complex_read,
      availability: :mandatory,
      title: "Expert search",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "tagClassName",
          type: "Long String"
        }
      ],
      result: [
        %{
          name: "friend.id",
          type: "ID"
        },
        %{
          name: "friend.firstName",
          type: "String"
        },
        %{
          name: "friend.lastName",
          type: "String"
        },
        %{
          name: "tagNames",
          type: "\\{Long String\\}",
          category: "aggregated"
        },
        %{
          name: "replyCount",
          type: "32-bit Integer",
          category: "aggregated"
        }
      ],
      ordering: [
        %{
          name: "replyCount",
          direction: "desc"
        },
        %{
          name: "friend.id",
          direction: "asc"
        }
      ],
      limit: 20,
      variants: [
        "default"
      ],
      choke_points: [
        "3.3",
        "7.2",
        "7.3",
        "8.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-12.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-13@v1.2.0",
      upstream_id: "Interactive Complex Read 13",
      family: :complex_read,
      availability: :mandatory,
      title: "Single shortest path",
      parameters: [
        %{
          name: "person1Id",
          type: "ID"
        },
        %{
          name: "person2Id",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "shortestPathLength",
          type: "32-bit Integer",
          category: "calculated"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [
        "3.3",
        "7.2",
        "7.3",
        "7.5",
        "7.8",
        "8.1",
        "8.6"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-13.yaml"
    },
    %{
      id: "ldbc/snb-interactive/complex-read-14@v1.2.0",
      upstream_id: "Interactive Complex Read 14",
      family: :complex_read,
      availability: :mandatory,
      title: "Trusted connection paths (v1)",
      parameters: [
        %{
          name: "person1Id",
          type: "ID"
        },
        %{
          name: "person2Id",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "personIdsInPath",
          type: "[ID]",
          category: "calculated"
        },
        %{
          name: "pathWeight",
          type: "64-bit Float",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "pathWeight",
          direction: "desc"
        }
      ],
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "3.3",
        "5.3",
        "7.2",
        "7.3",
        "7.5",
        "7.7",
        "8.1",
        "8.2",
        "8.3",
        "8.6"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [],
      source_path: "query-specifications/interactive-complex-read-14-v1.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-01@v1.2.0",
      upstream_id: "Interactive Short Read 1",
      family: :short_read,
      availability: :mandatory,
      title: "Profile of a person",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "person.firstName",
          type: "String"
        },
        %{
          name: "person.lastName",
          type: "String"
        },
        %{
          name: "person.birthday",
          type: "Date"
        },
        %{
          name: "person.locationIP",
          type: "String"
        },
        %{
          name: "person.browserUsed",
          type: "String"
        },
        %{
          name: "city.id",
          type: "ID"
        },
        %{
          name: "person.gender",
          type: "String"
        },
        %{
          name: "person.creationDate",
          type: "DateTime"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-01.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-02@v1.2.0",
      upstream_id: "Interactive Short Read 2",
      family: :short_read,
      availability: :mandatory,
      title: "Recent messages of a person",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "message.id",
          type: "ID"
        },
        %{
          name: "message.content or message.imageFile (for photos)",
          type: "Text"
        },
        %{
          name: "message.creationDate",
          type: "DateTime"
        },
        %{
          name: "post.id",
          type: "ID"
        },
        %{
          name: "originalPoster.id",
          type: "ID"
        },
        %{
          name: "originalPoster.firstName",
          type: "String"
        },
        %{
          name: "originalPoster.lastName",
          type: "String"
        }
      ],
      ordering: [
        %{
          name: "message.creationDate",
          direction: "desc"
        },
        %{
          name: "message.id",
          direction: "desc"
        }
      ],
      limit: 10,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-02.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-03@v1.2.0",
      upstream_id: "Interactive Short Read 3",
      family: :short_read,
      availability: :mandatory,
      title: "Friends of a person",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "friend.id",
          type: "ID"
        },
        %{
          name: "friend.firstName",
          type: "String"
        },
        %{
          name: "friend.lastName",
          type: "String"
        },
        %{
          name: "knows.creationDate",
          type: "DateTime"
        }
      ],
      ordering: [
        %{
          name: "knows.creationDate",
          direction: "desc"
        },
        %{
          name: "friend.id",
          direction: "asc"
        }
      ],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-03.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-04@v1.2.0",
      upstream_id: "Interactive Short Read 4",
      family: :short_read,
      availability: :mandatory,
      title: "Content of a message",
      parameters: [
        %{
          name: "messageId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "message.creationDate",
          type: "DateTime"
        },
        %{
          name: "message.content or message.imageFile (for photos)",
          type: "Text"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-04.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-05@v1.2.0",
      upstream_id: "Interactive Short Read 5",
      family: :short_read,
      availability: :mandatory,
      title: "Creator of a message",
      parameters: [
        %{
          name: "messageId",
          type: "ID"
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
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-05.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-06@v1.2.0",
      upstream_id: "Interactive Short Read 6",
      family: :short_read,
      availability: :mandatory,
      title: "Forum of a message",
      parameters: [
        %{
          name: "messageId",
          type: "ID"
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
          name: "moderator.id",
          type: "ID"
        },
        %{
          name: "moderator.firstName",
          type: "String"
        },
        %{
          name: "moderator.lastName",
          type: "String"
        }
      ],
      ordering: [],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-06.yaml"
    },
    %{
      id: "ldbc/snb-interactive/short-read-07@v1.2.0",
      upstream_id: "Interactive Short Read 7",
      family: :short_read,
      availability: :mandatory,
      title: "Replies of a message",
      parameters: [
        %{
          name: "messageId",
          type: "ID"
        }
      ],
      result: [
        %{
          name: "comment.id",
          type: "ID"
        },
        %{
          name: "comment.content",
          type: "Text"
        },
        %{
          name: "comment.creationDate",
          type: "DateTime"
        },
        %{
          name: "replyAuthor.id",
          type: "ID"
        },
        %{
          name: "replyAuthor.firstName",
          type: "String"
        },
        %{
          name: "replyAuthor.lastName",
          type: "String"
        },
        %{
          name: "knows",
          type: "Boolean",
          category: "calculated"
        }
      ],
      ordering: [
        %{
          name: "comment.creationDate",
          direction: "desc"
        },
        %{
          name: "replyAuthor.id",
          direction: "asc"
        }
      ],
      limit: nil,
      variants: [
        "default"
      ],
      choke_points: [],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :driver_update_context
      ],
      source_path: "query-specifications/interactive-short-read-07.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-01@v1.2.0",
      upstream_id: "Interactive Update 1",
      family: :insert,
      availability: :mandatory,
      title: "Add person",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "personFirstName",
          type: "String"
        },
        %{
          name: "personLastName",
          type: "String"
        },
        %{
          name: "gender",
          type: "String"
        },
        %{
          name: "birthday",
          type: "Date"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        },
        %{
          name: "locationIP",
          type: "String"
        },
        %{
          name: "browserUsed",
          type: "String"
        },
        %{
          name: "cityId",
          type: "ID"
        },
        %{
          name: "languages",
          type: "\\{String\\}"
        },
        %{
          name: "emails",
          type: "\\{Long String\\}"
        },
        %{
          name: "tagIds",
          type: "\\{ID\\}"
        },
        %{
          name: "studyAt",
          type: "\\{\\<ID, 32-bit Integer>\\}"
        },
        %{
          name: "workAt",
          type: "\\{\\<ID, 32-bit Integer>\\}"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.1",
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-01.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-02@v1.2.0",
      upstream_id: "Interactive Update 2",
      family: :insert,
      availability: :mandatory,
      title: "Add like to post",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "postId",
          type: "ID"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-02.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-03@v1.2.0",
      upstream_id: "Interactive Update 3",
      family: :insert,
      availability: :mandatory,
      title: "Add like to comment",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "commentId",
          type: "ID"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-03.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-04@v1.2.0",
      upstream_id: "Interactive Update 4",
      family: :insert,
      availability: :mandatory,
      title: "Add forum",
      parameters: [
        %{
          name: "forumId",
          type: "ID"
        },
        %{
          name: "forumTitle",
          type: "Long String"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        },
        %{
          name: "moderatorId",
          type: "ID"
        },
        %{
          name: "tagIds",
          type: "\\{ID\\}"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.1",
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-04.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-05@v1.2.0",
      upstream_id: "Interactive Update 5",
      family: :insert,
      availability: :mandatory,
      title: "Add forum membership",
      parameters: [
        %{
          name: "personId",
          type: "ID"
        },
        %{
          name: "forumId",
          type: "ID"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.1",
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-05.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-06@v1.2.0",
      upstream_id: "Interactive Update 6",
      family: :insert,
      availability: :mandatory,
      title: "Add post",
      parameters: [
        %{
          name: "postId",
          type: "ID"
        },
        %{
          name: "imageFile",
          type: "String"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        },
        %{
          name: "locationIP",
          type: "String"
        },
        %{
          name: "browserUsed",
          type: "String"
        },
        %{
          name: "language",
          type: "String"
        },
        %{
          name: "content",
          type: "Text"
        },
        %{
          name: "length",
          type: "32-bit Integer"
        },
        %{
          name: "authorPersonId",
          type: "ID"
        },
        %{
          name: "forumId",
          type: "ID"
        },
        %{
          name: "countryId",
          type: "ID"
        },
        %{
          name: "tagIds",
          type: "\\{ID\\}"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.1",
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-06.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-07@v1.2.0",
      upstream_id: "Interactive Update 7",
      family: :insert,
      availability: :mandatory,
      title: "Add comment",
      parameters: [
        %{
          name: "commentId",
          type: "ID"
        },
        %{
          name: "creationDate",
          type: "DateTime"
        },
        %{
          name: "locationIP",
          type: "String"
        },
        %{
          name: "browserUsed",
          type: "String"
        },
        %{
          name: "content",
          type: "Text"
        },
        %{
          name: "length",
          type: "32-bit Integer"
        },
        %{
          name: "authorPersonId",
          type: "ID"
        },
        %{
          name: "countryId",
          type: "ID"
        },
        %{
          name: "replyToPostId",
          type: "ID",
          category: "calculated"
        },
        %{
          name: "replyToCommentId",
          type: "ID",
          category: "calculated"
        },
        %{
          name: "tagIds",
          type: "\\{ID\\}"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.1",
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-07.yaml"
    },
    %{
      id: "ldbc/snb-interactive/insert-08@v1.2.0",
      upstream_id: "Interactive Update 8",
      family: :insert,
      availability: :mandatory,
      title: "Add friendship",
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
          name: "creationDate",
          type: "DateTime"
        }
      ],
      result: [],
      ordering: :update_stream,
      limit: nil,
      variants: [
        "v1"
      ],
      choke_points: [
        "9.2"
      ],
      frequency: %{
        kind: :driver_defined
      },
      dependencies: [
        :preceding_update
      ],
      source_path: "query-specifications/insert-08.yaml"
    }
  ]
}
