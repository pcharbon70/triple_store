# Generated from the pinned SNB BI catalog and translation artifacts; checksums are content-addressed.
%{
  id: "triplestore-snb-bi-translations-v1",
  benchmark: :snb_bi,
  upstream_source: "snb-bi-1.0.3",
  upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
  transformation_version: "triplestore-snb-bi-v1",
  mapping_version: "snb-rdf-v1",
  parameter_references: %{
    1 => %{"datetime" => %{kind: :scalar}},
    2 => %{
      "date" => %{kind: :scalar},
      "tagClass" => %{kind: :entity_name, entity: "TagClass"}
    },
    3 => %{
      "tagClass" => %{kind: :entity_name, entity: "TagClass"},
      "country" => %{kind: :entity_name, entity: "Place"}
    },
    4 => %{"date" => %{kind: :scalar}},
    5 => %{"tag" => %{kind: :entity_name, entity: "Tag"}},
    6 => %{"tag" => %{kind: :entity_name, entity: "Tag"}},
    7 => %{"tag" => %{kind: :entity_name, entity: "Tag"}},
    8 => %{
      "tag" => %{kind: :entity_name, entity: "Tag"},
      "startDate" => %{kind: :scalar},
      "endDate" => %{kind: :scalar}
    },
    9 => %{"startDate" => %{kind: :scalar}, "endDate" => %{kind: :scalar}},
    10 => %{
      "personId" => %{kind: :entity_id, entity: "Person"},
      "country" => %{kind: :entity_name, entity: "Place"},
      "tagClass" => %{kind: :entity_name, entity: "TagClass"},
      "minPathDistance" => %{kind: :scalar},
      "maxPathDistance" => %{kind: :scalar}
    },
    11 => %{
      "country" => %{kind: :entity_name, entity: "Place"},
      "startDate" => %{kind: :scalar},
      "endDate" => %{kind: :scalar}
    },
    12 => %{
      "startDate" => %{kind: :scalar},
      "lengthThreshold" => %{kind: :scalar},
      "languages" => %{kind: :scalar}
    },
    13 => %{
      "country" => %{kind: :entity_name, entity: "Place"},
      "endDate" => %{kind: :scalar}
    },
    14 => %{
      "country1" => %{kind: :entity_name, entity: "Place"},
      "country2" => %{kind: :entity_name, entity: "Place"}
    },
    15 => %{
      "person1Id" => %{kind: :entity_id, entity: "Person"},
      "person2Id" => %{kind: :entity_id, entity: "Person"},
      "startDate" => %{kind: :scalar},
      "endDate" => %{kind: :scalar}
    },
    16 => %{
      "tagA" => %{kind: :entity_name, entity: "Tag"},
      "dateA" => %{kind: :scalar},
      "tagB" => %{kind: :entity_name, entity: "Tag"},
      "dateB" => %{kind: :scalar},
      "maxKnowsLimit" => %{kind: :scalar}
    },
    17 => %{
      "tag" => %{kind: :entity_name, entity: "Tag"},
      "delta" => %{kind: :scalar}
    },
    18 => %{"tag" => %{kind: :entity_name, entity: "Tag"}},
    19 => %{
      "city1Id" => %{kind: :entity_id, entity: "Place"},
      "city2Id" => %{kind: :entity_id, entity: "Place"}
    },
    20 => %{
      "company" => %{kind: :entity_name, entity: "Organisation"},
      "person2Id" => %{kind: :entity_id, entity: "Person"}
    }
  },
  reference_profiles: %{
    official: %{
      provenance: :official,
      manifest_profile: :any,
      fields: :all
    },
    snb_bi_smoke: %{
      provenance: :smoke,
      manifest_profile: "snb_bi_smoke",
      fields: %{
        10 => ["personId"],
        15 => ["person1Id", "person2Id"],
        20 => ["person2Id"]
      }
    }
  },
  translations: [
    %{
      operation_id: "ldbc/snb-bi/read-01@v1.0.3",
      number: 1,
      variant: "default",
      path: "bi-01-default.rq",
      source_card: "query-specifications/bi-read-01.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-1.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:61bb381c4efad8051c81d0997ae028ea28115e2066b97d1746cf9bcd10a5ae6d"
    },
    %{
      operation_id: "ldbc/snb-bi/read-02@v1.0.3",
      number: 2,
      variant: "a",
      path: "bi-02-a.rq",
      source_card: "query-specifications/bi-read-02.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-2.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:0a1379f8b0f858b638c14bb503d8928fc0d2d406fd8bacc15fd828188fa3d384"
    },
    %{
      operation_id: "ldbc/snb-bi/read-02@v1.0.3",
      number: 2,
      variant: "b",
      path: "bi-02-b.rq",
      source_card: "query-specifications/bi-read-02.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-2.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:0a1379f8b0f858b638c14bb503d8928fc0d2d406fd8bacc15fd828188fa3d384"
    },
    %{
      operation_id: "ldbc/snb-bi/read-02@v1.0.3",
      number: 2,
      variant: "m",
      path: "bi-02-m.rq",
      source_card: "query-specifications/bi-read-02.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-2.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:0a1379f8b0f858b638c14bb503d8928fc0d2d406fd8bacc15fd828188fa3d384"
    },
    %{
      operation_id: "ldbc/snb-bi/read-03@v1.0.3",
      number: 3,
      variant: "default",
      path: "bi-03-default.rq",
      source_card: "query-specifications/bi-read-03.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-3.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:2962e30009c36ad6c54a3e906b4e01290cca31e83626dbc39e67afbc2039799f"
    },
    %{
      operation_id: "ldbc/snb-bi/read-04@v1.0.3",
      number: 4,
      variant: "default",
      path: "bi-04-default.rq",
      source_card: "query-specifications/bi-read-04.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-4.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:777baea4479627af9f50a475ec82cf9bc271271ac22389951623173011df2540"
    },
    %{
      operation_id: "ldbc/snb-bi/read-05@v1.0.3",
      number: 5,
      variant: "default",
      path: "bi-05-default.rq",
      source_card: "query-specifications/bi-read-05.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-5.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:fe499195197351ddee98caf8450ad75502b85f3dad19c4e1575859005ae385cc"
    },
    %{
      operation_id: "ldbc/snb-bi/read-06@v1.0.3",
      number: 6,
      variant: "default",
      path: "bi-06-default.rq",
      source_card: "query-specifications/bi-read-06.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-6.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:b2b28686d012a4134d61693d1331daf3402fad7ee22f58af1ae709a0e104522a"
    },
    %{
      operation_id: "ldbc/snb-bi/read-07@v1.0.3",
      number: 7,
      variant: "default",
      path: "bi-07-default.rq",
      source_card: "query-specifications/bi-read-07.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-7.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:6ef778c5cce3cce9796ca163221403e6f7ae45ea6cf085076fe124061360d0c7"
    },
    %{
      operation_id: "ldbc/snb-bi/read-08@v1.0.3",
      number: 8,
      variant: "a",
      path: "bi-08-a.rq",
      source_card: "query-specifications/bi-read-08.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-8.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:48a819019ef15773c35972bac959ac2e7afd031077a72db0ff1a0c78591ea536"
    },
    %{
      operation_id: "ldbc/snb-bi/read-08@v1.0.3",
      number: 8,
      variant: "b",
      path: "bi-08-b.rq",
      source_card: "query-specifications/bi-read-08.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-8.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:48a819019ef15773c35972bac959ac2e7afd031077a72db0ff1a0c78591ea536"
    },
    %{
      operation_id: "ldbc/snb-bi/read-08@v1.0.3",
      number: 8,
      variant: "m",
      path: "bi-08-m.rq",
      source_card: "query-specifications/bi-read-08.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-8.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:48a819019ef15773c35972bac959ac2e7afd031077a72db0ff1a0c78591ea536"
    },
    %{
      operation_id: "ldbc/snb-bi/read-09@v1.0.3",
      number: 9,
      variant: "default",
      path: "bi-09-default.rq",
      source_card: "query-specifications/bi-read-09.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-9.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:39909427758990b186cffd05fb3c857f993dad590a0044024acda2b7ea71ac6c"
    },
    %{
      operation_id: "ldbc/snb-bi/read-11@v1.0.3",
      number: 11,
      variant: "default",
      path: "bi-11-default.rq",
      source_card: "query-specifications/bi-read-11.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-11.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:b78d7f6ce788cf267582f65f407a3e0885032574db4d44121e897ffda3a3db2b"
    },
    %{
      operation_id: "ldbc/snb-bi/read-12@v1.0.3",
      number: 12,
      variant: "default",
      path: "bi-12-default.rq",
      source_card: "query-specifications/bi-read-12.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-12.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:ec5f9266a458408a59baf4dd951c6252710c95373f5064b14c9c0560405e269e"
    },
    %{
      operation_id: "ldbc/snb-bi/read-13@v1.0.3",
      number: 13,
      variant: "default",
      path: "bi-13-default.rq",
      source_card: "query-specifications/bi-read-13.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-13.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:947996391ecd044c22f490bee9703ec9ba22337f4c6149c302e72edb7c43ee75"
    },
    %{
      operation_id: "ldbc/snb-bi/read-14@v1.0.3",
      number: 14,
      variant: "a",
      path: "bi-14-a.rq",
      source_card: "query-specifications/bi-read-14.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-14.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:227790e29f747e7763225426fec0f50c69bf905ada39850e45b412594ceb7f90"
    },
    %{
      operation_id: "ldbc/snb-bi/read-14@v1.0.3",
      number: 14,
      variant: "b",
      path: "bi-14-b.rq",
      source_card: "query-specifications/bi-read-14.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-14.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:227790e29f747e7763225426fec0f50c69bf905ada39850e45b412594ceb7f90"
    },
    %{
      operation_id: "ldbc/snb-bi/read-16@v1.0.3",
      number: 16,
      variant: "a",
      path: "bi-16-a.rq",
      source_card: "query-specifications/bi-read-16.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-16.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:3b73db177e5ffa9273b6c449d4c09277cdcd43fefb1322bbbbf5bdc90ad72f9b"
    },
    %{
      operation_id: "ldbc/snb-bi/read-16@v1.0.3",
      number: 16,
      variant: "b",
      path: "bi-16-b.rq",
      source_card: "query-specifications/bi-read-16.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-16.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:3b73db177e5ffa9273b6c449d4c09277cdcd43fefb1322bbbbf5bdc90ad72f9b"
    },
    %{
      operation_id: "ldbc/snb-bi/read-17@v1.0.3",
      number: 17,
      variant: "default",
      path: "bi-17-default.rq",
      source_card: "query-specifications/bi-read-17.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-17.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:36c50d86e31e4c05d832ffacf15dd42b4d24191488011c4b0288f81f05e46d4a"
    },
    %{
      operation_id: "ldbc/snb-bi/read-18@v1.0.3",
      number: 18,
      variant: "default",
      path: "bi-18-default.rq",
      source_card: "query-specifications/bi-read-18.yaml",
      source_card_commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
      reference_query: "ldbc_snb_bi:cypher/queries/bi-18.cypher",
      upstream_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
      transformation_version: "triplestore-snb-bi-v1",
      mapping_version: "snb-rdf-v1",
      checksum: "sha256:e0772de992e50ce18bfa173875bbd985235a098d1e0777fd27f68c2c75e7ad33"
    }
  ]
}
