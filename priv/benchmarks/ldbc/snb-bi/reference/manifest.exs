%{
  schema_version: 2,
  layout_version: "snb-bi-reference-answers-v1",
  catalog_id: "ldbc-snb-bi-v1.0.3",
  external_layout: %{
    address: "<dataset-checksum>/<operation-id>/<variant>/<parameter-row>.json",
    digest: :sha256,
    storage: :external
  },
  answers: [
    %{
      operation_id: "ldbc/snb-bi/read-01@v1.0.3",
      variant: "default",
      purpose: :contract_fixture,
      parameter: %{
        sequence: "1",
        row_index: 0,
        checksum: "sha256:e00a070ac1540defeb7348e7a5b0b65ea90f7270826541be12209dab0f14c302",
        source_id: "snb-bi-1.0.3",
        generator_pin: "5f7967235593eefa98b336e240039780b6a76a9a"
      },
      dataset: %{
        id: "snb-bi-smoke",
        checksum: "sha256:90f9b6e38f2689be479c0d40b368db3e2f0542d503cb7d1ec9950b0e524ecf33",
        mapping_version: "snb-rdf-v1"
      },
      operation_source: %{
        source_id: "snb-specification-2.2.4",
        path: "query-specifications/bi-read-01.yaml",
        commit: "5f7956e07a214373c363b371a3b88bc83ddcd118",
        reference: "ldbc_snb_bi:cypher/queries/bi-1.cypher"
      },
      reference_source: %{
        implementation: "ldbc-snb-bi-cypher",
        version: "v1.0.3",
        commit: "5f7967235593eefa98b336e240039780b6a76a9a"
      },
      artifact: %{
        kind: :embedded,
        path: "smoke/bi-01/default/000.json",
        checksum: "sha256:bd349c43a8039831620a4df23fb14c83577447ba6170596357bce8762d9ebfe4"
      }
    }
  ]
}
