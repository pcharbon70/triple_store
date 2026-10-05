source = fn id, benchmark, profiles, roles, repository, release, commit, runtimes, readiness ->
  %{
    id: id,
    benchmark: benchmark,
    profiles: profiles,
    roles: roles,
    repository: repository,
    release: release,
    commit: commit,
    checksum: %{algorithm: :git_sha1, value: commit},
    license: "Apache-2.0",
    notice: "NOTICE.txt",
    runtimes: runtimes,
    platforms: ["Linux", "macOS"],
    readiness: readiness,
    asset_policy: :generate_or_download
  }
end

[
  source.(
    "spb-2.0.2",
    :spb,
    ["spb-smoke-v2.0.2", "spb-development-v2.0.2", "spb-comparable-v2.0.2"],
    [:specification, :generator, :driver, :operation_catalog, :validation, :scoring],
    "https://github.com/ldbc/ldbc_spb_bm_2.0",
    "2.0.2",
    "ce6323c0936306729408233dc70d26f2389b34c6",
    ["Java 8", "Apache Ant"],
    :stable
  ),
  source.(
    "snb-specification-2.2.4",
    :snb_bi,
    ["snb-bi-smoke-v1.0.3", "snb-bi-comparable-v1.0.3", "snb-interactive-v1.2.0"],
    [:specification, :operation_catalog, :audit_policy],
    "https://github.com/ldbc/ldbc_snb_docs",
    "v2.2.4",
    "5f7956e07a214373c363b371a3b88bc83ddcd118",
    ["Python 3", "LaTeX"],
    :stable
  ),
  source.(
    "snb-bi-datagen-0.5.1",
    :snb_bi,
    ["snb-bi-smoke-v1.0.3", "snb-bi-comparable-v1.0.3"],
    [:dataset_generator, :factor_generator],
    "https://github.com/ldbc/ldbc_snb_datagen_spark",
    "v0.5.1",
    "2459f4e45834c78902a50511fc64a05c48dd4029",
    ["Java 8 or 11", "Scala 2.12", "Apache Spark 3.2"],
    :stable
  ),
  source.(
    "snb-bi-1.0.3",
    :snb_bi,
    ["snb-bi-smoke-v1.0.3", "snb-bi-development-v1.0.3", "snb-bi-comparable-v1.0.3"],
    [:parameter_generator, :scoring, :reference_implementation, :validation],
    "https://github.com/ldbc/ldbc_snb_bi",
    "v1.0.3",
    "5f7967235593eefa98b336e240039780b6a76a9a",
    ["Python 3", "reference database runtime"],
    :stable
  ),
  source.(
    "snb-interactive-v1-datagen-1.0.0",
    :snb_interactive,
    ["snb-interactive-smoke-v1.2.0", "snb-interactive-comparable-v1.2.0"],
    [:dataset_generator, :parameter_generator, :update_stream_generator],
    "https://github.com/ldbc/ldbc_snb_datagen_hadoop",
    "v1.0.0",
    "37d35f40f5023fcf1afd3b6d0984f71c202f4bca",
    ["Java 8", "Apache Hadoop 2"],
    :audited_stable
  ),
  source.(
    "snb-interactive-v1-driver-1.2.0",
    :snb_interactive,
    ["snb-interactive-smoke-v1.2.0", "snb-interactive-comparable-v1.2.0"],
    [:driver, :validation, :metrics],
    "https://github.com/ldbc/ldbc_snb_interactive_v1_driver",
    "v1.2.0",
    "4cd13735f964406ad34f34ccd5bef4d6e6c284d0",
    ["Java 8", "Apache Maven 3"],
    :audited_stable
  ),
  source.(
    "snb-interactive-v1-impls-1.0.0",
    :snb_interactive,
    ["snb-interactive-development-v1.2.0", "snb-interactive-comparable-v1.2.0"],
    [:reference_implementation, :validation, :audit_tooling],
    "https://github.com/ldbc/ldbc_snb_interactive_v1_impls",
    "1.0.0",
    "f9c394a92cd55e535893f6c9907b141d6533c817",
    ["Java 8", "Apache Maven 3", "reference database runtime"],
    :audited_stable
  ),
  source.(
    "snb-interactive-v2-driver-30a73a28",
    :snb_interactive,
    ["snb-interactive-deep-delete-development-30a73a28"],
    [:driver, :operation_catalog, :deep_delete],
    "https://github.com/ldbc/ldbc_snb_interactive_v2_driver",
    "commit-30a73a28",
    "30a73a28614b3da326c851d7833ced9b17f11987",
    ["Java 21", "Apache Maven 3"],
    :work_in_progress
  )
]
