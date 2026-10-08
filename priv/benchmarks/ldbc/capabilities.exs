entry = fn operation_id,
           status,
           parse_support,
           execution_support,
           features,
           owner,
           phase,
           rationale,
           evidence ->
  %{
    operation_id: operation_id,
    status: status,
    parse_support: parse_support,
    execution_support: execution_support,
    features: features,
    owner: owner,
    implementation_phase: phase,
    rationale: rationale,
    evidence: evidence
  }
end

spb_aggregation =
  Enum.map(1..25, fn number ->
    id = "ldbc/spb/aggregation-#{String.pad_leading(Integer.to_string(number), 2, "0")}@v2.0.2"

    entry.(
      id,
      :requires_fix,
      :template_requires_binding,
      :unverified,
      [:sparql_1_1, :aggregation, :expressions, :ordering, :slicing, :graph_context],
      "TripleStore.SPARQL.Query",
      4,
      "The upstream SPARQL template exists, but bound-query execution and exact typed results are not yet cross-validated.",
      [
        "lib/triple_store/sparql/query.ex",
        "ldbc_spb_bm_2.0:datasets_and_queries/sparql/advanced/aggregation_standard"
      ]
    )
  end)

spb_editorial =
  Enum.map(["insert", "update", "delete"], fn name ->
    entry.(
      "ldbc/spb/editorial-#{name}@v2.0.2",
      :requires_fix,
      :template_requires_binding,
      :unverified,
      [:sparql_update, :graph_context, :atomic_mutation],
      "TripleStore.SPARQL.UpdateExecutor",
      4,
      "SPARQL update primitives exist, but SPB state transitions and graph-visible postconditions require validation.",
      [
        "lib/triple_store/sparql/update_executor.ex",
        "specs/contracts/transaction_and_isolation_contract.md"
      ]
    )
  end)

spb_validation =
  Enum.map(["insert", "update", "delete"], fn name ->
    entry.(
      "ldbc/spb/validate-#{name}@v2.0.2",
      :requires_fix,
      :template_requires_binding,
      :unverified,
      [:ask, :graph_context, :typed_comparison],
      "TripleStore.Benchmark.LDBC.SPB",
      4,
      "Validation templates are catalogued but have no TripleStore binding or accepted-answer comparison.",
      ["ldbc_spb_bm_2.0:datasets_and_queries/sparql/advanced/validation_standard"]
    )
  end)

spb_conformance =
  Enum.map(
    [
      "prp-irp",
      "prp-asyp",
      "prp-pdw",
      "prp-adp",
      "cax-dw",
      "cax-adc",
      "cls-maxc1",
      "prp-key",
      "prp-spo2",
      "prp-inv1"
    ],
    fn rule ->
      entry.(
        "ldbc/spb/conformance-#{rule}@v2.0.2",
        :requires_fix,
        :not_applicable,
        :unverified,
        [:owl2_rl, :persistent_inference, :query_visible_derived_facts],
        "TripleStore.Reasoner",
        4,
        "The rule engine has OWL 2 RL coverage, but the SPB rule set is not mapped to persistent query-visible benchmark state.",
        ["specs/contracts/reasoning_contract.md", "lib/triple_store/reasoner"]
      )
    end
  )

spb_lifecycle =
  Enum.map(
    [
      "load-ontologies",
      "load-reference-datasets",
      "generate-creative-works",
      "load-creative-works",
      "generate-parameters",
      "validate-query-results",
      "warm-up",
      "benchmark",
      "cleanup"
    ],
    fn name ->
      entry.(
        "ldbc/spb/#{name}@v2.0.2",
        :requires_fix,
        :not_applicable,
        :unverified,
        [:benchmark_lifecycle, :resource_ownership],
        "TripleStore.Benchmark.LDBC.SPB",
        4,
        "Underlying APIs exist for some phases, but the pinned SPB lifecycle is not implemented as one validated workflow.",
        ["lib/triple_store.ex", "ldbc_spb_bm_2.0:readme.txt#Benchmark-Phases"]
      )
    end
  )

spb_resilience =
  Enum.map(
    [
      "online-replication-backup",
      "full-backup-start",
      "full-backup-restore",
      "system-shutdown",
      "system-start"
    ],
    fn name ->
      {status, owner, rationale} =
        case name do
          backup when backup in ["full-backup-start", "full-backup-restore"] ->
            {:supported, "TripleStore.Benchmark.LDBC.SPB.Resilience",
             "The coordinated backup profile validates full backup, fresh-path restore, graph contexts, derived facts, and accepted answers."}

          _ ->
            {:profile_exclusion, "TripleStore.Benchmark.LDBC.SPB",
             "TripleStore has no online replication/failover product surface; the action is audit-only and excluded."}
        end

      entry.(
        "ldbc/spb/resilience-#{name}@v2.0.2",
        status,
        :not_applicable,
        if(status == :profile_exclusion, do: :unsupported, else: :verified),
        [:backup, :recovery, :availability],
        owner,
        4,
        rationale,
        [
          "lib/triple_store/benchmark/ldbc/spb/resilience.ex",
          "guides/benchmarks/ldbc-spb-resilience.md",
          "ldbc_spb_bm_2.0:datasets_and_queries/scripts/enterprise"
        ]
      )
    end
  )

bi_reads =
  Enum.map(1..20, fn number ->
    extension? = number in [10, 15, 19, 20]

    entry.(
      "ldbc/snb-bi/read-#{String.pad_leading(Integer.to_string(number), 2, "0")}@v1.0.3",
      if(extension?, do: :requires_extension, else: :requires_fix),
      :translation_required,
      if(extension?, do: :unsupported, else: :unverified),
      if(extension?,
        do: [:graph_algorithm, :shortest_path, :path_cost],
        else: [:aggregation, :subquery, :expressions, :ordering, :slicing]
      ),
      if(extension?, do: "TripleStore.SPARQL.GraphAlgorithms", else: "TripleStore.SPARQL.Query"),
      5,
      if(extension?,
        do:
          "The operation returns distance or weighted path cost, which standard SPARQL 1.1 property paths do not expose.",
        else: "An RDF/SPARQL translation and exact reference-answer validation are required."
      ),
      [
        "ldbc_snb_docs:query-specifications/bi-read-#{String.pad_leading(Integer.to_string(number), 2, "0")}.yaml",
        "lib/triple_store/sparql/query.ex"
      ]
    )
  end)

bi_updates = [
  entry.(
    "ldbc/snb-bi/update-batch@v1.0.3",
    :requires_fix,
    :translation_required,
    :unverified,
    [:ordered_microbatch, :atomic_mutation, :insert, :delete],
    "TripleStore.Transaction",
    5,
    "The BI batch must be mapped to RDF and applied with a defined request boundary and checkpoint lifecycle.",
    [
      "specs/contracts/transaction_and_isolation_contract.md",
      "ldbc_snb_bi:{cypher,tigergraph,umbra}/dml"
    ]
  )
]

interactive_complex =
  Enum.map(1..14, fn number ->
    extension? = number in [13, 14]

    entry.(
      "ldbc/snb-interactive/complex-read-#{String.pad_leading(Integer.to_string(number), 2, "0")}@v1.2.0",
      if(extension?, do: :requires_extension, else: :requires_fix),
      :translation_required,
      if(extension?, do: :unsupported, else: :unverified),
      if(extension?,
        do: [:shortest_path, :path_result],
        else: [:neighbourhood_traversal, :optional, :ordering, :top_k]
      ),
      if(extension?, do: "TripleStore.SPARQL.GraphAlgorithms", else: "TripleStore.SPARQL.Query"),
      6,
      if(extension?,
        do:
          "The operation must return shortest-path length or weighted path details unavailable from the current property-path executor.",
        else: "An RDF/SPARQL translation and exact reference-answer validation are required."
      ),
      [
        "ldbc_snb_docs:query-specifications/interactive-complex-read-#{String.pad_leading(Integer.to_string(number), 2, "0")}.yaml",
        "lib/triple_store/sparql/property_path.ex"
      ]
    )
  end)

interactive_short =
  Enum.map(1..7, fn number ->
    entry.(
      "ldbc/snb-interactive/short-read-#{String.pad_leading(Integer.to_string(number), 2, "0")}@v1.2.0",
      :requires_fix,
      :translation_required,
      :unverified,
      [:point_lookup, :ordering, :update_dependency],
      "TripleStore.SPARQL.Query",
      6,
      "The operation requires a mapped query and driver-visible post-update correctness validation.",
      [
        "ldbc_snb_docs:query-specifications/interactive-short-read-#{String.pad_leading(Integer.to_string(number), 2, "0")}.yaml"
      ]
    )
  end)

interactive_inserts =
  Enum.map(1..8, fn number ->
    entry.(
      "ldbc/snb-interactive/insert-#{String.pad_leading(Integer.to_string(number), 2, "0")}@v1.2.0",
      :requires_fix,
      :translation_required,
      :unverified,
      [:atomic_mutation, :ordered_update_stream, :cache_invalidation],
      "TripleStore.Transaction",
      6,
      "The mapped RDF mutation and benchmark read/write visibility contract are not implemented.",
      [
        "specs/contracts/transaction_and_isolation_contract.md",
        "ldbc_snb_docs:query-specifications/insert-*.yaml"
      ]
    )
  end)

interactive_v2 =
  Enum.map(
    [
      "complex-read-03a",
      "complex-read-03b",
      "complex-read-13a",
      "complex-read-13b",
      "complex-read-14a",
      "complex-read-14b"
    ] ++ Enum.map(1..8, &"delete-#{String.pad_leading(Integer.to_string(&1), 2, "0")}"),
    fn name ->
      entry.(
        "ldbc/snb-interactive/#{name}@commit-30a73a28",
        :profile_exclusion,
        :translation_required,
        :unsupported,
        if(String.starts_with?(name, "delete"),
          do: [:deep_delete, :cascade, :atomic_mutation],
          else: [:version_specific_path_semantics]
        ),
        "TripleStore.Benchmark.LDBC.Interactive",
        6,
        "The Interactive v2 source is work in progress and remains outside stable and comparable profiles.",
        ["ldbc_snb_interactive_v2_driver@30a73a28"]
      )
    end
  )

operations =
  spb_aggregation ++
    spb_editorial ++
    spb_validation ++
    spb_conformance ++
    spb_lifecycle ++
    spb_resilience ++
    bi_reads ++
    bi_updates ++
    interactive_complex ++
    interactive_short ++
    interactive_inserts ++ interactive_v2

finding = fn id, area, status, observed, required, owner, phase, evidence ->
  %{
    id: id,
    area: area,
    status: status,
    observed_behavior: observed,
    required_behavior: required,
    owner: owner,
    implementation_phase: phase,
    evidence: evidence
  }
end

system_findings = [
  finding.(
    "LDBC-CAP-001",
    :query,
    :requires_fix,
    "The parser and executor support major SPARQL 1.1 constructs, but no complete LDBC translation has passed typed answer comparison.",
    "Each translated operation must pass reference-answer validation before timing.",
    "TripleStore.SPARQL.Query",
    3,
    ["lib/triple_store/sparql/query.ex"]
  ),
  finding.(
    "LDBC-CAP-002",
    :graph_algorithms,
    :requires_extension,
    "Property paths return reachability bindings but do not expose path length, path identity, or weighted cost.",
    "BI 10/15/19/20 and Interactive 13/14 need engine-owned path algorithms with typed results.",
    "TripleStore.SPARQL.GraphAlgorithms",
    5,
    ["lib/triple_store/sparql/property_path.ex"]
  ),
  finding.(
    "LDBC-CAP-003",
    :transactions,
    :requires_fix,
    "TripleStore.query/3 bypasses the store transaction coordinator while coordinator queries serialize behind updates.",
    "Interactive benchmark reads and writes need one documented visibility boundary.",
    "TripleStore.Transaction",
    6,
    ["specs/contracts/transaction_and_isolation_contract.md"]
  ),
  finding.(
    "LDBC-CAP-004",
    :transactions,
    :requires_fix,
    "Direct insert, delete, and loader writes remain outside the transaction queue.",
    "Comparable workload mutation must reject uncoordinated entry paths.",
    "TripleStore.Transaction",
    6,
    ["lib/triple_store.ex", "specs/contracts/transaction_and_isolation_contract.md"]
  ),
  finding.(
    "LDBC-CAP-005",
    :reasoning,
    :requires_fix,
    "Local materialize/2 computes inferred facts in memory and discards the fact set; graph-scoped quad reasoning is the persistent graph-aware path.",
    "SPB inference must be persistent, context-preserving, query-visible, and validated against its rule set.",
    "TripleStore.Reasoner",
    4,
    ["specs/contracts/reasoning_contract.md"]
  ),
  finding.(
    "LDBC-CAP-006",
    :contexts,
    :requires_fix,
    "Quad schema preserves named graphs; triple schema is default-graph only.",
    "The canonical SPB profile must use quad schema and preserve source contexts through load, update, reasoning, and export.",
    "TripleStore.QuadOperations",
    4,
    ["specs/contracts/storage_runtime_contract.md"]
  ),
  finding.(
    "LDBC-CAP-007",
    :optional_indices,
    :profile_exclusion,
    "TripleStore has no SPB-specific text or geospatial index surface.",
    "Profiles must mark upstream optional text and geospatial features excluded unless implemented and validated later.",
    "TripleStore.Benchmark.LDBC.SPB",
    4,
    ["ldbc_spb_bm_2.0:readme.md"]
  ),
  finding.(
    "LDBC-CAP-008",
    :resilience,
    :requires_fix,
    "Backup and restore exist, but no SPB milestone adapter is present.",
    "Supported resilience profiles need driver actions and answer checks around restore.",
    "TripleStore.Backup",
    4,
    ["lib/triple_store/backup.ex"]
  ),
  finding.(
    "LDBC-CAP-009",
    :resilience,
    :profile_exclusion,
    "TripleStore has no online replication or failover product surface.",
    "Replication and failover claims must fail profile validation.",
    "TripleStore.Benchmark.LDBC.SPB",
    4,
    ["lib/triple_store"]
  ),
  finding.(
    "LDBC-CAP-010",
    :runner,
    :requires_fix,
    "The generic benchmark runner can time failed operations and is not protocol-aware.",
    "LDBC scoring must use a fail-fast runner that excludes every failed, timed-out, or incorrect sample.",
    "TripleStore.Benchmark.LDBC.Runner",
    3,
    ["lib/triple_store/benchmark/runner.ex"]
  ),
  finding.(
    "LDBC-CAP-011",
    :adapter,
    :requires_fix,
    "No upstream Java-driver bridge exists.",
    "Implement ADR-0002's benchmark-owned framed Port protocol with typed values, cancellation, timeouts, and deterministic shutdown.",
    "TripleStore.Benchmark.LDBC.Bridge",
    3,
    ["specs/adr/ADR-0002-ldbc-benchmark-boundary.md"]
  ),
  finding.(
    "LDBC-CAP-012",
    :rdf_mapping,
    :requires_fix,
    "SNB is language-neutral and no canonical RDF mapping is currently owned by the repository.",
    "Phase 2 must define one versioned mapping used by data conversion, translations, validation, and artifacts.",
    "TripleStore.Benchmark.LDBC.SNBMapping",
    2,
    ["specs/adr/ADR-0002-ldbc-benchmark-boundary.md"]
  )
]

%{
  version: 1,
  architecture_decision: "ADR-0002",
  catalog_versions: ["v2.0.2", "v1.0.3", "v1.2.0", "commit-30a73a28"],
  operations: operations,
  system_findings: system_findings
}
