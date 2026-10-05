operation = fn id, upstream_id, family, availability, title, source_path, overrides ->
  Map.merge(
    %{
      id: "ldbc/spb/#{id}@v2.0.2",
      upstream_id: upstream_id,
      family: family,
      availability: availability,
      title: title,
      parameters: [],
      result: [],
      ordering: :not_applicable,
      limit: nil,
      variants: ["standard"],
      choke_points: [],
      frequency: %{kind: :driver_defined},
      dependencies: [],
      source_path: source_path
    },
    overrides
  )
end

aggregation =
  Enum.map(1..25, fn number ->
    operation.(
      "aggregation-#{String.pad_leading(Integer.to_string(number), 2, "0")}",
      "query#{number}",
      :aggregation,
      :mandatory,
      "SPB aggregation query #{number}",
      "datasets_and_queries/sparql/advanced/aggregation_standard/query#{number}.txt",
      %{
        parameters: [%{name: "substitutionParameters", type: "query-defined lexical values"}],
        result: [%{name: "solutions", type: "SPARQL result sequence"}],
        ordering: :query_defined,
        limit: :query_defined,
        variants: ["standard", "graphdb", "virtuoso"]
      }
    )
  end)

editorial =
  Enum.map([{"insert", "INSERT"}, {"update", "UPDATE"}, {"delete", "DELETE"}], fn {id, name} ->
    operation.(
      "editorial-#{id}",
      name,
      :editorial,
      :mandatory,
      "SPB editorial #{String.downcase(name)}",
      "datasets_and_queries/sparql/advanced/editorial/#{id}.txt",
      %{
        parameters: [%{name: "editorialParameters", type: "operation-defined lexical values"}],
        result: [%{name: "stateTransition", type: "update completion"}],
        ordering: :editorial_stream,
        variants: ["advanced", "basic"]
      }
    )
  end)

validation =
  Enum.map([{"insert", "Insert"}, {"update", "Update"}, {"delete", "Delete"}], fn {id, name} ->
    operation.(
      "validate-#{id}",
      "validate#{name}",
      :validation,
      :mandatory,
      "Validate SPB editorial #{id}",
      "datasets_and_queries/sparql/advanced/validation_standard/validate#{name}.txt",
      %{
        parameters: [%{name: "validationParameters", type: "operation-defined lexical values"}],
        result: [%{name: "valid", type: "Boolean"}],
        variants: ["standard", "graphdb", "virtuoso"]
      }
    )
  end)

conformance_rules = [
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
]

conformance =
  Enum.map(conformance_rules, fn rule ->
    operation.(
      "conformance-#{rule}",
      rule,
      :conformance,
      :mandatory,
      "OWL 2 RL conformance check #{rule}",
      "datasets_and_queries/sparql/advanced/conformance",
      %{
        result: [%{name: "conforms", type: "Boolean"}],
        frequency: %{kind: :validation_phase}
      }
    )
  end)

lifecycle_definitions = [
  {"load-ontologies", "loadOntologies", "Load benchmark ontologies", []},
  {"load-reference-datasets", "loadDatasets", "Load reference datasets", [:load_ontologies]},
  {"generate-creative-works", "generateCreativeWorks", "Generate creative works",
   [:load_ontologies, :load_reference_datasets]},
  {"load-creative-works", "loadCreativeWorks", "Load generated creative works",
   [:generate_creative_works]},
  {"generate-parameters", "generateQuerySubstitutionParameters",
   "Generate substitution parameters", [:load_creative_works]},
  {"validate-query-results", "validateQueryResults", "Validate aggregation and editorial results",
   [:load_reference_datasets]},
  {"warm-up", "warmUp", "Warm up aggregation queries", [:generate_parameters]},
  {"benchmark", "benchmark", "Run mixed aggregation and editorial workload",
   [:generate_parameters]},
  {"cleanup", "cleanup", "Clear the benchmark repository", []}
]

lifecycle =
  Enum.map(lifecycle_definitions, fn {id, upstream_id, title, dependencies} ->
    operation.(
      id,
      upstream_id,
      :lifecycle,
      if(id == "cleanup", do: :optional, else: :mandatory),
      title,
      "readme.txt#Benchmark-Phases",
      %{
        result: [%{name: "completed", type: "Boolean"}],
        frequency: %{kind: :benchmark_phase},
        dependencies: dependencies
      }
    )
  end)

resilience_definitions = [
  {"online-replication-backup", "benchmarkOnlineReplicationAndBackup",
   "Measure under online replication and backup"},
  {"full-backup-start", "full_backup_start", "Start a full backup"},
  {"full-backup-restore", "full_backup_restore", "Restore a full backup"},
  {"system-shutdown", "system_shutdown", "Stop the benchmark database"},
  {"system-start", "system_start", "Start the benchmark database"}
]

resilience =
  Enum.map(resilience_definitions, fn {id, upstream_id, title} ->
    operation.(
      "resilience-#{id}",
      upstream_id,
      :resilience,
      :audit_only,
      title,
      "datasets_and_queries/scripts/enterprise",
      %{
        result: [%{name: "milestone", type: "operational event"}],
        frequency: %{kind: :resilience_phase},
        dependencies: [:vendor_implementation]
      }
    )
  end)

operations = aggregation ++ editorial ++ validation ++ conformance ++ lifecycle ++ resilience

%{
  id: "ldbc-spb-v2.0.2",
  benchmark: :spb,
  version: "v2.0.2",
  source_id: "spb-2.0.2",
  expected_operation_ids: Enum.map(operations, & &1.id),
  operations: operations
}
