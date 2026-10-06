canonical = %{
  complete_operation_mix: true,
  canonical_parameters: true,
  canonical_schedule: true,
  canonical_validation: true,
  canonical_scoring: true
}

profile = fn id, benchmark, version, level, sources, catalog, protocol, evidence, description ->
  %{
    id: id,
    benchmark: benchmark,
    version: version,
    claim_level: level,
    source_ids: sources,
    catalog: catalog,
    protocol: protocol,
    score_namespace:
      if(level in [:comparable, :audit_preparation], do: :canonical, else: :diagnostic),
    required_evidence: evidence,
    description: description
  }
end

suite_profiles = fn benchmark, version, prefix, sources, catalog ->
  [
    profile.(
      "#{prefix}-smoke-#{version}",
      benchmark,
      version,
      :smoke,
      sources,
      catalog,
      %{
        canonical
        | complete_operation_mix: false,
          canonical_parameters: false,
          canonical_schedule: false,
          canonical_validation: false,
          canonical_scoring: false
      },
      [:correctness, :environment, :provenance],
      "Offline reduced fixture and representative operation coverage; never comparable."
    ),
    profile.(
      "#{prefix}-development-#{version}",
      benchmark,
      version,
      :development,
      sources,
      catalog,
      %{
        canonical
        | complete_operation_mix: false,
          canonical_schedule: false,
          canonical_scoring: false
      },
      [:correctness, :environment, :provenance],
      "Selected families or shortened durations for engineering work; never comparable."
    ),
    profile.(
      "#{prefix}-comparable-#{version}",
      benchmark,
      version,
      :comparable,
      sources,
      catalog,
      canonical,
      [:correctness, :environment, :provenance, :protocol_log],
      "Complete pinned upstream protocol suitable for qualified comparisons."
    ),
    profile.(
      "#{prefix}-audit-preparation-#{version}",
      benchmark,
      version,
      :audit_preparation,
      sources,
      catalog,
      canonical,
      [
        :correctness,
        :environment,
        :provenance,
        :protocol_log,
        :configuration,
        :pricing,
        :full_disclosure
      ],
      "Comparable protocol plus evidence collection for external audit preparation."
    )
  ]
end

suite_profiles.(
  :spb,
  "v2.0.2",
  "spb",
  ["spb-2.0.2"],
  "ldbc-spb-v2.0.2"
) ++
  suite_profiles.(
    :snb_bi,
    "v1.0.3",
    "snb-bi",
    ["snb-specification-2.2.4", "snb-bi-datagen-0.5.1", "snb-bi-1.0.3"],
    "ldbc-snb-bi-v1.0.3"
  ) ++
  suite_profiles.(
    :snb_interactive,
    "v1.2.0",
    "snb-interactive",
    [
      "snb-specification-2.2.4",
      "snb-interactive-v1-datagen-1.0.0",
      "snb-interactive-v1-driver-1.2.0",
      "snb-interactive-v1-impls-1.0.0"
    ],
    "ldbc-snb-interactive-v1.2.0"
  ) ++
  [
    profile.(
      "snb-interactive-deep-delete-development-30a73a28",
      :snb_interactive,
      "commit-30a73a28",
      :development,
      ["snb-specification-2.2.4", "snb-bi-datagen-0.5.1", "snb-interactive-v2-driver-30a73a28"],
      "ldbc-snb-interactive-v2-30a73a28",
      %{
        canonical
        | complete_operation_mix: false,
          canonical_schedule: false,
          canonical_validation: false,
          canonical_scoring: false
      },
      [:correctness, :environment, :provenance, :readiness],
      "Separately versioned work-in-progress profile for Interactive v2 deep deletes."
    )
  ]
