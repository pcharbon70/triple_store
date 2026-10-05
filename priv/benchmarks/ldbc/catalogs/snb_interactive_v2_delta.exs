operation = fn id, upstream_id, family, title, source_path ->
  %{
    id: "ldbc/snb-interactive/#{id}@commit-30a73a28",
    upstream_id: upstream_id,
    family: family,
    availability: :version_specific,
    title: title,
    parameters: [],
    result: [],
    ordering: :driver_defined,
    limit: nil,
    variants: ["interactive-v2-work-in-progress"],
    choke_points: [],
    frequency: %{kind: :driver_defined},
    dependencies: [:work_in_progress_profile],
    source_path: source_path
  }
end

read_variants = [
  {"complex-read-03a", "Interactive Complex Read 3a"},
  {"complex-read-03b", "Interactive Complex Read 3b"},
  {"complex-read-13a", "Interactive Complex Read 13a"},
  {"complex-read-13b", "Interactive Complex Read 13b"},
  {"complex-read-14a", "Interactive Complex Read 14a"},
  {"complex-read-14b", "Interactive Complex Read 14b"}
]

reads =
  Enum.map(read_variants, fn {id, title} ->
    operation.(
      id,
      title,
      :complex_read,
      title,
      "src/main/java/org/ldbcouncil/snb/driver/workloads/interactive/queries"
    )
  end)

delete_titles = [
  "Remove person",
  "Remove post like",
  "Remove comment like",
  "Remove forum",
  "Remove forum membership",
  "Remove post thread",
  "Remove comment subthread",
  "Remove friendship"
]

deletes =
  delete_titles
  |> Enum.with_index(1)
  |> Enum.map(fn {title, number} ->
    operation.(
      "delete-#{String.pad_leading(Integer.to_string(number), 2, "0")}",
      "Interactive Delete #{number}",
      :editorial,
      title,
      "src/main/java/org/ldbcouncil/snb/driver/workloads/interactive/queries"
    )
  end)

operations = reads ++ deletes

%{
  id: "ldbc-snb-interactive-v2-30a73a28",
  benchmark: :snb_interactive,
  version: "commit-30a73a28",
  source_id: "snb-interactive-v2-driver-30a73a28",
  expected_operation_ids: Enum.map(operations, & &1.id),
  operations: operations
}
