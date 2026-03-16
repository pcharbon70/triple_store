defmodule TripleStore.Specs.Validator do
  @moduledoc """
  Validates the repository-local `specs/` governance and conformance surfaces.

  The validator is intentionally documentation-centric. It checks the current
  markdown-based traceability model rather than attempting to execute feature
  scenarios directly.
  """

  @type mode :: :all | :governance | :conformance

  @type report :: %{
          errors: [String.t()],
          warnings: [String.t()],
          stats: %{
            requirements: non_neg_integer(),
            scenarios: non_neg_integer(),
            acceptances: non_neg_integer(),
            adrs: non_neg_integer(),
            matrix_rows: non_neg_integer()
          }
        }

  @req_id_regex ~r/\bREQ-[A-Z]+-\d{3}\b/
  @req_family_regex ~r/REQ-[A-Z]+-\*/
  @scn_id_regex ~r/\bSCN-\d{3}\b/
  @ac_id_regex ~r/\bAC-[A-Z]+-\d+\b/
  @adr_id_regex ~r/\bADR-\d{4}\b/
  @repo_path_regex ~r/\b(?:specs|test|lib|notes|scripts)\/[A-Za-z0-9_\/\.\-]+\.(?:md|exs|ex|sh)\b/
  @divider_regex ~r/^\s*\|(?:\s*:?-{3,}:?\s*\|)+\s*$/

  @required_spec_files [
    "specs/README.md",
    "specs/architecture-overview.md",
    "specs/topology.md",
    "specs/boundaries.md",
    "specs/control-planes.md",
    "specs/specs-governance-and-compliance-guide.md",
    "specs/contracts/README.md",
    "specs/contracts/control_plane_ownership_matrix.md",
    "specs/contracts/storage_runtime_contract.md",
    "specs/contracts/query_execution_contract.md",
    "specs/contracts/transaction_and_isolation_contract.md",
    "specs/contracts/reasoning_contract.md",
    "specs/contracts/observability_contract.md",
    "specs/conformance/README.md",
    "specs/conformance/scenario_catalog.md",
    "specs/conformance/spec_conformance_matrix.md",
    "specs/planning/README.md",
    "specs/operations/README.md",
    "specs/adr/ADR-0001-control-plane-authority.md",
    "specs/runtime/README.md",
    "specs/storage/README.md",
    "specs/query/README.md",
    "specs/reasoning/README.md"
  ]

  @ownership_required_paths [
    "specs/architecture-overview.md",
    "specs/topology.md",
    "specs/boundaries.md",
    "specs/control-planes.md",
    "specs/runtime/README.md",
    "specs/storage/README.md",
    "specs/query/README.md",
    "specs/reasoning/README.md"
  ]

  @area_index_paths [
    "specs/runtime/README.md",
    "specs/storage/README.md",
    "specs/query/README.md",
    "specs/reasoning/README.md"
  ]

  @component_dirs [
    "specs/runtime",
    "specs/storage",
    "specs/query",
    "specs/reasoning",
    "specs/operations"
  ]

  @doc """
  Validate the `specs/` tree.

  Supported options:

  - `:spec_root` - path to the `specs/` directory
  - `:mode` - `:governance`, `:conformance`, or `:all`
  """
  @spec validate(keyword()) :: {:ok, report()} | {:error, report()}
  def validate(opts \\ []) do
    spec_root =
      opts
      |> Keyword.get(:spec_root, Path.expand("specs", File.cwd!()))
      |> Path.expand()

    mode = opts |> Keyword.get(:mode, :all) |> normalize_mode()
    context = load_context(spec_root)

    {errors, warnings} =
      case mode do
        :governance ->
          {governance_errors(context), governance_warnings(context)}

        :conformance ->
          {conformance_errors(context), []}

        :all ->
          {governance_errors(context) ++ conformance_errors(context),
           governance_warnings(context)}
      end

    report = %{
      errors: errors |> Enum.uniq() |> Enum.sort(),
      warnings: warnings |> Enum.uniq() |> Enum.sort(),
      stats: build_stats(context)
    }

    if report.errors == [] do
      {:ok, report}
    else
      {:error, report}
    end
  end

  defp normalize_mode(mode) when mode in [:all, :governance, :conformance], do: mode
  defp normalize_mode(_mode), do: :all

  defp governance_errors(context) do
    []
    |> Kernel.++(missing_required_files(context))
    |> Kernel.++(duplicate_identifier_errors(context))
    |> Kernel.++(ownership_matrix_errors(context))
    |> Kernel.++(area_index_shape_errors(context))
    |> Kernel.++(component_doc_shape_errors(context))
    |> Kernel.++(tracked_native_artifact_errors(context))
  end

  defp governance_warnings(context) do
    if context.matrix_rows == [] do
      ["specs/conformance/spec_conformance_matrix.md did not yield any parsed matrix rows"]
    else
      []
    end
  end

  defp conformance_errors(context) do
    []
    |> Kernel.++(matrix_reference_errors(context))
    |> Kernel.++(requirement_coverage_errors(context))
    |> Kernel.++(scenario_coverage_errors(context))
    |> Kernel.++(area_acceptance_mapping_errors(context))
    |> Kernel.++(acceptance_evidence_errors(context))
  end

  defp missing_required_files(context) do
    Enum.flat_map(@required_spec_files, fn relative_path ->
      if Map.has_key?(context.contents, relative_path) do
        []
      else
        ["missing required specs file: #{relative_path}"]
      end
    end)
  end

  defp duplicate_identifier_errors(context) do
    []
    |> Kernel.++(duplicate_errors(context.req_ids_by_contract, "REQ"))
    |> Kernel.++(duplicate_errors(context.scn_ids_by_file, "SCN"))
    |> Kernel.++(duplicate_errors(context.ac_ids_by_file, "AC"))
    |> Kernel.++(duplicate_errors(context.adr_ids_by_file, "ADR"))
  end

  defp duplicate_errors(ids_by_file, label) do
    ids_by_file
    |> Enum.reduce(%{}, fn {path, ids}, acc ->
      Enum.reduce(ids, acc, fn id, inner_acc ->
        Map.update(inner_acc, id, [path], &[path | &1])
      end)
    end)
    |> Enum.flat_map(fn {id, paths} ->
      unique_paths = paths |> Enum.uniq() |> Enum.sort()

      if length(unique_paths) > 1 do
        ["duplicate #{label} identifier #{id} defined in #{Enum.join(unique_paths, ", ")}"]
      else
        []
      end
    end)
  end

  defp ownership_matrix_errors(context) do
    ownership_paths =
      context.ownership_rows
      |> Enum.flat_map(fn row -> extract_repo_paths(row["Area"] || "") end)
      |> MapSet.new()

    missing_entries =
      Enum.flat_map(@ownership_required_paths, fn relative_path ->
        if MapSet.member?(ownership_paths, relative_path) do
          []
        else
          ["control-plane ownership matrix is missing required path #{relative_path}"]
        end
      end)

    broken_paths =
      context.ownership_rows
      |> Enum.flat_map(fn row ->
        row["Area"]
        |> extract_repo_paths()
        |> Enum.flat_map(
          &missing_file_errors(
            context.repo_root,
            &1,
            "control-plane ownership matrix references missing path"
          )
        )
      end)

    missing_entries ++ broken_paths
  end

  defp area_index_shape_errors(context) do
    Enum.flat_map(@area_index_paths, fn path ->
      content = Map.get(context.contents, path, "")
      tables = Map.get(context.acceptance_tables, path, [])
      errors = []

      errors =
        if String.contains?(content, "## Control Plane") do
          errors
        else
          ["#{path} is missing a `## Control Plane` section" | errors]
        end

      case List.first(tables) do
        nil ->
          ["#{path} is missing an acceptance-criteria table" | errors]

        table ->
          errors
          |> maybe_add_error(
            "Related Requirements" in table.headers,
            "#{path} acceptance table is missing a `Related Requirements` column"
          )
          |> maybe_add_error(
            "Related Scenarios" in table.headers,
            "#{path} acceptance table is missing a `Related Scenarios` column"
          )
      end
    end)
  end

  defp component_doc_shape_errors(context) do
    context.component_doc_paths
    |> Enum.flat_map(fn path ->
      content = Map.get(context.contents, path, "")
      tables = Map.get(context.acceptance_tables, path, [])
      errors = []

      errors =
        if String.contains?(content, "## Control Plane") do
          errors
        else
          ["#{path} is missing a `## Control Plane` section" | errors]
        end

      if tables == [] do
        ["#{path} is missing an acceptance-criteria table" | errors]
      else
        errors
      end
    end)
  end

  defp tracked_native_artifact_errors(context) do
    Enum.map(context.tracked_native_artifacts, fn relative_path ->
      "generated native artifact is tracked in git: #{relative_path}"
    end)
  end

  defp matrix_reference_errors(context) do
    Enum.flat_map(context.matrix_rows, fn row ->
      []
      |> Kernel.++(matrix_contract_errors(context, row))
      |> Kernel.++(matrix_requirement_errors(context, row))
      |> Kernel.++(matrix_primary_spec_errors(context, row))
      |> Kernel.++(matrix_scenario_errors(context, row))
    end)
  end

  defp matrix_contract_errors(context, row) do
    case extract_link_target(row["Owning Contract"] || "", context.matrix_path, context.repo_root) do
      nil ->
        ["#{row.path}:#{row.line} matrix row is missing an owning-contract link"]

      contract_path ->
        if file_exists?(context.repo_root, contract_path) do
          []
        else
          ["#{row.path}:#{row.line} matrix row references missing contract #{contract_path}"]
        end
    end
  end

  defp matrix_requirement_errors(context, row) do
    contract_path =
      extract_link_target(row["Owning Contract"] || "", context.matrix_path, context.repo_root)

    expanded_requirements = expand_requirement_expr(row["Requirement"] || "")

    no_requirements =
      if expanded_requirements == [] do
        ["#{row.path}:#{row.line} matrix row did not expand to any concrete REQ identifiers"]
      else
        []
      end

    missing_in_contract =
      Enum.flat_map(expanded_requirements, fn req_id ->
        contract_ids = Map.get(context.req_ids_by_contract, contract_path, MapSet.new())

        if MapSet.member?(contract_ids, req_id) do
          []
        else
          [
            "#{row.path}:#{row.line} matrix row assigns #{req_id} to #{contract_path}, but that identifier is not defined there"
          ]
        end
      end)

    no_requirements ++ missing_in_contract
  end

  defp matrix_primary_spec_errors(context, row) do
    spec_paths = extract_repo_paths(row["Primary Specs"] || "")

    missing_paths =
      Enum.flat_map(spec_paths, fn relative_path ->
        if file_exists?(context.repo_root, relative_path) do
          []
        else
          ["#{row.path}:#{row.line} matrix row references missing primary spec #{relative_path}"]
        end
      end)

    if spec_paths == [] do
      ["#{row.path}:#{row.line} matrix row does not reference any primary specs" | missing_paths]
    else
      missing_paths
    end
  end

  defp matrix_scenario_errors(context, row) do
    scenarios = scan_ids(row["Scenario"] || "", @scn_id_regex)

    errors =
      Enum.flat_map(scenarios, fn scenario_id ->
        if MapSet.member?(context.scenario_ids, scenario_id) do
          []
        else
          ["#{row.path}:#{row.line} matrix row references unknown scenario #{scenario_id}"]
        end
      end)

    if scenarios == [] do
      ["#{row.path}:#{row.line} matrix row does not reference any scenarios" | errors]
    else
      errors
    end
  end

  defp requirement_coverage_errors(context) do
    matrix_requirement_ids =
      context.matrix_rows
      |> Enum.flat_map(fn row -> expand_requirement_expr(row["Requirement"] || "") end)
      |> MapSet.new()

    context.requirement_ids
    |> Enum.reject(&MapSet.member?(matrix_requirement_ids, &1))
    |> Enum.map(fn req_id ->
      "conformance matrix does not cover requirement #{req_id}"
    end)
  end

  defp scenario_coverage_errors(context) do
    matrix_scenarios =
      context.matrix_rows
      |> Enum.flat_map(fn row -> scan_ids(row["Scenario"] || "", @scn_id_regex) end)
      |> MapSet.new()

    context.scenario_ids
    |> Enum.reject(&MapSet.member?(matrix_scenarios, &1))
    |> Enum.map(fn scenario_id ->
      "conformance matrix does not reference scenario #{scenario_id}"
    end)
  end

  defp area_acceptance_mapping_errors(context) do
    Enum.flat_map(@area_index_paths, fn path ->
      tables = Map.get(context.acceptance_tables, path, [])

      Enum.flat_map(tables, fn table ->
        Enum.flat_map(table.rows, &area_acceptance_row_errors(context, path, &1))
      end)
    end)
  end

  defp acceptance_evidence_errors(context) do
    Enum.flat_map(context.acceptance_tables, fn {path, tables} ->
      Enum.flat_map(tables, &acceptance_table_evidence_errors(context, path, &1))
    end)
  end

  defp maybe_add_error(errors, true, _message), do: errors
  defp maybe_add_error(errors, false, message), do: [message | errors]

  defp maybe_append(errors, false, _message), do: errors
  defp maybe_append(errors, true, message), do: [message | errors]

  defp load_context(spec_root) do
    repo_root = Path.dirname(spec_root)
    files = Path.wildcard(Path.join(spec_root, "**/*.md")) |> Enum.sort()

    contents =
      Map.new(files, fn absolute_path ->
        {Path.relative_to(absolute_path, repo_root), File.read!(absolute_path)}
      end)

    contract_paths =
      files
      |> Enum.map(&Path.relative_to(&1, repo_root))
      |> Enum.filter(
        &(String.starts_with?(&1, "specs/contracts/") and &1 != "specs/contracts/README.md")
      )

    req_ids_by_contract = ids_by_file(contract_paths, contents, @req_id_regex)

    scn_ids_by_file =
      ids_by_file(["specs/conformance/scenario_catalog.md"], contents, @scn_id_regex)

    adr_ids_by_file = ids_by_file(adr_paths(files, repo_root), contents, @adr_id_regex)
    ac_ids_by_file = ids_by_file(acceptance_doc_paths(files, repo_root), contents, @ac_id_regex)

    acceptance_tables =
      acceptance_doc_paths(files, repo_root)
      |> Map.new(fn path ->
        tables =
          contents
          |> Map.fetch!(path)
          |> parse_tables()
          |> Enum.filter(fn table -> "Acceptance ID" in table.headers end)

        {path, tables}
      end)

    matrix_path = "specs/conformance/spec_conformance_matrix.md"
    ownership_path = "specs/contracts/control_plane_ownership_matrix.md"

    %{
      repo_root: repo_root,
      spec_root: spec_root,
      contents: contents,
      req_ids_by_contract: req_ids_by_contract,
      scn_ids_by_file: scn_ids_by_file,
      adr_ids_by_file: adr_ids_by_file,
      ac_ids_by_file: ac_ids_by_file,
      requirement_ids: req_ids_by_contract |> union_ids() |> MapSet.new(),
      requirement_families: requirement_families(req_ids_by_contract),
      scenario_ids: scn_ids_by_file |> union_ids() |> MapSet.new(),
      acceptance_ids: ac_ids_by_file |> union_ids() |> MapSet.new(),
      adr_ids: adr_ids_by_file |> union_ids() |> MapSet.new(),
      acceptance_tables: acceptance_tables,
      matrix_path: matrix_path,
      matrix_rows: parse_named_table(contents[matrix_path], "Requirement", matrix_path),
      ownership_rows: parse_named_table(contents[ownership_path], "Area", ownership_path),
      component_doc_paths: component_doc_paths(files, repo_root),
      tracked_native_artifacts: tracked_native_artifacts(repo_root)
    }
  end

  defp ids_by_file(paths, contents, regex) do
    Map.new(paths, fn path ->
      {path, contents |> Map.get(path, "") |> scan_ids(regex) |> MapSet.new()}
    end)
  end

  defp union_ids(ids_by_file) do
    ids_by_file
    |> Enum.reduce(MapSet.new(), fn {_path, ids}, acc -> MapSet.union(acc, ids) end)
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp requirement_families(req_ids_by_contract) do
    req_ids_by_contract
    |> union_ids()
    |> Enum.map(fn req_id -> req_id |> String.replace(~r/\d{3}$/, "*") end)
    |> MapSet.new()
  end

  defp adr_paths(files, repo_root) do
    files
    |> Enum.map(&Path.relative_to(&1, repo_root))
    |> Enum.filter(&String.starts_with?(&1, "specs/adr/"))
  end

  defp acceptance_doc_paths(files, repo_root) do
    files
    |> Enum.map(&Path.relative_to(&1, repo_root))
    |> Enum.filter(fn path ->
      Enum.any?(@component_dirs, &String.starts_with?(path, &1 <> "/"))
    end)
  end

  defp component_doc_paths(files, repo_root) do
    files
    |> Enum.map(&Path.relative_to(&1, repo_root))
    |> Enum.filter(fn path ->
      Enum.any?(@component_dirs, &String.starts_with?(path, &1 <> "/")) and
        not String.ends_with?(path, "/README.md")
    end)
    |> Enum.sort()
  end

  defp parse_named_table(nil, _header_name, _path), do: []

  defp parse_named_table(content, header_name, path) do
    content
    |> parse_tables()
    |> Enum.find_value([], fn table ->
      if header_name in table.headers do
        Enum.map(table.rows, &Map.put(&1, :path, path))
      else
        false
      end
    end)
  end

  defp parse_tables(content) do
    lines = String.split(content, "\n")
    do_parse_tables(lines, 1, [])
  end

  defp do_parse_tables(lines, line_number, tables) when line_number > length(lines) do
    Enum.reverse(tables)
  end

  defp do_parse_tables(lines, line_number, tables) do
    current = Enum.at(lines, line_number - 1, "")
    next = Enum.at(lines, line_number, "")

    if String.starts_with?(current, "|") and Regex.match?(@divider_regex, next) do
      headers = parse_pipe_row(current)
      {rows, next_line} = collect_table_rows(lines, line_number + 2, headers, [])
      table = %{headers: headers, rows: rows, start_line: line_number}
      do_parse_tables(lines, next_line, [table | tables])
    else
      do_parse_tables(lines, line_number + 1, tables)
    end
  end

  defp collect_table_rows(lines, line_number, _headers, rows) when line_number > length(lines) do
    {Enum.reverse(rows), line_number}
  end

  defp collect_table_rows(lines, line_number, headers, rows) do
    current = Enum.at(lines, line_number - 1, "")

    cond do
      not String.starts_with?(current, "|") ->
        {Enum.reverse(rows), line_number}

      Regex.match?(@divider_regex, current) ->
        collect_table_rows(lines, line_number + 1, headers, rows)

      true ->
        values = parse_pipe_row(current)

        row =
          headers
          |> Enum.zip(pad_values(values, length(headers)))
          |> Map.new()
          |> Map.put(:line, line_number)

        collect_table_rows(lines, line_number + 1, headers, [row | rows])
    end
  end

  defp parse_pipe_row(line) do
    line
    |> String.trim()
    |> String.trim_leading("|")
    |> String.trim_trailing("|")
    |> String.split("|")
    |> Enum.map(&String.trim/1)
  end

  defp pad_values(values, header_count) when length(values) >= header_count do
    Enum.take(values, header_count)
  end

  defp pad_values(values, header_count) do
    values ++ List.duplicate("", header_count - length(values))
  end

  defp extract_link_target(cell, base_path, repo_root) do
    case Regex.run(~r/\(([^)]+)\)/, cell, capture: :all_but_first) do
      [target] ->
        context_base =
          repo_root
          |> Path.join(base_path)
          |> Path.dirname()

        context_base
        |> Path.join(target)
        |> Path.expand()
        |> Path.relative_to(repo_root)

      _ ->
        nil
    end
  end

  defp extract_repo_paths(text) do
    text
    |> scan_ids(@repo_path_regex)
    |> Enum.uniq()
  end

  defp expand_requirement_expr(expr) do
    req_ids = scan_ids(expr, @req_id_regex)

    maybe_expand_requirement_range(expr, req_ids)
  end

  defp unknown_requirement_family_errors(context, path, line, acceptance_id, family) do
    if MapSet.member?(context.requirement_families, family) do
      []
    else
      ["#{path}:#{line} #{acceptance_id} references unknown REQ family #{family}"]
    end
  end

  defp unknown_scenario_errors(context, path, line, acceptance_id, scenario_id) do
    if MapSet.member?(context.scenario_ids, scenario_id) do
      []
    else
      ["#{path}:#{line} #{acceptance_id} references unknown scenario #{scenario_id}"]
    end
  end

  defp missing_file_errors(repo_root, relative_path, prefix) do
    if file_exists?(repo_root, relative_path) do
      []
    else
      ["#{prefix} #{relative_path}"]
    end
  end

  defp maybe_expand_requirement_range(expr, req_ids) do
    if String.contains?(expr, ".."), do: expand_requirement_range(req_ids), else: req_ids
  end

  defp area_acceptance_row_errors(context, path, row) do
    req_families = scan_ids(row["Related Requirements"] || "", @req_family_regex)
    scenarios = scan_ids(row["Related Scenarios"] || "", @scn_id_regex)
    acceptance_id = row["Acceptance ID"] || "<unknown>"

    []
    |> maybe_append(
      req_families == [],
      "#{path}:#{row.line} #{acceptance_id} does not reference any REQ family"
    )
    |> maybe_append(
      scenarios == [],
      "#{path}:#{row.line} #{acceptance_id} does not reference any SCN scenario"
    )
    |> Kernel.++(
      Enum.flat_map(req_families, fn family ->
        unknown_requirement_family_errors(context, path, row.line, acceptance_id, family)
      end)
    )
    |> Kernel.++(acceptance_scenario_errors(context, path, row.line, acceptance_id, scenarios))
  end

  defp acceptance_scenario_errors(context, path, line, acceptance_id, scenarios) do
    Enum.flat_map(scenarios, fn scenario_id ->
      unknown_scenario_errors(context, path, line, acceptance_id, scenario_id)
    end)
  end

  defp acceptance_table_evidence_errors(context, path, table) do
    Enum.flat_map(table.rows, &acceptance_row_evidence_errors(context, path, &1))
  end

  defp acceptance_row_evidence_errors(context, path, row) do
    row
    |> acceptance_evidence_paths()
    |> Enum.flat_map(fn relative_path ->
      missing_file_errors(
        context.repo_root,
        relative_path,
        "#{path}:#{row.line} acceptance evidence references missing path"
      )
    end)
  end

  defp acceptance_evidence_paths(row) do
    row
    |> Map.drop(["Acceptance ID"])
    |> Map.values()
    |> Enum.filter(&is_binary/1)
    |> Enum.flat_map(&extract_repo_paths/1)
    |> Enum.uniq()
  end

  defp expand_requirement_range([first, last]) do
    {first_prefix, first_num} = split_requirement_id(first)
    {last_prefix, last_num} = split_requirement_id(last)

    if first_prefix == last_prefix and first_num <= last_num do
      Enum.map(first_num..last_num, fn value ->
        first_prefix <> String.pad_leading(Integer.to_string(value), 3, "0")
      end)
    else
      [first, last]
    end
  end

  defp expand_requirement_range(req_ids), do: req_ids

  defp split_requirement_id(req_id) do
    [prefix, numeric] = Regex.run(~r/^(REQ-[A-Z]+-)(\d{3})$/, req_id, capture: :all_but_first)
    {prefix, String.to_integer(numeric)}
  end

  defp build_stats(context) do
    %{
      requirements: count_ids(context.requirement_ids),
      scenarios: count_ids(context.scenario_ids),
      acceptances: count_ids(context.acceptance_ids),
      adrs: count_ids(context.adr_ids),
      matrix_rows: length(context.matrix_rows)
    }
  end

  defp count_ids(%MapSet{} = ids), do: MapSet.size(ids)

  defp file_exists?(repo_root, relative_path) do
    repo_root
    |> Path.join(relative_path)
    |> File.exists?()
  end

  defp tracked_native_artifacts(repo_root) do
    case System.cmd("git", ["-C", repo_root, "ls-files", "--", "priv/native"],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        output
        |> String.split("\n", trim: true)
        |> Enum.filter(fn path ->
          String.ends_with?(path, ".so") or String.ends_with?(path, ".dylib")
        end)
        |> Enum.sort()

      {_output, _status} ->
        []
    end
  end

  defp scan_ids(text, regex) do
    regex
    |> Regex.scan(text)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.sort()
  end
end
