defmodule TripleStore.Benchmark.LDBC.SNB.BI.TranslationCorpus do
  @moduledoc """
  Loads and structurally validates the versioned SNB BI SPARQL corpus.

  The canonical BI catalog owns parameter and result contracts. Translation
  artifacts own only the RDF/SPARQL mapping of those contracts and immutable
  source provenance. Graph-algorithm extensions are deliberately excluded.

  A successful load proves corpus coverage, provenance, integrity, parser
  acceptance, parameter placeholders, projected columns, and named-graph
  assumptions. It does not claim reference-answer correctness; that requires
  the separate SNB BI correctness qualification.
  """

  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.SPARQL.Parser

  @graph_operations MapSet.new([10, 15, 19, 20])
  @upstream_commit "5f7967235593eefa98b336e240039780b6a76a9a"
  @transformation_version "triplestore-snb-bi-v1"
  @initial_graph "https://ldbcouncil.org/snb/graph/snb_bi/initial"
  @updates_graph "https://ldbcouncil.org/snb/graph/snb_bi/updates"
  @allowed_graphs MapSet.new([@initial_graph, @updates_graph])
  @placeholder_regex ~r/\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/
  @dataset_clause_regex ~r/(?:FROM(?:\s+NAMED)?|GRAPH)\s*<([^>]+)>/i

  @type entry :: %{
          required(:operation_id) => String.t(),
          required(:variant) => String.t(),
          required(:path) => String.t(),
          required(:source_card) => String.t(),
          required(:source_card_commit) => String.t(),
          required(:reference_query) => String.t(),
          required(:upstream_commit) => String.t(),
          required(:transformation_version) => String.t(),
          required(:mapping_version) => String.t(),
          required(:checksum) => String.t(),
          required(:placeholders) => [String.t()],
          required(:projected_columns) => [String.t()],
          required(:sparql) => String.t(),
          required(:ast) => term()
        }

  @type validation_error ::
          {:translation, String.t() | nil, String.t() | nil, String.t() | nil, atom(), term()}

  @type reference_rule ::
          %{required(:kind) => :scalar}
          | %{required(:kind) => :entity_id | :entity_name, required(:entity) => String.t()}

  @type reference_metadata :: %{
          required(:rules) => %{pos_integer() => %{String.t() => reference_rule()}},
          required(:profiles) => %{atom() => map()}
        }

  @doc "Returns the repository-owned translation corpus root."
  @spec default_root() :: Path.t()
  def default_root do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../../../priv/benchmarks/ldbc/snb-bi/translations", __DIR__)

      priv_dir ->
        Path.join(to_string(priv_dir), "benchmarks/ldbc/snb-bi/translations")
    end
  end

  @doc "Loads and validates the complete standard-query translation corpus."
  @spec load([map()], keyword()) :: {:ok, [entry()]} | {:error, [validation_error()]}
  def load(definitions, opts \\ [])

  def load(definitions, opts) when is_list(definitions) do
    root = Keyword.get(opts, :root, default_root())

    with {:ok, manifest} <- read_manifest(root) do
      validate_and_load(manifest, definitions, root)
    else
      {:error, reason} -> {:error, [error(nil, nil, "manifest.exs", :manifest, reason)]}
    end
  end

  def load(_definitions, _opts),
    do: {:error, [error(nil, nil, nil, :definitions, :must_be_a_list)]}

  @doc "Loads the parameter-reference rules and reduced-fixture profiles."
  @spec reference_metadata([map()], keyword()) ::
          {:ok, reference_metadata()} | {:error, [validation_error()]}
  def reference_metadata(definitions, opts \\ [])

  def reference_metadata(definitions, opts) when is_list(definitions) do
    root = Keyword.get(opts, :root, default_root())

    with {:ok, manifest} <- read_manifest(root),
         [] <- reference_metadata_errors(manifest, definitions) do
      {:ok,
       %{
         rules: Map.fetch!(manifest, :parameter_references),
         profiles: Map.fetch!(manifest, :reference_profiles)
       }}
    else
      {:error, reason} -> {:error, [error(nil, nil, "manifest.exs", :manifest, reason)]}
      errors when is_list(errors) -> {:error, errors}
    end
  end

  def reference_metadata(_definitions, _opts),
    do: {:error, [error(nil, nil, nil, :definitions, :must_be_a_list)]}

  defp validate_and_load(manifest, definitions, root) when is_map(manifest) do
    expected = expected_definitions(definitions)
    entries = Map.get(manifest, :translations, [])

    errors =
      manifest_errors(manifest, entries)
      |> Kernel.++(reference_metadata_errors(manifest, definitions))
      |> Kernel.++(coverage_errors(entries, expected))
      |> Kernel.++(unmanifested_errors(entries, root))

    {loaded, artifact_errors} = load_entries(entries, expected, root)
    errors = errors ++ artifact_errors

    if errors == [] do
      {:ok, Enum.sort_by(loaded, &entry_key/1)}
    else
      {:error, errors}
    end
  end

  defp validate_and_load(_manifest, _definitions, _root),
    do: {:error, [error(nil, nil, "manifest.exs", :manifest, :must_be_a_map)]}

  defp read_manifest(root) do
    path = Path.join(root, "manifest.exs")

    if File.regular?(path) do
      {manifest, _binding} = Code.eval_file(path)
      {:ok, manifest}
    else
      {:error, {:manifest_not_found, path}}
    end
  rescue
    exception -> {:error, {:invalid_manifest, Exception.message(exception)}}
  end

  defp manifest_errors(manifest, entries) do
    []
    |> require(is_list(entries), nil, nil, "manifest.exs", :translations, :must_be_a_list)
    |> require(
      Map.get(manifest, :mapping_version) == Mapping.version(),
      nil,
      nil,
      "manifest.exs",
      :mapping_version,
      %{expected: Mapping.version(), actual: Map.get(manifest, :mapping_version)}
    )
    |> require(
      Map.get(manifest, :upstream_commit) == @upstream_commit,
      nil,
      nil,
      "manifest.exs",
      :upstream_commit,
      Map.get(manifest, :upstream_commit)
    )
    |> require(
      Map.get(manifest, :transformation_version) == @transformation_version,
      nil,
      nil,
      "manifest.exs",
      :transformation_version,
      Map.get(manifest, :transformation_version)
    )
  end

  defp reference_metadata_errors(manifest, definitions) do
    rules = Map.get(manifest, :parameter_references)
    profiles = Map.get(manifest, :reference_profiles)

    []
    |> validate_reference_rules(rules, definitions)
    |> validate_reference_profiles(profiles, rules)
  end

  defp validate_reference_rules(errors, rules, definitions) when is_map(rules) do
    expected_numbers = definitions |> Enum.map(& &1.number) |> MapSet.new()
    actual_numbers = rules |> Map.keys() |> MapSet.new()

    coverage_errors =
      if expected_numbers == actual_numbers do
        []
      else
        [
          error(nil, nil, "manifest.exs", :parameter_reference_coverage, %{
            expected: expected_numbers,
            actual: actual_numbers
          })
        ]
      end

    rule_errors =
      definitions
      |> Enum.group_by(& &1.number)
      |> Enum.flat_map(fn {number, variants} ->
        case Map.get(rules, number) do
          operation_rules when is_map(operation_rules) ->
            variants
            |> Enum.flat_map(fn definition ->
              expected = definition.parameters |> Enum.map(& &1.name) |> MapSet.new()
              actual = operation_rules |> Map.keys() |> MapSet.new()

              name_errors =
                if expected == actual do
                  []
                else
                  [
                    error(
                      definition.id,
                      definition.variant,
                      "manifest.exs",
                      :parameter_reference_names,
                      %{expected: expected, actual: actual}
                    )
                  ]
                end

              value_errors =
                Enum.flat_map(operation_rules, fn {name, rule} ->
                  if valid_reference_rule?(rule) do
                    []
                  else
                    [
                      error(
                        definition.id,
                        definition.variant,
                        "manifest.exs",
                        :parameter_reference_rule,
                        %{parameter: name, rule: rule}
                      )
                    ]
                  end
                end)

              name_errors ++ value_errors
            end)

          invalid ->
            Enum.map(variants, fn definition ->
              error(
                definition.id,
                definition.variant,
                "manifest.exs",
                :parameter_reference_rule,
                %{operation: number, rules: invalid}
              )
            end)
        end
      end)

    errors ++ coverage_errors ++ rule_errors
  end

  defp validate_reference_rules(errors, _rules, _definitions),
    do:
      errors ++
        [error(nil, nil, "manifest.exs", :parameter_references, :must_be_a_map)]

  defp validate_reference_profiles(errors, profiles, rules)
       when is_map(profiles) and is_map(rules) do
    required = MapSet.new([:official, :snb_bi_smoke])

    coverage_errors =
      if MapSet.subset?(required, Map.keys(profiles) |> MapSet.new()) do
        []
      else
        [error(nil, nil, "manifest.exs", :reference_profiles, :missing_required_profile)]
      end

    profile_errors =
      Enum.flat_map(profiles, fn
        {:official, %{provenance: :official, manifest_profile: :any, fields: :all}} ->
          []

        {:snb_bi_smoke, %{provenance: :smoke, manifest_profile: profile, fields: fields}}
        when is_binary(profile) and is_map(fields) ->
          validate_smoke_fields(fields, rules)

        {name, profile} ->
          [error(nil, nil, "manifest.exs", :reference_profile, %{name: name, profile: profile})]
      end)

    errors ++ coverage_errors ++ profile_errors
  end

  defp validate_reference_profiles(errors, _profiles, _rules),
    do: errors ++ [error(nil, nil, "manifest.exs", :reference_profiles, :must_be_a_map)]

  defp validate_smoke_fields(fields, rules) do
    Enum.flat_map(fields, fn {number, names} ->
      operation_rules = Map.get(rules, number, %{})

      if is_list(names) and
           Enum.all?(names, fn name ->
             match?(%{kind: kind} when kind in [:entity_id, :entity_name], operation_rules[name])
           end) do
        []
      else
        [
          error(nil, nil, "manifest.exs", :smoke_reference_fields, %{
            operation: number,
            fields: names
          })
        ]
      end
    end)
  end

  defp valid_reference_rule?(%{kind: :scalar}), do: true

  defp valid_reference_rule?(%{kind: kind, entity: entity})
       when kind in [:entity_id, :entity_name] and is_binary(entity),
       do: entity in Mapping.entity_types()

  defp valid_reference_rule?(_rule), do: false

  defp coverage_errors(entries, expected) when is_list(entries) do
    keys = Enum.map(entries, &entry_key/1)

    duplicate_errors =
      keys
      |> Enum.frequencies()
      |> Enum.flat_map(fn
        {{operation_id, variant}, count} when count > 1 ->
          path = entries |> Enum.find(&(entry_key(&1) == {operation_id, variant})) |> entry_path()
          [error(operation_id, variant, path, :duplicate, count)]

        _other ->
          []
      end)

    expected_keys = Map.keys(expected) |> MapSet.new()
    actual_keys = MapSet.new(keys)

    missing_errors =
      expected_keys
      |> MapSet.difference(actual_keys)
      |> Enum.map(fn {operation_id, variant} ->
        definition = Map.fetch!(expected, {operation_id, variant})
        error(operation_id, variant, expected_filename(definition), :missing, :manifest_entry)
      end)

    unexpected_errors =
      actual_keys
      |> MapSet.difference(expected_keys)
      |> Enum.map(fn {operation_id, variant} ->
        entry = Enum.find(entries, &(entry_key(&1) == {operation_id, variant}))

        rule =
          if is_map(entry) and Map.get(entry, :number) in @graph_operations,
            do: :extension_excluded,
            else: :unexpected

        error(operation_id, variant, entry_path(entry), rule, :manifest_entry)
      end)

    duplicate_errors ++ missing_errors ++ unexpected_errors
  end

  defp coverage_errors(_entries, _expected), do: []

  defp unmanifested_errors(entries, root) when is_list(entries) do
    manifested = entries |> Enum.map(&entry_path/1) |> Enum.filter(&is_binary/1) |> MapSet.new()

    root
    |> File.ls!()
    |> Enum.filter(&(Path.extname(&1) == ".rq"))
    |> Enum.reject(&MapSet.member?(manifested, &1))
    |> Enum.map(&error(nil, nil, &1, :unmanifested, :artifact))
  end

  defp unmanifested_errors(_entries, _root), do: []

  defp load_entries(entries, expected, root) when is_list(entries) do
    entries
    |> Enum.uniq_by(&entry_key/1)
    |> Enum.reduce({[], []}, fn entry, {loaded, errors} ->
      case Map.fetch(expected, entry_key(entry)) do
        {:ok, definition} ->
          case load_entry(entry, definition, root) do
            {:ok, translation} -> {[translation | loaded], errors}
            {:error, entry_errors} -> {loaded, errors ++ entry_errors}
          end

        :error ->
          {loaded, errors}
      end
    end)
  end

  defp load_entries(_entries, _expected, _root), do: {[], []}

  defp load_entry(entry, definition, root) when is_map(entry) do
    operation_id = Map.get(entry, :operation_id)
    variant = Map.get(entry, :variant)
    path = Map.get(entry, :path)
    provenance_errors = provenance_errors(entry, definition)

    with :ok <- safe_artifact_path(path),
         artifact_path <- Path.join(root, path),
         {:ok, sparql} <- File.read(artifact_path) do
      actual_placeholders = placeholders(sparql)
      expected_placeholders = Enum.map(definition.parameters, & &1.name)
      checksum = checksum(sparql)

      checksum_errors =
        if checksum == Map.get(entry, :checksum),
          do: [],
          else: [
            error(operation_id, variant, path, :checksum, %{
              expected: entry[:checksum],
              actual: checksum
            })
          ]

      placeholder_errors =
        if MapSet.new(actual_placeholders) == MapSet.new(expected_placeholders),
          do: [],
          else: [
            error(operation_id, variant, path, :placeholders, %{
              expected: expected_placeholders,
              actual: actual_placeholders
            })
          ]

      mapping_errors = mapping_errors(sparql, operation_id, variant, path)

      case parse(sparql, definition, operation_id, variant, path) do
        {:ok, ast, projected_columns} ->
          projection_errors =
            if projected_columns == Enum.map(definition.result, & &1.name),
              do: [],
              else: [
                error(operation_id, variant, path, :projected_columns, %{
                  expected: Enum.map(definition.result, & &1.name),
                  actual: projected_columns
                })
              ]

          contract_errors = query_contract_errors(ast, definition, operation_id, variant, path)

          errors =
            provenance_errors ++
              checksum_errors ++
              placeholder_errors ++ mapping_errors ++ projection_errors ++ contract_errors

          if errors == [] do
            {:ok,
             entry
             |> Map.put(:placeholders, expected_placeholders)
             |> Map.put(:projected_columns, projected_columns)
             |> Map.put(:sparql, sparql)
             |> Map.put(:ast, ast)}
          else
            {:error, errors}
          end

        {:error, parse_error} ->
          {:error,
           provenance_errors ++
             checksum_errors ++
             placeholder_errors ++
             mapping_errors ++
             [error(operation_id, variant, path, :parse, parse_error)]}
      end
    else
      {:error, :unsafe_path} ->
        {:error, provenance_errors ++ [error(operation_id, variant, path, :path, :unsafe)]}

      {:error, :enoent} ->
        {:error, provenance_errors ++ [error(operation_id, variant, path, :missing, :artifact)]}

      {:error, reason} ->
        {:error, provenance_errors ++ [error(operation_id, variant, path, :artifact, reason)]}
    end
  end

  defp load_entry(entry, _definition, _root),
    do: {:error, [error(nil, nil, entry_path(entry), :entry, :must_be_a_map)]}

  defp provenance_errors(entry, definition) do
    operation_id = Map.get(entry, :operation_id)
    variant = Map.get(entry, :variant)
    path = Map.get(entry, :path)

    []
    |> require(
      Map.get(entry, :number) == definition.number,
      operation_id,
      variant,
      path,
      :number,
      definition.number
    )
    |> require(
      path == expected_filename(definition),
      operation_id,
      variant,
      path,
      :filename,
      expected_filename(definition)
    )
    |> require(
      Map.get(entry, :source_card) == definition.source.path,
      operation_id,
      variant,
      path,
      :source_card,
      definition.source.path
    )
    |> require(
      Map.get(entry, :source_card_commit) == definition.source.commit,
      operation_id,
      variant,
      path,
      :source_card_commit,
      definition.source.commit
    )
    |> require(
      Map.get(entry, :reference_query) == definition.source.reference,
      operation_id,
      variant,
      path,
      :reference_query,
      definition.source.reference
    )
    |> require(
      Map.get(entry, :upstream_commit) == @upstream_commit,
      operation_id,
      variant,
      path,
      :upstream_commit,
      Map.get(entry, :upstream_commit)
    )
    |> require(
      Map.get(entry, :transformation_version) == definition.transformation_version,
      operation_id,
      variant,
      path,
      :transformation_version,
      definition.transformation_version
    )
    |> require(
      Map.get(entry, :mapping_version) == Mapping.version(),
      operation_id,
      variant,
      path,
      :mapping_version,
      Mapping.version()
    )
  end

  defp parse(sparql, definition, operation_id, variant, path) do
    expanded =
      Enum.reduce(definition.parameters, sparql, fn parameter, query ->
        String.replace(query, "{{#{parameter.name}}}", sample_value(parameter.type))
      end)

    case Parser.parse(expanded) do
      {:ok, ast} ->
        case projected_variables(ast) do
          {:ok, variables} -> {:ok, ast, Enum.map(variables, &decode_column/1)}
          :error -> {:error, :missing_projection}
        end

      {:error, reason} ->
        {:error, %{operation_id: operation_id, variant: variant, path: path, reason: reason}}
    end
  end

  defp projected_variables({:project, _input, variables}) do
    {:ok, Enum.map(variables, fn {:variable, name} -> name end)}
  end

  defp projected_variables(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.find_value(:error, fn value ->
      case projected_variables(value) do
        {:ok, _variables} = result -> result
        :error -> nil
      end
    end)
  end

  defp projected_variables(list) when is_list(list) do
    Enum.find_value(list, :error, fn
      {_key, value} ->
        case projected_variables(value) do
          {:ok, _variables} = result -> result
          :error -> nil
        end

      value ->
        case projected_variables(value) do
          {:ok, _variables} = result -> result
          :error -> nil
        end
    end)
  end

  defp projected_variables(_value), do: :error

  defp query_contract_errors(ast, definition, operation_id, variant, path) do
    case outer_query_contract(ast) do
      {:ok, ordering, limit} ->
        expected_ordering =
          Enum.map(definition.ordering, fn item ->
            {String.to_existing_atom(item.direction), normalize_order_name(item.name)}
          end)

        []
        |> require(
          ordering == expected_ordering,
          operation_id,
          variant,
          path,
          :ordering,
          %{expected: expected_ordering, actual: ordering}
        )
        |> require(
          limit == definition.limit,
          operation_id,
          variant,
          path,
          :limit,
          %{expected: definition.limit, actual: limit}
        )

      {:error, reason} ->
        [error(operation_id, variant, path, :query_contract, reason)]
    end
  end

  defp outer_query_contract({:select, properties}) do
    properties
    |> Enum.find_value(fn
      {"pattern", pattern} -> pattern
      _property -> nil
    end)
    |> extract_outer_contract(nil)
  end

  defp outer_query_contract(_ast), do: {:error, :not_a_select_query}

  defp extract_outer_contract({:slice, input, 0, limit}, _current_limit),
    do: extract_outer_contract(input, limit)

  defp extract_outer_contract({:distinct, input}, limit),
    do: extract_outer_contract(input, limit)

  defp extract_outer_contract({:reduced, input}, limit),
    do: extract_outer_contract(input, limit)

  defp extract_outer_contract({:project, input, _variables}, limit),
    do: extract_ordering(input, limit)

  defp extract_outer_contract(_pattern, _limit), do: {:error, :missing_outer_projection}

  defp extract_ordering({:order_by, _input, expressions}, limit) do
    ordering =
      Enum.map(expressions, fn {direction, expression} ->
        {direction, order_expression_name(expression)}
      end)

    {:ok, ordering, limit}
  end

  defp extract_ordering(_input, limit), do: {:ok, [], limit}

  defp order_expression_name({:variable, name}), do: decode_column(name)

  defp order_expression_name({:add, left, right}) do
    order_expression_name(left) <> " + " <> order_expression_name(right)
  end

  defp order_expression_name(expression), do: inspect(expression)

  defp normalize_order_name(name) do
    name
    |> String.replace("\\mbox{", "")
    |> String.replace("}", "")
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
  end

  defp mapping_errors(sparql, operation_id, variant, path) do
    referenced_graphs =
      @dataset_clause_regex
      |> Regex.scan(sparql, capture: :all_but_first)
      |> List.flatten()
      |> MapSet.new()

    valid? =
      String.contains?(sparql, "https://ldbcouncil.org/snb/ontology/") and
        String.contains?(sparql, "FROM <#{@initial_graph}>") and
        String.contains?(sparql, "FROM <#{@updates_graph}>") and
        MapSet.subset?(referenced_graphs, @allowed_graphs) and
        not String.match?(sparql, ~r/\bSERVICE\b/i) and
        not String.contains?(sparql, "translationAnchor")

    if valid? do
      []
    else
      [
        error(operation_id, variant, path, :mapping_assumptions, %{
          expected_graphs: MapSet.to_list(@allowed_graphs) |> Enum.sort(),
          referenced_graphs: MapSet.to_list(referenced_graphs) |> Enum.sort()
        })
      ]
    end
  end

  defp expected_definitions(definitions) do
    definitions
    |> Enum.reject(&MapSet.member?(@graph_operations, &1.number))
    |> Map.new(&{{&1.catalog_id, &1.variant}, &1})
  end

  defp expected_filename(definition) do
    variant = if definition.variant == "default", do: "default", else: definition.variant
    "bi-#{definition.number |> Integer.to_string() |> String.pad_leading(2, "0")}-#{variant}.rq"
  end

  defp safe_artifact_path(path) when is_binary(path) do
    if Path.type(path) == :relative and Path.basename(path) == path and
         Path.extname(path) == ".rq",
       do: :ok,
       else: {:error, :unsafe_path}
  end

  defp safe_artifact_path(_path), do: {:error, :unsafe_path}

  defp placeholders(sparql) do
    @placeholder_regex
    |> Regex.scan(sparql, capture: :all_but_first)
    |> List.flatten()
    |> Enum.uniq()
  end

  defp sample_value("ID"), do: "1"
  defp sample_value("32-bit Integer"), do: "1"
  defp sample_value("64-bit Integer"), do: "1"
  defp sample_value("32-bit Float"), do: "1.0"
  defp sample_value("Boolean"), do: "true"
  defp sample_value("Date"), do: ~S["2012-11-29"^^<http://www.w3.org/2001/XMLSchema#date>]

  defp sample_value("DateTime"),
    do: ~S["2012-11-29T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>]

  defp sample_value(type) when type in ["String", "Long String", "\\{String\\}"], do: ~S["value"]

  defp decode_column(variable), do: String.replace(variable, "__", ".")

  defp checksum(content) do
    "sha256:" <> (:crypto.hash(:sha256, content) |> Base.encode16(case: :lower))
  end

  defp entry_key(entry) when is_map(entry),
    do: {Map.get(entry, :operation_id), Map.get(entry, :variant)}

  defp entry_key(_entry), do: {nil, nil}

  defp entry_path(entry) when is_map(entry), do: Map.get(entry, :path)
  defp entry_path(_entry), do: nil

  defp require(errors, true, _operation_id, _variant, _path, _rule, _detail), do: errors

  defp require(errors, false, operation_id, variant, path, rule, detail),
    do: errors ++ [error(operation_id, variant, path, rule, detail)]

  defp error(operation_id, variant, path, rule, detail),
    do: {:translation, operation_id, variant, path, rule, detail}
end
