defmodule TripleStore.Benchmark.LDBC.Phase5ACorpusAndReferenceIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{SNB.Converter, StoreFixture}

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Parameters,
    ReferenceAnswers,
    TranslationCorpus,
    Workload
  }

  @moduletag :integration

  @graph_operations MapSet.new([10, 15, 19, 20])
  @reference_source %{
    implementation: "ldbc-snb-bi-cypher",
    version: "v1.0.3",
    commit: "5f7967235593eefa98b336e240039780b6a76a9a"
  }

  test "all canonical variants have exactly one executable strategy" do
    assert {:ok, definitions} = Workload.load()
    assert length(definitions) == 31

    {extensions, standard} =
      Enum.split_with(definitions, &MapSet.member?(@graph_operations, &1.number))

    assert length(standard) == 22
    assert length(extensions) == 9

    assert {:ok, corpus} = TranslationCorpus.load(definitions)
    assert length(corpus) == length(standard)

    assert corpus
           |> Enum.frequencies_by(&{&1.operation_id, &1.variant})
           |> Map.values() == List.duplicate(1, length(standard))

    Enum.each(standard, fn definition ->
      assert %{translation: translation, strategy: {:native, _module, :execute}} = definition
      assert translation.operation_id == definition.catalog_id
      assert translation.variant == definition.variant
      assert match?({:select, _properties}, translation.ast)
      assert is_binary(translation.sparql) and translation.sparql != ""
    end)

    expected_extensions = %{
      10 => "ldbc.snb.bi.shortest-path-range.v1",
      15 => "ldbc.snb.bi-bounded-path.v1",
      19 => "ldbc.snb.bi-weighted-interaction-path.v1",
      20 => "ldbc.snb.bi-weighted-tag-path.v1"
    }

    Enum.each(extensions, fn definition ->
      assert {:extension, extension_id, _module} = definition.strategy
      assert extension_id == Map.fetch!(expected_extensions, definition.number)
      refute Map.has_key?(definition, :translation)
    end)

    extensions
    |> Enum.group_by(& &1.number, fn %{strategy: {:extension, id, _module}} -> id end)
    |> Enum.each(fn {number, ids} ->
      assert Enum.uniq(ids) == [Map.fetch!(expected_extensions, number)]
    end)
  end

  test "smoke parameters validate through the mapped store and identity failures close the gate",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()

    assert {:ok, parameters} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    try do
      assert :ok =
               Parameters.validate(parameters, fixture, definitions,
                 reference_profile: :snb_bi_smoke
               )

      missing_entity = put_in(parameters, [:sequences, "10a", Access.at(0), "personId"], 999)

      assert {:error, errors} =
               Parameters.validate(missing_entity, fixture, definitions,
                 reference_profile: :snb_bi_smoke
               )

      assert {"10a", 0, "personId", {:unknown_reference, :person, 999}} in errors

      wrong_dataset = %{parameters | dataset_checksum: sha256("another-dataset")}

      assert {:error, errors} =
               Parameters.validate(wrong_dataset, fixture, definitions,
                 reference_profile: :snb_bi_smoke
               )

      assert :dataset_checksum in errors
    after
      assert :ok = StoreFixture.teardown(fixture, delete: true)
    end
  end

  test "translation corruption is reported with operation-scoped fail-closed errors", %{
    test: test
  } do
    assert {:ok, definitions} = Workload.load()

    for {kind, mutate, expected_rule, refresh_checksum?} <- [
          {:missing, &File.rm!/1, :missing, false},
          {:malformed, &File.write!(&1, "NOT A SPARQL QUERY\n"), :parse, true},
          {:checksum, &File.write!(&1, File.read!(&1) <> "\n# changed\n"), :checksum, false},
          {:placeholder,
           &File.write!(&1, String.replace(File.read!(&1), "{{datetime}}", "{{unknown}}")),
           :placeholders, true}
        ] do
      root = copy_translation_corpus({test, kind})
      manifest = read_manifest(root)
      entry = hd(manifest.translations)
      path = Path.join(root, entry.path)
      mutate.(path)

      if refresh_checksum? do
        write_manifest(root, replace_translation_checksum(manifest, entry, path))
      end

      assert {:error, errors} = TranslationCorpus.load(definitions, root: root)

      assert Enum.any?(errors, fn
               {:translation, operation_id, variant, _path, ^expected_rule, _detail} ->
                 operation_id == entry.operation_id and variant == entry.variant

               _error ->
                 false
             end)
    end
  end

  test "checked-in reference coverage is explicit, deterministic, and fail closed" do
    assert {:ok, definitions} = Workload.load()
    manifest = read_manifest(ReferenceAnswers.default_root())

    coverage = reference_coverage(definitions, manifest.answers)
    assert Enum.count(coverage, &(&1.status == :contract_fixture)) == 1
    refute Enum.any?(coverage, &(&1.status == :external))
    assert Enum.count(coverage, &(&1.status == :missing)) == 30

    assert [%{number: 1, variant: "default", status: :contract_fixture}] =
             Enum.filter(coverage, &(&1.status == :contract_fixture))

    {dataset, parameters} = checked_in_context()
    assert {:ok, definition} = Workload.fetch(definitions, 1)

    assert {:ok, first} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               reference_source: @reference_source
             )

    assert {:ok, second} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               reference_source: @reference_source
             )

    assert first.result.rows == []
    assert first.content_checksum == second.content_checksum
    assert first.identity == second.identity

    assert {:error, {:reference_answer_not_accepted, :contract_fixture}} =
             ReferenceAnswers.compare(definition, 0, first.result, dataset, parameters,
               reference_source: @reference_source
             )

    assert {:ok, missing_definition} = Workload.fetch(definitions, 2, "a")

    assert {:error, {:reference_answer_not_found, operation_id, "a", 0}} =
             ReferenceAnswers.load(missing_definition, 0, dataset, parameters,
               reference_source: @reference_source
             )

    assert operation_id == missing_definition.catalog_id
  end

  test "missing, malformed, and checksum-mismatched reference artifacts fail closed", %{
    test: test
  } do
    assert {:ok, definitions} = Workload.load()
    {dataset, parameters} = synthetic_context(definitions, 1)
    assert {:ok, definition} = Workload.fetch(definitions, 1)

    for {kind, mutation, expected} <- [
          {:missing, &File.rm!/1, :missing},
          {:malformed, &File.write!(&1, "not-json\n"), :malformed},
          {:checksum, &File.write!(&1, File.read!(&1) <> "changed\n"), :checksum}
        ] do
      root =
        write_reference_root(
          {test, kind},
          definition,
          dataset,
          parameters,
          [read_1_row(2012, 7, 2.5)]
        )

      artifact_path = Path.join(root, "answer.json")
      mutation.(artifact_path)

      if kind == :malformed do
        manifest = read_manifest(root)

        write_manifest(
          root,
          put_in(
            manifest,
            [:answers, Access.at(0), :artifact, :checksum],
            file_checksum(artifact_path)
          )
        )
      end

      assert_reference_artifact_error(expected, root, definition, dataset, parameters)
    end
  end

  test "reference contract fixtures preserve types, order, duplicates, and deterministic hashes",
       %{test: test} do
    assert {:ok, definitions} = Workload.load()
    {dataset, parameters} = synthetic_context(definitions, 1)
    assert {:ok, definition} = Workload.fetch(definitions, 1)

    rows = [
      read_1_row(2012, 7, 2.5),
      read_1_row(2012, 7, 2.5),
      read_1_row(2011, 4_294_967_296, 1.5)
    ]

    root = write_reference_root({test, :typed}, definition, dataset, parameters, rows)

    assert {:ok, expected} = load_reference(root, definition, dataset, parameters)
    assert {:ok, repeated} = load_reference(root, definition, dataset, parameters)

    assert expected.content_checksum == repeated.content_checksum
    assert expected.result.rows == repeated.result.rows
    assert Enum.map(expected.result.rows, &hd/1) == [2012, 2012, 2011]
    assert Enum.at(expected.result.rows, 0) == Enum.at(expected.result.rows, 1)

    assert expected.result.types == [
             {:integer, 32},
             :boolean,
             {:integer, 32},
             {:integer, 64},
             :float32,
             {:integer, 64},
             :float32
           ]

    assert {:ok, exact} =
             ReferenceAnswers.compare(
               definition,
               0,
               expected.result,
               dataset,
               parameters,
               root: root,
               reference_source: @reference_source,
               full_comparison_limit: 1
             )

    assert exact.status == :correct
    assert {:hash, :ordered, digest} = exact.comparison

    assert {:ok, repeated_comparison} =
             ReferenceAnswers.compare(
               definition,
               0,
               expected.result,
               dataset,
               parameters,
               root: root,
               reference_source: @reference_source,
               full_comparison_limit: 1
             )

    assert {:hash, :ordered, ^digest} = repeated_comparison.comparison

    assert_mismatch(
      :row_mismatch,
      expected.result,
      definition,
      dataset,
      parameters,
      root,
      fn result ->
        %{result | rows: tl(result.rows)}
      end
    )

    assert_mismatch(
      :row_mismatch,
      expected.result,
      definition,
      dataset,
      parameters,
      root,
      fn result ->
        %{result | rows: result.rows ++ [[2010, false, 0, 1, 1.0, 1, 25.0]]}
      end
    )

    assert_mismatch(
      :misordered,
      expected.result,
      definition,
      dataset,
      parameters,
      root,
      fn result ->
        %{result | rows: Enum.reverse(result.rows)}
      end
    )

    assert_mismatch(
      :mistyped,
      expected.result,
      definition,
      dataset,
      parameters,
      root,
      fn result ->
        %{result | types: List.replace_at(result.types, 0, :string)}
      end
    )

    assert {:error, {:invalid_reference_answer, errors}} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               root: root,
               reference_source: %{@reference_source | commit: String.duplicate("0", 40)}
             )

    assert :reference_source in errors
  end

  test "empty and timestamp reference contract fixtures round trip", %{test: test} do
    assert {:ok, definitions} = Workload.load()
    {dataset_1, parameters_1} = synthetic_context(definitions, 1)
    assert {:ok, definition_1} = Workload.fetch(definitions, 1)

    empty_root = write_reference_root({test, :empty}, definition_1, dataset_1, parameters_1, [])
    assert {:ok, empty} = load_reference(empty_root, definition_1, dataset_1, parameters_1)
    assert empty.result.rows == []

    {dataset_3, parameters_3} = synthetic_context(definitions, 3)
    assert {:ok, definition_3} = Workload.fetch(definitions, 3)

    timestamp_rows = [
      %{
        "forum.id" => 9_223_372_036_854_775_000,
        "forum.title" => "Forum",
        "forum.creationDate" => "2012-11-29T00:00:00.123Z",
        "person.id" => 4_294_967_296,
        "messageCount" => 7
      }
    ]

    timestamp_root =
      write_reference_root(
        {test, :timestamp},
        definition_3,
        dataset_3,
        parameters_3,
        timestamp_rows
      )

    assert {:ok, timestamp} =
             load_reference(timestamp_root, definition_3, dataset_3, parameters_3)

    [row] = timestamp.result.rows
    assert Enum.at(row, 0) == 9_223_372_036_854_775_000
    assert Enum.at(row, 3) == 4_294_967_296
    assert DateTime.to_iso8601(Enum.at(row, 2)) == "2012-11-29T00:00:00.123Z"
  end

  defp assert_mismatch(category, expected, definition, dataset, parameters, root, mutation) do
    actual = mutation.(expected)

    assert {:ok, comparison} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source
             )

    assert comparison.status == :incorrect
    assert comparison.category == category
  end

  defp assert_reference_artifact_error(expected, root, definition, dataset, parameters) do
    assert {:error, reason} = load_reference(root, definition, dataset, parameters)

    case {expected, reason} do
      {:missing, {:reference_answer_missing, _path}} -> :ok
      {:malformed, {:malformed_reference_answer, _detail}} -> :ok
      {:checksum, {:reference_answer_checksum_mismatch, _expected, _actual}} -> :ok
      other -> flunk("unexpected reference artifact result: #{inspect(other)}")
    end
  end

  defp reference_coverage(definitions, answers) do
    Enum.map(definitions, fn definition ->
      entries =
        Enum.filter(answers, fn answer ->
          answer.operation_id == definition.catalog_id and answer.variant == definition.variant
        end)

      status =
        case entries do
          [%{purpose: :contract_fixture}] -> :contract_fixture
          [%{purpose: :accepted_answer, artifact: %{kind: :embedded}}] -> :checked_in
          [%{artifact: %{kind: :external}}] -> :external
          [] -> :missing
        end

      %{number: definition.number, variant: definition.variant, status: status}
    end)
  end

  defp checked_in_context do
    dataset = %{
      dataset_id: "snb-bi-smoke",
      transformation: %{
        output_checksum:
          "sha256:90f9b6e38f2689be479c0d40b368db3e2f0542d503cb7d1ec9950b0e524ecf33",
        mapping_version: "snb-rdf-v1"
      }
    }

    parameters = %{
      dataset_checksum: dataset.transformation.output_checksum,
      source_id: "snb-bi-1.0.3",
      generator_pin: "5f7967235593eefa98b336e240039780b6a76a9a",
      sequences: %{"1" => [%{"datetime" => "2012-11-29T00:00:00Z"}]}
    }

    {dataset, parameters}
  end

  defp synthetic_context(definitions, number) do
    assert {:ok, definition} = Workload.fetch(definitions, number)

    dataset = %{
      dataset_id: "snb-bi-contract-fixture",
      transformation: %{
        output_checksum: sha256("snb-bi-contract-fixture"),
        mapping_version: "snb-rdf-v1"
      }
    }

    row = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})

    parameters = %{
      dataset_checksum: dataset.transformation.output_checksum,
      source_id: "snb-bi-1.0.3",
      generator_pin: "5f7967235593eefa98b336e240039780b6a76a9a",
      sequences: %{sequence_label(definition) => [row]}
    }

    {dataset, parameters}
  end

  defp write_reference_root(test, definition, dataset, parameters, rows) do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)
    artifact_path = Path.join(root, "answer.json")
    File.mkdir_p!(root)
    File.write!(artifact_path, Jason.encode!(%{schema_version: 1, rows: rows}) <> "\n")

    parameter_row = parameters.sequences[sequence_label(definition)] |> hd()

    manifest = %{
      schema_version: 2,
      layout_version: "snb-bi-reference-answers-v1",
      catalog_id: Workload.catalog_id(),
      answers: [
        %{
          operation_id: definition.catalog_id,
          variant: definition.variant,
          purpose: :accepted_answer,
          parameter: %{
            sequence: sequence_label(definition),
            row_index: 0,
            checksum: term_checksum(parameter_row),
            source_id: parameters.source_id,
            generator_pin: parameters.generator_pin
          },
          dataset: %{
            id: dataset.dataset_id,
            checksum: dataset.transformation.output_checksum,
            mapping_version: dataset.transformation.mapping_version
          },
          operation_source: Map.take(definition.source, [:source_id, :path, :commit, :reference]),
          reference_source: @reference_source,
          artifact: %{
            kind: :embedded,
            path: "answer.json",
            checksum: file_checksum(artifact_path)
          }
        }
      ]
    }

    write_manifest(root, manifest)
    root
  end

  defp load_reference(root, definition, dataset, parameters) do
    ReferenceAnswers.load(definition, 0, dataset, parameters,
      root: root,
      reference_source: @reference_source
    )
  end

  defp copy_translation_corpus(test) do
    destination = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(destination) end)
    File.cp_r!(TranslationCorpus.default_root(), destination)
    destination
  end

  defp replace_translation_checksum(manifest, entry, path) do
    translations =
      Enum.map(manifest.translations, fn candidate ->
        if candidate.operation_id == entry.operation_id and candidate.variant == entry.variant do
          %{candidate | checksum: file_checksum(path)}
        else
          candidate
        end
      end)

    %{manifest | translations: translations}
  end

  defp read_manifest(root), do: root |> Path.join("manifest.exs") |> Code.eval_file() |> elem(0)

  defp write_manifest(root, manifest) do
    File.write!(
      Path.join(root, "manifest.exs"),
      inspect(manifest, pretty: true, limit: :infinity)
    )
  end

  defp parameter_sequences(definitions) do
    Map.new(definitions, fn definition ->
      row = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})
      {sequence_label(definition), [row]}
    end)
  end

  defp read_1_row(year, count, average) do
    %{
      "year" => year,
      "isComment" => false,
      "lengthCategory" => 0,
      "messageCount" => count,
      "averageMessageLength" => average,
      "sumMessageLength" => count,
      "percentageOfMessages" => 25.0
    }
  end

  defp sequence_label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp parameter_value("ID"), do: 1
  defp parameter_value("32-bit Integer"), do: 1
  defp parameter_value("64-bit Integer"), do: 1
  defp parameter_value("32-bit Float"), do: 1.0
  defp parameter_value("Boolean"), do: true
  defp parameter_value("Date"), do: "2012-11-29"
  defp parameter_value("DateTime"), do: "2012-11-29T00:00:00Z"
  defp parameter_value(type) when type in ["String", "Long String"], do: "value"
  defp parameter_value("\\{String\\}"), do: ["value"]

  defp fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_dir(test) do
    Path.join(
      System.tmp_dir!(),
      "phase_5a_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
    )
  end

  defp term_checksum(term),
    do: term |> :erlang.term_to_binary([:deterministic]) |> sha256()

  defp file_checksum(path), do: path |> File.read!() |> sha256()

  defp sha256(value),
    do: "sha256:" <> (:crypto.hash(:sha256, value) |> Base.encode16(case: :lower))
end
