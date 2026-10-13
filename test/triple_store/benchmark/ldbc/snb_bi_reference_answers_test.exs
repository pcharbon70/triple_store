defmodule TripleStore.Benchmark.LDBC.SNBBIReferenceAnswersTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.Result
  alias TripleStore.Benchmark.LDBC.SNB.BI.{ReferenceAnswers, ResultContract, Workload}

  @catalog_id "ldbc-snb-bi-v1.0.3"
  @reference_source %{
    implementation: "ldbc-snb-bi-cypher",
    version: "v1.0.3",
    commit: "5f7967235593eefa98b336e240039780b6a76a9a"
  }

  test "checked-in smoke answers are bound to the deterministic fixture dataset" do
    {definition, _dataset, parameters} = context()

    dataset = %{
      dataset_id: "snb-bi-smoke",
      transformation: %{
        output_checksum:
          "sha256:90f9b6e38f2689be479c0d40b368db3e2f0542d503cb7d1ec9950b0e524ecf33",
        mapping_version: "snb-rdf-v1"
      }
    }

    parameters = %{parameters | dataset_checksum: dataset.transformation.output_checksum}

    assert {:ok, answer} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               reference_source: @reference_source
             )

    assert answer.result.rows == []
    assert answer.purpose == :contract_fixture

    assert answer.content_checksum ==
             "sha256:bd349c43a8039831620a4df23fb14c83577447ba6170596357bce8762d9ebfe4"
  end

  test "inventory distinguishes contract fixtures from accepted answers" do
    assert {:ok, [entry]} = ReferenceAnswers.inventory()
    assert entry.purpose == :contract_fixture
    assert entry.artifact.kind == :embedded
  end

  test "loads typed duplicate-bearing rows and preserves the BI ordering contract", %{test: test} do
    {definition, dataset, parameters} = context()

    rows = [
      read_1_row(2012, 7, 2.5),
      read_1_row(2012, 7, 2.5),
      read_1_row(2011, 4_294_967_296, 1.5)
    ]

    root = write_reference_root(test, definition, dataset, parameters, rows)

    assert {:ok, answer} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               root: root,
               reference_source: @reference_source
             )

    assert %Result{} = answer.result

    assert answer.result.types == [
             {:integer, 32},
             :boolean,
             {:integer, 32},
             {:integer, 64},
             :float32,
             {:integer, 64},
             :float32
           ]

    assert Enum.map(answer.result.rows, &Enum.at(&1, 0)) == [2012, 2012, 2011]
    assert Enum.at(answer.result.rows, 0) == Enum.at(answer.result.rows, 1)
    assert answer.result.duplicate_semantics == :bag
    assert answer.identity.parameter.row_index == 0
    assert String.starts_with?(answer.content_checksum, "sha256:")
  end

  test "rejects accepted answers that violate canonical order or the operation limit", %{
    test: test
  } do
    {definition, dataset, parameters} = context()

    misordered = [read_1_row(2011, 1, 1.0), read_1_row(2012, 2, 2.0)]
    root = write_reference_root({test, :order}, definition, dataset, parameters, misordered)

    assert {:error, {:malformed_reference_answer, :noncanonical_result_order}} =
             load(root, definition, dataset, parameters)

    limited_definition = %{definition | limit: 1}
    excessive = List.duplicate(read_1_row(2012, 1, 1.0), limited_definition.limit + 1)

    root =
      write_reference_root({test, :limit}, limited_definition, dataset, parameters, excessive)

    assert {:error,
            {:malformed_reference_answer, {:result_limit_exceeded, expected_limit, actual_count}}} =
             load(root, limited_definition, dataset, parameters)

    assert expected_limit == limited_definition.limit
    assert actual_count == limited_definition.limit + 1
  end

  test "decodes reference timestamps without losing UTC identity", %{test: test} do
    {definition, dataset, parameters} = context(3)

    rows = [
      %{
        "forum.id" => 9_223_372_036_854_775_000,
        "forum.title" => "Forum",
        "forum.creationDate" => "2012-11-29T00:00:00.123Z",
        "person.id" => 4_294_967_296,
        "messageCount" => 7
      }
    ]

    root = write_reference_root(test, definition, dataset, parameters, rows)
    assert {:ok, answer} = load(root, definition, dataset, parameters)

    timestamp = answer.result.rows |> hd() |> Enum.at(2)
    assert %DateTime{} = timestamp
    assert DateTime.to_iso8601(timestamp) == "2012-11-29T00:00:00.123Z"
    assert Enum.at(hd(answer.result.rows), 0) == 9_223_372_036_854_775_000
  end

  test "rejects every mismatched run identity before reading an answer", %{test: test} do
    {definition, dataset, parameters} = context()
    root = write_reference_root(test, definition, dataset, parameters, [read_1_row(2012, 1, 1.5)])

    mismatches = [
      {:catalog, fn manifest -> %{manifest | catalog_id: "another-catalog"} end},
      {:dataset_checksum,
       fn manifest ->
         update_in(manifest, [:answers, Access.at(0), :dataset, :checksum], fn _ ->
           sha256("other-dataset")
         end)
       end},
      {:mapping_version,
       fn manifest ->
         put_in(manifest, [:answers, Access.at(0), :dataset, :mapping_version], "other-mapping")
       end},
      {:parameter_checksum,
       fn manifest ->
         update_in(manifest, [:answers, Access.at(0), :parameter, :checksum], fn _ ->
           sha256("other-parameter")
         end)
       end},
      {:parameter_source,
       fn manifest ->
         put_in(manifest, [:answers, Access.at(0), :parameter, :source_id], "other-source")
       end},
      {:operation_source,
       fn manifest ->
         put_in(
           manifest,
           [:answers, Access.at(0), :operation_source, :commit],
           String.duplicate("0", 40)
         )
       end}
    ]

    Enum.each(mismatches, fn {expected, mutation} ->
      manifest = read_manifest(root) |> mutation.()
      write_manifest(root, manifest)

      assert {:error, {:invalid_reference_answer, errors}} =
               ReferenceAnswers.load(definition, 0, dataset, parameters,
                 root: root,
                 reference_source: @reference_source
               )

      assert expected in errors
      write_manifest(root, read_manifest_backup(root))
    end)

    assert {:error, {:invalid_reference_answer, errors}} =
             ReferenceAnswers.load(definition, 0, dataset, parameters,
               root: root,
               reference_source: %{@reference_source | version: "another-version"}
             )

    assert :reference_source in errors
  end

  test "missing malformed and external answers invalidate the run", %{test: test} do
    {definition, dataset, parameters} = context()
    root = write_reference_root(test, definition, dataset, parameters, [read_1_row(2012, 1, 1.5)])

    File.rm!(Path.join(root, "smoke/bi-01-default-000.json"))

    assert {:error, {:reference_answer_missing, _path}} =
             load(root, definition, dataset, parameters)

    root = write_reference_root({test, :malformed}, definition, dataset, parameters, [])
    path = Path.join(root, "smoke/bi-01-default-000.json")
    File.write!(path, ~s({"rows":))
    rewrite_artifact_checksum(root, path)

    assert {:error, {:malformed_reference_answer, _reason}} =
             load(root, definition, dataset, parameters)

    root = write_reference_root({test, :external}, definition, dataset, parameters, [])

    manifest =
      root
      |> read_manifest()
      |> put_in([:answers, Access.at(0), :artifact], %{
        kind: :external,
        uri: "https://results.example.test/by-sha256/answer.json",
        checksum: sha256("external-answer")
      })

    write_manifest(root, manifest)

    assert {:error, {:external_reference_answer_required, address}} =
             load(root, definition, dataset, parameters)

    assert address.uri == "https://results.example.test/by-sha256/answer.json"
  end

  test "compares through the shared correctness model with full smoke differences", %{test: test} do
    {definition, dataset, parameters} = context()
    expected_rows = [read_1_row(2012, 7, 2.5), read_1_row(2012, 7, 2.5)]
    root = write_reference_root(test, definition, dataset, parameters, expected_rows)

    assert {:ok, actual} = ResultContract.materialize(definition, [read_1_row(2012, 7, 2.5)])

    assert {:ok, comparison} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source
             )

    assert comparison.status == :incorrect
    assert comparison.category == :row_mismatch
    assert comparison.comparison == :full
    assert comparison.details.missing == [%{row: hd(actual.rows), count: 1}]
    assert comparison.reference_answer.parameter.row_index == 0
  end

  test "large answers retain deterministic hashes", %{test: test} do
    {definition, dataset, parameters} = context()
    rows = [read_1_row(2012, 7, 2.5), read_1_row(2011, 8, 1.5)]
    root = write_reference_root(test, definition, dataset, parameters, rows)
    assert {:ok, actual} = ResultContract.materialize(definition, rows)

    assert {:ok, first} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source,
               full_comparison_limit: 1
             )

    assert {:ok, second} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source,
               full_comparison_limit: 1
             )

    assert first.status == :correct
    assert {:hash, :ordered, digest} = first.comparison
    assert digest == elem(second.comparison, 2)
  end

  test "accepted divergences must match the loaded reference source version", %{test: test} do
    {definition, dataset, parameters} = context()
    root = write_reference_root(test, definition, dataset, parameters, [read_1_row(2012, 1, 1.0)])
    assert {:ok, actual} = ResultContract.materialize(definition, [read_1_row(2011, 1, 1.0)])

    divergence = %{
      permitted?: true,
      reason: "Pinned reference-engine exception",
      source_version: "different-version"
    }

    assert {:error, :invalid_accepted_divergence} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source,
               accepted_divergence: divergence
             )

    assert {:ok, %{status: :accepted_divergence}} =
             ReferenceAnswers.compare(definition, 0, actual, dataset, parameters,
               root: root,
               reference_source: @reference_source,
               accepted_divergence: %{divergence | source_version: @reference_source.version}
             )
  end

  defp load(root, definition, dataset, parameters) do
    ReferenceAnswers.load(definition, 0, dataset, parameters,
      root: root,
      reference_source: @reference_source
    )
  end

  defp context(number \\ 1) do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, definition} = Workload.fetch(definitions, number)

    dataset = %{
      dataset_id: "snb-bi-smoke",
      transformation: %{
        output_checksum: sha256("smoke-dataset"),
        mapping_version: "snb-rdf-v1"
      }
    }

    parameter_row = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})

    parameters = %{
      dataset_checksum: dataset.transformation.output_checksum,
      source_id: "snb-bi-1.0.3",
      generator_pin: "5f7967235593eefa98b336e240039780b6a76a9a",
      sequences: %{sequence_label(definition) => [parameter_row]}
    }

    {definition, dataset, parameters}
  end

  defp write_reference_root(test, definition, dataset, parameters, rows) do
    root =
      Path.join(
        System.tmp_dir!(),
        "snb_bi_answers_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    artifact_path = Path.join(root, "smoke/bi-01-default-000.json")
    File.mkdir_p!(Path.dirname(artifact_path))

    File.write!(
      artifact_path,
      Jason.encode!(%{schema_version: 1, rows: rows}, pretty: true) <> "\n"
    )

    sequence = sequence_label(definition)
    parameter_row = parameters.sequences[sequence] |> Enum.at(0)

    manifest = %{
      schema_version: 2,
      layout_version: "snb-bi-reference-answers-v1",
      catalog_id: @catalog_id,
      answers: [
        %{
          operation_id: definition.catalog_id,
          variant: definition.variant,
          purpose: :accepted_answer,
          parameter: %{
            sequence: sequence,
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
            path: "smoke/bi-01-default-000.json",
            checksum: file_checksum(artifact_path)
          }
        }
      ]
    }

    write_manifest(root, manifest)
    File.cp!(Path.join(root, "manifest.exs"), Path.join(root, "manifest.backup.exs"))
    root
  end

  defp rewrite_artifact_checksum(root, path) do
    manifest = read_manifest(root)

    manifest =
      put_in(
        manifest,
        [:answers, Access.at(0), :artifact, :checksum],
        file_checksum(path)
      )

    write_manifest(root, manifest)
  end

  defp read_manifest(root), do: root |> Path.join("manifest.exs") |> Code.eval_file() |> elem(0)

  defp read_manifest_backup(root),
    do: root |> Path.join("manifest.backup.exs") |> Code.eval_file() |> elem(0)

  defp write_manifest(root, manifest) do
    File.write!(
      Path.join(root, "manifest.exs"),
      inspect(manifest, pretty: true, limit: :infinity)
    )
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

  defp term_checksum(term),
    do: term |> :erlang.term_to_binary([:deterministic]) |> sha256()

  defp file_checksum(path), do: path |> File.read!() |> sha256()

  defp sha256(value),
    do: "sha256:" <> (:crypto.hash(:sha256, value) |> Base.encode16(case: :lower))
end
