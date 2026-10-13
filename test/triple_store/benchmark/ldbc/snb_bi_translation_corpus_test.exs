defmodule TripleStore.Benchmark.LDBC.SNBBITranslationCorpusTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SNB.BI.{TranslationCorpus, Workload}

  @graph_operations [10, 15, 19, 20]

  test "loads every standard BI variant, parses it, and binds it to the workload" do
    assert {:ok, definitions} = Workload.load()

    {extensions, standard} =
      Enum.split_with(definitions, &(&1.number in @graph_operations))

    assert length(standard) == 22

    assert Enum.all?(
             standard,
             &match?(%{translation: %{sparql: sparql}} when is_binary(sparql), &1)
           )

    assert Enum.all?(extensions, &(not Map.has_key?(&1, :translation)))

    assert {:ok, corpus} = TranslationCorpus.load(definitions)
    assert length(corpus) == 22

    Enum.each(corpus, fn translation ->
      definition =
        Enum.find(
          definitions,
          &(&1.catalog_id == translation.operation_id and &1.variant == translation.variant)
        )

      assert translation.placeholders == Enum.map(definition.parameters, & &1.name)
      assert translation.projected_columns == Enum.map(definition.result, & &1.name)
      assert translation.mapping_version == "snb-rdf-v1"
      assert translation.transformation_version == "triplestore-snb-bi-v1"
      assert translation.source_card == definition.source.path
      assert translation.source_card_commit == definition.source.commit
      assert translation.reference_query == definition.source.reference
      assert translation.upstream_commit == "5f7967235593eefa98b336e240039780b6a76a9a"
      assert String.starts_with?(translation.checksum, "sha256:")
      assert match?({:select, _properties}, translation.ast)
      refute String.contains?(translation.sparql, "translationAnchor")

      Enum.each(semantic_markers(translation.number), fn marker ->
        assert String.contains?(translation.sparql, marker),
               "BI #{translation.number} is missing semantic marker #{inspect(marker)}"
      end)
    end)

    corpus
    |> Enum.group_by(& &1.number, & &1.sparql)
    |> Enum.each(fn {_number, variants} -> assert length(Enum.uniq(variants)) == 1 end)
  end

  test "rejects missing, duplicate, unmanifested, and checksum-mismatched artifacts", %{
    test: test
  } do
    assert {:ok, definitions} = Workload.load()

    root = copy_corpus(test)
    manifest = read_manifest(root)
    [first | rest] = manifest.translations
    write_manifest(root, %{manifest | translations: [first, first | rest]})

    assert_error(root, definitions, first, :duplicate)

    root = copy_corpus({test, :missing})
    manifest = read_manifest(root)
    [first | rest] = manifest.translations
    write_manifest(root, %{manifest | translations: rest})

    assert_error(root, definitions, first, :missing)

    root = copy_corpus({test, :missing_artifact})
    [first | _rest] = read_manifest(root).translations
    File.rm!(Path.join(root, first.path))

    assert_error(root, definitions, first, :missing)

    root = copy_corpus({test, :unmanifested})
    File.write!(Path.join(root, "unmanifested.rq"), "SELECT ?value WHERE { VALUES ?value { 1 } }")

    assert {:error, errors} = TranslationCorpus.load(definitions, root: root)

    assert Enum.any?(
             errors,
             &match?({:translation, nil, nil, "unmanifested.rq", :unmanifested, _}, &1)
           )

    root = copy_corpus({test, :checksum})
    [first | _rest] = read_manifest(root).translations
    File.write!(Path.join(root, first.path), "\n# changed\n", [:append])

    assert_error(root, definitions, first, :checksum)
  end

  test "rejects placeholder, projection, parser, and mapping-assumption violations", %{test: test} do
    assert {:ok, definitions} = Workload.load()

    for {suffix, operation_id, mutation, rule} <- [
          {:placeholder, "ldbc/snb-bi/read-01@v1.0.3",
           &String.replace(&1, "{{datetime}}", "{{unknown}}"), :placeholders},
          {:projection, "ldbc/snb-bi/read-01@v1.0.3", &String.replace(&1, "?year", "?unexpected"),
           :projected_columns},
          {:parser, "ldbc/snb-bi/read-01@v1.0.3",
           &String.replace(&1, "SELECT", "NOT A QUERY", global: false), :parse},
          {:mapping, "ldbc/snb-bi/read-01@v1.0.3",
           &String.replace(
             &1,
             "https://ldbcouncil.org/snb/graph/snb_bi/initial",
             "https://example.test/wrong-graph"
           ), :mapping_assumptions},
          {:dynamic_graph, "ldbc/snb-bi/read-01@v1.0.3",
           &String.replace(
             &1,
             "WHERE {",
             "WHERE { GRAPH ?unexpectedGraph { ?dynamicS ?dynamicP ?dynamicO . }",
             global: false
           ), :mapping_assumptions},
          {:structural_shell, "ldbc/snb-bi/read-01@v1.0.3", &structural_shell/1,
           :mapping_assumptions},
          {:ordering, "ldbc/snb-bi/read-01@v1.0.3",
           &String.replace(&1, "DESC(?year)", "ASC(?year)"), :ordering},
          {:limit, "ldbc/snb-bi/read-02@v1.0.3", &String.replace(&1, "LIMIT 100", "LIMIT 99"),
           :limit}
        ] do
      root = copy_corpus({test, suffix})
      manifest = read_manifest(root)
      entry = Enum.find(manifest.translations, &(&1.operation_id == operation_id))
      path = Path.join(root, entry.path)
      sparql = path |> File.read!() |> mutation.()
      File.write!(path, sparql)
      write_manifest(root, update_checksum(manifest, entry, sparql))

      assert_error(root, definitions, entry, rule)
    end
  end

  defp assert_error(root, definitions, entry, rule) do
    assert {:error, errors} = TranslationCorpus.load(definitions, root: root)

    assert Enum.any?(errors, fn
             {:translation, operation_id, variant, _path, ^rule, _detail} ->
               operation_id == entry.operation_id and variant == entry.variant

             _other ->
               false
           end)
  end

  defp update_checksum(manifest, entry, sparql) do
    checksum = "sha256:" <> (:crypto.hash(:sha256, sparql) |> Base.encode16(case: :lower))

    translations =
      Enum.map(manifest.translations, fn candidate ->
        if candidate.operation_id == entry.operation_id and candidate.variant == entry.variant,
          do: %{candidate | checksum: checksum},
          else: candidate
      end)

    %{manifest | translations: translations}
  end

  defp copy_corpus(test) do
    destination =
      Path.join(
        System.tmp_dir!(),
        "snb_bi_corpus_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(destination) end)
    File.cp_r!(TranslationCorpus.default_root(), destination)
    destination
  end

  defp read_manifest(root) do
    root |> Path.join("manifest.exs") |> Code.eval_file() |> elem(0)
  end

  defp write_manifest(root, manifest) do
    File.write!(
      Path.join(root, "manifest.exs"),
      inspect(manifest, pretty: true, limit: :infinity)
    )
  end

  defp semantic_markers(1), do: ["COUNT(?allMessage)", "AVG(?messageLength)"]
  defp semantic_markers(2), do: ["P100D", "P200D", "ABS(?countWindow1"]
  defp semantic_markers(3), do: ["snb:hasModerator", "snb:replyOf*"]
  defp semantic_markers(4), do: ["?memberCount", "COALESCE(?countedMessages"]
  defp semantic_markers(5), do: ["?messageReplies", "10 * ?likeCount"]
  defp semantic_markers(6), do: ["rdf:predicate snb:likes", "COUNT(DISTINCT ?like)"]
  defp semantic_markers(7), do: ["FILTER NOT EXISTS", "COUNT(DISTINCT ?comment)"]
  defp semantic_markers(8), do: ["?individualScore", "SUM(?friendScore)"]
  defp semantic_markers(9), do: ["COUNT(DISTINCT ?post)", "COUNT(DISTINCT ?reply)"]
  defp semantic_markers(11), do: ["?k1Date", "?k2Date", "?k3Date"]
  defp semantic_markers(12), do: ["VALUES ?language", "COUNT(?person)"]
  defp semantic_markers(13), do: ["HAVING(COUNT(?candidateMessage)", "?zombieScore"]
  defp semantic_markers(14), do: ["?candidateScore", "FILTER NOT EXISTS"]
  defp semantic_markers(16), do: ["HAVING(COUNT(DISTINCT ?friend)", "?messageCountB"]
  defp semantic_markers(17), do: ["?minimumDelay", "snb:hasMember"]
  defp semantic_markers(18), do: ["COUNT(DISTINCT ?mutualFriend)", "FILTER NOT EXISTS"]

  defp structural_shell(_sparql) do
    """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX snb: <https://ldbcouncil.org/snb/ontology/>

    SELECT ?year ?isComment ?lengthCategory ?messageCount ?averageMessageLength ?sumMessageLength ?percentageOfMessages
    FROM <https://ldbcouncil.org/snb/graph/snb_bi/initial>
    FROM <https://ldbcouncil.org/snb/graph/snb_bi/updates>
    WHERE {
      BIND({{datetime}} AS ?ignored)
      VALUES (?year ?isComment ?lengthCategory ?messageCount ?averageMessageLength ?sumMessageLength ?percentageOfMessages) {
        (2012 false 0 0 0.0 0 0.0)
      }
      ?translationAnchor rdf:type snb:Message .
    }
    ORDER BY DESC(?year) ASC(?isComment) ASC(?lengthCategory)
    """
  end
end
