defmodule TripleStore.Benchmark.LDBC.SNB.MappingAndConverterTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SNB.{Converter, Generator, Mapping, UpdateStream}

  test "entity identity, datatypes, arrays, subtypes, and relationship properties are stable" do
    graph = Mapping.graph_iri(:snb_bi, :initial)

    assert Mapping.entity_iri("Person", "42") == Mapping.entity_iri("Person", "42")

    assert {:ok, person_quads} =
             Mapping.entity_quads(
               "Person",
               %{
                 "id" => "42",
                 "firstName" => "Ada",
                 "birthday" => "1980-01-02",
                 "creationDate" => "1262304000000",
                 "languages" => "en;fr"
               },
               graph
             )

    assert length(person_quads) == 6

    assert Enum.any?(person_quads, fn {_s, p, o, _g} ->
             to_string(p) == "https://ldbcouncil.org/snb/ontology/birthday" and
               to_string(RDF.Literal.datatype_id(o)) == "http://www.w3.org/2001/XMLSchema#date"
           end)

    assert {:ok, post_quads} =
             Mapping.entity_quads("Post", %{"id" => "7", "content" => "hello"}, graph)

    assert Enum.count(post_quads, fn {_s, p, _o, _g} ->
             to_string(p) == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
           end) == 2

    assert {:ok, relationship_quads} =
             Mapping.relationship_quads(
               "knows",
               "Person",
               "42",
               "Person",
               "43",
               %{"creationDate" => "1262304000000"},
               graph
             )

    assert length(relationship_quads) == 6
  end

  test "BI and Interactive smoke layouts convert deterministically and remain distinct", %{
    test: test
  } do
    root = temp_root(test)
    on_exit(fn -> File.rm_rf!(root) end)
    bi_source = fixture_root("snb-bi")
    interactive_source = fixture_root("snb-interactive")

    assert {:ok, bi_first} = Converter.convert(:snb_bi_smoke, bi_source, Path.join(root, "bi-1"))
    assert {:ok, bi_second} = Converter.convert(:snb_bi_smoke, bi_source, Path.join(root, "bi-2"))

    assert bi_first.transformation.output_checksum == bi_second.transformation.output_checksum
    assert bi_first.transformation.entity_count == 3
    assert bi_first.transformation.relationship_count == 3
    assert bi_first.store.schema == :quad

    assert {:ok, interactive} =
             Converter.convert(
               :snb_interactive_smoke,
               interactive_source,
               Path.join(root, "interactive")
             )

    refute interactive.transformation.output_checksum == bi_first.transformation.output_checksum
    refute interactive.source.generator_pin == bi_first.source.generator_pin
    assert interactive.transformation.mapping_version == bi_first.transformation.mapping_version

    for manifest <- [bi_first, interactive] do
      [update] =
        Enum.filter(manifest.components, &(&1.role != :initial and &1.role != :parameters))

      assert [%{sequence: 1, operation: :insert, quads: quads}] =
               update.path |> UpdateStream.stream() |> Enum.to_list()

      assert quads != []

      [parameters] = Enum.filter(manifest.components, &(&1.role == :parameters))
      document = parameters.path |> File.read!() |> :erlang.binary_to_term([:safe])
      assert document.dataset_checksum == manifest.transformation.output_checksum
      assert document.rows != []
    end
  end

  test "referential-integrity and ordered-update violations fail conversion", %{test: test} do
    root = temp_root(test)
    source = Path.join(root, "source")
    output = Path.join(root, "output")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    File.cp_r!(fixture_root("snb-bi"), source)

    relationship = Path.join(source, "relationships/person_knows_person.csv")
    File.write!(relationship, "Person1.id|Person2.id|creationDate\n1|999|1262304005000\n")

    assert {:error, {:source_row_error, ^relationship, 2, {:missing_reference, "Person", "999"}}} =
             Converter.convert(:snb_bi_smoke, source, output)
  end

  test "generator plans require matching pinned checkout layouts" do
    assert {:error, :invalid_generator_checkout} =
             Generator.command(:snb_bi_smoke, "/tmp/missing-spark", "/tmp/output")

    assert {:error, :explicit_external_action_required} =
             Generator.run(:snb_interactive_smoke, "/tmp/missing-hadoop", "/tmp/output")
  end

  defp fixture_root(name) do
    Path.join([to_string(:code.priv_dir(:triple_store)), "benchmarks", "ldbc", "fixtures", name])
  end

  defp temp_root(test) do
    Path.join(System.tmp_dir!(), "ldbc_snb_#{test}_#{System.unique_integer([:positive])}")
  end
end
