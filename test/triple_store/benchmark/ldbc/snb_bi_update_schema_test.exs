defmodule TripleStore.Benchmark.LDBC.SNBBIUpdateSchemaTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.UpdateSchema
  alias TripleStore.Benchmark.LDBC.SNB.Mapping

  test "catalog is versioned, pinned, complete, and covered by its fixtures" do
    assert {:ok, catalog} = UpdateSchema.catalog()
    assert catalog.schema_version == 1
    assert catalog.mapping_version == Mapping.version()
    assert catalog.generator.source_id == "snb-bi-datagen-0.5.1"
    assert catalog.generator.version == "v0.5.1"
    assert catalog.generator.pin == "2459f4e45834c78902a50511fc64a05c48dd4029"

    expected =
      for operation <- [:insert, :delete],
          number <- 1..8,
          do: "#{operation}-#{number |> Integer.to_string() |> String.pad_leading(2, "0")}"

    assert Enum.map(catalog.record_types, & &1.id) |> Enum.sort() == Enum.sort(expected)

    assert MapSet.new(Enum.map(UpdateSchema.fixture_records(), & &1.type)) ==
             MapSet.new(expected)

    assert :ok = UpdateSchema.verify_fixture_coverage()
  end

  test "validation rejects unknown and incomplete record shapes" do
    [record | _] = UpdateSchema.fixture_records()

    assert {:error, {:unknown_update_type, "insert-99"}} =
             UpdateSchema.validate_records([%{record | type: "insert-99"}])

    assert {:error, {:unknown_update_fields, 1, ["surprise"]}} =
             UpdateSchema.validate_records([
               put_in(record, [:fields, "surprise"], "not canonical")
             ])

    assert {:error, {:missing_update_fields, 1, ["personId"]}} =
             UpdateSchema.validate_records([
               %{record | fields: Map.delete(record.fields, "personId")}
             ])
  end

  test "validation rejects duplicate identities and out-of-order dependencies" do
    person = fixture("insert-01")

    duplicate = %{person | sequence: person.sequence + 1}

    assert {:error, {:duplicate_update_identity, "insert-01", ["1001"]}} =
             UpdateSchema.validate_records([person, duplicate])

    like = fixture("insert-02") |> Map.put(:sequence, 1)
    post = fixture("insert-06") |> Map.put(:sequence, 2)

    assert {:error, {:out_of_order_update_dependency, 1, {"Post", "6001"}, 2}} =
             UpdateSchema.validate_records([like, post])
  end

  test "all insert fixtures translate through the canonical mapping" do
    records =
      ~w(insert-01 insert-04 insert-06 insert-07 insert-02 insert-03 insert-05 insert-08)
      |> Enum.map(&fixture/1)
      |> resequence()

    assert {:ok, translated} = UpdateSchema.translate_records(nil, records)
    assert Enum.map(translated, & &1.operation) == List.duplicate(:insert, 8)
    assert Enum.all?(translated, &(&1.quads != []))

    quads = Enum.flat_map(translated, & &1.quads)

    assert Enum.all?(quads, fn {_subject, _predicate, _object, graph} ->
             graph == Mapping.graph_iri(:snb_bi, :updates)
           end)
  end

  test "every delete family produces a complete cleanup plan and fails closed without a target" do
    cases = [
      {"delete-01", [person("1001")]},
      {"delete-02",
       [person("1001"), fixture("insert-04"), fixture("insert-06"), fixture("insert-02")]},
      {"delete-03",
       [
         person("1001"),
         fixture("insert-04"),
         fixture("insert-06"),
         fixture("insert-07"),
         fixture("insert-03")
       ]},
      {"delete-04",
       [person("1001"), fixture("insert-04"), fixture("insert-06"), fixture("insert-07")]},
      {"delete-05", [person("1001"), fixture("insert-04"), fixture("insert-05")]},
      {"delete-06",
       [person("1001"), fixture("insert-04"), fixture("insert-06"), fixture("insert-07")]},
      {"delete-07",
       [person("1001"), fixture("insert-04"), fixture("insert-06"), fixture("insert-07")]},
      {"delete-08", [person("1001"), person("1002"), fixture("insert-08")]}
    ]

    for {delete_type, inserts} <- cases do
      records = resequence(inserts ++ [fixture(delete_type)])
      assert {:ok, translated} = UpdateSchema.translate_records(nil, records)
      assert %{type: ^delete_type, operation: :delete, quads: [_ | _]} = List.last(translated)

      delete = fixture(delete_type) |> Map.put(:sequence, 1)

      assert {:error, {:unprovable_delete_cleanup, 1, ^delete_type}} =
               UpdateSchema.translate_records(nil, [delete])
    end
  end

  defp fixture(type), do: Enum.find(UpdateSchema.fixture_records(), &(&1.type == type))

  defp person(id) do
    record = fixture("insert-01")
    %{record | fields: Map.put(record.fields, "personId", id)}
  end

  defp resequence(records) do
    records
    |> Enum.with_index(1)
    |> Enum.map(fn {record, sequence} -> %{record | sequence: sequence} end)
  end
end
