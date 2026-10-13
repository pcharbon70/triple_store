defmodule TripleStore.Benchmark.LDBC.SNBBIParameterReferencesTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.LDBC.{SNB.Converter, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SNB.Mapping

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    ParameterReferences,
    Parameters,
    Workload
  }

  alias TripleStore.QuadOperations

  @moduletag :integration

  test "translation metadata classifies every parameter as scalar or store-backed" do
    assert {:ok, definitions} = Workload.load()

    assert Enum.all?(definitions, fn definition ->
             Map.keys(definition.parameter_references) |> Enum.sort() ==
               Enum.map(definition.parameters, & &1.name) |> Enum.sort()
           end)

    assert reference_rule(definitions, 1, "default", "datetime") == %{kind: :scalar}

    assert reference_rule(definitions, 10, "a", "personId") == %{
             kind: :entity_id,
             entity: "Person"
           }

    assert reference_rule(definitions, 14, "b", "country2") == %{
             kind: :entity_name,
             entity: "Place",
             subtype: "Country"
           }

    assert reference_rule(definitions, 20, "m", "company") == %{
             kind: :entity_name,
             entity: "Organisation",
             subtype: "Company"
           }
  end

  test "store-backed validation is identity-bound, ordered, typed, and smoke-profile explicit",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    assert {:ok, definitions} = Workload.load()

    assert {:ok, bundle} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert :ok =
             Parameters.validate(bundle, fixture, definitions, reference_profile: :snb_bi_smoke)

    assert {:ok, comparable_bundle} = Parameters.new(manifest, parameter_sequences(definitions))

    assert {:error, comparable_errors} =
             Parameters.validate(comparable_bundle, fixture, definitions, comparable: true)

    assert {"2a", 0, "tagClass", {:unknown_reference, :tag_class, "value"}} in comparable_errors

    unknown = put_in(bundle, [:sequences, "10a", Access.at(0), "personId"], 999)

    assert {:error, errors} =
             Parameters.validate(unknown, fixture, definitions, reference_profile: :snb_bi_smoke)

    assert hd(errors) == {"10a", 0, "personId", {:unknown_reference, :person, 999}}

    [valid_10a] = bundle.sequences["10a"]

    ordered_failures =
      bundle
      |> put_in([:sequences, "10a"], [valid_10a, %{valid_10a | "personId" => 997}])
      |> put_in([:sequences, "10b", Access.at(0), "personId"], 998)

    assert {:error, [first, second]} =
             Parameters.validate(ordered_failures, fixture, definitions,
               reference_profile: :snb_bi_smoke
             )

    assert first == {"10a", 1, "personId", {:unknown_reference, :person, 997}}
    assert second == {"10b", 0, "personId", {:unknown_reference, :person, 998}}

    assert :ok = insert_entity(fixture.store, :snb_interactive, "Person", 999, %{})

    assert {:error, errors} =
             Parameters.validate(unknown, fixture, definitions, reference_profile: :snb_bi_smoke)

    assert hd(errors) ==
             {"10a", 0, "personId", {:graph_inappropriate_reference, :person, 999}}

    mismatched = put_in(bundle, [:dataset_identity, :store_path_identity], "another-store")

    assert {:error, errors} =
             Parameters.validate(mismatched, fixture, definitions,
               reference_profile: :snb_bi_smoke
             )

    assert :dataset_identity in errors

    assert {:error, [:store_identity]} =
             Parameters.validate(
               bundle,
               %{fixture | store_path: fixture.store_path <> "-other"},
               definitions,
               reference_profile: :snb_bi_smoke
             )

    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  test "name references reject ambiguity without scanning decoded RDF", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    assert :ok = insert_entity(fixture.store, :snb_bi, "Tag", 100, %{"name" => "duplicate"})
    assert :ok = insert_entity(fixture.store, :snb_bi, "Tag", 101, %{"name" => "duplicate"})

    rule = %{kind: :entity_name, entity: "Tag"}

    assert {:error, {:ambiguous_reference, :tag, "duplicate"}} =
             ParameterReferences.validate_rule(rule, "duplicate", fixture.store)

    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  test "source subtype discriminators distinguish countries, cities, and companies", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    assert :ok =
             insert_entity(fixture.store, :snb_bi, "Place", 100, %{
               "name" => "Shared Place",
               "type" => "Country"
             })

    assert :ok =
             insert_entity(fixture.store, :snb_bi, "Place", 101, %{
               "name" => "Shared Place",
               "type" => "City"
             })

    country = %{kind: :entity_name, entity: "Place", subtype: "Country"}
    city_id = %{kind: :entity_id, entity: "Place", subtype: "City"}
    country_id = %{kind: :entity_id, entity: "Place", subtype: "Country"}

    assert :ok = ParameterReferences.validate_rule(country, "Shared Place", fixture.store)
    assert :ok = ParameterReferences.validate_rule(city_id, 101, fixture.store)

    assert {:error, {:unknown_reference, :country, 101}} =
             ParameterReferences.validate_rule(country_id, 101, fixture.store)

    assert :ok =
             insert_entity(fixture.store, :snb_bi, "Organisation", 200, %{
               "name" => "University Only",
               "type" => "University"
             })

    company = %{kind: :entity_name, entity: "Organisation", subtype: "Company"}

    assert {:error, {:unknown_reference, :company, "University Only"}} =
             ParameterReferences.validate_rule(company, "University Only", fixture.store)

    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  defp reference_rule(definitions, number, variant, name) do
    definition = Enum.find(definitions, &(&1.number == number and &1.variant == variant))
    Map.fetch!(definition.parameter_references, name)
  end

  defp parameter_sequences(definitions) do
    Map.new(definitions, fn definition ->
      label = Integer.to_string(definition.number) <> variant_suffix(definition.variant)
      row = Map.new(definition.parameters, &{&1.name, value_for(&1.name, &1.type)})
      {label, [row]}
    end)
  end

  defp value_for(name, "ID") when name in ["personId", "person1Id", "person2Id"], do: 1
  defp value_for(_name, "ID"), do: 1
  defp value_for(_name, "32-bit Integer"), do: 1
  defp value_for(_name, "64-bit Integer"), do: 1
  defp value_for(_name, "32-bit Float"), do: 1.0
  defp value_for(_name, "Boolean"), do: true
  defp value_for(_name, "Date"), do: "2012-11-29"
  defp value_for(_name, "DateTime"), do: "2012-11-29T00:00:00Z"
  defp value_for(_name, type) when type in ["String", "Long String"], do: "value"
  defp value_for(_name, "\\{String\\}"), do: ["value"]

  defp variant_suffix("default"), do: ""
  defp variant_suffix(variant), do: variant

  defp insert_entity(store, suite, type, id, properties) do
    graph = Mapping.graph_iri(suite, :initial)

    with {:ok, quads} <- Mapping.entity_quads(type, Map.put(properties, "id", id), graph),
         {:ok, encoded} <- Adapter.from_rdf_quads(store.dict_manager, quads) do
      QuadOperations.insert_quads(store.db, encoded, sync: true)
    end
  end

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
      "snb_bi_parameter_refs_#{test}_#{System.unique_integer([:positive])}"
    )
  end
end
