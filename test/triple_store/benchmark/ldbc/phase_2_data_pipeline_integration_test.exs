defmodule TripleStore.Benchmark.LDBC.Phase2DataPipelineIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Backend.RocksDB.Iterator
  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.DatasetManifest
  alias TripleStore.Benchmark.LDBC.{RDFStream, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SNB.{Converter, Generator, Mapping}
  alias TripleStore.Benchmark.LDBC.SPB.Pipeline
  alias TripleStore.QuadOperations
  alias TripleStore.Snapshot

  @moduletag :integration

  test "SPB generation, load, reopen, parameters, and fail-closed inputs compose end to end",
       %{test: test} do
    root = temp_root(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, inputs} = Pipeline.inputs()
    assert inputs.generator_pin == "ce6323c0936306729408233dc70d26f2389b34c6"
    assert inputs.ontologies != []

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "dataset"), seed: 117)
    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    original_quads = initial_quads(manifest)

    assert length(original_quads) == manifest.transformation.statement_count
    assert Enum.all?(original_quads, &quad_exists?(fixture.store, &1))

    assert original_quads
           |> Enum.map(fn {_s, _p, _o, graph} -> to_string(graph) end)
           |> MapSet.new() ==
             MapSet.new([
               "urn:ldbc:spb:graph:ontology",
               "urn:ldbc:spb:graph:reference",
               "urn:ldbc:spb:graph:creative-works"
             ])

    parameter = Enum.find(manifest.components, &(&1.role == :parameters))
    assert {:ok, parameters} = Pipeline.read_parameters(parameter.path)
    assert parameters.dataset_checksum == manifest.transformation.output_checksum
    assert Enum.all?(parameters.values, &term_present?(fixture.store, RDF.iri(&1)))

    assert {:ok, closed} = StoreFixture.close(fixture)
    assert {:ok, reopened} = StoreFixture.reopen(closed)
    assert Enum.all?(original_quads, &quad_exists?(reopened.store, &1))
    assert :ok = StoreFixture.teardown(reopened, delete: true)

    assert_spb_failures_before_promotion(root, manifest)
  end

  test "BI and Interactive conversion, parameters, updates, and reset retain exact semantics",
       %{test: test} do
    root = temp_root(test)
    on_exit(fn -> File.rm_rf!(root) end)

    for {profile, suite, fixture_name, update_role, entity_count, relationship_count,
         parameter_quad} <- [
          {:snb_bi_smoke, :snb_bi, "snb-bi", :bi_update_batch, 3, 3,
           first_name_quad(:snb_bi, "1", "Alice")},
          {:snb_interactive_smoke, :snb_interactive, "snb-interactive",
           :interactive_update_stream, 3, 2, first_name_quad(:snb_interactive, "10", "Dora")}
        ] do
      output = Path.join(root, "#{profile}-converted")
      fixture_root = Path.join(root, "#{profile}-fixture")
      assert {:ok, manifest} = Converter.convert(profile, snb_fixture(fixture_name), output)
      assert manifest.transformation.entity_count == entity_count
      assert manifest.transformation.relationship_count == relationship_count
      assert manifest.transformation.statement_count > entity_count + relationship_count

      initial = initial_quads(manifest)
      expected_answers = answer_vector(initial)

      assert {:ok, fixture} =
               StoreFixture.setup(fixture_root, manifest,
                 expected_suite: suite,
                 expected_profile: profile
               )

      assert answer_vector(fixture.store, initial) == expected_answers
      assert quad_exists?(fixture.store, parameter_quad)

      parameter = Enum.find(manifest.components, &(&1.role == :parameters))
      parameter_document = parameter.path |> File.read!() |> :erlang.binary_to_term([:safe])
      assert parameter_document.dataset_checksum == manifest.transformation.output_checksum
      assert parameter_document.rows != []

      update = Enum.find(manifest.components, &(&1.role == update_role))
      assert {:ok, updated, %{records: 1}} = StoreFixture.apply_updates(fixture, update.path)
      assert count_index(updated.store.db, :gspo) > manifest.transformation.statement_count

      answer_probe = fn store ->
        if answer_vector(store, initial) == expected_answers,
          do: :ok,
          else: {:error, :answer_vector_changed}
      end

      assert {:ok, reset} = StoreFixture.reset(updated, probes: [answer_probe])
      assert answer_vector(reset.store, initial) == expected_answers
      assert :ok = StoreFixture.teardown(reset, delete: true)
    end
  end

  test "repeated fixture cycles release store processes, snapshots, iterators, and locks",
       %{test: test} do
    root = temp_root(test)
    on_exit(fn -> File.rm_rf!(root) end)
    snapshot_count = Snapshot.count()
    iterator_count = length(iterator_pids())

    for cycle <- 1..2,
        {profile, fixture_name} <- [
          {:snb_bi_smoke, "snb-bi"},
          {:snb_interactive_smoke, "snb-interactive"}
        ] do
      output = Path.join(root, "#{profile}-#{cycle}-converted")
      fixture_root = Path.join(root, "#{profile}-#{cycle}-fixture")
      assert {:ok, manifest} = Converter.convert(profile, snb_fixture(fixture_name), output)
      assert {:ok, fixture} = StoreFixture.setup(fixture_root, manifest)
      initial_pids = store_pids(fixture.store)
      assert {:ok, closed} = StoreFixture.close(fixture)
      assert Enum.all?(initial_pids, &(not Process.alive?(&1)))
      assert {:ok, reopened} = StoreFixture.reopen(closed)
      reopened_pids = store_pids(reopened.store)
      assert {:ok, reset} = StoreFixture.reset(reopened)
      assert Enum.all?(reopened_pids, &(not Process.alive?(&1)))
      final_pids = store_pids(reset.store)
      lock_path = reset.lock_path
      assert :ok = StoreFixture.teardown(reset, delete: true)
      assert Enum.all?(final_pids, &(not Process.alive?(&1)))
      refute File.exists?(lock_path)
    end

    assert Snapshot.count() == snapshot_count
    assert length(iterator_pids()) == iterator_count
  end

  test "all smoke artifacts reproduce and suite expectations prevent manifest interchange",
       %{test: test} do
    root = temp_root(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, spb_1} = Pipeline.generate_smoke(Path.join(root, "spb-1"), seed: 33)
    assert {:ok, spb_2} = Pipeline.generate_smoke(Path.join(root, "spb-2"), seed: 33)
    assert reproducible_manifest(spb_1) == reproducible_manifest(spb_2)

    assert {:ok, bi_1} =
             Converter.convert(:snb_bi_smoke, snb_fixture("snb-bi"), Path.join(root, "bi-1"))

    assert {:ok, bi_2} =
             Converter.convert(:snb_bi_smoke, snb_fixture("snb-bi"), Path.join(root, "bi-2"))

    assert {:ok, interactive_1} =
             Converter.convert(
               :snb_interactive_smoke,
               snb_fixture("snb-interactive"),
               Path.join(root, "interactive-1")
             )

    assert {:ok, interactive_2} =
             Converter.convert(
               :snb_interactive_smoke,
               snb_fixture("snb-interactive"),
               Path.join(root, "interactive-2")
             )

    assert reproducible_manifest(bi_1) == reproducible_manifest(bi_2)
    assert reproducible_manifest(interactive_1) == reproducible_manifest(interactive_2)
    refute bi_1.transformation.output_checksum == interactive_1.transformation.output_checksum

    assert {:error, {:manifest_suite_mismatch, :snb_interactive, :snb_bi}} =
             StoreFixture.setup(Path.join(root, "wrong-suite"), bi_1,
               expected_suite: :snb_interactive
             )

    assert {:error, {:manifest_profile_mismatch, "snb_interactive_smoke", "snb_bi_smoke"}} =
             StoreFixture.setup(Path.join(root, "wrong-profile"), bi_1,
               expected_profile: :snb_interactive_smoke
             )

    assert {:error, :explicit_external_action_required} =
             Generator.run(:snb_bi_smoke, "/not-used", "/not-used")
  end

  defp assert_spb_failures_before_promotion(root, manifest) do
    checkout = Path.join(root, "incomplete-spb-checkout")
    File.mkdir_p!(checkout)
    File.write!(Path.join(checkout, "build.xml"), "")
    File.write!(Path.join(checkout, "test.properties"), "")

    assert {:error, {:missing_spb_input, missing}} =
             Pipeline.external_generator_spec(checkout, Path.join(root, "external"),
               jar: "spb.jar",
               dataset_size: 100,
               seed: 1
             )

    assert String.contains?(missing, "ontologies")

    bad_checksum = put_initial_checksum(manifest, "sha256:" <> String.duplicate("0", 64))
    mismatch_root = Path.join(root, "checksum-mismatch")

    assert {:error, {:component_validation_failed, :initial, {:checksum_mismatch, _, _}}} =
             StoreFixture.setup(mismatch_root, bad_checksum)

    refute File.exists?(mismatch_root)

    initial = Enum.find(manifest.components, &(&1.role == :initial))
    File.write!(initial.path, "<urn:subject> malformed\n")
    {:ok, checksum} = Artifact.checksum(initial.path)

    malformed =
      manifest
      |> put_initial_checksum(checksum)
      |> put_in([Access.key!(:transformation), :output_checksum], checksum)
      |> put_in([Access.key!(:transformation), :statement_count], 1)
      |> update_initial_count(1)

    malformed_root = Path.join(root, "malformed")

    assert {:error, {:load_failed, {:rdf_parse_error, 1, _reason}, _metrics}} =
             StoreFixture.setup(malformed_root, malformed)

    refute File.exists?(Path.join([malformed_root, "stores", malformed.store.path_identity]))

    refute File.exists?(
             Path.join([malformed_root, "locks", malformed.store.path_identity <> ".lock"])
           )
  end

  defp put_initial_checksum(manifest, checksum) do
    components =
      Enum.map(manifest.components, fn
        %{role: :initial} = component -> %{component | checksum: checksum}
        component -> component
      end)

    %{manifest | components: components}
  end

  defp update_initial_count(manifest, count) do
    components =
      Enum.map(manifest.components, fn
        %{role: :initial} = component -> %{component | count: count}
        component -> component
      end)

    %{manifest | components: components}
  end

  defp initial_quads(manifest) do
    initial = Enum.find(manifest.components, &(&1.role == :initial))
    assert {:ok, quads, count} = RDFStream.reduce(initial.path, :nquads, [], &[&1 | &2])
    assert count == initial.count
    Enum.reverse(quads)
  end

  defp first_name_quad(suite, person_id, name) do
    graph = Mapping.graph_iri(suite, :initial)

    {:ok, quads} =
      Mapping.entity_quads("Person", %{"id" => person_id, "firstName" => name}, graph)

    Enum.find(quads, fn {_subject, predicate, _object, _graph} ->
      to_string(predicate) == "https://ldbcouncil.org/snb/ontology/firstName"
    end)
  end

  defp quad_exists?(store, quad) do
    {:ok, encoded} = Adapter.from_rdf_quad(store.dict_manager, quad)
    QuadOperations.quad_exists?(store.db, encoded)
  end

  defp term_present?(store, term) do
    {:ok, id} = Adapter.term_to_id(store.dict_manager, term)

    QuadOperations.lookup_quads(store.db, {:bound, :var, :var, :var}, %{s: id}) != [] or
      QuadOperations.lookup_quads(store.db, {:var, :var, :bound, :var}, %{o: id}) != []
  end

  defp answer_vector(quads), do: %{count: length(quads), quads: MapSet.new(quads)}

  defp answer_vector(store, quads) do
    %{
      count: count_index(store.db, :gspo),
      quads: MapSet.new(Enum.filter(quads, &quad_exists?(store, &1)))
    }
  end

  defp count_index(db, index) do
    ErlangAdapter.fold_keys(db, index, <<>>, 0, fn _key, count -> count + 1 end,
      fill_cache: false
    )
  end

  defp reproducible_manifest(manifest) do
    %{
      identity: DatasetManifest.identity(manifest),
      source: manifest.source,
      transformation: manifest.transformation,
      store: manifest.store,
      components: Enum.map(manifest.components, &Map.take(&1, [:role, :checksum, :count]))
    }
  end

  defp store_pids(store) do
    [store.db, store.dict_manager, store.transaction] |> Enum.filter(&is_pid/1)
  end

  defp iterator_pids do
    Enum.filter(Process.list(), &iterator_process?/1)
  end

  defp iterator_process?(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} ->
        iterator_initial_call?(Keyword.get(dictionary, :"$initial_call"))

      nil ->
        false
    end
  end

  defp iterator_initial_call?({Iterator, _function, _arity}), do: true
  defp iterator_initial_call?(_initial_call), do: false

  defp snb_fixture(name) do
    Path.join([to_string(:code.priv_dir(:triple_store)), "benchmarks", "ldbc", "fixtures", name])
  end

  defp temp_root(test) do
    Path.join(System.tmp_dir!(), "ldbc_phase2_#{test}_#{System.unique_integer([:positive])}")
  end
end
