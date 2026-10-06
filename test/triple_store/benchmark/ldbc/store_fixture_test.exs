defmodule TripleStore.Benchmark.LDBC.StoreFixtureTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.SNB.Converter
  alias TripleStore.Benchmark.LDBC.SPB.Pipeline
  alias TripleStore.Benchmark.LDBC.{StoreFixture, StreamLoader}

  test "SPB loads through all quad indices, reopens, reports metrics, and locks its path", %{
    test: test
  } do
    root = temp_root(test)
    dataset = Path.join(root, "dataset")
    fixture_root = Path.join(root, "fixture")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(dataset, seed: 91)
    assert {:ok, fixture} = StoreFixture.setup(fixture_root, manifest, batch_size: 2)

    assert fixture.load_metrics.count == 7
    assert fixture.load_metrics.parse_us > 0
    assert fixture.load_metrics.dictionary_us > 0
    assert fixture.load_metrics.write_us > 0
    assert fixture.load_metrics.throughput_statements_per_second > 0
    assert fixture.load_metrics.memory_high_water_bytes > 0
    assert fixture.load_metrics.store_size_bytes > 0
    assert fixture.load_metrics.warnings == []
    assert fixture.verification.index_counts == %{gspo: 7, gpos: 7, spog: 7, posg: 7}

    assert {:error, {:fixture_locked, _path}} =
             StoreFixture.setup(fixture_root, manifest)

    assert {:ok, closed} = StoreFixture.close(fixture)
    assert {:ok, reopened} = StoreFixture.reopen(closed)
    assert reopened.verification.statement_count == 7
    assert :ok = StoreFixture.teardown(reopened, delete: true)
    refute File.exists?(fixture.lock_path)
  end

  test "an ordered SNB update changes state and reset restores exact pristine counts", %{
    test: test
  } do
    root = temp_root(test)
    output = Path.join(root, "converted")
    fixture_root = Path.join(root, "fixture")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_interactive_smoke, snb_fixture("snb-interactive"), output)

    expected = manifest.transformation.statement_count

    count_probe = fn store ->
      case count_index(store.db, :gspo) do
        ^expected -> :ok
        actual -> {:error, {:unexpected_count, expected, actual}}
      end
    end

    assert {:ok, fixture} =
             StoreFixture.setup(fixture_root, manifest, batch_size: 3, probes: [count_probe])

    update = Enum.find(manifest.components, &(&1.role == :interactive_update_stream))

    assert {:ok, updated, %{records: 1, last_sequence: 1}} =
             StoreFixture.apply_updates(fixture, update.path)

    assert count_index(updated.store.db, :gspo) > expected
    assert {:ok, reset} = StoreFixture.reset(updated, probes: [count_probe])
    assert count_index(reset.store.db, :gspo) == expected
    assert :ok = StoreFixture.teardown(reset, delete: true)
  end

  test "cancellation closes resources, removes staging state, and releases the lock", %{
    test: test
  } do
    root = temp_root(test)
    dataset = Path.join(root, "dataset")
    fixture_root = Path.join(root, "fixture")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(dataset)

    assert {:error, {:load_cancelled, metrics}} =
             StoreFixture.setup(fixture_root, manifest, cancel?: fn -> true end)

    assert metrics.count == 0
    lock = Path.join([fixture_root, "locks", manifest.store.path_identity <> ".lock"])
    store = Path.join([fixture_root, "stores", manifest.store.path_identity])
    refute File.exists?(lock)
    refute File.exists?(store)

    assert {:ok, fixture} = StoreFixture.setup(fixture_root, manifest)
    assert {:ok, verification} = StreamLoader.verify(fixture.store, 7)
    assert verification.schema == :quad
    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  test "a stale fixture path is rejected without deleting its existing data", %{test: test} do
    root = temp_root(test)
    dataset = Path.join(root, "dataset")
    fixture_root = Path.join(root, "fixture")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(dataset)
    store = Path.join([fixture_root, "stores", manifest.store.path_identity])
    pristine = Path.join([fixture_root, "pristine", manifest.store.path_identity])
    File.mkdir_p!(store)
    File.mkdir_p!(pristine)
    File.write!(Path.join(store, "sentinel"), "store")
    File.write!(Path.join(pristine, "sentinel"), "pristine")

    assert {:error, :store_already_exists} = StoreFixture.setup(fixture_root, manifest)
    assert File.read!(Path.join(store, "sentinel")) == "store"
    assert File.read!(Path.join(pristine, "sentinel")) == "pristine"

    lock = Path.join([fixture_root, "locks", manifest.store.path_identity <> ".lock"])
    refute File.exists?(lock)
  end

  defp count_index(db, index) do
    ErlangAdapter.fold_keys(db, index, <<>>, 0, fn _key, count -> count + 1 end,
      fill_cache: false
    )
  end

  defp snb_fixture(name) do
    Path.join([to_string(:code.priv_dir(:triple_store)), "benchmarks", "ldbc", "fixtures", name])
  end

  defp temp_root(test) do
    Path.join(System.tmp_dir!(), "ldbc_store_#{test}_#{System.unique_integer([:positive])}")
  end
end
