defmodule TripleStore.Reasoner.FactLoaderTest do
  @moduledoc """
  Verifies tagged local-materialization input failures and iterator ownership.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Index
  alias TripleStore.Reasoner.FactLoader

  defp open_adapter(opts \\ []) do
    path =
      Path.join(
        System.tmp_dir!(),
        "fact_loader_#{System.unique_integer([:positive, :monotonic])}"
      )

    {:ok, db} = ErlangAdapter.open(path, opts)
    on_exit(fn -> File.rm_rf!(path) end)
    {db, path}
  end

  defp close_adapter(db) do
    if Process.alive?(db), do: ErlangAdapter.close(db)
  end

  defp adapter_links(db) do
    case Process.info(db, :links) do
      {:links, links} -> MapSet.new(links)
      nil -> MapSet.new()
    end
  end

  describe "load_facts_from_db/2" do
    test "loads explicit triples into a fact set" do
      {db, _path} = open_adapter()

      try do
        assert :ok = Index.insert_triple(db, {1, 2, 3})
        assert :ok = Index.insert_triple(db, {4, 5, 6})

        assert {:ok, facts} = FactLoader.load_facts_from_db(db, [])
        assert facts == MapSet.new([{1, 2, 3}, {4, 5, 6}])
      after
        close_adapter(db)
      end
    end

    test "returns a tagged iterator error when scan setup fails" do
      {db, _path} = open_adapter(read_failure: {:spo, :injected_iterator_failure})

      try do
        assert {:error, {:fact_iterator_failed, :injected_iterator_failure}} =
                 FactLoader.load_facts_from_db(db, [])

        assert adapter_links(db) == MapSet.new()
      after
        close_adapter(db)
      end
    end

    test "closes the owned iterator when scanning fails" do
      {db, _path} = open_adapter(iterator_move_failure: {:spo, :injected_scan_failure})
      links_before = adapter_links(db)

      try do
        assert {:error, {:fact_scan_failed, :injected_scan_failure}} =
                 FactLoader.load_facts_from_db(db, [])

        assert adapter_links(db) == links_before
      after
        close_adapter(db)
      end
    end

    test "stops at a malformed persisted key and closes the iterator" do
      {db, _path} = open_adapter()
      links_before = adapter_links(db)

      try do
        assert :ok = ErlangAdapter.put(db, :spo, <<0>>, <<>>)

        assert {:error, {:fact_decode_failed, {:invalid_spo_key_size, 1}}} =
                 FactLoader.load_facts_from_db(db, [])

        assert adapter_links(db) == links_before
      after
        close_adapter(db)
      end
    end

    test "returns a tagged storage error when the adapter is unavailable" do
      {db, _path} = open_adapter()
      assert :ok = ErlangAdapter.close(db)

      assert {:error, {:fact_storage_failed, _reason}} =
               FactLoader.load_facts_from_db(db, [])
    end
  end

  describe "TripleStore.materialize/2" do
    test "propagates injected input scan failures through the public facade" do
      {db, _path} = open_adapter(read_failure: {:spo, :injected_materialization_failure})
      store = %{db: db, dict_manager: self()}

      try do
        assert {:error, {:fact_iterator_failed, :injected_materialization_failure}} =
                 TripleStore.materialize(store, profile: :rdfs)
      after
        close_adapter(db)
      end
    end

    test "propagates malformed persisted keys through the public facade" do
      {db, _path} = open_adapter()
      store = %{db: db, dict_manager: self()}

      try do
        assert :ok = ErlangAdapter.put(db, :spo, <<1, 2, 3>>, <<>>)

        assert {:error, {:fact_decode_failed, {:invalid_spo_key_size, 3}}} =
                 TripleStore.materialize(store, profile: :rdfs)
      after
        close_adapter(db)
      end
    end
  end
end
