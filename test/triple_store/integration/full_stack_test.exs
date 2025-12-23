defmodule TripleStore.Integration.FullStackTest do
  @moduledoc """
  Minimal end-to-end integration test for Phase 1 storage foundation.

  Uses an isolated DB (not the pool) to avoid interference from other tests.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index

  setup do
    path =
      System.tmp_dir!() <>
        "/ts_fullstack_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = NIF.open(path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      File.rm_rf(path)
    end)

    {:ok, db: db, manager: manager}
  end

  test "RDF triple round-trips through dictionary and index", %{db: db, manager: manager} do
    subject = RDF.iri("http://example.org/subject")
    predicate = RDF.iri("http://example.org/predicate")
    object = RDF.literal("Hello, World!")

    {:ok, [s_id, p_id, o_id]} = Adapter.terms_to_ids(manager, [subject, predicate, object])

    :ok = Index.insert_triple(db, {s_id, p_id, o_id})

    {:ok, results} = Index.lookup_all(db, {{:bound, s_id}, {:bound, p_id}, :var})
    assert results == [{s_id, p_id, o_id}]

    {:ok, decoded_s} = Adapter.id_to_term(db, s_id)
    {:ok, decoded_p} = Adapter.id_to_term(db, p_id)
    {:ok, decoded_o} = Adapter.id_to_term(db, o_id)

    assert decoded_s == subject
    assert decoded_p == predicate
    assert decoded_o == object
  end
end
