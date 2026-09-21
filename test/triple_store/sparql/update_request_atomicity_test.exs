defmodule TripleStore.SPARQL.UpdateRequestAtomicityTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Index
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.SPARQL.UpdateSession

  test "a later planning failure discards earlier triple mutations" do
    {db, manager, ctx} = open_context(:triple)

    assert {:ok, ast} =
             Parser.parse_update("""
             INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> } ;
             LOAD <http://example.org/not-loaded.ttl>
             """)

    assert {:error, :load_not_implemented} = UpdateExecutor.execute(ctx, ast)
    assert {:ok, 0} = Index.count(db, {:var, :var, :var})

    close_context(db, manager)
  end

  test "a later planning failure discards earlier quad mutations" do
    {db, manager, ctx} = open_context(:quad)

    assert {:ok, ast} =
             Parser.parse_update("""
             INSERT DATA {
               GRAPH <http://example.org/g> {
                 <http://example.org/s> <http://example.org/p> <http://example.org/o>
               }
             } ;
             LOAD <http://example.org/not-loaded.ttl>
             """)

    assert {:error, :load_not_implemented} = UpdateExecutor.execute(ctx, ast)
    assert [] == ErlangAdapter.fold_keys(db, :gspo, <<>>, [], fn key, acc -> [key | acc] end)

    close_context(db, manager)
  end

  test "final mixed-batch failure leaves every explicit triple index unchanged" do
    {db, manager, ctx} = open_context(:triple, mixed_batch_failure: :injected)

    assert {:ok, ast} =
             Parser.parse_update(
               "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
             )

    assert {:error, {:storage, :injected}} = UpdateExecutor.execute(ctx, ast)

    for index <- [:spo, :pos, :osp] do
      assert [] == ErlangAdapter.fold_keys(db, index, <<>>, [], fn key, acc -> [key | acc] end)
    end

    close_context(db, manager)
  end

  test "later operations read staged inserts and DELETE-before-INSERT wins" do
    {db, manager, ctx} = open_context(:triple)

    assert {:ok, ast} =
             Parser.parse_update("""
             INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> } ;
             DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }
             """)

    assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)
    assert {:ok, 0} = Index.count(db, {:var, :var, :var})

    assert {:ok, seed_ast} =
             Parser.parse_update(
               "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
             )

    assert {:ok, 1} = UpdateExecutor.execute(ctx, seed_ast)

    assert {:ok, replace_ast} =
             Parser.parse_update("""
             DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> } ;
             INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }
             """)

    assert {:ok, 2} = UpdateExecutor.execute(ctx, replace_ast)
    assert {:ok, 1} = Index.count(db, {:var, :var, :var})

    close_context(db, manager)
  end

  test "session reads its own staged mutations without publishing them" do
    {db, manager, ctx} = open_context(:triple)
    assert {:ok, session} = UpdateSession.start_link(ctx)
    assert UpdateSession.staging?(session)

    assert :ok = ErlangAdapter.write_batch(session, [{:spo, <<1::64, 2::64, 3::64>>, <<>>}])
    assert {:ok, true} = ErlangAdapter.exists(session, :spo, <<1::64, 2::64, 3::64>>)
    assert {:ok, false} = ErlangAdapter.exists(db, :spo, <<1::64, 2::64, 3::64>>)

    assert {:ok, %{mutation_count: 1}} = UpdateSession.commit(session)
    assert {:ok, true} = ErlangAdapter.exists(db, :spo, <<1::64, 2::64, 3::64>>)

    UpdateSession.stop(session)
    close_context(db, manager)
  end

  test "MODIFY reads an earlier staged insert" do
    {db, manager, ctx} = open_context(:triple)

    assert {:ok, ast} =
             Parser.parse_update("""
             INSERT DATA {
               <http://example.org/s> <http://example.org/old> "value"
             } ;
             DELETE { ?s <http://example.org/old> ?value }
             INSERT { ?s <http://example.org/new> ?value }
             WHERE  { ?s <http://example.org/old> ?value }
             """)

    assert {:ok, 3} = UpdateExecutor.execute(ctx, ast)
    assert {:ok, 1} = Index.count(db, {:var, :var, :var})

    close_context(db, manager)
  end

  test "COPY reads a source graph inserted earlier in the same request" do
    {db, manager, ctx} = open_context(:quad)

    assert {:ok, ast} =
             Parser.parse_update("""
             INSERT DATA {
               GRAPH <http://example.org/source> {
                 <http://example.org/s> <http://example.org/p> <http://example.org/o>
               }
             } ;
             COPY GRAPH <http://example.org/source> TO GRAPH <http://example.org/target>
             """)

    assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)
    assert 2 == ErlangAdapter.fold_keys(db, :gspo, <<>>, 0, fn _key, count -> count + 1 end)

    close_context(db, manager)
  end

  defp open_context(schema, opts \\ []) do
    path =
      Path.join(
        System.tmp_dir!(),
        "update_request_atomicity_#{schema}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    {:ok, db} = ErlangAdapter.open(path, Keyword.put(opts, :schema, schema))
    {:ok, manager} = DictManager.start_link(db: db)
    {db, manager, %{db: db, dict_manager: manager}}
  end

  defp close_context(db, manager) do
    if Process.alive?(manager), do: DictManager.stop(manager)
    ErlangAdapter.close(db)
  end
end
