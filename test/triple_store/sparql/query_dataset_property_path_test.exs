defmodule TripleStore.SPARQL.QueryDatasetPropertyPathTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Query

  test "FROM graphs form the active default graph for quad property paths", %{test: test} do
    path =
      Path.join(
        System.tmp_dir!(),
        "query_dataset_path_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    assert {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    assert {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    terms =
      for name <- ["a", "b", "c", "d", "e", "f", "x", "y", "z", "p", "q", "g1", "g2", "g3"],
          into: %{} do
        {:ok, id} = Manager.get_or_create_id(manager, RDF.iri("http://example.org/#{name}"))
        {name, id}
      end

    assert :ok =
             QuadOperations.insert_quads(
               db,
               [
                 {terms["a"], terms["p"], terms["b"], terms["g1"]},
                 {terms["b"], terms["q"], terms["c"], terms["g2"]},
                 {terms["d"], terms["p"], terms["e"], terms["g2"]},
                 {terms["e"], terms["q"], terms["f"], terms["g2"]},
                 {terms["x"], terms["p"], terms["y"], terms["g3"]},
                 {terms["y"], terms["q"], terms["z"], terms["g3"]}
               ],
               []
             )

    query = """
    SELECT ?s ?o
    FROM <http://example.org/g1>
    FROM <http://example.org/g2>
    WHERE { ?s <http://example.org/p>/<http://example.org/q> ?o }
    ORDER BY ?s
    """

    assert {:error, :unauthorized} = Query.query(%{db: db, dict_manager: manager}, query)

    assert {:ok, rows} =
             Query.query(%{db: db, dict_manager: manager, permit_all: true}, query)

    assert rows == [
             %{
               "s" => {:named_node, "http://example.org/a"},
               "o" => {:named_node, "http://example.org/c"}
             },
             %{
               "s" => {:named_node, "http://example.org/d"},
               "o" => {:named_node, "http://example.org/f"}
             }
           ]
  end
end
