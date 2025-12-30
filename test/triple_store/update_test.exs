defmodule TripleStore.UpdateTest do
  use ExUnit.Case, async: false

  alias TripleStore.Update
  alias TripleStore.Transaction
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.SPARQL.Query

  @moduletag :update_api

  setup do
    db_path = "/tmp/triple_store_update_test_#{System.unique_integer([:positive])}"
    {:ok, db} = NIF.open(db_path)

    {:ok, manager} = DictManager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: DictManager.stop(manager)
      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    %{db: db, manager: manager, db_path: db_path}
  end

  # ===========================================================================
  # Tests with Context Map (Direct)
  # ===========================================================================

  describe "update/2 with context map" do
    test "inserts a single triple", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      assert {:ok, 1} = result
    end

    test "inserts multiple triples", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
            <http://example.org/bob> <http://example.org/name> "Bob" .
            <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
          }
        """)

      assert {:ok, 3} = result
    end

    test "deletes an existing triple", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert first
      {:ok, 1} =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      # Then delete
      result =
        Update.update(ctx, """
          DELETE DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      assert {:ok, 1} = result
    end

    test "returns 0 for non-existent triple deletion", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result =
        Update.update(ctx, """
          DELETE DATA {
            <http://example.org/nonexistent> <http://example.org/p> <http://example.org/o> .
          }
        """)

      assert {:ok, 0} = result
    end

    test "returns parse error for invalid SPARQL", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result = Update.update(ctx, "NOT VALID SPARQL")

      assert {:error, _} = result
    end

    test "handles typed literals", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
          }
        """)

      assert {:ok, 1} = result
    end

    test "handles language-tagged literals", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice"@en .
          }
        """)

      assert {:ok, 1} = result
    end
  end

  describe "insert/2 with context map" do
    test "inserts triples using RDF.ex terms", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      result = Update.insert(ctx, triples)
      assert {:ok, 1} = result
    end

    test "inserts multiple triples", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("v1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("v2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal("v3")}
      ]

      result = Update.insert(ctx, triples)
      assert {:ok, 3} = result
    end

    test "handles empty list", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result = Update.insert(ctx, [])
      assert {:ok, 0} = result
    end

    test "handles typed literals", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/age"),
         RDF.literal(30, datatype: RDF.NS.XSD.integer())}
      ]

      result = Update.insert(ctx, triples)
      assert {:ok, 1} = result
    end

    test "handles language-tagged literals", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/name"),
         RDF.literal("Alice", language: "en")}
      ]

      result = Update.insert(ctx, triples)
      assert {:ok, 1} = result
    end

    test "handles blank nodes", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.bnode("b1"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      result = Update.insert(ctx, triples)
      assert {:ok, 1} = result
    end
  end

  describe "delete/2 with context map" do
    test "deletes triples using RDF.ex terms", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert first
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      {:ok, 1} = Update.insert(ctx, triples)

      # Then delete
      result = Update.delete(ctx, triples)
      assert {:ok, 1} = result
    end

    test "handles non-existent triples (idempotent)", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      triples = [
        {RDF.iri("http://example.org/nonexistent"), RDF.iri("http://example.org/p"),
         RDF.literal("value")}
      ]

      result = Update.delete(ctx, triples)
      assert {:ok, 0} = result
    end

    test "handles empty list", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result = Update.delete(ctx, [])
      assert {:ok, 0} = result
    end
  end

  describe "execute/2 with context map" do
    test "executes pre-parsed UPDATE AST", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse_update("""
          INSERT DATA {
            <http://example.org/s> <http://example.org/p> <http://example.org/o> .
          }
        """)

      result = Update.execute(ctx, ast)
      assert {:ok, 1} = result
    end
  end

  describe "clear/1 with context map" do
    test "clears all triples", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert some data
      {:ok, 3} =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
            <http://example.org/s2> <http://example.org/p> <http://example.org/o2> .
            <http://example.org/s3> <http://example.org/p> <http://example.org/o3> .
          }
        """)

      # Clear
      result = Update.clear(ctx)
      assert {:ok, 3} = result
    end

    test "handles empty database", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      result = Update.clear(ctx)
      assert {:ok, 0} = result
    end
  end

  # ===========================================================================
  # Tests with Transaction Manager
  # ===========================================================================

  describe "update/2 with Transaction manager" do
    test "inserts via transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result =
        Update.update(txn, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "deletes via transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert first
      {:ok, 1} =
        Update.update(txn, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      # Then delete
      result =
        Update.update(txn, """
          DELETE DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "insert/2 with Transaction manager" do
    test "inserts via transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      result = Update.insert(txn, triples)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "delete/2 with Transaction manager" do
    test "deletes via transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert first
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      {:ok, 1} = Update.insert(txn, triples)

      # Then delete
      result = Update.delete(txn, triples)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "execute/2 with Transaction manager" do
    test "executes pre-parsed AST via transaction", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse_update("""
          INSERT DATA {
            <http://example.org/s> <http://example.org/p> <http://example.org/o> .
          }
        """)

      result = Update.execute(txn, ast)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "clear/1 with Transaction manager" do
    test "clears via transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert some data
      {:ok, 2} =
        Update.update(txn, """
          INSERT DATA {
            <http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
            <http://example.org/s2> <http://example.org/p> <http://example.org/o2> .
          }
        """)

      # Clear
      result = Update.clear(txn)
      assert {:ok, 2} = result
      Transaction.stop(txn)
    end
  end

  # ===========================================================================
  # Data Integrity Tests
  # ===========================================================================

  describe "data integrity" do
    test "inserted data is queryable", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert
      {:ok, 1} =
        Update.insert(ctx, [
          {RDF.iri("http://example.org/alice"), RDF.iri("http://example.org/name"),
           RDF.literal("Alice")}
        ])

      # Query
      {:ok, prepared} =
        Query.prepare("""
          SELECT ?name WHERE {
            <http://example.org/alice> <http://example.org/name> ?name .
          }
        """)

      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 1
    end

    test "deleted data is not queryable", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert
      triples = [
        {RDF.iri("http://example.org/alice"), RDF.iri("http://example.org/name"),
         RDF.literal("Alice")}
      ]

      {:ok, 1} = Update.insert(ctx, triples)

      # Delete
      {:ok, 1} = Update.delete(ctx, triples)

      # Query - should be empty
      {:ok, prepared} =
        Query.prepare("""
          SELECT ?name WHERE {
            <http://example.org/alice> <http://example.org/name> ?name .
          }
        """)

      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 0
    end

    test "cleared data is not queryable", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert
      {:ok, 2} =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
            <http://example.org/s2> <http://example.org/p> <http://example.org/o2> .
          }
        """)

      # Clear
      {:ok, 2} = Update.clear(ctx)

      # Query - should be empty
      {:ok, prepared} =
        Query.prepare("""
          SELECT ?s ?o WHERE {
            ?s <http://example.org/p> ?o .
          }
        """)

      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 0
    end

    test "SPARQL UPDATE and direct API are interoperable", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert via SPARQL
      {:ok, 1} =
        Update.update(ctx, """
          INSERT DATA {
            <http://example.org/alice> <http://example.org/name> "Alice" .
          }
        """)

      # Insert via direct API
      {:ok, 1} =
        Update.insert(ctx, [
          {RDF.iri("http://example.org/bob"), RDF.iri("http://example.org/name"),
           RDF.literal("Bob")}
        ])

      # Query should find both
      {:ok, prepared} =
        Query.prepare("""
          SELECT ?name WHERE {
            ?s <http://example.org/name> ?name .
          }
        """)

      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 2

      # Delete via direct API
      {:ok, 1} =
        Update.delete(ctx, [
          {RDF.iri("http://example.org/alice"), RDF.iri("http://example.org/name"),
           RDF.literal("Alice")}
        ])

      # Query should find only Bob
      {:ok, results2} = Query.execute(ctx, prepared)
      assert length(results2) == 1
    end
  end

  # ===========================================================================
  # Named Transaction Manager Tests
  # ===========================================================================

  describe "with named Transaction manager" do
    test "works with named transaction manager", %{db: db, manager: manager} do
      name = :"test_txn_#{System.unique_integer([:positive])}"
      {:ok, _txn} = Transaction.start_link(db: db, dict_manager: manager, name: name)

      result =
        Update.update(name, """
          INSERT DATA {
            <http://example.org/s> <http://example.org/p> <http://example.org/o> .
          }
        """)

      assert {:ok, 1} = result
      Transaction.stop(name)
    end
  end
end
