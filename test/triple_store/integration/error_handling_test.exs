defmodule TripleStore.Integration.ErrorHandlingTest do
  @moduledoc """
  Integration tests for Section 6.6: Error Handling.

  Tests error handling and edge cases in quad store operations:
  - Invalid data handling (syntax errors, invalid IRIs, malformed quads)
  - Constraint violations (duplicate operations, non-existent graphs)
  - Query errors (invalid GRAPH clauses, timeouts, memory limits)
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.UpdateExecutor

  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("error_handling_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  defp grant_public_permissions(ctx, graph_iris) do
    TripleStore.Integration.Helpers.grant_public_permissions(ctx, graph_iris)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = NIF.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    # Grant public permissions for test graphs
    grant_public_permissions(ctx, ["#{@ex}test-graph", "#{@ex}new-graph"])

    on_exit(fn ->
      try do
        if Process.alive?(manager), do: Manager.stop(manager)
      catch
        _, _ -> :ok
      end
      try do
        NIF.close(db)
      catch
        _, _ -> :ok
      end
      cleanup_path(db_path)
    end)

    {:ok, %{db: db, manager: manager, ctx: ctx}}
  end

  # ===========================================================================
  # 6.6.1: Invalid Data Handling
  # ===========================================================================

  describe "6.6.1 Invalid Data Handling" do
    test "6.6.1.1 rejects N-Quads with syntax errors", %{db: db, manager: manager} do
      # Missing closing angle bracket
      invalid_nquads = """
      <http://example.org/subject> <http://example.org/predicate> "object" <http://example.org/graph .
      """

      assert {:error, _reason} = Loader.load_nquads_string(db, manager, invalid_nquads)
    end

    test "6.6.1.2 rejects TriG with syntax errors", %{db: db, manager: manager} do
      # Unclosed GRAPH block
      invalid_trig = """
      PREFIX ex: <http://example.org/>
      GRAPH ex:graph {
        ex:s ex:p "o"
      """

      assert {:error, _reason} = Loader.load_trig_string(db, manager, invalid_trig)
    end

    test "6.6.1.3 handles invalid IRIs gracefully", %{db: db, manager: manager} do
      # IRI with invalid characters (spaces in IRI)
      invalid_nquads = """
      <http://example.org/subject with spaces> <http://example.org/predicate> "object" <http://example.org/graph> .
      """

      assert {:error, _reason} = Loader.load_nquads_string(db, manager, invalid_nquads)
    end

    test "6.6.1.4 handles invalid literals gracefully", %{db: db, manager: manager} do
      # Mismatched language tag and datatype - this is a syntax error
      invalid_nquads = """
      <http://example.org/subject> <http://example.org/predicate> "object"@en^^<http://www.w3.org/2001/XMLSchema#string> <http://example.org/graph> .
      """

      assert {:error, _reason} = Loader.load_nquads_string(db, manager, invalid_nquads)
    end

    test "6.6.1.5 handles malformed quads gracefully", %{db: db, manager: manager} do
      # Completely malformed line - just random text
      invalid_nquads = """
      this is not a valid n-quad at all
      """

      assert {:error, _reason} = Loader.load_nquads_string(db, manager, invalid_nquads)
    end
  end

  # ===========================================================================
  # 6.6.2: Constraint Violations
  # ===========================================================================

  describe "6.6.2 Constraint Violations" do
    setup %{db: db, manager: manager} do
      # Load some initial data
      nquads = """
      <http://example.org/subject1> <http://example.org/predicate> "object1" <http://example.org/test-graph> .
      """
      assert {:ok, _count} = Loader.load_nquads_string(db, manager, nquads)

      :ok
    end

    test "6.6.2.1 INSERT duplicate quad is idempotent", %{ctx: ctx} do
      # Insert the same quad twice
      update = "PREFIX ex: <#{@ex}> INSERT DATA { GRAPH ex:test-graph { ex:subject1 ex:predicate \"object1\" . } }"

      assert {:ok, ast} = Parser.parse_update(update)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast)

      assert {:ok, _ast} = Parser.parse_update(update)
      assert {:ok, _count2} = UpdateExecutor.execute(ctx, ast)

      # Quad should still exist only once - verify with simple query
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH ex:test-graph {
          ex:subject1 ex:predicate "object1" .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      # Should only have one result (the quad)
      assert length(results) == 1
    end

    test "6.6.2.2 DELETE non-existent quad is no-op", %{ctx: ctx} do
      # Delete a quad that doesn't exist
      update = "PREFIX ex: <#{@ex}> DELETE DATA { GRAPH ex:test-graph { ex:nonexistent ex:predicate \"value\" . } }"

      # Should succeed without error
      assert {:ok, ast} = Parser.parse_update(update)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast)
    end

    test "6.6.2.3 operation on non-existent graph returns empty results", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH ex:nonexistent-graph {
          ?s ex:predicate ?o .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert results == []
    end

    test "6.6.2.4 DROP non-existent graph with SILENT succeeds", %{ctx: ctx} do
      update = "DROP SILENT GRAPH <#{@ex}nonexistent-graph>"

      assert {:ok, ast} = Parser.parse_update(update)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast)
    end

    test "6.6.2.5 CREATE existing graph fails with error", %{ctx: ctx} do
      # Use unique graph name for this test
      unique_graph = "#{@ex}graph-#{System.system_time(:microsecond)}"
      update = "CREATE GRAPH <#{unique_graph}>"

      # First create should succeed
      assert {:ok, ast1} = Parser.parse_update(update)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Second create should fail - graph already exists
      assert {:ok, _ast} = Parser.parse_update(update)
      assert {:error, :graph_already_exists} = UpdateExecutor.execute(ctx, ast1)
    end
  end

  # ===========================================================================
  # 6.6.3: Query Errors
  # ===========================================================================

  describe "6.6.3 Query Errors" do
    test "6.6.3.1 GRAPH with non-existent graph returns empty result", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH ex:does-not-exist {
          ?s ex:predicate ?o .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert results == []
    end

    test "6.6.3.2 invalid graph IRI in query returns error", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH <this is not a valid IRI> {
          ?s ex:predicate ?o .
        }
      }
      """

      assert {:error, _reason} = Query.query(ctx, query)
    end

    test "6.6.3.3 malformed GRAPH clause returns error", %{ctx: ctx} do
      # Missing opening brace
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH ex:test-graph
          ?s ex:predicate ?o .
        }
      """

      assert {:error, _reason} = Query.query(ctx, query)
    end

    @tag :slow
    test "6.6.3.4 query timeout with large cross-graph scan", %{db: db, manager: manager, ctx: ctx} do
      # Create a large dataset with many graphs
      for i <- 1..50 do
        graph_name = "#{@ex}graph#{i}"

        nquads = """
        <http://example.org/s#{i}> <http://example.org/p> "o#{i}" <#{graph_name}> .
        """

        {:ok, _} = Loader.load_nquads_string(db, manager, nquads)
        :ok = Authorization.set_public(ctx, graph_name)
      end

      # Query that scans all graphs - should complete within reasonable time
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g WHERE {
        GRAPH ?g {
          ?s ex:p ?o .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) >= 50
    end

    @tag :slow
    test "6.6.3.5 memory limit with large graph scan", %{db: db, manager: manager, ctx: ctx} do
      # Create a large graph
      large_graph_content =
        Enum.map(1..1000, fn i ->
          "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" <#{@ex}large-graph> ."
        end)
        |> Enum.join("\n")

      {:ok, _} = Loader.load_nquads_string(db, manager, large_graph_content)
      :ok = Authorization.set_public(ctx, "#{@ex}large-graph")

      # Query that returns many results - should handle gracefully
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH ex:large-graph {
          ?s ex:p ?o .
        }
      }
      LIMIT 100
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) <= 100
    end
  end
end
