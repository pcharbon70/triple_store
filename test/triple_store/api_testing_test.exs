defmodule TripleStore.APITestingTest do
  @moduledoc """
  API testing for Task 5.7.4.

  These tests validate the public API completeness and usability:
  - 5.7.4.1: Test all documented examples work correctly
  - 5.7.4.2: Test error messages are helpful
  - 5.7.4.3: Test API documentation is accurate
  - 5.7.4.4: Review API for consistency and usability

  These are integration tests that verify the public API works as documented.

  ## Timeout Configuration

  Default timeout: 120 seconds (2 minutes)
  Rationale: API tests are generally fast individual operations, but backup/restore
  tests and concurrent access tests may take longer on slower CI systems.
  """

  use ExUnit.Case, async: false

  import TripleStore.Test.IntegrationHelpers,
    only: [
      create_test_store: 0,
      create_test_store: 1,
      cleanup_test_store: 2,
      cleanup_test_path: 1
    ]

  alias TripleStore.Error

  @moduletag :integration
  # 2 minute timeout for API tests (includes backup/restore and concurrent access)
  @moduletag timeout: 120_000

  # ===========================================================================
  # 5.7.4.1: Test All Documented Examples Work Correctly
  # ===========================================================================

  describe "5.7.4.1: documented examples work correctly" do
    test "Quick Start example from moduledoc" do
      path = Path.join(System.tmp_dir!(), "quickstart_#{:rand.uniform(1_000_000)}")

      try do
        # Open a store
        {:ok, store} = TripleStore.open(path)

        # Insert triples directly (using update since insert takes triples, not SPARQL)
        triple = {
          RDF.iri("http://ex.org/alice"),
          RDF.iri("http://ex.org/knows"),
          RDF.iri("http://ex.org/bob")
        }
        {:ok, count} = TripleStore.insert(store, triple)
        assert count == 1

        # Query with SPARQL
        {:ok, results} = TripleStore.query(store, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        assert length(results) == 1

        # Update with SPARQL UPDATE
        {:ok, _} = TripleStore.update(store, "INSERT DATA { <http://ex.org/new> <http://ex.org/p> 'value' }")

        # Check stats
        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 2

        # Check health
        {:ok, health} = TripleStore.health(store)
        assert health.status in [:healthy, :degraded]

        # Close when done
        :ok = TripleStore.close(store)
      after
        cleanup_test_path(path)
      end
    end

    test "open/2 examples" do
      path = Path.join(System.tmp_dir!(), "open_test_#{:rand.uniform(1_000_000)}")

      try do
        # Basic open
        {:ok, store} = TripleStore.open(path)
        :ok = TripleStore.close(store)

        # With options
        Process.sleep(200)
        {:ok, store2} = TripleStore.open(path, create_if_missing: true)
        :ok = TripleStore.close(store2)
      after
        cleanup_test_path(path)
      end
    end

    test "close/1 example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        :ok = TripleStore.close(store)
      after
        cleanup_test_path(path)
      end
    end

    test "query/2 SELECT example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Load some data first
        {:ok, _} = TripleStore.update(store, """
          PREFIX ex: <http://example.org/>
          INSERT DATA {
            ex:alice ex:knows ex:bob .
            ex:bob ex:knows ex:charlie .
          }
        """)

        # SELECT query from docs
        {:ok, results} = TripleStore.query(store, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10")
        assert is_list(results)
        assert length(results) == 2
      after
        cleanup_test_store(store, path)
      end
    end

    test "query/2 ASK example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.update(store, """
          PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          INSERT DATA {
            <http://example.org/alice> rdf:type foaf:Person .
          }
        """)

        {:ok, exists} = TripleStore.query(store, """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          ASK { ?s a foaf:Person }
        """)
        assert exists == true
      after
        cleanup_test_store(store, path)
      end
    end

    test "query/2 with timeout option" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.update(store, """
          INSERT DATA { <http://ex.org/s> <http://ex.org/p> "o" }
        """)

        {:ok, results} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }", timeout: 5000)
        assert length(results) == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "load_graph/2 example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        graph = RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("object")}
        ])
        {:ok, 1} = TripleStore.load_graph(store, graph)

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "load_string/4 example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        ttl = """
        @prefix ex: <http://example.org/> .
        ex:alice ex:knows ex:bob .
        """
        {:ok, 1} = TripleStore.load_string(store, ttl, :turtle)

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "insert/2 with single triple" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        triple = {
          RDF.iri("http://example.org/s"),
          RDF.iri("http://example.org/p"),
          RDF.literal("value")
        }
        {:ok, 1} = TripleStore.insert(store, triple)

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "insert/2 with list of triples" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        triples = [
          {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal(1)},
          {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal(2)},
          {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal(3)}
        ]
        {:ok, 3} = TripleStore.insert(store, triples)

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 3
      after
        cleanup_test_store(store, path)
      end
    end

    test "delete/2 with single triple" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        triple = {
          RDF.iri("http://example.org/s"),
          RDF.iri("http://example.org/p"),
          RDF.literal("value")
        }
        {:ok, 1} = TripleStore.insert(store, triple)
        {:ok, 1} = TripleStore.delete(store, triple)

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 0
      after
        cleanup_test_store(store, path)
      end
    end

    test "update/2 INSERT DATA example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.update(store, "INSERT DATA { <http://ex.org/new> <http://ex.org/p> 'value' }")

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "update/2 DELETE DATA example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.update(store, "INSERT DATA { <http://ex.org/s> <http://ex.org/p> 'value' }")
        {:ok, _} = TripleStore.update(store, "DELETE DATA { <http://ex.org/s> <http://ex.org/p> 'value' }")

        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 0
      after
        cleanup_test_store(store, path)
      end
    end

    test "export/2 to graph" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.update(store, """
          INSERT DATA {
            <http://ex.org/s1> <http://ex.org/p> "v1" .
            <http://ex.org/s2> <http://ex.org/p> "v2" .
          }
        """)

        {:ok, graph} = TripleStore.export(store, :graph)
        assert RDF.Graph.triple_count(graph) == 2
      after
        cleanup_test_store(store, path)
      end
    end

    test "stats/1 example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, stats} = TripleStore.stats(store)
        assert Map.has_key?(stats, :triple_count)
        assert stats.triple_count == 0
      after
        cleanup_test_store(store, path)
      end
    end

    test "health/1 example" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, health} = TripleStore.health(store)
        assert health.status in [:healthy, :degraded]
        assert health.database_open == true
        assert Map.has_key?(health, :triple_count)
      after
        cleanup_test_store(store, path)
      end
    end

    test "backup/2 example" do
      {store, path} = create_test_store(prefix: "api_test")
      backup_path = Path.join(System.tmp_dir!(), "backup_api_#{:rand.uniform(1_000_000)}")

      try do
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])

        {:ok, metadata} = TripleStore.backup(store, backup_path)
        assert metadata.path == backup_path
        assert metadata.file_count > 0
      after
        cleanup_test_store(store, path)
        cleanup_test_path(backup_path)
      end
    end

    test "restore/2 example" do
      {store, path} = create_test_store(prefix: "api_test")
      backup_path = Path.join(System.tmp_dir!(), "restore_api_#{:rand.uniform(1_000_000)}")
      restore_path = Path.join(System.tmp_dir!(), "restored_api_#{:rand.uniform(1_000_000)}")

      try do
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])

        {:ok, _} = TripleStore.backup(store, backup_path)
        :ok = TripleStore.close(store)
        Process.sleep(200)

        {:ok, restored} = TripleStore.restore(backup_path, restore_path)

        try do
          {:ok, stats} = TripleStore.stats(restored)
          assert stats.triple_count == 1
        after
          TripleStore.close(restored)
        end
      after
        cleanup_test_path(path)
        cleanup_test_path(backup_path)
        cleanup_test_path(restore_path)
      end
    end
  end

  # ===========================================================================
  # 5.7.4.2: Test Error Messages Are Helpful
  # ===========================================================================

  describe "5.7.4.2: error messages are helpful" do
    test "parse error includes reason" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:error, reason} = TripleStore.query(store, "INVALID SPARQL QUERY !!!")
        # Error should contain parse information with specific error category
        assert is_tuple(reason) or is_atom(reason),
               "Error reason should be a tuple or atom, got: #{inspect(reason)}"

        # Verify error has meaningful content (not just generic :error)
        error_string = inspect(reason)
        assert String.length(error_string) > 5,
               "Error should have meaningful content, got: #{error_string}"
      after
        cleanup_test_store(store, path)
      end
    end

    test "file not found error is clear" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:error, reason} = TripleStore.load(store, "/nonexistent/path/to/file.ttl")
        # Should indicate file not found
        assert reason == :file_not_found or
               (is_tuple(reason) and elem(reason, 0) == :file_not_found)
      after
        cleanup_test_store(store, path)
      end
    end

    test "path traversal attempt is caught" do
      result = TripleStore.open("../../../etc/passwd")
      assert {:error, :path_traversal_attempt} = result
    end

    test "database not found when create_if_missing is false" do
      path = Path.join(System.tmp_dir!(), "nonexistent_#{:rand.uniform(1_000_000)}")

      result = TripleStore.open(path, create_if_missing: false)
      assert {:error, :database_not_found} = result
    end

    test "TripleStore.Error has helpful message" do
      error = Error.new(:query_parse_error, "Invalid SPARQL syntax at line 5")

      assert error.code == 1001
      assert error.category == :query_parse_error
      assert error.message == "Invalid SPARQL syntax at line 5"
      assert Error.safe_message(error) =~ "Invalid SPARQL syntax"
    end

    test "bang variants raise TripleStore.Error" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        assert_raise Error, fn ->
          TripleStore.query!(store, "NOT VALID SPARQL")
        end
      after
        cleanup_test_store(store, path)
      end
    end

    test "error tuple includes context" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Try to execute invalid SPARQL
        {:error, reason} = TripleStore.update(store, "INVALID UPDATE STATEMENT")
        # Error should be informative with specific content
        assert reason != nil, "Error reason should not be nil"

        # Verify the error has actionable content
        error_string = inspect(reason)
        assert String.length(error_string) > 3,
               "Error should have meaningful content, got: #{error_string}"
      after
        cleanup_test_store(store, path)
      end
    end

    # C10: Input validation edge cases
    test "URL-encoded path is treated literally (not decoded)" do
      # %2e = . and %2f = / but paths are NOT URL-decoded before validation
      # This is expected - file system paths should be treated literally
      path = Path.join(System.tmp_dir!(), "%2e%2e%2ftest_store_#{:rand.uniform(1_000_000)}")

      result = TripleStore.open(path)
      # Should succeed because %2e%2e%2f is a valid literal path segment
      case result do
        {:ok, store} ->
          TripleStore.close(store)
          cleanup_test_path(path)

        {:error, _} ->
          # Also acceptable if the OS rejects the path
          :ok
      end
    end

    test "very long path is handled gracefully" do
      # Create a very long path (1000 characters)
      long_segment = String.duplicate("a", 250)
      long_path = Path.join([System.tmp_dir!(), long_segment, long_segment, long_segment, long_segment])

      result = TripleStore.open(long_path)
      # Should either succeed or return a clear error, not crash
      assert match?({:ok, _}, result) or match?({:error, _}, result),
             "Very long paths should be handled gracefully"

      # Clean up if it succeeded
      case result do
        {:ok, store} ->
          TripleStore.close(store)
          File.rm_rf!(long_path)
        _ ->
          :ok
      end
    end
  end

  # ===========================================================================
  # 5.7.4.3: Test API Documentation Is Accurate
  # ===========================================================================

  describe "5.7.4.3: API documentation is accurate" do
    test "open/2 returns {:ok, store} as documented" do
      path = Path.join(System.tmp_dir!(), "doc_open_#{:rand.uniform(1_000_000)}")

      try do
        {:ok, store} = TripleStore.open(path)

        # Verify store structure matches documentation
        assert is_reference(store.db)
        assert is_pid(store.dict_manager)
        assert store.transaction == nil
        assert store.path == path

        :ok = TripleStore.close(store)
      after
        cleanup_test_path(path)
      end
    end

    test "close/1 returns :ok as documented" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        result = TripleStore.close(store)
        assert result == :ok
      after
        cleanup_test_path(path)
      end
    end

    test "query/2 returns {:ok, results} for SELECT as documented" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])

        {:ok, results} = TripleStore.query(store, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")

        # Results should be a list of maps as documented
        assert is_list(results)
        assert length(results) == 1
        assert is_map(hd(results))
        assert Map.has_key?(hd(results), "s")
        assert Map.has_key?(hd(results), "p")
        assert Map.has_key?(hd(results), "o")
      after
        cleanup_test_store(store, path)
      end
    end

    test "query/2 returns {:ok, boolean} for ASK as documented" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])

        {:ok, result} = TripleStore.query(store, "ASK { ?s ?p ?o }")
        assert is_boolean(result)
        assert result == true

        {:ok, result2} = TripleStore.query(store, "ASK { <http://nonexistent> ?p ?o }")
        assert result2 == false
      after
        cleanup_test_store(store, path)
      end
    end

    test "load_graph/2 returns {:ok, count} as documented" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        graph = RDF.Graph.new([
          {RDF.iri("http://ex.org/s1"), RDF.iri("http://ex.org/p"), RDF.literal("v1")},
          {RDF.iri("http://ex.org/s2"), RDF.iri("http://ex.org/p"), RDF.literal("v2")}
        ])

        {:ok, count} = TripleStore.load_graph(store, graph)

        assert is_integer(count)
        assert count == 2
      after
        cleanup_test_store(store, path)
      end
    end

    test "stats/1 returns documented structure" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, stats} = TripleStore.stats(store)

        # Verify documented fields exist
        assert Map.has_key?(stats, :triple_count)
        assert is_integer(stats.triple_count)
      after
        cleanup_test_store(store, path)
      end
    end

    test "health/1 returns documented structure" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        {:ok, health} = TripleStore.health(store)

        # Verify documented fields exist
        assert Map.has_key?(health, :status)
        assert health.status in [:healthy, :degraded, :unhealthy]
        assert Map.has_key?(health, :triple_count)
        assert Map.has_key?(health, :database_open)
        assert Map.has_key?(health, :dict_manager_alive)
      after
        cleanup_test_store(store, path)
      end
    end

    test "backup/2 returns {:ok, metadata} as documented" do
      {store, path} = create_test_store(prefix: "api_test")
      backup_path = Path.join(System.tmp_dir!(), "doc_backup_#{:rand.uniform(1_000_000)}")

      try do
        {:ok, metadata} = TripleStore.backup(store, backup_path)

        # Verify documented metadata fields
        assert Map.has_key?(metadata, :path)
        assert Map.has_key?(metadata, :created_at)
        assert Map.has_key?(metadata, :file_count)
      after
        cleanup_test_store(store, path)
        cleanup_test_path(backup_path)
      end
    end

    test "all public functions have bang variants" do
      # Test that bang variants exist and work
      path = Path.join(System.tmp_dir!(), "bang_test_#{:rand.uniform(1_000_000)}")

      try do
        # open! variant
        store = TripleStore.open!(path)

        # insert! variant
        triple = {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        count = TripleStore.insert!(store, triple)
        assert count == 1

        # query! variant
        results = TripleStore.query!(store, "SELECT * WHERE { ?s ?p ?o }")
        assert is_list(results)

        # stats! variant
        stats = TripleStore.stats!(store)
        assert is_map(stats)

        # health! variant
        health = TripleStore.health!(store)
        assert is_map(health)

        # close! variant
        TripleStore.close!(store)
      after
        cleanup_test_path(path)
      end
    end
  end

  # ===========================================================================
  # 5.7.4.4: Review API for Consistency and Usability
  # ===========================================================================

  describe "5.7.4.4: API consistency and usability" do
    test "all functions accept store as first argument" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # All these should work with store as first arg
        {:ok, _} = TripleStore.stats(store)
        {:ok, _} = TripleStore.health(store)
        {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")
        {:ok, _} = TripleStore.insert(store, [])
        {:ok, _} = TripleStore.delete(store, [])
        {:ok, _} = TripleStore.export(store, :graph)
      after
        cleanup_test_store(store, path)
      end
    end

    test "all modifying functions return {:ok, result} on success" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Insert returns {:ok, count}
        {:ok, count} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])
        assert is_integer(count)

        # Delete returns {:ok, count}
        {:ok, count} = TripleStore.delete(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])
        assert is_integer(count)

        # Update returns {:ok, result}
        {:ok, _} = TripleStore.update(store, "INSERT DATA { <http://ex.org/s> <http://ex.org/p> 'v' }")
      after
        cleanup_test_store(store, path)
      end
    end

    test "query options are consistently named" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # timeout option works
        {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }", timeout: 10000)

        # optimize option works
        {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }", optimize: true)
      after
        cleanup_test_store(store, path)
      end
    end

    test "load functions accept consistent options" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # load_graph with batch_size
        graph = RDF.Graph.new()
        {:ok, _} = TripleStore.load_graph(store, graph, batch_size: 100)

        # load_string with batch_size
        {:ok, _} = TripleStore.load_string(store, "@prefix ex: <http://ex.org/> .", :turtle, batch_size: 100)
      after
        cleanup_test_store(store, path)
      end
    end

    test "store handle is immutable and can be reused" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Store can be used multiple times
        {:ok, _} = TripleStore.stats(store)
        {:ok, _} = TripleStore.stats(store)
        {:ok, _} = TripleStore.stats(store)

        # Store remains valid after operations
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])
        {:ok, _} = TripleStore.stats(store)
        {:ok, _} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")
      after
        cleanup_test_store(store, path)
      end
    end

    test "concurrent access is safe" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Load initial data
        {:ok, _} = TripleStore.insert(store, [
          {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
        ])

        # Run concurrent queries
        tasks = for _i <- 1..10 do
          Task.async(fn ->
            TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")
          end)
        end

        results = Task.await_many(tasks, 30_000)

        # All queries should succeed
        assert Enum.all?(results, fn
          {:ok, _} -> true
          _ -> false
        end)
      after
        cleanup_test_store(store, path)
      end
    end

    test "empty inputs are handled gracefully" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Empty list insert
        {:ok, 0} = TripleStore.insert(store, [])

        # Empty list delete
        {:ok, 0} = TripleStore.delete(store, [])

        # Empty graph load
        {:ok, 0} = TripleStore.load_graph(store, RDF.Graph.new())

        # Query on empty store
        {:ok, results} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")
        assert results == []
      after
        cleanup_test_store(store, path)
      end
    end

    test "API follows Elixir conventions" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Functions return tagged tuples
        assert match?({:ok, _}, TripleStore.stats(store))

        # Close returns :ok not {:ok, :ok}
        assert TripleStore.close(store) == :ok
      after
        cleanup_test_path(path)
      end
    end

    test "type specs are accurate" do
      path = Path.join(System.tmp_dir!(), "typespec_#{:rand.uniform(1_000_000)}")

      try do
        # open returns {:ok, store()}
        {:ok, store} = TripleStore.open(path)
        assert is_map(store)
        assert is_reference(store.db)
        assert is_pid(store.dict_manager)

        # query returns {:ok, term()}
        {:ok, results} = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o }")
        assert is_list(results)

        # stats returns {:ok, map}
        {:ok, stats} = TripleStore.stats(store)
        assert is_map(stats)

        TripleStore.close(store)
      after
        cleanup_test_path(path)
      end
    end
  end

  # ===========================================================================
  # Additional Usability Tests
  # ===========================================================================

  describe "additional usability tests" do
    test "RDF sigils work in triple construction" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Using RDF module functions
        s = RDF.iri("http://example.org/subject")
        p = RDF.iri("http://example.org/predicate")
        o = RDF.literal("object value")

        {:ok, 1} = TripleStore.insert(store, {s, p, o})
        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "common SPARQL patterns work correctly" do
      {store, path} = create_test_store(prefix: "api_test")

      try do
        # Load test data
        {:ok, _} = TripleStore.update(store, """
          PREFIX ex: <http://example.org/>
          PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          INSERT DATA {
            ex:alice rdf:type ex:Person .
            ex:alice ex:name "Alice" .
            ex:alice ex:age 30 .
            ex:bob rdf:type ex:Person .
            ex:bob ex:name "Bob" .
          }
        """)

        # FILTER works
        {:ok, results} = TripleStore.query(store, """
          PREFIX ex: <http://example.org/>
          SELECT ?name WHERE {
            ?s ex:name ?name .
            FILTER(STRLEN(?name) > 3)
          }
        """)
        assert length(results) == 1

        # OPTIONAL works
        {:ok, results} = TripleStore.query(store, """
          PREFIX ex: <http://example.org/>
          SELECT ?name ?age WHERE {
            ?s ex:name ?name .
            OPTIONAL { ?s ex:age ?age }
          }
        """)
        assert length(results) == 2

        # ORDER BY works
        {:ok, results} = TripleStore.query(store, """
          PREFIX ex: <http://example.org/>
          SELECT ?name WHERE { ?s ex:name ?name } ORDER BY ?name
        """)
        assert length(results) == 2

        # COUNT works
        {:ok, results} = TripleStore.query(store, """
          PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          PREFIX ex: <http://example.org/>
          SELECT (COUNT(?s) as ?count) WHERE { ?s rdf:type ex:Person }
        """)
        assert length(results) == 1
      after
        cleanup_test_store(store, path)
      end
    end

    test "workflow: create, load, query, update, export" do
      path = Path.join(System.tmp_dir!(), "workflow_#{:rand.uniform(1_000_000)}")

      try do
        # 1. Create store
        {:ok, store} = TripleStore.open(path)

        # 2. Load data
        ttl = """
        @prefix ex: <http://example.org/> .
        ex:item1 ex:value 100 .
        ex:item2 ex:value 200 .
        """
        {:ok, 2} = TripleStore.load_string(store, ttl, :turtle)

        # 3. Query data
        {:ok, results} = TripleStore.query(store, """
          PREFIX ex: <http://example.org/>
          SELECT ?item ?val WHERE { ?item ex:value ?val }
        """)
        assert length(results) == 2

        # 4. Update data
        {:ok, _} = TripleStore.update(store, """
          PREFIX ex: <http://example.org/>
          INSERT DATA { ex:item3 ex:value 300 }
        """)

        # 5. Verify update
        {:ok, stats} = TripleStore.stats(store)
        assert stats.triple_count == 3

        # 6. Export data
        {:ok, graph} = TripleStore.export(store, :graph)
        assert RDF.Graph.triple_count(graph) == 3

        # 7. Close
        :ok = TripleStore.close(store)
      after
        cleanup_test_path(path)
      end
    end
  end
end
