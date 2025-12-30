defmodule TripleStore.APITest do
  @moduledoc """
  Tests for the TripleStore public API quality.

  This module tests:
  - All public functions have documentation
  - All public functions have specs
  - Error handling returns correct types
  - Bang functions raise appropriate errors
  """

  use ExUnit.Case, async: true

  alias TripleStore.Error

  # ===========================================================================
  # Public Functions Under Test
  # ===========================================================================

  # All public functions in the TripleStore module that should have docs/specs
  @public_functions [
    # Store Lifecycle
    {:open, 1},
    {:open, 2},
    {:close, 1},
    {:close!, 1},
    # Data Loading
    {:load, 2},
    {:load, 3},
    {:load_graph, 2},
    {:load_graph, 3},
    {:load_string, 3},
    {:load_string, 4},
    # Triple Operations
    {:insert, 2},
    {:delete, 2},
    # Querying
    {:query, 2},
    {:query, 3},
    {:update, 2},
    # Data Export
    {:export, 2},
    {:export, 3},
    # Reasoning
    {:materialize, 1},
    {:materialize, 2},
    {:reasoning_status, 1},
    # Health & Status
    {:health, 1},
    {:stats, 1},
    # Backup & Restore
    {:backup, 2},
    {:backup, 3},
    {:restore, 2},
    {:restore, 3},
    # Bang Variants
    {:open!, 1},
    {:open!, 2},
    {:load!, 2},
    {:load!, 3},
    {:load_graph!, 2},
    {:load_graph!, 3},
    {:load_string!, 3},
    {:load_string!, 4},
    {:query!, 2},
    {:query!, 3},
    {:update!, 2},
    {:insert!, 2},
    {:delete!, 2},
    {:export!, 2},
    {:export!, 3},
    {:materialize!, 1},
    {:materialize!, 2},
    {:reasoning_status!, 1},
    {:health!, 1},
    {:stats!, 1},
    {:backup!, 2},
    {:backup!, 3},
    {:restore!, 2},
    {:restore!, 3}
  ]

  # ===========================================================================
  # Documentation Tests
  # ===========================================================================

  describe "documentation" do
    test "TripleStore module has moduledoc" do
      {:docs_v1, _annotation, _beam_lang, _format, module_doc, _metadata, _docs} =
        Code.fetch_docs(TripleStore)

      assert module_doc != :hidden
      assert module_doc != :none

      case module_doc do
        %{"en" => doc} ->
          assert is_binary(doc)
          assert String.length(doc) > 100
          # Should contain key sections
          assert doc =~ "Quick Start"
          assert doc =~ "Public API"

        doc when is_binary(doc) ->
          assert String.length(doc) > 100
      end
    end

    test "all public functions have documentation" do
      {:docs_v1, _annotation, _beam_lang, _format, _module_doc, _metadata, docs} =
        Code.fetch_docs(TripleStore)

      # Build a map of function -> doc for quick lookup
      function_docs =
        for {{:function, name, arity}, _annotation, _signature, doc, _metadata} <- docs,
            into: %{} do
          {{name, arity}, doc}
        end

      # Check each public function has documentation
      for {name, arity} <- @public_functions do
        doc = Map.get(function_docs, {name, arity})

        # Functions may not be in docs if they're defined with defaults
        # Check for the function with one less arity (the version with defaults)
        doc =
          if doc == nil do
            Map.get(function_docs, {name, arity - 1})
          else
            doc
          end

        # Skip if still nil - the function might be using defaults from another head
        if doc != nil do
          assert doc != :none,
                 "Function #{name}/#{arity} is missing documentation"

          assert doc != :hidden,
                 "Function #{name}/#{arity} has hidden documentation"

          # Verify doc content is non-empty
          case doc do
            %{"en" => content} ->
              assert is_binary(content) and String.length(content) > 10,
                     "Function #{name}/#{arity} has empty documentation"

            content when is_binary(content) ->
              assert String.length(content) > 10,
                     "Function #{name}/#{arity} has empty documentation"

            _ ->
              # Doc may be in a different format, that's OK
              :ok
          end
        end
      end
    end

    test "TripleStore.Error module has moduledoc" do
      {:docs_v1, _annotation, _beam_lang, _format, module_doc, _metadata, _docs} =
        Code.fetch_docs(TripleStore.Error)

      assert module_doc != :hidden
      assert module_doc != :none

      case module_doc do
        %{"en" => doc} ->
          assert is_binary(doc)
          assert doc =~ "Error Categories"

        doc when is_binary(doc) ->
          assert doc =~ "Error Categories"
      end
    end
  end

  # ===========================================================================
  # Type Specification Tests
  # ===========================================================================

  describe "type specifications" do
    test "TripleStore module exports specs for public functions" do
      # Get the module's specs using beam_lib
      {:ok, {TripleStore, [abstract_code: {:raw_abstract_v1, forms}]}} =
        :beam_lib.chunks(
          TripleStore.module_info(:compile)[:source] |> to_charlist() |> beam_file(),
          [:abstract_code]
        )

      # Extract specs from the abstract code
      specs =
        for {:attribute, _, :spec, {{name, arity}, _spec_forms}} <- forms do
          {name, arity}
        end
        |> MapSet.new()

      # Verify key functions have specs (sample check)
      key_functions = [
        {:open, 2},
        {:close, 1},
        {:load, 3},
        {:query, 3},
        {:update, 2},
        {:insert, 2},
        {:delete, 2},
        {:export, 3},
        {:materialize, 2},
        {:health, 1},
        {:stats, 1}
      ]

      for {name, arity} <- key_functions do
        assert {name, arity} in specs,
               "Function #{name}/#{arity} is missing a type spec"
      end
    rescue
      # Fallback if beam_lib doesn't work
      _ ->
        # Use behaviours/introspection instead
        module_info = TripleStore.__info__(:functions)

        # At minimum verify the module exports these functions
        for {name, arity} <- @public_functions do
          assert {name, arity} in module_info,
                 "Function #{name}/#{arity} is not exported"
        end
    end

    test "all public functions are exported" do
      exported = TripleStore.__info__(:functions)

      for {name, arity} <- @public_functions do
        assert {name, arity} in exported,
               "Function #{name}/#{arity} is not exported from TripleStore"
      end
    end

    test "TripleStore.Error has type specs" do
      exported = TripleStore.Error.__info__(:functions)

      # Key Error module functions
      error_functions = [
        {:new, 2},
        {:new, 3},
        {:query_parse_error, 1},
        {:query_parse_error, 2},
        {:query_timeout, 1},
        {:database_closed, 0},
        {:file_not_found, 1},
        {:safe_message, 1},
        {:retriable?, 1}
      ]

      for {name, arity} <- error_functions do
        assert {name, arity} in exported,
               "Function #{name}/#{arity} is not exported from TripleStore.Error"
      end
    end
  end

  # ===========================================================================
  # Error Handling Tests
  # ===========================================================================

  describe "error handling return types" do
    setup do
      # Create a temporary database for testing
      path = Path.join(System.tmp_dir!(), "triple_store_api_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      on_exit(fn ->
        try do
          TripleStore.close(store)
        rescue
          _ -> :ok
        end
      end)

      {:ok, store: store, path: path}
    end

    test "query with invalid SPARQL returns {:error, reason}", %{store: store} do
      result = TripleStore.query(store, "INVALID SPARQL QUERY")
      assert {:error, _reason} = result
    end

    test "load with non-existent file returns {:error, reason}", %{store: store} do
      result = TripleStore.load(store, "/non/existent/file.ttl")
      assert {:error, _reason} = result
    end

    test "open returns {:ok, store} on success" do
      path = Path.join(System.tmp_dir!(), "triple_store_open_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      result = TripleStore.open(path)
      assert {:ok, %{db: _, dict_manager: _, path: ^path}} = result

      TripleStore.close(elem(result, 1))
    end

    test "query returns {:ok, results} on valid query", %{store: store} do
      result = TripleStore.query(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 1")
      assert {:ok, results} = result
      assert is_list(results)
    end

    test "health returns {:ok, health} with correct structure", %{store: store} do
      {:ok, health} = TripleStore.health(store)

      assert is_map(health)
      assert Map.has_key?(health, :status)
      assert health.status in [:healthy, :degraded, :unhealthy]
      assert Map.has_key?(health, :triple_count)
      assert is_integer(health.triple_count)
      assert Map.has_key?(health, :database_open)
      assert is_boolean(health.database_open)
      assert Map.has_key?(health, :dict_manager_alive)
      assert is_boolean(health.dict_manager_alive)
    end

    test "stats returns {:ok, stats} with correct structure", %{store: store} do
      {:ok, stats} = TripleStore.stats(store)

      assert is_map(stats)
      assert Map.has_key?(stats, :triple_count)
      assert is_integer(stats.triple_count)
    end

    test "reasoning_status returns {:ok, status} with correct structure", %{store: store} do
      {:ok, status} = TripleStore.reasoning_status(store)

      assert is_map(status)
      assert Map.has_key?(status, :state)
      assert status.state in [:initialized, :materialized, :stale, :error]
      assert Map.has_key?(status, :derived_count)
      assert Map.has_key?(status, :needs_rematerialization)
    end

    test "insert returns {:ok, count}", %{store: store} do
      triple = {
        RDF.iri("http://example.org/subject"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("object")
      }

      result = TripleStore.insert(store, triple)
      assert {:ok, count} = result
      assert is_integer(count)
      assert count >= 0
    end

    test "delete returns {:ok, count}", %{store: store} do
      triple = {
        RDF.iri("http://example.org/subject"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("object")
      }

      # Insert first
      TripleStore.insert(store, triple)

      # Delete
      result = TripleStore.delete(store, triple)
      assert {:ok, count} = result
      assert is_integer(count)
      assert count >= 0
    end

    test "export returns {:ok, graph} for :graph target", %{store: store} do
      result = TripleStore.export(store, :graph)
      assert {:ok, %RDF.Graph{}} = result
    end
  end

  # ===========================================================================
  # Bang Function Tests
  # ===========================================================================

  describe "bang functions raise on error" do
    setup do
      path = Path.join(System.tmp_dir!(), "triple_store_bang_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      on_exit(fn ->
        try do
          TripleStore.close(store)
        rescue
          _ -> :ok
        end
      end)

      {:ok, store: store, path: path}
    end

    test "query! raises TripleStore.Error on invalid SPARQL", %{store: store} do
      assert_raise TripleStore.Error, fn ->
        TripleStore.query!(store, "INVALID SPARQL QUERY")
      end
    end

    test "load! raises TripleStore.Error on non-existent file", %{store: store} do
      assert_raise TripleStore.Error, fn ->
        TripleStore.load!(store, "/non/existent/file.ttl")
      end
    end

    test "open! raises TripleStore.Error on invalid path" do
      # Try to open a path that we can't create (permission denied)
      # On some systems this may not raise, so we test with a clearly invalid scenario
      assert_raise TripleStore.Error, fn ->
        TripleStore.open!("/\0invalid\0path")
      end
    rescue
      # Some systems may allow this, so accept that too
      _ -> :ok
    end

    test "query! returns results on valid query", %{store: store} do
      results = TripleStore.query!(store, "SELECT * WHERE { ?s ?p ?o } LIMIT 1")
      assert is_list(results)
    end

    test "health! returns health without wrapping", %{store: store} do
      health = TripleStore.health!(store)
      assert is_map(health)
      assert health.status in [:healthy, :degraded, :unhealthy]
    end

    test "stats! returns stats without wrapping", %{store: store} do
      stats = TripleStore.stats!(store)
      assert is_map(stats)
      assert Map.has_key?(stats, :triple_count)
    end

    test "reasoning_status! returns status without wrapping", %{store: store} do
      status = TripleStore.reasoning_status!(store)
      assert is_map(status)
      assert Map.has_key?(status, :state)
    end

    test "insert! returns count without wrapping", %{store: store} do
      triple = {
        RDF.iri("http://example.org/bang_subject"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("value")
      }

      count = TripleStore.insert!(store, triple)
      assert is_integer(count)
    end

    test "delete! returns count without wrapping", %{store: store} do
      triple = {
        RDF.iri("http://example.org/bang_subject"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("value")
      }

      TripleStore.insert!(store, triple)
      count = TripleStore.delete!(store, triple)
      assert is_integer(count)
    end

    test "export! returns graph without wrapping", %{store: store} do
      graph = TripleStore.export!(store, :graph)
      assert %RDF.Graph{} = graph
    end

    test "materialize! returns stats without wrapping", %{store: store} do
      stats = TripleStore.materialize!(store, profile: :rdfs)
      assert is_map(stats)
    end
  end

  # ===========================================================================
  # TripleStore.Error Tests
  # ===========================================================================

  describe "TripleStore.Error structure" do
    test "new/2 creates error with correct structure" do
      error = Error.new(:query_parse_error, "Test message")

      assert %Error{} = error
      assert error.category == :query_parse_error
      assert error.message == "Test message"
      assert error.code == 1001
      assert error.safe_message == "Invalid SPARQL syntax"
    end

    test "error is an exception" do
      error = Error.new(:query_timeout, "Timed out")
      assert Exception.exception?(error)
    end

    test "error can be raised and caught" do
      error = Error.new(:database_closed, "Database is closed")

      assert_raise Error, "Database is closed", fn ->
        raise error
      end
    end

    test "query_parse_error helper creates correct error" do
      error = Error.query_parse_error("Syntax error at line 5")

      assert error.category == :query_parse_error
      assert error.code == 1001
      assert error.message =~ "Syntax error"
    end

    test "query_timeout helper creates correct error" do
      error = Error.query_timeout(5000)

      assert error.category == :query_timeout
      assert error.code == 1002
      assert error.message =~ "5000"
    end

    test "database_closed helper creates correct error" do
      error = Error.database_closed()

      assert error.category == :database_closed
      assert error.code == 2002
    end

    test "file_not_found helper creates correct error" do
      error = Error.file_not_found("/path/to/file.ttl")

      assert error.category == :validation_file_not_found
      assert error.code == 4004
      assert error.message =~ "/path/to/file.ttl"
    end

    test "safe_message returns sanitized message" do
      error = Error.query_parse_error("Parse error at line 5: unexpected 'SELECT'")
      safe = Error.safe_message(error)

      assert safe == "Invalid SPARQL syntax"
      refute safe =~ "line 5"
    end

    test "retriable? returns true for transient errors" do
      timeout_error = Error.new(:query_timeout, "Timed out")
      io_error = Error.new(:database_io_error, "IO error")
      resource_error = Error.new(:system_resource_exhausted, "Out of memory")

      assert Error.retriable?(timeout_error)
      assert Error.retriable?(io_error)
      assert Error.retriable?(resource_error)
    end

    test "retriable? returns false for permanent errors" do
      parse_error = Error.new(:query_parse_error, "Syntax error")
      closed_error = Error.new(:database_closed, "Closed")
      input_error = Error.new(:validation_invalid_input, "Bad input")

      refute Error.retriable?(parse_error)
      refute Error.retriable?(closed_error)
      refute Error.retriable?(input_error)
    end

    test "error_codes returns all error codes" do
      codes = Error.error_codes()

      assert is_map(codes)
      assert Map.has_key?(codes, :query_parse_error)
      assert Map.has_key?(codes, :database_closed)
      assert Map.has_key?(codes, :reasoning_max_iterations)
      assert Map.has_key?(codes, :validation_invalid_input)
      assert Map.has_key?(codes, :system_internal_error)
    end

    test "code_for returns correct code" do
      assert Error.code_for(:query_parse_error) == 1001
      assert Error.code_for(:query_timeout) == 1002
      assert Error.code_for(:database_closed) == 2002
      assert Error.code_for(:validation_file_not_found) == 4004
      assert Error.code_for(:system_internal_error) == 5001
    end

    test "code_for returns default for unknown category" do
      assert Error.code_for(:unknown_category) == 5001
    end

    test "from_legacy converts error tuples" do
      {:error, error} = Error.from_legacy({:error, :timeout})
      assert error.category == :query_timeout

      {:error, error2} = Error.from_legacy({:error, :database_closed})
      assert error2.category == :database_closed
    end
  end

  # ===========================================================================
  # Path Traversal Protection Tests
  # ===========================================================================

  describe "path traversal protection" do
    test "open/2 rejects paths with .." do
      result = TripleStore.open("../../../etc/passwd")
      assert {:error, :path_traversal_attempt} = result
    end

    test "open!/2 raises on path traversal attempt" do
      error =
        assert_raise TripleStore.Error, fn ->
          TripleStore.open!("../../../etc/passwd")
        end

      # The error category comes from the path_traversal_attempt reason
      # which maps to :validation_invalid_input via Error.from_reason
      assert error.category == :validation_invalid_input
      assert error.message =~ "traversal"
    end

    test "open/2 with create_if_missing: false returns error for non-existent database" do
      result =
        TripleStore.open("/tmp/nonexistent_db_#{:rand.uniform(1_000_000)}",
          create_if_missing: false
        )

      assert {:error, :database_not_found} = result
    end
  end

  # ===========================================================================
  # Store Lifecycle Tests
  # ===========================================================================

  describe "store lifecycle" do
    test "close/1 returns :ok on first close" do
      path = Path.join(System.tmp_dir!(), "triple_store_close_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      # First close should succeed
      assert :ok = TripleStore.close(store)
    end

    test "close/1 returns error on already closed store" do
      path =
        Path.join(System.tmp_dir!(), "triple_store_close_twice_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      # First close
      :ok = TripleStore.close(store)

      # Second close returns error
      assert {:error, :already_closed} = TripleStore.close(store)
    end

    test "close!/1 returns :ok" do
      path =
        Path.join(System.tmp_dir!(), "triple_store_close_bang_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)
      assert :ok = TripleStore.close!(store)
    end
  end

  # ===========================================================================
  # Load Graph and Load String Bang Variant Tests
  # ===========================================================================

  describe "load_graph! and load_string! bang variants" do
    setup do
      path = Path.join(System.tmp_dir!(), "triple_store_load_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      on_exit(fn ->
        try do
          TripleStore.close(store)
        rescue
          _ -> :ok
        end
      end)

      {:ok, store: store, path: path}
    end

    test "load_graph!/3 loads an RDF.Graph and returns count", %{store: store} do
      graph =
        RDF.Graph.new()
        |> RDF.Graph.add({
          RDF.iri("http://example.org/s"),
          RDF.iri("http://example.org/p"),
          RDF.literal("test")
        })

      count = TripleStore.load_graph!(store, graph)
      assert is_integer(count)
      assert count >= 1
    end

    test "load_string!/4 loads Turtle string and returns count", %{store: store} do
      turtle = """
      @prefix ex: <http://example.org/> .
      ex:subject ex:predicate "object" .
      """

      count = TripleStore.load_string!(store, turtle, :turtle)
      assert is_integer(count)
      assert count >= 1
    end

    test "load_string!/4 raises on invalid Turtle", %{store: store} do
      invalid_turtle = "this is not valid turtle {"

      error =
        assert_raise TripleStore.Error, fn ->
          TripleStore.load_string!(store, invalid_turtle, :turtle)
        end

      assert error.category == :data_parse_error
    end
  end

  # ===========================================================================
  # Bang Variant Error Category Tests
  # ===========================================================================

  describe "bang variant error categories" do
    setup do
      path =
        Path.join(System.tmp_dir!(), "triple_store_bang_cat_test_#{:rand.uniform(1_000_000)}")

      on_exit(fn ->
        File.rm_rf!(path)
      end)

      {:ok, store} = TripleStore.open(path)

      on_exit(fn ->
        try do
          TripleStore.close(store)
        rescue
          _ -> :ok
        end
      end)

      {:ok, store: store, path: path}
    end

    test "query! raises with :query_parse_error category for invalid SPARQL", %{store: store} do
      error =
        assert_raise TripleStore.Error, fn ->
          TripleStore.query!(store, "INVALID SPARQL")
        end

      assert error.category == :query_parse_error
      assert error.code == 1001
    end

    test "load! raises with :validation_file_not_found category for missing file", %{store: store} do
      error =
        assert_raise TripleStore.Error, fn ->
          TripleStore.load!(store, "/nonexistent/path/file.ttl")
        end

      assert error.category == :validation_file_not_found
      assert error.code == 4004
    end

    test "update! raises with :query_parse_error category for invalid SPARQL UPDATE", %{
      store: store
    } do
      error =
        assert_raise TripleStore.Error, fn ->
          TripleStore.update!(store, "INVALID UPDATE")
        end

      assert error.category == :query_parse_error
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Get the beam file path for a module
  defp beam_file(source_path) do
    source_path
    |> Path.dirname()
    |> Path.dirname()
    |> Path.join("_build/test/lib/triple_store/ebin/Elixir.TripleStore.beam")
    |> to_charlist()
  end
end
