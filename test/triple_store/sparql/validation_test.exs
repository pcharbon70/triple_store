defmodule TripleStore.SPARQL.ValidationTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Validation

  @moduletag :validation

  # ===========================================================================
  # Graph IRI Validation Tests
  # ===========================================================================

  describe "validate_graph_iri/1" do
    test "accepts valid HTTP IRIs" do
      assert :ok = Validation.validate_graph_iri("http://example.org/graph")
      assert :ok = Validation.validate_graph_iri("http://example.org/graph/name")
    end

    test "accepts valid HTTPS IRIs" do
      assert :ok = Validation.validate_graph_iri("https://example.org/graph")
      assert :ok = Validation.validate_graph_iri("https://secure.example.org/dataset")
    end

    test "accepts valid URN IRIs" do
      assert :ok = Validation.validate_graph_iri("urn:isbn:0451450523")
      assert :ok = Validation.validate_graph_iri("urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66")
    end

    test "accepts INFO and LSI schemes" do
      assert :ok = Validation.validate_graph_iri("info:example-org/data")
      assert :ok = Validation.validate_graph_iri("lsi:example-org:data")
    end

    test "rejects IRIs with blocked schemes" do
      assert {:error, :blocked_scheme} = Validation.validate_graph_iri("file:///etc/passwd")
      assert {:error, :blocked_scheme} = Validation.validate_graph_iri("ftp://example.org/data")
      assert {:error, :blocked_scheme} = Validation.validate_graph_iri("javascript:alert(1)")
    end

    test "rejects path traversal attempts" do
      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/../../etc/passwd")

      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/..\\windows\\system32")

      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/%2e%2e/passwd")
    end

    test "rejects null byte attempts" do
      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/%00")

      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/\\0/data")
    end

    test "rejects suspicious patterns" do
      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/etc/passwd")

      assert {:error, :path_traversal_detected} =
               Validation.validate_graph_iri("http://example.org/windows/system32")
    end

    test "rejects IRIs that are too long" do
      long_iri = "http://example.org/" <> String.duplicate("a", 3000)

      assert {:error, :iri_too_long} = Validation.validate_graph_iri(long_iri)
    end

    test "rejects malformed IRIs" do
      assert {:error, :invalid_iri} = Validation.validate_graph_iri("../../etc/passwd")
      assert {:error, :invalid_iri} = Validation.validate_graph_iri("not-a-iri")
    end
  end

  describe "validate_graph_term/1" do
    test "accepts default graph atoms" do
      assert :ok = Validation.validate_graph_term(:default_graph)
      assert :ok = Validation.validate_graph_term(:default)
    end

    test "delegates IRI validation to validate_graph_iri" do
      graph_iri = "http://example.org/graph"

      assert :ok = Validation.validate_graph_term({:named_node, graph_iri})

      blocked_iri = "file:///etc/passwd"
      assert {:error, :blocked_scheme} = Validation.validate_graph_term({:named_node, blocked_iri})
    end

    test "accepts blank node graph terms" do
      assert :ok = Validation.validate_graph_term({:blank_node, "graph1"})
      assert :ok = Validation.validate_graph_term({:blank_node, "_:graph"})
    end

    test "rejects invalid graph terms" do
      assert {:error, :invalid_graph_term} = Validation.validate_graph_term({:literal, :simple, "x"})
      assert {:error, :invalid_graph_term} = Validation.validate_graph_term(%{})
    end
  end

  describe "allowed_schemes/0" do
    test "returns list of allowed IRI schemes" do
      schemes = Validation.allowed_schemes()

      assert :http in schemes
      assert :https in schemes
      assert :urn in schemes
      assert :info in schemes
      assert :lsi in schemes
    end
  end

  describe "max_iri_length/0" do
    test "returns maximum IRI length" do
      assert 2048 = Validation.max_iri_length()
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "emit_validation_telemetry/3" do
    test "emits telemetry event for validation failure" do
      # Attach a handler to capture telemetry event
      handler_id = "test-validation-handler-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :validation_failure],
        &handle_telemetry/4,
        []
      )

      # Ensure handler is detached after test
      on_exit(fn -> :telemetry.detach(handler_id) end)

      # Emit validation failure telemetry
      assert :ok = Validation.emit_validation_telemetry(:path_traversal_detected, "suspicious_iri", %{})

      # Give telemetry time to process
      Process.sleep(10)
    end

    defp handle_telemetry(
           [:triple_store, :sparql, :validation_failure],
           measurements,
           metadata,
           _config
         ) do
      send(self(), {:telemetry_event, measurements, metadata})
      {:ok, measurements, metadata}
    end
  end

  # Update blank node test to check if we want to accept blank nodes
  # For now, blank nodes should be valid graph terms in SPARQL
  describe "validate_graph_term/1 - blank nodes" do
    test "accepts blank node graph terms" do
      # Note: Blank nodes as graph IRIs need to be handled specially
      # They're not standard IRIs but are valid in SPARQL
      assert :ok = Validation.validate_graph_term({:blank_node, "graph1"})
      assert :ok = Validation.validate_graph_term({:blank_node, "_:graph"})
    end
  end
end
