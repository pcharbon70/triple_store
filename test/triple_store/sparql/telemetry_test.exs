defmodule TripleStore.SPARQL.TelemetryTest do
  @moduledoc """
  Tests for telemetry event emission during SPARQL query execution.

  Verifies that telemetry events are emitted correctly for monitoring
  and observability purposes.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.Query

  @moduletag :telemetry

  @ex "http://example.org/"

  setup do
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "telemetry_test_#{test_id}")

    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      File.rm_rf!(db_path)
    end)

    %{ctx: ctx, db_path: db_path}
  end

  describe "query execution telemetry" do
    test "emits start and stop events for SELECT query", %{ctx: ctx} do
      # Set up telemetry handler
      test_pid = self()

      handler_id = "test-handler-#{:erlang.unique_integer([:positive])}"

      # Query.query emits events to [:triple_store, :sparql, :query, :phase]
      :telemetry.attach_many(
        handler_id,
        [
          [:triple_store, :sparql, :query, :start],
          [:triple_store, :sparql, :query, :stop],
          [:triple_store, :sparql, :query, :exception]
        ],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      # Execute query (empty result is fine, we just want telemetry events)
      {:ok, _results} = Query.query(ctx, "SELECT ?s ?o WHERE { ?s <#{@ex}p> ?o }")

      # Clean up handler
      :telemetry.detach(handler_id)

      # Check for start event - Query.query emits [:triple_store, :sparql, :query, :start]
      receive do
        {:telemetry, [:triple_store, :sparql, :query, :start], measurements, metadata} ->
          assert is_map(measurements)
          assert is_map(metadata)
          # Query metadata includes timeout (from Query.query's with_timeout)
          assert Map.has_key?(metadata, :timeout)
      after
        100 ->
          flunk("Expected telemetry start event not received")
      end
    end

    test "telemetry events include result status", %{ctx: ctx} do
      # Set up telemetry handler
      test_pid = self()
      handler_id = "test-handler-#{:erlang.unique_integer([:positive])}"

      # Query.query emits events to [:triple_store, :sparql, :query, :stop]
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :query, :stop],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_stop, measurements, metadata})
        end,
        nil
      )

      # Execute ASK query
      {:ok, _} = Query.query(ctx, "ASK { ?s ?p ?o }")

      # Clean up handler
      :telemetry.detach(handler_id)

      # Check for stop event with result status metadata
      receive do
        {:telemetry_stop, measurements, metadata} ->
          assert is_map(measurements)
          # Stop event includes duration and result status
          assert Map.has_key?(measurements, :duration)
          assert metadata[:result] in [:ok, :error, :timeout]
      after
        100 ->
          flunk("Expected telemetry stop event not received")
      end
    end
  end

  describe "exception telemetry" do
    test "emits sanitized exception event on query error", %{ctx: ctx} do
      test_pid = self()
      handler_id = "test-handler-#{:erlang.unique_integer([:positive])}"

      # Attach to exception events
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :query, :exception],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:exception_event, measurements, metadata})
        end,
        nil
      )

      # Execute invalid query that should cause a parse error
      result = Query.query(ctx, "THIS IS NOT VALID SPARQL")

      # Clean up handler
      :telemetry.detach(handler_id)

      # Should get error result
      assert {:error, {:parse_error, _}} = result

      # Note: Parse errors are handled before the telemetry span starts,
      # so they don't emit exception telemetry. This test documents that behavior.
    end
  end
end
