defmodule TripleStore.Reasoner.TelemetryTest do
  # async: false because telemetry handlers are global
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{SemiNaive, Rules, Telemetry}

  # ============================================================================
  # Setup
  # ============================================================================

  setup do
    # Clean up any previous handlers
    test_id = "telemetry-test-#{System.unique_integer([:positive])}"
    {:ok, test_id: test_id}
  end

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp iri(local), do: {:iri, "http://example.org/#{local}"}
  defp rdf_type, do: {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
  defp rdfs_subClassOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"}

  defp attach_handler(test_id, events, pid) do
    :telemetry.attach_many(
      test_id,
      events,
      fn event, measurements, metadata, _ ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )
  end

  defp detach_handler(test_id) do
    :telemetry.detach(test_id)
  end

  # ============================================================================
  # Tests: event_names/0
  # ============================================================================

  describe "event_names/0" do
    test "returns all materialize events" do
      events = Telemetry.event_names()

      assert [:triple_store, :reasoner, :materialize, :start] in events
      assert [:triple_store, :reasoner, :materialize, :stop] in events
      assert [:triple_store, :reasoner, :materialize, :iteration] in events
    end

    test "returns all compile events" do
      events = Telemetry.event_names()

      assert [:triple_store, :reasoner, :compile, :start] in events
      assert [:triple_store, :reasoner, :compile, :stop] in events
      assert [:triple_store, :reasoner, :compile, :exception] in events
    end

    test "returns total of 17 events" do
      # compile: start, stop, exception, complete = 4
      # optimize: start, stop, complete = 3
      # extract_schema: start, stop, complete = 3
      # materialize: start, stop, iteration = 3
      # delete: start, stop = 2
      # backward_trace: complete = 1
      # forward_rederive: complete = 1
      # Total = 17
      assert length(Telemetry.event_names()) == 17
    end
  end

  # ============================================================================
  # Tests: Materialization Telemetry
  # ============================================================================

  describe "materialize telemetry" do
    test "emits start event with correct metadata", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [[:triple_store, :reasoner, :materialize, :start]], pid)

      on_exit(fn -> detach_handler(test_id) end)

      entities = for i <- 1..3, do: {iri("entity#{i}"), rdf_type(), iri("Thing")}
      hierarchy = [{iri("Thing"), rdfs_subClassOf(), iri("Entity")}]
      initial = MapSet.new(entities ++ hierarchy)
      rules = [Rules.cax_sco()]

      SemiNaive.materialize_in_memory(rules, initial, emit_telemetry: true)

      assert_receive {:telemetry_event, event, measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :materialize, :start]
      assert is_integer(measurements.system_time)
      assert metadata.rule_count == 1
      assert metadata.initial_fact_count == 4
      assert metadata.parallel == false
    end

    test "emits stop event with correct stats", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [[:triple_store, :reasoner, :materialize, :stop]], pid)

      on_exit(fn -> detach_handler(test_id) end)

      entities = for i <- 1..3, do: {iri("entity#{i}"), rdf_type(), iri("Thing")}
      hierarchy = [{iri("Thing"), rdfs_subClassOf(), iri("Entity")}]
      initial = MapSet.new(entities ++ hierarchy)
      rules = [Rules.cax_sco()]

      SemiNaive.materialize_in_memory(rules, initial, emit_telemetry: true)

      assert_receive {:telemetry_event, event, measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :materialize, :stop]
      assert is_integer(measurements.duration)
      assert metadata.iterations == 2
      assert metadata.total_derived == 3
      assert is_integer(metadata.duration_ms)
      assert metadata.rules_applied == 2
    end

    test "emits iteration events", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [[:triple_store, :reasoner, :materialize, :iteration]], pid)

      on_exit(fn -> detach_handler(test_id) end)

      entities = for i <- 1..3, do: {iri("entity#{i}"), rdf_type(), iri("Thing")}
      hierarchy = [{iri("Thing"), rdfs_subClassOf(), iri("Entity")}]
      initial = MapSet.new(entities ++ hierarchy)
      rules = [Rules.cax_sco()]

      SemiNaive.materialize_in_memory(rules, initial, emit_telemetry: true)

      # Should receive at least one iteration event
      assert_receive {:telemetry_event, event, measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :materialize, :iteration]
      assert measurements.derivations == 3
      assert metadata.iteration == 1
    end

    test "does not emit events when emit_telemetry: false", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [
        [:triple_store, :reasoner, :materialize, :start],
        [:triple_store, :reasoner, :materialize, :stop],
        [:triple_store, :reasoner, :materialize, :iteration]
      ], pid)

      on_exit(fn -> detach_handler(test_id) end)

      entities = for i <- 1..3, do: {iri("entity#{i}"), rdf_type(), iri("Thing")}
      hierarchy = [{iri("Thing"), rdfs_subClassOf(), iri("Entity")}]
      initial = MapSet.new(entities ++ hierarchy)
      rules = [Rules.cax_sco()]

      SemiNaive.materialize_in_memory(rules, initial, emit_telemetry: false)

      # Wait a bit to make sure no events are emitted
      refute_receive {:telemetry_event, _, _, _}, 100
    end

    test "emits stop event with error on max_iterations_exceeded", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [[:triple_store, :reasoner, :materialize, :stop]], pid)

      on_exit(fn -> detach_handler(test_id) end)

      # Create a scenario that exceeds max_iterations
      chain = for i <- 1..10, do: {iri("n#{i}"), rdfs_subClassOf(), iri("n#{i + 1}")}
      initial = MapSet.new(chain)
      rules = [Rules.scm_sco()]

      # Set max_iterations very low
      result = SemiNaive.materialize_in_memory(rules, initial, max_iterations: 1, emit_telemetry: true)
      assert {:error, :max_iterations_exceeded} = result

      assert_receive {:telemetry_event, event, _measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :materialize, :stop]
      assert metadata.error == :max_iterations_exceeded
    end
  end

  # ============================================================================
  # Tests: emit_iteration/2
  # ============================================================================

  describe "emit_iteration/2" do
    test "emits iteration event with correct format", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [[:triple_store, :reasoner, :materialize, :iteration]], pid)

      on_exit(fn -> detach_handler(test_id) end)

      Telemetry.emit_iteration(42, 5)

      assert_receive {:telemetry_event, event, measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :materialize, :iteration]
      assert measurements.derivations == 42
      assert metadata.iteration == 5
    end
  end

  # ============================================================================
  # Tests: span/3
  # ============================================================================

  describe "span/3" do
    test "emits start and stop events", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [
        [:triple_store, :reasoner, :test_event, :start],
        [:triple_store, :reasoner, :test_event, :stop]
      ], pid)

      on_exit(fn -> detach_handler(test_id) end)

      result = Telemetry.span(:test_event, %{profile: :owl2rl}, fn ->
        %{result: :success, count: 10}
      end)

      assert result == %{result: :success, count: 10}

      assert_receive {:telemetry_event, start_event, _start_m, start_meta}, 1000
      assert start_event == [:triple_store, :reasoner, :test_event, :start]
      assert start_meta.profile == :owl2rl

      assert_receive {:telemetry_event, stop_event, stop_m, stop_meta}, 1000
      assert stop_event == [:triple_store, :reasoner, :test_event, :stop]
      assert is_integer(stop_m.duration)
      assert stop_meta.profile == :owl2rl
      assert stop_meta.result == :success
      assert stop_meta.count == 10
    end

    test "emits exception event on error", %{test_id: test_id} do
      pid = self()

      attach_handler(test_id, [
        [:triple_store, :reasoner, :test_event, :exception]
      ], pid)

      on_exit(fn -> detach_handler(test_id) end)

      assert_raise RuntimeError, "test error", fn ->
        Telemetry.span(:test_event, %{profile: :test}, fn ->
          raise "test error"
        end)
      end

      assert_receive {:telemetry_event, event, measurements, metadata}, 1000
      assert event == [:triple_store, :reasoner, :test_event, :exception]
      assert is_integer(measurements.duration)
      assert metadata.kind == :error
      assert %RuntimeError{} = metadata.reason
    end
  end
end
