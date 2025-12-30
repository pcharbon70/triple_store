defmodule TripleStore.TelemetryTest do
  @moduledoc """
  Tests for the unified telemetry module.

  Verifies that all documented events are properly defined and
  that the telemetry API functions work correctly.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Telemetry

  describe "all_events/0" do
    test "returns a list of event names" do
      events = Telemetry.all_events()
      assert is_list(events)
      assert length(events) > 0

      # All events should be lists of atoms
      for event <- events do
        assert is_list(event)
        assert Enum.all?(event, &is_atom/1)
        # All events should start with :triple_store
        assert hd(event) == :triple_store
      end
    end

    test "includes query events" do
      events = Telemetry.all_events()

      assert [:triple_store, :query, :execute, :start] in events
      assert [:triple_store, :query, :execute, :stop] in events
      assert [:triple_store, :query, :execute, :exception] in events
      assert [:triple_store, :query, :parse, :start] in events
      assert [:triple_store, :query, :parse, :stop] in events
    end

    test "includes insert events" do
      events = Telemetry.all_events()

      assert [:triple_store, :insert, :start] in events
      assert [:triple_store, :insert, :stop] in events
      assert [:triple_store, :insert, :exception] in events
    end

    test "includes delete events" do
      events = Telemetry.all_events()

      assert [:triple_store, :delete, :start] in events
      assert [:triple_store, :delete, :stop] in events
      assert [:triple_store, :delete, :exception] in events
    end

    test "includes cache events" do
      events = Telemetry.all_events()

      # Plan cache
      assert [:triple_store, :cache, :plan, :hit] in events
      assert [:triple_store, :cache, :plan, :miss] in events

      # Query result cache
      assert [:triple_store, :cache, :query, :hit] in events
      assert [:triple_store, :cache, :query, :miss] in events
      assert [:triple_store, :cache, :query, :expired] in events
    end

    test "includes reasoner events" do
      events = Telemetry.all_events()

      assert [:triple_store, :reasoner, :materialize, :start] in events
      assert [:triple_store, :reasoner, :materialize, :stop] in events
      assert [:triple_store, :reasoner, :compile, :start] in events
      assert [:triple_store, :reasoner, :compile, :stop] in events
    end

    test "includes load events" do
      events = Telemetry.all_events()

      assert [:triple_store, :load, :start] in events
      assert [:triple_store, :load, :stop] in events
      assert [:triple_store, :load, :exception] in events
    end
  end

  describe "query_events/0" do
    test "returns parse and execute events" do
      events = Telemetry.query_events()

      assert [:triple_store, :query, :parse, :start] in events
      assert [:triple_store, :query, :parse, :stop] in events
      assert [:triple_store, :query, :parse, :exception] in events
      assert [:triple_store, :query, :execute, :start] in events
      assert [:triple_store, :query, :execute, :stop] in events
      assert [:triple_store, :query, :execute, :exception] in events
    end
  end

  describe "insert_events/0" do
    test "returns insert lifecycle events" do
      events = Telemetry.insert_events()

      assert [:triple_store, :insert, :start] in events
      assert [:triple_store, :insert, :stop] in events
      assert [:triple_store, :insert, :exception] in events
    end
  end

  describe "delete_events/0" do
    test "returns delete lifecycle events" do
      events = Telemetry.delete_events()

      assert [:triple_store, :delete, :start] in events
      assert [:triple_store, :delete, :stop] in events
      assert [:triple_store, :delete, :exception] in events
    end
  end

  describe "cache_events/0" do
    test "returns cache hit/miss events for all cache types" do
      events = Telemetry.cache_events()

      # Plan cache
      assert [:triple_store, :cache, :plan, :hit] in events
      assert [:triple_store, :cache, :plan, :miss] in events

      # Stats cache
      assert [:triple_store, :cache, :stats, :hit] in events
      assert [:triple_store, :cache, :stats, :miss] in events

      # Query result cache
      assert [:triple_store, :cache, :query, :hit] in events
      assert [:triple_store, :cache, :query, :miss] in events
      assert [:triple_store, :cache, :query, :expired] in events
      assert [:triple_store, :cache, :query, :persist] in events
      assert [:triple_store, :cache, :query, :warm] in events
    end
  end

  describe "load_events/0" do
    test "returns load lifecycle events" do
      events = Telemetry.load_events()

      assert [:triple_store, :load, :start] in events
      assert [:triple_store, :load, :stop] in events
      assert [:triple_store, :load, :exception] in events
      assert [:triple_store, :load, :batch, :complete] in events
    end
  end

  describe "reasoner_events/0" do
    test "returns all reasoner events" do
      events = Telemetry.reasoner_events()

      # Compilation
      assert [:triple_store, :reasoner, :compile, :start] in events
      assert [:triple_store, :reasoner, :compile, :stop] in events

      # Materialization
      assert [:triple_store, :reasoner, :materialize, :start] in events
      assert [:triple_store, :reasoner, :materialize, :stop] in events
      assert [:triple_store, :reasoner, :materialize, :iteration] in events

      # Deletion with reasoning
      assert [:triple_store, :reasoner, :delete, :start] in events
      assert [:triple_store, :reasoner, :delete, :stop] in events
    end
  end

  describe "span/4" do
    test "emits start and stop events" do
      test_pid = self()
      handler_id = "span-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [
          [:triple_store, :test, :operation, :start],
          [:triple_store, :test, :operation, :stop]
        ],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      result =
        Telemetry.span(:test, :operation, %{key: "value"}, fn ->
          Process.sleep(10)
          :ok
        end)

      :telemetry.detach(handler_id)

      assert result == :ok

      assert_receive {:telemetry, [:triple_store, :test, :operation, :start], measurements,
                      metadata}

      assert is_integer(measurements.system_time)
      assert is_integer(measurements.monotonic_time)
      assert metadata.key == "value"

      assert_receive {:telemetry, [:triple_store, :test, :operation, :stop], measurements,
                      metadata}

      assert is_integer(measurements.duration)
      assert measurements.duration > 0
      assert is_integer(metadata.duration_ms)
      assert metadata.key == "value"
    end

    test "emits exception event on error" do
      test_pid = self()
      handler_id = "span-exception-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :test, :failing, :exception],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      assert_raise RuntimeError, "test error", fn ->
        Telemetry.span(:test, :failing, %{}, fn ->
          raise "test error"
        end)
      end

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :test, :failing, :exception], measurements,
                      metadata}

      assert is_integer(measurements.duration)
      assert metadata.kind == :error
      # Exception telemetry is sanitized - no raw exception or stacktrace
      # Only type, message, and stacktrace depth are included for security
      assert metadata.exception_type == RuntimeError
      assert metadata.exception_message == "test error"
      assert is_integer(metadata.stacktrace_depth)
    end

    test "supports extra metadata in stop event" do
      test_pid = self()
      handler_id = "span-extra-metadata-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :test, :meta, :stop],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Telemetry.span(:test, :meta, %{initial: true}, fn ->
        {:result, %{extra: "data", count: 42}}
      end)

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :test, :meta, :stop], _measurements, metadata}
      assert metadata.initial == true
      assert metadata.extra == "data"
      assert metadata.count == 42
    end
  end

  describe "emit_cache_hit/2" do
    test "emits cache hit event" do
      test_pid = self()
      handler_id = "cache-hit-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :test_cache, :hit],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Telemetry.emit_cache_hit(:test_cache, %{key: "test_key"})

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :cache, :test_cache, :hit], %{count: 1},
                      metadata}

      assert metadata.key == "test_key"
    end
  end

  describe "emit_cache_miss/2" do
    test "emits cache miss event" do
      test_pid = self()
      handler_id = "cache-miss-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :test_cache, :miss],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Telemetry.emit_cache_miss(:test_cache, %{key: "test_key"})

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :cache, :test_cache, :miss], %{count: 1},
                      metadata}

      assert metadata.key == "test_key"
    end
  end

  describe "emit_start/2" do
    test "emits start event with timing" do
      test_pid = self()
      handler_id = "start-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :custom, :op, :start],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Telemetry.emit_start([:triple_store, :custom, :op], %{custom: "data"})

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :custom, :op, :start], measurements, metadata}
      assert is_integer(measurements.system_time)
      assert is_integer(measurements.monotonic_time)
      assert metadata.custom == "data"
    end
  end

  describe "emit_stop/3" do
    test "emits stop event with duration" do
      test_pid = self()
      handler_id = "stop-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :custom, :op, :stop],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      duration = System.convert_time_unit(100, :millisecond, :native)
      Telemetry.emit_stop([:triple_store, :custom, :op], duration, %{result: "ok"})

      :telemetry.detach(handler_id)

      assert_receive {:telemetry, [:triple_store, :custom, :op, :stop], measurements, metadata}
      assert measurements.duration == duration
      assert metadata.result == "ok"
      assert is_integer(metadata.duration_ms)
    end
  end

  describe "attach_handler/3" do
    test "attaches handler to all events" do
      handler_id = "attach-test-#{:erlang.unique_integer([:positive])}"

      assert :ok = Telemetry.attach_handler(handler_id, fn _, _, _, _ -> :ok end)

      # Verify handler is attached (detach returns :ok if it exists)
      assert :ok = :telemetry.detach(handler_id)
    end

    test "returns error for duplicate handler" do
      handler_id = "duplicate-test-#{:erlang.unique_integer([:positive])}"

      assert :ok = Telemetry.attach_handler(handler_id, fn _, _, _, _ -> :ok end)

      assert {:error, :already_exists} =
               Telemetry.attach_handler(handler_id, fn _, _, _, _ -> :ok end)

      :telemetry.detach(handler_id)
    end
  end

  describe "detach_handler/1" do
    test "detaches existing handler" do
      handler_id = "detach-test-#{:erlang.unique_integer([:positive])}"

      Telemetry.attach_handler(handler_id, fn _, _, _, _ -> :ok end)
      assert :ok = Telemetry.detach_handler(handler_id)
    end

    test "returns error for non-existent handler" do
      assert {:error, :not_found} = Telemetry.detach_handler("non-existent-handler")
    end
  end

  describe "utility functions" do
    test "prefix/0 returns event prefix" do
      assert Telemetry.prefix() == [:triple_store]
    end

    test "to_milliseconds/1 converts native time units" do
      native = System.convert_time_unit(1000, :millisecond, :native)
      # Allow for some rounding
      assert abs(Telemetry.to_milliseconds(native) - 1000) < 1
    end

    test "event_path/2 builds full event path" do
      assert Telemetry.event_path(:query, :execute) == [:triple_store, :query, :execute]
      assert Telemetry.event_path(:cache, :hit) == [:triple_store, :cache, :hit]
    end
  end
end
