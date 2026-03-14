defmodule TripleStore.SPARQL.CostTelemetryTest do
  @moduledoc """
  Tests for cost model telemetry (S5).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.CostTelemetry

  setup do
    # Ensure handlers are detached before each test
    CostTelemetry.detach_handler()
    CostTelemetry.detach_handler(:test_handler)

    on_exit(fn ->
      CostTelemetry.detach_handler()
      CostTelemetry.detach_handler(:test_handler)
    end)

    :ok
  end

  describe "handler management" do
    test "attaches and detaches handlers" do
      assert :ok = CostTelemetry.attach_handler()
      assert :ok = CostTelemetry.detach_handler()
    end

    test "attaches handler with custom id" do
      assert :ok = CostTelemetry.attach_handler(:test_handler)
      assert :ok = CostTelemetry.detach_handler(:test_handler)
    end
  end

  describe "query telemetry" do
    test "records query start event" do
      query = "SELECT * WHERE { ?s a ?type }"
      algebra = {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "type"}}]}

      # Attach a handler to capture the event
      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_query_start,
          [:triple_store, :sparql, :query, :start],
          fn _event, measurements, metadata, _config ->
            if Map.has_key?(metadata, :algebra_type) and metadata.query == query do
              send(test_pid, {:query_start, measurements, metadata})
            end
          end,
          nil
        )

      start_time = CostTelemetry.query_start(query, algebra, estimated_cost: 100.0)

      assert_receive {:query_start, measurements, metadata}, 100
      assert Map.has_key?(measurements, :system_time)
      assert metadata.query == query
      assert metadata.estimated_cost == 100.0
      assert metadata.algebra_type == :bgp
      refute metadata.has_filter
      assert metadata.pattern_count == 1

      :telemetry.detach(handler_id)
    end

    test "records query stop event" do
      algebra = {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_query_stop,
          [:triple_store, :sparql, :query, :stop],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:query_stop, measurements, metadata})
          end,
          nil
        )

      start_time = System.monotonic_time()
      Process.sleep(10)
      CostTelemetry.query_stop(start_time, 42, strategy: :leapfrog)

      assert_receive {:query_stop, measurements, metadata}, 100
      assert measurements.duration > 0
      assert measurements.result_count == 42
      assert metadata.strategy == :leapfrog

      :telemetry.detach(handler_id)
    end

    test "detects filter in algebra" do
      query = "SELECT * WHERE { ?s a ?type FILTER(?s > 0) }"

      algebra =
        {:filter, {:binary_op, :>, {:variable, "s"}, {:literal, 0}},
         {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "type"}}]}}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_filter,
          [:triple_store, :sparql, :query, :start],
          fn _event, _measurements, metadata, _config ->
            if Map.has_key?(metadata, :has_filter) and metadata.query == query do
              send(test_pid, {:filter_detected, metadata})
            end
          end,
          nil
        )

      CostTelemetry.query_start(query, algebra)

      assert_receive {:filter_detected, metadata}, 100
      assert metadata.has_filter == true

      :telemetry.detach(handler_id)
    end
  end

  describe "cost telemetry" do
    test "records cost estimate" do
      algebra = {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_cost_estimate,
          [:triple_store, :sparql, :cost, :estimate],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:cost_estimate, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.cost_estimate(algebra, 100.0, strategy: :standard_join)

      assert_receive {:cost_estimate, measurements, metadata}, 100
      assert measurements.estimated_cost == 100.0
      assert metadata.strategy == :standard_join
      assert metadata.algebra_type == :bgp

      :telemetry.detach(handler_id)
    end

    test "records actual cost with error ratio" do
      algebra =
        {:join, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]},
         {:bgp, [{:triple, {:variable, "s"}, 2, {:variable, "p"}}]}}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_cost_actual,
          [:triple_store, :sparql, :cost, :actual],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:cost_actual, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.cost_actual(algebra, 200.0, 100.0, strategy: :leapfrog)

      assert_receive {:cost_actual, measurements, metadata}, 100
      assert measurements.actual_cost == 200.0
      assert metadata.estimated_cost == 100.0
      assert metadata.error_ratio == 1.0
      assert metadata.strategy == :leapfrog

      :telemetry.detach(handler_id)
    end

    test "handles zero estimated cost" do
      algebra = {:bgp, []}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_zero_cost,
          [:triple_store, :sparql, :cost, :actual],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:cost_actual, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.cost_actual(algebra, 50.0, 0, [])

      assert_receive {:cost_actual, measurements, metadata}, 100
      assert measurements.actual_cost == 50.0
      assert metadata.error_ratio == 0.0

      :telemetry.detach(handler_id)
    end
  end

  describe "cardinality telemetry" do
    test "records cardinality estimate" do
      pattern = {:triple, {:variable, "s"}, 1, {:variable, "o"}}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_card_estimate,
          [:triple_store, :sparql, :cardinality, :estimate],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:card_estimate, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.cardinality_estimate(pattern, 100.0, graph: 0)

      assert_receive {:card_estimate, measurements, metadata}, 100
      assert measurements.estimated == 100.0
      assert metadata.pattern_type == :triple
      assert metadata.bound_terms == 1
      assert metadata.graph == 0

      :telemetry.detach(handler_id)
    end

    test "records actual cardinality" do
      pattern = {:triple, {:variable, "s"}, 1, {:variable, "o"}}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_card_actual,
          [:triple_store, :sparql, :cardinality, :actual],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:card_actual, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.cardinality_actual(pattern, 150, 100.0, graph: 0)

      assert_receive {:card_actual, measurements, metadata}, 100
      assert measurements.actual == 150
      assert metadata.estimated == 100.0
      assert metadata.error_ratio == 0.5

      :telemetry.detach(handler_id)
    end

    test "handles quad pattern" do
      pattern = {:quad, {:variable, "s"}, 1, {:variable, "o"}, 0}

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_quad_pattern,
          [:triple_store, :sparql, :cardinality, :estimate],
          fn _event, _measurements, metadata, _config ->
            send(test_pid, {:pattern_type, metadata})
          end,
          nil
        )

      CostTelemetry.cardinality_estimate(pattern, 100.0)

      assert_receive {:pattern_type, metadata}, 100
      assert metadata.pattern_type == :quad
      assert metadata.bound_terms == 2

      :telemetry.detach(handler_id)
    end
  end

  describe "join telemetry" do
    test "records join start" do
      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_join_start,
          [:triple_store, :sparql, :join, :start],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:join_start, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.join_start(100, 50, :leapfrog)

      assert_receive {:join_start, measurements, metadata}, 100
      assert measurements.left_size == 100
      assert measurements.right_size == 50
      assert metadata.algorithm == :leapfrog
      assert is_integer(metadata.start_time)

      :telemetry.detach(handler_id)
    end

    test "records join stop with duration" do
      start_time = System.monotonic_time()
      Process.sleep(10)

      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_join_stop,
          [:triple_store, :sparql, :join, :stop],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:join_stop, measurements, metadata})
          end,
          nil
        )

      CostTelemetry.join_stop(start_time, 75, :leapfrog)

      assert_receive {:join_stop, measurements, metadata}, 100
      assert measurements.duration > 0
      assert measurements.result_size == 75
      assert metadata.algorithm == :leapfrog

      :telemetry.detach(handler_id)
    end
  end

  describe "accuracy summary" do
    test "returns summary structure" do
      summary = CostTelemetry.get_accuracy_summary()

      assert Map.has_key?(summary, :total_queries)
      assert Map.has_key?(summary, :avg_error_ratio)
      assert Map.has_key?(summary, :median_error_ratio)
      assert Map.has_key?(summary, :max_error_ratio)
      assert Map.has_key?(summary, :estimates_within_2x)
      assert Map.has_key?(summary, :estimates_within_10x)
    end
  end

  describe "algebra detection" do
    test "correctly identifies algebra types" do
      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_algebra_types,
          [:triple_store, :sparql, :query, :start],
          fn _event, _measurements, metadata, _config ->
            case metadata do
              %{algebra_type: algebra_type} -> send(test_pid, {:algebra_type, algebra_type})
              _ -> :ok
            end
          end,
          nil
        )

      CostTelemetry.query_start("test", {:bgp, []})
      assert_receive {:algebra_type, :bgp}, 100

      CostTelemetry.query_start("test", {:join, {:bgp, []}, {:bgp, []}})
      assert_receive {:algebra_type, :join}, 100

      CostTelemetry.query_start("test", {:filter, {:literal, true}, {:bgp, []}})
      assert_receive {:algebra_type, :filter}, 100

      CostTelemetry.query_start("test", {:union, {:bgp, []}, {:bgp, []}})
      assert_receive {:algebra_type, :union}, 100

      :telemetry.detach(handler_id)
    end

    test "counts patterns correctly" do
      test_pid = self()

      handler_id =
        :telemetry.attach(
          :test_pattern_count,
          [:triple_store, :sparql, :query, :start],
          fn _event, _measurements, metadata, _config ->
            case metadata do
              %{pattern_count: pattern_count} -> send(test_pid, {:pattern_count, pattern_count})
              _ -> :ok
            end
          end,
          nil
        )

      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 1, {:variable, "o"}},
           {:triple, {:variable, "s"}, 2, {:variable, "p"}}
         ]}

      CostTelemetry.query_start("test", algebra)
      assert_receive {:pattern_count, 2}, 100

      :telemetry.detach(handler_id)
    end
  end
end
