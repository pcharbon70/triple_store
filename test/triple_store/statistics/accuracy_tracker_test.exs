defmodule TripleStore.Statistics.AccuracyTrackerTest do
  @moduledoc """
  Tests for the statistics accuracy tracker.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Statistics.AccuracyTracker

  @telemetry_event [:triple_store, :statistics, :estimate_accuracy]

  setup do
    # Start the tracker for each test
    start_supervised!(AccuracyTracker)

    # Reset tracking data
    AccuracyTracker.reset(%{})

    :ok
  end

  describe "track_estimate/3" do
    test "stores estimated cardinality for a pattern" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      assert :ok = AccuracyTracker.track_estimate(%{}, pattern, 100)
    end

    test "rejects negative estimates" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      assert {:error, :invalid_estimate} = AccuracyTracker.track_estimate(%{}, pattern, -1)
    end

    test "tracks multiple patterns independently" do
      pattern1 = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      pattern2 = {:quad, 1, 10, {:variable, "o"}, 0}

      assert :ok = AccuracyTracker.track_estimate(%{}, pattern1, 100)
      assert :ok = AccuracyTracker.track_estimate(%{}, pattern2, 50)
    end
  end

  describe "track_actual/3" do
    test "stores actual result and calculates error metrics" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      assert :ok = AccuracyTracker.track_actual(%{}, pattern, 90)
    end

    test "calculates correct relative error" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      # Estimate 100, actual 90 => 10% error
      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 90)

      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      assert stats[{:quad, :var, {:bound, 10}, :var, {:bound, 0}}].avg_relative_error == 10.0
    end

    test "handles zero actual values" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 0)

      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      assert stats[{:quad, :var, {:bound, 10}, :var, {:bound, 0}}].avg_relative_error == 100.0
    end

    test "handles perfect estimates" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 100)

      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      assert stats[{:quad, :var, {:bound, 10}, :var, {:bound, 0}}].avg_relative_error == 0.0
    end

    test "returns error when no estimate was tracked" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      assert {:error, :no_estimate} = AccuracyTracker.track_actual(%{}, pattern, 50)
    end

    test "emits telemetry event with accuracy metrics" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      # Attach telemetry handler
      attach_telemetry_handler()

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 80)

      assert_receive {:telemetry_event, @telemetry_event, measurements, metadata}
      assert measurements.relative_error == 20.0
      assert metadata.pattern_type == :quad
      assert metadata.estimated == 100
      assert metadata.actual == 80
      assert metadata.absolute_error == 20

      :telemetry.detach("test_handler")
    end
  end

  describe "get_accuracy_stats/1" do
    test "returns empty map when no tracking data" do
      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      assert stats == %{}
    end

    test "aggregates multiple samples for same pattern" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 90)  # 10% error

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 80)  # 20% error

      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      pattern_stats = stats[{:quad, :var, {:bound, 10}, :var, {:bound, 0}}]

      assert pattern_stats.samples == 2
      assert pattern_stats.avg_relative_error == 15.0
    end
  end

  describe "get_aggregated_stats/1" do
    test "returns zero stats when no data" do
      {:ok, stats} = AccuracyTracker.get_aggregated_stats(%{})
      assert stats.total_samples == 0
      assert stats.avg_relative_error == 0.0
      assert stats.max_relative_error == 0.0
    end

    test "aggregates stats across all patterns" do
      pattern1 = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      pattern2 = {:quad, 1, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern1, 100)
      AccuracyTracker.track_actual(%{}, pattern1, 90)  # 10% error

      AccuracyTracker.track_estimate(%{}, pattern2, 50)
      AccuracyTracker.track_actual(%{}, pattern2, 25)  # 50% error

      {:ok, stats} = AccuracyTracker.get_aggregated_stats(%{})

      assert stats.total_samples == 2
      assert stats.avg_relative_error == 30.0
      assert stats.max_relative_error == 50.0
    end
  end

  describe "reset/1" do
    test "clears all tracking data" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}

      AccuracyTracker.track_estimate(%{}, pattern, 100)
      AccuracyTracker.track_actual(%{}, pattern, 90)

      {:ok, _stats} = AccuracyTracker.get_accuracy_stats(%{})
      {:ok, agg_stats} = AccuracyTracker.get_aggregated_stats(%{})
      assert agg_stats.total_samples > 0

      AccuracyTracker.reset(%{})

      {:ok, stats} = AccuracyTracker.get_accuracy_stats(%{})
      assert stats == %{}

      {:ok, new_agg_stats} = AccuracyTracker.get_aggregated_stats(%{})
      assert new_agg_stats.total_samples == 0
    end
  end

  # Helper functions

  defp attach_telemetry_handler do
    :telemetry.attach(
      "test_handler",
      @telemetry_event,
      fn event_name, measurements, metadata, _config ->
        send(self(), {:telemetry_event, event_name, measurements, metadata})
      end,
      nil
    )
  end
end
