defmodule TripleStore.SPARQL.EnhancedExplainTest do
  @moduledoc """
  Tests for enhanced explain plan output (S3).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.Optimizer

  @sample_stats %{
    quad_count: 10_000,
    triple_count: 10_000,
    distinct_subjects: 500,
    distinct_predicates: 50,
    distinct_objects: 2000
  }

  describe "explain_detailed/2" do
    test "returns detailed explanation with cost breakdown" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert details.cost_breakdown.total_cost > 0
      assert details.cost_breakdown.most_expensive != nil
    end

    test "includes transformation tracking" do
      algebra =
        {:filter, {:binary_op, :>, {:variable, "x"}, {:literal, 5}},
         {:bgp, [{:triple, {:variable, "x"}, 10, {:variable, "o"}}]}}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert is_list(details.transformations)
    end

    test "includes recommended plan" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert details.recommended_plan.strategy != nil
      assert details.recommended_plan.steps != nil
      assert details.recommended_plan.estimated_cost != nil
    end

    test "formats output as string when format: :string" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, details} =
        Optimizer.explain_detailed(algebra,
          stats: @sample_stats,
          format: :string
        )

      assert is_binary(details)
      assert String.contains?(details, "Query Execution Plan")
      assert String.contains?(details, "Recommended Strategy")
    end

    test "returns map by default" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert is_map(details)
      refute is_binary(details)
    end
  end

  describe "cost breakdown" do
    test "calculates cost for simple BGP" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert details.cost_breakdown.total_cost == 100.0
    end

    test "calculates cost for filter operation" do
      algebra =
        {:filter, {:binary_op, :>, {:variable, "x"}, {:literal, 5}},
         {:bgp, [{:triple, {:variable, "x"}, 10, {:variable, "o"}}]}}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      # Filter cost is 10% of child cost
      assert details.cost_breakdown.total_cost > 0
    end

    test "calculates cost for join operation" do
      algebra =
        {:join, {:bgp, [{:triple, {:variable, "s"}, 10, {:variable, "o"}}]},
         {:bgp, [{:triple, {:variable, "s"}, 11, {:variable, "p"}}]}}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      # Cost includes both BGP scans plus join operation overhead
      assert details.cost_breakdown.total_cost > 200
    end
  end

  describe "transformation tracking" do
    test "detects filter push-down opportunity" do
      algebra =
        {:filter, {:binary_op, :>, {:variable, "x"}, {:literal, 5}},
         {:bgp, [{:triple, {:variable, "x"}, 10, {:variable, "o"}}]}}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert Enum.any?(details.transformations, fn t -> t.type == :filter_push_down end)
    end

    test "detects BGP reordering for multi-pattern BGPs" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}},
           {:triple, {:variable, "s"}, 11, {:variable, "p"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert Enum.any?(details.transformations, fn t -> t.type == :bgp_reordering end)
    end
  end

  describe "recommended plan" do
    test "recommends standard join for simple queries" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert details.recommended_plan.strategy.name == :standard_join
    end

    test "recommends leapfrog for complex queries" do
      # Create a BGP with 5+ patterns (threshold is > 4)
      patterns =
        for i <- 1..5 do
          {:triple, {:variable, "s"}, i + 10, {:variable, "o"}}
        end

      algebra = {:bgp, patterns}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert details.recommended_plan.strategy.name == :leapfrog_join
    end

    test "includes execution steps" do
      algebra =
        {:filter, {:binary_op, :>, {:variable, "x"}, {:literal, 5}},
         {:bgp, [{:triple, {:variable, "x"}, 10, {:variable, "o"}}]}}

      {:explain, details} = Optimizer.explain_detailed(algebra, stats: @sample_stats)

      assert is_list(details.recommended_plan.steps)
      refute Enum.empty?(details.recommended_plan.steps)
    end
  end

  describe "string format output" do
    test "includes all major sections" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 10, {:variable, "o"}}
         ]}

      {:explain, output} =
        Optimizer.explain_detailed(algebra,
          stats: @sample_stats,
          format: :string
        )

      assert String.contains?(output, "Query Execution Plan")
      assert String.contains?(output, "Recommended Strategy")
      assert String.contains?(output, "Total Estimated Cost")
      assert String.contains?(output, "Execution Steps")
    end
  end
end
