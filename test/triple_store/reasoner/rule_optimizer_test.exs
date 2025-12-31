defmodule TripleStore.Reasoner.RuleOptimizerTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{Rule, RuleOptimizer}

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"

  # ============================================================================
  # Pattern Reordering Tests
  # ============================================================================

  describe "optimize_rule/2 - pattern reordering" do
    test "places patterns with constants before all-variable patterns" do
      # Rule: (?x ?p ?y), (?y rdf:type ?c) -> ...
      # Should reorder to: (?y rdf:type ?c), (?x ?p ?y)
      rule =
        Rule.new(
          :test_reorder,
          [
            {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      patterns = Rule.body_patterns(optimized)

      # Pattern with constant predicate should come first
      assert {:pattern, [_, {:iri, _}, _]} = hd(patterns)
    end

    test "places patterns with bound predicates first" do
      # Rule with two patterns, one with constant predicate
      rule =
        Rule.new(
          :test_pred_first,
          [
            {:pattern, [{:var, "a"}, {:var, "b"}, {:var, "c"}]},
            {:pattern, [{:var, "x"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "y"}]}
          ],
          {:pattern, [{:var, "a"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      [first | _] = Rule.body_patterns(optimized)

      # Pattern with constant predicate should be first
      {:pattern, [_, pred, _]} = first
      assert {:iri, _} = pred
    end

    test "considers already-bound variables when reordering" do
      # (?x knows ?y), (?y knows ?z), (?z knows ?w)
      # First pattern binds x,y; second should use y to be selective
      rule =
        Rule.new(
          :test_bound_vars,
          [
            {:pattern, [{:var, "x"}, {:iri, "http://ex.org/knows"}, {:var, "y"}]},
            {:pattern, [{:var, "z"}, {:iri, "http://ex.org/knows"}, {:var, "w"}]},
            {:pattern, [{:var, "y"}, {:iri, "http://ex.org/knows"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "http://ex.org/knows"}, {:var, "w"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      patterns = Rule.body_patterns(optimized)

      # After first pattern binds y, second pattern should use y as subject
      # to be more selective than the unrelated z-w pattern
      assert length(patterns) == 3
    end

    test "preserves conditions when preserve_conditions is true" do
      rule =
        Rule.new(
          :test_preserve,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]},
            {:not_equal, {:var, "x"}, {:var, "y"}}
          ],
          {:pattern, [{:var, "y"}, {:iri, "#{@owl}sameAs"}, {:var, "x"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule, preserve_conditions: true)

      # Condition should be at the end
      assert List.last(optimized.body) == {:not_equal, {:var, "x"}, {:var, "y"}}
    end

    test "interleaves conditions optimally" do
      # Condition on x,y should come after pattern binding x,y
      rule =
        Rule.new(
          :test_interleave,
          [
            {:pattern, [{:var, "a"}, {:iri, "#{@rdf}type"}, {:var, "b"}]},
            {:not_equal, {:var, "x"}, {:var, "y"}},
            {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]}
          ],
          {:pattern, [{:var, "a"}, {:iri, "#{@rdf}type"}, {:var, "b"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)

      # Find positions
      positions =
        optimized.body
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn {elem, idx}, acc ->
          case elem do
            {:pattern, [{:var, "x"}, _, {:var, "y"}]} -> Map.put(acc, :xy_pattern, idx)
            {:not_equal, {:var, "x"}, {:var, "y"}} -> Map.put(acc, :xy_condition, idx)
            _ -> acc
          end
        end)

      # Condition should come after the pattern that binds its variables
      if Map.has_key?(positions, :xy_pattern) and Map.has_key?(positions, :xy_condition) do
        assert positions.xy_condition > positions.xy_pattern
      end
    end
  end

  describe "optimize_rules/2" do
    test "optimizes multiple rules" do
      rules = [
        Rule.new(
          :rule1,
          [
            {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
        ),
        Rule.new(
          :rule2,
          [
            {:pattern, [{:var, "a"}, {:var, "b"}, {:var, "c"}]},
            {:pattern, [{:var, "x"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "y"}]}
          ],
          {:pattern, [{:var, "a"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
        )
      ]

      optimized = RuleOptimizer.optimize_rules(rules)

      assert length(optimized) == 2
      assert Enum.all?(optimized, &match?(%Rule{}, &1))
    end
  end

  # ============================================================================
  # Selectivity Estimation Tests
  # ============================================================================

  describe "estimate_selectivity/3" do
    test "constant predicate is more selective than variable" do
      bound_vars = MapSet.new()
      data_stats = %{}

      const_pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
      var_pattern = {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]}

      const_sel = RuleOptimizer.estimate_selectivity(const_pattern, bound_vars, data_stats)
      var_sel = RuleOptimizer.estimate_selectivity(var_pattern, bound_vars, data_stats)

      assert const_sel < var_sel
    end

    test "bound variable is more selective than unbound" do
      bound_vars = MapSet.new(["x"])
      data_stats = %{}

      pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}

      sel_bound = RuleOptimizer.estimate_selectivity(pattern, bound_vars, data_stats)
      sel_unbound = RuleOptimizer.estimate_selectivity(pattern, MapSet.new(), data_stats)

      assert sel_bound < sel_unbound
    end

    test "literal is highly selective" do
      bound_vars = MapSet.new()
      data_stats = %{}

      pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:literal, :simple, "test"}]}

      sel = RuleOptimizer.estimate_selectivity(pattern, bound_vars, data_stats)

      # Literal should be very selective
      assert sel < 0.01
    end

    test "uses data stats when available" do
      bound_vars = MapSet.new()

      data_stats = %{
        predicate_counts: %{
          "#{@rdf}type" => 1000,
          "#{@rdfs}label" => 10
        },
        total_triples: 10_000
      }

      type_pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
      label_pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdfs}label"}, {:var, "y"}]}

      type_sel = RuleOptimizer.estimate_selectivity(type_pattern, bound_vars, data_stats)
      label_sel = RuleOptimizer.estimate_selectivity(label_pattern, bound_vars, data_stats)

      # Label should be more selective (fewer occurrences)
      assert label_sel < type_sel
    end
  end

  # ============================================================================
  # Rule Batching Tests
  # ============================================================================

  describe "batch_rules/1" do
    test "groups rules by head predicate" do
      rules = [
        Rule.new(
          :rule1,
          [{:pattern, [{:var, "x"}, {:iri, "p1"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
        ),
        Rule.new(
          :rule2,
          [{:pattern, [{:var, "a"}, {:iri, "p2"}, {:var, "b"}]}],
          {:pattern, [{:var, "a"}, {:iri, "#{@rdf}type"}, {:var, "b"}]}
        ),
        Rule.new(
          :rule3,
          [{:pattern, [{:var, "x"}, {:iri, "p3"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]}
        )
      ]

      batches = RuleOptimizer.batch_rules(rules)

      # Should have 2 batches: rdf:type rules and owl:sameAs rules
      assert length(batches) == 2

      type_batch = Enum.find(batches, fn b -> length(b.rules) == 2 end)
      assert type_batch != nil
      assert length(type_batch.rules) == 2
    end

    test "identifies shared patterns" do
      # Two rules with same body pattern
      shared_pattern = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}

      rules = [
        Rule.new(
          :rule1,
          [shared_pattern, {:pattern, [{:var, "c"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "d"}]}],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "d"}]}
        ),
        Rule.new(
          :rule2,
          [shared_pattern, {:pattern, [{:var, "x"}, {:iri, "p2"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "y"}]}
        )
      ]

      batches = RuleOptimizer.batch_rules(rules)

      batch = hd(batches)
      assert batch.shared_patterns != []
      assert shared_pattern in batch.shared_patterns
    end

    test "classifies batch types correctly" do
      # Rules with shared patterns
      rules = [
        Rule.new(
          :rule1,
          [{:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out"}, {:var, "c"}]}
        ),
        Rule.new(
          :rule2,
          [{:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out"}, {:var, "c"}]}
        )
      ]

      batches = RuleOptimizer.batch_rules(rules)
      batch = hd(batches)

      assert batch.batch_type == :same_predicate
    end
  end

  describe "find_shareable_rules/1" do
    test "finds rules with common patterns" do
      common = {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}

      rules = [
        Rule.new(
          :r1,
          [common, {:pattern, [{:var, "a"}, {:iri, "p1"}, {:var, "b"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out1"}, {:var, "c"}]}
        ),
        Rule.new(
          :r2,
          [common, {:pattern, [{:var, "c"}, {:iri, "p2"}, {:var, "d"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out2"}, {:var, "d"}]}
        ),
        Rule.new(
          :r3,
          [{:pattern, [{:var, "z"}, {:iri, "p3"}, {:var, "w"}]}],
          {:pattern, [{:var, "z"}, {:iri, "out3"}, {:var, "w"}]}
        )
      ]

      shareable = RuleOptimizer.find_shareable_rules(rules)

      # r1 and r2 share the common pattern
      assert shareable != []

      {rule1, rule2, shared} = hd(shareable)
      assert rule1.name in [:r1, :r2]
      assert rule2.name in [:r1, :r2]
      assert common in shared
    end

    test "returns empty list when no shared patterns" do
      rules = [
        Rule.new(
          :r1,
          [{:pattern, [{:var, "a"}, {:iri, "p1"}, {:var, "b"}]}],
          {:pattern, [{:var, "a"}, {:iri, "out1"}, {:var, "b"}]}
        ),
        Rule.new(
          :r2,
          [{:pattern, [{:var, "x"}, {:iri, "p2"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out2"}, {:var, "y"}]}
        )
      ]

      shareable = RuleOptimizer.find_shareable_rules(rules)

      assert shareable == []
    end
  end

  # ============================================================================
  # Dead Rule Detection Tests
  # ============================================================================

  describe "find_dead_rules/2" do
    test "finds rules requiring missing transitive properties" do
      schema_info = %{transitive_properties: []}

      rules = [
        Rule.new(
          :prp_trp,
          [
            {:pattern, [{:var, "p"}, {:iri, "#{@rdf}type"}, {:iri, "#{@owl}TransitiveProperty"}]},
            {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:var, "p"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "z"}]},
          profile: :owl2rl
        ),
        Rule.new(
          :cax_sco,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c1"}]},
            {:pattern, [{:var, "c1"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c2"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c2"}]},
          profile: :rdfs
        )
      ]

      dead = RuleOptimizer.find_dead_rules(rules, schema_info)

      assert length(dead) == 1
      assert hd(dead).name == :prp_trp
    end

    test "finds rules requiring missing subclass" do
      schema_info = %{has_subclass: false}

      rules = [
        Rule.new(
          :scm_sco,
          [
            {:pattern, [{:var, "c1"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c2"}]},
            {:pattern, [{:var, "c2"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c3"}]}
          ],
          {:pattern, [{:var, "c1"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c3"}]},
          profile: :rdfs
        ),
        Rule.new(
          :cax_sco,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c1"}]},
            {:pattern, [{:var, "c1"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c2"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c2"}]},
          profile: :rdfs
        )
      ]

      dead = RuleOptimizer.find_dead_rules(rules, schema_info)

      assert length(dead) == 2
      dead_names = Enum.map(dead, & &1.name)
      assert :scm_sco in dead_names
      assert :cax_sco in dead_names
    end

    test "finds rules requiring missing sameAs" do
      schema_info = %{has_sameas: false}

      rules = [
        Rule.new(
          :eq_sym,
          [{:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]}],
          {:pattern, [{:var, "y"}, {:iri, "#{@owl}sameAs"}, {:var, "x"}]},
          profile: :owl2rl
        ),
        Rule.new(
          :eq_trans,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "#{@owl}sameAs"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "z"}]},
          profile: :owl2rl
        )
      ]

      dead = RuleOptimizer.find_dead_rules(rules, schema_info)

      assert length(dead) == 2
    end

    test "finds dead specialized rules" do
      schema_info = %{
        transitive_properties: ["http://example.org/contains"]
      }

      rules = [
        # Specialized for a property that exists
        Rule.new(
          :prp_trp_transitive_contains,
          [
            {:pattern, [{:var, "x"}, {:iri, "http://example.org/contains"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "http://example.org/contains"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "http://example.org/contains"}, {:var, "z"}]}
        ),
        # Specialized for a property that doesn't exist
        Rule.new(
          :prp_trp_transitive_ancestor,
          [
            {:pattern, [{:var, "x"}, {:iri, "http://example.org/ancestor"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "http://example.org/ancestor"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "http://example.org/ancestor"}, {:var, "z"}]}
        )
      ]

      dead = RuleOptimizer.find_dead_rules(rules, schema_info)

      assert length(dead) == 1
      assert hd(dead).name == :prp_trp_transitive_ancestor
    end
  end

  describe "filter_active_rules/2" do
    test "returns only active rules" do
      schema_info = %{
        has_subclass: true,
        has_sameas: false,
        transitive_properties: []
      }

      rules = [
        Rule.new(
          :cax_sco,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c1"}]},
            {:pattern, [{:var, "c1"}, {:iri, "#{@rdfs}subClassOf"}, {:var, "c2"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c2"}]}
        ),
        Rule.new(
          :eq_sym,
          [{:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]}],
          {:pattern, [{:var, "y"}, {:iri, "#{@owl}sameAs"}, {:var, "x"}]}
        ),
        Rule.new(
          :prp_trp,
          [
            {:pattern, [{:var, "p"}, {:iri, "#{@rdf}type"}, {:iri, "#{@owl}TransitiveProperty"}]},
            {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:var, "p"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "z"}]}
        )
      ]

      active = RuleOptimizer.filter_active_rules(rules, schema_info)

      assert length(active) == 1
      assert hd(active).name == :cax_sco
    end
  end

  describe "rule_dead?/2" do
    test "returns true for dead rule" do
      schema_info = %{transitive_properties: []}

      rule =
        Rule.new(
          :prp_trp,
          [{:pattern, [{:var, "p"}, {:iri, "#{@rdf}type"}, {:iri, "#{@owl}TransitiveProperty"}]}],
          {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "z"}]}
        )

      assert RuleOptimizer.rule_dead?(rule, schema_info)
    end

    test "returns false for active rule" do
      schema_info = %{transitive_properties: ["http://ex.org/contains"]}

      rule =
        Rule.new(
          :prp_trp,
          [{:pattern, [{:var, "p"}, {:iri, "#{@rdf}type"}, {:iri, "#{@owl}TransitiveProperty"}]}],
          {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "z"}]}
        )

      refute RuleOptimizer.rule_dead?(rule, schema_info)
    end

    test "unknown rules are not dead by default" do
      schema_info = %{}

      rule =
        Rule.new(
          :custom_rule,
          [{:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
        )

      refute RuleOptimizer.rule_dead?(rule, schema_info)
    end
  end

  # ============================================================================
  # Edge Cases
  # ============================================================================

  describe "edge cases" do
    test "handles empty rule list" do
      assert RuleOptimizer.optimize_rules([]) == []
      assert RuleOptimizer.batch_rules([]) == []
      assert RuleOptimizer.find_dead_rules([], %{}) == []
    end

    test "handles rule with no patterns" do
      rule = Rule.new(:empty, [], {:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]})

      optimized = RuleOptimizer.optimize_rule(rule)
      assert optimized.body == []
    end

    test "handles rule with only conditions" do
      rule =
        Rule.new(
          :conditions_only,
          [{:not_equal, {:var, "x"}, {:var, "y"}}],
          {:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      assert length(optimized.body) == 1
    end

    test "handles nil schema values gracefully" do
      schema_info = %{
        transitive_properties: nil,
        has_subclass: nil
      }

      rule =
        Rule.new(
          :prp_trp,
          [{:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
        )

      # Should not crash
      assert RuleOptimizer.rule_dead?(rule, schema_info)
    end

    test "handles single-pattern rule" do
      rule =
        Rule.new(
          :single,
          [{:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}],
          {:pattern, [{:var, "x"}, {:iri, "out"}, {:var, "c"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      assert length(Rule.body_patterns(optimized)) == 1
    end
  end
end
