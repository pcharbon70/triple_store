defmodule TripleStore.Reasoner.SemiNaiveTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.SemiNaive
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp owl_sameAs, do: {:iri, @owl <> "sameAs"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}

  # ============================================================================
  # Tests: Basic Materialization
  # ============================================================================

  describe "materialize_in_memory/3" do
    test "materializes simple class hierarchy" do
      # Person -> Animal -> LivingThing
      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Animal")},
        {iri("Animal"), rdfs_subClassOf(), iri("LivingThing")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should derive alice rdf:type Animal and alice rdf:type LivingThing
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Animal")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("LivingThing")})

      assert stats.iterations > 0
      assert stats.total_derived >= 2
    end

    test "reaches fixpoint and terminates" do
      # Simple hierarchy that reaches fixpoint quickly
      initial = MapSet.new([
        {iri("bob"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should complete in finite iterations
      assert stats.iterations <= 10
      assert stats.duration_ms >= 0
    end

    test "handles empty initial facts" do
      initial = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      assert MapSet.size(all_facts) == 0
      assert stats.iterations == 0
      assert stats.total_derived == 0
    end

    test "handles empty rules" do
      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      rules = []

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # No derivations without rules, but still processes initial delta
      assert MapSet.size(all_facts) == 1
      assert stats.total_derived == 0
    end

    test "returns derivations_per_iteration statistics" do
      initial = MapSet.new([
        {iri("a"), rdfs_subClassOf(), iri("b")},
        {iri("b"), rdfs_subClassOf(), iri("c")},
        {iri("c"), rdfs_subClassOf(), iri("d")}
      ])

      rules = [Rules.scm_sco()]

      {:ok, _all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should have derivation counts for each iteration
      assert is_list(stats.derivations_per_iteration)
      assert Enum.sum(stats.derivations_per_iteration) == stats.total_derived
    end
  end

  # ============================================================================
  # Tests: Transitive Closure
  # ============================================================================

  describe "transitive closure" do
    test "computes full transitive closure of subClassOf" do
      # Chain: A -> B -> C -> D -> E
      initial = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")},
        {iri("C"), rdfs_subClassOf(), iri("D")},
        {iri("D"), rdfs_subClassOf(), iri("E")}
      ])

      rules = [Rules.scm_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should derive all transitive relationships
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("C")})
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("D")})
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("E")})
      assert MapSet.member?(all_facts, {iri("B"), rdfs_subClassOf(), iri("D")})
      assert MapSet.member?(all_facts, {iri("B"), rdfs_subClassOf(), iri("E")})
      assert MapSet.member?(all_facts, {iri("C"), rdfs_subClassOf(), iri("E")})
    end

    test "computes transitive property closure" do
      prop = iri("contains")

      initial = MapSet.new([
        {prop, rdf_type(), owl_TransitiveProperty()},
        {iri("box1"), prop, iri("box2")},
        {iri("box2"), prop, iri("box3")},
        {iri("box3"), prop, iri("box4")}
      ])

      rules = [Rules.prp_trp()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should derive transitive containment
      assert MapSet.member?(all_facts, {iri("box1"), prop, iri("box3")})
      assert MapSet.member?(all_facts, {iri("box1"), prop, iri("box4")})
      assert MapSet.member?(all_facts, {iri("box2"), prop, iri("box4")})
    end
  end

  # ============================================================================
  # Tests: Multiple Rules
  # ============================================================================

  describe "multiple rule interaction" do
    test "applies multiple rules together" do
      initial = MapSet.new([
        # Type hierarchy
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")},
        # Class hierarchy
        {iri("Agent"), rdfs_subClassOf(), iri("Entity")}
      ])

      # Use both scm_sco (subclass transitivity) and cax_sco (type through subclass)
      rules = [Rules.scm_sco(), Rules.cax_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # scm_sco should derive transitive subclass relationships
      assert MapSet.member?(all_facts, {iri("Student"), rdfs_subClassOf(), iri("Agent")})
      assert MapSet.member?(all_facts, {iri("Student"), rdfs_subClassOf(), iri("Entity")})

      # cax_sco should derive type memberships
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Agent")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Entity")})
    end

    test "applies sameAs rules for equality propagation" do
      initial = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("alice_smith")},
        {iri("alice_smith"), owl_sameAs(), iri("alice_jones")}
      ])

      rules = [Rules.eq_sym(), Rules.eq_trans()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Symmetry
      assert MapSet.member?(all_facts, {iri("alice_smith"), owl_sameAs(), iri("alice")})
      assert MapSet.member?(all_facts, {iri("alice_jones"), owl_sameAs(), iri("alice_smith")})

      # Transitivity
      assert MapSet.member?(all_facts, {iri("alice"), owl_sameAs(), iri("alice_jones")})
      assert MapSet.member?(all_facts, {iri("alice_jones"), owl_sameAs(), iri("alice")})
    end
  end

  # ============================================================================
  # Tests: Limits and Error Handling
  # ============================================================================

  describe "limits" do
    test "respects max_iterations limit" do
      # Create a scenario that would take many iterations
      initial = MapSet.new([
        {iri("a"), rdfs_subClassOf(), iri("b")},
        {iri("b"), rdfs_subClassOf(), iri("c")},
        {iri("c"), rdfs_subClassOf(), iri("d")}
      ])

      rules = [Rules.scm_sco()]

      result = SemiNaive.materialize_in_memory(rules, initial, max_iterations: 1)

      # Should either complete or hit limit
      case result do
        {:ok, _facts, stats} ->
          assert stats.iterations <= 1

        {:error, :max_iterations_exceeded} ->
          :ok
      end
    end

    test "respects max_facts limit" do
      # Create many entities
      entities = for i <- 1..100, do: {iri("entity#{i}"), rdf_type(), iri("Thing")}
      hierarchy = [{iri("Thing"), rdfs_subClassOf(), iri("Entity")}]

      initial = MapSet.new(entities ++ hierarchy)
      rules = [Rules.cax_sco()]

      result = SemiNaive.materialize_in_memory(rules, initial, max_facts: 50)

      case result do
        {:ok, facts, _stats} ->
          assert MapSet.size(facts) <= 50

        {:error, :max_facts_exceeded} ->
          :ok
      end
    end
  end

  # ============================================================================
  # Tests: Statistics
  # ============================================================================

  describe "statistics" do
    test "compute_stats calculates correct values" do
      initial_count = 5
      all_facts = MapSet.new([
        {iri("a"), iri("p"), iri("b")},
        {iri("b"), iri("p"), iri("c")},
        {iri("c"), iri("p"), iri("d")},
        {iri("d"), iri("p"), iri("e")},
        {iri("e"), iri("p"), iri("f")},
        # Derived
        {iri("a"), iri("q"), iri("c")},
        {iri("a"), iri("q"), iri("d")}
      ])

      stats = SemiNaive.compute_stats(all_facts, initial_count)

      assert stats.total_facts == 7
      assert stats.initial_facts == 5
      assert stats.derived_facts == 2
      assert stats.expansion_ratio == 7 / 5
    end

    test "compute_stats handles zero initial facts" do
      stats = SemiNaive.compute_stats(MapSet.new(), 0)

      assert stats.total_facts == 0
      assert stats.initial_facts == 0
      assert stats.derived_facts == 0
      assert stats.expansion_ratio == 0.0
    end
  end

  # ============================================================================
  # Tests: Rule Stratification
  # ============================================================================

  describe "stratify_rules/1" do
    test "places all OWL 2 RL rules in stratum 0" do
      rules = Rules.all_rules()

      strata = SemiNaive.stratify_rules(rules)

      assert length(strata) == 1
      assert hd(strata).level == 0
      assert length(hd(strata).rules) == length(rules)
    end

    test "handles empty rules list" do
      strata = SemiNaive.stratify_rules([])

      assert length(strata) == 1
      assert hd(strata).rules == []
    end
  end

  # ============================================================================
  # Tests: materialize/5 with callbacks
  # ============================================================================

  describe "materialize/5 with external store" do
    test "uses lookup_fn and store_fn correctly" do
      # Track calls to lookup and store
      {:ok, store_agent} = Agent.start_link(fn -> MapSet.new() end)
      {:ok, lookup_calls} = Agent.start_link(fn -> [] end)

      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Animal")}
      ])

      # Initialize store with initial facts
      Agent.update(store_agent, fn _ -> initial end)

      lookup_fn = fn pattern ->
        Agent.update(lookup_calls, fn calls -> [pattern | calls] end)
        facts = Agent.get(store_agent, & &1)
        matching = match_pattern(pattern, facts)
        {:ok, matching}
      end

      store_fn = fn new_facts ->
        Agent.update(store_agent, fn existing -> MapSet.union(existing, new_facts) end)
        :ok
      end

      rules = [Rules.cax_sco()]

      {:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, initial)

      # Verify derivations occurred
      assert stats.total_derived > 0

      # Verify lookup was called
      calls = Agent.get(lookup_calls, & &1)
      assert length(calls) > 0

      # Verify store contains derived facts
      final_facts = Agent.get(store_agent, & &1)
      assert MapSet.member?(final_facts, {iri("alice"), rdf_type(), iri("Animal")})

      Agent.stop(store_agent)
      Agent.stop(lookup_calls)
    end

    test "handles lookup_fn errors gracefully" do
      initial = MapSet.new([{iri("a"), iri("p"), iri("b")}])

      # Lookup that always fails
      lookup_fn = fn _pattern -> {:error, :database_error} end
      store_fn = fn _facts -> :ok end

      rules = [Rules.cax_sco()]

      # Should still work because no rules match (rule requires rdf:type pattern)
      {:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, initial)
      # First iteration processes initial delta but finds no matches
      assert stats.total_derived == 0
    end
  end

  # ============================================================================
  # Tests: Complex Scenarios
  # ============================================================================

  describe "complex scenarios" do
    test "handles diamond inheritance pattern" do
      #       Thing
      #      /     \
      #   Agent   Physical
      #      \     /
      #       Robot
      initial = MapSet.new([
        {iri("Robot"), rdfs_subClassOf(), iri("Agent")},
        {iri("Robot"), rdfs_subClassOf(), iri("Physical")},
        {iri("Agent"), rdfs_subClassOf(), iri("Thing")},
        {iri("Physical"), rdfs_subClassOf(), iri("Thing")},
        {iri("r2d2"), rdf_type(), iri("Robot")}
      ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # r2d2 should be a Robot, Agent, Physical, and Thing
      assert MapSet.member?(all_facts, {iri("r2d2"), rdf_type(), iri("Agent")})
      assert MapSet.member?(all_facts, {iri("r2d2"), rdf_type(), iri("Physical")})
      assert MapSet.member?(all_facts, {iri("r2d2"), rdf_type(), iri("Thing")})

      # Subclass transitivity should work too
      assert MapSet.member?(all_facts, {iri("Robot"), rdfs_subClassOf(), iri("Thing")})
    end

    test "handles multiple instances with shared hierarchy" do
      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("bob"), rdf_type(), iri("Student")},
        {iri("charlie"), rdf_type(), iri("Professor")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Professor"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # All should be Persons and Agents
      for person <- [iri("alice"), iri("bob"), iri("charlie")] do
        assert MapSet.member?(all_facts, {person, rdf_type(), iri("Person")})
        assert MapSet.member?(all_facts, {person, rdf_type(), iri("Agent")})
      end
    end
  end

  # ============================================================================
  # Tests: Parallel Evaluation
  # ============================================================================

  describe "parallel evaluation" do
    test "parallel produces same results as sequential" do
      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("bob"), rdf_type(), iri("Teacher")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Teacher"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      # Run sequential
      {:ok, seq_facts, seq_stats} = SemiNaive.materialize_in_memory(rules, initial, parallel: false)

      # Run parallel
      {:ok, par_facts, par_stats} = SemiNaive.materialize_in_memory(rules, initial, parallel: true)

      # Results should be identical
      assert seq_facts == par_facts
      assert seq_stats.total_derived == par_stats.total_derived
    end

    test "parallel with max_concurrency option" do
      initial = MapSet.new([
        {iri("a"), rdfs_subClassOf(), iri("b")},
        {iri("b"), rdfs_subClassOf(), iri("c")},
        {iri("c"), rdfs_subClassOf(), iri("d")}
      ])

      rules = [Rules.scm_sco()]

      {:ok, _facts, stats} = SemiNaive.materialize_in_memory(
        rules,
        initial,
        parallel: true,
        max_concurrency: 2
      )

      assert stats.total_derived > 0
    end

    test "materialize_parallel convenience function works" do
      {:ok, store_agent} = Agent.start_link(fn -> MapSet.new() end)

      initial = MapSet.new([
        {iri("x"), rdf_type(), iri("A")},
        {iri("A"), rdfs_subClassOf(), iri("B")}
      ])

      Agent.update(store_agent, fn _ -> initial end)

      lookup_fn = fn pattern ->
        facts = Agent.get(store_agent, & &1)
        {:ok, match_pattern(pattern, facts)}
      end

      store_fn = fn new_facts ->
        Agent.update(store_agent, fn existing -> MapSet.union(existing, new_facts) end)
        :ok
      end

      rules = [Rules.cax_sco()]

      {:ok, stats} = SemiNaive.materialize_parallel(lookup_fn, store_fn, rules, initial)

      assert stats.total_derived > 0

      final_facts = Agent.get(store_agent, & &1)
      assert MapSet.member?(final_facts, {iri("x"), rdf_type(), iri("B")})

      Agent.stop(store_agent)
    end

    test "default_concurrency returns positive integer" do
      concurrency = SemiNaive.default_concurrency()
      assert is_integer(concurrency)
      assert concurrency > 0
    end

    test "parallel handles many rules efficiently" do
      # Create a larger set of rules to test parallelism
      initial = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("bob"), rdf_type(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")},
        {iri("Agent"), rdfs_subClassOf(), iri("Entity")},
        {iri("alice"), owl_sameAs(), iri("alice2")}
      ])

      # Use multiple rules that can run in parallel
      rules = [
        Rules.scm_sco(),
        Rules.cax_sco(),
        Rules.eq_sym(),
        Rules.eq_trans()
      ]

      {:ok, _facts, stats} = SemiNaive.materialize_in_memory(rules, initial, parallel: true)

      # Should complete without error
      assert stats.rules_applied > 0
    end

    test "parallel is deterministic across multiple runs" do
      initial = MapSet.new([
        {iri("a"), rdfs_subClassOf(), iri("b")},
        {iri("b"), rdfs_subClassOf(), iri("c")},
        {iri("c"), rdfs_subClassOf(), iri("d")},
        {iri("x"), rdf_type(), iri("a")}
      ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      # Run multiple times and ensure same result
      results = for _ <- 1..5 do
        {:ok, facts, _stats} = SemiNaive.materialize_in_memory(rules, initial, parallel: true)
        facts
      end

      # All results should be identical
      first = hd(results)
      assert Enum.all?(results, fn r -> r == first end)
    end
  end

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp match_pattern({:pattern, [s, p, o]}, facts) do
    Enum.filter(facts, fn {fs, fp, fo} ->
      matches_term?(fs, s) and matches_term?(fp, p) and matches_term?(fo, o)
    end)
  end

  defp matches_term?(_fact_term, {:var, _}), do: true
  defp matches_term?(fact_term, pattern_term), do: fact_term == pattern_term
end
