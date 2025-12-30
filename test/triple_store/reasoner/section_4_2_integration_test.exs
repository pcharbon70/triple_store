defmodule TripleStore.Reasoner.Section42IntegrationTest do
  @moduledoc """
  Comprehensive integration tests for Section 4.2: Semi-Naive Evaluation.

  These tests verify the complete requirements from Task 4.2.5:
  - Delta computation finds new facts only
  - Fixpoint terminates correctly
  - Fixpoint produces complete inference closure
  - Parallel evaluation produces same results as sequential
  - Derived facts stored separately
  - Clear derived removes only inferred triples
  """
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{SemiNaive, DeltaComputation, DerivedStore, Rules}
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @moduletag :integration

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

  defp make_lookup(facts) do
    fn {:pattern, [s, p, o]} ->
      matching =
        facts
        |> Enum.filter(fn {fs, fp, fo} ->
          matches?(fs, s) and matches?(fp, p) and matches?(fo, o)
        end)

      {:ok, Enum.to_list(matching)}
    end
  end

  defp matches?(_fact_term, {:var, _}), do: true
  defp matches?(fact_term, pattern_term), do: fact_term == pattern_term

  # Database setup for integration tests
  @test_db_path "/tmp/section_4_2_test"

  defp setup_db do
    db_path = "#{@test_db_path}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(db_path)
    {db, db_path}
  end

  defp cleanup_db(db, db_path) do
    NIF.close(db)
    File.rm_rf!(db_path)
  end

  # ============================================================================
  # Task 4.2.5.1: Test delta computation finds new facts only
  # ============================================================================

  describe "delta computation finds new facts only" do
    test "apply_rule_delta excludes facts already in existing set" do
      # Setup facts
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")},
          {iri("Animal"), rdfs_subClassOf(), iri("Thing")}
        ])

      # Delta is the type assertion
      delta =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      # Existing already has alice as Animal
      existing =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Animal")}
        ])

      lookup = make_lookup(all_facts)
      rule = Rules.cax_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # The result should NOT include alice:Animal (already exists)
      refute MapSet.member?(new_facts, {iri("alice"), rdf_type(), iri("Animal")})

      # But may include other derivations like alice:Thing
      # (depending on whether it was in existing)
    end

    test "apply_rule_delta only returns facts not in delta or existing" do
      all_facts =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")},
          {iri("b"), rdfs_subClassOf(), iri("c")},
          {iri("c"), rdfs_subClassOf(), iri("d")}
        ])

      delta =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")}
        ])

      # Simulate a->c already being known
      existing =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")},
          {iri("a"), rdfs_subClassOf(), iri("c")}
        ])

      lookup = make_lookup(all_facts)
      rule = Rules.scm_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should NOT contain a->c (already in existing)
      refute MapSet.member?(new_facts, {iri("a"), rdfs_subClassOf(), iri("c")})
    end

    test "empty delta produces no new facts" do
      all_facts =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")}
        ])

      delta = MapSet.new()
      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.scm_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      assert MapSet.size(new_facts) == 0
    end
  end

  # ============================================================================
  # Task 4.2.5.2: Test fixpoint terminates correctly
  # ============================================================================

  describe "fixpoint terminates correctly" do
    test "terminates with empty delta" do
      initial = MapSet.new()
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      {:ok, _facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # With empty input, should terminate immediately
      assert stats.iterations == 0
      assert stats.total_derived == 0
    end

    test "terminates when no more facts can be derived" do
      # Simple hierarchy that reaches fixpoint
      initial =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")},
          {iri("b"), rdfs_subClassOf(), iri("c")}
        ])

      rules = [Rules.scm_sco()]

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should derive a->c and terminate
      assert MapSet.member?(all_facts, {iri("a"), rdfs_subClassOf(), iri("c")})

      # The last iteration should have derived 0 facts (empty delta)
      last_derivation = List.last(stats.derivations_per_iteration)
      assert last_derivation == 0
    end

    test "terminates with cyclic dependencies" do
      # Create a cycle: A -> B -> C -> A
      initial =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")},
          {iri("C"), rdfs_subClassOf(), iri("A")}
        ])

      rules = [Rules.scm_sco()]

      # Should not infinite loop
      {:ok, _all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should complete in finite iterations
      assert stats.iterations < 100
    end

    test "terminates with sameAs reflexive facts" do
      # sameAs can create many derivations with symmetry/transitivity
      initial =
        MapSet.new([
          {iri("a"), owl_sameAs(), iri("b")},
          {iri("b"), owl_sameAs(), iri("c")},
          {iri("c"), owl_sameAs(), iri("d")}
        ])

      rules = [Rules.eq_sym(), Rules.eq_trans()]

      {:ok, _all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Should terminate
      assert stats.iterations < 50
    end

    test "max_iterations option prevents infinite execution" do
      # Create a chain that would take many iterations
      chain =
        for i <- 1..20 do
          {iri("n#{i}"), rdfs_subClassOf(), iri("n#{i + 1}")}
        end

      initial = MapSet.new(chain)
      rules = [Rules.scm_sco()]

      result = SemiNaive.materialize_in_memory(rules, initial, max_iterations: 3)

      case result do
        {:ok, _facts, stats} ->
          assert stats.iterations <= 3

        {:error, :max_iterations_exceeded} ->
          # Also acceptable - means limit was hit
          :ok
      end
    end
  end

  # ============================================================================
  # Task 4.2.5.3: Test fixpoint produces complete inference closure
  # ============================================================================

  describe "fixpoint produces complete inference closure" do
    test "computes full transitive closure of linear chain" do
      # Chain: A -> B -> C -> D -> E
      initial =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")},
          {iri("C"), rdfs_subClassOf(), iri("D")},
          {iri("D"), rdfs_subClassOf(), iri("E")}
        ])

      rules = [Rules.scm_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # All transitive relationships should be present
      # A -> C, A -> D, A -> E
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("C")})
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("D")})
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("E")})

      # B -> D, B -> E
      assert MapSet.member?(all_facts, {iri("B"), rdfs_subClassOf(), iri("D")})
      assert MapSet.member?(all_facts, {iri("B"), rdfs_subClassOf(), iri("E")})

      # C -> E
      assert MapSet.member?(all_facts, {iri("C"), rdfs_subClassOf(), iri("E")})

      # Total transitive edges: 6 derived + 4 original = 10
      subclass_facts = Enum.filter(all_facts, fn {_, p, _} -> p == rdfs_subClassOf() end)
      assert length(subclass_facts) == 10
    end

    test "computes complete type inference through hierarchy" do
      # Type hierarchy with multiple instances
      initial =
        MapSet.new([
          # Instances
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("bob"), rdf_type(), iri("Teacher")},
          # Hierarchy
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("Teacher"), rdfs_subClassOf(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Agent")},
          {iri("Agent"), rdfs_subClassOf(), iri("Thing")}
        ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # All type inferences for alice
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Agent")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Thing")})

      # All type inferences for bob
      assert MapSet.member?(all_facts, {iri("bob"), rdf_type(), iri("Person")})
      assert MapSet.member?(all_facts, {iri("bob"), rdf_type(), iri("Agent")})
      assert MapSet.member?(all_facts, {iri("bob"), rdf_type(), iri("Thing")})
    end

    test "computes complete sameAs equivalence closure" do
      # Full equivalence relation properties
      initial =
        MapSet.new([
          {iri("a"), owl_sameAs(), iri("b")},
          {iri("b"), owl_sameAs(), iri("c")}
        ])

      rules = [Rules.eq_sym(), Rules.eq_trans()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # Symmetry
      assert MapSet.member?(all_facts, {iri("b"), owl_sameAs(), iri("a")})
      assert MapSet.member?(all_facts, {iri("c"), owl_sameAs(), iri("b")})

      # Transitivity
      assert MapSet.member?(all_facts, {iri("a"), owl_sameAs(), iri("c")})
      assert MapSet.member?(all_facts, {iri("c"), owl_sameAs(), iri("a")})
    end

    test "computes complete transitive property closure" do
      contains = iri("contains")

      initial =
        MapSet.new([
          {contains, rdf_type(), owl_TransitiveProperty()},
          {iri("box1"), contains, iri("box2")},
          {iri("box2"), contains, iri("box3")},
          {iri("box3"), contains, iri("box4")},
          {iri("box4"), contains, iri("box5")}
        ])

      rules = [Rules.prp_trp()]

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)

      # All transitive containments
      assert MapSet.member?(all_facts, {iri("box1"), contains, iri("box3")})
      assert MapSet.member?(all_facts, {iri("box1"), contains, iri("box4")})
      assert MapSet.member?(all_facts, {iri("box1"), contains, iri("box5")})
      assert MapSet.member?(all_facts, {iri("box2"), contains, iri("box4")})
      assert MapSet.member?(all_facts, {iri("box2"), contains, iri("box5")})
      assert MapSet.member?(all_facts, {iri("box3"), contains, iri("box5")})
    end
  end

  # ============================================================================
  # Task 4.2.5.4: Test parallel evaluation produces same results as sequential
  # ============================================================================

  describe "parallel evaluation produces same results as sequential" do
    test "simple hierarchy same results" do
      initial =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Agent")}
        ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      # Sequential
      {:ok, seq_facts, _} = SemiNaive.materialize_in_memory(rules, initial, parallel: false)

      # Parallel
      {:ok, par_facts, _} = SemiNaive.materialize_in_memory(rules, initial, parallel: true)

      assert seq_facts == par_facts
    end

    test "complex multi-rule interaction same results" do
      initial =
        MapSet.new([
          # Type hierarchy
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("bob"), rdf_type(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Agent")},
          # sameAs
          {iri("alice"), owl_sameAs(), iri("alice2")},
          # Transitive property
          {iri("partOf"), rdf_type(), owl_TransitiveProperty()},
          {iri("hand"), iri("partOf"), iri("arm")},
          {iri("arm"), iri("partOf"), iri("body")}
        ])

      rules = [
        Rules.scm_sco(),
        Rules.cax_sco(),
        Rules.eq_sym(),
        Rules.eq_trans(),
        Rules.prp_trp()
      ]

      # Run both modes
      {:ok, seq_facts, seq_stats} =
        SemiNaive.materialize_in_memory(rules, initial, parallel: false)

      {:ok, par_facts, par_stats} =
        SemiNaive.materialize_in_memory(rules, initial, parallel: true)

      # Facts must be identical
      assert seq_facts == par_facts

      # Total derived should match
      assert seq_stats.total_derived == par_stats.total_derived
    end

    test "parallel deterministic across multiple runs" do
      initial =
        MapSet.new([
          {iri("a"), rdfs_subClassOf(), iri("b")},
          {iri("b"), rdfs_subClassOf(), iri("c")},
          {iri("c"), rdfs_subClassOf(), iri("d")},
          {iri("x"), rdf_type(), iri("a")},
          {iri("y"), rdf_type(), iri("b")}
        ])

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      # Run 10 times
      results =
        for _ <- 1..10 do
          {:ok, facts, _} = SemiNaive.materialize_in_memory(rules, initial, parallel: true)
          facts
        end

      # All results should be identical
      first = hd(results)
      assert Enum.all?(results, fn r -> r == first end)
    end

    test "materialize_parallel function works correctly" do
      {:ok, store_agent} = Agent.start_link(fn -> MapSet.new() end)

      initial =
        MapSet.new([
          {iri("x"), rdf_type(), iri("A")},
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")}
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

      rules = [Rules.scm_sco(), Rules.cax_sco()]

      {:ok, stats} = SemiNaive.materialize_parallel(lookup_fn, store_fn, rules, initial)

      final_facts = Agent.get(store_agent, & &1)

      # Should have derived type inferences
      assert MapSet.member?(final_facts, {iri("x"), rdf_type(), iri("B")})
      assert MapSet.member?(final_facts, {iri("x"), rdf_type(), iri("C")})
      assert stats.total_derived > 0

      Agent.stop(store_agent)
    end
  end

  # ============================================================================
  # Task 4.2.5.5: Test derived facts stored separately
  # ============================================================================

  describe "derived facts stored separately" do
    test "derived column family stores only inferred facts" do
      {db, db_path} = setup_db()

      try do
        # Insert explicit facts into main indices
        explicit_facts = [{1, 2, 3}, {4, 5, 6}]
        :ok = Index.insert_triples(db, explicit_facts)

        # Insert derived facts into derived column family
        derived_facts = [{7, 8, 9}, {10, 11, 12}]
        :ok = DerivedStore.insert_derived(db, derived_facts)

        # Query derived only - should not include explicit
        {:ok, derived_stream} = DerivedStore.lookup_derived(db, {:var, :var, :var})
        derived_result = Enum.to_list(derived_stream) |> Enum.sort()

        assert derived_result == [{7, 8, 9}, {10, 11, 12}]
        refute {1, 2, 3} in derived_result
        refute {4, 5, 6} in derived_result

        # Query explicit only - should not include derived
        {:ok, explicit_stream} = DerivedStore.lookup_explicit(db, {:var, :var, :var})
        explicit_result = Enum.to_list(explicit_stream) |> Enum.sort()

        assert {1, 2, 3} in explicit_result
        assert {4, 5, 6} in explicit_result
        refute {7, 8, 9} in explicit_result

        # Query both - should include all
        {:ok, all_stream} = DerivedStore.lookup_all(db, {:var, :var, :var})
        all_result = Enum.to_list(all_stream) |> Enum.sort()

        assert length(all_result) == 4
      after
        cleanup_db(db, db_path)
      end
    end

    test "derived_exists? only checks derived column family" do
      {db, db_path} = setup_db()

      try do
        # Insert explicit fact
        :ok = Index.insert_triples(db, [{1, 2, 3}])

        # Insert derived fact
        :ok = DerivedStore.insert_derived(db, [{4, 5, 6}])

        # Check existence
        assert {:ok, false} = DerivedStore.derived_exists?(db, {1, 2, 3})
        assert {:ok, true} = DerivedStore.derived_exists?(db, {4, 5, 6})
      after
        cleanup_db(db, db_path)
      end
    end

    test "count only counts derived facts" do
      {db, db_path} = setup_db()

      try do
        # Insert explicit facts
        :ok = Index.insert_triples(db, [{1, 2, 3}, {4, 5, 6}])

        # Insert derived facts
        :ok = DerivedStore.insert_derived(db, [{7, 8, 9}])

        # Count should only count derived
        assert {:ok, 1} = DerivedStore.count(db)
      after
        cleanup_db(db, db_path)
      end
    end
  end

  # ============================================================================
  # Task 4.2.5.6: Test clear derived removes only inferred triples
  # ============================================================================

  describe "clear derived removes only inferred triples" do
    test "clear_all removes derived but preserves explicit" do
      {db, db_path} = setup_db()

      try do
        # Insert explicit facts
        explicit = [{1, 2, 3}, {4, 5, 6}]
        :ok = Index.insert_triples(db, explicit)

        # Insert derived facts
        derived = [{7, 8, 9}, {10, 11, 12}, {13, 14, 15}]
        :ok = DerivedStore.insert_derived(db, derived)

        # Clear all derived
        assert {:ok, 3} = DerivedStore.clear_all(db)

        # Derived should be empty
        assert {:ok, 0} = DerivedStore.count(db)

        # Explicit should remain
        {:ok, explicit_stream} = DerivedStore.lookup_explicit(db, {:var, :var, :var})
        explicit_result = Enum.to_list(explicit_stream) |> Enum.sort()
        assert length(explicit_result) == 2
        assert {1, 2, 3} in explicit_result
        assert {4, 5, 6} in explicit_result
      after
        cleanup_db(db, db_path)
      end
    end

    test "clear_all returns correct count" do
      {db, db_path} = setup_db()

      try do
        # Insert various derived facts
        :ok =
          DerivedStore.insert_derived(db, [{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}])

        # Clear should return 5
        assert {:ok, 5} = DerivedStore.clear_all(db)

        # Second clear should return 0
        assert {:ok, 0} = DerivedStore.clear_all(db)
      after
        cleanup_db(db, db_path)
      end
    end

    test "clear_all enables rematerialization" do
      {db, db_path} = setup_db()

      try do
        # Insert some derived facts
        :ok = DerivedStore.insert_derived(db, [{100, 100, 100}])
        assert {:ok, 1} = DerivedStore.count(db)

        # Clear for rematerialization
        {:ok, _} = DerivedStore.clear_all(db)

        # Now we can insert new derived facts
        :ok = DerivedStore.insert_derived(db, [{200, 200, 200}, {300, 300, 300}])
        assert {:ok, 2} = DerivedStore.count(db)

        # New facts should be there
        assert {:ok, true} = DerivedStore.derived_exists?(db, {200, 200, 200})
        assert {:ok, true} = DerivedStore.derived_exists?(db, {300, 300, 300})

        # Old fact should be gone
        assert {:ok, false} = DerivedStore.derived_exists?(db, {100, 100, 100})
      after
        cleanup_db(db, db_path)
      end
    end

    test "delete_derived removes specific facts only" do
      {db, db_path} = setup_db()

      try do
        # Insert multiple derived facts
        :ok = DerivedStore.insert_derived(db, [{1, 1, 1}, {2, 2, 2}, {3, 3, 3}])

        # Delete specific facts
        :ok = DerivedStore.delete_derived(db, [{2, 2, 2}])

        # Check what remains
        assert {:ok, true} = DerivedStore.derived_exists?(db, {1, 1, 1})
        assert {:ok, false} = DerivedStore.derived_exists?(db, {2, 2, 2})
        assert {:ok, true} = DerivedStore.derived_exists?(db, {3, 3, 3})
      after
        cleanup_db(db, db_path)
      end
    end
  end

  # ============================================================================
  # Additional Integration Tests
  # ============================================================================

  describe "end-to-end integration" do
    test "full reasoning pipeline with in-memory store" do
      # This test verifies the complete reasoning pipeline works end-to-end
      # using in-memory storage (term-level representation)
      parent = iri("parent")
      ancestor = iri("ancestor")

      a = iri("a")
      b = iri("b")
      c = iri("c")

      initial =
        MapSet.new([
          {a, parent, b},
          {b, parent, c}
        ])

      # Define a simple transitive rule
      rule =
        TripleStore.Reasoner.Rule.new(
          :ancestor,
          [
            {:pattern, [{:var, :x}, parent, {:var, :y}]},
            {:pattern, [{:var, :y}, parent, {:var, :z}]}
          ],
          {:pattern, [{:var, :x}, ancestor, {:var, :z}]}
        )

      # Run in-memory materialization
      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory([rule], initial)

      # Should have derived the transitive relationship: a ancestor c
      assert MapSet.member?(all_facts, {a, ancestor, c})
      assert stats.total_derived >= 1
    end

    test "DerivedStore integrates with ID-level reasoning" do
      {db, db_path} = setup_db()

      try do
        # Insert explicit facts using integer IDs
        # (In a real system, these would be dictionary-encoded terms)
        parent_id = 1
        ancestor_id = 2
        a_id = 10
        b_id = 11
        c_id = 12

        explicit_facts = [
          {a_id, parent_id, b_id},
          {b_id, parent_id, c_id}
        ]

        :ok = Index.insert_triples(db, explicit_facts)

        # Create store function that persists to derived column family
        store_fn = DerivedStore.make_store_fn(db)

        # Simulate deriving a fact
        derived_fact = {a_id, ancestor_id, c_id}
        :ok = store_fn.(MapSet.new([derived_fact]))

        # Verify derived fact is stored
        assert {:ok, true} = DerivedStore.derived_exists?(db, derived_fact)

        # Verify explicit facts are still accessible
        {:ok, explicit_stream} = Index.lookup(db, {:var, :var, :var})
        explicit_result = Enum.to_list(explicit_stream)
        assert length(explicit_result) == 2
      after
        cleanup_db(db, db_path)
      end
    end
  end

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp match_pattern({:pattern, [s, p, o]}, facts) do
    Enum.filter(facts, fn {fs, fp, fo} ->
      matches?(fs, s) and matches?(fp, p) and matches?(fo, o)
    end)
  end
end
