defmodule TripleStore.Reasoner.DeltaComputationTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.DeltaComputation
  alias TripleStore.Reasoner.Rule
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

  # Create a simple lookup function from a fact set
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

  # ============================================================================
  # Tests: Basic Delta Application
  # ============================================================================

  describe "apply_rule_delta/5" do
    test "applies rule with delta facts and derives new triples" do
      # Setup: class hierarchy Person -> Animal -> Thing
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")},
          {iri("Animal"), rdfs_subClassOf(), iri("Thing")}
        ])

      # Delta: alice is a Person
      delta =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      # Existing facts (empty - we want to see derivations)
      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.cax_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive: alice rdf:type Animal
      assert MapSet.member?(new_facts, {iri("alice"), rdf_type(), iri("Animal")})
    end

    test "filters out existing facts from derivations" do
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")}
        ])

      delta =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      # alice is already known to be an Animal
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

      # Should not include already existing fact
      refute MapSet.member?(new_facts, {iri("alice"), rdf_type(), iri("Animal")})
      assert Enum.empty?(new_facts)
    end

    test "returns empty set when no rules fire" do
      all_facts =
        MapSet.new([
          {iri("alice"), iri("name"), iri("Alice")}
        ])

      delta =
        MapSet.new([
          {iri("alice"), iri("name"), iri("Alice")}
        ])

      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      # Requires rdf:type and rdfs:subClassOf patterns
      rule = Rules.cax_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      assert Enum.empty?(new_facts)
    end

    test "handles rules with no body patterns" do
      # Create a rule with empty body (unusual but valid)
      rule = Rule.new(:empty_rule, [], Rule.pattern(Rule.var("x"), Rule.iri("p"), Rule.var("y")))

      lookup = fn _ -> {:ok, []} end
      delta = MapSet.new()
      existing = MapSet.new()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      assert Enum.empty?(new_facts)
    end

    test "respects max_derivations limit" do
      # Create many facts that will derive many new triples
      persons = for i <- 1..200, do: {iri("person#{i}"), rdf_type(), iri("Human")}
      hierarchy = [{iri("Human"), rdfs_subClassOf(), iri("Animal")}]

      all_facts = MapSet.new(persons ++ hierarchy)
      delta = MapSet.new(persons)
      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.cax_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing,
          max_derivations: 50
        )

      # Should be limited to 50 derivations
      assert MapSet.size(new_facts) <= 50
    end
  end

  # ============================================================================
  # Tests: Transitive Rules
  # ============================================================================

  describe "transitive rule application" do
    test "applies subClassOf transitivity rule" do
      all_facts =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")},
          {iri("C"), rdfs_subClassOf(), iri("D")}
        ])

      # Delta: A subClassOf B (new fact)
      delta =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")}
        ])

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

      # Should derive: A subClassOf C (via A->B->C)
      assert MapSet.member?(new_facts, {iri("A"), rdfs_subClassOf(), iri("C")})
    end

    test "handles transitive property rule (prp_trp)" do
      prop = iri("contains")

      all_facts =
        MapSet.new([
          {prop, rdf_type(), owl_TransitiveProperty()},
          {iri("box1"), prop, iri("box2")},
          {iri("box2"), prop, iri("box3")}
        ])

      delta =
        MapSet.new([
          {iri("box1"), prop, iri("box2")}
        ])

      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.prp_trp()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive: box1 contains box3
      assert MapSet.member?(new_facts, {iri("box1"), prop, iri("box3")})
    end
  end

  # ============================================================================
  # Tests: sameAs Rules
  # ============================================================================

  describe "sameAs rule application" do
    test "applies sameAs symmetry rule" do
      all_facts =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("alice_smith")}
        ])

      delta =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("alice_smith")}
        ])

      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.eq_sym()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive: alice_smith sameAs alice
      assert MapSet.member?(new_facts, {iri("alice_smith"), owl_sameAs(), iri("alice")})
    end

    test "applies sameAs transitivity rule" do
      all_facts =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("bob")},
          {iri("bob"), owl_sameAs(), iri("charlie")}
        ])

      delta =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("bob")}
        ])

      existing = MapSet.new()

      lookup = make_lookup(all_facts)
      rule = Rules.eq_trans()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive: alice sameAs charlie
      assert MapSet.member?(new_facts, {iri("alice"), owl_sameAs(), iri("charlie")})
    end
  end

  # ============================================================================
  # Tests: Binding Generation
  # ============================================================================

  describe "generate_bindings/6" do
    test "generates bindings for single pattern" do
      facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("bob"), rdf_type(), iri("Person")}
        ])

      delta = facts
      delta_index = DeltaComputation.index_by_predicate(delta)

      patterns = [Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c"))]

      lookup = make_lookup(facts)

      bindings =
        DeltaComputation.generate_bindings(
          lookup,
          patterns,
          delta,
          delta_index,
          # delta position
          0,
          # no conditions
          []
        )

      assert length(bindings) == 2

      xs = Enum.map(bindings, &Map.get(&1, "x"))
      assert iri("alice") in xs
      assert iri("bob") in xs
    end

    test "generates bindings with join between patterns" do
      facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")}
        ])

      delta =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      delta_index = DeltaComputation.index_by_predicate(delta)

      patterns = [
        Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c1")),
        Rule.pattern(Rule.var("c1"), rdfs_subClassOf(), Rule.var("c2"))
      ]

      lookup = make_lookup(facts)

      bindings =
        DeltaComputation.generate_bindings(
          lookup,
          patterns,
          delta,
          delta_index,
          # delta position
          0,
          # no conditions
          []
        )

      assert length(bindings) == 1
      binding = hd(bindings)
      assert Map.get(binding, "x") == iri("alice")
      assert Map.get(binding, "c1") == iri("Person")
      assert Map.get(binding, "c2") == iri("Animal")
    end

    test "applies conditions to filter bindings" do
      facts =
        MapSet.new([
          # self-loop
          {iri("alice"), iri("knows"), iri("alice")},
          # different entities
          {iri("alice"), iri("knows"), iri("bob")}
        ])

      delta = facts
      delta_index = DeltaComputation.index_by_predicate(delta)

      patterns = [Rule.pattern(Rule.var("x"), iri("knows"), Rule.var("y"))]
      conditions = [Rule.not_equal(Rule.var("x"), Rule.var("y"))]

      lookup = make_lookup(facts)

      bindings =
        DeltaComputation.generate_bindings(
          lookup,
          patterns,
          delta,
          delta_index,
          0,
          conditions
        )

      # Only alice->bob should survive (x != y)
      assert length(bindings) == 1
      binding = hd(bindings)
      assert Map.get(binding, "x") == iri("alice")
      assert Map.get(binding, "y") == iri("bob")
    end
  end

  # ============================================================================
  # Tests: Head Instantiation
  # ============================================================================

  describe "instantiate_head/2" do
    test "instantiates head with complete binding" do
      head = Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c"))
      binding = %{"x" => iri("alice"), "c" => iri("Person")}

      result = DeltaComputation.instantiate_head(head, binding)

      assert result == {iri("alice"), rdf_type(), iri("Person")}
    end

    test "returns nil for incomplete binding" do
      head = Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c"))
      # missing "c"
      binding = %{"x" => iri("alice")}

      result = DeltaComputation.instantiate_head(head, binding)

      assert result == nil
    end

    test "handles constants in head pattern" do
      head = Rule.pattern(Rule.var("x"), {:iri, "fixed_predicate"}, Rule.var("y"))
      binding = %{"x" => iri("a"), "y" => iri("b")}

      result = DeltaComputation.instantiate_head(head, binding)

      assert result == {iri("a"), {:iri, "fixed_predicate"}, iri("b")}
    end
  end

  # ============================================================================
  # Tests: Index by Predicate
  # ============================================================================

  describe "index_by_predicate/1" do
    test "groups facts by predicate" do
      facts =
        MapSet.new([
          {iri("a"), rdf_type(), iri("Person")},
          {iri("b"), rdf_type(), iri("Animal")},
          {iri("a"), iri("knows"), iri("b")}
        ])

      index = DeltaComputation.index_by_predicate(facts)

      assert length(Map.get(index, rdf_type())) == 2
      assert length(Map.get(index, iri("knows"))) == 1
    end

    test "handles empty fact set" do
      index = DeltaComputation.index_by_predicate(MapSet.new())
      assert index == %{}
    end
  end

  # ============================================================================
  # Tests: Ground Term Check
  # ============================================================================

  describe "ground_term?/1" do
    test "returns true for IRI" do
      assert DeltaComputation.ground_term?({:iri, "http://example.org/foo"})
    end

    test "returns true for literal" do
      assert DeltaComputation.ground_term?({:literal, :simple, "hello"})
    end

    test "returns true for blank node" do
      assert DeltaComputation.ground_term?({:blank_node, "b1"})
    end

    test "returns false for variable" do
      refute DeltaComputation.ground_term?({:var, "x"})
    end
  end

  # ============================================================================
  # Tests: Utility Functions
  # ============================================================================

  describe "filter_existing/2" do
    test "removes existing facts from derived set" do
      derived =
        MapSet.new([
          {iri("a"), iri("p"), iri("b")},
          {iri("c"), iri("p"), iri("d")}
        ])

      existing =
        MapSet.new([
          {iri("a"), iri("p"), iri("b")}
        ])

      result = DeltaComputation.filter_existing(derived, existing)

      assert MapSet.size(result) == 1
      assert MapSet.member?(result, {iri("c"), iri("p"), iri("d")})
      refute MapSet.member?(result, {iri("a"), iri("p"), iri("b")})
    end
  end

  describe "merge_delta/2" do
    test "combines existing and delta facts" do
      existing = MapSet.new([{iri("a"), iri("p"), iri("b")}])
      delta = MapSet.new([{iri("c"), iri("p"), iri("d")}])

      result = DeltaComputation.merge_delta(existing, delta)

      assert MapSet.size(result) == 2
      assert MapSet.member?(result, {iri("a"), iri("p"), iri("b")})
      assert MapSet.member?(result, {iri("c"), iri("p"), iri("d")})
    end
  end

  # ============================================================================
  # Tests: Multiple Delta Positions
  # ============================================================================

  describe "delta position handling" do
    test "finds derivations from different delta positions" do
      # Facts for both patterns of cax_sco:
      # Pattern 1: ?x rdf:type ?c1
      # Pattern 2: ?c1 rdfs:subClassOf ?c2
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("bob"), rdf_type(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      # Delta includes both a type assertion and a subclass assertion
      delta =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      existing = MapSet.new()
      lookup = make_lookup(all_facts)
      rule = Rules.cax_sco()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive alice rdf:type Person (and possibly bob too depending on positions)
      assert MapSet.member?(new_facts, {iri("alice"), rdf_type(), iri("Person")})
    end
  end

  # ============================================================================
  # Tests: Complex Rule Application
  # ============================================================================

  describe "complex multi-pattern rules" do
    test "applies cls_hv1 (hasValue restriction)" do
      # cls_hv1: x rdf:type c, c owl:hasValue v, c owl:onProperty p -> x p v
      owl_hasValue = {:iri, @owl <> "hasValue"}
      owl_onProperty = {:iri, @owl <> "onProperty"}

      all_facts =
        MapSet.new([
          {iri("john"), rdf_type(), iri("EmployedPerson")},
          {iri("EmployedPerson"), owl_hasValue, iri("Company")},
          {iri("EmployedPerson"), owl_onProperty, iri("worksFor")}
        ])

      delta =
        MapSet.new([
          {iri("john"), rdf_type(), iri("EmployedPerson")}
        ])

      existing = MapSet.new()
      lookup = make_lookup(all_facts)
      rule = Rules.cls_hv1()

      {:ok, new_facts} =
        DeltaComputation.apply_rule_delta(
          lookup,
          rule,
          delta,
          existing
        )

      # Should derive: john worksFor Company
      assert MapSet.member?(new_facts, {iri("john"), iri("worksFor"), iri("Company")})
    end
  end
end
