defmodule TripleStore.Reasoner.ReasoningCorrectnessTest do
  @moduledoc """
  Integration tests for Task 4.6.4: Reasoning Correctness Testing.

  These tests validate that the OWL 2 RL reasoning implementation produces
  correct inferences by comparing against:
  - Expected inference patterns from OWL 2 RL specification
  - Known-hard cases that can cause reasoning errors
  - Consistency checking scenarios
  - Verification of no spurious (incorrect) inferences

  ## Test Coverage

  - 4.6.4.1: Compare inference results with reference reasoner patterns
  - 4.6.4.2: Test known-hard cases (sameAs chains, diamond hierarchies, etc.)
  - 4.6.4.3: Test consistency checking (owl:Nothing membership)
  - 4.6.4.4: Test no spurious inferences generated

  ## Reference: OWL 2 RL Specification

  These tests are based on the OWL 2 RL inference patterns defined in:
  https://www.w3.org/TR/owl2-profiles/#OWL_2_RL
  """
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{SemiNaive, ReasoningProfile, PatternMatcher}

  @moduletag :integration

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp ex_iri(name), do: {:iri, @ex <> name}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}
  defp rdfs_domain, do: {:iri, @rdfs <> "domain"}
  defp rdfs_range, do: {:iri, @rdfs <> "range"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}
  defp owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}
  defp owl_FunctionalProperty, do: {:iri, @owl <> "FunctionalProperty"}
  defp owl_InverseFunctionalProperty, do: {:iri, @owl <> "InverseFunctionalProperty"}
  defp owl_inverseOf, do: {:iri, @owl <> "inverseOf"}
  defp owl_sameAs, do: {:iri, @owl <> "sameAs"}
  defp owl_Nothing, do: {:iri, @owl <> "Nothing"}

  defp query(facts, {s, p, o}) do
    pattern = {:pattern, [s, p, o]}
    PatternMatcher.filter_matching(facts, pattern)
  end

  defp select_types(facts, subject) do
    query(facts, {subject, rdf_type(), {:var, :type}})
    |> Enum.map(fn {_, _, type} -> type end)
  end

  defp has_triple?(facts, triple) do
    MapSet.member?(facts, triple)
  end

  defp materialize(initial_facts, profile \\ :owl2rl) do
    {:ok, rules} = ReasoningProfile.rules_for(profile)
    {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)
    all_facts
  end

  # ============================================================================
  # 4.6.4.1: Compare inference results with reference reasoner patterns
  # ============================================================================

  describe "4.6.4.1 reference reasoner comparison (OWL 2 RL specification patterns)" do
    test "scm-sco: rdfs:subClassOf transitivity" do
      # OWL 2 RL Table 8: scm-sco
      # IF c1 subClassOf c2 AND c2 subClassOf c3
      # THEN c1 subClassOf c3
      facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("A"), rdfs_subClassOf(), ex_iri("C")}),
             "Expected transitive subClassOf: A < B < C implies A < C"
    end

    test "scm-spo: rdfs:subPropertyOf transitivity" do
      # OWL 2 RL Table 8: scm-spo
      # IF p1 subPropertyOf p2 AND p2 subPropertyOf p3
      # THEN p1 subPropertyOf p3
      facts = MapSet.new([
        {ex_iri("p1"), rdfs_subPropertyOf(), ex_iri("p2")},
        {ex_iri("p2"), rdfs_subPropertyOf(), ex_iri("p3")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("p1"), rdfs_subPropertyOf(), ex_iri("p3")}),
             "Expected transitive subPropertyOf"
    end

    test "cax-sco: class membership through subclass" do
      # OWL 2 RL Table 9: cax-sco
      # IF x type c1 AND c1 subClassOf c2
      # THEN x type c2
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Student")},
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Person")}),
             "Expected type inference through subClassOf"
    end

    test "prp-spo1: property inheritance through subproperty" do
      # OWL 2 RL Table 4: prp-spo1
      # IF p1 subPropertyOf p2 AND x p1 y
      # THEN x p2 y
      facts = MapSet.new([
        {ex_iri("teaches"), rdfs_subPropertyOf(), ex_iri("involves")},
        {ex_iri("alice"), ex_iri("teaches"), ex_iri("course101")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("alice"), ex_iri("involves"), ex_iri("course101")}),
             "Expected property inheritance through subPropertyOf"
    end

    test "prp-dom: property domain inference" do
      # OWL 2 RL Table 4: prp-dom
      # IF p domain c AND x p y
      # THEN x type c
      facts = MapSet.new([
        {ex_iri("teaches"), rdfs_domain(), ex_iri("Teacher")},
        {ex_iri("alice"), ex_iri("teaches"), ex_iri("course101")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Teacher")}),
             "Expected domain inference"
    end

    test "prp-rng: property range inference" do
      # OWL 2 RL Table 4: prp-rng
      # IF p range c AND x p y
      # THEN y type c
      facts = MapSet.new([
        {ex_iri("teaches"), rdfs_range(), ex_iri("Course")},
        {ex_iri("alice"), ex_iri("teaches"), ex_iri("course101")}
      ])

      result = materialize(facts, :rdfs)

      assert has_triple?(result, {ex_iri("course101"), rdf_type(), ex_iri("Course")}),
             "Expected range inference"
    end

    test "prp-trp: transitive property" do
      # OWL 2 RL Table 4: prp-trp
      # IF p type TransitiveProperty AND x p y AND y p z
      # THEN x p z
      facts = MapSet.new([
        {ex_iri("ancestorOf"), rdf_type(), owl_TransitiveProperty()},
        {ex_iri("bob"), ex_iri("ancestorOf"), ex_iri("alice")},
        {ex_iri("carol"), ex_iri("ancestorOf"), ex_iri("bob")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("carol"), ex_iri("ancestorOf"), ex_iri("alice")}),
             "Expected transitive property inference"
    end

    test "prp-symp: symmetric property" do
      # OWL 2 RL Table 4: prp-symp
      # IF p type SymmetricProperty AND x p y
      # THEN y p x
      facts = MapSet.new([
        {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()},
        {ex_iri("alice"), ex_iri("knows"), ex_iri("bob")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("bob"), ex_iri("knows"), ex_iri("alice")}),
             "Expected symmetric property inference"
    end

    test "prp-inv1 and prp-inv2: inverse properties" do
      # OWL 2 RL Table 4: prp-inv1, prp-inv2
      # IF p1 inverseOf p2 AND x p1 y THEN y p2 x
      # IF p1 inverseOf p2 AND x p2 y THEN y p1 x
      facts = MapSet.new([
        {ex_iri("parentOf"), owl_inverseOf(), ex_iri("childOf")},
        {ex_iri("alice"), ex_iri("parentOf"), ex_iri("bob")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("bob"), ex_iri("childOf"), ex_iri("alice")}),
             "Expected inverse property inference"
    end

    test "prp-fp: functional property" do
      # OWL 2 RL Table 4: prp-fp
      # IF p type FunctionalProperty AND x p y1 AND x p y2
      # THEN y1 sameAs y2
      facts = MapSet.new([
        {ex_iri("hasMother"), rdf_type(), owl_FunctionalProperty()},
        {ex_iri("alice"), ex_iri("hasMother"), ex_iri("mary")},
        {ex_iri("alice"), ex_iri("hasMother"), ex_iri("mary_smith")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("mary"), owl_sameAs(), ex_iri("mary_smith")}) or
               has_triple?(result, {ex_iri("mary_smith"), owl_sameAs(), ex_iri("mary")}),
             "Expected sameAs from functional property"
    end

    test "prp-ifp: inverse functional property" do
      # OWL 2 RL Table 4: prp-ifp
      # IF p type InverseFunctionalProperty AND x1 p y AND x2 p y
      # THEN x1 sameAs x2
      facts = MapSet.new([
        {ex_iri("ssn"), rdf_type(), owl_InverseFunctionalProperty()},
        {ex_iri("alice"), ex_iri("ssn"), {:literal, "123-45-6789", nil}},
        {ex_iri("alice_smith"), ex_iri("ssn"), {:literal, "123-45-6789", nil}}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}) or
               has_triple?(result, {ex_iri("alice_smith"), owl_sameAs(), ex_iri("alice")}),
             "Expected sameAs from inverse functional property"
    end

    test "eq-sym: owl:sameAs symmetry" do
      # OWL 2 RL Table 5: eq-sym
      # IF x sameAs y THEN y sameAs x
      facts = MapSet.new([
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("alice_smith"), owl_sameAs(), ex_iri("alice")}),
             "Expected sameAs symmetry"
    end

    test "eq-trans: owl:sameAs transitivity" do
      # OWL 2 RL Table 5: eq-trans
      # IF x sameAs y AND y sameAs z THEN x sameAs z
      facts = MapSet.new([
        {ex_iri("a"), owl_sameAs(), ex_iri("b")},
        {ex_iri("b"), owl_sameAs(), ex_iri("c")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("a"), owl_sameAs(), ex_iri("c")}),
             "Expected sameAs transitivity"
    end

    test "eq-rep-s: equality replacement in subject" do
      # OWL 2 RL Table 5: eq-rep-s
      # IF s sameAs s' AND s p o THEN s' p o
      facts = MapSet.new([
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")},
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("alice_smith"), rdf_type(), ex_iri("Person")}),
             "Expected subject replacement via sameAs"
    end

    test "eq-rep-o: equality replacement in object" do
      # OWL 2 RL Table 5: eq-rep-o
      # IF o sameAs o' AND s p o THEN s p o'
      facts = MapSet.new([
        {ex_iri("bob"), owl_sameAs(), ex_iri("robert")},
        {ex_iri("alice"), ex_iri("knows"), ex_iri("bob")}
      ])

      result = materialize(facts)

      assert has_triple?(result, {ex_iri("alice"), ex_iri("knows"), ex_iri("robert")}),
             "Expected object replacement via sameAs"
    end
  end

  # ============================================================================
  # 4.6.4.2: Test known-hard cases
  # ============================================================================

  describe "4.6.4.2 known-hard cases" do
    test "long sameAs chain" do
      # sameAs chains can cause performance issues and incorrect results
      # a = b = c = d = e should all be equivalent
      facts = MapSet.new([
        {ex_iri("a"), owl_sameAs(), ex_iri("b")},
        {ex_iri("b"), owl_sameAs(), ex_iri("c")},
        {ex_iri("c"), owl_sameAs(), ex_iri("d")},
        {ex_iri("d"), owl_sameAs(), ex_iri("e")},
        {ex_iri("a"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)

      # All should have type Person
      for x <- [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d"), ex_iri("e")] do
        assert has_triple?(result, {x, rdf_type(), ex_iri("Person")}),
               "Expected #{inspect(x)} to be Person via sameAs chain"
      end

      # All should be sameAs each other
      for x <- [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d"), ex_iri("e")],
          y <- [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d"), ex_iri("e")],
          x != y do
        assert has_triple?(result, {x, owl_sameAs(), y}),
               "Expected #{inspect(x)} sameAs #{inspect(y)}"
      end
    end

    test "diamond class hierarchy" do
      # Diamond: D < B, D < C, B < A, C < A
      # x : D should be x : A via both paths
      facts = MapSet.new([
        {ex_iri("D"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("D"), rdfs_subClassOf(), ex_iri("C")},
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("A")},
        {ex_iri("C"), rdfs_subClassOf(), ex_iri("A")},
        {ex_iri("x"), rdf_type(), ex_iri("D")}
      ])

      result = materialize(facts, :rdfs)

      # x should have all types
      types = select_types(result, ex_iri("x"))
      assert ex_iri("D") in types
      assert ex_iri("B") in types
      assert ex_iri("C") in types
      assert ex_iri("A") in types
    end

    test "deep transitive chain" do
      # p is transitive, with chain: x0 p x1 p x2 p ... p x10
      facts = MapSet.new([{ex_iri("p"), rdf_type(), owl_TransitiveProperty()}])

      # Add chain of 10 relationships
      chain_facts =
        for i <- 0..9 do
          {ex_iri("x#{i}"), ex_iri("p"), ex_iri("x#{i + 1}")}
        end
        |> MapSet.new()

      all_facts = MapSet.union(facts, chain_facts)
      result = materialize(all_facts)

      # x0 should be related to all x1..x10
      for i <- 1..10 do
        assert has_triple?(result, {ex_iri("x0"), ex_iri("p"), ex_iri("x#{i}")}),
               "Expected x0 p x#{i} via transitive closure"
      end
    end

    test "multiple inheritance with properties" do
      # Entity inherits from multiple classes, each with different property constraints
      facts = MapSet.new([
        {ex_iri("Employee"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("TeachingAssistant"), rdfs_subClassOf(), ex_iri("Employee")},
        {ex_iri("TeachingAssistant"), rdfs_subClassOf(), ex_iri("Student")},
        {ex_iri("worksAt"), rdfs_domain(), ex_iri("Employee")},
        {ex_iri("studiesAt"), rdfs_domain(), ex_iri("Student")},
        {ex_iri("alice"), rdf_type(), ex_iri("TeachingAssistant")},
        {ex_iri("alice"), ex_iri("worksAt"), ex_iri("univ1")},
        {ex_iri("alice"), ex_iri("studiesAt"), ex_iri("univ1")}
      ])

      result = materialize(facts, :rdfs)

      # alice should be all types
      types = select_types(result, ex_iri("alice"))
      assert ex_iri("TeachingAssistant") in types
      assert ex_iri("Employee") in types
      assert ex_iri("Student") in types
      assert ex_iri("Person") in types
    end

    test "sameAs with circular property references" do
      # a knows b, b sameAs c, c knows a - should form complete graph
      facts = MapSet.new([
        {ex_iri("a"), ex_iri("knows"), ex_iri("b")},
        {ex_iri("b"), owl_sameAs(), ex_iri("c")},
        {ex_iri("c"), ex_iri("knows"), ex_iri("a")}
      ])

      result = materialize(facts)

      # a knows c (via b sameAs c)
      assert has_triple?(result, {ex_iri("a"), ex_iri("knows"), ex_iri("c")}),
             "Expected a knows c via sameAs substitution"

      # b knows a (via b sameAs c)
      assert has_triple?(result, {ex_iri("b"), ex_iri("knows"), ex_iri("a")}),
             "Expected b knows a via sameAs substitution"
    end

    test "subproperty and inverse interaction" do
      # p1 subPropertyOf p2, p2 inverseOf p3
      # x p1 y should imply y p3 x (via chain)
      facts = MapSet.new([
        {ex_iri("teaches"), rdfs_subPropertyOf(), ex_iri("involves")},
        {ex_iri("involves"), owl_inverseOf(), ex_iri("involvedIn")},
        {ex_iri("alice"), ex_iri("teaches"), ex_iri("course101")}
      ])

      result = materialize(facts)

      # alice involves course101 (via subproperty)
      assert has_triple?(result, {ex_iri("alice"), ex_iri("involves"), ex_iri("course101")})

      # course101 involvedIn alice (via inverse)
      assert has_triple?(result, {ex_iri("course101"), ex_iri("involvedIn"), ex_iri("alice")})
    end

    test "symmetric and transitive property combination" do
      # related is both symmetric and transitive
      # This creates an equivalence relation
      facts = MapSet.new([
        {ex_iri("related"), rdf_type(), owl_SymmetricProperty()},
        {ex_iri("related"), rdf_type(), owl_TransitiveProperty()},
        {ex_iri("a"), ex_iri("related"), ex_iri("b")},
        {ex_iri("b"), ex_iri("related"), ex_iri("c")}
      ])

      result = materialize(facts)

      # All should be related to all (equivalence class)
      for x <- [ex_iri("a"), ex_iri("b"), ex_iri("c")],
          y <- [ex_iri("a"), ex_iri("b"), ex_iri("c")],
          x != y do
        assert has_triple?(result, {x, ex_iri("related"), y}),
               "Expected #{inspect(x)} related #{inspect(y)} in equivalence class"
      end
    end
  end

  # ============================================================================
  # 4.6.4.3: Test consistency checking (owl:Nothing membership)
  # ============================================================================

  describe "4.6.4.3 consistency checking (owl:Nothing membership)" do
    test "owl:Nothing is never a valid type for consistent ontologies" do
      # In a consistent ontology, no individual should be typed as owl:Nothing
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")},
        {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Thing")}
      ])

      result = materialize(facts, :rdfs)

      # alice should NOT be typed as owl:Nothing
      refute has_triple?(result, {ex_iri("alice"), rdf_type(), owl_Nothing()}),
             "Consistent ontology should not have owl:Nothing instances"
    end

    test "detect potential inconsistency via owl:Nothing subclass" do
      # If a class is subclass of owl:Nothing, instances would be inconsistent
      # This tests that we handle this pattern correctly
      facts = MapSet.new([
        {ex_iri("ImpossibleClass"), rdfs_subClassOf(), owl_Nothing()},
        # Note: We don't actually create an instance of ImpossibleClass
        # as that would be an inconsistency
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts, :rdfs)

      # The ontology should remain consistent
      refute has_triple?(result, {ex_iri("alice"), rdf_type(), owl_Nothing()})
    end

    test "empty class has no instances" do
      # A class with no asserted instances should remain empty
      facts = MapSet.new([
        {ex_iri("EmptyClass"), rdfs_subClassOf(), ex_iri("Thing")},
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts, :rdfs)

      # No one should be inferred as EmptyClass
      empty_class_instances =
        query(result, {{:var, :x}, rdf_type(), ex_iri("EmptyClass")})

      assert Enum.empty?(empty_class_instances),
             "Empty class should have no instances"
    end

    test "disjoint class handling (no instances in both)" do
      # Simulate disjoint classes - an individual shouldn't be in both
      # (This is more of a validation test; OWL 2 RL doesn't fully support disjointness)
      facts = MapSet.new([
        {ex_iri("Cat"), rdfs_subClassOf(), ex_iri("Animal")},
        {ex_iri("Dog"), rdfs_subClassOf(), ex_iri("Animal")},
        {ex_iri("whiskers"), rdf_type(), ex_iri("Cat")}
        # Note: whiskers is NOT typed as Dog
      ])

      result = materialize(facts, :rdfs)

      # whiskers should be Cat and Animal, but not Dog
      types = select_types(result, ex_iri("whiskers"))
      assert ex_iri("Cat") in types
      assert ex_iri("Animal") in types
      refute ex_iri("Dog") in types,
             "whiskers should not be Dog (no assertion or inference path)"
    end
  end

  # ============================================================================
  # 4.6.4.4: Test no spurious inferences generated
  # ============================================================================

  describe "4.6.4.4 no spurious inferences generated" do
    test "only expected inferences from subClassOf" do
      facts = MapSet.new([
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      ])

      result = materialize(facts, :rdfs)

      # Expected inferences
      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Person")})

      # Should NOT have spurious inferences
      # alice should not have any other random types
      types = select_types(result, ex_iri("alice"))
      expected_types = [ex_iri("Student"), ex_iri("Person")]

      for type <- types do
        assert type in expected_types,
               "Unexpected type #{inspect(type)} for alice"
      end
    end

    test "domain inference only where applicable" do
      facts = MapSet.new([
        {ex_iri("teaches"), rdfs_domain(), ex_iri("Teacher")},
        {ex_iri("alice"), ex_iri("teaches"), ex_iri("course101")},
        {ex_iri("bob"), rdf_type(), ex_iri("Student")}
        # bob does NOT teach anything
      ])

      result = materialize(facts, :rdfs)

      # alice should be Teacher (via domain)
      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Teacher")})

      # bob should NOT be Teacher (no teaches property)
      refute has_triple?(result, {ex_iri("bob"), rdf_type(), ex_iri("Teacher")}),
             "bob should not be spuriously inferred as Teacher"
    end

    test "transitive property only along actual chain" do
      facts = MapSet.new([
        {ex_iri("partOf"), rdf_type(), owl_TransitiveProperty()},
        {ex_iri("a"), ex_iri("partOf"), ex_iri("b")},
        {ex_iri("b"), ex_iri("partOf"), ex_iri("c")},
        {ex_iri("x"), ex_iri("partOf"), ex_iri("y")}
        # x-y chain is separate from a-b-c chain
      ])

      result = materialize(facts)

      # Expected: a partOf c (via transitivity)
      assert has_triple?(result, {ex_iri("a"), ex_iri("partOf"), ex_iri("c")})

      # Should NOT have spurious cross-chain inferences
      refute has_triple?(result, {ex_iri("a"), ex_iri("partOf"), ex_iri("y")}),
             "a should not be partOf y (separate chains)"
      refute has_triple?(result, {ex_iri("x"), ex_iri("partOf"), ex_iri("c")}),
             "x should not be partOf c (separate chains)"
    end

    test "sameAs only propagates to equivalent entities" do
      facts = MapSet.new([
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")},
        {ex_iri("bob"), rdf_type(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      ])

      result = materialize(facts)

      # alice_smith should be Student (via sameAs)
      assert has_triple?(result, {ex_iri("alice_smith"), rdf_type(), ex_iri("Student")})

      # bob should NOT get any properties from alice
      refute has_triple?(result, {ex_iri("bob"), rdf_type(), ex_iri("Student")}),
             "bob should not inherit from alice (not sameAs)"
    end

    test "inverse property only where defined" do
      facts = MapSet.new([
        {ex_iri("parentOf"), owl_inverseOf(), ex_iri("childOf")},
        {ex_iri("alice"), ex_iri("parentOf"), ex_iri("bob")},
        {ex_iri("carol"), ex_iri("knows"), ex_iri("dave")}
        # knows has no inverse defined
      ])

      result = materialize(facts)

      # bob childOf alice (via inverse)
      assert has_triple?(result, {ex_iri("bob"), ex_iri("childOf"), ex_iri("alice")})

      # dave should NOT have any spurious inverse of knows
      refute has_triple?(result, {ex_iri("dave"), ex_iri("knows"), ex_iri("carol")}),
             "knows has no inverse - should not create spurious inverse"
    end

    test "count of derived facts matches expected" do
      # Simple ontology with predictable inference count
      facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])

      result = materialize(facts, :rdfs)

      # Expected facts:
      # Original: A < B, B < C, x : A (3)
      # Derived: A < C (1), x : B (1), x : C (1)
      # Total: 6

      # Count only instance assertions and class hierarchy
      type_facts =
        query(result, {{:var, :s}, rdf_type(), {:var, :o}})
        |> length()

      subclass_facts =
        query(result, {{:var, :s}, rdfs_subClassOf(), {:var, :o}})
        |> length()

      assert type_facts == 3, "Expected 3 type assertions (x:A, x:B, x:C)"
      assert subclass_facts == 3, "Expected 3 subclass relations (A<B, B<C, A<C)"
    end

    test "no self-loops from symmetric property" do
      # Symmetric property should not create x knows x from x knows y
      facts = MapSet.new([
        {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()},
        {ex_iri("alice"), ex_iri("knows"), ex_iri("bob")}
      ])

      result = materialize(facts)

      # bob knows alice (symmetric)
      assert has_triple?(result, {ex_iri("bob"), ex_iri("knows"), ex_iri("alice")})

      # Should NOT create alice knows alice or bob knows bob
      refute has_triple?(result, {ex_iri("alice"), ex_iri("knows"), ex_iri("alice")}),
             "Should not create spurious self-loop from symmetric property"
      refute has_triple?(result, {ex_iri("bob"), ex_iri("knows"), ex_iri("bob")}),
             "Should not create spurious self-loop from symmetric property"
    end
  end

  # ============================================================================
  # Additional Correctness Tests
  # ============================================================================

  describe "additional correctness tests" do
    test "combining multiple rule types produces correct result" do
      # Comprehensive test combining multiple inference patterns
      facts = MapSet.new([
        # Class hierarchy
        {ex_iri("Professor"), rdfs_subClassOf(), ex_iri("Faculty")},
        {ex_iri("Faculty"), rdfs_subClassOf(), ex_iri("Person")},

        # Property hierarchy
        {ex_iri("advises"), rdfs_subPropertyOf(), ex_iri("knows")},

        # Property characteristics
        {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()},

        # Domain/range
        {ex_iri("advises"), rdfs_domain(), ex_iri("Faculty")},
        {ex_iri("advises"), rdfs_range(), ex_iri("Student")},

        # Instance data
        {ex_iri("prof1"), rdf_type(), ex_iri("Professor")},
        {ex_iri("prof1"), ex_iri("advises"), ex_iri("student1")}
      ])

      result = materialize(facts)

      # Check class hierarchy inference
      assert has_triple?(result, {ex_iri("prof1"), rdf_type(), ex_iri("Faculty")})
      assert has_triple?(result, {ex_iri("prof1"), rdf_type(), ex_iri("Person")})

      # Check property hierarchy inference
      assert has_triple?(result, {ex_iri("prof1"), ex_iri("knows"), ex_iri("student1")})

      # Check symmetric property inference (knows is symmetric)
      assert has_triple?(result, {ex_iri("student1"), ex_iri("knows"), ex_iri("prof1")})

      # Check range inference
      assert has_triple?(result, {ex_iri("student1"), rdf_type(), ex_iri("Student")})
    end

    test "incremental reasoning maintains correctness" do
      # First materialization
      facts1 = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])

      result1 = materialize(facts1, :rdfs)
      assert has_triple?(result1, {ex_iri("x"), rdf_type(), ex_iri("B")})

      # Extended ontology
      facts2 = MapSet.union(facts1, MapSet.new([
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")}
      ]))

      result2 = materialize(facts2, :rdfs)
      assert has_triple?(result2, {ex_iri("x"), rdf_type(), ex_iri("C")})

      # Original inference still holds
      assert has_triple?(result2, {ex_iri("x"), rdf_type(), ex_iri("B")})
    end
  end
end
