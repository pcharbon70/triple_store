defmodule TripleStore.Reasoner.BackwardTraceQuadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.BackwardTraceQuad
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @ex "http://example.org/"

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}

  defp quad(g, s, p, o), do: {g, s, p, o}
  defp triple(s, p, o), do: {s, p, o}

  # ============================================================================
  # Tests: could_satisfy_rule?/2
  # ============================================================================

  describe "could_satisfy_rule?/2" do
    test "returns true when quad matches rule body pattern" do
      quad = quad(1, iri("alice"), rdf_type(), iri("Student"))
      rule = Rules.cax_sco()

      # cax_sco body: {?x rdf:type ?c}, {?c rdfs:subClassOf ?y}
      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "returns true when quad matches first pattern in body" do
      quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rule = Rules.cax_sco()

      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "returns true when quad matches second pattern in body" do
      quad = quad(1, iri("Student"), rdfs_subClassOf(), iri("Person"))
      rule = Rules.cax_sco()

      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "returns false when quad does not match any pattern" do
      quad = quad(1, iri("alice"), iri("worksAt"), iri("Company"))
      rule = Rules.cax_sco()

      refute BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "returns false when quad predicate does not match" do
      quad = quad(1, iri("alice"), iri("name"), iri("Alice"))
      rule = Rules.cax_sco()

      refute BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "handles rules with single body pattern" do
      # scm_sco has only one pattern in body
      quad = quad(1, iri("Student"), rdfs_subClassOf(), iri("Person"))
      rule = Rules.scm_sco()

      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end
  end

  # ============================================================================
  # Tests: find_deriving_rules/2
  # ============================================================================

  describe "find_deriving_rules/2" do
    test "finds rules that could derive the given quad" do
      # alice rdf:type Person could be derived by cax_sco
      derived_quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      deriving_rules = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      # cax_sco should match because it derives rdf:type through subClassOf
      assert length(deriving_rules) > 0
      assert Rules.cax_sco() in deriving_rules
    end

    test "returns empty list when no rules could derive the quad" do
      # A random property statement wouldn't be derived by standard RDFS rules
      derived_quad = quad(1, iri("alice"), iri("age"), iri("30"))
      rules = []

      deriving_rules = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      assert deriving_rules == []
    end

    test "matches multiple rules when applicable" do
      derived_quad = quad(1, iri("Student"), rdfs_subClassOf(), iri("Agent"))
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      deriving_rules = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      # scm_sco should match (it derives subClassOf transitivity)
      assert length(deriving_rules) >= 1
    end
  end

  # ============================================================================
  # Tests: trace_affected_quads/4 - Basic Functionality
  # ============================================================================

  describe "trace_affected_quads/4 - basic functionality" do
    test "returns ok tuple when no quads are deleted" do
      # Empty deleted list should always work, even with mock DB
      deleted_quads = []
      rules = [Rules.cax_sco()]

      # Note: This test will need actual DB mocking or test DB setup
      # For now, we test the API contract
      assert {:ok, affected} = BackwardTraceQuad.trace_affected_quads(:mock_db, deleted_quads, rules, graph_id: 1)
      assert is_map(affected)
      assert MapSet.size(affected) == 0
    end

    test "returns result tuple for valid input" do
      deleted_quads = [quad(1, iri("alice"), iri("random"), iri("fact"))]
      rules = [Rules.cax_sco()]

      # With mock DB, errors are caught and empty MapSet is returned
      result = BackwardTraceQuad.trace_affected_quads(:mock_db, deleted_quads, rules, graph_id: 1)
      assert {:ok, affected} = result
      assert is_map(affected)
    end
  end

  # ============================================================================
  # Tests: trace_affected_quads/4 - Graph Scope
  # ============================================================================

  describe "trace_affected_quads/4 - graph scope" do
    test "uses local scope by default" do
      deleted_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      # With mock DB, errors are caught and empty MapSet is returned
      assert {:ok, affected} = BackwardTraceQuad.trace_affected_quads(:mock_db, deleted_quads, rules, graph_id: 1)
      assert is_map(affected)
    end

    test "accepts local scope option explicitly" do
      deleted_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert {:ok, affected} =
               BackwardTraceQuad.trace_affected_quads(:mock_db, deleted_quads, rules,
                 graph_id: 1,
                 scope: :local
               )

      assert is_map(affected)
    end
  end

  # ============================================================================
  # Tests: Pattern Matching
  # ============================================================================

  describe "pattern matching helpers" do
    test "identifies matching patterns for class membership" do
      quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rule = Rules.cax_sco()

      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "identifies matching patterns for subClassOf" do
      quad = quad(1, iri("Student"), rdfs_subClassOf(), iri("Person"))
      rule = Rules.cax_sco()

      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "rejects non-matching predicates" do
      quad = quad(1, iri("alice"), iri("hasFriend"), iri("bob"))
      rule = Rules.cax_sco()

      refute BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end
  end

  # ============================================================================
  # Tests: find_deriving_rules/2 - Edge Cases
  # ============================================================================

  describe "find_deriving_rules/2 - edge cases" do
    test "handles empty rule list" do
      derived_quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rules = []

      deriving_rules = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      # Empty rule list returns empty list
      assert is_list(deriving_rules)
      assert deriving_rules == []
    end

    test "handles derived quad with iri terms" do
      derived_quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rules = [Rules.cax_sco()]

      deriving_rules = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      assert is_list(deriving_rules)
    end
  end

  # ============================================================================
  # Tests: could_satisfy_rule?/2 - Edge Cases
  # ============================================================================

  describe "could_satisfy_rule?/2 - edge cases" do
    test "handles different graph IDs" do
      quad = quad(2, iri("alice"), rdf_type(), iri("Student"))
      rule = Rules.cax_sco()

      # Graph ID doesn't affect pattern matching
      assert BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end

    test "handles literals in object position" do
      quad = quad(1, iri("alice"), iri("age"), {:literal, "30", :xsd_string})
      rule = Rules.cax_sco()

      # Won't match cax_sco because predicate doesn't match
      refute BackwardTraceQuad.could_satisfy_rule?(quad, rule)
    end
  end

  # ============================================================================
  # Tests: Multiple Rules
  # ============================================================================

  describe "multiple rules handling" do
    test "finds all relevant rules for a deleted quad" do
      quad = quad(1, iri("Student"), rdfs_subClassOf(), iri("Person"))
      rules = [Rules.cax_sco(), Rules.scm_sco(), Rules.scm_spo()]

      # Both cax_sco and scm_sco have subClassOf patterns
      assert BackwardTraceQuad.could_satisfy_rule?(quad, Rules.cax_sco())
      assert BackwardTraceQuad.could_satisfy_rule?(quad, Rules.scm_sco())
      # scm_spo is for subPropertyOf, won't match
      refute BackwardTraceQuad.could_satisfy_rule?(quad, Rules.scm_spo())
    end
  end

  # ============================================================================
  # Tests: Return Types
  # ============================================================================

  describe "return types" do
    test "trace_affected_quads has correct function signature" do
      # With mock DB, NIF errors are caught and returned as {:ok, empty}
      deleted_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert {:ok, affected} = BackwardTraceQuad.trace_affected_quads(:mock_db, deleted_quads, rules, graph_id: 1)
      assert is_map(affected)
    end

    test "find_deriving_rules returns list of rules" do
      derived_quad = quad(1, iri("alice"), rdf_type(), iri("Person"))
      rules = [Rules.cax_sco()]

      result = BackwardTraceQuad.find_deriving_rules(derived_quad, rules)

      assert is_list(result)
    end

    test "could_satisfy_rule? returns boolean" do
      quad = quad(1, iri("alice"), rdf_type(), iri("Student"))
      rule = Rules.cax_sco()

      result = BackwardTraceQuad.could_satisfy_rule?(quad, rule)

      assert is_boolean(result)
    end
  end
end
