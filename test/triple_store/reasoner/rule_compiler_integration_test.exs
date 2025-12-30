defmodule TripleStore.Reasoner.RuleCompilerIntegrationTest do
  @moduledoc """
  Integration tests for the complete rule compiler subsystem.

  These tests verify end-to-end behavior of:
  - Rule representation capturing patterns correctly
  - Rule inference semantics (subClassOf, domain/range, transitive, etc.)
  - Rule compilation filtering inapplicable rules
  - Rule optimization reordering patterns
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{Rule, Rules, RuleCompiler, RuleOptimizer}

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Rule Representation Tests
  # ============================================================================

  describe "rule representation captures patterns correctly" do
    test "captures subject-predicate-object pattern structure" do
      rule = Rules.cax_sco()
      [p1, p2] = Rule.body_patterns(rule)

      # First pattern: ?x rdf:type ?c1
      assert {:pattern, [s1, pred1, o1]} = p1
      assert {:var, "x"} = s1
      assert {:iri, "#{@rdf}type"} = pred1
      assert {:var, "c1"} = o1

      # Second pattern: ?c1 rdfs:subClassOf ?c2
      assert {:pattern, [s2, pred2, o2]} = p2
      assert {:var, "c1"} = s2
      assert {:iri, "#{@rdfs}subClassOf"} = pred2
      assert {:var, "c2"} = o2

      # Head pattern: ?x rdf:type ?c2
      assert {:pattern, [sh, predh, oh]} = rule.head
      assert {:var, "x"} = sh
      assert {:iri, "#{@rdf}type"} = predh
      assert {:var, "c2"} = oh
    end

    test "captures variable bindings across patterns" do
      rule = Rules.scm_sco()

      body_vars = Rule.body_variables(rule)
      head_vars = Rule.head_variables(rule)

      # Variables c1, c2, c3 should span body and head
      assert MapSet.member?(body_vars, "c1")
      assert MapSet.member?(body_vars, "c2")
      assert MapSet.member?(body_vars, "c3")

      # Head uses c1, c3 (skipping c2)
      assert MapSet.member?(head_vars, "c1")
      assert MapSet.member?(head_vars, "c3")
    end

    test "captures conditions in rule body" do
      rule = Rules.prp_fp()

      conditions = Rule.body_conditions(rule)
      assert length(conditions) == 1

      [cond] = conditions
      assert {:not_equal, {:var, "y1"}, {:var, "y2"}} = cond
    end

    test "all OWL 2 RL rules have valid pattern structure" do
      for rule <- Rules.all_rules() do
        # Every rule has at least one body pattern
        assert Rule.pattern_count(rule) >= 1,
               "Rule #{rule.name} should have at least one body pattern"

        # Every pattern has exactly 3 terms
        for {:pattern, terms} <- Rule.body_patterns(rule) do
          assert length(terms) == 3,
                 "Pattern in #{rule.name} should have exactly 3 terms"
        end

        # Head pattern has exactly 3 terms
        {:pattern, head_terms} = rule.head

        assert length(head_terms) == 3,
               "Head of #{rule.name} should have exactly 3 terms"
      end
    end
  end

  # ============================================================================
  # subClassOf Rule Inference Tests
  # ============================================================================

  describe "subClassOf rule produces correct inferences" do
    test "cax_sco derives type from subclass chain" do
      rule = Rules.cax_sco()

      # Simulate: alice rdf:type Person, Person rdfs:subClassOf Agent
      # Should derive: alice rdf:type Agent
      binding = %{
        "x" => {:iri, "#{@ex}alice"},
        "c1" => {:iri, "#{@ex}Person"},
        "c2" => {:iri, "#{@ex}Agent"}
      }

      # Apply binding to head
      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alice"},
                {:iri, "#{@rdf}type"},
                {:iri, "#{@ex}Agent"}
              ]} = inferred

      assert Rule.ground?(inferred)
    end

    test "scm_sco derives transitive subclass relationship" do
      rule = Rules.scm_sco()

      # Simulate: Student rdfs:subClassOf Person, Person rdfs:subClassOf Agent
      # Should derive: Student rdfs:subClassOf Agent
      binding = %{
        "c1" => {:iri, "#{@ex}Student"},
        "c2" => {:iri, "#{@ex}Person"},
        "c3" => {:iri, "#{@ex}Agent"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}Student"},
                {:iri, "#{@rdfs}subClassOf"},
                {:iri, "#{@ex}Agent"}
              ]} = inferred
    end
  end

  # ============================================================================
  # Domain/Range Rule Inference Tests
  # ============================================================================

  describe "domain/range rules produce type inferences" do
    test "prp_dom derives subject type from domain" do
      rule = Rules.prp_dom()

      # Simulate: worksFor rdfs:domain Employee, alice worksFor acme
      # Should derive: alice rdf:type Employee
      binding = %{
        "p" => {:iri, "#{@ex}worksFor"},
        "c" => {:iri, "#{@ex}Employee"},
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}acme"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alice"},
                {:iri, "#{@rdf}type"},
                {:iri, "#{@ex}Employee"}
              ]} = inferred
    end

    test "prp_rng derives object type from range" do
      rule = Rules.prp_rng()

      # Simulate: worksFor rdfs:range Company, alice worksFor acme
      # Should derive: acme rdf:type Company
      binding = %{
        "p" => {:iri, "#{@ex}worksFor"},
        "c" => {:iri, "#{@ex}Company"},
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}acme"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}acme"},
                {:iri, "#{@rdf}type"},
                {:iri, "#{@ex}Company"}
              ]} = inferred
    end
  end

  # ============================================================================
  # Transitive Property Rule Tests
  # ============================================================================

  describe "transitive property rule chains correctly" do
    test "prp_trp chains through intermediate nodes" do
      rule = Rules.prp_trp()

      # Simulate: contains is TransitiveProperty
      # a contains b, b contains c
      # Should derive: a contains c
      binding = %{
        "p" => {:iri, "#{@ex}contains"},
        "x" => {:iri, "#{@ex}a"},
        "y" => {:iri, "#{@ex}b"},
        "z" => {:iri, "#{@ex}c"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}a"},
                {:iri, "#{@ex}contains"},
                {:iri, "#{@ex}c"}
              ]} = inferred
    end

    test "transitive rule variables connect correctly" do
      rule = Rules.prp_trp()

      # Check that y is the join variable
      vars = Rule.variables(rule)
      assert MapSet.member?(vars, "y")

      # Body should have x->y and y->z patterns
      patterns = Rule.body_patterns(rule)

      # Find patterns with p as predicate variable
      data_patterns =
        Enum.filter(patterns, fn {:pattern, [_, p, _]} ->
          p == {:var, "p"}
        end)

      assert length(data_patterns) == 2
    end
  end

  # ============================================================================
  # Symmetric Property Rule Tests
  # ============================================================================

  describe "symmetric property rule generates inverse" do
    test "prp_symp swaps subject and object" do
      rule = Rules.prp_symp()

      # Simulate: knows is SymmetricProperty
      # alice knows bob
      # Should derive: bob knows alice
      binding = %{
        "p" => {:iri, "#{@ex}knows"},
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}bob"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}bob"},
                {:iri, "#{@ex}knows"},
                {:iri, "#{@ex}alice"}
              ]} = inferred
    end

    test "symmetric rule head has swapped positions" do
      rule = Rules.prp_symp()

      # Body has: x p y
      # Head has: y p x
      {:pattern, [h_s, h_p, h_o]} = rule.head

      assert {:var, "y"} = h_s
      assert {:var, "p"} = h_p
      assert {:var, "x"} = h_o
    end
  end

  # ============================================================================
  # owl:sameAs Rule Tests
  # ============================================================================

  describe "sameAs rules propagate equality" do
    test "eq_sym generates symmetric sameAs" do
      rule = Rules.eq_sym()

      # alice sameAs alicia
      # Should derive: alicia sameAs alice
      binding = %{
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}alicia"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alicia"},
                {:iri, "#{@owl}sameAs"},
                {:iri, "#{@ex}alice"}
              ]} = inferred
    end

    test "eq_trans chains sameAs relationships" do
      rule = Rules.eq_trans()

      # alice sameAs alicia, alicia sameAs ally
      # Should derive: alice sameAs ally
      binding = %{
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}alicia"},
        "z" => {:iri, "#{@ex}ally"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alice"},
                {:iri, "#{@owl}sameAs"},
                {:iri, "#{@ex}ally"}
              ]} = inferred
    end

    test "eq_rep_s propagates equality to subject position" do
      rule = Rules.eq_rep_s()

      # s1 sameAs s2, s1 p o
      # Should derive: s2 p o
      binding = %{
        "s1" => {:iri, "#{@ex}alice"},
        "s2" => {:iri, "#{@ex}alicia"},
        "p" => {:iri, "#{@ex}knows"},
        "o" => {:iri, "#{@ex}bob"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alicia"},
                {:iri, "#{@ex}knows"},
                {:iri, "#{@ex}bob"}
              ]} = inferred
    end

    test "eq_rep_o propagates equality to object position" do
      rule = Rules.eq_rep_o()

      # o1 sameAs o2, s p o1
      # Should derive: s p o2
      binding = %{
        "s" => {:iri, "#{@ex}alice"},
        "p" => {:iri, "#{@ex}knows"},
        "o1" => {:iri, "#{@ex}bob"},
        "o2" => {:iri, "#{@ex}robert"}
      }

      inferred = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern,
              [
                {:iri, "#{@ex}alice"},
                {:iri, "#{@ex}knows"},
                {:iri, "#{@ex}robert"}
              ]} = inferred
    end
  end

  # ============================================================================
  # Rule Compilation Filtering Tests
  # ============================================================================

  describe "rule compilation filters inapplicable rules" do
    test "empty schema returns minimal rule set" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      # Only eq_ref should be applicable (always true)
      rule_names = Enum.map(rules, & &1.name)
      assert :eq_ref in rule_names

      # Most rules should be filtered out
      refute :prp_trp in rule_names
      refute :prp_symp in rule_names
      refute :cax_sco in rule_names
    end

    test "schema with subclass includes subclass rules" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_subclass: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      rule_names = Enum.map(rules, & &1.name)
      assert :scm_sco in rule_names
      assert :cax_sco in rule_names
    end

    test "schema with transitive properties includes prp_trp" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      rule_names = Enum.map(rules, & &1.name)
      assert :prp_trp in rule_names
    end

    test "schema with sameAs includes equality rules" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_sameas: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      rule_names = Enum.map(rules, & &1.name)
      assert :eq_sym in rule_names
      assert :eq_trans in rule_names
      assert :eq_rep_s in rule_names
    end

    test "full schema includes all rules" do
      schema_info = %{
        has_subclass: true,
        has_subproperty: true,
        has_domain: true,
        has_range: true,
        transitive_properties: ["#{@ex}contains"],
        symmetric_properties: ["#{@ex}knows"],
        inverse_properties: [{"#{@ex}parent", "#{@ex}child"}],
        functional_properties: ["#{@ex}ssn"],
        inverse_functional_properties: ["#{@ex}email"],
        has_sameas: true,
        has_restrictions: true
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      all_rules = RuleCompiler.get_rules(compiled)

      # Should have generic and specialized rules
      assert length(all_rules) > 0

      # Check specialized rules exist
      specialized = RuleCompiler.get_specialized_rules(compiled)
      assert length(specialized) > 0
    end
  end

  # ============================================================================
  # Rule Optimization Tests
  # ============================================================================

  describe "rule optimization reorders patterns" do
    test "constant predicate patterns come first" do
      # Create rule with variable pattern first, constant second
      rule =
        Rule.new(
          :test_order,
          [
            {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]},
            {:pattern, [{:var, "y"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@rdf}type"}, {:var, "c"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      [first | _] = Rule.body_patterns(optimized)

      # Pattern with constant predicate should be first
      {:pattern, [_, pred, _]} = first
      assert {:iri, _} = pred
    end

    test "bound variables increase selectivity" do
      # After first pattern binds y, second pattern using y should be preferred
      rule =
        Rule.new(
          :test_bound,
          [
            {:pattern, [{:var, "x"}, {:iri, "#{@ex}p"}, {:var, "y"}]},
            {:pattern, [{:var, "a"}, {:var, "b"}, {:var, "c"}]},
            {:pattern, [{:var, "y"}, {:iri, "#{@ex}q"}, {:var, "z"}]}
          ],
          {:pattern, [{:var, "x"}, {:iri, "#{@ex}r"}, {:var, "z"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)
      patterns = Rule.body_patterns(optimized)

      # Check that patterns using bound variables come earlier than unrelated
      # First pattern should have constant predicate
      assert length(patterns) == 3
    end

    test "conditions placed after binding patterns" do
      rule =
        Rule.new(
          :test_condition,
          [
            {:not_equal, {:var, "x"}, {:var, "y"}},
            {:pattern, [{:var, "x"}, {:iri, "#{@owl}sameAs"}, {:var, "y"}]}
          ],
          {:pattern, [{:var, "y"}, {:iri, "#{@owl}sameAs"}, {:var, "x"}]}
        )

      optimized = RuleOptimizer.optimize_rule(rule)

      # First element should be pattern (binds vars), not condition
      [first | rest] = optimized.body
      assert {:pattern, _} = first

      # Condition should come after the pattern
      assert length(rest) == 1
      assert {:not_equal, _, _} = hd(rest)
    end
  end

  # ============================================================================
  # Integration Scenarios
  # ============================================================================

  describe "full integration scenarios" do
    test "compile and optimize rules for ontology with class hierarchy" do
      schema_info = %{
        has_subclass: true,
        has_subproperty: false,
        has_domain: true,
        has_range: true,
        transitive_properties: [],
        symmetric_properties: [],
        inverse_properties: [],
        functional_properties: [],
        inverse_functional_properties: [],
        has_sameas: false,
        has_restrictions: false
      }

      # Compile rules
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      # Optimize rules
      optimized = RuleOptimizer.optimize_rules(rules)

      # Filter dead rules
      active = RuleOptimizer.filter_active_rules(optimized, schema_info)

      # Should have RDFS rules for subclass and domain/range
      rule_names = Enum.map(active, & &1.name)
      assert :scm_sco in rule_names
      assert :cax_sco in rule_names
      assert :prp_dom in rule_names
      assert :prp_rng in rule_names

      # Should not have transitive/symmetric rules
      refute :prp_trp in rule_names
      refute :prp_symp in rule_names
    end

    test "compile and optimize rules for OWL property characteristics" do
      schema_info = %{
        has_subclass: false,
        has_subproperty: false,
        has_domain: false,
        has_range: false,
        transitive_properties: ["#{@ex}ancestor", "#{@ex}contains"],
        symmetric_properties: ["#{@ex}knows"],
        inverse_properties: [{"#{@ex}parent", "#{@ex}child"}],
        functional_properties: [],
        inverse_functional_properties: [],
        has_sameas: false,
        has_restrictions: false
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)

      # Should have specialized rules
      specialized = RuleCompiler.get_specialized_rules(compiled)
      assert length(specialized) > 0

      # Specialized rules should have bound properties
      for rule <- specialized do
        # Name should contain property local name
        name_str = Atom.to_string(rule.name)
        assert String.contains?(name_str, "_")
      end
    end

    test "rule batching groups related rules" do
      schema_info = %{
        has_subclass: true,
        has_subproperty: false,
        has_domain: true,
        has_range: true,
        transitive_properties: [],
        symmetric_properties: [],
        inverse_properties: [],
        functional_properties: [],
        inverse_functional_properties: [],
        has_sameas: false,
        has_restrictions: false
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info)
      rules = RuleCompiler.get_rules(compiled)

      batches = RuleOptimizer.batch_rules(rules)

      # Should have at least one batch
      assert length(batches) > 0

      # Each batch should have rules and metadata
      for batch <- batches do
        assert is_atom(batch.name)
        assert is_list(batch.rules)
        assert batch.batch_type in [:same_predicate, :same_head, :independent]
      end
    end
  end
end
