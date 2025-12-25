defmodule TripleStore.Reasoner.RulesTest do
  @moduledoc """
  Tests for the OWL 2 RL rule definitions.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{Rule, Rules}

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"

  # ============================================================================
  # API Tests
  # ============================================================================

  describe "API functions" do
    test "rdfs_rules/0 returns RDFS rules" do
      rules = Rules.rdfs_rules()
      assert length(rules) == 6

      names = Enum.map(rules, & &1.name)
      assert :scm_sco in names
      assert :scm_spo in names
      assert :cax_sco in names
      assert :prp_spo1 in names
      assert :prp_dom in names
      assert :prp_rng in names
    end

    test "owl2rl_rules/0 returns OWL 2 RL rules" do
      rules = Rules.owl2rl_rules()
      # 6 property rules + 6 equality rules + 5 class restriction rules = 17
      assert length(rules) == 17
    end

    test "all_rules/0 returns all rules" do
      rules = Rules.all_rules()
      # 6 RDFS + 17 OWL 2 RL = 23
      assert length(rules) == 23
    end

    test "rules_for_profile/1 returns correct rules" do
      rdfs = Rules.rules_for_profile(:rdfs)
      assert length(rdfs) == 6

      owl2rl = Rules.rules_for_profile(:owl2rl)
      assert length(owl2rl) == 23

      all = Rules.rules_for_profile(:all)
      assert length(all) == 23
    end

    test "get_rule/1 returns rule by name" do
      rule = Rules.get_rule(:scm_sco)
      assert rule.name == :scm_sco
      assert rule.profile == :rdfs
    end

    test "get_rule/1 returns nil for unknown rule" do
      assert Rules.get_rule(:unknown_rule) == nil
    end

    test "rule_names/0 returns all rule names" do
      names = Rules.rule_names()
      assert length(names) == 23
      assert :scm_sco in names
      assert :prp_trp in names
      assert :eq_trans in names
      assert :cls_avf in names
    end
  end

  # ============================================================================
  # RDFS Schema Rule Tests
  # ============================================================================

  describe "scm-sco: rdfs:subClassOf transitivity" do
    test "has correct structure" do
      rule = Rules.scm_sco()

      assert rule.name == :scm_sco
      assert rule.profile == :rdfs
      assert rule.description =~ "subClassOf"
      assert Rule.pattern_count(rule) == 2
      assert Rule.condition_count(rule) == 0
      assert Rule.safe?(rule)
    end

    test "has correct body patterns" do
      rule = Rules.scm_sco()
      [p1, p2] = Rule.body_patterns(rule)

      # (?c1 rdfs:subClassOf ?c2)
      assert {:pattern, [{:var, "c1"}, {:iri, subClassOf}, {:var, "c2"}]} = p1
      assert subClassOf == "#{@rdfs}subClassOf"

      # (?c2 rdfs:subClassOf ?c3)
      assert {:pattern, [{:var, "c2"}, {:iri, ^subClassOf}, {:var, "c3"}]} = p2
    end

    test "has correct head pattern" do
      rule = Rules.scm_sco()

      # (?c1 rdfs:subClassOf ?c3)
      assert {:pattern, [{:var, "c1"}, {:iri, subClassOf}, {:var, "c3"}]} = rule.head
      assert subClassOf == "#{@rdfs}subClassOf"
    end
  end

  describe "scm-spo: rdfs:subPropertyOf transitivity" do
    test "has correct structure" do
      rule = Rules.scm_spo()

      assert rule.name == :scm_spo
      assert rule.profile == :rdfs
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end
  end

  describe "cax-sco: class membership through subclass" do
    test "has correct structure" do
      rule = Rules.cax_sco()

      assert rule.name == :cax_sco
      assert rule.profile == :rdfs
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "derives type from subclass relationship" do
      rule = Rules.cax_sco()
      [p1, p2] = Rule.body_patterns(rule)

      # (?x rdf:type ?c1)
      assert {:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "c1"}]} = p1
      assert rdf_type == "#{@rdf}type"

      # (?c1 rdfs:subClassOf ?c2)
      assert {:pattern, [{:var, "c1"}, {:iri, _}, {:var, "c2"}]} = p2

      # Head: (?x rdf:type ?c2)
      assert {:pattern, [{:var, "x"}, {:iri, ^rdf_type}, {:var, "c2"}]} = rule.head
    end
  end

  describe "prp-spo1: property inheritance" do
    test "has correct structure" do
      rule = Rules.prp_spo1()

      assert rule.name == :prp_spo1
      assert rule.profile == :rdfs
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end
  end

  describe "prp-dom: property domain" do
    test "has correct structure" do
      rule = Rules.prp_dom()

      assert rule.name == :prp_dom
      assert rule.profile == :rdfs
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "derives type from domain" do
      rule = Rules.prp_dom()

      # Head should infer x rdf:type c
      assert {:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "c"}]} = rule.head
      assert rdf_type == "#{@rdf}type"
    end
  end

  describe "prp-rng: property range" do
    test "has correct structure" do
      rule = Rules.prp_rng()

      assert rule.name == :prp_rng
      assert rule.profile == :rdfs
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "derives type from range" do
      rule = Rules.prp_rng()

      # Head should infer y rdf:type c (object gets the type)
      assert {:pattern, [{:var, "y"}, {:iri, rdf_type}, {:var, "c"}]} = rule.head
      assert rdf_type == "#{@rdf}type"
    end
  end

  # ============================================================================
  # OWL 2 RL Property Rule Tests
  # ============================================================================

  describe "prp-trp: transitive property" do
    test "has correct structure" do
      rule = Rules.prp_trp()

      assert rule.name == :prp_trp
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.condition_count(rule) == 0
      assert Rule.safe?(rule)
    end

    test "requires TransitiveProperty declaration" do
      rule = Rules.prp_trp()
      [p1 | _] = Rule.body_patterns(rule)

      assert {:pattern, [{:var, "p"}, {:iri, rdf_type}, {:iri, trans}]} = p1
      assert rdf_type == "#{@rdf}type"
      assert trans == "#{@owl}TransitiveProperty"
    end

    test "chains through intermediate" do
      rule = Rules.prp_trp()
      patterns = Rule.body_patterns(rule)

      # Should have x->y and y->z patterns
      vars = Rule.variables(rule)
      assert MapSet.member?(vars, "x")
      assert MapSet.member?(vars, "y")
      assert MapSet.member?(vars, "z")
      assert MapSet.member?(vars, "p")

      # Head: x p z
      assert {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "z"}]} = rule.head
    end
  end

  describe "prp-symp: symmetric property" do
    test "has correct structure" do
      rule = Rules.prp_symp()

      assert rule.name == :prp_symp
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "swaps subject and object" do
      rule = Rules.prp_symp()

      # Head should swap x and y
      assert {:pattern, [{:var, "y"}, {:var, "p"}, {:var, "x"}]} = rule.head
    end
  end

  describe "prp-inv1: inverse property (forward)" do
    test "has correct structure" do
      rule = Rules.prp_inv1()

      assert rule.name == :prp_inv1
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "uses p1 in body, p2 in head" do
      rule = Rules.prp_inv1()
      [inv_pattern, data_pattern] = Rule.body_patterns(rule)

      # p1 owl:inverseOf p2
      assert {:pattern, [{:var, "p1"}, {:iri, inverseOf}, {:var, "p2"}]} = inv_pattern
      assert inverseOf == "#{@owl}inverseOf"

      # x p1 y
      assert {:pattern, [{:var, "x"}, {:var, "p1"}, {:var, "y"}]} = data_pattern

      # Head: y p2 x
      assert {:pattern, [{:var, "y"}, {:var, "p2"}, {:var, "x"}]} = rule.head
    end
  end

  describe "prp-inv2: inverse property (backward)" do
    test "has correct structure" do
      rule = Rules.prp_inv2()

      assert rule.name == :prp_inv2
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "uses p2 in body, p1 in head" do
      rule = Rules.prp_inv2()

      # Head: y p1 x
      assert {:pattern, [{:var, "y"}, {:var, "p1"}, {:var, "x"}]} = rule.head
    end
  end

  describe "prp-fp: functional property" do
    test "has correct structure" do
      rule = Rules.prp_fp()

      assert rule.name == :prp_fp
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.condition_count(rule) == 1
      assert Rule.safe?(rule)
    end

    test "has not_equal condition" do
      rule = Rules.prp_fp()
      [cond] = Rule.body_conditions(rule)

      assert {:not_equal, {:var, "y1"}, {:var, "y2"}} = cond
    end

    test "derives sameAs" do
      rule = Rules.prp_fp()

      assert {:pattern, [{:var, "y1"}, {:iri, sameAs}, {:var, "y2"}]} = rule.head
      assert sameAs == "#{@owl}sameAs"
    end
  end

  describe "prp-ifp: inverse functional property" do
    test "has correct structure" do
      rule = Rules.prp_ifp()

      assert rule.name == :prp_ifp
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.condition_count(rule) == 1
      assert Rule.safe?(rule)
    end

    test "has not_equal condition" do
      rule = Rules.prp_ifp()
      [cond] = Rule.body_conditions(rule)

      assert {:not_equal, {:var, "x1"}, {:var, "x2"}} = cond
    end

    test "derives sameAs" do
      rule = Rules.prp_ifp()

      assert {:pattern, [{:var, "x1"}, {:iri, sameAs}, {:var, "x2"}]} = rule.head
      assert sameAs == "#{@owl}sameAs"
    end
  end

  # ============================================================================
  # OWL 2 RL Equality Rule Tests
  # ============================================================================

  describe "eq-ref: owl:sameAs reflexivity" do
    test "has correct structure" do
      rule = Rules.eq_ref()

      assert rule.name == :eq_ref
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 1
      # Note: head derives x sameAs x (reflexive)
    end
  end

  describe "eq-sym: owl:sameAs symmetry" do
    test "has correct structure" do
      rule = Rules.eq_sym()

      assert rule.name == :eq_sym
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 1
      assert Rule.safe?(rule)
    end

    test "swaps subject and object" do
      rule = Rules.eq_sym()
      [p] = Rule.body_patterns(rule)

      # x owl:sameAs y
      assert {:pattern, [{:var, "x"}, {:iri, sameAs}, {:var, "y"}]} = p
      assert sameAs == "#{@owl}sameAs"

      # Head: y owl:sameAs x
      assert {:pattern, [{:var, "y"}, {:iri, ^sameAs}, {:var, "x"}]} = rule.head
    end
  end

  describe "eq-trans: owl:sameAs transitivity" do
    test "has correct structure" do
      rule = Rules.eq_trans()

      assert rule.name == :eq_trans
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "chains through intermediate" do
      rule = Rules.eq_trans()

      # Head: x owl:sameAs z
      assert {:pattern, [{:var, "x"}, {:iri, sameAs}, {:var, "z"}]} = rule.head
      assert sameAs == "#{@owl}sameAs"
    end
  end

  describe "eq-rep-s: equality replacement in subject" do
    test "has correct structure" do
      rule = Rules.eq_rep_s()

      assert rule.name == :eq_rep_s
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "replaces subject with equivalent" do
      rule = Rules.eq_rep_s()

      # Head: s2 p o
      assert {:pattern, [{:var, "s2"}, {:var, "p"}, {:var, "o"}]} = rule.head
    end
  end

  describe "eq-rep-p: equality replacement in predicate" do
    test "has correct structure" do
      rule = Rules.eq_rep_p()

      assert rule.name == :eq_rep_p
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end
  end

  describe "eq-rep-o: equality replacement in object" do
    test "has correct structure" do
      rule = Rules.eq_rep_o()

      assert rule.name == :eq_rep_o
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule)
    end

    test "replaces object with equivalent" do
      rule = Rules.eq_rep_o()

      # Head: s p o2
      assert {:pattern, [{:var, "s"}, {:var, "p"}, {:var, "o2"}]} = rule.head
    end
  end

  # ============================================================================
  # OWL 2 RL Class Restriction Rule Tests
  # ============================================================================

  describe "cls-hv1: hasValue restriction (property inference)" do
    test "has correct structure" do
      rule = Rules.cls_hv1()

      assert rule.name == :cls_hv1
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.condition_count(rule) == 0
      assert Rule.safe?(rule)
    end

    test "requires class membership and restriction definition" do
      rule = Rules.cls_hv1()
      patterns = Rule.body_patterns(rule)

      # Should have patterns for:
      # 1. x rdf:type c
      # 2. c owl:hasValue v
      # 3. c owl:onProperty p
      assert length(patterns) == 3

      predicates = Enum.map(patterns, fn {:pattern, [_, {:iri, p}, _]} -> p end)
      assert "#{@rdf}type" in predicates
      assert "#{@owl}hasValue" in predicates
      assert "#{@owl}onProperty" in predicates
    end

    test "derives property value" do
      rule = Rules.cls_hv1()

      # Head: x p v
      assert {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "v"}]} = rule.head
    end
  end

  describe "cls-hv2: hasValue restriction (class inference)" do
    test "has correct structure" do
      rule = Rules.cls_hv2()

      assert rule.name == :cls_hv2
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.safe?(rule)
    end

    test "derives class membership" do
      rule = Rules.cls_hv2()

      # Head: x rdf:type c
      assert {:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "c"}]} = rule.head
      assert rdf_type == "#{@rdf}type"
    end
  end

  describe "cls-svf1: someValuesFrom restriction" do
    test "has correct structure" do
      rule = Rules.cls_svf1()

      assert rule.name == :cls_svf1
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 4
      assert Rule.safe?(rule)
    end

    test "requires typed value and restriction definition" do
      rule = Rules.cls_svf1()
      patterns = Rule.body_patterns(rule)

      # Should have patterns for:
      # 1. x p y
      # 2. y rdf:type c
      # 3. r owl:someValuesFrom c
      # 4. r owl:onProperty p
      assert length(patterns) == 4

      predicates = Enum.map(patterns, fn {:pattern, [_, p, _]} ->
        case p do
          {:iri, iri} -> iri
          {:var, _} -> :variable
        end
      end)

      assert "#{@rdf}type" in predicates
      assert "#{@owl}someValuesFrom" in predicates
      assert "#{@owl}onProperty" in predicates
    end

    test "derives restriction class membership" do
      rule = Rules.cls_svf1()

      # Head: x rdf:type r
      assert {:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "r"}]} = rule.head
      assert rdf_type == "#{@rdf}type"
    end
  end

  describe "cls-svf2: someValuesFrom owl:Thing" do
    test "has correct structure" do
      rule = Rules.cls_svf2()

      assert rule.name == :cls_svf2
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 3
      assert Rule.safe?(rule)
    end

    test "uses owl:Thing as filler" do
      rule = Rules.cls_svf2()
      patterns = Rule.body_patterns(rule)

      # Find the someValuesFrom pattern
      svf_pattern = Enum.find(patterns, fn {:pattern, [_, p, _]} ->
        case p do
          {:iri, iri} -> iri == "#{@owl}someValuesFrom"
          _ -> false
        end
      end)

      assert {:pattern, [_, _, {:iri, owl_thing}]} = svf_pattern
      assert owl_thing == "#{@owl}Thing"
    end
  end

  describe "cls-avf: allValuesFrom restriction" do
    test "has correct structure" do
      rule = Rules.cls_avf()

      assert rule.name == :cls_avf
      assert rule.profile == :owl2rl
      assert Rule.pattern_count(rule) == 4
      assert Rule.safe?(rule)
    end

    test "requires class membership and restriction definition" do
      rule = Rules.cls_avf()
      patterns = Rule.body_patterns(rule)

      # Should have patterns for:
      # 1. x rdf:type r
      # 2. x p y
      # 3. r owl:allValuesFrom c
      # 4. r owl:onProperty p
      assert length(patterns) == 4
    end

    test "derives value type" do
      rule = Rules.cls_avf()

      # Head: y rdf:type c
      assert {:pattern, [{:var, "y"}, {:iri, rdf_type}, {:var, "c"}]} = rule.head
      assert rdf_type == "#{@rdf}type"
    end
  end

  # ============================================================================
  # Rule Validation Tests
  # ============================================================================

  describe "all rules are valid" do
    test "all rules are safe (head variables in body)" do
      for rule <- Rules.all_rules() do
        assert Rule.safe?(rule),
               "Rule #{rule.name} is not safe: head variables not all in body"
      end
    end

    test "all rules have descriptions" do
      for rule <- Rules.all_rules() do
        assert rule.description != nil,
               "Rule #{rule.name} has no description"
      end
    end

    test "all rules have profiles" do
      for rule <- Rules.all_rules() do
        assert rule.profile in [:rdfs, :owl2rl],
               "Rule #{rule.name} has invalid profile: #{inspect(rule.profile)}"
      end
    end

    test "all rules have unique names" do
      names = Rules.rule_names()
      unique_names = Enum.uniq(names)

      assert length(names) == length(unique_names),
             "Duplicate rule names found"
    end

    test "all rules have at least one body pattern" do
      for rule <- Rules.all_rules() do
        assert Rule.pattern_count(rule) >= 1,
               "Rule #{rule.name} has no body patterns"
      end
    end
  end

  # ============================================================================
  # Substitution Integration Tests
  # ============================================================================

  describe "rule instantiation" do
    test "cax_sco can be instantiated with binding" do
      rule = Rules.cax_sco()

      binding = %{
        "x" => {:iri, "http://example.org/alice"},
        "c1" => {:iri, "http://example.org/Person"},
        "c2" => {:iri, "http://example.org/Agent"}
      }

      head = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern, [
        {:iri, "http://example.org/alice"},
        {:iri, "#{@rdf}type"},
        {:iri, "http://example.org/Agent"}
      ]} = head

      assert Rule.ground?(head)
    end

    test "prp_trp can be instantiated with binding" do
      rule = Rules.prp_trp()

      binding = %{
        "p" => {:iri, "http://example.org/contains"},
        "x" => {:iri, "http://example.org/a"},
        "y" => {:iri, "http://example.org/b"},
        "z" => {:iri, "http://example.org/c"}
      }

      head = Rule.substitute_pattern(rule.head, binding)

      assert {:pattern, [
        {:iri, "http://example.org/a"},
        {:iri, "http://example.org/contains"},
        {:iri, "http://example.org/c"}
      ]} = head

      assert Rule.ground?(head)
    end

    test "prp_fp condition filters equal values" do
      rule = Rules.prp_fp()

      # When y1 == y2, condition should fail
      binding_same = %{
        "y1" => {:iri, "http://example.org/val"},
        "y2" => {:iri, "http://example.org/val"}
      }

      # When y1 != y2, condition should pass
      binding_diff = %{
        "y1" => {:iri, "http://example.org/val1"},
        "y2" => {:iri, "http://example.org/val2"}
      }

      assert Rule.evaluate_conditions(rule, binding_same) == false
      assert Rule.evaluate_conditions(rule, binding_diff) == true
    end
  end
end
