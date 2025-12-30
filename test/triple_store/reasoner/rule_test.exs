defmodule TripleStore.Reasoner.RuleTest do
  @moduledoc """
  Tests for the Rule struct and rule representation.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.Rule

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Term Constructor Tests
  # ============================================================================

  describe "term constructors" do
    test "var/1 creates a variable term" do
      assert Rule.var("x") == {:var, "x"}
      assert Rule.var("subject") == {:var, "subject"}
    end

    test "iri/1 creates an IRI term" do
      assert Rule.iri("#{@ex}Person") == {:iri, "#{@ex}Person"}
    end

    test "literal/1 creates a simple literal" do
      assert Rule.literal("hello") == {:literal, :simple, "hello"}
    end

    test "literal/2 creates a typed literal" do
      xsd_integer = "http://www.w3.org/2001/XMLSchema#integer"
      assert Rule.literal("42", xsd_integer) == {:literal, :typed, "42", xsd_integer}
    end

    test "lang_literal/2 creates a language-tagged literal" do
      assert Rule.lang_literal("hello", "en") == {:literal, :lang, "hello", "en"}
    end
  end

  # ============================================================================
  # Pattern Constructor Tests
  # ============================================================================

  describe "pattern constructor" do
    test "pattern/3 creates a triple pattern" do
      s = Rule.var("x")
      p = Rule.iri("#{@rdf}type")
      o = Rule.var("class")

      assert Rule.pattern(s, p, o) == {:pattern, [s, p, o]}
    end

    test "pattern can mix variables and constants" do
      pattern =
        Rule.pattern(
          Rule.var("x"),
          Rule.iri("#{@rdf}type"),
          Rule.iri("#{@ex}Person")
        )

      assert {:pattern,
              [
                {:var, "x"},
                {:iri, "#{@rdf}type"},
                {:iri, "#{@ex}Person"}
              ]} = pattern
    end
  end

  # ============================================================================
  # Condition Constructor Tests
  # ============================================================================

  describe "condition constructors" do
    test "not_equal/2 creates a not-equal condition" do
      cond = Rule.not_equal(Rule.var("x"), Rule.var("y"))
      assert cond == {:not_equal, {:var, "x"}, {:var, "y"}}
    end

    test "is_iri/1 creates an is-IRI condition" do
      cond = Rule.is_iri(Rule.var("x"))
      assert cond == {:is_iri, {:var, "x"}}
    end

    test "is_blank/1 creates an is-blank condition" do
      cond = Rule.is_blank(Rule.var("x"))
      assert cond == {:is_blank, {:var, "x"}}
    end

    test "is_literal/1 creates an is-literal condition" do
      cond = Rule.is_literal(Rule.var("x"))
      assert cond == {:is_literal, {:var, "x"}}
    end

    test "bound/1 creates a bound condition" do
      cond = Rule.bound(Rule.var("x"))
      assert cond == {:bound, {:var, "x"}}
    end
  end

  # ============================================================================
  # Common IRI Tests
  # ============================================================================

  describe "common IRI helpers" do
    test "rdf_type returns rdf:type IRI" do
      assert Rule.rdf_type() == {:iri, "#{@rdf}type"}
    end

    test "rdfs_subClassOf returns rdfs:subClassOf IRI" do
      assert Rule.rdfs_subClassOf() == {:iri, "#{@rdfs}subClassOf"}
    end

    test "rdfs_subPropertyOf returns rdfs:subPropertyOf IRI" do
      assert Rule.rdfs_subPropertyOf() == {:iri, "#{@rdfs}subPropertyOf"}
    end

    test "rdfs_domain returns rdfs:domain IRI" do
      assert Rule.rdfs_domain() == {:iri, "#{@rdfs}domain"}
    end

    test "rdfs_range returns rdfs:range IRI" do
      assert Rule.rdfs_range() == {:iri, "#{@rdfs}range"}
    end

    test "owl_sameAs returns owl:sameAs IRI" do
      assert Rule.owl_sameAs() == {:iri, "#{@owl}sameAs"}
    end

    test "owl_TransitiveProperty returns owl:TransitiveProperty IRI" do
      assert Rule.owl_TransitiveProperty() == {:iri, "#{@owl}TransitiveProperty"}
    end

    test "owl_SymmetricProperty returns owl:SymmetricProperty IRI" do
      assert Rule.owl_SymmetricProperty() == {:iri, "#{@owl}SymmetricProperty"}
    end

    test "owl_inverseOf returns owl:inverseOf IRI" do
      assert Rule.owl_inverseOf() == {:iri, "#{@owl}inverseOf"}
    end
  end

  # ============================================================================
  # Rule Constructor Tests
  # ============================================================================

  describe "Rule.new/4" do
    test "creates a rule with required fields" do
      body = [
        Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c1")),
        Rule.pattern(Rule.var("c1"), Rule.rdfs_subClassOf(), Rule.var("c2"))
      ]

      head = Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c2"))

      rule = Rule.new(:cax_sco, body, head)

      assert rule.name == :cax_sco
      assert rule.body == body
      assert rule.head == head
      assert rule.description == nil
      assert rule.profile == nil
    end

    test "creates a rule with optional fields" do
      body = [Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))]
      head = Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("x"))

      rule =
        Rule.new(:prp_symp, body, head,
          description: "Symmetric property inference",
          profile: :owl2rl
        )

      assert rule.name == :prp_symp
      assert rule.description == "Symmetric property inference"
      assert rule.profile == :owl2rl
    end
  end

  # ============================================================================
  # Variable Extraction Tests
  # ============================================================================

  describe "variables/1" do
    test "extracts all variables from body and head" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("z"))
        )

      vars = Rule.variables(rule)

      assert MapSet.equal?(vars, MapSet.new(["x", "y", "z"]))
    end

    test "extracts variables from conditions" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.not_equal(Rule.var("x"), Rule.var("z"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      vars = Rule.variables(rule)

      assert MapSet.member?(vars, "x")
      assert MapSet.member?(vars, "y")
      assert MapSet.member?(vars, "z")
    end

    test "deduplicates variables" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("x"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}r"), Rule.var("y"))
        )

      vars = Rule.variables(rule)

      assert MapSet.size(vars) == 2
      assert MapSet.equal?(vars, MapSet.new(["x", "y"]))
    end
  end

  describe "body_variables/1" do
    test "extracts variables from body only" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("z"))
        )

      vars = Rule.body_variables(rule)

      assert MapSet.equal?(vars, MapSet.new(["x", "y"]))
    end
  end

  describe "head_variables/1" do
    test "extracts variables from head only" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("z"))
        )

      vars = Rule.head_variables(rule)

      assert MapSet.equal?(vars, MapSet.new(["y", "z"]))
    end
  end

  # ============================================================================
  # Analysis Function Tests
  # ============================================================================

  describe "pattern_count/1" do
    test "counts patterns in body" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("z")),
            Rule.not_equal(Rule.var("x"), Rule.var("z"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}r"), Rule.var("z"))
        )

      assert Rule.pattern_count(rule) == 2
    end
  end

  describe "condition_count/1" do
    test "counts conditions in body" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.not_equal(Rule.var("x"), Rule.var("y")),
            Rule.is_iri(Rule.var("x"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      assert Rule.condition_count(rule) == 2
    end
  end

  describe "safe?/1" do
    test "returns true when all head variables are in body" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      assert Rule.safe?(rule) == true
    end

    test "returns false when head has unbound variables" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("z"))
        )

      assert Rule.safe?(rule) == false
    end
  end

  describe "body_patterns/1" do
    test "returns only patterns from body" do
      p1 = Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))
      p2 = Rule.pattern(Rule.var("y"), Rule.iri("#{@ex}q"), Rule.var("z"))
      c1 = Rule.not_equal(Rule.var("x"), Rule.var("z"))

      rule =
        Rule.new(
          :test,
          [p1, c1, p2],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}r"), Rule.var("z"))
        )

      patterns = Rule.body_patterns(rule)

      assert patterns == [p1, p2]
    end
  end

  describe "body_conditions/1" do
    test "returns only conditions from body" do
      p1 = Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))
      c1 = Rule.not_equal(Rule.var("x"), Rule.var("y"))
      c2 = Rule.is_iri(Rule.var("x"))

      rule =
        Rule.new(
          :test,
          [p1, c1, c2],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      conditions = Rule.body_conditions(rule)

      assert conditions == [c1, c2]
    end
  end

  # ============================================================================
  # Substitution Tests
  # ============================================================================

  describe "substitute/2" do
    test "substitutes a bound variable" do
      binding = %{"x" => {:iri, "#{@ex}alice"}}

      result = Rule.substitute({:var, "x"}, binding)

      assert result == {:iri, "#{@ex}alice"}
    end

    test "returns variable unchanged when not bound" do
      binding = %{"y" => {:iri, "#{@ex}bob"}}

      result = Rule.substitute({:var, "x"}, binding)

      assert result == {:var, "x"}
    end

    test "returns constant unchanged" do
      binding = %{"x" => {:iri, "#{@ex}alice"}}

      result = Rule.substitute({:iri, "#{@ex}Person"}, binding)

      assert result == {:iri, "#{@ex}Person"}
    end
  end

  describe "substitute_pattern/2" do
    test "substitutes all variables in pattern" do
      binding = %{
        "x" => {:iri, "#{@ex}alice"},
        "y" => {:iri, "#{@ex}bob"}
      }

      pattern = Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}knows"), Rule.var("y"))
      result = Rule.substitute_pattern(pattern, binding)

      assert result ==
               {:pattern,
                [
                  {:iri, "#{@ex}alice"},
                  {:iri, "#{@ex}knows"},
                  {:iri, "#{@ex}bob"}
                ]}
    end

    test "leaves unbound variables in pattern" do
      binding = %{"x" => {:iri, "#{@ex}alice"}}

      pattern = Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}knows"), Rule.var("y"))
      result = Rule.substitute_pattern(pattern, binding)

      assert result ==
               {:pattern,
                [
                  {:iri, "#{@ex}alice"},
                  {:iri, "#{@ex}knows"},
                  {:var, "y"}
                ]}
    end
  end

  # ============================================================================
  # Ground Check Tests
  # ============================================================================

  describe "ground?/1" do
    test "returns true for fully ground pattern" do
      pattern =
        Rule.pattern(
          Rule.iri("#{@ex}alice"),
          Rule.iri("#{@ex}knows"),
          Rule.iri("#{@ex}bob")
        )

      assert Rule.ground?(pattern) == true
    end

    test "returns false for pattern with variables" do
      pattern =
        Rule.pattern(
          Rule.iri("#{@ex}alice"),
          Rule.iri("#{@ex}knows"),
          Rule.var("y")
        )

      assert Rule.ground?(pattern) == false
    end
  end

  # ============================================================================
  # Condition Evaluation Tests
  # ============================================================================

  describe "evaluate_condition/2" do
    test "not_equal returns true for different values" do
      binding = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}b"}}
      cond = Rule.not_equal(Rule.var("x"), Rule.var("y"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "not_equal returns false for equal values" do
      binding = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}a"}}
      cond = Rule.not_equal(Rule.var("x"), Rule.var("y"))

      assert Rule.evaluate_condition(cond, binding) == false
    end

    test "is_iri returns true for IRI" do
      binding = %{"x" => {:iri, "#{@ex}a"}}
      cond = Rule.is_iri(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "is_iri returns false for literal" do
      binding = %{"x" => {:literal, :simple, "hello"}}
      cond = Rule.is_iri(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == false
    end

    test "is_blank returns true for blank node" do
      binding = %{"x" => {:blank_node, "b1"}}
      cond = Rule.is_blank(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "is_blank returns false for IRI" do
      binding = %{"x" => {:iri, "#{@ex}a"}}
      cond = Rule.is_blank(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == false
    end

    test "is_literal returns true for simple literal" do
      binding = %{"x" => {:literal, :simple, "hello"}}
      cond = Rule.is_literal(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "is_literal returns true for typed literal" do
      binding = %{"x" => {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"}}
      cond = Rule.is_literal(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "is_literal returns true for lang literal" do
      binding = %{"x" => {:literal, :lang, "hello", "en"}}
      cond = Rule.is_literal(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "is_literal returns false for IRI" do
      binding = %{"x" => {:iri, "#{@ex}a"}}
      cond = Rule.is_literal(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == false
    end

    test "bound returns true for bound variable" do
      binding = %{"x" => {:iri, "#{@ex}a"}}
      cond = Rule.bound(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == true
    end

    test "bound returns false for unbound variable" do
      binding = %{"y" => {:iri, "#{@ex}a"}}
      cond = Rule.bound(Rule.var("x"))

      assert Rule.evaluate_condition(cond, binding) == false
    end

    test "bound returns true for constant" do
      binding = %{}
      cond = Rule.bound(Rule.iri("#{@ex}a"))

      assert Rule.evaluate_condition(cond, binding) == true
    end
  end

  describe "evaluate_conditions/2" do
    test "returns true when all conditions pass" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.not_equal(Rule.var("x"), Rule.var("y")),
            Rule.is_iri(Rule.var("x"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      binding = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}b"}}

      assert Rule.evaluate_conditions(rule, binding) == true
    end

    test "returns false when any condition fails" do
      rule =
        Rule.new(
          :test,
          [
            Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y")),
            Rule.not_equal(Rule.var("x"), Rule.var("y")),
            # This will fail
            Rule.is_literal(Rule.var("x"))
          ],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      binding = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}b"}}

      assert Rule.evaluate_conditions(rule, binding) == false
    end

    test "returns true when no conditions" do
      rule =
        Rule.new(
          :test,
          [Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}p"), Rule.var("y"))],
          Rule.pattern(Rule.var("x"), Rule.iri("#{@ex}q"), Rule.var("y"))
        )

      binding = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}b"}}

      assert Rule.evaluate_conditions(rule, binding) == true
    end
  end

  # ============================================================================
  # Real OWL 2 RL Rule Examples
  # ============================================================================

  describe "OWL 2 RL rule examples" do
    test "cax-sco: class membership through subclass" do
      # (?x rdf:type ?c1), (?c1 rdfs:subClassOf ?c2) -> (?x rdf:type ?c2)
      rule =
        Rule.new(
          :cax_sco,
          [
            Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c1")),
            Rule.pattern(Rule.var("c1"), Rule.rdfs_subClassOf(), Rule.var("c2"))
          ],
          Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c2")),
          description: "Class membership through subclass",
          profile: :owl2rl
        )

      assert rule.name == :cax_sco
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
      assert MapSet.equal?(Rule.variables(rule), MapSet.new(["x", "c1", "c2"]))
    end

    test "scm-sco: subClassOf transitivity" do
      # (?c1 rdfs:subClassOf ?c2), (?c2 rdfs:subClassOf ?c3) -> (?c1 rdfs:subClassOf ?c3)
      rule =
        Rule.new(
          :scm_sco,
          [
            Rule.pattern(Rule.var("c1"), Rule.rdfs_subClassOf(), Rule.var("c2")),
            Rule.pattern(Rule.var("c2"), Rule.rdfs_subClassOf(), Rule.var("c3"))
          ],
          Rule.pattern(Rule.var("c1"), Rule.rdfs_subClassOf(), Rule.var("c3")),
          description: "rdfs:subClassOf transitivity",
          profile: :rdfs
        )

      assert rule.name == :scm_sco
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "prp-trp: transitive property" do
      # (?p rdf:type owl:TransitiveProperty), (?x ?p ?y), (?y ?p ?z) -> (?x ?p ?z)
      rule =
        Rule.new(
          :prp_trp,
          [
            Rule.pattern(Rule.var("p"), Rule.rdf_type(), Rule.owl_TransitiveProperty()),
            Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
            Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("z"))
          ],
          Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("z")),
          description: "Transitive property inference",
          profile: :owl2rl
        )

      assert rule.name == :prp_trp
      assert Rule.pattern_count(rule) == 3
      assert Rule.safe?(rule) == true
    end

    test "prp-symp: symmetric property" do
      # (?p rdf:type owl:SymmetricProperty), (?x ?p ?y) -> (?y ?p ?x)
      rule =
        Rule.new(
          :prp_symp,
          [
            Rule.pattern(Rule.var("p"), Rule.rdf_type(), Rule.owl_SymmetricProperty()),
            Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("x")),
          description: "Symmetric property inference",
          profile: :owl2rl
        )

      assert rule.name == :prp_symp
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "prp-dom: property domain" do
      # (?p rdfs:domain ?c), (?x ?p ?y) -> (?x rdf:type ?c)
      rule =
        Rule.new(
          :prp_dom,
          [
            Rule.pattern(Rule.var("p"), Rule.rdfs_domain(), Rule.var("c")),
            Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("x"), Rule.rdf_type(), Rule.var("c")),
          description: "Property domain inference",
          profile: :rdfs
        )

      assert rule.name == :prp_dom
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "prp-rng: property range" do
      # (?p rdfs:range ?c), (?x ?p ?y) -> (?y rdf:type ?c)
      rule =
        Rule.new(
          :prp_rng,
          [
            Rule.pattern(Rule.var("p"), Rule.rdfs_range(), Rule.var("c")),
            Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("y"), Rule.rdf_type(), Rule.var("c")),
          description: "Property range inference",
          profile: :rdfs
        )

      assert rule.name == :prp_rng
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "eq-sym: owl:sameAs symmetry" do
      # (?x owl:sameAs ?y) -> (?y owl:sameAs ?x)
      rule =
        Rule.new(
          :eq_sym,
          [Rule.pattern(Rule.var("x"), Rule.owl_sameAs(), Rule.var("y"))],
          Rule.pattern(Rule.var("y"), Rule.owl_sameAs(), Rule.var("x")),
          description: "owl:sameAs symmetry",
          profile: :owl2rl
        )

      assert rule.name == :eq_sym
      assert Rule.pattern_count(rule) == 1
      assert Rule.safe?(rule) == true
    end

    test "eq-trans: owl:sameAs transitivity" do
      # (?x owl:sameAs ?y), (?y owl:sameAs ?z) -> (?x owl:sameAs ?z)
      rule =
        Rule.new(
          :eq_trans,
          [
            Rule.pattern(Rule.var("x"), Rule.owl_sameAs(), Rule.var("y")),
            Rule.pattern(Rule.var("y"), Rule.owl_sameAs(), Rule.var("z"))
          ],
          Rule.pattern(Rule.var("x"), Rule.owl_sameAs(), Rule.var("z")),
          description: "owl:sameAs transitivity",
          profile: :owl2rl
        )

      assert rule.name == :eq_trans
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "prp-inv1: inverse property (forward)" do
      # (?p1 owl:inverseOf ?p2), (?x ?p1 ?y) -> (?y ?p2 ?x)
      rule =
        Rule.new(
          :prp_inv1,
          [
            Rule.pattern(Rule.var("p1"), Rule.owl_inverseOf(), Rule.var("p2")),
            Rule.pattern(Rule.var("x"), Rule.var("p1"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("y"), Rule.var("p2"), Rule.var("x")),
          description: "Inverse property inference (forward)",
          profile: :owl2rl
        )

      assert rule.name == :prp_inv1
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "prp-inv2: inverse property (backward)" do
      # (?p1 owl:inverseOf ?p2), (?x ?p2 ?y) -> (?y ?p1 ?x)
      rule =
        Rule.new(
          :prp_inv2,
          [
            Rule.pattern(Rule.var("p1"), Rule.owl_inverseOf(), Rule.var("p2")),
            Rule.pattern(Rule.var("x"), Rule.var("p2"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("y"), Rule.var("p1"), Rule.var("x")),
          description: "Inverse property inference (backward)",
          profile: :owl2rl
        )

      assert rule.name == :prp_inv2
      assert Rule.pattern_count(rule) == 2
      assert Rule.safe?(rule) == true
    end

    test "rule with not_equal condition" do
      # Example: infer different-from when sameAs leads to contradiction
      # (?x owl:sameAs ?y), ?x != ?y -> track for inconsistency
      rule =
        Rule.new(
          :example_with_condition,
          [
            Rule.pattern(Rule.var("x"), Rule.owl_sameAs(), Rule.var("y")),
            Rule.not_equal(Rule.var("x"), Rule.var("y"))
          ],
          Rule.pattern(Rule.var("x"), Rule.owl_sameAs(), Rule.var("y")),
          description: "Example rule with not_equal condition"
        )

      assert Rule.pattern_count(rule) == 1
      assert Rule.condition_count(rule) == 1

      # Condition should filter out reflexive sameAs
      binding_same = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}a"}}
      binding_diff = %{"x" => {:iri, "#{@ex}a"}, "y" => {:iri, "#{@ex}b"}}

      assert Rule.evaluate_conditions(rule, binding_same) == false
      assert Rule.evaluate_conditions(rule, binding_diff) == true
    end
  end
end
