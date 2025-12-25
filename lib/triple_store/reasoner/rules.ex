defmodule TripleStore.Reasoner.Rules do
  @moduledoc """
  Standard OWL 2 RL rule definitions for forward-chaining inference.

  This module provides the complete set of OWL 2 RL rules organized by category:

  ## RDFS Rules (Profile: :rdfs)
  - `scm_sco` - rdfs:subClassOf transitivity
  - `scm_spo` - rdfs:subPropertyOf transitivity
  - `cax_sco` - Class membership through subclass
  - `prp_spo1` - Property inheritance through subproperty
  - `prp_dom` - Property domain inference
  - `prp_rng` - Property range inference

  ## OWL 2 RL Property Rules (Profile: :owl2rl)
  - `prp_trp` - Transitive property
  - `prp_symp` - Symmetric property
  - `prp_inv1`, `prp_inv2` - Inverse properties
  - `prp_fp` - Functional property
  - `prp_ifp` - Inverse functional property

  ## OWL 2 RL Equality Rules (Profile: :owl2rl)
  - `eq_ref` - owl:sameAs reflexivity
  - `eq_sym` - owl:sameAs symmetry
  - `eq_trans` - owl:sameAs transitivity
  - `eq_rep_s`, `eq_rep_p`, `eq_rep_o` - Equality replacement

  ## OWL 2 RL Class Restriction Rules (Profile: :owl2rl)
  - `cls_hv1`, `cls_hv2` - hasValue restrictions
  - `cls_svf1`, `cls_svf2` - someValuesFrom restrictions
  - `cls_avf` - allValuesFrom restrictions

  ## Usage

      # Get all RDFS rules
      Rules.rdfs_rules()

      # Get all OWL 2 RL rules
      Rules.owl2rl_rules()

      # Get all rules
      Rules.all_rules()

      # Get rules by profile
      Rules.rules_for_profile(:rdfs)
  """

  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Returns all RDFS rules.
  """
  @spec rdfs_rules() :: [Rule.t()]
  def rdfs_rules do
    [
      scm_sco(),
      scm_spo(),
      cax_sco(),
      prp_spo1(),
      prp_dom(),
      prp_rng()
    ]
  end

  @doc """
  Returns all OWL 2 RL rules (excluding RDFS).
  """
  @spec owl2rl_rules() :: [Rule.t()]
  def owl2rl_rules do
    property_rules() ++ equality_rules() ++ class_restriction_rules()
  end

  @doc """
  Returns all rules (RDFS + OWL 2 RL).
  """
  @spec all_rules() :: [Rule.t()]
  def all_rules do
    rdfs_rules() ++ owl2rl_rules()
  end

  @doc """
  Returns rules for a specific profile.
  """
  @spec rules_for_profile(:rdfs | :owl2rl | :all) :: [Rule.t()]
  def rules_for_profile(:rdfs), do: rdfs_rules()
  def rules_for_profile(:owl2rl), do: all_rules()
  def rules_for_profile(:all), do: all_rules()

  @doc """
  Returns a rule by name.
  """
  @spec get_rule(atom()) :: Rule.t() | nil
  def get_rule(name) do
    Enum.find(all_rules(), fn rule -> rule.name == name end)
  end

  @doc """
  Returns a list of all rule names.
  """
  @spec rule_names() :: [atom()]
  def rule_names do
    Enum.map(all_rules(), & &1.name)
  end

  # ============================================================================
  # Property Rules
  # ============================================================================

  defp property_rules do
    [
      prp_trp(),
      prp_symp(),
      prp_inv1(),
      prp_inv2(),
      prp_fp(),
      prp_ifp()
    ]
  end

  # ============================================================================
  # Equality Rules
  # ============================================================================

  defp equality_rules do
    [
      eq_ref(),
      eq_sym(),
      eq_trans(),
      eq_rep_s(),
      eq_rep_p(),
      eq_rep_o()
    ]
  end

  # ============================================================================
  # Class Restriction Rules
  # ============================================================================

  defp class_restriction_rules do
    [
      cls_hv1(),
      cls_hv2(),
      cls_svf1(),
      cls_svf2(),
      cls_avf()
    ]
  end

  # ============================================================================
  # RDFS Schema Rules
  # ============================================================================

  @doc """
  scm-sco: rdfs:subClassOf transitivity

  If c1 rdfs:subClassOf c2, and c2 rdfs:subClassOf c3, then c1 rdfs:subClassOf c3.
  """
  @spec scm_sco() :: Rule.t()
  def scm_sco do
    Rule.new(:scm_sco,
      [
        Rule.pattern(Rule.var("c1"), rdfs_subClassOf(), Rule.var("c2")),
        Rule.pattern(Rule.var("c2"), rdfs_subClassOf(), Rule.var("c3"))
      ],
      Rule.pattern(Rule.var("c1"), rdfs_subClassOf(), Rule.var("c3")),
      description: "rdfs:subClassOf transitivity",
      profile: :rdfs
    )
  end

  @doc """
  scm-spo: rdfs:subPropertyOf transitivity

  If p1 rdfs:subPropertyOf p2, and p2 rdfs:subPropertyOf p3, then p1 rdfs:subPropertyOf p3.
  """
  @spec scm_spo() :: Rule.t()
  def scm_spo do
    Rule.new(:scm_spo,
      [
        Rule.pattern(Rule.var("p1"), rdfs_subPropertyOf(), Rule.var("p2")),
        Rule.pattern(Rule.var("p2"), rdfs_subPropertyOf(), Rule.var("p3"))
      ],
      Rule.pattern(Rule.var("p1"), rdfs_subPropertyOf(), Rule.var("p3")),
      description: "rdfs:subPropertyOf transitivity",
      profile: :rdfs
    )
  end

  @doc """
  cax-sco: Class membership through subclass

  If x rdf:type c1, and c1 rdfs:subClassOf c2, then x rdf:type c2.
  """
  @spec cax_sco() :: Rule.t()
  def cax_sco do
    Rule.new(:cax_sco,
      [
        Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c1")),
        Rule.pattern(Rule.var("c1"), rdfs_subClassOf(), Rule.var("c2"))
      ],
      Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c2")),
      description: "Class membership through subclass",
      profile: :rdfs
    )
  end

  @doc """
  prp-spo1: Property inheritance through subproperty

  If p1 rdfs:subPropertyOf p2, and x p1 y, then x p2 y.
  """
  @spec prp_spo1() :: Rule.t()
  def prp_spo1 do
    Rule.new(:prp_spo1,
      [
        Rule.pattern(Rule.var("p1"), rdfs_subPropertyOf(), Rule.var("p2")),
        Rule.pattern(Rule.var("x"), Rule.var("p1"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("x"), Rule.var("p2"), Rule.var("y")),
      description: "Property inheritance through subproperty",
      profile: :rdfs
    )
  end

  @doc """
  prp-dom: Property domain inference

  If p rdfs:domain c, and x p y, then x rdf:type c.
  """
  @spec prp_dom() :: Rule.t()
  def prp_dom do
    Rule.new(:prp_dom,
      [
        Rule.pattern(Rule.var("p"), rdfs_domain(), Rule.var("c")),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c")),
      description: "Property domain inference",
      profile: :rdfs
    )
  end

  @doc """
  prp-rng: Property range inference

  If p rdfs:range c, and x p y, then y rdf:type c.
  """
  @spec prp_rng() :: Rule.t()
  def prp_rng do
    Rule.new(:prp_rng,
      [
        Rule.pattern(Rule.var("p"), rdfs_range(), Rule.var("c")),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("y"), rdf_type(), Rule.var("c")),
      description: "Property range inference",
      profile: :rdfs
    )
  end

  # ============================================================================
  # OWL 2 RL Property Characteristic Rules
  # ============================================================================

  @doc """
  prp-trp: Transitive property

  If p rdf:type owl:TransitiveProperty, x p y, and y p z, then x p z.
  """
  @spec prp_trp() :: Rule.t()
  def prp_trp do
    Rule.new(:prp_trp,
      [
        Rule.pattern(Rule.var("p"), rdf_type(), owl_TransitiveProperty()),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
        Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("z"))
      ],
      Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("z")),
      description: "Transitive property inference",
      profile: :owl2rl
    )
  end

  @doc """
  prp-symp: Symmetric property

  If p rdf:type owl:SymmetricProperty, and x p y, then y p x.
  """
  @spec prp_symp() :: Rule.t()
  def prp_symp do
    Rule.new(:prp_symp,
      [
        Rule.pattern(Rule.var("p"), rdf_type(), owl_SymmetricProperty()),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("y"), Rule.var("p"), Rule.var("x")),
      description: "Symmetric property inference",
      profile: :owl2rl
    )
  end

  @doc """
  prp-inv1: Inverse property (forward direction)

  If p1 owl:inverseOf p2, and x p1 y, then y p2 x.
  """
  @spec prp_inv1() :: Rule.t()
  def prp_inv1 do
    Rule.new(:prp_inv1,
      [
        Rule.pattern(Rule.var("p1"), owl_inverseOf(), Rule.var("p2")),
        Rule.pattern(Rule.var("x"), Rule.var("p1"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("y"), Rule.var("p2"), Rule.var("x")),
      description: "Inverse property inference (forward)",
      profile: :owl2rl
    )
  end

  @doc """
  prp-inv2: Inverse property (backward direction)

  If p1 owl:inverseOf p2, and x p2 y, then y p1 x.
  """
  @spec prp_inv2() :: Rule.t()
  def prp_inv2 do
    Rule.new(:prp_inv2,
      [
        Rule.pattern(Rule.var("p1"), owl_inverseOf(), Rule.var("p2")),
        Rule.pattern(Rule.var("x"), Rule.var("p2"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("y"), Rule.var("p1"), Rule.var("x")),
      description: "Inverse property inference (backward)",
      profile: :owl2rl
    )
  end

  @doc """
  prp-fp: Functional property

  If p rdf:type owl:FunctionalProperty, x p y1, and x p y2, then y1 owl:sameAs y2.

  Note: This rule can derive equality between different values of a functional property.
  """
  @spec prp_fp() :: Rule.t()
  def prp_fp do
    Rule.new(:prp_fp,
      [
        Rule.pattern(Rule.var("p"), rdf_type(), owl_FunctionalProperty()),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y1")),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y2")),
        Rule.not_equal(Rule.var("y1"), Rule.var("y2"))
      ],
      Rule.pattern(Rule.var("y1"), owl_sameAs(), Rule.var("y2")),
      description: "Functional property derives equality",
      profile: :owl2rl
    )
  end

  @doc """
  prp-ifp: Inverse functional property

  If p rdf:type owl:InverseFunctionalProperty, x1 p y, and x2 p y, then x1 owl:sameAs x2.

  Note: This rule can derive equality between different subjects of an inverse functional property.
  """
  @spec prp_ifp() :: Rule.t()
  def prp_ifp do
    Rule.new(:prp_ifp,
      [
        Rule.pattern(Rule.var("p"), rdf_type(), owl_InverseFunctionalProperty()),
        Rule.pattern(Rule.var("x1"), Rule.var("p"), Rule.var("y")),
        Rule.pattern(Rule.var("x2"), Rule.var("p"), Rule.var("y")),
        Rule.not_equal(Rule.var("x1"), Rule.var("x2"))
      ],
      Rule.pattern(Rule.var("x1"), owl_sameAs(), Rule.var("x2")),
      description: "Inverse functional property derives equality",
      profile: :owl2rl
    )
  end

  # ============================================================================
  # OWL 2 RL Equality Rules
  # ============================================================================

  @doc """
  eq-ref: owl:sameAs reflexivity

  Every resource is the same as itself: x owl:sameAs x.

  Note: This rule is typically not materialized fully (would create infinite triples).
  Instead, it's used implicitly during query answering.
  """
  @spec eq_ref() :: Rule.t()
  def eq_ref do
    Rule.new(:eq_ref,
      [
        # Match any triple to find resources
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("x"), owl_sameAs(), Rule.var("x")),
      description: "owl:sameAs reflexivity (subject)",
      profile: :owl2rl
    )
  end

  @doc """
  eq-sym: owl:sameAs symmetry

  If x owl:sameAs y, then y owl:sameAs x.
  """
  @spec eq_sym() :: Rule.t()
  def eq_sym do
    Rule.new(:eq_sym,
      [
        Rule.pattern(Rule.var("x"), owl_sameAs(), Rule.var("y"))
      ],
      Rule.pattern(Rule.var("y"), owl_sameAs(), Rule.var("x")),
      description: "owl:sameAs symmetry",
      profile: :owl2rl
    )
  end

  @doc """
  eq-trans: owl:sameAs transitivity

  If x owl:sameAs y, and y owl:sameAs z, then x owl:sameAs z.
  """
  @spec eq_trans() :: Rule.t()
  def eq_trans do
    Rule.new(:eq_trans,
      [
        Rule.pattern(Rule.var("x"), owl_sameAs(), Rule.var("y")),
        Rule.pattern(Rule.var("y"), owl_sameAs(), Rule.var("z"))
      ],
      Rule.pattern(Rule.var("x"), owl_sameAs(), Rule.var("z")),
      description: "owl:sameAs transitivity",
      profile: :owl2rl
    )
  end

  @doc """
  eq-rep-s: Equality replacement in subject position

  If s1 owl:sameAs s2, and s1 p o, then s2 p o.
  """
  @spec eq_rep_s() :: Rule.t()
  def eq_rep_s do
    Rule.new(:eq_rep_s,
      [
        Rule.pattern(Rule.var("s1"), owl_sameAs(), Rule.var("s2")),
        Rule.pattern(Rule.var("s1"), Rule.var("p"), Rule.var("o"))
      ],
      Rule.pattern(Rule.var("s2"), Rule.var("p"), Rule.var("o")),
      description: "Equality replacement in subject position",
      profile: :owl2rl
    )
  end

  @doc """
  eq-rep-p: Equality replacement in predicate position

  If p1 owl:sameAs p2, and s p1 o, then s p2 o.

  Note: Predicate equality is rare in practice but valid in OWL 2 RL.
  """
  @spec eq_rep_p() :: Rule.t()
  def eq_rep_p do
    Rule.new(:eq_rep_p,
      [
        Rule.pattern(Rule.var("p1"), owl_sameAs(), Rule.var("p2")),
        Rule.pattern(Rule.var("s"), Rule.var("p1"), Rule.var("o"))
      ],
      Rule.pattern(Rule.var("s"), Rule.var("p2"), Rule.var("o")),
      description: "Equality replacement in predicate position",
      profile: :owl2rl
    )
  end

  @doc """
  eq-rep-o: Equality replacement in object position

  If o1 owl:sameAs o2, and s p o1, then s p o2.
  """
  @spec eq_rep_o() :: Rule.t()
  def eq_rep_o do
    Rule.new(:eq_rep_o,
      [
        Rule.pattern(Rule.var("o1"), owl_sameAs(), Rule.var("o2")),
        Rule.pattern(Rule.var("s"), Rule.var("p"), Rule.var("o1"))
      ],
      Rule.pattern(Rule.var("s"), Rule.var("p"), Rule.var("o2")),
      description: "Equality replacement in object position",
      profile: :owl2rl
    )
  end

  # ============================================================================
  # OWL 2 RL Class Restriction Rules
  # ============================================================================

  @doc """
  cls-hv1: hasValue restriction (membership inference)

  If x rdf:type c, c owl:hasValue v, and c owl:onProperty p, then x p v.

  This rule infers property values from class membership when the class
  is defined with a hasValue restriction.
  """
  @spec cls_hv1() :: Rule.t()
  def cls_hv1 do
    Rule.new(:cls_hv1,
      [
        Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c")),
        Rule.pattern(Rule.var("c"), owl_hasValue(), Rule.var("v")),
        Rule.pattern(Rule.var("c"), owl_onProperty(), Rule.var("p"))
      ],
      Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("v")),
      description: "hasValue restriction infers property value",
      profile: :owl2rl
    )
  end

  @doc """
  cls-hv2: hasValue restriction (class inference)

  If x p v, c owl:hasValue v, and c owl:onProperty p, then x rdf:type c.

  This rule infers class membership from property values when a class
  is defined with a hasValue restriction.
  """
  @spec cls_hv2() :: Rule.t()
  def cls_hv2 do
    Rule.new(:cls_hv2,
      [
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("v")),
        Rule.pattern(Rule.var("c"), owl_hasValue(), Rule.var("v")),
        Rule.pattern(Rule.var("c"), owl_onProperty(), Rule.var("p"))
      ],
      Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("c")),
      description: "hasValue restriction infers class membership",
      profile: :owl2rl
    )
  end

  @doc """
  cls-svf1: someValuesFrom restriction (class inference from typed value)

  If x p y, y rdf:type c, r owl:someValuesFrom c, and r owl:onProperty p, then x rdf:type r.

  This rule infers class membership when a property has a value of the required type.
  """
  @spec cls_svf1() :: Rule.t()
  def cls_svf1 do
    Rule.new(:cls_svf1,
      [
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
        Rule.pattern(Rule.var("y"), rdf_type(), Rule.var("c")),
        Rule.pattern(Rule.var("r"), owl_someValuesFrom(), Rule.var("c")),
        Rule.pattern(Rule.var("r"), owl_onProperty(), Rule.var("p"))
      ],
      Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("r")),
      description: "someValuesFrom restriction infers class membership",
      profile: :owl2rl
    )
  end

  @doc """
  cls-svf2: someValuesFrom restriction with owl:Thing

  If x p y, r owl:someValuesFrom owl:Thing, and r owl:onProperty p, then x rdf:type r.

  This rule handles someValuesFrom restrictions where the filler is owl:Thing,
  meaning any value satisfies the restriction.
  """
  @spec cls_svf2() :: Rule.t()
  def cls_svf2 do
    Rule.new(:cls_svf2,
      [
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
        Rule.pattern(Rule.var("r"), owl_someValuesFrom(), owl_Thing()),
        Rule.pattern(Rule.var("r"), owl_onProperty(), Rule.var("p"))
      ],
      Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("r")),
      description: "someValuesFrom owl:Thing restriction",
      profile: :owl2rl
    )
  end

  @doc """
  cls-avf: allValuesFrom restriction (value type inference)

  If x rdf:type r, x p y, r owl:allValuesFrom c, and r owl:onProperty p, then y rdf:type c.

  This rule infers the type of property values based on allValuesFrom restrictions.
  """
  @spec cls_avf() :: Rule.t()
  def cls_avf do
    Rule.new(:cls_avf,
      [
        Rule.pattern(Rule.var("x"), rdf_type(), Rule.var("r")),
        Rule.pattern(Rule.var("x"), Rule.var("p"), Rule.var("y")),
        Rule.pattern(Rule.var("r"), owl_allValuesFrom(), Rule.var("c")),
        Rule.pattern(Rule.var("r"), owl_onProperty(), Rule.var("p"))
      ],
      Rule.pattern(Rule.var("y"), rdf_type(), Rule.var("c")),
      description: "allValuesFrom restriction infers value type",
      profile: :owl2rl
    )
  end

  # ============================================================================
  # IRI Helpers (local to this module)
  # ============================================================================

  defp rdf_type, do: Rule.iri(@rdf <> "type")
  defp rdfs_subClassOf, do: Rule.iri(@rdfs <> "subClassOf")
  defp rdfs_subPropertyOf, do: Rule.iri(@rdfs <> "subPropertyOf")
  defp rdfs_domain, do: Rule.iri(@rdfs <> "domain")
  defp rdfs_range, do: Rule.iri(@rdfs <> "range")

  defp owl_sameAs, do: Rule.iri(@owl <> "sameAs")
  defp owl_TransitiveProperty, do: Rule.iri(@owl <> "TransitiveProperty")
  defp owl_SymmetricProperty, do: Rule.iri(@owl <> "SymmetricProperty")
  defp owl_inverseOf, do: Rule.iri(@owl <> "inverseOf")
  defp owl_FunctionalProperty, do: Rule.iri(@owl <> "FunctionalProperty")
  defp owl_InverseFunctionalProperty, do: Rule.iri(@owl <> "InverseFunctionalProperty")
  defp owl_hasValue, do: Rule.iri(@owl <> "hasValue")
  defp owl_onProperty, do: Rule.iri(@owl <> "onProperty")
  defp owl_someValuesFrom, do: Rule.iri(@owl <> "someValuesFrom")
  defp owl_allValuesFrom, do: Rule.iri(@owl <> "allValuesFrom")
  defp owl_Thing, do: Rule.iri(@owl <> "Thing")
end
