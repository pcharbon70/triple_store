defmodule TripleStore.Reasoner.ReasoningProfileTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.ReasoningProfile
  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp rule_names(rules) do
    Enum.map(rules, & &1.name) |> Enum.sort()
  end

  # ============================================================================
  # Tests: Profile Names
  # ============================================================================

  describe "profile_names/0" do
    test "returns all valid profile names" do
      names = ReasoningProfile.profile_names()
      assert :rdfs in names
      assert :owl2rl in names
      assert :custom in names
      assert :none in names
    end
  end

  describe "valid_profile?/1" do
    test "returns true for valid profiles" do
      assert ReasoningProfile.valid_profile?(:rdfs)
      assert ReasoningProfile.valid_profile?(:owl2rl)
      assert ReasoningProfile.valid_profile?(:custom)
      assert ReasoningProfile.valid_profile?(:none)
    end

    test "returns false for invalid profiles" do
      refute ReasoningProfile.valid_profile?(:unknown)
      refute ReasoningProfile.valid_profile?(:full)
      refute ReasoningProfile.valid_profile?("rdfs")
    end
  end

  # ============================================================================
  # Tests: RDFS Profile
  # ============================================================================

  describe "rules_for(:rdfs)" do
    test "returns RDFS rules" do
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      names = rule_names(rules)

      assert :scm_sco in names
      assert :scm_spo in names
      assert :cax_sco in names
      assert :prp_spo1 in names
      assert :prp_dom in names
      assert :prp_rng in names
    end

    test "returns exactly 6 rules" do
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      assert length(rules) == 6
    end

    test "does not include OWL property rules" do
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      names = rule_names(rules)

      refute :prp_trp in names
      refute :prp_symp in names
      refute :eq_sym in names
    end

    test "all rules are Rule structs" do
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      assert Enum.all?(rules, &match?(%Rule{}, &1))
    end
  end

  # ============================================================================
  # Tests: OWL 2 RL Profile
  # ============================================================================

  describe "rules_for(:owl2rl)" do
    test "returns all rules including RDFS" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)
      names = rule_names(rules)

      # RDFS rules
      assert :scm_sco in names
      assert :prp_dom in names

      # Property rules
      assert :prp_trp in names
      assert :prp_symp in names
      assert :prp_inv1 in names
      assert :prp_fp in names

      # Equality rules
      assert :eq_sym in names
      assert :eq_trans in names

      # Restriction rules
      assert :cls_hv1 in names
      assert :cls_avf in names
    end

    test "includes more rules than RDFS" do
      {:ok, rdfs_rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, owl_rules} = ReasoningProfile.rules_for(:owl2rl)

      assert length(owl_rules) > length(rdfs_rules)
    end
  end

  # ============================================================================
  # Tests: None Profile
  # ============================================================================

  describe "rules_for(:none)" do
    test "returns empty rule list" do
      {:ok, rules} = ReasoningProfile.rules_for(:none)
      assert rules == []
    end
  end

  # ============================================================================
  # Tests: Custom Profile
  # ============================================================================

  describe "rules_for(:custom)" do
    test "returns specified rules" do
      {:ok, rules} = ReasoningProfile.rules_for(:custom, rules: [:scm_sco, :prp_trp])
      names = rule_names(rules)

      assert names == [:prp_trp, :scm_sco]
    end

    test "returns error when rules option missing" do
      {:error, reason} = ReasoningProfile.rules_for(:custom)
      assert {:missing_option, :rules, _} = reason
    end

    test "returns error for unknown rule names" do
      {:error, reason} = ReasoningProfile.rules_for(:custom, rules: [:unknown_rule])
      assert {:unknown_rules, [:unknown_rule]} = reason
    end

    test "returns partial error for mixed known/unknown rules" do
      {:error, reason} = ReasoningProfile.rules_for(:custom, rules: [:scm_sco, :fake_rule])
      assert {:unknown_rules, [:fake_rule]} = reason
    end

    test "allows single rule" do
      {:ok, rules} = ReasoningProfile.rules_for(:custom, rules: [:prp_trp])
      assert length(rules) == 1
      assert hd(rules).name == :prp_trp
    end
  end

  # ============================================================================
  # Tests: Exclusion Option
  # ============================================================================

  describe "exclude option" do
    test "excludes specified rules from RDFS profile" do
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs, exclude: [:prp_dom, :prp_rng])
      names = rule_names(rules)

      assert :scm_sco in names
      refute :prp_dom in names
      refute :prp_rng in names
    end

    test "excludes specified rules from OWL 2 RL profile" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl, exclude: [:eq_ref, :eq_sym, :eq_trans])
      names = rule_names(rules)

      # Other equality rules still present
      assert :eq_rep_s in names

      # Excluded rules not present
      refute :eq_ref in names
      refute :eq_sym in names
      refute :eq_trans in names
    end

    test "excludes specified rules from custom profile" do
      {:ok, rules} = ReasoningProfile.rules_for(:custom,
        rules: [:scm_sco, :cax_sco, :prp_dom],
        exclude: [:cax_sco]
      )
      names = rule_names(rules)

      assert :scm_sco in names
      assert :prp_dom in names
      refute :cax_sco in names
    end
  end

  # ============================================================================
  # Tests: rules_for!/2
  # ============================================================================

  describe "rules_for!/2" do
    test "returns rules for valid profile" do
      rules = ReasoningProfile.rules_for!(:rdfs)
      assert is_list(rules)
      assert length(rules) == 6
    end

    test "raises for invalid profile" do
      assert_raise ArgumentError, fn ->
        ReasoningProfile.rules_for!(:invalid)
      end
    end
  end

  # ============================================================================
  # Tests: Profile Info
  # ============================================================================

  describe "info/1" do
    test "returns info for none profile" do
      info = ReasoningProfile.info(:none)
      assert info.name == :none
      assert info.rule_count == 0
      assert info.rule_names == []
    end

    test "returns info for RDFS profile" do
      info = ReasoningProfile.info(:rdfs)
      assert info.name == :rdfs
      assert info.rule_count == 6
      assert :scm_sco in info.rule_names
      assert :schema_hierarchy in info.categories
    end

    test "returns info for OWL 2 RL profile" do
      info = ReasoningProfile.info(:owl2rl)
      assert info.name == :owl2rl
      assert info.rule_count > 6
      assert :equality in info.categories
      assert :restrictions in info.categories
    end

    test "returns info for custom profile" do
      info = ReasoningProfile.info(:custom)
      assert info.name == :custom
      assert info.rule_count == :variable
    end
  end

  # ============================================================================
  # Tests: Available Rules
  # ============================================================================

  describe "available_rules/0" do
    test "returns list of rule names" do
      rules = ReasoningProfile.available_rules()
      assert is_list(rules)
      assert :scm_sco in rules
      assert :prp_trp in rules
    end
  end

  # ============================================================================
  # Tests: Rules by Category
  # ============================================================================

  describe "rules_by_category/0" do
    test "returns rules grouped by category" do
      categories = ReasoningProfile.rules_by_category()

      assert is_map(categories)
      assert :rdfs in Map.keys(categories)
      assert :property_characteristics in Map.keys(categories)
      assert :equality in Map.keys(categories)
      assert :restrictions in Map.keys(categories)
    end

    test "rdfs category has RDFS rules" do
      categories = ReasoningProfile.rules_by_category()
      rdfs = categories.rdfs

      assert :scm_sco in rdfs
      assert :prp_dom in rdfs
    end

    test "property_characteristics category has property rules" do
      categories = ReasoningProfile.rules_by_category()
      props = categories.property_characteristics

      assert :prp_trp in props
      assert :prp_symp in props
    end
  end

  describe "rules_for_category/1" do
    test "returns rules for valid category" do
      {:ok, rules} = ReasoningProfile.rules_for_category(:rdfs)
      names = rule_names(rules)

      assert :scm_sco in names
    end

    test "returns error for unknown category" do
      {:error, reason} = ReasoningProfile.rules_for_category(:unknown)
      assert {:unknown_category, :unknown} = reason
    end
  end

  # ============================================================================
  # Tests: From Categories
  # ============================================================================

  describe "from_categories/2" do
    test "combines rules from multiple categories" do
      {:ok, rules} = ReasoningProfile.from_categories([:rdfs, :property_characteristics])
      names = rule_names(rules)

      # Has RDFS rules
      assert :scm_sco in names
      # Has property rules
      assert :prp_trp in names
      # Does not have equality rules
      refute :eq_sym in names
    end

    test "handles single category" do
      {:ok, rules} = ReasoningProfile.from_categories([:equality])
      names = rule_names(rules)

      assert :eq_sym in names
      refute :scm_sco in names
    end

    test "returns error for unknown categories" do
      {:error, reason} = ReasoningProfile.from_categories([:rdfs, :unknown])
      assert {:unknown_categories, [:unknown]} = reason
    end

    test "deduplicates rules" do
      {:ok, rules} = ReasoningProfile.from_categories([:rdfs, :rdfs])
      assert length(rules) == 6
    end

    test "supports exclude option" do
      {:ok, rules} = ReasoningProfile.from_categories([:rdfs], exclude: [:prp_dom])
      names = rule_names(rules)

      refute :prp_dom in names
      assert :scm_sco in names
    end
  end

  # ============================================================================
  # Tests: Profile Suggestion
  # ============================================================================

  describe "suggest_profile/1" do
    test "suggests RDFS for simple schema" do
      schema = %{
        has_subclass: true,
        has_domain: true
      }

      assert ReasoningProfile.suggest_profile(schema) == :rdfs
    end

    test "suggests OWL 2 RL when transitive properties present" do
      schema = %{
        has_subclass: true,
        transitive_properties: ["http://example.org/contains"]
      }

      assert ReasoningProfile.suggest_profile(schema) == :owl2rl
    end

    test "suggests OWL 2 RL when symmetric properties present" do
      schema = %{
        symmetric_properties: ["http://example.org/knows"]
      }

      assert ReasoningProfile.suggest_profile(schema) == :owl2rl
    end

    test "suggests OWL 2 RL when sameAs present" do
      schema = %{
        has_sameas: true
      }

      assert ReasoningProfile.suggest_profile(schema) == :owl2rl
    end

    test "suggests OWL 2 RL when restrictions present" do
      schema = %{
        has_restrictions: true
      }

      assert ReasoningProfile.suggest_profile(schema) == :owl2rl
    end

    test "suggests RDFS for empty schema" do
      schema = %{}
      assert ReasoningProfile.suggest_profile(schema) == :rdfs
    end
  end

  # ============================================================================
  # Tests: Invalid Profile
  # ============================================================================

  describe "invalid profile handling" do
    test "returns error for unknown profile" do
      {:error, reason} = ReasoningProfile.rules_for(:unknown)
      assert {:invalid_profile, :unknown, _} = reason
    end
  end
end
