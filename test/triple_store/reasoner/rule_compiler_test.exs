defmodule TripleStore.Reasoner.RuleCompilerTest do
  @moduledoc """
  Tests for the RuleCompiler module.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{Rule, Rules, RuleCompiler}

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Empty Schema Info Tests
  # ============================================================================

  describe "empty_schema_info/0" do
    test "returns a complete schema info structure" do
      info = RuleCompiler.empty_schema_info()

      assert info.has_subclass == false
      assert info.has_subproperty == false
      assert info.has_domain == false
      assert info.has_range == false
      assert info.transitive_properties == []
      assert info.symmetric_properties == []
      assert info.inverse_properties == []
      assert info.functional_properties == []
      assert info.inverse_functional_properties == []
      assert info.has_sameas == false
      assert info.has_restrictions == false
    end
  end

  # ============================================================================
  # Compile with Schema Tests
  # ============================================================================

  describe "compile_with_schema/2" do
    test "compiles with empty schema returns only eq_ref" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      # Only eq_ref should be applicable (always true)
      assert length(compiled.rules) == 1
      assert hd(compiled.rules).name == :eq_ref
      assert compiled.profile == :owl2rl
    end

    test "compiles with subclass schema includes subclass rules" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_subclass: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :scm_sco in rule_names
      assert :cax_sco in rule_names
    end

    test "compiles with domain/range includes prp_dom and prp_rng" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | has_domain: true,
          has_range: true
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_dom in rule_names
      assert :prp_rng in rule_names
    end

    test "compiles with transitive properties includes prp_trp" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_trp in rule_names
    end

    test "compiles with symmetric properties includes prp_symp" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | symmetric_properties: ["#{@ex}knows"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_symp in rule_names
    end

    test "compiles with inverse properties includes prp_inv1 and prp_inv2" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | inverse_properties: [{"#{@ex}hasParent", "#{@ex}hasChild"}]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_inv1 in rule_names
      assert :prp_inv2 in rule_names
    end

    test "compiles with functional properties includes prp_fp" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | functional_properties: ["#{@ex}hasSSN"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_fp in rule_names
    end

    test "compiles with inverse functional properties includes prp_ifp" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | inverse_functional_properties: ["#{@ex}hasSSN"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :prp_ifp in rule_names
    end

    test "compiles with sameAs includes equality rules" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_sameas: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :eq_sym in rule_names
      assert :eq_trans in rule_names
      assert :eq_rep_s in rule_names
      assert :eq_rep_p in rule_names
      assert :eq_rep_o in rule_names
    end

    test "compiles with restrictions includes cls_* rules" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_restrictions: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      rule_names = Enum.map(compiled.rules, & &1.name)
      assert :cls_hv1 in rule_names
      assert :cls_hv2 in rule_names
      assert :cls_svf1 in rule_names
      assert :cls_svf2 in rule_names
      assert :cls_avf in rule_names
    end

    test "rdfs profile excludes owl2rl-only rules" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | has_subclass: true,
          transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      rule_names = Enum.map(compiled.rules, & &1.name)
      # RDFS rules should be included
      assert :scm_sco in rule_names
      assert :cax_sco in rule_names
      # OWL rules should NOT be included in RDFS profile
      refute :prp_trp in rule_names
    end
  end

  # ============================================================================
  # Rule Specialization Tests
  # ============================================================================

  describe "rule specialization" do
    test "specializes transitive property rule" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      specialized_names = Enum.map(compiled.specialized_rules, & &1.name)
      # Should have a specialized rule for "contains"
      assert Enum.any?(specialized_names, &String.contains?(to_string(&1), "contains"))
    end

    test "specialized rule has fewer body patterns" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      generic_rule = Enum.find(compiled.rules, &(&1.name == :prp_trp))

      specialized_rule =
        Enum.find(compiled.specialized_rules, fn r ->
          String.contains?(to_string(r.name), "transitive")
        end)

      # Generic rule has 3 patterns (type declaration + 2 property patterns)
      assert Rule.pattern_count(generic_rule) == 3
      # Specialized rule has 2 patterns (type declaration removed)
      assert Rule.pattern_count(specialized_rule) == 2
    end

    test "specialized rule has bound property" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | symmetric_properties: ["#{@ex}knows"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      specialized_rule =
        Enum.find(compiled.specialized_rules, fn r ->
          String.contains?(to_string(r.name), "knows")
        end)

      assert specialized_rule != nil
      # The specialized rule should have the property bound in head
      {:pattern, [_, pred, _]} = specialized_rule.head
      assert pred == {:iri, "#{@ex}knows"}
    end

    test "specializes inverse property rules" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | inverse_properties: [{"#{@ex}hasParent", "#{@ex}hasChild"}]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      specialized_names = Enum.map(compiled.specialized_rules, &to_string(&1.name))
      # Should have specialized rules for the inverse pair
      assert Enum.any?(specialized_names, &String.contains?(&1, "hasParent"))
      assert Enum.any?(specialized_names, &String.contains?(&1, "hasChild"))
    end

    test "specializes multiple properties" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains", "#{@ex}partOf"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      specialized_names = Enum.map(compiled.specialized_rules, &to_string(&1.name))
      assert Enum.any?(specialized_names, &String.contains?(&1, "contains"))
      assert Enum.any?(specialized_names, &String.contains?(&1, "partOf"))
    end

    test "specialize: false skips specialization" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: false
        )

      assert compiled.specialized_rules == []
    end
  end

  # ============================================================================
  # Get Rules Tests
  # ============================================================================

  describe "get_rules/1" do
    test "returns specialized rules when available" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      rules = RuleCompiler.get_rules(compiled)
      # Should include both specialized and generic rules
      assert length(rules) > length(compiled.rules)
    end

    test "returns generic rules when no specialization" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_subclass: true}

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :rdfs,
          specialize: false
        )

      rules = RuleCompiler.get_rules(compiled)
      assert rules == compiled.rules
    end
  end

  describe "get_generic_rules/1" do
    test "returns only generic rules" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      generic = RuleCompiler.get_generic_rules(compiled)
      assert generic == compiled.rules
    end
  end

  describe "get_specialized_rules/1" do
    test "returns only specialized rules" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      specialized = RuleCompiler.get_specialized_rules(compiled)
      assert specialized == compiled.specialized_rules
    end
  end

  # ============================================================================
  # Persistent Term Storage Tests
  # ============================================================================

  describe "persistent_term storage" do
    test "store/2 and load/2 round-trip" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_subclass: true}
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      key = :"test_ontology_#{:erlang.unique_integer([:positive])}"

      try do
        assert :ok = RuleCompiler.store(compiled, key)
        assert {:ok, loaded} = RuleCompiler.load(key)
        assert loaded.rules == compiled.rules
        assert loaded.profile == compiled.profile
      after
        RuleCompiler.remove(key)
      end
    end

    test "load/1 returns error for missing key" do
      assert {:error, :not_found} = RuleCompiler.load(:nonexistent_key)
    end

    test "exists?/1 returns true for stored key" do
      schema_info = RuleCompiler.empty_schema_info()
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      key = :"test_exists_#{:erlang.unique_integer([:positive])}"

      try do
        refute RuleCompiler.exists?(key)
        RuleCompiler.store(compiled, key)
        assert RuleCompiler.exists?(key)
      after
        RuleCompiler.remove(key)
      end
    end

    test "remove/1 removes stored key" do
      schema_info = RuleCompiler.empty_schema_info()
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      key = :"test_remove_#{:erlang.unique_integer([:positive])}"

      RuleCompiler.store(compiled, key)
      assert RuleCompiler.exists?(key)

      RuleCompiler.remove(key)
      refute RuleCompiler.exists?(key)
    end
  end

  # ============================================================================
  # Filter Applicable Rules Tests
  # ============================================================================

  describe "filter_applicable_rules/2" do
    test "filters out inapplicable rules" do
      all_rules = Rules.all_rules()
      empty_schema = RuleCompiler.empty_schema_info()

      filtered = RuleCompiler.filter_applicable_rules(all_rules, empty_schema)

      # Should only include eq_ref (always applicable)
      assert length(filtered) == 1
      assert hd(filtered).name == :eq_ref
    end

    test "includes rules when schema has matching features" do
      all_rules = Rules.all_rules()

      schema = %{
        RuleCompiler.empty_schema_info()
        | has_subclass: true,
          has_subproperty: true,
          has_domain: true,
          has_range: true
      }

      filtered = RuleCompiler.filter_applicable_rules(all_rules, schema)

      rule_names = Enum.map(filtered, & &1.name)
      assert :scm_sco in rule_names
      assert :scm_spo in rule_names
      assert :cax_sco in rule_names
      assert :prp_spo1 in rule_names
      assert :prp_dom in rule_names
      assert :prp_rng in rule_names
    end
  end

  # ============================================================================
  # Compiled Structure Tests
  # ============================================================================

  describe "compiled structure" do
    test "includes compilation timestamp" do
      schema_info = RuleCompiler.empty_schema_info()
      before = DateTime.utc_now()

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      after_time = DateTime.utc_now()

      assert DateTime.compare(compiled.compiled_at, before) in [:eq, :gt]
      assert DateTime.compare(compiled.compiled_at, after_time) in [:eq, :lt]
    end

    test "includes schema_info" do
      schema_info = %{RuleCompiler.empty_schema_info() | has_subclass: true}

      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)

      assert compiled.schema_info == schema_info
    end

    test "includes profile" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled1} = RuleCompiler.compile_with_schema(schema_info, profile: :rdfs)
      {:ok, compiled2} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      assert compiled1.profile == :rdfs
      assert compiled2.profile == :owl2rl
    end
  end

  # ============================================================================
  # Full Schema Tests
  # ============================================================================

  describe "full schema compilation" do
    test "compiles with all features enabled" do
      schema_info = %{
        has_subclass: true,
        has_subproperty: true,
        has_domain: true,
        has_range: true,
        transitive_properties: ["#{@ex}contains"],
        symmetric_properties: ["#{@ex}knows"],
        inverse_properties: [{"#{@ex}hasParent", "#{@ex}hasChild"}],
        functional_properties: ["#{@ex}hasSSN"],
        inverse_functional_properties: ["#{@ex}email"],
        has_sameas: true,
        has_restrictions: true
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          specialize: true
        )

      # All 23 rules should be applicable
      assert length(compiled.rules) == 23

      # Should have specialized rules
      assert length(compiled.specialized_rules) > 0

      # Check some specific specialized rules exist
      specialized_names = Enum.map(compiled.specialized_rules, &to_string(&1.name))
      assert Enum.any?(specialized_names, &String.contains?(&1, "contains"))
      assert Enum.any?(specialized_names, &String.contains?(&1, "knows"))
      assert Enum.any?(specialized_names, &String.contains?(&1, "hasParent"))
    end
  end
end
