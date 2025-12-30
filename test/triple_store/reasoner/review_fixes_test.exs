defmodule TripleStore.Reasoner.ReviewFixesTest do
  @moduledoc """
  Tests for Section 4.1 review fixes:
  - Namespaces module
  - SchemaInfo struct
  - Rule validation
  - IRI validation for SPARQL injection prevention
  - Rule explain/debug capabilities
  - Delta pattern marking
  - Telemetry events
  - Persistent term lifecycle management
  """
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{
    Namespaces,
    SchemaInfo,
    Rule,
    Rules,
    RuleCompiler,
    RuleOptimizer,
    Telemetry
  }

  # ============================================================================
  # Namespaces Tests
  # ============================================================================

  describe "Namespaces module" do
    test "returns namespace prefixes" do
      assert Namespaces.rdf() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
      assert Namespaces.rdfs() == "http://www.w3.org/2000/01/rdf-schema#"
      assert Namespaces.owl() == "http://www.w3.org/2002/07/owl#"
      assert Namespaces.xsd() == "http://www.w3.org/2001/XMLSchema#"
    end

    test "builds full IRIs from local names" do
      assert Namespaces.rdf("type") == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      assert Namespaces.rdfs("subClassOf") == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    end

    test "provides common IRI helpers" do
      assert Namespaces.rdf_type() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      assert Namespaces.rdfs_subClassOf() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
      assert Namespaces.owl_sameAs() == "http://www.w3.org/2002/07/owl#sameAs"
    end

    test "extracts local name from hash-based IRI" do
      assert Namespaces.extract_local_name("http://example.org#Person") == "Person"
    end

    test "extracts local name from slash-based IRI" do
      assert Namespaces.extract_local_name("http://example.org/Person") == "Person"
    end

    test "validates safe IRIs" do
      assert {:ok, _} = Namespaces.validate_iri("http://example.org/Person")
      assert {:ok, _} = Namespaces.validate_iri("http://www.w3.org/2002/07/owl#Thing")
    end

    test "rejects dangerous IRIs with injection characters" do
      assert {:error, :invalid_iri_characters} =
               Namespaces.validate_iri("http://example.org/Person>")

      assert {:error, :invalid_iri_characters} =
               Namespaces.validate_iri("http://example.org/Person}")

      assert {:error, :invalid_iri_characters} =
               Namespaces.validate_iri("http://example.org/Person; DROP")

      assert {:error, :invalid_iri_characters} =
               Namespaces.validate_iri("http://example.org/Person\n")
    end

    test "valid_iri? returns boolean" do
      assert Namespaces.valid_iri?("http://example.org/Person") == true
      assert Namespaces.valid_iri?("http://example.org/Person>") == false
    end

    test "validate_iri! raises on invalid IRI" do
      assert_raise ArgumentError, fn ->
        Namespaces.validate_iri!("http://example.org/Person>")
      end
    end
  end

  # ============================================================================
  # SchemaInfo Tests
  # ============================================================================

  describe "SchemaInfo struct" do
    test "creates with default values" do
      schema = SchemaInfo.new()

      assert schema.has_subclass == false
      assert schema.has_subproperty == false
      assert schema.transitive_properties == []
      assert schema.version != nil
    end

    test "creates with provided values" do
      schema =
        SchemaInfo.new(
          has_subclass: true,
          transitive_properties: ["http://example.org/contains"]
        )

      assert schema.has_subclass == true
      assert schema.transitive_properties == ["http://example.org/contains"]
    end

    test "limits property list sizes" do
      props = Enum.map(1..100, &"http://example.org/p#{&1}")

      schema =
        SchemaInfo.new(
          transitive_properties: props,
          max_properties: 10
        )

      assert length(schema.transitive_properties) == 10
    end

    test "converts from map" do
      map = %{
        has_subclass: true,
        has_subproperty: false,
        transitive_properties: ["http://example.org/contains"]
      }

      schema = SchemaInfo.from_map(map)

      assert schema.has_subclass == true
      assert schema.transitive_properties == ["http://example.org/contains"]
    end

    test "validates valid schema" do
      schema = SchemaInfo.new(has_subclass: true)
      assert {:ok, ^schema} = SchemaInfo.validate(schema)
    end

    test "validates invalid IRIs" do
      schema = %SchemaInfo{
        has_subclass: true,
        has_subproperty: false,
        has_domain: false,
        has_range: false,
        has_sameas: false,
        has_restrictions: false,
        transitive_properties: ["http://example.org/p>"],
        symmetric_properties: [],
        inverse_properties: [],
        functional_properties: [],
        inverse_functional_properties: [],
        version: "test"
      }

      assert {:error, {:invalid_iri, "http://example.org/p>"}} = SchemaInfo.validate(schema)
    end

    test "has_feature? checks features" do
      schema =
        SchemaInfo.new(
          has_subclass: true,
          transitive_properties: ["http://example.org/p"]
        )

      assert SchemaInfo.has_feature?(schema, :subclass) == true
      assert SchemaInfo.has_feature?(schema, :transitive_properties) == true
      assert SchemaInfo.has_feature?(schema, :subproperty) == false
      assert SchemaInfo.has_feature?(schema, :symmetric_properties) == false
    end

    test "property_count returns total" do
      schema =
        SchemaInfo.new(
          transitive_properties: ["a", "b"],
          symmetric_properties: ["c"]
        )

      assert SchemaInfo.property_count(schema) == 3
    end
  end

  # ============================================================================
  # Rule Validation Tests
  # ============================================================================

  describe "Rule.validate/1" do
    test "validates safe rule" do
      rule =
        Rule.new(
          :test,
          [{:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
        )

      assert {:ok, ^rule} = Rule.validate(rule)
    end

    test "detects unsafe rule" do
      rule =
        Rule.new(
          :test,
          [{:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}],
          # z not in body
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "z"}]}
        )

      assert {:error, errors} = Rule.validate(rule)
      assert :unsafe_rule in errors
    end

    test "detects invalid pattern structure" do
      rule =
        Rule.new(
          :test,
          # only 2 terms
          [{:pattern, [{:var, "x"}, {:var, "y"}]}],
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
        )

      assert {:error, errors} = Rule.validate(rule)
      assert :invalid_pattern_structure in errors
    end

    test "detects unsatisfiable conditions" do
      rule =
        Rule.new(
          :test,
          [
            {:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]},
            # same var, always false
            {:not_equal, {:var, "x"}, {:var, "x"}}
          ],
          {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
        )

      assert {:error, errors} = Rule.validate(rule)
      assert :unsatisfiable_condition in errors
    end

    test "all OWL 2 RL rules are valid" do
      for rule <- Rules.all_rules() do
        assert {:ok, _} = Rule.validate(rule), "Rule #{rule.name} should be valid"
      end
    end
  end

  # ============================================================================
  # Rule Explain Tests
  # ============================================================================

  describe "Rule.explain/1" do
    test "explains a rule" do
      rule = Rules.cax_sco()
      explanation = Rule.explain(rule)

      assert explanation =~ "Rule: cax_sco"
      assert explanation =~ "Description:"
      assert explanation =~ "Body"
      assert explanation =~ "Head"
      assert explanation =~ "Safe: true"
    end
  end

  describe "Rule.explain_applicability/2" do
    test "explains applicable rule" do
      rule = Rules.prp_trp()
      schema = %{transitive_properties: ["http://example.org/contains"]}

      assert {:applicable, msg} = Rule.explain_applicability(rule, schema)
      assert msg =~ "can apply"
    end

    test "explains not applicable rule" do
      rule = Rules.prp_trp()
      schema = %{transitive_properties: []}

      assert {:not_applicable, msg} = Rule.explain_applicability(rule, schema)
      assert msg =~ "requires"
    end
  end

  # ============================================================================
  # Delta Pattern Marking Tests
  # ============================================================================

  describe "Rule.mark_delta_positions/2" do
    test "marks default delta positions" do
      rule = Rules.scm_sco()
      marked = Rule.mark_delta_positions(rule)

      assert marked.metadata != nil
      assert marked.metadata.delta_positions == [0, 1]
    end

    test "marks custom delta positions" do
      rule = Rules.scm_sco()
      marked = Rule.mark_delta_positions(rule, [0])

      assert marked.metadata.delta_positions == [0]
    end
  end

  describe "Rule.delta_positions/1" do
    test "returns default positions for unmarked rule" do
      rule = Rules.scm_sco()
      positions = Rule.delta_positions(rule)

      # Should return all pattern positions
      assert length(positions) == 2
    end

    test "returns marked positions" do
      rule = Rules.scm_sco()
      marked = Rule.mark_delta_positions(rule, [1])

      assert Rule.delta_positions(marked) == [1]
    end
  end

  # ============================================================================
  # Blank Node Support Tests
  # ============================================================================

  describe "blank node support" do
    test "Rule.blank_node creates blank node term" do
      assert Rule.blank_node("b1") == {:blank_node, "b1"}
    end

    test "evaluate_condition handles is_blank" do
      binding = %{"x" => {:blank_node, "b1"}}

      assert Rule.evaluate_condition({:is_blank, {:var, "x"}}, binding) == true
      assert Rule.evaluate_condition({:is_iri, {:var, "x"}}, binding) == false
    end
  end

  # ============================================================================
  # Persistent Term Lifecycle Tests
  # ============================================================================

  describe "RuleCompiler persistent_term lifecycle" do
    test "list_stored returns empty initially" do
      # Clear any existing entries first
      RuleCompiler.clear_all()

      assert RuleCompiler.list_stored() == []
    end

    test "store and load work correctly" do
      schema = SchemaInfo.new(has_subclass: true)
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema)

      # Use unique key to avoid conflicts
      key = :"test_ontology_#{:erlang.unique_integer()}"

      assert :ok = RuleCompiler.store(compiled, key)
      assert RuleCompiler.exists?(key)
      assert {:ok, loaded} = RuleCompiler.load(key)
      assert loaded.profile == compiled.profile

      # Cleanup
      RuleCompiler.remove(key)
    end

    test "list_stored tracks stored keys" do
      RuleCompiler.clear_all()

      schema = SchemaInfo.new()
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema)

      key1 = :"test_key1_#{:erlang.unique_integer()}"
      key2 = :"test_key2_#{:erlang.unique_integer()}"

      RuleCompiler.store(compiled, key1)
      RuleCompiler.store(compiled, key2)

      stored = RuleCompiler.list_stored()
      assert key1 in stored
      assert key2 in stored

      # Cleanup
      RuleCompiler.clear_all()
    end

    test "stale? detects version mismatch" do
      schema = SchemaInfo.new()
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema)

      key = :"stale_test_#{:erlang.unique_integer()}"
      RuleCompiler.store(compiled, key)

      assert RuleCompiler.stale?(key, "wrong_version") == true
      assert RuleCompiler.stale?(key, compiled.version) == false

      # Cleanup
      RuleCompiler.remove(key)
    end
  end

  # ============================================================================
  # Telemetry Tests
  # ============================================================================

  describe "Telemetry module" do
    test "event_names returns all events" do
      events = Telemetry.event_names()

      assert [:triple_store, :reasoner, :compile, :start] in events
      assert [:triple_store, :reasoner, :compile, :stop] in events
      assert [:triple_store, :reasoner, :optimize, :stop] in events
    end

    test "span emits events" do
      test_pid = self()

      # Attach handler
      :telemetry.attach(
        "test-handler-#{:erlang.unique_integer()}",
        [:triple_store, :reasoner, :compile, :stop],
        fn _event, _measurements, metadata, _ ->
          send(test_pid, {:telemetry_event, metadata})
        end,
        nil
      )

      Telemetry.span(:compile, %{profile: :owl2rl}, fn ->
        %{rule_count: 5}
      end)

      assert_receive {:telemetry_event, metadata}
      assert metadata.profile == :owl2rl
      assert metadata.rule_count == 5
    end
  end

  # ============================================================================
  # RuleOptimizer Selectivity Constants Tests
  # ============================================================================

  describe "RuleOptimizer selectivity" do
    test "estimates selectivity using module attributes" do
      # Pattern with bound predicate should be more selective
      pattern1 = {:pattern, [{:var, "x"}, {:iri, "http://example.org/p"}, {:var, "y"}]}
      pattern2 = {:pattern, [{:var, "x"}, {:var, "p"}, {:var, "y"}]}

      sel1 = RuleOptimizer.estimate_selectivity(pattern1, MapSet.new(), %{})
      sel2 = RuleOptimizer.estimate_selectivity(pattern2, MapSet.new(), %{})

      # Pattern with constant predicate should be more selective (lower value)
      assert sel1 < sel2
    end

    test "bound variables increase selectivity" do
      pattern = {:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}

      sel_unbound = RuleOptimizer.estimate_selectivity(pattern, MapSet.new(), %{})
      sel_x_bound = RuleOptimizer.estimate_selectivity(pattern, MapSet.new(["x"]), %{})

      assert sel_x_bound < sel_unbound
    end
  end
end
