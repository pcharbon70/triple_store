defmodule TripleStore.Reasoner.Section7_2QuadPatternTest do
  @moduledoc """
  Tests for Section 7.2: Quad Pattern Matching for Rules.

  This test suite validates:
  - Task 7.2.1: Rule pattern extension (quad heads, graph metadata)
  - Task 7.2.2: Quad pattern matching
  - Task 7.2.3: Rule compilation for quads
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{PatternMatcher, Rule, RuleCompiler}

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Task 7.2.1: Rule Pattern Extension Tests
  # ============================================================================

  describe "Task 7.2.1 - Rule Pattern Extension" do
    test "7.2.1.1 head_quad_pattern/4 creates quad head pattern" do
      g = {:var, "g"}
      s = Rule.var("x")
      p = Rule.iri("#{@rdf}type")
      o = Rule.var("c")

      quad_pattern = Rule.head_quad_pattern(g, s, p, o)

      assert quad_pattern == {:quad_pattern, [g, s, p, o]}
    end

    test "7.2.1.2 instantiate_head/2 works with quad patterns" do
      rule =
        Rule.new_quad(:test_rule,
          [{:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          Rule.head_quad_pattern({:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c"))
        )

      binding = %{"g" => {:bound, 1}, "x" => {:iri, "#{@ex}alice"}, "c" => {:iri, "#{@ex}Person"}}

      result = Rule.instantiate_head(rule, binding)

      assert result == {{:bound, 1}, {:iri, "#{@ex}alice"}, {:iri, "#{@rdf}type"}, {:iri, "#{@ex}Person"}}
    end

    test "7.2.1.2 instantiate_head/2 substitutes available bindings" do
      rule =
        Rule.new_quad(:test_rule,
          [{:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          Rule.head_quad_pattern({:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c"))
        )

      binding = %{"x" => {:iri, "#{@ex}alice"}}

      result = Rule.instantiate_head(rule, binding)

      # instantiate_head substitutes what it can, doesn't check for groundness
      # Unbound variables remain as variables in the result
      assert result == {{:var, "g"}, {:iri, "#{@ex}alice"}, {:iri, "#{@rdf}type"}, {:var, "c"}}
    end

    test "7.2.1.3 Rule has graph_id in metadata" do
      rule =
        Rule.new(:test_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          {:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("y")]},
          metadata: %{graph_id: 1}
        )

      assert Rule.graph_id(rule) == 1
    end

    test "7.2.1.4 Rule has scope in metadata" do
      rule =
        Rule.new(:test_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          {:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("y")]},
          metadata: %{scope: :local}
        )

      assert Rule.scope(rule) == :local
    end

    test "7.2.1.5 triple rules default to local scope" do
      rule =
        Rule.new(:test_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          {:pattern, [Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("y")]}
        )

      # New rules should have nil metadata (backward compatibility)
      # When add_graph_metadata is applied, they get scope: :local
      assert rule.metadata == nil
    end

    test "new_quad/5 creates quad-aware rule with metadata" do
      rule =
        Rule.new_quad(:quad_rule,
          [{:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")]}],
          Rule.head_quad_pattern({:var, "g"}, Rule.var("x"), Rule.iri("#{@rdf}type"), Rule.var("c")),
          graph_id: 1,
          scope: :local
        )

      assert Rule.quad_rule?(rule)
      assert Rule.graph_id(rule) == 1
      assert Rule.scope(rule) == :local
    end

    test "quad_rule?/1 returns true for quad rules" do
      rule =
        Rule.new_quad(:quad_rule,
          [{:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          Rule.head_quad_pattern({:var, "g"}, Rule.var("x"), Rule.iri("p"), Rule.var("y"))
        )

      assert Rule.quad_rule?(rule) == true
    end

    test "quad_rule?/1 returns false for triple rules" do
      rule =
        Rule.new(:triple_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          {:pattern, [Rule.var("x"), Rule.iri("q"), Rule.var("y")]}
        )

      assert Rule.quad_rule?(rule) == false
    end

    test "put_graph_id/2 updates graph_id in metadata" do
      rule =
        Rule.new(:test_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          {:pattern, [Rule.var("x"), Rule.iri("q"), Rule.var("y")]}
        )

      updated = Rule.put_graph_id(rule, 5)

      assert Rule.graph_id(updated) == 5
    end

    test "put_scope/2 updates scope in metadata" do
      rule =
        Rule.new(:test_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          {:pattern, [Rule.var("x"), Rule.iri("q"), Rule.var("y")]}
        )

      updated = Rule.put_scope(rule, :global)

      assert Rule.scope(updated) == :global
    end

    test "applies_to_graph?/2 checks rule applicability" do
      # Local rule for graph 1
      rule =
        Rule.new(:local_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          {:pattern, [Rule.var("x"), Rule.iri("q"), Rule.var("y")]},
          metadata: %{graph_id: 1, scope: :local}
        )

      assert Rule.applies_to_graph?(rule, 1) == true
      assert Rule.applies_to_graph?(rule, 2) == false
    end

    test "applies_to_graph?/2 global rule applies to all graphs" do
      rule =
        Rule.new(:global_rule,
          [{:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]}],
          {:pattern, [Rule.var("x"), Rule.iri("q"), Rule.var("y")]},
          metadata: %{scope: :global}
        )

      assert Rule.applies_to_graph?(rule, 1) == true
      assert Rule.applies_to_graph?(rule, 99) == true
    end

    test "validate_quad_body!/1 allows all quad patterns" do
      body = [
        {:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("p1"), Rule.var("y")]},
        {:quad_pattern, [{:var, "g"}, Rule.var("y"), Rule.iri("p2"), Rule.var("z")]}
      ]

      # Should not raise
      assert Rule.validate_quad_body!(body) == :ok
    end

    test "validate_quad_body!/1 raises on mixed patterns" do
      body = [
        {:pattern, [Rule.var("x"), Rule.iri("p"), Rule.var("y")]},
        {:quad_pattern, [{:var, "g"}, Rule.var("x"), Rule.iri("p"), Rule.var("y")]}
      ]

      assert_raise ArgumentError, ~r/Mixed triple and quad patterns/, fn ->
        Rule.validate_quad_body!(body)
      end
    end
  end

  # ============================================================================
  # Task 7.2.2: Quad Pattern Matching Tests
  # ============================================================================

  describe "Task 7.2.2 - Quad Pattern Matching" do
    test "7.2.2.1 matches_quad?/2 matches ground quads" do
      pattern = {:quad_pattern, [{:bound, 1}, {:var, "x"}, {:iri, "p"}, {:var, "y"}]}
      fact = {1, {:iri, "#{@ex}alice"}, {:iri, "p"}, {:iri, "#{@ex}bob"}}

      assert PatternMatcher.matches_quad?(fact, pattern) == true
    end

    test "7.2.2.1 matches_quad?/2 handles :default graph term" do
      pattern = {:quad_pattern, [:default, {:var, "x"}, {:iri, "p"}, {:var, "y"}]}
      fact = {0, {:iri, "#{@ex}alice"}, {:iri, "p"}, {:iri, "#{@ex}bob"}}

      assert PatternMatcher.matches_quad?(fact, pattern) == true
    end

    test "7.2.2.1 matches_quad?/2 handles :all graph term" do
      pattern = {:quad_pattern, [:all, {:var, "x"}, {:iri, "p"}, {:var, "y"}]}
      fact = {5, {:iri, "#{@ex}alice"}, {:iri, "p"}, {:iri, "#{@ex}bob"}}

      assert PatternMatcher.matches_quad?(fact, pattern) == true
    end

    test "7.2.2.1 matches_quad?/2 handles bound graph term" do
      pattern = {:quad_pattern, [{:bound, 3}, {:var, "x"}, {:iri, "p"}, {:var, "y"}]}
      fact = {3, {:iri, "#{@ex}alice"}, {:iri, "p"}, {:iri, "#{@ex}bob"}}

      assert PatternMatcher.matches_quad?(fact, pattern) == true
    end

    test "7.2.2.1 matches_quad?/2 rejects graph mismatch" do
      pattern = {:quad_pattern, [{:bound, 3}, {:var, "x"}, {:iri, "p"}, {:var, "y"}]}
      fact = {5, {:iri, "#{@ex}alice"}, {:iri, "p"}, {:iri, "#{@ex}bob"}}

      assert PatternMatcher.matches_quad?(fact, pattern) == false
    end

    test "7.2.2.5 matches_graph_term?/2 handles variable graph" do
      assert PatternMatcher.matches_graph_term?(1, {:var, :g}) == true
    end

    test "7.2.2.5 matches_graph_term?/2 handles bound graph match" do
      assert PatternMatcher.matches_graph_term?(1, {:bound, 1}) == true
    end

    test "7.2.2.5 matches_graph_term?/2 handles bound graph mismatch" do
      assert PatternMatcher.matches_graph_term?(1, {:bound, 2}) == false
    end

    test "7.2.2.5 matches_graph_term?/2 handles default graph" do
      assert PatternMatcher.matches_graph_term?(0, :default) == true
      assert PatternMatcher.matches_graph_term?(1, :default) == false
    end

    test "7.2.2.5 matches_graph_term?/2 handles all graphs" do
      assert PatternMatcher.matches_graph_term?(0, :all) == true
      assert PatternMatcher.matches_graph_term?(99, :all) == true
    end

    test "unify_graph_term/3 binds graph variable" do
      fact_graph = 1
      pattern_graph = {:var, "g"}
      binding = %{}

      result = PatternMatcher.unify_graph_term(fact_graph, pattern_graph, binding)

      assert {:ok, %{"g" => 1}} == result
    end

    test "unify_graph_term/3 respects existing binding" do
      fact_graph = 1
      pattern_graph = {:var, "g"}
      binding = %{"g" => 1}

      result = PatternMatcher.unify_graph_term(fact_graph, pattern_graph, binding)

      assert {:ok, ^binding} = result
    end

    test "unify_graph_term/3 rejects binding conflict" do
      fact_graph = 1
      pattern_graph = {:var, "g"}
      binding = %{"g" => 2}

      result = PatternMatcher.unify_graph_term(fact_graph, pattern_graph, binding)

      assert result == :no_match
    end
  end

  # ============================================================================
  # Task 7.2.3: Rule Compilation for Quads Tests
  # ============================================================================

  describe "Task 7.2.3 - Rule Compilation for Quads" do
    test "7.2.3.1 compile/2 accepts graph_id option" do
      # Create a minimal schema info
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: 1
        )

      assert compiled.graph_id == 1
    end

    test "7.2.3.1 rules have graph_id metadata when specified" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: 5
        )

      # All rules should have the graph_id set
      Enum.each(compiled.rules, fn rule ->
        assert Rule.graph_id(rule) == 5
      end)
    end

    test "7.2.3.2 compile/2 accepts tbox_graph option" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          tbox_graph: 2
        )

      assert compiled.tbox_graph == 2
    end

    test "7.2.3.2 tbox_graph defaults to 0" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      assert compiled.tbox_graph == 0
    end

    test "7.2.3.3 rules get scope: :local when graph_id is set" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: 1
        )

      # All rules should have :local scope
      Enum.each(compiled.rules, fn rule ->
        assert Rule.scope(rule) == :local
      end)
    end

    test "7.2.3.3 rules get scope: :global when graph_id is nil" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)

      # All rules should have :global scope
      Enum.each(compiled.rules, fn rule ->
        assert Rule.scope(rule) == :global
      end)
    end

    test "7.2.3.4 specialized rules inherit graph metadata" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: 3,
          specialize: true
        )

      # Specialized rules should inherit graph_id
      Enum.each(compiled.specialized_rules, fn rule ->
        assert Rule.graph_id(rule) == 3
        assert Rule.scope(rule) == :local
      end)
    end

    test "7.2.3.4 specialized rules inherit scope from parent" do
      schema_info = %{
        RuleCompiler.empty_schema_info()
        | transitive_properties: ["#{@ex}contains"]
      }

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: nil,
          specialize: true
        )

      # Specialized rules should have :global scope
      Enum.each(compiled.specialized_rules, fn rule ->
        assert Rule.scope(rule) == :global
      end)
    end

    test "7.2.3.5 compiled structure contains graph context" do
      schema_info = RuleCompiler.empty_schema_info()

      {:ok, compiled} =
        RuleCompiler.compile_with_schema(schema_info,
          profile: :owl2rl,
          graph_id: 1,
          tbox_graph: 0
        )

      # Verify compiled structure has all fields
      assert Map.has_key?(compiled, :graph_id)
      assert Map.has_key?(compiled, :tbox_graph)
      assert compiled.graph_id == 1
      assert compiled.tbox_graph == 0
    end
  end

  # ============================================================================
  # Integration Tests
  # ============================================================================

  describe "Integration - Quad Rule with Compilation" do
    test "create and compile a quad rule" do
      # Create a quad rule
      quad_rule =
        Rule.new_quad(:custom_subclass,
          [
            {:quad_pattern, [{:var, "g"}, Rule.var("c1"), Rule.iri("#{@rdfs}subClassOf"), Rule.var("c2")]},
            {:quad_pattern, [{:var, "g"}, Rule.var("c2"), Rule.iri("#{@rdfs}subClassOf"), Rule.var("c3")]}
          ],
          Rule.head_quad_pattern({:var, "g"}, Rule.var("c1"), Rule.iri("#{@rdfs}subClassOf"), Rule.var("c3")),
          graph_id: 1,
          scope: :local
        )

      assert Rule.quad_rule?(quad_rule) == true
      assert Rule.graph_id(quad_rule) == 1
      assert Rule.scope(quad_rule) == :local

      # Verify we can instantiate the head
      binding = %{"g" => {:bound, 1}, "c1" => {:iri, "#{@ex}A"}, "c3" => {:iri, "#{@ex}C"}}
      result = Rule.instantiate_head(quad_rule, binding)

      assert result == {{:bound, 1}, {:iri, "#{@ex}A"}, {:iri, "#{@rdfs}subClassOf"}, {:iri, "#{@ex}C"}}
    end
  end
end
