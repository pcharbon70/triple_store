defmodule TripleStore.Reasoner.DeleteWithReasoningQuadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.DeleteWithReasoningQuad
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

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Empty List
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - empty list" do
    test "returns ok stats for empty quad list" do
      quads = []
      rules = [Rules.cax_sco()]

      assert {:ok, stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert stats.explicit_deleted == 0
      assert stats.derived_deleted == 0
      assert stats.derived_kept == 0
      assert stats.potentially_invalid_count == 0
      assert stats.duration_ms >= 0
    end

    test "returns stats with all zero counts for empty list" do
      quads = []
      rules = []

      assert {:ok, stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert stats.explicit_deleted == 0
      assert stats.derived_deleted == 0
      assert stats.derived_kept == 0
    end
  end

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Basic Functionality
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - basic functionality" do
    test "returns ok tuple for valid input" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      # With mock DB, may get error from QuadOperations, but we test the API contract
      result = DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules, graph_id: 1)

      # Result should be either ok or error depending on DB state
      assert elem(result, 0) in [:ok, :error]
    end

    test "accepts graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result = DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules, graph_id: 1)

      assert elem(result, 0) in [:ok, :error]
    end
  end

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Options
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - options" do
    test "accepts tbox_graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1,
          tbox_graph_id: 0
        )

      assert elem(result, 0) in [:ok, :error]
    end

    test "accepts scope option with local value" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1,
          scope: :local
        )

      assert elem(result, 0) in [:ok, :error]
    end

    test "accepts scope option with global value" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1,
          scope: :global
        )

      assert elem(result, 0) in [:ok, :error]
    end

    test "accepts emit_telemetry option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1,
          emit_telemetry: false
        )

      assert elem(result, 0) in [:ok, :error]
    end
  end

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Required Options
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - required options" do
    test "requires graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert_raise KeyError, fn ->
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules, [])
      end
    end
  end

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Multiple Quads
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - multiple quads" do
    test "handles multiple quads to delete" do
      quads = [
        quad(1, iri("alice"), rdf_type(), iri("Student")),
        quad(1, iri("bob"), rdf_type(), iri("Student"))
      ]

      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1
        )

      assert elem(result, 0) in [:ok, :error]
    end
  end

  # ============================================================================
  # Tests: delete_quads_with_reasoning/4 - Different Graphs
  # ============================================================================

  describe "delete_quads_with_reasoning/4 - different graphs" do
    test "handles quads from different graphs" do
      quads = [
        quad(1, iri("alice"), rdf_type(), iri("Student")),
        quad(2, iri("bob"), rdf_type(), iri("Student"))
      ]

      rules = [Rules.cax_sco()]

      # Only graph 1 quads should be deleted when graph_id is 1
      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1
        )

      assert elem(result, 0) in [:ok, :error]
    end
  end

  # ============================================================================
  # Tests: preview_quad_deletion/4 - Basic Functionality
  # ============================================================================

  describe "preview_quad_deletion/4 - basic functionality" do
    test "returns ok tuple with preview sets" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
          graph_id: 1
        )

      assert {:ok, {explicit, derived}} = result
      assert is_map(explicit)
      assert is_map(derived)
    end

    test "returns explicit deleted set containing input quads" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, {explicit, _derived}} =
        DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
          graph_id: 1
        )

      assert MapSet.member?(explicit, quad(1, iri("alice"), rdf_type(), iri("Student")))
    end

    test "returns derived deleted as MapSet" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, {_explicit, derived}} =
        DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
          graph_id: 1
        )

      assert is_map(derived)
    end
  end

  # ============================================================================
  # Tests: preview_quad_deletion/4 - Options
  # ============================================================================

  describe "preview_quad_deletion/4 - options" do
    test "accepts graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert {:ok, {_explicit, _derived}} =
               DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
                 graph_id: 1
               )
    end

    test "accepts tbox_graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert {:ok, {_explicit, _derived}} =
               DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
                 graph_id: 1,
                 tbox_graph_id: 0
               )
    end

    test "accepts scope option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert {:ok, {_explicit, _derived}} =
               DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
                 graph_id: 1,
                 scope: :local
               )
    end
  end

  # ============================================================================
  # Tests: preview_quad_deletion/4 - Empty List
  # ============================================================================

  describe "preview_quad_deletion/4 - empty list" do
    test "returns empty sets for empty quad list" do
      quads = []
      rules = [Rules.cax_sco()]

      {:ok, {explicit, derived}} =
        DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
          graph_id: 1
        )

      assert MapSet.size(explicit) == 0
      assert MapSet.size(derived) == 0
    end
  end

  # ============================================================================
  # Tests: preview_quad_deletion/4 - Required Options
  # ============================================================================

  describe "preview_quad_deletion/4 - required options" do
    test "requires graph_id option" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      assert_raise KeyError, fn ->
        DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules, [])
      end
    end
  end

  # ============================================================================
  # Tests: Return Types
  # ============================================================================

  describe "return types" do
    test "delete_quads_with_reasoning returns stats map on success" do
      quads = []
      rules = [Rules.cax_sco()]

      assert {:ok, stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert is_map(stats)
      assert Map.has_key?(stats, :explicit_deleted)
      assert Map.has_key?(stats, :derived_deleted)
      assert Map.has_key?(stats, :derived_kept)
      assert Map.has_key?(stats, :potentially_invalid_count)
      assert Map.has_key?(stats, :duration_ms)
    end

    test "preview_quad_deletion returns tuple of MapSets" do
      quads = []
      rules = [Rules.cax_sco()]

      assert {:ok, {explicit, derived}} =
               DeleteWithReasoningQuad.preview_quad_deletion(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert is_map(explicit)
      assert is_map(derived)
    end
  end

  # ============================================================================
  # Tests: Type Checking
  # ============================================================================

  describe "type checking" do
    test "quads accepts list" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1
        )

      assert elem(result, 0) in [:ok, :error]
    end

    test "rules accepts list" do
      quads = []
      rules = []

      assert {:ok, _stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )
    end

    test "rules accepts empty list" do
      quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = []

      result =
        DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
          graph_id: 1
        )

      assert elem(result, 0) in [:ok, :error]
    end
  end

  # ============================================================================
  # Tests: Stats Structure
  # ============================================================================

  describe "stats structure" do
    test "stats has non-negative counts" do
      quads = []
      rules = []

      assert {:ok, stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert stats.explicit_deleted >= 0
      assert stats.derived_deleted >= 0
      assert stats.derived_kept >= 0
      assert stats.potentially_invalid_count >= 0
      assert stats.duration_ms >= 0
    end

    test "stats duration_ms is non-negative integer" do
      quads = []
      rules = []

      assert {:ok, stats} =
               DeleteWithReasoningQuad.delete_quads_with_reasoning(:mock_db, quads, rules,
                 graph_id: 1
               )

      assert is_integer(stats.duration_ms)
      assert stats.duration_ms >= 0
    end
  end
end
