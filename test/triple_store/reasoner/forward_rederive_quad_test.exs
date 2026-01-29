defmodule TripleStore.Reasoner.ForwardRederiveQuadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.ForwardRederiveQuad
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
  # Tests: rederive_quads/5 - Basic Functionality
  # ============================================================================

  describe "rederive_quads/5 - basic functionality" do
    test "returns ok tuple with empty potentially invalid set" do
      potentially_invalid = MapSet.new()
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert is_map(result)
      assert MapSet.size(result.keep) == 0
      assert MapSet.size(result.delete) == 0
      assert result.quads_checked == 0
      assert result.rederivation_count == 0
    end

    test "returns result map with correct keys" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert Map.has_key?(result, :keep)
      assert Map.has_key?(result, :delete)
      assert Map.has_key?(result, :rederivation_count)
      assert Map.has_key?(result, :quads_checked)
    end
  end

  # ============================================================================
  # Tests: rederive_quads/5 - Graph Scope
  # ============================================================================

  describe "rederive_quads/5 - graph scope" do
    test "accepts graph_id option" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end

    test "accepts tbox_graph_id option" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 tbox_graph_id: 0
               )
    end

    test "accepts scope option" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 scope: :local
               )
    end
  end

  # ============================================================================
  # Tests: partition_invalid_quads/5
  # ============================================================================

  describe "partition_invalid_quads/5" do
    test "returns tuple of MapSets" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      result =
        ForwardRederiveQuad.partition_invalid_quads(:mock_db, potentially_invalid, deleted, rules,
          graph_id: 1
        )

      assert {keep, delete} = result
      assert is_map(keep)
      assert is_map(delete)
    end

    test "handles empty potentially invalid set" do
      potentially_invalid = MapSet.new()
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      {keep, delete} =
        ForwardRederiveQuad.partition_invalid_quads(:mock_db, potentially_invalid, deleted, rules,
          graph_id: 1
        )

      assert MapSet.size(keep) == 0
      assert MapSet.size(delete) == 0
    end
  end

  # ============================================================================
  # Tests: Return Values
  # ============================================================================

  describe "return values" do
    test "quads_checked equals size of potentially invalid set" do
      potentially_invalid =
        MapSet.new([
          quad(1, iri("alice"), rdf_type(), iri("Person")),
          quad(1, iri("bob"), rdf_type(), iri("Person"))
        ])

      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert result.quads_checked == 2
    end

    test "rederivation_count equals size of keep set" do
      potentially_invalid =
        MapSet.new([
          quad(1, iri("alice"), rdf_type(), iri("Person"))
        ])

      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert result.rederivation_count == MapSet.size(result.keep)
    end
  end

  # ============================================================================
  # Tests: Multiple Rules
  # ============================================================================

  describe "multiple rules" do
    test "handles multiple rules" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end
  end

  # ============================================================================
  # Tests: Deleted Quads
  # ============================================================================

  describe "deleted quads handling" do
    test "handles non-empty deleted set" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Student"))])
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end

    test "excludes deleted quads from valid facts" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Student"))])
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      # With mock DB, no facts will be found, so nothing can be re-derived
      assert result.rederivation_count == 0
    end
  end

  # ============================================================================
  # Tests: TBox Sharing
  # ============================================================================

  describe "TBox sharing" do
    test "handles nil tbox_graph_id" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 tbox_graph_id: nil
               )
    end

    test "handles tbox_graph_id option" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 tbox_graph_id: 0
               )
    end
  end

  # ============================================================================
  # Tests: Scope Handling
  # ============================================================================

  describe "scope handling" do
    test "handles local scope" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 scope: :local
               )
    end

    test "handles global scope" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1,
                 scope: :global
               )
    end

    test "defaults to local scope" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end
  end

  # ============================================================================
  # Tests: Keep/Delete Partitioning
  # ============================================================================

  describe "keep/delete partitioning" do
    test "returns empty keep and delete for empty input" do
      potentially_invalid = MapSet.new()
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert MapSet.size(result.keep) == 0
      assert MapSet.size(result.delete) == 0
    end

    test "partitions quads based on rederivability" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      # With mock DB, quads should go to delete set (cannot be re-derived without facts)
      assert MapSet.size(result.keep) + MapSet.size(result.delete) == 1
    end
  end

  # ============================================================================
  # Tests: Error Handling
  # ============================================================================

  describe "error handling" do
    test "requires graph_id option" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert_raise KeyError, fn ->
        ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules, [])
      end
    end
  end

  # ============================================================================
  # Tests: Type Checking
  # ============================================================================

  describe "type checking" do
    test "potentially_invalid accepts MapSet" do
      potentially_invalid = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end

    test "deleted accepts MapSet" do
      potentially_invalid = MapSet.new()
      deleted = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Student"))])
      rules = [Rules.cax_sco()]

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end

    test "rules accepts list" do
      potentially_invalid = MapSet.new()
      deleted = MapSet.new()
      rules = []

      assert {:ok, _result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )
    end
  end

  # ============================================================================
  # Tests: Large Input Sets
  # ============================================================================

  describe "large input sets" do
    test "handles multiple potentially invalid quads" do
      potentially_invalid =
        MapSet.new([
          quad(1, iri("alice"), rdf_type(), iri("Person")),
          quad(1, iri("bob"), rdf_type(), iri("Person")),
          quad(1, iri("charlie"), rdf_type(), iri("Person"))
        ])

      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      assert {:ok, result} =
               ForwardRederiveQuad.rederive_quads(:mock_db, potentially_invalid, deleted, rules,
                 graph_id: 1
               )

      assert result.quads_checked == 3
    end
  end
end
