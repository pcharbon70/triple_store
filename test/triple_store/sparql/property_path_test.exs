defmodule TripleStore.SPARQL.PropertyPathTest do
  @moduledoc """
  Unit tests for property path evaluation.

  Tests cover:
  - Simple link paths (equivalent to triple patterns)
  - Sequence paths (p1/p2)
  - Alternative paths (p1|p2)
  - Inverse paths (^p)
  - Negated property sets (!(p1|p2))
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.PropertyPath
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :property_path

  setup do
    # Create unique temp directory for each test
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "property_path_test_#{test_id}")

    # Clean up any existing directory
    File.rm_rf!(db_path)

    # Open database
    {:ok, db} = NIF.open(db_path)

    # Start dictionary manager
    {:ok, dict_manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: dict_manager}

    on_exit(fn ->
      if Process.alive?(dict_manager) do
        Manager.stop(dict_manager)
      end

      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    {:ok, ctx: ctx}
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp insert_triple(ctx, {s, p, o}) do
    quads = [
      {:quad, {:named_node, s}, {:named_node, p}, to_ast(o), :default_graph}
    ]

    {:ok, _count} = UpdateExecutor.execute_insert_data(ctx, quads)
  end

  defp to_ast({:literal, value}), do: {:literal, :simple, value}
  defp to_ast({:named_node, iri}), do: {:named_node, iri}
  defp to_ast(iri) when is_binary(iri), do: {:named_node, iri}

  defp collect_results(stream) do
    stream |> Enum.to_list()
  end

  # ===========================================================================
  # Simple Link Tests
  # ===========================================================================

  describe "link path" do
    test "matches existing triple", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:link, "http://ex.org/knows"}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/alice"}
      assert binding["o"] == {:named_node, "http://ex.org/bob"}
    end

    test "returns empty for non-existent predicate", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:link, "http://ex.org/likes"}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert results == []
    end

    test "handles bound subject", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/charlie", "http://ex.org/knows", "http://ex.org/dave"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:link, "http://ex.org/knows"}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["o"] == {:named_node, "http://ex.org/bob"}
    end

    test "handles bound object", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/charlie", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:link, "http://ex.org/knows"}
      object = {:named_node, "http://ex.org/bob"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 2
      subjects = Enum.map(results, & &1["s"])
      assert {:named_node, "http://ex.org/alice"} in subjects
      assert {:named_node, "http://ex.org/charlie"} in subjects
    end
  end

  # ===========================================================================
  # Sequence Path Tests (Note: spargebra expands these)
  # ===========================================================================

  describe "sequence path" do
    test "matches two-hop path", %{ctx: ctx} do
      # alice -knows-> bob -knows-> charlie
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:variable, "s"}
      path = {:sequence, {:link, "http://ex.org/knows"}, {:link, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/alice"}
      assert binding["o"] == {:named_node, "http://ex.org/charlie"}
    end

    test "matches multiple paths", %{ctx: ctx} do
      # alice -p1-> x1 -p2-> target
      # alice -p1-> x2 -p2-> target
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p1", "http://ex.org/x1"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p1", "http://ex.org/x2"})
      insert_triple(ctx, {"http://ex.org/x1", "http://ex.org/p2", "http://ex.org/target"})
      insert_triple(ctx, {"http://ex.org/x2", "http://ex.org/p2", "http://ex.org/target"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:sequence, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Both paths lead to target
      assert length(results) == 2
      assert Enum.all?(results, fn b -> b["o"] == {:named_node, "http://ex.org/target"} end)
    end

    test "returns empty when path broken", %{ctx: ctx} do
      # alice -p1-> x1, but no p2 from x1
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p1", "http://ex.org/x1"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:sequence, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert results == []
    end

    test "sequence with different predicates", %{ctx: ctx} do
      # alice -friend-> bob -supervisor-> carol
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/friend", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/supervisor", "http://ex.org/carol"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:sequence, {:link, "http://ex.org/friend"}, {:link, "http://ex.org/supervisor"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/carol"}
    end
  end

  # ===========================================================================
  # Alternative Path Tests
  # ===========================================================================

  describe "alternative path" do
    test "matches first alternative", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p1", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:alternative, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["s"] == {:named_node, "http://ex.org/alice"}
      assert hd(results)["o"] == {:named_node, "http://ex.org/bob"}
    end

    test "matches second alternative", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p2", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:alternative, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["s"] == {:named_node, "http://ex.org/alice"}
    end

    test "matches both alternatives", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p1", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p2", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:alternative, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 2
      objects = Enum.map(results, & &1["o"])
      assert {:named_node, "http://ex.org/bob"} in objects
      assert {:named_node, "http://ex.org/charlie"} in objects
    end

    test "returns empty when neither alternative matches", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/p3", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:alternative, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert results == []
    end
  end

  # ===========================================================================
  # Inverse Path Tests
  # ===========================================================================

  describe "inverse path" do
    test "reverses predicate direction", %{ctx: ctx} do
      # alice -knows-> bob, query bob ^knows ?o should find alice
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/bob"}
      path = {:reverse, {:link, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/alice"}
    end

    test "finds all inverse edges", %{ctx: ctx} do
      # alice, charlie both -knows-> bob
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/charlie", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/bob"}
      path = {:reverse, {:link, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 2
      objects = Enum.map(results, & &1["o"])
      assert {:named_node, "http://ex.org/alice"} in objects
      assert {:named_node, "http://ex.org/charlie"} in objects
    end

    test "inverse with both endpoints unbound", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:reverse, {:link, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # s is bound to what was the object, o is bound to what was the subject
      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/bob"}
      assert binding["o"] == {:named_node, "http://ex.org/alice"}
    end
  end

  # ===========================================================================
  # Negated Property Set Tests
  # ===========================================================================

  describe "negated property set" do
    test "excludes single predicate", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:negated_property_set, ["http://ex.org/knows"]}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/charlie"}
    end

    test "excludes multiple predicates", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/charlie"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/works", "http://ex.org/company"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:negated_property_set, ["http://ex.org/knows", "http://ex.org/likes"]}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/company"}
    end

    test "returns all when no predicates excluded", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      # Exclude a predicate that doesn't exist
      path = {:negated_property_set, ["http://ex.org/nonexistent"]}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 2
    end

    test "returns empty when all predicates excluded", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:negated_property_set, ["http://ex.org/knows"]}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert results == []
    end
  end

  # ===========================================================================
  # Query Integration Tests
  # ===========================================================================

  describe "query integration" do
    test "alternative path via SPARQL query", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/charlie"})

      sparql = """
      SELECT ?o WHERE {
        <http://ex.org/alice> (<http://ex.org/knows>|<http://ex.org/likes>) ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      assert length(results) == 2
      objects = Enum.map(results, & &1["o"])
      assert {:named_node, "http://ex.org/bob"} in objects
      assert {:named_node, "http://ex.org/charlie"} in objects
    end

    test "negated property set via SPARQL query", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/charlie"})
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/works", "http://ex.org/company"})

      sparql = """
      SELECT ?o WHERE {
        <http://ex.org/alice> !(<http://ex.org/knows>|<http://ex.org/likes>) ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/company"}
    end

    test "sequence path via SPARQL query (expanded by parser)", %{ctx: ctx} do
      # spargebra expands sequence paths to BGP with blank nodes
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      sparql = """
      SELECT ?s ?o WHERE {
        ?s <http://ex.org/knows>/<http://ex.org/knows> ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/alice"}
      assert binding["o"] == {:named_node, "http://ex.org/charlie"}
    end

    test "inverse path via SPARQL query (optimized by parser)", %{ctx: ctx} do
      # spargebra optimizes simple inverse paths by swapping subject/object
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      sparql = """
      SELECT ?s WHERE {
        <http://ex.org/bob> ^<http://ex.org/knows> ?s
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      assert length(results) == 1
      assert hd(results)["s"] == {:named_node, "http://ex.org/alice"}
    end
  end

  # ===========================================================================
  # Edge Cases and Error Handling
  # ===========================================================================

  describe "edge cases" do
    test "empty database returns empty results", %{ctx: ctx} do
      subject = {:variable, "s"}
      path = {:link, "http://ex.org/knows"}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert results == []
    end

    test "handles existing binding", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/charlie", "http://ex.org/knows", "http://ex.org/dave"})

      subject = {:variable, "s"}
      path = {:link, "http://ex.org/knows"}
      object = {:variable, "o"}

      # Pre-bind subject
      initial_binding = %{"s" => {:named_node, "http://ex.org/alice"}}

      {:ok, stream} = PropertyPath.evaluate(ctx, initial_binding, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["s"] == {:named_node, "http://ex.org/alice"}
      assert hd(results)["o"] == {:named_node, "http://ex.org/bob"}
    end

    test "unsupported path returns error", %{ctx: ctx} do
      subject = {:variable, "s"}
      path = {:unknown_path_type, "test"}
      object = {:variable, "o"}

      assert {:error, {:unsupported_path, {:unknown_path_type, "test"}}} =
               PropertyPath.evaluate(ctx, %{}, subject, path, object)
    end
  end

  # ===========================================================================
  # Recursive Paths (Task 3.4.2)
  # ===========================================================================

  describe "zero-or-more path (p*)" do
    test "identity - zero steps (subject = object)", %{ctx: ctx} do
      # ?s knows* ?o with alice, should include alice -> alice (0 steps)
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Should include alice (0 steps) and bob (1 step)
      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
    end

    test "transitive closure - multiple steps", %{ctx: ctx} do
      # alice -> bob -> charlie -> dave
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})
      insert_triple(ctx, {"http://ex.org/charlie", "http://ex.org/knows", "http://ex.org/dave"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/dave"})
      assert MapSet.size(objects) == 4
    end

    test "handles cycles", %{ctx: ctx} do
      # alice -> bob -> alice (cycle)
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/alice"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Should find alice and bob, without infinite loop
      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.size(objects) == 2
    end

    test "both variables unbound", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Should include: alice->alice, alice->bob, bob->bob
      assert length(results) >= 3
    end

    test "reverse - object bound, subject unbound", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:variable, "s"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/charlie"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # charlie, bob, alice can all reach charlie
      subjects = Enum.map(results, & &1["s"]) |> MapSet.new()
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/alice"})
    end

    test "both bound - path exists", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/bob"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end

    test "both bound - identity (zero steps)", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/alice"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # alice knows* alice is true (0 steps)
      assert length(results) == 1
    end
  end

  describe "one-or-more path (p+)" do
    test "excludes identity - requires at least one step", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      # Should NOT include alice (requires at least one step)
      refute MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
    end

    test "transitive closure - multiple steps", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.size(objects) == 2
    end

    test "handles cycles", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/alice"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # With cycle, alice can reach both alice (2 steps) and bob (1 step)
      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
    end

    test "both bound - path exists", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/bob"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end

    test "both bound - same node without cycle, no result", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/alice"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # alice knows+ alice requires at least one step and path back
      assert length(results) == 0
    end

    test "reverse - object bound, subject unbound", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:variable, "s"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/charlie"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      subjects = Enum.map(results, & &1["s"]) |> MapSet.new()
      # charlie itself should NOT be included (one or more)
      refute MapSet.member?(subjects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/alice"})
    end
  end

  describe "zero-or-one path (p?)" do
    test "identity - zero steps", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      # Should include alice (0 steps) and bob (1 step)
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
    end

    test "does NOT follow transitive paths - only 0 or 1 step", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      # Should include alice and bob, but NOT charlie (that's 2 steps)
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      refute MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
    end

    test "both bound - identity match", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/alice"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end

    test "both bound - one step match", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/bob"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end

    test "both bound - no match (2 steps)", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/charlie"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Charlie is 2 steps from alice, so no match
      assert length(results) == 0
    end

    test "reverse - object bound, subject unbound", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})

      subject = {:variable, "s"}
      path = {:zero_or_one, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/bob"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      subjects = Enum.map(results, & &1["s"]) |> MapSet.new()
      # bob itself (0 steps) and alice (1 step)
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/alice"})
    end
  end

  describe "recursive path integration with SPARQL" do
    test "zero-or-more path via SPARQL query", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      sparql = """
      SELECT ?o WHERE {
        <http://ex.org/alice> <http://ex.org/knows>* ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      objects =
        results
        |> Enum.map(& &1["o"])
        |> MapSet.new()

      # alice (0 steps), bob (1 step), charlie (2 steps)
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
    end

    test "one-or-more path via SPARQL query", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      sparql = """
      SELECT ?o WHERE {
        <http://ex.org/alice> <http://ex.org/knows>+ ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      objects =
        results
        |> Enum.map(& &1["o"])
        |> MapSet.new()

      # bob (1 step), charlie (2 steps) - NOT alice
      refute MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
    end

    test "optional path via SPARQL query", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      sparql = """
      SELECT ?o WHERE {
        <http://ex.org/alice> <http://ex.org/knows>? ?o
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      objects =
        results
        |> Enum.map(& &1["o"])
        |> MapSet.new()

      # alice (0 steps), bob (1 step) - NOT charlie (2 steps)
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      refute MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
    end
  end

  describe "complex recursive paths" do
    test "find nodes that can reach target via knows path", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      # Find all nodes that can reach charlie via knows (uses knows*, NOT ^knows*)
      subject = {:variable, "s"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/charlie"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      subjects = Enum.map(results, & &1["s"]) |> MapSet.new()
      # charlie (0 steps), bob (1 step), alice (2 steps)
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(subjects, {:named_node, "http://ex.org/alice"})
    end

    test "inverse path with zero-or-more from bound subject", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob"})
      insert_triple(ctx, {"http://ex.org/bob", "http://ex.org/knows", "http://ex.org/charlie"})

      # From charlie, follow ^knows* (inverse knows) to find who charlie "is known by" transitively
      # ^knows from charlie means: find X where X knows charlie = bob
      # ^knows from bob means: find X where X knows bob = alice
      subject = {:named_node, "http://ex.org/charlie"}
      path = {:zero_or_more, {:reverse, {:named_node, "http://ex.org/knows"}}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      # charlie (0 steps), bob (1 step - bob knows charlie), alice (2 steps)
      assert MapSet.member?(objects, {:named_node, "http://ex.org/charlie"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/bob"})
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
    end

    test "self-loop with zero-or-more", %{ctx: ctx} do
      # Node with self-loop
      insert_triple(ctx, {"http://ex.org/alice", "http://ex.org/likes", "http://ex.org/alice"})

      subject = {:named_node, "http://ex.org/alice"}
      path = {:zero_or_more, {:named_node, "http://ex.org/likes"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Should just have alice (identity and via loop both lead to alice)
      objects = Enum.map(results, & &1["o"]) |> MapSet.new()
      assert MapSet.member?(objects, {:named_node, "http://ex.org/alice"})
      assert MapSet.size(objects) == 1
    end
  end

  # =========================================================================
  # Path Optimization Tests (Task 3.4.3)
  # =========================================================================

  describe "path optimization - fixed length paths" do
    test "fixed-length path p1/p2 optimization", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/p1", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/p2", "http://ex.org/c"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:sequence, {:named_node, "http://ex.org/p1"}, {:named_node, "http://ex.org/p2"}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/c"}
    end

    test "fixed-length path p1/p2/p3 optimization", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/p1", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/p2", "http://ex.org/c"})
      insert_triple(ctx, {"http://ex.org/c", "http://ex.org/p3", "http://ex.org/d"})

      subject = {:named_node, "http://ex.org/a"}
      path =
        {:sequence, {:named_node, "http://ex.org/p1"},
         {:sequence, {:named_node, "http://ex.org/p2"}, {:named_node, "http://ex.org/p3"}}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/d"}
    end

    test "fixed-length path with reverse predicate", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/p1", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/c", "http://ex.org/p2", "http://ex.org/b"})

      # a -[p1]-> b <-[p2]- c
      # Path: p1/^p2 from a should reach c
      subject = {:named_node, "http://ex.org/a"}
      path =
        {:sequence, {:named_node, "http://ex.org/p1"},
         {:reverse, {:named_node, "http://ex.org/p2"}}}
      object = {:variable, "o"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["o"] == {:named_node, "http://ex.org/c"}
    end

    test "fixed-length path with unbound subject", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/p1", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/p2", "http://ex.org/c"})

      subject = {:variable, "s"}
      path = {:sequence, {:named_node, "http://ex.org/p1"}, {:named_node, "http://ex.org/p2"}}
      object = {:named_node, "http://ex.org/c"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
      assert hd(results)["s"] == {:named_node, "http://ex.org/a"}
    end
  end

  describe "path optimization - bidirectional search" do
    test "bidirectional search finds path between bound endpoints", %{ctx: ctx} do
      # Create a longer path: a -> b -> c -> d -> e
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/knows", "http://ex.org/c"})
      insert_triple(ctx, {"http://ex.org/c", "http://ex.org/knows", "http://ex.org/d"})
      insert_triple(ctx, {"http://ex.org/d", "http://ex.org/knows", "http://ex.org/e"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/e"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Path exists from a to e
      assert length(results) == 1
    end

    test "bidirectional search returns no path when none exists", %{ctx: ctx} do
      # Create disconnected components: a -> b and c -> d
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/c", "http://ex.org/knows", "http://ex.org/d"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/d"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # No path from a to d
      assert length(results) == 0
    end

    test "bidirectional search handles cycle correctly", %{ctx: ctx} do
      # Create a cycle: a -> b -> c -> a
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/knows", "http://ex.org/c"})
      insert_triple(ctx, {"http://ex.org/c", "http://ex.org/knows", "http://ex.org/a"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/a"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Path back to a via the cycle
      assert length(results) == 1
    end

    test "bidirectional search with zero-or-more same node", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:zero_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/a"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      # Zero-or-more: identity path exists
      assert length(results) == 1
    end

    test "bidirectional search 1-hop path", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/b"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end

    test "bidirectional search 2-hop path meeting in middle", %{ctx: ctx} do
      insert_triple(ctx, {"http://ex.org/a", "http://ex.org/knows", "http://ex.org/b"})
      insert_triple(ctx, {"http://ex.org/b", "http://ex.org/knows", "http://ex.org/c"})

      subject = {:named_node, "http://ex.org/a"}
      path = {:one_or_more, {:named_node, "http://ex.org/knows"}}
      object = {:named_node, "http://ex.org/c"}

      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, subject, path, object)
      results = collect_results(stream)

      assert length(results) == 1
    end
  end
end
