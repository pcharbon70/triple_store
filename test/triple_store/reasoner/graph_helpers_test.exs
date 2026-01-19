defmodule TripleStore.Reasoner.GraphHelpersTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.GraphHelpers

  doctest GraphHelpers

  describe "graph_id/1" do
    test "returns {:ok, graph_id} when valid graph_id is provided" do
      assert {:ok, 0} = GraphHelpers.graph_id(graph_id: 0)
      assert {:ok, 1} = GraphHelpers.graph_id(graph_id: 1)
      assert {:ok, 999} = GraphHelpers.graph_id(graph_id: 999)
    end

    test "returns :error when graph_id is missing" do
      assert :error = GraphHelpers.graph_id([])
      assert :error = GraphHelpers.graph_id(other: :option)
    end

    test "returns :error when graph_id is invalid" do
      assert :error = GraphHelpers.graph_id(graph_id: -1)
      assert :error = GraphHelpers.graph_id(graph_id: :invalid)
      assert :error = GraphHelpers.graph_id(graph_id: "0")
    end
  end

  describe "graph_id/2 with default" do
    test "returns {:ok, default} when graph_id is missing" do
      assert {:ok, 0} = GraphHelpers.graph_id([], default: 0)
      assert {:ok, 5} = GraphHelpers.graph_id([], default: 5)
    end

    test "returns {:ok, provided} when graph_id is present" do
      assert {:ok, 10} = GraphHelpers.graph_id(graph_id: 10, default: 0)
    end

    test "returns :error when provided value is invalid" do
      assert :error = GraphHelpers.graph_id(graph_id: -1, default: 0)
    end
  end

  describe "graph_id!/1" do
    test "returns graph_id when present and valid" do
      assert 1 = GraphHelpers.graph_id!(graph_id: 1)
    end

    test "raises KeyError when graph_id is missing" do
      assert_raise KeyError, fn ->
        GraphHelpers.graph_id!([])
      end
    end
  end

  describe "tbox_graph_id/2" do
    test "returns {:ok, tbox_graph} when explicitly provided" do
      assert {:ok, 0} = GraphHelpers.tbox_graph_id([tbox_graph_id: 0], 1)
      assert {:ok, 5} = GraphHelpers.tbox_graph_id([tbox_graph_id: 5], nil)
    end

    test "returns {:ok, :shared} when not provided and no default graph" do
      assert {:ok, :shared} = GraphHelpers.tbox_graph_id([], nil)
    end

    test "returns {:ok, default_graph} when provided (takes precedence over :shared)" do
      assert {:ok, 1} = GraphHelpers.tbox_graph_id([], 1)
      assert {:ok, 10} = GraphHelpers.tbox_graph_id([], 10)
    end

    test "prefers explicit tbox_graph_id over default graph" do
      assert {:ok, 5} = GraphHelpers.tbox_graph_id([tbox_graph_id: 5], 10)
    end

    test "returns {:ok, :shared} when explicitly set to :shared" do
      assert {:ok, :shared} = GraphHelpers.tbox_graph_id([tbox_graph_id: :shared], 1)
    end

    test "returns :error for invalid tbox_graph_id" do
      assert :error = GraphHelpers.tbox_graph_id([tbox_graph_id: -1], 1)
      assert :error = GraphHelpers.tbox_graph_id([tbox_graph_id: :invalid], 1)
    end
  end

  describe "graph_ids/1" do
    test "returns {:ok, list} when valid graph_ids provided" do
      assert {:ok, [0, 1, 2]} = GraphHelpers.graph_ids(graph_ids: [0, 1, 2])
      assert {:ok, []} = GraphHelpers.graph_ids(graph_ids: [])
    end

    test "returns :error when graph_ids is missing" do
      assert :error = GraphHelpers.graph_ids([])
    end

    test "returns :error when graph_ids contains invalid values" do
      assert :error = GraphHelpers.graph_ids(graph_ids: [0, -1, 2])
      assert :error = GraphHelpers.graph_ids(graph_ids: [:invalid])
    end
  end

  describe "valid_graph_id?/1" do
    test "returns :ok for valid graph IDs" do
      assert :ok = GraphHelpers.valid_graph_id?(0)
      assert :ok = GraphHelpers.valid_graph_id?(1)
      assert :ok = GraphHelpers.valid_graph_id?(9999)
    end

    test "returns :ok for special atoms" do
      assert :ok = GraphHelpers.valid_graph_id?(:global)
      assert :ok = GraphHelpers.valid_graph_id?(:shared)
    end

    test "returns :error for invalid values" do
      assert :error = GraphHelpers.valid_graph_id?(-1)
      assert :error = GraphHelpers.valid_graph_id?(:invalid)
      assert :error = GraphHelpers.valid_graph_id?("0")
    end
  end

  describe "is_graph_ref?/1" do
    test "returns true for non-negative integers" do
      assert GraphHelpers.is_graph_ref?(0)
      assert GraphHelpers.is_graph_ref?(1)
      assert GraphHelpers.is_graph_ref?(9999)
    end

    test "returns true for special atoms" do
      assert GraphHelpers.is_graph_ref?(:global)
      assert GraphHelpers.is_graph_ref?(:shared)
    end

    test "returns false for invalid values" do
      refute GraphHelpers.is_graph_ref?(-1)
      refute GraphHelpers.is_graph_ref?(:invalid)
      refute GraphHelpers.is_graph_ref?("0")
      refute GraphHelpers.is_graph_ref?(nil)
    end
  end

  describe "scope/1" do
    test "returns {:ok, scope} when valid scope provided" do
      assert {:ok, :local} = GraphHelpers.scope(scope: :local)
      assert {:ok, :global} = GraphHelpers.scope(scope: :global)
      assert {:ok, :hybrid} = GraphHelpers.scope(scope: :hybrid)
    end

    test "returns {:ok, :local} as default when not provided" do
      assert {:ok, :local} = GraphHelpers.scope([])
    end

    test "returns :error for invalid scope" do
      assert :error = GraphHelpers.scope(scope: :invalid)
    end
  end

  describe "scope/2 with default" do
    test "uses provided default" do
      assert {:ok, :global} = GraphHelpers.scope([], default: :global)
      assert {:ok, :hybrid} = GraphHelpers.scope([], default: :hybrid)
    end

    test "prefers explicit scope over default" do
      assert {:ok, :local} = GraphHelpers.scope([scope: :local], default: :global)
    end
  end

  describe "inferred_graph/1" do
    test "returns {:ok, value} for valid options" do
      assert {:ok, 100} = GraphHelpers.inferred_graph(inferred_graph: 100)
      assert {:ok, :separate} = GraphHelpers.inferred_graph(inferred_graph: :separate)
      assert {:ok, nil} = GraphHelpers.inferred_graph(inferred_graph: nil)
      assert {:ok, nil} = GraphHelpers.inferred_graph(inferred_graph: :self)
    end

    test "returns {:ok, nil} when not provided" do
      assert {:ok, nil} = GraphHelpers.inferred_graph([])
    end

    test "returns :error for invalid values" do
      assert :error = GraphHelpers.inferred_graph(inferred_graph: -1)
      assert :error = GraphHelpers.inferred_graph(inferred_graph: :invalid)
    end
  end
end
