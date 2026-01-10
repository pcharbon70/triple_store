defmodule TripleStore.Adapter.QuadConversionTest do
  @moduledoc """
  Tests for Section 2.1: Quad Term Conversion.

  Verifies that:
  - RDF.Quad with IRI graph converts correctly
  - RDF.Quad with blank node graph converts correctly
  - RDF.Quad with nil graph uses default_graph_id (0)
  - Internal quad converts to RDF.Quad correctly
  - default_graph_id (0) converts to nil graph in RDF.Quad
  - Batch conversion handles multiple quads
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager

  @test_db_base "/tmp/triple_store_adapter_quad_conversion_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # from_rdf_quad/2 Tests
  # ===========================================================================

  describe "from_rdf_quad/2" do
    test "converts quad with IRI graph", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
      assert is_integer(g_id)
      assert g_id > 0  # Named graph has ID > 0
    end

    test "converts quad with blank node graph", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.bnode("g1")

      quad = {subject, predicate, object, graph}
      {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
      assert is_integer(g_id)
      assert g_id > 0  # Named graph has ID > 0
    end

    test "converts quad with nil graph to default graph ID 0", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      quad = {subject, predicate, object, nil}
      {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
      assert g_id == 0  # Default graph is ID 0
    end

    test "converts quad with blank node subject and named graph", %{manager: manager} do
      subject = RDF.bnode("b1")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.iri("http://example.org/object")
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
      assert is_integer(g_id)
    end

    test "converts quad with inline-encoded integer object", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal(42)
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
      assert is_integer(g_id)
    end

    test "returns same IDs for same quad converted twice", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, {s1, p1, o1, g1}} = Adapter.from_rdf_quad(manager, quad)
      {:ok, {s2, p2, o2, g2}} = Adapter.from_rdf_quad(manager, quad)

      assert s1 == s2
      assert p1 == p2
      assert o1 == o2
      assert g1 == g2
    end
  end

  # ===========================================================================
  # to_rdf_quad/2 Tests
  # ===========================================================================

  describe "to_rdf_quad/2" do
    test "converts internal quad with IRI graph back to RDF quad", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, internal_quad} = Adapter.from_rdf_quad(manager, quad)

      {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, internal_quad)

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == "value"
      assert g == graph
    end

    test "converts internal quad with blank node graph roundtrip", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.bnode("g1")

      quad = {subject, predicate, object, graph}
      {:ok, internal_quad} = Adapter.from_rdf_quad(manager, quad)

      {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, internal_quad)

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == "value"
      assert g == graph
    end

    test "converts internal quad with default graph ID (0) to nil graph", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      quad = {subject, predicate, object, nil}
      {:ok, {s_id, p_id, o_id, 0}} = Adapter.from_rdf_quad(manager, quad)

      {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, {s_id, p_id, o_id, 0})

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == "value"
      assert g == nil  # Default graph ID 0 becomes nil
    end

    test "converts quad with blank node subject and object", %{db: db, manager: manager} do
      subject = RDF.bnode("b1")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.bnode("b2")
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, internal_quad} = Adapter.from_rdf_quad(manager, quad)

      {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, internal_quad)

      assert s == subject
      assert p == predicate
      assert o == object
      assert g == graph
    end

    test "converts quad with inline-encoded integer roundtrip", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal(42)
      graph = RDF.iri("http://example.org/graph")

      quad = {subject, predicate, object, graph}
      {:ok, internal_quad} = Adapter.from_rdf_quad(manager, quad)

      {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, internal_quad)

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == 42
      assert g == graph
    end

    test "returns :not_found for unknown term ID", %{db: db, manager: manager} do
      # Create some valid IDs, then use an unknown one
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")
      graph = RDF.iri("http://example.org/graph")

      {:ok, {s_id, p_id, o_id, _g_id}} = Adapter.from_rdf_quad(manager, {subject, predicate, object, graph})

      # Use a made-up graph ID that doesn't exist
      result = Adapter.to_rdf_quad(db, {s_id, p_id, o_id, 9_999_999})

      assert result == :not_found
    end
  end

  # ===========================================================================
  # from_rdf_quads/2 Tests (Batch Conversion)
  # ===========================================================================

  describe "from_rdf_quads/2" do
    test "converts empty list of quads", %{manager: manager} do
      assert {:ok, []} = Adapter.from_rdf_quads(manager, [])
    end

    test "converts multiple quads with mixed graph types", %{manager: manager} do
      quads = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.literal("o1"), RDF.iri("http://example.org/g1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"),
         RDF.literal("o2"), RDF.bnode("g2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"),
         RDF.literal("o3"), nil}  # Default graph
      ]

      {:ok, internal_quads} = Adapter.from_rdf_quads(manager, quads)

      assert length(internal_quads) == 3

      # First quad has named IRI graph
      {_, _, _, g1_id} = Enum.at(internal_quads, 0)
      assert g1_id > 0

      # Second quad has blank node graph
      {_, _, _, g2_id} = Enum.at(internal_quads, 1)
      assert g2_id > 0

      # Third quad has default graph
      {_, _, _, g3_id} = Enum.at(internal_quads, 2)
      assert g3_id == 0
    end

    test "handles quads with shared terms efficiently", %{manager: manager} do
      # All quads share the same predicate
      predicate = RDF.iri("http://example.org/common")

      quads = [
        {RDF.iri("http://example.org/s1"), predicate, RDF.literal("o1"),
         RDF.iri("http://example.org/g1")},
        {RDF.iri("http://example.org/s2"), predicate, RDF.literal("o2"),
         RDF.iri("http://example.org/g2")},
        {RDF.iri("http://example.org/s3"), predicate, RDF.literal("o3"),
         RDF.iri("http://example.org/g3")}
      ]

      {:ok, [{_, p1_id, _, _}, {_, p2_id, _, _}, {_, p3_id, _, _}]} =
        Adapter.from_rdf_quads(manager, quads)

      # All predicate IDs should be the same
      assert p1_id == p2_id
      assert p2_id == p3_id
    end
  end

  # ===========================================================================
  # to_rdf_quads/2 Tests (Batch Reverse Conversion)
  # ===========================================================================

  describe "to_rdf_quads/2" do
    test "converts empty list of internal quads", %{db: db} do
      assert {:ok, []} = Adapter.to_rdf_quads(db, [])
    end

    test "converts multiple internal quads back to RDF", %{db: db, manager: manager} do
      quads = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.literal("o1"), RDF.iri("http://example.org/g1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"),
         RDF.literal("o2"), RDF.bnode("g2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"),
         RDF.literal("o3"), nil}
      ]

      {:ok, internal_quads} = Adapter.from_rdf_quads(manager, quads)
      {:ok, result_quads} = Adapter.to_rdf_quads(db, internal_quads)

      assert length(result_quads) == 3

      # All quads should convert successfully (no :not_found)
      assert Enum.all?(result_quads, fn quad -> is_tuple(quad) end)

      # First quad: IRI graph
      {s1, p1, o1, g1} = Enum.at(result_quads, 0)
      assert s1.value == "http://example.org/s1"
      assert p1.value == "http://example.org/p"
      assert RDF.Literal.value(o1) == "o1"
      assert g1.value == "http://example.org/g1"

      # Second quad: blank node graph
      {_, _, _, g2} = Enum.at(result_quads, 1)
      assert g2.value == "g2"

      # Third quad: default graph (nil)
      {_, _, _, g3} = Enum.at(result_quads, 2)
      assert g3 == nil
    end

    test "handles quads with inline-encoded values in batch", %{db: db, manager: manager} do
      quads = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.literal(1), RDF.iri("http://example.org/g1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"),
         RDF.literal(2), RDF.iri("http://example.org/g2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"),
         RDF.literal(3), RDF.iri("http://example.org/g3")}
      ]

      {:ok, internal_quads} = Adapter.from_rdf_quads(manager, quads)
      {:ok, result_quads} = Adapter.to_rdf_quads(db, internal_quads)

      assert length(result_quads) == 3

      # All objects should be integers
      [{_, _, o1, _}, {_, _, o2, _}, {_, _, o3, _}] = result_quads
      assert RDF.Literal.value(o1) == 1
      assert RDF.Literal.value(o2) == 2
      assert RDF.Literal.value(o3) == 3
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip conversion" do
    test "quad with IRI graph converts back and forth", %{db: db, manager: manager} do
      original = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
                  RDF.literal("o"), RDF.iri("http://example.org/g")}

      {:ok, internal} = Adapter.from_rdf_quad(manager, original)
      {:ok, result} = Adapter.to_rdf_quad(db, internal)

      assert result == original
    end

    test "quad with blank node graph converts back and forth", %{db: db, manager: manager} do
      original = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
                  RDF.literal("o"), RDF.bnode("g1")}

      {:ok, internal} = Adapter.from_rdf_quad(manager, original)
      {:ok, result} = Adapter.to_rdf_quad(db, internal)

      assert result == original
    end

    test "quad with nil graph converts back and forth", %{db: db, manager: manager} do
      original = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
                  RDF.literal("o"), nil}

      {:ok, internal} = Adapter.from_rdf_quad(manager, original)
      {:ok, result} = Adapter.to_rdf_quad(db, internal)

      assert result == original
    end

    test "batch quads roundtrip correctly", %{db: db, manager: manager} do
      original_quads = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.literal("o1"), RDF.iri("http://example.org/g1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"),
         RDF.literal(42), RDF.bnode("g2")},
        {RDF.bnode("b1"), RDF.iri("http://example.org/p"),
         RDF.iri("http://example.org/o"), nil}
      ]

      {:ok, internal_quads} = Adapter.from_rdf_quads(manager, original_quads)
      {:ok, result_quads} = Adapter.to_rdf_quads(db, internal_quads)

      # All quads should roundtrip correctly
      assert length(result_quads) == length(original_quads)

      Enum.zip(result_quads, original_quads)
      |> Enum.each(fn {result, original} ->
        # Compare term values (ignoring struct differences)
        assert_term_equals(result, original)
      end)
    end
  end

  # ===========================================================================
  # Graph-Specific Tests
  # ===========================================================================

  describe "graph handling" do
    test "different graph names get different IDs", %{manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")
      g2 = RDF.iri("http://example.org/graph2")

      quad1 = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
               RDF.literal("o"), g1}
      quad2 = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
               RDF.literal("o"), g2}

      {:ok, {_, _, _, g1_id}} = Adapter.from_rdf_quad(manager, quad1)
      {:ok, {_, _, _, g2_id}} = Adapter.from_rdf_quad(manager, quad2)

      assert g1_id != g2_id
      assert g1_id > 0
      assert g2_id > 0
    end

    test "same graph name gets same ID across quads", %{manager: manager} do
      graph = RDF.iri("http://example.org/shared_graph")

      quad1 = {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
               RDF.literal("o1"), graph}
      quad2 = {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"),
               RDF.literal("o2"), graph}

      {:ok, {_, _, _, g1_id}} = Adapter.from_rdf_quad(manager, quad1)
      {:ok, {_, _, _, g2_id}} = Adapter.from_rdf_quad(manager, quad2)

      assert g1_id == g2_id
    end

    test "default graph ID is always 0 regardless of content", %{manager: manager} do
      quads = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"),
         RDF.literal("o1"), nil},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"),
         RDF.literal("o2"), nil},
        {RDF.bnode("b1"), RDF.iri("http://example.org/p3"),
         RDF.iri("http://example.org/o"), nil}
      ]

      {:ok, internal_quads} = Adapter.from_rdf_quads(manager, quads)

      Enum.each(internal_quads, fn {_, _, _, g_id} ->
        assert g_id == 0
      end)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp assert_term_equals({s1, p1, o1, g1}, {s2, p2, o2, g2}) do
    assert s1.value == s2.value
    assert p1.value == p2.value

    cond do
      o1.__struct__ == RDF.Literal and o2.__struct__ == RDF.Literal ->
        assert RDF.Literal.value(o1) == RDF.Literal.value(o2)

      true ->
        assert o1 == o2
    end

    cond do
      g1 == nil and g2 == nil ->
        :ok

      g1 != nil and g2 != nil ->
        assert g1.value == g2.value

      true ->
        flunk("Graph terms don't match: #{inspect(g1)} != #{inspect(g2)}")
    end
  end
end
