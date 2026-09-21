defmodule TripleStore.SPARQL.Leapfrog.QuadScanPlanTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Leapfrog.QuadScanPlan

  describe "build/1" do
    test "selects the longest real prefix for every bound-position mask" do
      variable = {:variable, "v"}

      cases = [
        {[variable, variable, variable, variable], {3, :gspo, 0, <<>>}},
        {[1, variable, variable, variable], {3, :spog, 1, <<1::64-big>>}},
        {[variable, 2, variable, variable], {3, :posg, 1, <<2::64-big>>}},
        {[variable, variable, 3, variable], {3, :gspo, 0, <<>>}},
        {[variable, variable, variable, 4], {3, :gspo, 1, <<4::64-big>>}},
        {[1, 2, variable, variable], {3, :spog, 2, <<1::64-big, 2::64-big>>}},
        {[1, variable, 3, variable], {3, :spog, 1, <<1::64-big>>}},
        {[1, variable, variable, 4], {3, :gspo, 2, <<4::64-big, 1::64-big>>}},
        {[variable, 2, 3, variable], {3, :posg, 2, <<2::64-big, 3::64-big>>}},
        {[variable, 2, variable, 4], {3, :gpos, 2, <<4::64-big, 2::64-big>>}},
        {[variable, variable, 3, 4], {3, :gspo, 1, <<4::64-big>>}},
        {[1, 2, 3, variable], {3, :spog, 3, <<1::64-big, 2::64-big, 3::64-big>>}},
        {[1, 2, variable, 4], {3, :gspo, 3, <<4::64-big, 1::64-big, 2::64-big>>}},
        {[1, variable, 3, 4], {3, :gspo, 2, <<4::64-big, 1::64-big>>}},
        {[variable, 2, 3, 4], {3, :gpos, 3, <<4::64-big, 2::64-big, 3::64-big>>}}
      ]

      for {[s, p, o, g], expected} <- cases do
        assert {:ok, [^expected]} = QuadScanPlan.build({:quad, s, p, o, g})
      end

      assert {:ok, []} = QuadScanPlan.build({:quad, 1, 2, 3, 4})
    end

    test "normalizes compatibility inputs before planning" do
      assert {:ok, [{3, :gspo, 2, <<0::64-big, 1::64-big>>}]} =
               QuadScanPlan.build(
                 {:quad, {:bound, 1}, {:variable, "p"}, {:variable, "o"}, :default_graph}
               )

      assert {:ok, normalized} =
               QuadScanPlan.normalize({:quad, {:bound, nil}, 2, 3, :default_graph})

      assert QuadScanPlan.not_found?(normalized)
    end

    test "returns component-specific validation errors" do
      assert {:error, :invalid_quad_pattern} = QuadScanPlan.build({:triple, 1, 2, 3})

      assert {:error, {:invalid_quad_component, :object, -1}} =
               QuadScanPlan.build({:quad, 1, 2, -1, 0})
    end
  end

  describe "key decoding and binding construction" do
    test "decodes every physical index order into semantic order" do
      assert {1, 2, 3, 4} = QuadScanPlan.decode_key(<<4::64, 1::64, 2::64, 3::64>>, :gspo)
      assert {1, 2, 3, 4} = QuadScanPlan.decode_key(<<4::64, 2::64, 3::64, 1::64>>, :gpos)
      assert {1, 2, 3, 4} = QuadScanPlan.decode_key(<<1::64, 2::64, 3::64, 4::64>>, :spog)
      assert {1, 2, 3, 4} = QuadScanPlan.decode_key(<<2::64, 3::64, 1::64, 4::64>>, :posg)
    end

    test "builds binary-key bindings and enforces repeated variables" do
      key = <<7::64, 7::64, 9::64, 11::64>>
      equal_pattern = {:quad, {:variable, "same"}, {:variable, "same"}, {:variable, "o"}, 11}
      mismatch_pattern = {:quad, {:variable, "same"}, 8, {:variable, "o"}, 11}

      assert {:ok, %{"same" => 7, "o" => 9}} =
               QuadScanPlan.bindings_for_key(key, :spog, equal_pattern)

      assert :no_match = QuadScanPlan.bindings_for_key(key, :spog, mismatch_pattern)
    end

    test "omits anonymous variables from bindings" do
      key = <<4::64, 1::64, 2::64, 3::64>>
      pattern = {:quad, {:variable, "s"}, {:variable, "_"}, 3, 4}

      assert {:ok, %{"s" => 1}} = QuadScanPlan.bindings_for_key(key, :gspo, pattern)
    end
  end
end
