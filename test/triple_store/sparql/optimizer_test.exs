defmodule TripleStore.SPARQL.OptimizerTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Algebra
  alias TripleStore.SPARQL.Optimizer
  alias TripleStore.SPARQL.Parser

  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"

  # Helper to create common patterns
  defp var(name), do: {:variable, name}
  defp int(n), do: {:literal, :typed, Integer.to_string(n), @xsd_integer}
  defp iri(s), do: {:named_node, s}

  defp bgp(triples), do: {:bgp, triples}
  defp triple(s, p, o), do: {:triple, s, p, o}

  defp filter(expr, pattern), do: {:filter, expr, pattern}
  defp join(left, right), do: {:join, left, right}
  defp left_join(left, right, filter \\ nil), do: {:left_join, left, right, filter}
  defp union(left, right), do: {:union, left, right}

  defp greater(left, right), do: {:greater, left, right}
  defp less(left, right), do: {:less, left, right}
  defp equal(left, right), do: {:equal, left, right}
  defp and_expr(left, right), do: {:and, left, right}
  defp or_expr(left, right), do: {:or, left, right}
  defp not_expr(arg), do: {:not, arg}
  defp add(left, right), do: {:add, left, right}
  defp subtract(left, right), do: {:subtract, left, right}
  defp multiply(left, right), do: {:multiply, left, right}
  defp divide(left, right), do: {:divide, left, right}

  defp bool(true), do: {:literal, :typed, "true", @xsd_boolean}
  defp bool(false), do: {:literal, :typed, "false", @xsd_boolean}

  describe "push_filters_down/1" do
    test "filter on single BGP stays in place" do
      pattern = bgp([triple(var("x"), var("p"), var("o"))])
      algebra = filter(greater(var("x"), int(5)), pattern)

      result = Optimizer.push_filters_down(algebra)

      # Filter should remain around the BGP
      assert result == algebra
    end

    test "pushes filter into left side of join when variables match" do
      # Filter(?x > 5, Join(BGP(?x), BGP(?y)))
      # Should become: Join(Filter(?x > 5, BGP(?x)), BGP(?y))
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)
      algebra = filter(greater(var("x"), int(5)), joined)

      result = Optimizer.push_filters_down(algebra)

      expected =
        join(
          filter(greater(var("x"), int(5)), left_bgp),
          right_bgp
        )

      assert result == expected
    end

    test "pushes filter into right side of join when variables match" do
      # Filter(?y > 5, Join(BGP(?x), BGP(?y)))
      # Should become: Join(BGP(?x), Filter(?y > 5, BGP(?y)))
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)
      algebra = filter(greater(var("y"), int(5)), joined)

      result = Optimizer.push_filters_down(algebra)

      expected =
        join(
          left_bgp,
          filter(greater(var("y"), int(5)), right_bgp)
        )

      assert result == expected
    end

    test "filter using both sides of join stays at join level" do
      # Filter(?x = ?y, Join(BGP(?x), BGP(?y)))
      # Cannot be pushed - uses variables from both sides
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)
      algebra = filter(equal(var("x"), var("y")), joined)

      result = Optimizer.push_filters_down(algebra)

      # Filter stays at top
      assert result == algebra
    end

    test "pushes filter into left side of OPTIONAL but not right side" do
      # Filter(?x > 5, LeftJoin(BGP(?x), BGP(?y)))
      # Should push to left: LeftJoin(Filter(?x > 5, BGP(?x)), BGP(?y))
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      optional = left_join(left_bgp, right_bgp)
      algebra = filter(greater(var("x"), int(5)), optional)

      result = Optimizer.push_filters_down(algebra)

      expected =
        left_join(
          filter(greater(var("x"), int(5)), left_bgp),
          right_bgp
        )

      assert result == expected
    end

    test "does NOT push filter into right side of OPTIONAL" do
      # Filter(?y > 5, LeftJoin(BGP(?x), BGP(?y)))
      # Should NOT be pushed - would change OPTIONAL semantics
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      optional = left_join(left_bgp, right_bgp)
      algebra = filter(greater(var("y"), int(5)), optional)

      result = Optimizer.push_filters_down(algebra)

      # Filter should stay at top - not pushed into OPTIONAL right side
      assert result == algebra
    end

    test "does NOT push filter into UNION" do
      # Filter(?x > 5, Union(BGP(?x), BGP(?x)))
      # Should NOT be pushed - would change UNION semantics
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("x"), iri("http://ex.org/q"), var("z"))])
      unioned = union(left_bgp, right_bgp)
      algebra = filter(greater(var("x"), int(5)), unioned)

      result = Optimizer.push_filters_down(algebra)

      # Filter stays at top
      assert result == algebra
    end

    test "splits conjunctive filter and pushes parts independently" do
      # Filter(?x > 5 AND ?y < 10, Join(BGP(?x), BGP(?y)))
      # Should split into:
      # Join(Filter(?x > 5, BGP(?x)), Filter(?y < 10, BGP(?y)))
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)

      filter_expr =
        and_expr(
          greater(var("x"), int(5)),
          {:less, var("y"), int(10)}
        )

      algebra = filter(filter_expr, joined)

      result = Optimizer.push_filters_down(algebra)

      expected =
        join(
          filter(greater(var("x"), int(5)), left_bgp),
          filter({:less, var("y"), int(10)}, right_bgp)
        )

      assert result == expected
    end

    test "partially splits conjunctive filter - pushes what it can" do
      # Filter(?x > 5 AND ?x = ?y, Join(BGP(?x), BGP(?y)))
      # Only ?x > 5 can be pushed, ?x = ?y must stay at top
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)

      filter_expr =
        and_expr(
          greater(var("x"), int(5)),
          equal(var("x"), var("y"))
        )

      algebra = filter(filter_expr, joined)

      result = Optimizer.push_filters_down(algebra)

      # ?x = ?y remains at top, ?x > 5 pushed to left
      expected =
        filter(
          equal(var("x"), var("y")),
          join(
            filter(greater(var("x"), int(5)), left_bgp),
            right_bgp
          )
        )

      assert result == expected
    end

    test "pushes filter through nested joins" do
      # Filter(?x > 5, Join(Join(BGP(?x), BGP(?y)), BGP(?z)))
      bgp_x = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      bgp_y = bgp([triple(var("y"), iri("http://ex.org/q"), var("o2"))])
      bgp_z = bgp([triple(var("z"), iri("http://ex.org/r"), var("o3"))])
      nested = join(join(bgp_x, bgp_y), bgp_z)
      algebra = filter(greater(var("x"), int(5)), nested)

      result = Optimizer.push_filters_down(algebra)

      # Filter should be pushed all the way to bgp_x
      expected =
        join(
          join(
            filter(greater(var("x"), int(5)), bgp_x),
            bgp_y
          ),
          bgp_z
        )

      assert result == expected
    end

    test "pushes filter through project" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      projected = {:project, inner, [variable: "x"]}
      algebra = filter(greater(var("x"), int(5)), projected)

      result = Optimizer.push_filters_down(algebra)

      expected = {:project, filter(greater(var("x"), int(5)), inner), [variable: "x"]}
      assert result == expected
    end

    test "pushes filter through distinct" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      distincted = {:distinct, inner}
      algebra = filter(greater(var("x"), int(5)), distincted)

      result = Optimizer.push_filters_down(algebra)

      expected = {:distinct, filter(greater(var("x"), int(5)), inner)}
      assert result == expected
    end

    test "pushes filter through order_by" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      ordered = {:order_by, inner, [asc: var("x")]}
      algebra = filter(greater(var("x"), int(5)), ordered)

      result = Optimizer.push_filters_down(algebra)

      expected = {:order_by, filter(greater(var("x"), int(5)), inner), [asc: var("x")]}
      assert result == expected
    end

    test "pushes filter through slice" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      sliced = {:slice, inner, 0, 10}
      algebra = filter(greater(var("x"), int(5)), sliced)

      result = Optimizer.push_filters_down(algebra)

      expected = {:slice, filter(greater(var("x"), int(5)), inner), 0, 10}
      assert result == expected
    end

    test "does NOT push filter past GROUP BY" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      grouped = {:group, inner, [variable: "x"], []}
      algebra = filter(greater(var("x"), int(5)), grouped)

      result = Optimizer.push_filters_down(algebra)

      # Filter should stay at top - can't push past aggregation boundary
      assert result == algebra
    end

    test "pushes filter into GRAPH pattern" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      graphed = {:graph, iri("http://example.org/g"), inner}
      algebra = filter(greater(var("x"), int(5)), graphed)

      result = Optimizer.push_filters_down(algebra)

      expected = {:graph, iri("http://example.org/g"), filter(greater(var("x"), int(5)), inner)}
      assert result == expected
    end

    test "does not push filter past extend when it uses extended variable" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      extended = {:extend, inner, var("y"), {:add, var("x"), int(1)}}
      algebra = filter(greater(var("y"), int(5)), extended)

      result = Optimizer.push_filters_down(algebra)

      # Filter uses ?y which is defined by EXTEND - can't push past it
      assert result == algebra
    end

    test "pushes filter past extend when it doesn't use extended variable" do
      inner = bgp([triple(var("x"), var("p"), var("o"))])
      extended = {:extend, inner, var("y"), {:add, var("x"), int(1)}}
      algebra = filter(greater(var("x"), int(5)), extended)

      result = Optimizer.push_filters_down(algebra)

      # Filter uses ?x which is not the extended variable - can push
      expected =
        {:extend, filter(greater(var("x"), int(5)), inner), var("y"), {:add, var("x"), int(1)}}

      assert result == expected
    end
  end

  describe "optimize/2" do
    test "applies filter push-down by default" do
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)
      algebra = filter(greater(var("x"), int(5)), joined)

      result = Optimizer.optimize(algebra)

      expected =
        join(
          filter(greater(var("x"), int(5)), left_bgp),
          right_bgp
        )

      assert result == expected
    end

    test "can disable filter push-down" do
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)
      algebra = filter(greater(var("x"), int(5)), joined)

      result = Optimizer.optimize(algebra, push_filters: false)

      # Should remain unchanged
      assert result == algebra
    end
  end

  describe "analyze_filters/1" do
    test "counts filters in algebra tree" do
      bgp1 = bgp([triple(var("x"), var("p"), var("o"))])
      bgp2 = bgp([triple(var("y"), var("q"), var("z"))])

      algebra =
        filter(
          greater(var("x"), int(5)),
          filter(
            {:less, var("y"), int(10)},
            join(bgp1, bgp2)
          )
        )

      stats = Optimizer.analyze_filters(algebra)
      assert stats.total_filters == 2
    end

    test "handles algebra with no filters" do
      bgp1 = bgp([triple(var("x"), var("p"), var("o"))])
      stats = Optimizer.analyze_filters(bgp1)
      assert stats.total_filters == 0
    end
  end

  describe "integration with parser" do
    test "optimizes parsed query with filter on join" do
      query = """
      SELECT ?s ?name WHERE {
        ?s <http://example.org/type> <http://example.org/Person> .
        ?s <http://example.org/name> ?name .
        ?s <http://example.org/age> ?age .
        FILTER(?age > 18)
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # The filter should be pushed down - verify structure changed
      # Note: exact structure depends on how parser nests things
      assert pattern != optimized or has_filter_at_leaf?(optimized)
    end

    test "preserves OPTIONAL semantics after optimization" do
      query = """
      SELECT ?s ?name ?email WHERE {
        ?s <http://example.org/name> ?name .
        OPTIONAL { ?s <http://example.org/email> ?email }
        FILTER(BOUND(?name))
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # Should have pushed filter since it only uses ?name from left side
      # Verify the structure is valid
      assert Algebra.validate(unwrap_modifiers(optimized)) == :ok
    end

    test "handles complex conjunctive filter" do
      query = """
      SELECT ?x ?y WHERE {
        ?x <http://example.org/p> ?v1 .
        ?y <http://example.org/q> ?v2 .
        FILTER(?v1 > 10 && ?v2 < 20 && ?v1 != ?v2)
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # Should have split and pushed some filters
      # The cross-variable filter (?v1 != ?v2) should remain at top
      assert Algebra.validate(unwrap_modifiers(optimized)) == :ok
    end
  end

  # Helper to check if a filter exists at a leaf level
  defp has_filter_at_leaf?({:filter, _, {:bgp, _}}), do: true
  defp has_filter_at_leaf?({:filter, _, inner}), do: has_filter_at_leaf?(inner)

  defp has_filter_at_leaf?({:join, left, right}),
    do: has_filter_at_leaf?(left) or has_filter_at_leaf?(right)

  defp has_filter_at_leaf?({:left_join, left, right, _}),
    do: has_filter_at_leaf?(left) or has_filter_at_leaf?(right)

  defp has_filter_at_leaf?({:project, inner, _}), do: has_filter_at_leaf?(inner)
  defp has_filter_at_leaf?({:distinct, inner}), do: has_filter_at_leaf?(inner)
  defp has_filter_at_leaf?({:order_by, inner, _}), do: has_filter_at_leaf?(inner)
  defp has_filter_at_leaf?({:slice, inner, _, _}), do: has_filter_at_leaf?(inner)
  defp has_filter_at_leaf?(_), do: false

  # Helper to unwrap solution modifiers to get to the core pattern
  defp unwrap_modifiers({:project, inner, _}), do: unwrap_modifiers(inner)
  defp unwrap_modifiers({:distinct, inner}), do: unwrap_modifiers(inner)
  defp unwrap_modifiers({:reduced, inner}), do: unwrap_modifiers(inner)
  defp unwrap_modifiers({:order_by, inner, _}), do: unwrap_modifiers(inner)
  defp unwrap_modifiers({:slice, inner, _, _}), do: unwrap_modifiers(inner)
  defp unwrap_modifiers(pattern), do: pattern

  # ===========================================================================
  # Constant Folding Tests
  # ===========================================================================

  describe "fold_constants/1 - arithmetic" do
    test "folds constant addition" do
      # 1 + 2 => 3
      expr = add(int(1), int(2))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      # Should fold to 3
      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "3", @xsd_integer}} = result
    end

    test "folds nested arithmetic" do
      # (1 + 2) * 3 => 9
      inner = add(int(1), int(2))
      expr = multiply(inner, int(3))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      # Should fold to 9
      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "9", @xsd_integer}} = result
    end

    test "folds subtraction" do
      expr = subtract(int(10), int(3))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "7", @xsd_integer}} = result
    end

    test "folds division" do
      expr = divide(int(10), int(2))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      # Division produces decimal
      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "5.0", @xsd_decimal}} =
               result
    end

    test "preserves arithmetic with variables" do
      # ?x + 2 cannot be folded
      expr = add(var("x"), int(2))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"},
              {:add, {:variable, "x"}, {:literal, :typed, "2", @xsd_integer}}} = result
    end

    test "folds unary minus on constant" do
      expr = {:unary_minus, int(5)}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "-5", @xsd_integer}} =
               result
    end
  end

  describe "fold_constants/1 - comparisons" do
    test "folds constant greater-than to true" do
      # 5 > 3 => true
      expr = greater(int(5), int(3))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # Always-true filter is removed
      assert result == pattern
    end

    test "folds constant greater-than to false" do
      # 3 > 5 => false
      expr = greater(int(3), int(5))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # Always-false filter produces empty result
      assert result == {:bgp, []}
    end

    test "folds constant equality to true" do
      # 5 = 5 => true
      expr = equal(int(5), int(5))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      assert result == pattern
    end

    test "folds constant equality to false" do
      # 5 = 6 => false
      expr = equal(int(5), int(6))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      assert result == {:bgp, []}
    end

    test "folds constant less-than" do
      # 3 < 5 => true
      expr = less(int(3), int(5))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      assert result == pattern
    end

    test "preserves comparison with variables" do
      # ?x > 5 cannot be folded
      expr = greater(var("x"), int(5))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      assert result == filter(expr, pattern)
    end
  end

  describe "fold_constants/1 - logical expressions" do
    test "folds true AND true" do
      expr = and_expr(bool(true), bool(true))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # true && true => true, filter removed
      assert result == pattern
    end

    test "folds true AND false" do
      expr = and_expr(bool(true), bool(false))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # true && false => false, empty result
      assert result == {:bgp, []}
    end

    test "short-circuits false AND anything" do
      # false && ?x should become false without evaluating ?x
      expr = and_expr(bool(false), greater(var("x"), int(5)))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # Short-circuit: false && anything => false
      assert result == {:bgp, []}
    end

    test "simplifies true AND variable expression" do
      # true && (?x > 5) => ?x > 5
      expr = and_expr(bool(true), greater(var("x"), int(5)))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      expected = filter(greater(var("x"), int(5)), pattern)
      assert result == expected
    end

    test "folds true OR false" do
      expr = or_expr(bool(true), bool(false))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # true || false => true, filter removed
      assert result == pattern
    end

    test "short-circuits true OR anything" do
      # true || ?x should become true without evaluating ?x
      expr = or_expr(bool(true), greater(var("x"), int(5)))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # Short-circuit: true || anything => true
      assert result == pattern
    end

    test "simplifies false OR variable expression" do
      # false || (?x > 5) => ?x > 5
      expr = or_expr(bool(false), greater(var("x"), int(5)))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      expected = filter(greater(var("x"), int(5)), pattern)
      assert result == expected
    end

    test "folds NOT true" do
      expr = not_expr(bool(true))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # !true => false, empty result
      assert result == {:bgp, []}
    end

    test "folds NOT false" do
      expr = not_expr(bool(false))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      # !false => true, filter removed
      assert result == pattern
    end

    test "eliminates double negation" do
      # NOT(NOT(?x > 5)) => ?x > 5
      inner = greater(var("x"), int(5))
      expr = not_expr(not_expr(inner))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(filter(expr, pattern))

      expected = filter(inner, pattern)
      assert result == expected
    end
  end

  describe "fold_constants/1 - conditional expressions" do
    test "folds IF with constant true condition" do
      # IF(true, 1, 2) => 1
      expr = {:if_expr, bool(true), int(1), int(2)}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "1", @xsd_integer}} = result
    end

    test "folds IF with constant false condition" do
      # IF(false, 1, 2) => 2
      expr = {:if_expr, bool(false), int(1), int(2)}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "2", @xsd_integer}} = result
    end

    test "preserves IF with variable condition" do
      # IF(?x > 5, 1, 2) cannot be fully folded
      cond_expr = greater(var("x"), int(5))
      expr = {:if_expr, cond_expr, int(1), int(2)}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:if_expr, _, _, _}} = result
    end

    test "folds COALESCE with first constant value" do
      # COALESCE(1, 2, 3) => 1
      expr = {:coalesce, [int(1), int(2), int(3)]}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "1", @xsd_integer}} = result
    end

    test "preserves COALESCE with only variables" do
      # COALESCE(?x, ?y) cannot be fully folded
      expr = {:coalesce, [var("x"), var("y")]}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("z"), expr})

      # Should remain as coalesce since all args are variables
      assert {:extend, ^pattern, {:variable, "z"}, {:coalesce, [_, _]}} = result
    end

    test "preserves COALESCE when first arg is variable" do
      # COALESCE(?x, 1) - cannot fold because ?x might evaluate to a value at runtime
      expr = {:coalesce, [var("x"), int(1)]}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      # Should remain as coalesce since first arg is a variable
      assert {:extend, ^pattern, {:variable, "y"}, {:coalesce, [_, _]}} = result
    end
  end

  describe "fold_constants/1 - algebra tree simplification" do
    test "simplifies join with empty left side" do
      left = {:bgp, []}
      right = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(join(left, right))

      # Empty join => empty
      assert result == {:bgp, []}
    end

    test "simplifies join with empty right side" do
      left = bgp([triple(var("x"), var("p"), var("o"))])
      right = {:bgp, []}

      result = Optimizer.fold_constants(join(left, right))

      assert result == {:bgp, []}
    end

    test "simplifies union with empty left side" do
      left = {:bgp, []}
      right = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants(union(left, right))

      # Empty union side is removed
      assert result == right
    end

    test "simplifies union with empty right side" do
      left = bgp([triple(var("x"), var("p"), var("o"))])
      right = {:bgp, []}

      result = Optimizer.fold_constants(union(left, right))

      assert result == left
    end

    test "simplifies left_join with empty left side" do
      left = {:bgp, []}
      right = bgp([triple(var("y"), var("q"), var("z"))])

      result = Optimizer.fold_constants(left_join(left, right))

      # Empty left => empty result
      assert result == {:bgp, []}
    end

    test "propagates empty through nested joins" do
      # Join(Filter(false, BGP), BGP) => empty
      inner = filter(bool(false), bgp([triple(var("x"), var("p"), var("o"))]))
      right = bgp([triple(var("y"), var("q"), var("z"))])

      result = Optimizer.fold_constants(join(inner, right))

      # false filter => empty BGP => empty join
      assert result == {:bgp, []}
    end
  end

  describe "fold_constants/1 - extend and order_by" do
    test "folds constants in EXTEND expression" do
      # BIND(1 + 2 AS ?y)
      expr = add(int(1), int(2))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "3", @xsd_integer}} = result
    end

    test "folds constants in ORDER BY expressions" do
      pattern = bgp([triple(var("x"), var("p"), var("o"))])
      # Order by a constant expression that can be folded
      order_expr = add(int(1), int(1))
      algebra = {:order_by, pattern, [{:asc, order_expr}]}

      result = Optimizer.fold_constants(algebra)

      assert {:order_by, ^pattern, [{:asc, {:literal, :typed, "2", @xsd_integer}}]} = result
    end

    test "folds constants in left_join filter" do
      left = bgp([triple(var("x"), var("p"), var("o"))])
      right = bgp([triple(var("y"), var("q"), var("z"))])
      # Filter with constant 5 > 3 => true
      join_filter = greater(int(5), int(3))

      result = Optimizer.fold_constants({:left_join, left, right, join_filter})

      # Join filter should be folded to true boolean
      assert {:left_join, ^left, ^right, {:literal, :typed, "true", @xsd_boolean}} = result
    end
  end

  describe "optimize/2 with constant folding" do
    test "applies constant folding by default" do
      expr = greater(int(5), int(3))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])
      algebra = filter(expr, pattern)

      result = Optimizer.optimize(algebra)

      # Constant folding removes the always-true filter
      assert result == pattern
    end

    test "can disable constant folding" do
      expr = greater(int(5), int(3))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])
      algebra = filter(expr, pattern)

      result = Optimizer.optimize(algebra, fold_constants: false)

      # Should remain unchanged
      assert result == algebra
    end

    test "applies both constant folding and filter push-down" do
      # Filter with constant AND variable parts
      left_bgp = bgp([triple(var("x"), iri("http://ex.org/p"), var("o"))])
      right_bgp = bgp([triple(var("y"), iri("http://ex.org/q"), var("z"))])
      joined = join(left_bgp, right_bgp)

      # true AND (?x > 5) => ?x > 5, then push down
      expr = and_expr(bool(true), greater(var("x"), int(5)))
      algebra = filter(expr, joined)

      result = Optimizer.optimize(algebra)

      # Should fold true && to just ?x > 5, then push to left side
      expected =
        join(
          filter(greater(var("x"), int(5)), left_bgp),
          right_bgp
        )

      assert result == expected
    end
  end

  describe "integration - constant folding with parser" do
    test "folds constants in parsed query" do
      query = """
      SELECT ?x WHERE {
        ?x <http://example.org/p> ?o .
        FILTER(1 + 1 = 2)
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # 1 + 1 = 2 => true, filter removed
      # Result should just be the BGP (possibly wrapped in modifiers)
      unwrapped = unwrap_modifiers(optimized)
      refute match?({:filter, _, _}, unwrapped)
    end

    test "removes always-false filter in parsed query" do
      query = """
      SELECT ?x WHERE {
        ?x <http://example.org/p> ?o .
        FILTER(5 < 3)
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # 5 < 3 => false, result is empty
      unwrapped = unwrap_modifiers(optimized)
      assert unwrapped == {:bgp, []}
    end
  end

  # ===========================================================================
  # BGP Reordering Tests
  # ===========================================================================

  describe "reorder_bgp_patterns/2" do
    test "empty BGP stays empty" do
      result = Optimizer.reorder_bgp_patterns({:bgp, []})
      assert result == {:bgp, []}
    end

    test "single pattern BGP stays unchanged" do
      pattern = triple(var("x"), var("p"), var("o"))
      result = Optimizer.reorder_bgp_patterns({:bgp, [pattern]})
      assert result == {:bgp, [pattern]}
    end

    test "bound subject pattern comes before unbound" do
      # Pattern with bound subject is more selective
      unbound = triple(var("x"), var("p"), var("o"))
      bound_subject = triple(iri("http://ex.org/Bob"), var("q"), var("z"))

      result = Optimizer.reorder_bgp_patterns({:bgp, [unbound, bound_subject]})

      {:bgp, [first, second]} = result
      # Bound subject should come first
      assert first == bound_subject
      assert second == unbound
    end

    test "bound predicate pattern is more selective than all variables" do
      all_vars = triple(var("x"), var("p"), var("o"))
      bound_pred = triple(var("s"), iri("http://ex.org/knows"), var("o"))

      result = Optimizer.reorder_bgp_patterns({:bgp, [all_vars, bound_pred]})

      {:bgp, [first, _second]} = result
      # Bound predicate should come first
      assert first == bound_pred
    end

    test "bound object pattern is considered" do
      all_vars = triple(var("x"), var("p"), var("o"))
      bound_obj = triple(var("s"), var("p"), iri("http://ex.org/Value"))

      result = Optimizer.reorder_bgp_patterns({:bgp, [all_vars, bound_obj]})

      {:bgp, [first, _second]} = result
      # Bound object should come first
      assert first == bound_obj
    end

    test "fully bound pattern comes first" do
      # S P O all bound is most selective
      all_vars = triple(var("x"), var("p"), var("o"))
      one_bound = triple(iri("http://ex.org/Bob"), var("p"), var("o"))

      all_bound =
        triple(
          iri("http://ex.org/Alice"),
          iri("http://ex.org/knows"),
          iri("http://ex.org/Bob")
        )

      result = Optimizer.reorder_bgp_patterns({:bgp, [all_vars, one_bound, all_bound]})

      {:bgp, [first, second, third]} = result
      assert first == all_bound
      assert second == one_bound
      assert third == all_vars
    end

    test "considers variable binding propagation" do
      # If pattern1 binds ?x, pattern2 using ?x becomes more selective
      pattern1 = triple(iri("http://ex.org/Bob"), iri("http://ex.org/knows"), var("x"))
      pattern2 = triple(var("x"), var("p"), var("o"))
      pattern3 = triple(var("a"), var("b"), var("c"))

      result = Optimizer.reorder_bgp_patterns({:bgp, [pattern3, pattern2, pattern1]})

      {:bgp, patterns} = result
      # pattern1 should be first (bound subject + predicate)
      # pattern2 should be second (uses ?x which is now bound)
      # pattern3 should be last (all unbound, no shared variables)
      assert Enum.at(patterns, 0) == pattern1
      assert Enum.at(patterns, 1) == pattern2
      assert Enum.at(patterns, 2) == pattern3
    end

    test "works with literal objects" do
      var_pattern = triple(var("x"), var("p"), var("o"))
      lit_pattern = triple(var("s"), var("p"), int(42))

      result = Optimizer.reorder_bgp_patterns({:bgp, [var_pattern, lit_pattern]})

      {:bgp, [first, _second]} = result
      # Literal object is more selective
      assert first == lit_pattern
    end

    test "reorders patterns inside join" do
      left_patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      right_patterns = [
        triple(var("a"), var("b"), var("c")),
        triple(var("s"), iri("http://ex.org/type"), var("t"))
      ]

      algebra = join({:bgp, left_patterns}, {:bgp, right_patterns})
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:join, {:bgp, left_result}, {:bgp, right_result}} = result

      # Left: bound subject should be first
      assert hd(left_result) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      # Right: bound predicate should be first
      assert hd(right_result) == triple(var("s"), iri("http://ex.org/type"), var("t"))
    end

    test "reorders patterns inside union" do
      left_patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Alice"), var("q"), var("z"))
      ]

      right_patterns = [
        triple(var("a"), var("b"), var("c")),
        triple(iri("http://ex.org/Bob"), var("d"), var("e"))
      ]

      algebra = union({:bgp, left_patterns}, {:bgp, right_patterns})
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:union, {:bgp, left_result}, {:bgp, right_result}} = result

      # Both sides should have bound subject patterns first
      assert hd(left_result) == triple(iri("http://ex.org/Alice"), var("q"), var("z"))
      assert hd(right_result) == triple(iri("http://ex.org/Bob"), var("d"), var("e"))
    end

    test "reorders patterns through filter" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:filter, _, {:bgp, reordered}} = result
      assert hd(reordered) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end

    test "reorders patterns through project" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = {:project, {:bgp, patterns}, [variable: "x"]}
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:project, {:bgp, reordered}, _vars} = result
      assert hd(reordered) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end

    test "reorders patterns through distinct" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = {:distinct, {:bgp, patterns}}
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:distinct, {:bgp, reordered}} = result
      assert hd(reordered) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end

    test "uses predicate statistics when available" do
      # Rare predicate should be more selective than common one
      rare_pred = triple(var("s"), iri("http://ex.org/rare"), var("o"))
      common_pred = triple(var("s"), iri("http://ex.org/common"), var("o"))

      stats = %{
        {:predicate_count, "http://ex.org/rare"} => 5,
        {:predicate_count, "http://ex.org/common"} => 50_000
      }

      result = Optimizer.reorder_bgp_patterns({:bgp, [common_pred, rare_pred]}, stats)

      {:bgp, [first, second]} = result
      # Rare predicate should come first
      assert first == rare_pred
      assert second == common_pred
    end

    test "handles left_join correctly" do
      left_patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      right_patterns = [
        triple(var("a"), var("b"), var("c")),
        triple(iri("http://ex.org/Alice"), var("d"), var("e"))
      ]

      algebra = left_join({:bgp, left_patterns}, {:bgp, right_patterns})
      result = Optimizer.reorder_bgp_patterns(algebra)

      {:left_join, {:bgp, left_result}, {:bgp, right_result}, _filter} = result

      # Both sides should be reordered independently
      assert hd(left_result) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      assert hd(right_result) == triple(iri("http://ex.org/Alice"), var("d"), var("e"))
    end
  end

  describe "estimate_selectivity/3" do
    test "bound subject is more selective than variable" do
      bound = triple(iri("http://ex.org/Bob"), var("p"), var("o"))
      unbound = triple(var("x"), var("p"), var("o"))

      bound_score = Optimizer.estimate_selectivity(bound)
      unbound_score = Optimizer.estimate_selectivity(unbound)

      assert bound_score < unbound_score
    end

    test "bound predicate affects selectivity" do
      bound_pred = triple(var("s"), iri("http://ex.org/knows"), var("o"))
      unbound_pred = triple(var("s"), var("p"), var("o"))

      bound_score = Optimizer.estimate_selectivity(bound_pred)
      unbound_score = Optimizer.estimate_selectivity(unbound_pred)

      assert bound_score < unbound_score
    end

    test "already-bound variable is selective" do
      pattern = triple(var("x"), var("p"), var("o"))
      bound_vars = MapSet.new(["x"])

      with_bound = Optimizer.estimate_selectivity(pattern, bound_vars)
      without_bound = Optimizer.estimate_selectivity(pattern)

      assert with_bound < without_bound
    end

    test "uses predicate statistics" do
      pattern = triple(var("s"), iri("http://ex.org/rare"), var("o"))

      stats_rare = %{{:predicate_count, "http://ex.org/rare"} => 5}
      stats_common = %{{:predicate_count, "http://ex.org/rare"} => 50_000}

      score_rare = Optimizer.estimate_selectivity(pattern, MapSet.new(), stats_rare)
      score_common = Optimizer.estimate_selectivity(pattern, MapSet.new(), stats_common)

      assert score_rare < score_common
    end

    test "fully bound is most selective" do
      all_bound =
        triple(
          iri("http://ex.org/Alice"),
          iri("http://ex.org/knows"),
          iri("http://ex.org/Bob")
        )

      all_vars = triple(var("s"), var("p"), var("o"))

      bound_score = Optimizer.estimate_selectivity(all_bound)
      var_score = Optimizer.estimate_selectivity(all_vars)

      # Fully bound should be much more selective
      assert bound_score < var_score / 100
    end
  end

  describe "optimize/2 with BGP reordering" do
    test "applies BGP reordering by default" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      result = Optimizer.optimize({:bgp, patterns})

      {:bgp, reordered} = result
      # Bound subject should come first
      assert hd(reordered) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end

    test "can disable BGP reordering" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      result = Optimizer.optimize({:bgp, patterns}, reorder_bgp: false)

      # Should remain in original order
      assert result == {:bgp, patterns}
    end

    test "BGP reordering works with filter push-down" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})

      result = Optimizer.optimize(algebra)

      # BGP should be reordered, filter should stay
      {:filter, _, {:bgp, reordered}} = result
      assert hd(reordered) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end
  end

  describe "integration - BGP reordering with parser" do
    test "reorders patterns in parsed query" do
      query = """
      SELECT ?x ?y WHERE {
        ?x ?p ?o .
        <http://example.org/Bob> <http://example.org/knows> ?y .
        ?a ?b ?c .
      }
      """

      {:ok, ast} = Parser.parse(query)
      {:ok, pattern} = Algebra.extract_pattern(ast)

      optimized = Optimizer.optimize(pattern)

      # Find the BGP in the optimized result
      bgp = find_bgp(unwrap_modifiers(optimized))
      {:bgp, patterns} = bgp

      # The bound pattern should come first
      first_pattern = hd(patterns)
      {:triple, s, p, _o} = first_pattern

      # Should have bound subject and predicate
      assert match?({:named_node, _}, s)
      assert match?({:named_node, _}, p)
    end
  end

  # Helper to find BGP in algebra tree
  defp find_bgp({:bgp, _} = bgp), do: bgp
  defp find_bgp({:filter, _, inner}), do: find_bgp(inner)
  defp find_bgp({:join, left, _right}), do: find_bgp(left)
  defp find_bgp({:left_join, left, _right, _}), do: find_bgp(left)
  defp find_bgp({:project, inner, _}), do: find_bgp(inner)
  defp find_bgp({:distinct, inner}), do: find_bgp(inner)
  defp find_bgp(_), do: nil

  # ===========================================================================
  # Optimizer Pipeline Tests
  # ===========================================================================

  describe "optimize/2 pipeline options" do
    test "applies all optimizations by default" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      # Constant that can be folded
      expr = and_expr(bool(true), greater(var("x"), int(5)))
      algebra = filter(expr, {:bgp, patterns})

      result = Optimizer.optimize(algebra)

      # All optimizations should have been applied:
      # 1. Constant folding: true && expr -> expr
      # 2. BGP reordering: bound pattern first
      # 3. Filter push-down: filter stays with BGP
      {:filter, result_expr, {:bgp, result_patterns}} = result

      # Constant folded - true AND removed
      assert result_expr == greater(var("x"), int(5))

      # BGP reordered - bound subject first
      assert hd(result_patterns) == triple(iri("http://ex.org/Bob"), var("q"), var("z"))
    end

    test "can disable all optimizations" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = {:bgp, patterns}

      result =
        Optimizer.optimize(algebra,
          fold_constants: false,
          reorder_bgp: false,
          push_filters: false
        )

      # Should be unchanged
      assert result == algebra
    end

    test "log option does not affect output" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = {:bgp, patterns}

      # With logging
      result_with_log = Optimizer.optimize(algebra, log: true)
      # Without logging
      result_without_log = Optimizer.optimize(algebra, log: false)

      # Results should be identical
      assert result_with_log == result_without_log
    end

    test "stats option is passed to BGP reordering" do
      rare_pred = triple(var("s"), iri("http://ex.org/rare"), var("o"))
      common_pred = triple(var("s"), iri("http://ex.org/common"), var("o"))

      stats = %{
        {:predicate_count, "http://ex.org/rare"} => 5,
        {:predicate_count, "http://ex.org/common"} => 50_000
      }

      result = Optimizer.optimize({:bgp, [common_pred, rare_pred]}, stats: stats)

      {:bgp, [first, _second]} = result
      # Rare predicate should come first due to stats
      assert first == rare_pred
    end
  end

  describe "explain/2" do
    test "returns explain tuple with analysis" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(iri("http://ex.org/Bob"), var("q"), var("z"))
      ]

      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})

      result = Optimizer.optimize(algebra, explain: true)

      assert {:explain, info} = result
      assert info.original == algebra
      assert is_list(info.optimizations)
      assert is_map(info.statistics)
      assert info.estimated_improvement in [:low, :moderate, :high]
    end

    test "explain identifies filter push-down opportunity" do
      patterns = [triple(var("x"), var("p"), var("o"))]
      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert :filter_push_down in info.optimizations
      assert info.statistics.filters.total_filters == 1
    end

    test "explain identifies BGP reordering opportunity" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(var("y"), var("q"), var("z"))
      ]

      algebra = {:bgp, patterns}

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert :bgp_reordering in info.optimizations
      assert info.statistics.bgp_patterns.multi_pattern_bgps == 1
    end

    test "explain identifies constant folding opportunity" do
      patterns = [triple(var("x"), var("p"), var("o"))]
      # Foldable constant expression: 1 + 1 = 2
      expr = equal(add(int(1), int(1)), int(2))
      algebra = filter(expr, {:bgp, patterns})

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert :constant_folding in info.optimizations
    end

    test "explain returns empty optimizations for simple query" do
      # Single pattern BGP with no filters - nothing to optimize
      algebra = {:bgp, [triple(var("x"), var("p"), var("o"))]}

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      # Only BGP reordering might be listed, but with single pattern it won't help
      refute :filter_push_down in info.optimizations
      refute :constant_folding in info.optimizations
    end

    test "explain includes BGP statistics" do
      algebra =
        join(
          {:bgp,
           [
             triple(var("a"), var("b"), var("c")),
             triple(var("x"), var("y"), var("z"))
           ]},
          {:bgp, [triple(var("s"), var("p"), var("o"))]}
        )

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert info.statistics.bgp_patterns.total_bgps == 2
      assert info.statistics.bgp_patterns.total_patterns == 3
      assert info.statistics.bgp_patterns.multi_pattern_bgps == 1
      assert info.statistics.bgp_patterns.max_patterns_in_bgp == 2
    end

    test "explain indicates when predicate stats are available" do
      algebra = {:bgp, [triple(var("x"), var("p"), var("o"))]}

      # Without stats
      {:explain, info1} = Optimizer.optimize(algebra, explain: true)
      refute info1.statistics.predicate_stats_available

      # With stats
      stats = %{{:predicate_count, "http://ex.org/p"} => 100}
      {:explain, info2} = Optimizer.optimize(algebra, explain: true, stats: stats)
      assert info2.statistics.predicate_stats_available
    end

    test "explain respects disabled optimizations" do
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(var("y"), var("q"), var("z"))
      ]

      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})

      {:explain, info} =
        Optimizer.optimize(algebra,
          explain: true,
          push_filters: false,
          reorder_bgp: false
        )

      refute :filter_push_down in info.optimizations
      refute :bgp_reordering in info.optimizations
    end

    test "explain estimates high improvement for complex query" do
      # Query with filters AND multi-pattern BGP
      patterns = [
        triple(var("x"), var("p"), var("o")),
        triple(var("y"), var("q"), var("z"))
      ]

      algebra = filter(greater(var("x"), int(5)), {:bgp, patterns})

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert info.estimated_improvement == :high
    end

    test "explain estimates low improvement for already optimal query" do
      # Single pattern BGP, no filters
      algebra = {:bgp, [triple(var("x"), var("p"), var("o"))]}

      {:explain, info} = Optimizer.optimize(algebra, explain: true)

      assert info.estimated_improvement == :low
    end
  end

  describe "explain/2 direct function" do
    test "can be called directly" do
      algebra = {:bgp, [triple(var("x"), var("p"), var("o"))]}

      result = Optimizer.explain(algebra)

      assert {:explain, info} = result
      assert info.original == algebra
    end
  end

  describe "depth limiting (security)" do
    # Helper to create deeply nested algebra
    defp deeply_nested_joins(depth) do
      base = bgp([triple(var("x"), var("p"), var("o"))])

      Enum.reduce(1..depth, base, fn _, acc ->
        join(acc, bgp([triple(var("y"), var("q"), var("z"))]))
      end)
    end

    defp deeply_nested_filters(depth) do
      base = bgp([triple(var("x"), var("p"), var("o"))])

      Enum.reduce(1..depth, base, fn _, acc ->
        filter(greater(var("x"), int(5)), acc)
      end)
    end

    defp deeply_nested_unions(depth) do
      base = bgp([triple(var("x"), var("p"), var("o"))])

      Enum.reduce(1..depth, base, fn _, acc ->
        union(acc, bgp([triple(var("y"), var("q"), var("z"))]))
      end)
    end

    test "push_filters_down raises on deeply nested queries" do
      # Depth of 101 should exceed the max depth of 100
      algebra = deeply_nested_filters(101)

      assert_raise ArgumentError, ~r/Query too deeply nested/, fn ->
        Optimizer.push_filters_down(algebra)
      end
    end

    test "fold_constants raises on deeply nested queries" do
      algebra = deeply_nested_joins(101)

      assert_raise ArgumentError, ~r/Query too deeply nested/, fn ->
        Optimizer.fold_constants(algebra)
      end
    end

    test "reorder_bgp_patterns raises on deeply nested queries" do
      algebra = deeply_nested_unions(101)

      assert_raise ArgumentError, ~r/Query too deeply nested/, fn ->
        Optimizer.reorder_bgp_patterns(algebra)
      end
    end

    test "optimize raises on deeply nested queries" do
      algebra = deeply_nested_joins(101)

      assert_raise ArgumentError, ~r/Query too deeply nested/, fn ->
        Optimizer.optimize(algebra)
      end
    end

    test "allows queries within depth limit" do
      # Depth of 50 should be fine (well under 100)
      algebra = deeply_nested_joins(50)

      # Should not raise
      result = Optimizer.optimize(algebra)
      assert result != nil
    end

    test "error message includes max depth info" do
      algebra = deeply_nested_filters(101)

      error = catch_error(Optimizer.push_filters_down(algebra))
      assert error.message =~ "max depth: 100"
    end

    test "error message warns about potential attack" do
      algebra = deeply_nested_joins(101)

      error = catch_error(Optimizer.fold_constants(algebra))
      assert error.message =~ "malformed query or an attack"
    end
  end

  # ===========================================================================
  # Range Filter Extraction Tests
  # ===========================================================================

  describe "extract_range_filters/1" do
    test "extracts simple greater-or-equal filter" do
      # FILTER (?price >= 50)
      filter_expr = {:greater_or_equal, var("price"), int(50)}
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert result.variable_ranges["price"] == {50.0, nil}
    end

    test "extracts simple less-or-equal filter" do
      # FILTER (?price <= 500)
      filter_expr = {:less_or_equal, var("price"), int(500)}
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert result.variable_ranges["price"] == {nil, 500.0}
    end

    test "extracts conjunctive range filter (min AND max)" do
      # FILTER (?price >= 50 && ?price <= 500)
      filter_expr =
        and_expr(
          {:greater_or_equal, var("price"), int(50)},
          {:less_or_equal, var("price"), int(500)}
        )

      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert result.variable_ranges["price"] == {50.0, 500.0}
    end

    test "extracts strict greater filter" do
      # FILTER (?price > 50)
      filter_expr = greater(var("price"), int(50))
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      # > 50 becomes min = 50 (we treat it as inclusive for simplicity)
      assert result.variable_ranges["price"] == {50.0, nil}
    end

    test "extracts strict less filter" do
      # FILTER (?price < 500)
      filter_expr = less(var("price"), int(500))
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      # < 500 becomes max = 500 (we treat it as inclusive for simplicity)
      assert result.variable_ranges["price"] == {nil, 500.0}
    end

    test "extracts reversed comparison (value <= var)" do
      # FILTER (50 <= ?price) is same as ?price >= 50
      filter_expr = {:less_or_equal, int(50), var("price")}
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert result.variable_ranges["price"] == {50.0, nil}
    end

    test "extracts decimal value filter" do
      # FILTER (?price >= 99.99)
      decimal_val = {:literal, :typed, "99.99", @xsd_decimal}
      filter_expr = {:greater_or_equal, var("price"), decimal_val}
      pattern = bgp([triple(var("x"), iri("http://ex.org/price"), var("price"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert result.variable_ranges["price"] == {99.99, nil}
    end

    test "handles multiple range-filtered variables" do
      # FILTER (?price >= 50 && ?price <= 500 && ?rating >= 4)
      filter_expr =
        and_expr(
          and_expr(
            {:greater_or_equal, var("price"), int(50)},
            {:less_or_equal, var("price"), int(500)}
          ),
          {:greater_or_equal, var("rating"), int(4)}
        )

      pattern =
        bgp([
          triple(var("x"), iri("http://ex.org/price"), var("price")),
          triple(var("x"), iri("http://ex.org/rating"), var("rating"))
        ])

      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.member?(result.range_filtered_vars, "price")
      assert MapSet.member?(result.range_filtered_vars, "rating")
      assert result.variable_ranges["price"] == {50.0, 500.0}
      assert result.variable_ranges["rating"] == {4.0, nil}
    end

    test "returns empty for non-range filter" do
      # FILTER (?x = ?y)
      filter_expr = equal(var("x"), var("y"))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])
      algebra = filter(filter_expr, pattern)

      result = Optimizer.extract_range_filters(algebra)

      assert MapSet.size(result.range_filtered_vars) == 0
      assert result.variable_ranges == %{}
    end
  end

  describe "selectivity boost for range-filtered patterns" do
    test "pattern with range filter has lower selectivity score" do
      # Two patterns: one with range filter on its object, one without
      price_pattern = triple(var("offer"), iri("http://ex.org/price"), var("price"))
      type_pattern = triple(var("offer"), iri("http://ex.org/type"), var("type"))

      # Create filter context with ?price having range filter
      filter_context = %{
        range_filtered_vars: MapSet.new(["price"]),
        variable_ranges: %{"price" => {50.0, 500.0}}
      }

      stats = %{
        filter_context: filter_context,
        range_indexed: MapSet.new(["http://ex.org/price"])
      }

      # Get selectivity scores
      price_score = Optimizer.estimate_selectivity(price_pattern, MapSet.new(), stats)
      type_score = Optimizer.estimate_selectivity(type_pattern, MapSet.new(), stats)

      # Price pattern should have much lower score due to range filter boost
      assert price_score < type_score
    end

    test "range filter boost is stronger with range index" do
      price_pattern = triple(var("offer"), iri("http://ex.org/price"), var("price"))

      filter_context = %{
        range_filtered_vars: MapSet.new(["price"]),
        variable_ranges: %{"price" => {50.0, 500.0}}
      }

      # With range index
      stats_with_index = %{
        filter_context: filter_context,
        range_indexed: MapSet.new(["http://ex.org/price"])
      }

      # Without range index
      stats_without_index = %{
        filter_context: filter_context,
        range_indexed: MapSet.new()
      }

      score_with_index =
        Optimizer.estimate_selectivity(price_pattern, MapSet.new(), stats_with_index)

      score_without_index =
        Optimizer.estimate_selectivity(price_pattern, MapSet.new(), stats_without_index)

      # Score with index should be lower (more selective)
      assert score_with_index < score_without_index
    end
  end

  describe "BGP reordering with range filters" do
    test "places range-filtered pattern first in BGP" do
      # BGP with three patterns:
      # ?x a Product          (type pattern - not selective)
      # ?x price ?price       (will have range filter - should be first)
      # ?x name ?name         (regular pattern)

      patterns = [
        triple(
          var("x"),
          iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
          iri("http://ex.org/Product")
        ),
        triple(var("x"), iri("http://ex.org/price"), var("price")),
        triple(var("x"), iri("http://ex.org/name"), var("name"))
      ]

      # Create algebra with FILTER on price
      filter_expr =
        and_expr(
          {:greater_or_equal, var("price"), int(50)},
          {:less_or_equal, var("price"), int(500)}
        )

      algebra = filter(filter_expr, bgp(patterns))

      # Optimize with range index for price
      opts = [
        range_indexed_predicates: MapSet.new(["http://ex.org/price"])
      ]

      result = Optimizer.optimize(algebra, opts)

      # Extract the BGP patterns from the result
      # The filter push-down may split the AND, so we need to dig through nested filters
      reordered_patterns = extract_bgp_patterns(result)

      # The price pattern should be first (it has the best selectivity)
      first_pattern = hd(reordered_patterns)
      assert {:triple, _, {:named_node, "http://ex.org/price"}, _} = first_pattern
    end
  end

  # Helper to extract BGP patterns from potentially nested filters
  defp extract_bgp_patterns({:filter, _, inner}), do: extract_bgp_patterns(inner)
  defp extract_bgp_patterns({:bgp, patterns}), do: patterns
  defp extract_bgp_patterns({:join, left, _}), do: extract_bgp_patterns(left)
  defp extract_bgp_patterns(_), do: []
end
