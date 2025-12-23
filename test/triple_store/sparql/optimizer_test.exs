defmodule TripleStore.SPARQL.OptimizerTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Optimizer
  alias TripleStore.SPARQL.Algebra
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

      expected = join(
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

      expected = join(
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

      expected = left_join(
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

      filter_expr = and_expr(
        greater(var("x"), int(5)),
        {:less, var("y"), int(10)}
      )
      algebra = filter(filter_expr, joined)

      result = Optimizer.push_filters_down(algebra)

      expected = join(
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

      filter_expr = and_expr(
        greater(var("x"), int(5)),
        equal(var("x"), var("y"))
      )
      algebra = filter(filter_expr, joined)

      result = Optimizer.push_filters_down(algebra)

      # ?x = ?y remains at top, ?x > 5 pushed to left
      expected = filter(
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
      expected = join(
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
      expected = {:extend, filter(greater(var("x"), int(5)), inner), var("y"), {:add, var("x"), int(1)}}
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

      expected = join(
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

      algebra = filter(
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
  defp has_filter_at_leaf?({:join, left, right}), do: has_filter_at_leaf?(left) or has_filter_at_leaf?(right)
  defp has_filter_at_leaf?({:left_join, left, right, _}), do: has_filter_at_leaf?(left) or has_filter_at_leaf?(right)
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
      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "5.0", @xsd_decimal}} = result
    end

    test "preserves arithmetic with variables" do
      # ?x + 2 cannot be folded
      expr = add(var("x"), int(2))
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:add, {:variable, "x"}, {:literal, :typed, "2", @xsd_integer}}} = result
    end

    test "folds unary minus on constant" do
      expr = {:unary_minus, int(5)}
      pattern = bgp([triple(var("x"), var("p"), var("o"))])

      result = Optimizer.fold_constants({:extend, pattern, var("y"), expr})

      assert {:extend, ^pattern, {:variable, "y"}, {:literal, :typed, "-5", @xsd_integer}} = result
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
      expected = join(
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
end
