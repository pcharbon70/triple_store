defmodule TripleStore.Reasoner.GroundPremiseRegressionTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.DeltaComputation
  alias TripleStore.Reasoner.Rule

  @a {:iri, "http://example.org/a"}
  @b {:iri, "http://example.org/b"}
  @p {:iri, "http://example.org/p"}
  @q {:iri, "http://example.org/q"}
  @r {:iri, "http://example.org/r"}

  test "a missing fully ground premise cannot produce a derivation" do
    p = {@a, @p, @b}
    rule = joined_rule([Rule.pattern(Rule.var("x"), @p, Rule.var("y")), ground_q()])

    assert {:ok, result} = apply_rule(rule, MapSet.new([p]), MapSet.new([p]))
    assert MapSet.size(result) == 0

    facts = MapSet.new([p, {@a, @q, @b}])
    assert {:ok, result} = apply_rule(rule, MapSet.new([p]), facts)
    assert MapSet.member?(result, {@a, @r, @b})
  end

  test "ground checks are enforced with reversed body order" do
    p = {@a, @p, @b}
    rule = joined_rule([ground_q(), Rule.pattern(Rule.var("x"), @p, Rule.var("y"))])

    assert {:ok, result} = apply_rule(rule, MapSet.new([p]), MapSet.new([p]))
    assert MapSet.size(result) == 0
  end

  test "repeated variables remain constrained while checking a ground premise" do
    p = {@a, @p, @b}

    rule =
      joined_rule([
        Rule.pattern(Rule.var("x"), @p, Rule.var("x")),
        ground_q()
      ])

    facts = MapSet.new([p, {@a, @q, @b}])
    assert {:ok, result} = apply_rule(rule, MapSet.new([p]), facts)
    assert MapSet.size(result) == 0
  end

  test "a ground quad premise must exist in the same graph" do
    g1 = {:iri, "http://example.org/g1"}
    g2 = {:iri, "http://example.org/g2"}
    p = {g1, @a, @p, @b}

    rule =
      Rule.new_quad(
        :quad_ground_join,
        [
          {:quad_pattern, [Rule.var("g"), Rule.var("x"), @p, Rule.var("y")]},
          {:quad_pattern, [g1, @a, @q, @b]}
        ],
        {:quad_pattern, [Rule.var("g"), Rule.var("x"), @r, Rule.var("y")]}
      )

    facts = MapSet.new([p, {g2, @a, @q, @b}])
    assert {:ok, result} = apply_rule(rule, MapSet.new([p]), facts)
    assert MapSet.size(result) == 0
  end

  test "lookup failure aborts rule application with a tagged error" do
    p = {@a, @p, @b}
    rule = joined_rule([Rule.pattern(Rule.var("x"), @p, Rule.var("y")), ground_q()])
    lookup = fn _pattern -> {:error, :storage_unavailable} end

    assert {:error, {:lookup_failed, :storage_unavailable}} =
             DeltaComputation.apply_rule_delta(lookup, rule, MapSet.new([p]), MapSet.new([p]))
  end

  defp ground_q, do: Rule.pattern(@a, @q, @b)

  defp joined_rule(body) do
    Rule.new(
      :ground_join,
      body,
      Rule.pattern(Rule.var("x"), @r, Rule.var("y"))
    )
  end

  defp apply_rule(rule, delta, facts) do
    DeltaComputation.apply_rule_delta(make_lookup(facts), rule, delta, facts)
  end

  defp make_lookup(facts) do
    fn pattern ->
      matches = Enum.filter(facts, &matches?(&1, pattern))
      {:ok, matches}
    end
  end

  defp matches?({s, p, o}, {:pattern, [ps, pp, po]}) do
    term_matches?(s, ps) and term_matches?(p, pp) and term_matches?(o, po)
  end

  defp matches?({g, s, p, o}, {:quad_pattern, [pg, ps, pp, po]}) do
    term_matches?(g, pg) and term_matches?(s, ps) and term_matches?(p, pp) and
      term_matches?(o, po)
  end

  defp matches?(_fact, _pattern), do: false
  defp term_matches?(_fact, {:var, _name}), do: true
  defp term_matches?(fact, fact), do: true
  defp term_matches?(_fact, _pattern), do: false
end
