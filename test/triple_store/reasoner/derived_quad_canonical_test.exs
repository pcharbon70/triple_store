defmodule TripleStore.Reasoner.DerivedQuadCanonicalTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.GraphScopedReasoner
  alias TripleStore.Reasoner.ReasoningConfig
  alias TripleStore.Reasoner.Rule

  test "derived quads use canonical GSPO bytes and survive reopen and deletion" do
    path = unique_path("reopen")
    quad = {17, 101, 203, 409}
    expected_key = QuadIndex.gspo_key(17, 101, 203, 409)
    refute expected_key == QuadIndex.spog_key(101, 203, 409, 17)

    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    assert :ok = DerivedStore.insert_derived_quads(db, [quad])
    assert [{^expected_key, <<>>}] = raw_derived(db)
    assert :ok = ErlangAdapter.close(db)

    {:ok, reopened} = ErlangAdapter.open(path, schema: :quad)

    try do
      assert {:ok, true} = DerivedStore.derived_quad_exists?(reopened, quad)
      assert {:ok, [^quad]} = DerivedStore.lookup_derived_quads_in_graph(reopened, 17)
      assert :ok = DerivedStore.delete_derived_quads(reopened, [quad])
      assert {:ok, false} = DerivedStore.derived_quad_exists?(reopened, quad)
    after
      ErlangAdapter.close(reopened)
      File.rm_rf(path)
    end
  end

  test "global per_graph_cf materialization writes graph 0 GSPO and survives reopen" do
    path = unique_path("materialize")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)

    {graph, subject, premise_predicate, object, derived_predicate, final_predicate} =
      {17, 101, 203, 409, 509, 607}

    :ok = QuadOperations.insert_quad(db, {subject, premise_predicate, object, graph})

    assert {:ok, explicit_facts} = GraphScopedReasoner.load_all_explicit_quads(db)
    assert explicit_facts == MapSet.new([{subject, premise_predicate, object}])

    first_rule =
      Rule.new(
        :phase_3_storage,
        [Rule.pattern(Rule.var("s"), premise_predicate, Rule.var("o"))],
        Rule.pattern(Rule.var("s"), derived_predicate, Rule.var("o"))
      )

    second_rule =
      Rule.new(
        :phase_3_lookup,
        [
          Rule.pattern(Rule.var("s"), derived_predicate, Rule.var("o")),
          Rule.pattern(subject, premise_predicate, object)
        ],
        Rule.pattern(Rule.var("s"), final_predicate, Rule.var("o"))
      )

    {:ok, config} =
      ReasoningConfig.new(profile: :none, scope: :global, storage_strategy: :per_graph_cf)

    assert {:ok, %{total_derived: 2, iterations: 3}} =
             GraphScopedReasoner.materialize_all(db,
               config: config,
               rules: [first_rule, second_rule],
               emit_telemetry: false
             )

    expected = {0, subject, derived_predicate, object}
    final = {0, subject, final_predicate, object}

    assert Enum.sort([
             {QuadIndex.gspo_key(0, subject, derived_predicate, object), <<>>},
             {QuadIndex.gspo_key(0, subject, final_predicate, object), <<>>}
           ]) == raw_derived(db)

    :ok = ErlangAdapter.close(db)

    {:ok, reopened} = ErlangAdapter.open(path, schema: :quad)

    try do
      assert {:ok, true} = DerivedStore.derived_quad_exists?(reopened, expected)
      assert {:ok, true} = DerivedStore.derived_quad_exists?(reopened, final)

      assert {:ok, derived} = DerivedStore.lookup_derived_quads_in_graph(reopened, 0)
      assert Enum.sort(derived) == Enum.sort([expected, final])
    after
      ErlangAdapter.close(reopened)
      File.rm_rf(path)
    end
  end

  test "batched derived writes propagate storage failure without touching explicit indices" do
    path = unique_path("failure")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad, write_batch_failure: :injected)
    explicit_key = QuadIndex.gspo_key(3, 11, 13, 17)
    :ok = ErlangAdapter.put(db, :gspo, explicit_key, <<>>)
    explicit_before = ErlangAdapter.get(db, :gspo, explicit_key)

    try do
      assert {:error, :injected} =
               DerivedStore.insert_derived_quads(db, [{19, 23, 29, 31}, {37, 41, 43, 47}])

      assert [] == raw_derived(db)
      assert explicit_before == ErlangAdapter.get(db, :gspo, explicit_key)
    after
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end

  test "backup-first rebuild preserves explicit data and replaces malformed derived bytes" do
    path = unique_path("rebuild")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    explicit_key = QuadIndex.gspo_key(5, 7, 11, 13)
    malformed_key = QuadIndex.spog_key(101, 103, 107, 0)
    rebuilt = {0, 101, 103, 107}

    :ok = ErlangAdapter.put(db, :gspo, explicit_key, <<>>)
    :ok = ErlangAdapter.put(db, :derived, malformed_key, <<>>)
    explicit_backup = ErlangAdapter.get(db, :gspo, explicit_key)

    try do
      assert {:ok, 1} = DerivedStore.clear_all(db)
      assert :ok = DerivedStore.insert_derived_quads(db, [rebuilt])
      assert {:ok, true} = DerivedStore.derived_quad_exists?(db, rebuilt)
      assert explicit_backup == ErlangAdapter.get(db, :gspo, explicit_key)
    after
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end

  defp raw_derived(db) do
    db
    |> ErlangAdapter.fold(:derived, <<>>, [], fn entry, acc -> [entry | acc] end)
    |> Enum.sort()
  end

  defp unique_path(label),
    do: "/tmp/triple_store_derived_canonical_#{label}_#{System.unique_integer([:positive])}"
end
