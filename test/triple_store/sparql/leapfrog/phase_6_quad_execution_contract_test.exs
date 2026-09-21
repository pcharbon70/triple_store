defmodule TripleStore.SPARQL.Leapfrog.Phase6QuadExecutionContractTest do
  @moduledoc """
  Regression coverage for Phase 6, Section 1.1's quad execution contract.

  These tests exercise the public executor and QuadLeapfrog boundaries so the
  binding representation, prefix plan, resource ownership, and error behavior
  cannot drift independently again.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Executor
  alias TripleStore.SPARQL.Leapfrog.QuadLeapfrog
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :integration
  @moduletag :phase_6_quad_contract

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_phase_6_quad_#{System.unique_integer([:positive, :monotonic])}"
      )

    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      if Process.alive?(db), do: ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    {:ok, ctx: ctx, db: db}
  end

  test "all-variable executor queries return decoded binary-key bindings", %{ctx: ctx} do
    assert {:ok, 2} =
             UpdateExecutor.execute_insert_data(ctx, [
               quad("s1", "p", "o1", "g1"),
               quad("s2", "p", "o2", "g2")
             ])

    pattern =
      {:bgp,
       [
         {:quad, {:variable, "subject"}, {:variable, "predicate"}, {:variable, "object"},
          {:variable, "graph"}}
       ]}

    assert {:ok, stream} = Executor.execute_quad_pattern(ctx, pattern, %{})
    results = Enum.to_list(stream)

    assert length(results) == 2

    assert Enum.all?(results, fn binding ->
             Enum.sort(Map.keys(binding)) == ["graph", "object", "predicate", "subject"] and
               match?({:named_node, _}, binding["subject"]) and
               match?({:named_node, _}, binding["predicate"]) and
               match?({:literal, _, _}, binding["object"]) and
               match?({:named_node, _}, binding["graph"])
           end)
  end

  test "graph-bound executor queries honor default and named graph scope", %{ctx: ctx} do
    assert {:ok, 3} =
             UpdateExecutor.execute_insert_data(ctx, [
               quad("default", "p", "default-value", :default_graph),
               quad("named-1", "p", "named-value-1", "g1"),
               quad("named-2", "p", "named-value-2", "g1")
             ])

    triple = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
    assert {:ok, default_stream} = Executor.execute_quad_pattern(ctx, {:bgp, [triple]}, %{})
    assert [%{"s" => {:named_node, "http://example.org/default"}}] = Enum.to_list(default_stream)

    named =
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
       {:named_node, "http://example.org/g1"}}

    assert {:ok, named_stream} = Executor.execute_quad_pattern(ctx, {:bgp, [named]}, %{})

    assert named_stream
           |> Enum.map(& &1["s"])
           |> Enum.sort() == [
             {:named_node, "http://example.org/named-1"},
             {:named_node, "http://example.org/named-2"}
           ]
  end

  test "an existing outer binding constrains the quad scan", %{ctx: ctx} do
    assert {:ok, 2} =
             UpdateExecutor.execute_insert_data(ctx, [
               quad("s1", "p", "o1", "g"),
               quad("s2", "p", "o2", "g")
             ])

    pattern =
      {:bgp,
       [
         {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
       ]}

    initial = %{"s" => {:named_node, "http://example.org/s1"}}
    assert {:ok, stream} = Executor.execute_quad_pattern(ctx, pattern, initial)

    assert [%{"s" => {:named_node, "http://example.org/s1"}} = result] = Enum.to_list(stream)
    assert result["o"] == {:literal, :simple, "o1"}
  end

  test "iterator plans encode only the longest contiguous bound prefix" do
    all_variables =
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

    graph_bound =
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 5}

    subject_bound =
      {:quad, 7, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

    assert {:ok, [{3, :gspo, 0, <<>>}]} = QuadLeapfrog.plan_iterators(all_variables)
    assert {:ok, [{3, :gspo, 1, <<5::64-big>>}]} = QuadLeapfrog.plan_iterators(graph_bound)
    assert {:ok, [{3, :spog, 1, <<7::64-big>>}]} = QuadLeapfrog.plan_iterators(subject_bound)
    assert {:ok, []} = QuadLeapfrog.plan_iterators({:quad, 1, 2, 3, 4})
  end

  test "malformed patterns and unavailable stores return tagged errors", %{db: db} do
    assert {:error, :invalid_quad_pattern} = QuadLeapfrog.from_pattern(db, {:triple, 1, 2, 3})

    assert {:error, {:invalid_quad_component, :subject, -1}} =
             QuadLeapfrog.from_pattern(db, {:quad, -1, 2, 3, 4})

    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_phase_6_closed_#{System.unique_integer([:positive, :monotonic])}"
      )

    {:ok, closed_db} = ErlangAdapter.open(path, schema: :quad)
    :ok = ErlangAdapter.close(closed_db)

    assert {:error, :store_unavailable} =
             QuadLeapfrog.from_pattern(
               closed_db,
               {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
             )

    File.rm_rf!(path)
  end

  test "stream enumeration owns and closes its iterator on early halt", %{db: db} do
    :ok = QuadOperations.insert_quad(db, {1, 2, 3, 4})

    pattern =
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

    assert {:ok, qlf} = QuadLeapfrog.from_pattern(db, pattern)
    [tagged] = qlf.tagged_iterators
    iterator_pid = tagged.iterator.iter_ref

    assert [_binding] = qlf |> QuadLeapfrog.stream() |> Enum.take(1)
    refute Process.alive?(iterator_pid)
  end

  test "unique query variable names do not allocate atoms", %{db: db} do
    :ok = QuadOperations.insert_quad(db, {1, 2, 3, 4})

    warmup = {:quad, {:variable, "warmup"}, 2, 3, 4}
    assert {:ok, qlf} = QuadLeapfrog.from_pattern(db, warmup)
    assert [_] = Enum.to_list(QuadLeapfrog.stream(qlf))

    before_count = :erlang.system_info(:atom_count)

    for suffix <- 1..64 do
      pattern = {:quad, {:variable, "phase6_unique_#{suffix}"}, 2, 3, 4}
      assert {:ok, qlf} = QuadLeapfrog.from_pattern(db, pattern)
      assert [_] = Enum.to_list(QuadLeapfrog.stream(qlf))
    end

    assert :erlang.system_info(:atom_count) == before_count
  end

  defp quad(subject, predicate, object, :default_graph) do
    {:quad, iri(subject), iri(predicate), {:literal, :simple, object}, :default_graph}
  end

  defp quad(subject, predicate, object, graph) do
    {:quad, iri(subject), iri(predicate), {:literal, :simple, object}, iri(graph)}
  end

  defp iri(value), do: {:named_node, "http://example.org/#{value}"}
end
