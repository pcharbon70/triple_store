defmodule TripleStore.Stress.QuadLargeScaleTest do
  @moduledoc """
  Large-scale stress tests for quad operations (S23).

  Tests the system's ability to handle:
  - 100+ graphs
  - Millions of quads
  - Complex queries across many graphs

  These tests are tagged with :large_dataset and :slow, so they need to be
  explicitly enabled:

      mix test --include large_dataset,slow
      mix test test/triple_store/stress/quad_large_scale_test.exs

  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations

  @moduletag :large_dataset
  @moduletag :slow

  @ex "http://example.org/"

  # ===========================================================================
  # Test Setup
  # ===========================================================================

  setup do
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "stress_quad_large_scale_#{test_id}")

    {:ok, db} = ErlangAdapter.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      File.rm_rf!(db_path)
    end)

    %{ctx: ctx, db_path: db_path}
  end

  # ===========================================================================
  # Stress Test: 100+ Graphs
  # ===========================================================================

  @tag timeout: 300_000
  @tag stress_test_100_graphs: true
  test "handles 100+ named graphs with moderate data", %{ctx: ctx} do
    num_graphs = 120
    quads_per_graph = 100

    # Insert quads across 120 graphs
    Enum.each(1..num_graphs, fn graph_num ->
      graph_uri = "#{@ex}graph#{graph_num}"

      Enum.each(1..quads_per_graph, fn i ->
        subject = "#{@ex}s#{graph_num}_#{i}"
        predicate = "#{@ex}p#{i}"
        object = "#{@ex}o#{i}"

        assert :ok =
                 QuadOperations.insert_quad(
                   ctx.db,
                   ctx.dict_manager,
                   subject,
                   predicate,
                   object,
                   graph_uri
                 )
      end)
    end)

    # Verify each graph has the correct number of quads
    Enum.each(1..num_graphs, fn graph_num ->
      graph_uri = "#{@ex}graph#{graph_num}"

      {:ok, count} =
        QuadOperations.count_quads_in_graph(ctx.db, ctx.dict_manager, graph_uri)

      assert count == quads_per_graph,
             "Graph #{graph_num} should have #{quads_per_graph} quads, got #{count}"
    end)

    # Test query across all graphs
    total_quads = num_graphs * quads_per_graph

    {:ok, all_quads} =
      QuadOperations.match_pattern(
        ctx.db,
        ctx.dict_manager,
        {:variable, "s"},
        {:variable, "p"},
        {:variable, "o"},
        {:variable, "g"}
      )

    assert length(all_quads) == total_quads
  end

  # ===========================================================================
  # Stress Test: Many Quads (100k+)
  # ===========================================================================

  @tag timeout: 300_000
  @tag stress_test_many_quads: true
  test "handles 100k quads efficiently", %{ctx: ctx} do
    num_quads = 100_000
    num_subjects = 1_000
    num_predicates = 10

    # Generate quads with realistic distribution
    {time_us, _} =
      :timer.tc(fn ->
        Enum.each(1..num_quads, fn i ->
          subject_num = rem(i, num_subjects) + 1
          predicate_num = rem(i, num_predicates) + 1

          subject = "#{@ex}s#{subject_num}"
          predicate = "#{@ex}p#{predicate_num}"
          object = "#{@ex}o#{i}"
          graph_uri = "#{@ex}default"

          QuadOperations.insert_quad(
            ctx.db,
            ctx.dict_manager,
            subject,
            predicate,
            object,
            graph_uri
          )
        end)
      end)

    # Should complete within reasonable time (< 30 seconds)
    assert time_us < 30_000_000,
           "Inserting #{num_quads} quads took #{time_us / 1_000_000}ms"

    # Verify total count
    {:ok, count} =
      QuadOperations.count_quads_in_graph(ctx.db, ctx.dict_manager, "#{@ex}default")

    assert count == num_quads

    # Test efficient lookup by subject
    subject = "#{@ex}s1"

    {lookup_time_us, {:ok, matches}} =
      :timer.tc(fn ->
        QuadOperations.match_pattern(
          ctx.db,
          ctx.dict_manager,
          {:bound, subject},
          {:variable, "p"},
          {:variable, "o"},
          {:variable, "g"}
        )
      end)

    # Should find approximately num_quads / num_subjects matches
    expected_matches = div(num_quads, num_subjects)
    assert length(matches) >= expected_matches - 5
    assert length(matches) <= expected_matches + 5

    # Lookup should be fast (< 100ms for ~100 matches)
    assert lookup_time_us < 100_000,
           "Lookup took #{lookup_time_us / 1000}ms for #{length(matches)} matches"
  end

  # ===========================================================================
  # Stress Test: Graph Pattern Performance
  # ===========================================================================

  @tag timeout: 300_000
  @tag stress_test_graph_pattern: true
  test "efficient pattern matching across multiple graphs", %{ctx: ctx} do
    num_graphs = 50
    num_subjects = 100

    # Insert same subject-predicate-object pattern in multiple graphs
    Enum.each(1..num_graphs, fn graph_num ->
      Enum.each(1..num_subjects, fn subj_num ->
        subject = "#{@ex}s#{subj_num}"
        predicate = "#{@ex}pred"
        object = "#{@ex}obj"
        graph_uri = "#{@ex}graph#{graph_num}"

        QuadOperations.insert_quad(
          ctx.db,
          ctx.dict_manager,
          subject,
          predicate,
          object,
          graph_uri
        )
      end)
    end)

    total_quads = num_graphs * num_subjects

    # Test lookup for specific pattern across all graphs
    {time_us, {:ok, results}} =
      :timer.tc(fn ->
        QuadOperations.match_pattern(
          ctx.db,
          ctx.dict_manager,
          {:bound, "#{@ex}s1"},
          {:bound, "#{@ex}pred"},
          {:bound, "#{@ex}obj"},
          {:variable, "g"}
        )
      end)

    # Should find one quad per graph
    assert length(results) == num_graphs

    # Should be fast even with many graphs
    assert time_us < 50_000, "Pattern matching took #{time_us / 1000}ms"

    # Test counting all quads
    {count_time_us, {:ok, count}} =
      :timer.tc(fn ->
        QuadOperations.count_all_quads(ctx.db, ctx.dict_manager)
      end)

    assert count == total_quads
    assert count_time_us < 100_000, "Counting took #{count_time_us / 1000}ms"
  end

  # ===========================================================================
  # Stress Test: Concurrent Operations
  # ===========================================================================

  @tag timeout: 300_000
  @tag stress_test_concurrent: true
  test "handles concurrent insertions across graphs", %{ctx: ctx} do
    num_tasks = 20
    quads_per_task = 1_000

    # Create tasks that insert quads concurrently
    tasks =
      Enum.map(1..num_tasks, fn task_id ->
        Task.async(fn ->
          Enum.each(1..quads_per_task, fn i ->
            subject = "#{@ex}s#{task_id}_#{i}"
            predicate = "#{@ex}p"
            object = "#{@ex}o#{i}"
            graph_uri = "#{@ex}graph#{task_id}"

            QuadOperations.insert_quad(
              ctx.db,
              ctx.dict_manager,
              subject,
              predicate,
              object,
              graph_uri
            )
          end)
        end)
      end)

    # Wait for all tasks to complete
    results = Task.await_many(tasks, 60_000)

    # All tasks should complete successfully
    assert Enum.all?(results, fn result -> result == :ok end)

    # Verify total count
    expected_total = num_tasks * quads_per_task

    {:ok, count} = QuadOperations.count_all_quads(ctx.db, ctx.dict_manager)

    assert count == expected_total

    # Verify each graph has the correct number of quads
    Enum.each(1..num_tasks, fn task_id ->
      graph_uri = "#{@ex}graph#{task_id}"

      {:ok, graph_count} =
        QuadOperations.count_quads_in_graph(ctx.db, ctx.dict_manager, graph_uri)

      assert graph_count == quads_per_task
    end)
  end

  # ===========================================================================
  # Stress Test: Memory Efficiency
  # ===========================================================================

  @tag stress_test_memory: true
  test "stream operations don't load all results into memory", %{ctx: ctx} do
    num_quads = 10_000

    # Insert test data
    Enum.each(1..num_quads, fn i ->
      subject = "#{@ex}s#{rem(i, 100)}"
      predicate = "#{@ex}p"
      object = "#{@ex}o#{i}"
      graph_uri = "#{@ex}default"

      QuadOperations.insert_quad(
        ctx.db,
        ctx.dict_manager,
        subject,
        predicate,
        object,
        graph_uri
      )
    end)

    # Use stream operation - should be efficient
    {stream_time_us, {:ok, stream}} =
      :timer.tc(fn ->
        QuadOperations.stream_quads(ctx.db, ctx.dict_manager, "#{@ex}default")
      end)

    # Stream should be created quickly
    assert stream_time_us < 10_000, "Stream creation took #{stream_time_us / 1000}ms"

    # Consume stream and measure time
    {consume_time_us, results} =
      :timer.tc(fn ->
        TripleStore.QuadStream.to_list(stream, max_results: :infinity)
      end)

    assert length(results) == num_quads

    # Total time should be reasonable
    total_time_ms = (stream_time_us + consume_time_us) / 1000
    assert total_time_ms < 5_000, "Total streaming took #{total_time_ms}ms"
  end

  # ===========================================================================
  # Stress Test: Query Complexity
  # ===========================================================================

  @tag timeout: 300_000
  @tag stress_test_query_complexity: true
  test "handles complex queries across many graphs", %{ctx: ctx} do
    num_graphs = 100
    subjects_per_graph = 50

    # Create interconnected graph data
    Enum.each(1..num_graphs, fn graph_num ->
      Enum.each(1..subjects_per_graph, fn subj_num ->
        subject = "#{@ex}s#{subj_num}"
        predicate = "#{@ex}linkedTo"
        # Link to next subject
        object = "#{@ex}s#{rem(subj_num, subjects_per_graph) + 1}"
        graph_uri = "#{@ex}graph#{graph_num}"

        QuadOperations.insert_quad(
          ctx.db,
          ctx.dict_manager,
          subject,
          predicate,
          object,
          graph_uri
        )
      end)
    end)

    # Test pattern with multiple bound terms
    {time_us, {:ok, results}} =
      :timer.tc(fn ->
        QuadOperations.match_pattern(
          ctx.db,
          ctx.dict_manager,
          {:bound, "#{@ex}s1"},
          {:bound, "#{@ex}linkedTo"},
          {:variable, "o"},
          {:variable, "g"}
        )
      end)

    # Should find one match per graph
    assert length(results) == num_graphs

    # Should be fast
    assert time_us < 50_000,
           "Complex query took #{time_us / 1000}ms for #{length(results)} results"

    # Test query with only graph bound
    {graph_time_us, {:ok, graph_results}} =
      :timer.tc(fn ->
        QuadOperations.match_pattern(
          ctx.db,
          ctx.dict_manager,
          {:variable, "s"},
          {:variable, "p"},
          {:variable, "o"},
          {:bound, "#{@ex}graph1"}
        )
      end)

    assert length(graph_results) == subjects_per_graph
    assert graph_time_us < 20_000, "Graph-scoped query took #{graph_time_us / 1000}ms"
  end

  # ===========================================================================
  # Stress Test: Delete Performance
  # ===========================================================================

  @tag stress_test_delete: true
  test "efficiently deletes large numbers of quads", %{ctx: ctx} do
    num_graphs = 20
    quads_per_graph = 500

    # Insert data
    Enum.each(1..num_graphs, fn graph_num ->
      graph_uri = "#{@ex}graph#{graph_num}"

      Enum.each(1..quads_per_graph, fn i ->
        subject = "#{@ex}s#{graph_num}_#{i}"
        predicate = "#{@ex}p"
        object = "#{@ex}o#{i}"

        QuadOperations.insert_quad(
          ctx.db,
          ctx.dict_manager,
          subject,
          predicate,
          object,
          graph_uri
        )
      end)
    end)

    # Delete a graph completely
    graph_to_delete = "#{@ex}graph1"

    {delete_time_us, :ok} =
      :timer.tc(fn ->
        QuadOperations.delete_graph(ctx.db, ctx.dict_manager, graph_to_delete)
      end)

    # Deletion should be reasonably fast
    assert delete_time_us < 10_000, "Graph deletion took #{delete_time_us / 1000}ms"

    # Verify graph is empty
    {:ok, count} =
      QuadOperations.count_quads_in_graph(ctx.db, ctx.dict_manager, graph_to_delete)

    assert count == 0

    # Verify other graphs are intact
    {:ok, total_count} = QuadOperations.count_all_quads(ctx.db, ctx.dict_manager)
    expected_count = (num_graphs - 1) * quads_per_graph

    assert total_count == expected_count
  end
end
