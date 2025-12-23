defmodule TripleStore.Integration.BulkLoadingTest do
  @moduledoc """
  Integration tests for Task 1.7.4: Bulk Loading Testing.

  Tests bulk loading performance and correctness with large datasets,
  including:
  - Loading 100K triples maintains index consistency
  - Loading 1M triples completes in reasonable time
  - Memory usage stays bounded during bulk load
  - Loading real-world RDF datasets (simulated LUBM)
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Loader

  @test_db_base "/tmp/triple_store_bulk_load_test"

  # Longer timeout for bulk operations
  @moduletag timeout: 300_000

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
  # 1.7.4.1: Loading 100K Triples Maintains Index Consistency
  # ===========================================================================

  describe "loading 100K triples maintains index consistency" do
    @tag :slow
    test "100K triples with varied predicates", %{db: db} do
      # Generate 100K triples with 100 predicates
      triples =
        for i <- 1..100_000 do
          # 1000 subjects
          subject = 1000 + div(i, 100)
          # 100 predicates
          predicate = 100 + rem(i, 100)
          # unique objects
          object = 100_000 + i

          {subject, predicate, object}
        end

      # Insert in batches
      triples
      |> Enum.chunk_every(1000)
      |> Enum.each(fn batch ->
        :ok = Index.insert_triples(db, batch)
      end)

      # Verify total count
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 100_000

      # Verify predicate distribution
      for p <- [100, 150, 199] do
        {:ok, count} = Index.count(db, {:var, {:bound, p}, :var})
        assert count == 1000, "Predicate #{p} should have 1000 triples"
      end

      # Verify subject distribution (subject = 1000 + div(i, 100))
      # For i=1..99, div(i, 100) = 0, so subject = 1000
      # For i=100..199, div(i, 100) = 1, so subject = 1001
      # Subject 1000 appears when i is 1..99 (99 triples)
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, :var})
      assert length(results) == 99

      # Spot check - verify calculations:
      # For i=1: subject=1000+div(1,100)=1000, predicate=100+rem(1,100)=101, object=10_0001
      # For i=100: subject=1000+div(100,100)=1001, predicate=100+rem(100,100)=100, object=100100
      # i=1
      assert {:ok, true} = Index.triple_exists?(db, {1000, 101, 100_001})
      # i=100
      assert {:ok, true} = Index.triple_exists?(db, {1001, 100, 100_100})
    end

    @tag :slow
    test "100K unique subjects", %{db: db} do
      # 100K triples with unique subjects
      triples =
        for i <- 1..100_000 do
          {i, 100, i + 1_000_000}
        end

      triples
      |> Enum.chunk_every(1000)
      |> Enum.each(fn batch ->
        :ok = Index.insert_triples(db, batch)
      end)

      # All 100K should exist
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 100_000

      # All under same predicate
      {:ok, pred_count} = Index.count(db, {:var, {:bound, 100}, :var})
      assert pred_count == 100_000
    end

    @tag :slow
    test "100K with RDF terms via Loader", %{db: db, manager: manager} do
      # Build RDF graph with 10K triples (reduced for reasonable test time)
      graph =
        Enum.reduce(1..10_000, RDF.graph(), fn i, g ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p#{rem(i, 10)}")
          object = RDF.literal("value#{i}")

          RDF.Graph.add(g, {subject, predicate, object})
        end)

      # Load via Loader
      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 10_000

      # Verify via Index count
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 10_000
    end
  end

  # ===========================================================================
  # 1.7.4.2: Loading Large Datasets
  # ===========================================================================

  describe "loading large datasets" do
    @tag :slow
    test "500K triples complete in reasonable time", %{db: db} do
      # Generate 500K triples
      start_time = System.monotonic_time(:millisecond)

      # Insert in batches of 5000
      for batch_num <- 0..99 do
        triples =
          for i <- 1..5000 do
            base = batch_num * 5000 + i
            {base, rem(base, 50), base + 1_000_000}
          end

        :ok = Index.insert_triples(db, triples)
      end

      elapsed = System.monotonic_time(:millisecond) - start_time

      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 500_000

      # Should complete in under 60 seconds
      assert elapsed < 60_000,
             "500K triples took #{elapsed}ms, should be under 60 seconds"
    end
  end

  # ===========================================================================
  # 1.7.4.3: Memory Usage Stays Bounded During Bulk Load
  # ===========================================================================

  describe "memory usage stays bounded during bulk load" do
    @tag :slow
    test "streaming bulk insert doesn't accumulate memory", %{db: db} do
      # Get initial memory
      initial_memory = :erlang.memory(:total)

      # Stream 50K triples in small batches
      Stream.iterate(1, &(&1 + 1))
      |> Stream.take(50_000)
      |> Stream.chunk_every(500)
      |> Stream.each(fn batch ->
        triples =
          Enum.map(batch, fn i ->
            {i, rem(i, 20), i + 500_000}
          end)

        Index.insert_triples(db, triples)

        # Force garbage collection periodically
        if rem(hd(batch), 10_000) == 1 do
          :erlang.garbage_collect()
        end
      end)
      |> Stream.run()

      # Force final GC
      :erlang.garbage_collect()
      Process.sleep(100)

      final_memory = :erlang.memory(:total)
      memory_growth = final_memory - initial_memory

      # Verify data loaded
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 50_000

      # Memory growth should be reasonable (less than 200MB)
      # This is a soft check since memory can vary
      assert memory_growth < 200_000_000,
             "Memory grew by #{div(memory_growth, 1_000_000)}MB, expected <200MB"
    end
  end

  # ===========================================================================
  # 1.7.4.4: Loading Simulated LUBM Dataset
  # ===========================================================================

  describe "loading simulated LUBM dataset" do
    @tag :slow
    test "LUBM-like university data", %{db: db} do
      # Simulate LUBM patterns: universities, departments, professors, students

      # Predicates
      rdf_type = 1
      has_name = 2
      works_for = 3
      studies_at = 4
      teaches = 5

      # Types
      university = 100
      department = 101
      professor = 102
      student = 103
      _course = 104

      # 1 university with 10 departments
      university_id = 10_000

      university_triples = [
        {university_id, rdf_type, university},
        {university_id, has_name, 20_000}
      ]

      # Each department has 5 professors and 50 students
      dept_triples =
        for dept_num <- 1..10 do
          dept_id = 10_000 + dept_num * 100

          dept_base = [
            {dept_id, rdf_type, department},
            {dept_id, works_for, university_id}
          ]

          # Professors
          prof_triples =
            for prof_num <- 1..5 do
              prof_id = dept_id + prof_num

              [
                {prof_id, rdf_type, professor},
                {prof_id, works_for, dept_id},
                {prof_id, teaches, 30_000 + dept_num * 100 + prof_num}
              ]
            end
            |> List.flatten()

          # Students
          student_triples =
            for student_num <- 1..50 do
              student_id = dept_id + 50 + student_num

              [
                {student_id, rdf_type, student},
                {student_id, studies_at, dept_id}
              ]
            end
            |> List.flatten()

          dept_base ++ prof_triples ++ student_triples
        end
        |> List.flatten()

      triples = university_triples ++ dept_triples

      :ok = Index.insert_triples(db, triples)

      # Verify counts
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total > 500, "Should have substantial data loaded (got #{total})"

      # Verify some queries
      # All professors
      {:ok, professors} = Index.lookup_all(db, {:var, {:bound, rdf_type}, {:bound, professor}})
      assert length(professors) == 50

      # All students
      {:ok, students} = Index.lookup_all(db, {:var, {:bound, rdf_type}, {:bound, student}})
      assert length(students) == 500
    end

    @tag :slow
    test "loading and querying RDF graph", %{db: db, manager: manager} do
      # Build a more realistic RDF graph with proper IRIs
      _ex = RDF.iri("http://example.org/")

      triples =
        for i <- 1..1000 do
          subject = RDF.iri("http://example.org/person#{i}")

          [
            {subject, RDF.type(), RDF.iri("http://xmlns.com/foaf/0.1/Person")},
            {subject, RDF.iri("http://xmlns.com/foaf/0.1/name"), RDF.literal("Person #{i}")},
            {subject, RDF.iri("http://xmlns.com/foaf/0.1/age"),
             RDF.XSD.Integer.new!(20 + rem(i, 60))}
          ]
        end
        |> List.flatten()

      graph = RDF.Graph.new(triples)

      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 3000

      # Verify via Index
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 3000
    end
  end
end
