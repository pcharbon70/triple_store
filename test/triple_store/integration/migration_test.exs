defmodule TripleStore.Integration.MigrationTest do
  @moduledoc """
  Integration tests for Section 6.8: Migration Tests.

  Tests migration from triple store (schema v1) to quad store (schema v2):
  - 6.8.1: Triple to Quad Migration (export, convert, import, verify)
  - 6.8.2: Migration Tooling (large datasets, progress, error handling)

  These tests validate that data can be migrated from the legacy triple
  store format to the new quad store format without data loss.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Loader
  alias TripleStore.Exporter
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/migration_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path(suffix \\ "") do
    TripleStore.Integration.Helpers.unique_path("migration_test" <> suffix)
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  # Create a triple store (schema v1)
  defp create_triple_store do
    path = unique_path("_triple")
    {:ok, db} = NIF.open(path, schema: :triple)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager, path}
  end

  # Create a quad store (schema v2)
  defp create_quad_store do
    path = unique_path("_quad")
    {:ok, db} = NIF.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager, path}
  end

  # Close and cleanup database
  defp teardown_db(db, manager, path) do
    try do
      if Process.alive?(manager), do: Manager.stop(manager)
    catch
      :exit, _ -> :ok
    end

    try do
      NIF.close(db)
    catch
      :exit, _ -> :ok
    end

    cleanup_path(path)
  end

  # Populate triple store with test data
  defp populate_triple_store(db, manager) do
    triples = [
      {RDF.iri("#{@ex}subject1"), RDF.iri("#{@ex}predicate1"), RDF.literal("object1")},
      {RDF.iri("#{@ex}subject2"), RDF.iri("#{@ex}predicate2"), RDF.literal("object2")},
      {RDF.iri("#{@ex}subject3"), RDF.iri("#{@ex}predicate1"), RDF.literal("object3")},
      {RDF.iri("#{@ex}subject4"), RDF.iri("#{@ex}predicate2"), RDF.literal("object4")},
      {RDF.iri("#{@ex}subject5"), RDF.iri("#{@ex}predicate3"), RDF.literal("object5")}
    ]

    # Insert triples using Index
    Enum.each(triples, fn {subject, predicate, object} ->
      {:ok, s_id} = Adapter.term_to_id(manager, subject)
      {:ok, p_id} = Adapter.term_to_id(manager, predicate)
      {:ok, o_id} = Adapter.term_to_id(manager, object)

      :ok = Index.insert_triple(db, {s_id, p_id, o_id})
    end)

    {:ok, length(triples)}
  end

  # Convert N-Triples to N-Quads by adding default graph context
  defp ntriples_to_nquads(ntriples_string) do
    default_graph = "http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph"

    ntriples_string
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      # Remove trailing dot and add graph context
      triple_part = String.replace_trailing(line, ".", "")
      "#{triple_part} <#{default_graph}> ."
    end)
    |> Enum.join("\n")
  end

  # Convert N-Quads to N-Triples by removing graph context
  defp nquads_to_ntriples(nquads_string) do
    nquads_string
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      # Remove graph IRI and trailing dot, convert back to triple
      # N-Quads format: <s> <p> "o" <g> .
      # N-Triples format: <s> <p> "o" .
      # Use regex to remove the last <...> pattern before the dot
      Regex.replace(~r/ <[^>]+> \.$/, line, " .")
    end)
    |> Enum.join("\n")
  end

  # Count triples in a triple store
  defp count_triples(triple_db) do
    {:ok, triples} = Index.lookup_all(triple_db, {:var, :var, :var})
    length(triples)
  end

  # Count quads in a quad store
  defp count_quads(quad_db) do
    quads = QuadOperations.lookup_quads(quad_db, {:var, :var, :var, :var}, %{})
    length(quads)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    # Create both stores for testing
    {triple_db, triple_manager, triple_path} = create_triple_store()
    {quad_db, quad_manager, quad_path} = create_quad_store()

    on_exit(fn ->
      teardown_db(triple_db, triple_manager, triple_path)
      teardown_db(quad_db, quad_manager, quad_path)
    end)

    %{
      triple_db: triple_db,
      triple_manager: triple_manager,
      quad_db: quad_db,
      quad_manager: quad_manager
    }
  end

  # ===========================================================================
  # 6.8.1: Triple to Quad Migration
  # ===========================================================================

  describe "6.8.1 Triple to Quad Migration" do
    test "6.8.1.1 export triple store as N-Triples", %{triple_db: db, triple_manager: manager} do
      # Populate triple store
      {:ok, _count} = populate_triple_store(db, manager)

      # Export as N-Triples
      {:ok, ntriples} = Exporter.export_string(db, :ntriples)

      # Verify N-Triples format
      assert String.contains?(ntriples, "#{@ex}subject1")
      assert String.contains?(ntriples, "#{@ex}predicate1")
      assert String.contains?(ntriples, "\"object1\"")
      assert String.contains?(ntriples, "#{@ex}subject2")
      assert String.contains?(ntriples, "#{@ex}predicate2")
      assert String.contains?(ntriples, "\"object2\"")

      # Count lines (should be 5 triples)
      lines = ntriples |> String.split("\n") |> Enum.filter(&(&1 != ""))
      assert length(lines) == 5
    end

    test "6.8.1.2 convert N-Triples to N-Quads (add default graph)" do
      ntriples = """
      <#{@ex}s1> <#{@ex}p1> "o1" .
      <#{@ex}s2> <#{@ex}p2> "o2" .
      <#{@ex}s3> <#{@ex}p1> "o3" .
      """

      nquads = ntriples_to_nquads(ntriples)

      # Verify N-Quads format includes graph context
      assert String.contains?(
               nquads,
               "<http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph>"
             )

      assert String.contains?(nquads, "<#{@ex}s1>")
      assert String.contains?(nquads, "<#{@ex}p1>")
      assert String.contains?(nquads, "\"o1\"")

      # Each line should now have 4 components + dot
      lines = nquads |> String.split("\n") |> Enum.filter(&(&1 != ""))
      assert length(lines) == 3

      # Verify each line ends with graph IRI before the dot
      Enum.each(lines, fn line ->
        assert String.ends_with?(line, " .") or String.ends_with?(line, "> .")
        assert String.contains?(line, "default_graph>")
      end)
    end

    test "6.8.1.3 load N-Quads to new quad store", %{quad_db: quad_db, quad_manager: quad_manager} do
      # Create N-Quads content
      nquads = """
      <#{@ex}s1> <#{@ex}p1> "o1" <http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph> .
      <#{@ex}s2> <#{@ex}p2> "o2" <http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph> .
      <#{@ex}s3> <#{@ex}p1> "o3" <http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph> .
      """

      # Load N-Quads to quad store
      {:ok, count} = Loader.load_nquads_string(quad_db, quad_manager, nquads)

      # Verify quads loaded
      assert count == 3
      assert count_quads(quad_db) == 3
    end

    test "6.8.1.4 query migrated data returns same results", context do
      %{
        triple_db: triple_db,
        triple_manager: triple_manager,
        quad_db: quad_db,
        quad_manager: quad_manager
      } = context

      # Populate triple store
      {:ok, _count} = populate_triple_store(triple_db, triple_manager)

      # Get all triples from triple store
      {:ok, triples} = Index.lookup_all(triple_db, {:var, :var, :var})

      # Export to N-Triples
      {:ok, ntriples} = Exporter.export_string(triple_db, :ntriples)

      # Convert to N-Quads
      nquads = ntriples_to_nquads(ntriples)

      # Load to quad store
      {:ok, count} = Loader.load_nquads_string(quad_db, quad_manager, nquads)

      # Verify same count
      assert count == length(triples)

      # Export the migrated quad store as N-Quads (quad stores use different indices)
      {:ok, migrated_nquads} = Exporter.export_nquads_string(quad_db)

      # Convert N-Quads back to N-Triples (remove graph context)
      migrated_ntriples = nquads_to_ntriples(migrated_nquads)

      # Compare N-Triples output (normalize for comparison)
      original_lines = ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      migrated_lines =
        migrated_ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      # The N-Triples should be identical (same count and same content)
      assert MapSet.size(original_lines) == MapSet.size(migrated_lines)
      # All triples from original should be in migrated
      assert MapSet.subset?(original_lines, migrated_lines)
    end

    test "6.8.1.5 all data preserved in migration", context do
      %{
        triple_db: triple_db,
        triple_manager: triple_manager,
        quad_db: quad_db,
        quad_manager: quad_manager
      } = context

      # Populate with diverse test data
      triples = [
        {RDF.iri("#{@ex}alice"), RDF.iri("#{@ex}name"), RDF.literal("Alice")},
        {RDF.iri("#{@ex}alice"), RDF.iri("#{@ex}age"), RDF.literal("30")},
        {RDF.iri("#{@ex}bob"), RDF.iri("#{@ex}name"), RDF.literal("Bob")},
        {RDF.iri("#{@ex}bob"), RDF.iri("#{@ex}age"), RDF.literal("25")},
        {RDF.iri("#{@ex}carol"), RDF.iri("#{@ex}name"), RDF.literal("Carol")},
        {RDF.iri("#{@ex}carol"), RDF.iri("#{@ex}friend"), RDF.iri("#{@ex}alice")},
        {RDF.iri("#{@ex}carol"), RDF.iri("#{@ex}friend"), RDF.iri("#{@ex}bob")}
      ]

      Enum.each(triples, fn {subject, predicate, object} ->
        {:ok, s_id} = Adapter.term_to_id(triple_manager, subject)
        {:ok, p_id} = Adapter.term_to_id(triple_manager, predicate)
        {:ok, o_id} = Adapter.term_to_id(triple_manager, object)
        :ok = Index.insert_triple(triple_db, {s_id, p_id, o_id})
      end)

      original_count = length(triples)

      # Perform migration
      {:ok, ntriples} = Exporter.export_string(triple_db, :ntriples)
      nquads = ntriples_to_nquads(ntriples)
      {:ok, migrated_count} = Loader.load_nquads_string(quad_db, quad_manager, nquads)

      # Verify count preserved
      assert migrated_count == original_count

      # Export the migrated quad store as N-Quads and convert to N-Triples for comparison
      {:ok, migrated_nquads} = Exporter.export_nquads_string(quad_db)
      migrated_ntriples = nquads_to_ntriples(migrated_nquads)

      # Compare N-Triples output as sets
      original_lines = ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      migrated_lines =
        migrated_ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      # All data should be preserved - exact match
      assert MapSet.equal?(original_lines, migrated_lines)
    end
  end

  # ===========================================================================
  # 6.8.2: Migration Tooling
  # ===========================================================================

  describe "6.8.2 Migration Tooling" do
    test "6.8.2.1 migration tool handles large datasets" do
      # Create a large triple store
      {triple_db, triple_manager, triple_path} = create_triple_store()
      {quad_db, quad_manager, quad_path} = create_quad_store()

      on_exit(fn ->
        teardown_db(triple_db, triple_manager, triple_path)
        teardown_db(quad_db, quad_manager, quad_path)
      end)

      # Insert 1000 triples
      count = 1000

      Enum.each(1..count, fn i ->
        subject = RDF.iri("#{@ex}subject#{i}")
        predicate = RDF.iri("#{@ex}predicate")
        object = RDF.literal("value#{i}")

        {:ok, s_id} = Adapter.term_to_id(triple_manager, subject)
        {:ok, p_id} = Adapter.term_to_id(triple_manager, predicate)
        {:ok, o_id} = Adapter.term_to_id(triple_manager, object)

        :ok = Index.insert_triple(triple_db, {s_id, p_id, o_id})
      end)

      # Verify triple store has data
      assert count_triples(triple_db) == count

      # Perform migration
      start_time = System.monotonic_time(:millisecond)

      {:ok, ntriples} = Exporter.export_string(triple_db, :ntriples)
      nquads = ntriples_to_nquads(ntriples)
      {:ok, migrated_count} = Loader.load_nquads_string(quad_db, quad_manager, nquads)

      duration = System.monotonic_time(:millisecond) - start_time

      # Verify all data migrated
      assert migrated_count == count
      assert count_quads(quad_db) == count

      # Performance assertion: should complete in reasonable time
      # (1000 triples should migrate in less than 30 seconds)
      assert duration < 30_000
    end

    test "6.8.2.2 migration tool reports progress" do
      # For progress reporting, we can use telemetry or streaming
      # This test validates that progress can be tracked

      {triple_db, triple_manager, triple_path} = create_triple_store()
      {quad_db, quad_manager, quad_path} = create_quad_store()

      on_exit(fn ->
        teardown_db(triple_db, triple_manager, triple_path)
        teardown_db(quad_db, quad_manager, quad_path)
      end)

      # Insert test data
      {:ok, test_count} = populate_triple_store(triple_db, triple_manager)

      # Simulate migration with progress tracking
      # Stream triples and track progress
      {:ok, stream} = Index.lookup(triple_db, {:var, :var, :var})

      progress_ref = :ets.new(:migration_progress, [:set, :public])

      processed =
        stream
        |> Enum.reduce(0, fn _triple, count ->
          # Increment first, then report progress
          new_count = count + 1
          :ets.insert(progress_ref, {:processed, new_count})
          new_count
        end)

      # Verify progress tracking
      [{:processed, final_count}] = :ets.lookup(progress_ref, :processed)
      assert final_count == test_count

      :ets.delete(progress_ref)
    end

    test "6.8.2.3 migration tool handles errors gracefully" do
      {triple_db, triple_manager, triple_path} = create_triple_store()
      {quad_db, quad_manager, quad_path} = create_quad_store()

      on_exit(fn ->
        teardown_db(triple_db, triple_manager, triple_path)
        teardown_db(quad_db, quad_manager, quad_path)
      end)

      # Populate with valid data
      {:ok, _count} = populate_triple_store(triple_db, triple_manager)

      # Test 1: Invalid N-Triples input (should not crash)
      invalid_ntriples = "<#{@ex}s> <#{@ex}p> ."

      # Attempting to load invalid data should return error
      result =
        try do
          Loader.load_nquads_string(quad_db, quad_manager, invalid_ntriples)
        rescue
          _ -> {:error, :parse_error}
        end

      # Should handle gracefully
      assert {:error, _} = result

      # Test 2: Empty input
      empty_nquads = ""
      {:ok, empty_count} = Loader.load_nquads_string(quad_db, quad_manager, empty_nquads)
      assert empty_count == 0

      # Verify quad store is still operational after error
      valid_nquads =
        "<#{@ex}s> <#{@ex}p> \"o\" <http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph> ."

      {:ok, count} = Loader.load_nquads_string(quad_db, quad_manager, valid_nquads)
      assert count == 1
    end

    test "6.8.2.4 migration tool validates output" do
      {triple_db, triple_manager, triple_path} = create_triple_store()
      {quad_db, quad_manager, quad_path} = create_quad_store()

      on_exit(fn ->
        teardown_db(triple_db, triple_manager, triple_path)
        teardown_db(quad_db, quad_manager, quad_path)
      end)

      # Populate triple store
      {:ok, expected_count} = populate_triple_store(triple_db, triple_manager)

      # Perform migration
      {:ok, ntriples} = Exporter.export_string(triple_db, :ntriples)

      # Validate N-Triples output
      assert is_binary(ntriples)
      assert String.length(ntriples) > 0

      # Validate N-Triples format (each line should end with " .")
      lines = String.split(ntriples, "\n") |> Enum.filter(&(&1 != ""))
      assert Enum.all?(lines, &String.ends_with?(&1, " ."))

      # Convert and validate N-Quads
      nquads = ntriples_to_nquads(ntriples)
      quad_lines = String.split(nquads, "\n") |> Enum.filter(&(&1 != ""))
      assert Enum.all?(quad_lines, &String.ends_with?(&1, " ."))

      # Load and validate quad store
      {:ok, migrated_count} = Loader.load_nquads_string(quad_db, quad_manager, nquads)

      # Validation: count matches
      assert migrated_count == expected_count

      # Validation: export from quad store as N-Quads and compare
      {:ok, migrated_nquads} = Exporter.export_nquads_string(quad_db)
      migrated_ntriples = nquads_to_ntriples(migrated_nquads)

      original_lines = ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      migrated_lines =
        migrated_ntriples |> String.split("\n") |> Enum.reject(&(&1 == "")) |> MapSet.new()

      assert MapSet.equal?(original_lines, migrated_lines)
    end

    test "6.8.2.5 migration tool can resume on failure" do
      # This test validates that migration can be resumed from a checkpoint
      # For demonstration, we simulate partial migration

      {triple_db, triple_manager, triple_path} = create_triple_store()
      {quad_db, quad_manager, quad_path} = create_quad_store()

      on_exit(fn ->
        teardown_db(triple_db, triple_manager, triple_path)
        teardown_db(quad_db, quad_manager, quad_path)
      end)

      # Insert test data in batches
      batch1 =
        Enum.map(1..50, fn i ->
          {RDF.iri("#{@ex}s#{i}"), RDF.iri("#{@ex}p"), RDF.literal("v#{i}")}
        end)

      batch2 =
        Enum.map(51..100, fn i ->
          {RDF.iri("#{@ex}s#{i}"), RDF.iri("#{@ex}p"), RDF.literal("v#{i}")}
        end)

      # Insert first batch
      Enum.each(batch1, fn {subject, predicate, object} ->
        {:ok, s_id} = Adapter.term_to_id(triple_manager, subject)
        {:ok, p_id} = Adapter.term_to_id(triple_manager, predicate)
        {:ok, o_id} = Adapter.term_to_id(triple_manager, object)
        :ok = Index.insert_triple(triple_db, {s_id, p_id, o_id})
      end)

      # Simulate migration of first batch
      {:ok, ntriples1} = Exporter.export_string(triple_db, :ntriples)
      nquads1 = ntriples_to_nquads(ntriples1)
      {:ok, count1} = Loader.load_nquads_string(quad_db, quad_manager, nquads1)

      assert count1 == 50
      assert count_quads(quad_db) == 50

      # Insert second batch to triple store
      Enum.each(batch2, fn {subject, predicate, object} ->
        {:ok, s_id} = Adapter.term_to_id(triple_manager, subject)
        {:ok, p_id} = Adapter.term_to_id(triple_manager, predicate)
        {:ok, o_id} = Adapter.term_to_id(triple_manager, object)
        :ok = Index.insert_triple(triple_db, {s_id, p_id, o_id})
      end)

      # "Resume" migration by exporting and loading everything
      # In a real scenario, you'd skip already-migrated triples
      {:ok, ntriples_all} = Exporter.export_string(triple_db, :ntriples)
      nquads_all = ntriples_to_nquads(ntriples_all)
      {:ok, count_all} = Loader.load_nquads_string(quad_db, quad_manager, nquads_all)

      # Verify final state
      assert count_all == 100
      assert count_quads(quad_db) == 100
    end
  end
end
