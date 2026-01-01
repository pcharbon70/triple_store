defmodule TripleStore.LoaderTest do
  @moduledoc """
  Tests for Task 1.5.3: Bulk Loading Pipeline.

  Verifies:
  - load_graph/4 loads RDF.Graph into storage
  - load_file/4 parses and loads RDF files
  - load_string/5 parses and loads RDF content
  - load_stream/4 loads from arbitrary stream
  - Batching works correctly
  - Telemetry events are emitted
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Loader

  @test_db_base "/tmp/triple_store_loader_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      try do
        Manager.stop(manager)
      catch
        :exit, _ -> :ok
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # load_graph/4 Tests
  # ===========================================================================

  describe "load_graph/4" do
    test "loads empty graph", %{db: db, manager: manager} do
      graph = RDF.Graph.new()
      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 0
    end

    test "loads single triple", %{db: db, manager: manager} do
      graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("o")}
        ])

      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 1
    end

    test "loads multiple triples", %{db: db, manager: manager} do
      triples =
        for i <- 1..10 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 10
    end

    test "triples are stored in index", %{db: db, manager: manager} do
      graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("o")}
        ])

      {:ok, _} = Loader.load_graph(db, manager, graph)

      # Verify using index lookup (all triples pattern)
      {:ok, triples} = Index.lookup_all(db, {:var, :var, :var})
      assert length(triples) == 1
    end

    test "respects batch_size option", %{db: db, manager: manager} do
      # Create 25 triples, load with batch_size of 10
      triples =
        for i <- 1..25 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, count} = Loader.load_graph(db, manager, graph, batch_size: 10)
      assert count == 25
    end

    test "handles various term types", %{db: db, manager: manager} do
      triples = [
        # IRI subject and object
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.iri("http://example.org/o")},
        # Blank node subject
        {RDF.bnode("b1"), RDF.iri("http://example.org/p"), RDF.literal("value")},
        # Integer literal
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal(42)},
        # Language-tagged literal
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"),
         RDF.literal("hello", language: "en")}
      ]

      graph = RDF.Graph.new(triples)
      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 4
    end
  end

  # ===========================================================================
  # load_string/5 Tests
  # ===========================================================================

  describe "load_string/5" do
    test "loads Turtle content", %{db: db, manager: manager} do
      ttl = """
      @prefix ex: <http://example.org/> .
      ex:subject ex:predicate "object" .
      """

      {:ok, count} = Loader.load_string(db, manager, ttl, :turtle)
      assert count == 1
    end

    test "loads N-Triples content", %{db: db, manager: manager} do
      nt = """
      <http://example.org/s1> <http://example.org/p> "value1" .
      <http://example.org/s2> <http://example.org/p> "value2" .
      """

      {:ok, count} = Loader.load_string(db, manager, nt, :ntriples)
      assert count == 2
    end

    test "loads content with base IRI", %{db: db, manager: manager} do
      ttl = """
      @prefix : <> .
      :subject :predicate "object" .
      """

      {:ok, count} =
        Loader.load_string(db, manager, ttl, :turtle, base_iri: "http://example.org/")

      assert count == 1
    end

    test "returns error for invalid content", %{db: db, manager: manager} do
      invalid = "this is not valid turtle {{{{}"
      result = Loader.load_string(db, manager, invalid, :turtle)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # load_stream/4 Tests
  # ===========================================================================

  describe "load_stream/4" do
    test "loads from list of triples", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("o2")}
      ]

      {:ok, count} = Loader.load_stream(db, manager, triples)
      assert count == 2
    end

    test "loads from lazy stream", %{db: db, manager: manager} do
      stream =
        Stream.map(1..5, fn i ->
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end)

      {:ok, count} = Loader.load_stream(db, manager, stream)
      assert count == 5
    end

    test "loads empty stream", %{db: db, manager: manager} do
      {:ok, count} = Loader.load_stream(db, manager, [])
      assert count == 0
    end
  end

  # ===========================================================================
  # load_file/4 Tests
  # ===========================================================================

  describe "load_file/4" do
    setup %{path: test_path} do
      # Create test files directory
      files_dir = Path.join(test_path, "files")
      File.mkdir_p!(files_dir)
      {:ok, files_dir: files_dir}
    end

    test "loads Turtle file", %{db: db, manager: manager, files_dir: files_dir} do
      ttl_path = Path.join(files_dir, "test.ttl")

      File.write!(ttl_path, """
      @prefix ex: <http://example.org/> .
      ex:s1 ex:p "value1" .
      ex:s2 ex:p "value2" .
      """)

      {:ok, count} = Loader.load_file(db, manager, ttl_path)
      assert count == 2
    end

    test "loads N-Triples file", %{db: db, manager: manager, files_dir: files_dir} do
      nt_path = Path.join(files_dir, "test.nt")

      File.write!(nt_path, """
      <http://example.org/s1> <http://example.org/p> "value1" .
      <http://example.org/s2> <http://example.org/p> "value2" .
      <http://example.org/s3> <http://example.org/p> "value3" .
      """)

      {:ok, count} = Loader.load_file(db, manager, nt_path)
      assert count == 3
    end

    test "returns error for non-existent file", %{db: db, manager: manager} do
      result = Loader.load_file(db, manager, "/nonexistent/path/file.ttl")
      assert {:error, :file_not_found} = result
    end

    test "returns error for unsupported format", %{db: db, manager: manager, files_dir: files_dir} do
      txt_path = Path.join(files_dir, "test.txt")
      File.write!(txt_path, "some content")

      result = Loader.load_file(db, manager, txt_path)
      assert {:error, {:unsupported_format, ".txt", [supported: _]}} = result
    end

    test "respects explicit format option", %{db: db, manager: manager, files_dir: files_dir} do
      # Create file with wrong extension but valid content
      data_path = Path.join(files_dir, "data.txt")

      File.write!(data_path, """
      <http://example.org/s> <http://example.org/p> "value" .
      """)

      {:ok, count} = Loader.load_file(db, manager, data_path, format: :ntriples)
      assert count == 1
    end

    test "returns error for path traversal attempt", %{db: db, manager: manager} do
      result = Loader.load_file(db, manager, "../../../etc/passwd")
      assert {:error, :invalid_path} = result
    end

    test "returns error for file too large", %{db: db, manager: manager, files_dir: files_dir} do
      # Create a small file
      small_path = Path.join(files_dir, "small.ttl")
      File.write!(small_path, "<http://example.org/s> <http://example.org/p> \"v\" .")

      # Set a tiny max size to trigger the error
      result = Loader.load_file(db, manager, small_path, max_file_size: 10)
      assert {:error, {:file_too_large, _size, 10}} = result
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "telemetry events" do
    test "emits start event for load_graph", %{db: db, manager: manager} do
      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "test-start-#{inspect(ref)}",
        [:triple_store, :loader, :start],
        fn _event, measurements, metadata, _config ->
          send(pid, {:telemetry_start, measurements, metadata})
        end,
        nil
      )

      graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("o")}
        ])

      {:ok, _} = Loader.load_graph(db, manager, graph)

      assert_receive {:telemetry_start, measurements, metadata}
      assert Map.has_key?(measurements, :system_time)
      assert metadata.source == :graph

      :telemetry.detach("test-start-#{inspect(ref)}")
    end

    test "emits batch event for each batch", %{db: db, manager: manager} do
      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "test-batch-#{inspect(ref)}",
        [:triple_store, :loader, :batch],
        fn _event, measurements, metadata, _config ->
          send(pid, {:telemetry_batch, measurements, metadata})
        end,
        nil
      )

      # Create 250 triples, load with batch_size of 100 (should emit 3 batch events)
      # Note: minimum batch_size is 100 (clamped by loader)
      triples =
        for i <- 1..250 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph, batch_size: 100)

      # Should receive 3 batch events: 100, 100, 50
      assert_receive {:telemetry_batch, %{count: 100}, %{batch_number: 1}}
      assert_receive {:telemetry_batch, %{count: 100}, %{batch_number: 2}}
      assert_receive {:telemetry_batch, %{count: 50}, %{batch_number: 3}}

      :telemetry.detach("test-batch-#{inspect(ref)}")
    end

    test "emits stop event with total count", %{db: db, manager: manager} do
      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "test-stop-#{inspect(ref)}",
        [:triple_store, :loader, :stop],
        fn _event, measurements, metadata, _config ->
          send(pid, {:telemetry_stop, measurements, metadata})
        end,
        nil
      )

      triples =
        for i <- 1..15 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph)

      assert_receive {:telemetry_stop, measurements, metadata}
      assert measurements.total_count == 15
      assert Map.has_key?(measurements, :duration)
      assert metadata.source == :graph

      :telemetry.detach("test-stop-#{inspect(ref)}")
    end
  end

  # ===========================================================================
  # Large Scale Tests
  # ===========================================================================

  describe "large scale loading" do
    @tag :slow
    test "loads 10,000 triples efficiently", %{db: db, manager: manager} do
      triples =
        for i <- 1..10_000 do
          {
            RDF.iri("http://example.org/subject/#{i}"),
            RDF.iri("http://example.org/predicate/#{rem(i, 10)}"),
            RDF.literal("value-#{i}")
          }
        end

      graph = RDF.Graph.new(triples)

      {time_us, {:ok, count}} = :timer.tc(fn -> Loader.load_graph(db, manager, graph) end)

      assert count == 10_000

      # Should complete in under 5 seconds
      assert time_us < 5_000_000

      # Verify all triples are in storage
      {:ok, stored} = Index.count(db, {:var, :var, :var})
      assert stored == 10_000
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles unicode in literals", %{db: db, manager: manager} do
      graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"),
           RDF.literal("日本語 Русский 🌍")}
        ])

      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 1
    end

    test "handles very long IRIs", %{db: db, manager: manager} do
      long_iri = "http://example.org/" <> String.duplicate("a", 1000)

      graph =
        RDF.Graph.new([
          {RDF.iri(long_iri), RDF.iri("http://example.org/p"), RDF.literal("value")}
        ])

      {:ok, count} = Loader.load_graph(db, manager, graph)
      assert count == 1
    end

    test "handles duplicate triples in input", %{db: db, manager: manager} do
      triple =
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("o")}

      # RDF.Graph deduplicates, so this tests that case
      graph = RDF.Graph.new([triple, triple, triple])
      {:ok, count} = Loader.load_graph(db, manager, graph)
      # Graph only has 1 unique triple
      assert count == 1
    end
  end
end
