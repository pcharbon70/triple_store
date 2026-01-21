defmodule TripleStore.NQuadsTest do
  @moduledoc """
  Tests for Section 2.2: N-Quads Format Support.

  Verifies that:
  - N-Quads file loading preserves all named graphs
  - N-Quads file loading with default graph only works correctly
  - N-Quads file loading with mixed graphs works correctly
  - N-Quads export produces valid N-Quads format
  - N-Quads roundtrip (load + export) preserves all data
  - N-Quads string loading works correctly
  - N-Quads export to string works correctly
  - Empty N-Quads file loading works
  - Progress callback during N-Quads loading works
  - Batch processing with large N-Quads file works
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.Exporter
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/triple_store_nquads_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path, schema: :quad)
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
  # N-Quads File Loading Tests
  # ===========================================================================

  describe "load_nquads_file/3" do
    test "loads N-Quads file with named graphs", %{db: db, manager: manager} do
      # Create a test N-Quads file with named graphs
      nquads_content = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" <http://example.org/g2> .
      <http://example.org/s3> <http://example.org/p> "o3" <http://example.org/g1> .
      """

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, nquads_content)

      {:ok, count} = Loader.load_nquads_file(db, manager, nq_file)

      assert count == 3

      # Verify quads were loaded in named graphs
      g1_iri = RDF.iri("http://example.org/g1")

      {:ok, g1_id} = Manager.get_or_create_id(manager, g1_iri)

      # Lookup quads in graph g1
      quads_in_g1 =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})

      assert length(quads_in_g1) == 2

      File.rm_rf(nq_file)
    end

    test "loads N-Quads file with default graph only", %{db: db, manager: manager} do
      # Create a test N-Quads file with default graph (no graph name)
      nquads_content = """
      <http://example.org/s1> <http://example.org/p> "o1" .
      <http://example.org/s2> <http://example.org/p> "o2" .
      """

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, nquads_content)

      {:ok, count} = Loader.load_nquads_file(db, manager, nq_file)

      assert count == 2

      # Verify quads were loaded in default graph (ID 0)
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2

      File.rm_rf(nq_file)
    end

    test "loads N-Quads file with mixed graphs", %{db: db, manager: manager} do
      # Create a test N-Quads file with both named and default graphs
      nquads_content = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" .
      <http://example.org/s3> <http://example.org/p> "o3" <http://example.org/g2> .
      """

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, nquads_content)

      {:ok, count} = Loader.load_nquads_file(db, manager, nq_file)

      assert count == 3

      # Verify default graph has 1 quad
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 1

      # Verify named graphs have 2 quads total
      g1_iri = RDF.iri("http://example.org/g1")
      {:ok, g1_id} = Manager.get_or_create_id(manager, g1_iri)

      quads_in_g1 =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})

      assert length(quads_in_g1) == 1

      File.rm_rf(nq_file)
    end

    test "loads empty N-Quads file", %{db: db, manager: manager} do
      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, "")

      {:ok, count} = Loader.load_nquads_file(db, manager, nq_file)

      assert count == 0

      File.rm_rf(nq_file)
    end

    test "handles file not found error", %{db: db, manager: manager} do
      nq_file = "/nonexistent/path/file.nq"

      result = Loader.load_nquads_file(db, manager, nq_file)

      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # N-Quads String Loading Tests
  # ===========================================================================

  describe "load_nquads_string/3" do
    test "loads N-Quads from string with named graphs", %{db: db, manager: manager} do
      nquads_string = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" <http://example.org/g2> .
      """

      {:ok, count} = Loader.load_nquads_string(db, manager, nquads_string)

      assert count == 2
    end

    test "loads N-Quads from string with default graph", %{db: db, manager: manager} do
      nquads_string = """
      <http://example.org/s1> <http://example.org/p> "o1" .
      <http://example.org/s2> <http://example.org/p> "o2" .
      """

      {:ok, count} = Loader.load_nquads_string(db, manager, nquads_string)

      assert count == 2

      # Verify loaded in default graph
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2
    end

    test "handles invalid N-Quads string", %{db: db, manager: manager} do
      invalid_nquads = "this is not valid n-quads"

      result = Loader.load_nquads_string(db, manager, invalid_nquads)

      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # N-Quads Export Tests
  # ===========================================================================

  describe "export_nquads_file/3" do
    test "exports quads to N-Quads file", %{db: db, manager: manager} do
      # Load some test quads
      nquads_string = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" .
      """

      {:ok, _} = Loader.load_nquads_string(db, manager, nquads_string)

      # Export to file
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.nq"
      {:ok, count} = Exporter.export_nquads_file(db, output_file)

      assert count == 2

      # Verify file was created and contains valid N-Quads
      assert File.exists?(output_file)
      content = File.read!(output_file)

      assert String.contains?(content, "<http://example.org/s1>")
      assert String.contains?(content, "<http://example.org/g1>")

      File.rm_rf(output_file)
    end

    test "exports only default graph quads when pattern specifies", %{db: db, manager: manager} do
      # Load some test quads
      nquads_string = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" .
      """

      {:ok, _} = Loader.load_nquads_string(db, manager, nquads_string)

      # Export only default graph
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.nq"

      {:ok, count} =
        Exporter.export_nquads_file(db, output_file,
          pattern: {:var, :var, :var, :bound},
          graph_id: 0
        )

      assert count == 1

      content = File.read!(output_file)

      # Should NOT contain g1
      refute String.contains?(content, "<http://example.org/g1>")
      # Should contain s2
      assert String.contains?(content, "<http://example.org/s2>")

      File.rm_rf(output_file)
    end
  end

  describe "export_nquads_string/2" do
    test "exports quads to N-Quads string", %{db: db, manager: manager} do
      # Load some test quads
      nquads_string = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" .
      """

      {:ok, _} = Loader.load_nquads_string(db, manager, nquads_string)

      # Export to string
      {:ok, exported} = Exporter.export_nquads_string(db)

      assert is_binary(exported)
      assert String.contains?(exported, "<http://example.org/s1>")
      assert String.contains?(exported, "<http://example.org/g1>")
      assert String.contains?(exported, "<http://example.org/s2>")
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip" do
    test "N-Quads roundtrip preserves all data", %{db: db, manager: manager} do
      original_nquads = """
      <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/g1> .
      <http://example.org/s2> <http://example.org/p> "o2" <http://example.org/g2> .
      <http://example.org/s3> <http://example.org/p> "o3" .
      <http://example.org/s4> <http://example.org/p> "o4" <http://example.org/g1> .
      """

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, original_nquads)

      # Load
      {:ok, load_count} = Loader.load_nquads_file(db, manager, nq_file)

      # Export
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.nq"
      {:ok, export_count} = Exporter.export_nquads_file(db, output_file)

      assert load_count == export_count
      assert load_count == 4

      # Verify content is preserved
      exported_content = File.read!(output_file)

      # Check that named graphs are preserved
      assert String.contains?(exported_content, "<http://example.org/g1>")
      assert String.contains?(exported_content, "<http://example.org/g2>")

      # Check that default graph is preserved (no graph name)
      assert exported_content =~
               ~r/<http:\/\/example\.org\/s3>.*<http:\/\/example\.org\/p>.*"o3"\s*\./

      File.rm_rf(nq_file)
      File.rm_rf(output_file)
    end

    test "N-Quads string roundtrip preserves all data", %{db: db, manager: manager} do
      original_nquads = """
      <http://example.org/s> <http://example.org/p> "o" <http://example.org/g> .
      """

      # Load from string
      {:ok, load_count} = Loader.load_nquads_string(db, manager, original_nquads)

      # Export to string
      {:ok, exported} = Exporter.export_nquads_string(db)

      assert load_count == 1
      assert String.contains?(exported, "<http://example.org/g>")
    end
  end

  # ===========================================================================
  # Progress Callback Tests
  # ===========================================================================

  describe "progress callback" do
    test "progress callback is called during N-Quads loading", %{db: db, manager: manager} do
      # Create a larger N-Quads file to trigger multiple batches
      # Need at least 300 quads to trigger 3 batches (min batch_size is 100)
      quads =
        for i <- 1..300 do
          "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" <http://example.org/g> ."
        end
        |> Enum.join("\n")

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, quads)

      # Track progress callback invocations
      {:ok, callback_invocations} = Agent.start_link(fn -> [] end)

      progress_callback = fn info ->
        Agent.update(callback_invocations, fn list -> [info | list] end)
        :continue
      end

      {:ok, count} =
        Loader.load_nquads_file(db, manager, nq_file,
          batch_size: 100,
          progress_callback: progress_callback,
          progress_interval: 2
        )

      assert count == 300

      # Verify callback was invoked
      invocations = Agent.get(callback_invocations, & &1)
      assert length(invocations) > 0

      Agent.stop(callback_invocations)
      File.rm_rf(nq_file)
    end

    test "progress callback can halt loading", %{db: db, manager: manager} do
      # Create quads - use 500 to get multiple batches
      quads =
        for i <- 1..500 do
          "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" ."
        end
        |> Enum.join("\n")

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, quads)

      # Halt immediately when callback is first invoked
      progress_callback = fn _info ->
        :halt
      end

      result =
        Loader.load_nquads_file(db, manager, nq_file,
          batch_size: 100,
          progress_callback: progress_callback,
          progress_interval: 1,
          # Use sequential loading for deterministic halting
          parallel: false
        )

      assert {:halted, halted_count} = result
      # Should have loaded at most one batch (100 items) with sequential loading
      assert halted_count <= 100

      File.rm_rf(nq_file)
    end
  end

  # ===========================================================================
  # Batch Processing Tests
  # ===========================================================================

  describe "batch processing" do
    test "handles large N-Quads file with batch processing", %{db: db, manager: manager} do
      # Create a larger file
      quads =
        for i <- 1..500 do
          "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" <http://example.org/g#{rem(i, 5)}> ."
        end
        |> Enum.join("\n")

      nq_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.nq"
      File.write!(nq_file, quads)

      {:ok, count} =
        Loader.load_nquads_file(db, manager, nq_file,
          batch_size: 50,
          bulk_mode: true
        )

      assert count == 500

      # Verify all quads were loaded
      all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
      assert length(all_quads) == 500

      File.rm_rf(nq_file)
    end
  end

  # ===========================================================================
  # Special Characters and Encoding Tests
  # ===========================================================================

  describe "special characters" do
    test "handles literals with quotes in N-Quads", %{db: db, manager: manager} do
      nquads_string = ~s(<http://example.org/s> <http://example.org/p> "o with \\"quotes\\"" .)

      {:ok, count} = Loader.load_nquads_string(db, manager, nquads_string)

      assert count == 1
    end

    test "handles unicode literals in N-Quads", %{db: db, manager: manager} do
      nquads_string = ~s(<http://example.org/s> <http://example.org/p> "日本語" .)

      {:ok, count} = Loader.load_nquads_string(db, manager, nquads_string)

      assert count == 1
    end
  end
end
