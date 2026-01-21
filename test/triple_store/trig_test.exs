defmodule TripleStore.TrigTest do
  @moduledoc """
  Tests for Section 2.3: TriG Format Support.

  Verifies that:
  - TriG file loading preserves all named graphs
  - TriG file loading with default graph only works correctly
  - TriG file loading with mixed graphs works correctly
  - TriG export produces valid TriG format with GRAPH blocks
  - TriG roundtrip (load + export) preserves all data
  - TriG string loading works correctly
  - TriG export to string works correctly
  - Empty TriG file loading works
  - Progress callback during TriG loading works
  - Batch processing with large TriG file works
  - TriG with prefixes and base IRI works correctly
  - TriG with blank node graphs works correctly
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.Exporter
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/triple_store_trig_test"

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
  # TriG File Loading Tests
  # ===========================================================================

  describe "load_trig_file/3" do
    test "loads TriG file with multiple named graphs", %{db: db, manager: manager} do
      # Create a test TriG file with multiple named graphs
      trig_content = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }

      GRAPH <http://example.org/g2> {
        ex:s3 ex:p "o3" .
      }
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      {:ok, count} = Loader.load_trig_file(db, manager, trig_file)

      assert count == 3

      # Verify quads were loaded in named graphs
      g1_iri = RDF.iri("http://example.org/g1")

      {:ok, g1_id} = Manager.get_or_create_id(manager, g1_iri)

      # Lookup quads in graph g1
      quads_in_g1 =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})

      assert length(quads_in_g1) == 2

      File.rm_rf(trig_file)
    end

    test "loads TriG file with default graph only", %{db: db, manager: manager} do
      # Create a test TriG file with default graph (no GRAPH blocks)
      trig_content = """
      @prefix ex: <http://example.org/>.

      ex:s1 ex:p "o1" .
      ex:s2 ex:p "o2" .
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      {:ok, count} = Loader.load_trig_file(db, manager, trig_file)

      assert count == 2

      # Verify quads were loaded in default graph (ID 0)
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2

      File.rm_rf(trig_file)
    end

    test "loads TriG file with mixed default and named graphs", %{db: db, manager: manager} do
      # Create a test TriG file with both default and named graphs
      trig_content = """
      @prefix ex: <http://example.org/>.

      ex:s1 ex:p "o1" .

      GRAPH <http://example.org/g1> {
        ex:s2 ex:p "o2" .
      }

      ex:s3 ex:p "o3" .

      GRAPH <http://example.org/g2> {
        ex:s4 ex:p "o4" .
      }
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      {:ok, count} = Loader.load_trig_file(db, manager, trig_file)

      assert count == 4

      # Verify default graph has 2 quads
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2

      # Verify named graphs have 2 quads total
      g1_iri = RDF.iri("http://example.org/g1")
      {:ok, g1_id} = Manager.get_or_create_id(manager, g1_iri)

      quads_in_g1 =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})

      assert length(quads_in_g1) == 1

      File.rm_rf(trig_file)
    end

    test "loads empty TriG file", %{db: db, manager: manager} do
      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, "")

      {:ok, count} = Loader.load_trig_file(db, manager, trig_file)

      assert count == 0

      File.rm_rf(trig_file)
    end

    test "handles file not found error", %{db: db, manager: manager} do
      trig_file = "/nonexistent/path/file.trig"

      result = Loader.load_trig_file(db, manager, trig_file)

      assert {:error, _} = result
    end

    test "handles invalid TriG file", %{db: db, manager: manager} do
      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, "this is not valid trig")

      result = Loader.load_trig_file(db, manager, trig_file)

      assert {:error, _} = result

      File.rm_rf(trig_file)
    end
  end

  # ===========================================================================
  # TriG String Loading Tests
  # ===========================================================================

  describe "load_trig_string/3" do
    test "loads TriG from string with named graphs", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }

      GRAPH <http://example.org/g2> {
        ex:s3 ex:p "o3" .
      }
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 3
    end

    test "loads TriG from string with default graph", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.

      ex:s1 ex:p "o1" .
      ex:s2 ex:p "o2" .
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 2

      # Verify loaded in default graph
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2
    end

    test "loads TriG from string with mixed graphs", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.

      ex:s1 ex:p "o1" .

      GRAPH <http://example.org/g1> {
        ex:s2 ex:p "o2" .
      }

      ex:s3 ex:p "o3" .
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 3

      # Verify default graph has 2 quads
      quads_in_default =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      assert length(quads_in_default) == 2

      # Verify named graph has 1 quad
      g1_iri = RDF.iri("http://example.org/g1")
      {:ok, g1_id} = Manager.get_or_create_id(manager, g1_iri)

      quads_in_g1 =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})

      assert length(quads_in_g1) == 1
    end

    test "handles invalid TriG string", %{db: db, manager: manager} do
      invalid_trig = "this is not valid trig"

      result = Loader.load_trig_string(db, manager, invalid_trig)

      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # TriG Export Tests
  # ===========================================================================

  describe "export_trig_file/3" do
    test "exports quads to TriG file", %{db: db, manager: manager} do
      # Load some test quads
      trig_string = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
      }

      ex:s2 ex:p "o2" .
      """

      {:ok, _} = Loader.load_trig_string(db, manager, trig_string)

      # Export to file
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.trig"
      {:ok, count} = Exporter.export_trig_file(db, output_file)

      assert count == 2

      # Verify file was created and contains valid TriG
      assert File.exists?(output_file)
      content = File.read!(output_file)

      assert String.contains?(content, "GRAPH")
      assert String.contains?(content, "<http://example.org/g1>")

      File.rm_rf(output_file)
    end

    test "exports only default graph quads when pattern specifies", %{db: db, manager: manager} do
      # Load some test quads
      trig_string = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
      }

      ex:s2 ex:p "o2" .
      """

      {:ok, _} = Loader.load_trig_string(db, manager, trig_string)

      # Export only default graph
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.trig"

      {:ok, count} =
        Exporter.export_trig_file(db, output_file,
          pattern: {:var, :var, :var, :bound},
          graph_id: 0
        )

      assert count == 1

      content = File.read!(output_file)

      # Should NOT contain GRAPH keyword for g1
      refute String.contains?(content, "<http://example.org/g1>")
      # Should contain s2
      assert String.contains?(content, "<http://example.org/s2>")

      File.rm_rf(output_file)
    end
  end

  describe "export_trig_string/2" do
    test "exports quads to TriG string", %{db: db, manager: manager} do
      # Load some test quads
      trig_string = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
      }

      ex:s2 ex:p "o2" .
      """

      {:ok, _} = Loader.load_trig_string(db, manager, trig_string)

      # Export to string
      {:ok, exported} = Exporter.export_trig_string(db)

      assert is_binary(exported)
      assert String.contains?(exported, "GRAPH")
      assert String.contains?(exported, "<http://example.org/g1>")
      assert String.contains?(exported, "<http://example.org/s1>")
      assert String.contains?(exported, "<http://example.org/s2>")
    end

    test "exports empty store to TriG string", %{db: db, manager: manager} do
      {:ok, exported} = Exporter.export_trig_string(db)

      assert is_binary(exported)
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip" do
    test "TriG roundtrip preserves all data", %{db: db, manager: manager} do
      original_trig = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }

      ex:s3 ex:p "o3" .

      GRAPH <http://example.org/g2> {
        ex:s4 ex:p "o4" .
      }
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, original_trig)

      # Load
      {:ok, load_count} = Loader.load_trig_file(db, manager, trig_file)

      # Export
      output_file = "#{@test_db_base}_output_#{:erlang.unique_integer()}.trig"
      {:ok, export_count} = Exporter.export_trig_file(db, output_file)

      assert load_count == export_count
      assert load_count == 4

      # Verify content is preserved
      exported_content = File.read!(output_file)

      # Check that named graphs are preserved
      assert String.contains?(exported_content, "<http://example.org/g1>")
      assert String.contains?(exported_content, "<http://example.org/g2>")

      # Check that default graph is preserved (triples outside GRAPH blocks)
      assert String.contains?(exported_content, "<http://example.org/s3>")

      File.rm_rf(trig_file)
      File.rm_rf(output_file)
    end

    test "TriG string roundtrip preserves all data", %{db: db, manager: manager} do
      original_trig = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g> {
        ex:s ex:p "o" .
      }

      ex:s2 ex:p "o2" .
      """

      # Load from string
      {:ok, load_count} = Loader.load_trig_string(db, manager, original_trig)

      # Export to string
      {:ok, exported} = Exporter.export_trig_string(db)

      assert load_count == 2
      assert String.contains?(exported, "<http://example.org/g>")
      assert String.contains?(exported, "<http://example.org/s>")
      assert String.contains?(exported, "<http://example.org/s2>")
    end
  end

  # ===========================================================================
  # Progress Callback Tests
  # ===========================================================================

  describe "progress callback" do
    test "progress callback is called during TriG loading", %{db: db, manager: manager} do
      # Create a larger TriG file to trigger multiple batches
      # Need at least 300 quads to trigger 3 batches (min batch_size is 100)
      quads =
        for i <- 1..300 do
          "ex:s#{i} ex:p \"o#{i}\" ."
        end
        |> Enum.join("\n")

      trig_content = """
      @prefix ex: <http://example.org/>.

      #{quads}
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      # Track progress callback invocations
      {:ok, callback_invocations} = Agent.start_link(fn -> [] end)

      progress_callback = fn info ->
        Agent.update(callback_invocations, fn list -> [info | list] end)
        :continue
      end

      {:ok, count} =
        Loader.load_trig_file(db, manager, trig_file,
          batch_size: 100,
          progress_callback: progress_callback,
          progress_interval: 2
        )

      assert count == 300

      # Verify callback was invoked
      invocations = Agent.get(callback_invocations, fn list -> list end)
      assert length(invocations) > 0

      Agent.stop(callback_invocations)
      File.rm_rf(trig_file)
    end

    test "progress callback can halt loading", %{db: db, manager: manager} do
      # Create quads - use 500 to get multiple batches
      quads =
        for i <- 1..500 do
          "ex:s#{i} ex:p \"o#{i}\" ."
        end
        |> Enum.join("\n")

      trig_content = """
      @prefix ex: <http://example.org/>.

      #{quads}
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      # Halt immediately when callback is first invoked
      progress_callback = fn _info ->
        :halt
      end

      result =
        Loader.load_trig_file(db, manager, trig_file,
          batch_size: 100,
          progress_callback: progress_callback,
          progress_interval: 1,
          parallel: false
        )

      assert {:halted, halted_count} = result
      # Should have loaded at most one batch (100 items) with sequential loading
      assert halted_count <= 100

      File.rm_rf(trig_file)
    end
  end

  # ===========================================================================
  # Batch Processing Tests
  # ===========================================================================

  describe "batch processing" do
    test "handles large TriG file with batch processing", %{db: db, manager: manager} do
      # Create a larger file with multiple named graphs
      quads =
        for i <- 1..500 do
          graph_rem = rem(i, 5)
          "ex:s#{i} ex:p \"o#{i}\" ."
        end
        |> Enum.join("\n")

      trig_content = """
      @prefix ex: <http://example.org/>.

      GRAPH <http://example.org/g1> {
        #{quads}
      }
      """

      trig_file = "#{@test_db_base}_test_#{:erlang.unique_integer()}.trig"
      File.write!(trig_file, trig_content)

      {:ok, count} =
        Loader.load_trig_file(db, manager, trig_file,
          batch_size: 50,
          bulk_mode: true
        )

      assert count == 500

      # Verify all quads were loaded
      all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
      assert length(all_quads) == 500

      File.rm_rf(trig_file)
    end
  end

  # ===========================================================================
  # Prefix and Base IRI Tests
  # ===========================================================================

  describe "prefixes and base IRI" do
    test "TriG with prefixes loads correctly", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.
      @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.

      GRAPH <http://example.org/g1> {
        ex:s1 ex:p "o1" .
        ex:s2 rdf:type ex:Thing .
      }
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 2

      # Verify the IRIs were expanded correctly
      all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
      assert length(all_quads) == 2
    end

    test "TriG with base IRI loads correctly", %{db: db, manager: manager} do
      trig_string = """
      @base <http://example.org/>.
      @prefix : <>.

      GRAPH <http://example.org/g1> {
        <s1> <p> "o1" .
      }
      """

      {:ok, count} =
        Loader.load_trig_string(db, manager, trig_string, base_iri: "http://example.org/")

      assert count == 1
    end

    test "TriG export with base IRI option", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.

      ex:s ex:p "o" .
      """

      {:ok, _} = Loader.load_trig_string(db, manager, trig_string)

      {:ok, exported} =
        Exporter.export_trig_string(db, base_iri: "http://example.org/")

      assert is_binary(exported)
    end
  end

  # ===========================================================================
  # Special Characters and Encoding Tests
  # ===========================================================================

  describe "special characters" do
    test "handles literals with quotes in TriG", %{db: db, manager: manager} do
      trig_string = ~s(@prefix ex: <http://example.org/>. ex:s ex:p "o with \\"quotes\\"" .)

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 1
    end

    test "handles unicode literals in TriG", %{db: db, manager: manager} do
      trig_string = ~s(@prefix ex: <http://example.org/>. ex:s ex:p "日本語" .)

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 1
    end

    test "handles literals with newlines in TriG", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.
      ex:s ex:p \"""o with
      newline\""" .
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 1
    end
  end

  # ===========================================================================
  # Blank Node Graph Tests
  # ===========================================================================

  describe "blank node graphs" do
    test "TriG with blank node as graph name", %{db: db, manager: manager} do
      trig_string = """
      @prefix ex: <http://example.org/>.

      GRAPH _:g1 {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }
      """

      {:ok, count} = Loader.load_trig_string(db, manager, trig_string)

      assert count == 2

      # Verify all quads were loaded
      all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
      assert length(all_quads) == 2
    end
  end
end
