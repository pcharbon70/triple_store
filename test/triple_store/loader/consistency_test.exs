defmodule TripleStore.Loader.ConsistencyTest do
  @moduledoc """
  Consistency tests for bulk loading (Task 1.6.2).

  Verifies data integrity after parallel bulk loading:
  - 1.6.2.1 Dictionary consistency (no duplicate IDs)
  - 1.6.2.2 Dictionary bidirectionality (encode/decode roundtrip)
  - 1.6.2.3 All three indices contain same triples
  - 1.6.2.4 Queries return correct results after bulk load
  - 1.6.2.5 Persistence survives restart
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.ShardedManager
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Index
  alias TripleStore.Loader

  @moduletag :integration
  @moduletag timeout: 120_000

  @test_db_base "/tmp/triple_store_consistency_test"

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
  # 1.6.2.1: Dictionary Consistency (No Duplicate IDs)
  # ===========================================================================

  describe "1.6.2.1 dictionary consistency - no duplicate IDs" do
    test "bulk load assigns unique IDs to each unique term", %{db: db, manager: manager} do
      # Generate triples with unique subjects but shared predicates
      triple_count = 5000
      triples = generate_triples_with_reuse(triple_count)
      graph = RDF.Graph.new(triples)

      {:ok, _count} = Loader.load_graph(db, manager, graph, bulk_mode: true, batch_size: 1000)

      # Collect all unique terms from triples
      all_terms =
        triples
        |> Enum.flat_map(fn {s, p, o} -> [s, p, o] end)
        |> Enum.uniq()

      # Get IDs for all terms
      ids = for term <- all_terms, do: lookup_id(db, term)

      # All IDs should be unique (no duplicates)
      unique_ids = Enum.uniq(ids)
      assert length(unique_ids) == length(ids), "Duplicate IDs detected!"
    end

    test "parallel loading produces unique IDs across concurrent batches", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Concurrent creation of terms across 10 processes
      tasks =
        for proc_id <- 1..10 do
          Task.async(fn ->
            terms = for i <- 1..500, do: RDF.iri("http://example.org/parallel/#{proc_id}/#{i}")
            ShardedManager.get_or_create_ids(sharded, terms)
          end)
        end

      results = Task.await_many(tasks, 60_000)
      all_ids = Enum.flat_map(results, fn {:ok, ids} -> ids end)

      # All 5000 IDs should be unique
      assert length(all_ids) == 5000
      assert length(Enum.uniq(all_ids)) == 5000, "Duplicate IDs created in parallel!"

      ShardedManager.stop(sharded)
    end

    test "repeated terms in different batches get same ID", %{db: db, manager: manager} do
      # Create shared predicates
      shared_predicates = [
        RDF.iri("http://example.org/predicate/name"),
        RDF.iri("http://example.org/predicate/age"),
        RDF.iri("http://example.org/predicate/type")
      ]

      # Batch 1: triples 1-1000 using shared predicates
      batch1 =
        for i <- 1..1000 do
          predicate = Enum.at(shared_predicates, rem(i, 3))
          {RDF.iri("http://example.org/s#{i}"), predicate, RDF.literal("value#{i}")}
        end

      graph1 = RDF.Graph.new(batch1)
      {:ok, _} = Loader.load_graph(db, manager, graph1, batch_size: 500)

      # Get IDs for shared predicates after batch 1
      ids_after_batch1 = for p <- shared_predicates, do: lookup_id(db, p)

      # Batch 2: triples 1001-2000 using same shared predicates
      batch2 =
        for i <- 1001..2000 do
          predicate = Enum.at(shared_predicates, rem(i, 3))
          {RDF.iri("http://example.org/s#{i}"), predicate, RDF.literal("value#{i}")}
        end

      graph2 = RDF.Graph.new(batch2)
      {:ok, _} = Loader.load_graph(db, manager, graph2, batch_size: 500)

      # Get IDs for shared predicates after batch 2
      ids_after_batch2 = for p <- shared_predicates, do: lookup_id(db, p)

      # IDs should be identical
      assert ids_after_batch1 == ids_after_batch2
    end
  end

  # ===========================================================================
  # 1.6.2.2: Dictionary Bidirectionality (Encode/Decode Roundtrip)
  # ===========================================================================

  describe "1.6.2.2 dictionary bidirectionality" do
    test "str2id and id2str are inverse operations after bulk load", %{db: db, manager: manager} do
      # Load a variety of term types
      triples =
        for i <- 1..1000 do
          subject = RDF.iri("http://example.org/roundtrip/#{i}")
          predicate = RDF.iri("http://example.org/predicate/#{rem(i, 10)}")
          object = RDF.literal("Object value #{i}")
          {subject, predicate, object}
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph, bulk_mode: true)

      # Get all unique terms
      all_terms =
        triples
        |> Enum.flat_map(fn {s, p, o} -> [s, p, o] end)
        |> Enum.uniq()

      # Verify roundtrip for each term
      for term <- all_terms do
        {:ok, key} = StringToId.encode_term(term)

        # str2id lookup
        {:ok, <<id::64-big>>} = NIF.get(db, :str2id, key)

        # id2str lookup
        {:ok, recovered_key} = NIF.get(db, :id2str, <<id::64-big>>)

        assert recovered_key == key,
               "Roundtrip failed for term #{inspect(term)}: key mismatch"
      end
    end

    test "URIs, blank nodes, and literals all roundtrip correctly", %{db: db, manager: manager} do
      # Create triples with various term types
      triples = [
        # URIs
        {RDF.iri("http://example.org/uri1"),
         RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
         RDF.iri("http://example.org/Class1")},
        # Blank nodes
        {RDF.bnode("bn1"), RDF.iri("http://example.org/prop"), RDF.bnode("bn2")},
        # String literals
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/name"),
         RDF.literal("Plain string")},
        # Typed literals
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/count"),
         RDF.literal(42)},
        # Language-tagged literals
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/label"),
         RDF.literal("English label", language: "en")}
      ]

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph)

      # Verify each term type
      for {s, p, o} <- triples, term <- [s, p, o] do
        {:ok, key} = StringToId.encode_term(term)
        {:ok, <<id::64-big>>} = NIF.get(db, :str2id, key)
        {:ok, recovered_key} = NIF.get(db, :id2str, <<id::64-big>>)

        assert recovered_key == key,
               "Roundtrip failed for #{inspect(term)}"

        # Verify type tag is correct
        {type, _seq} = Dictionary.decode_id(id)
        expected_type = term_type(term)
        assert type == expected_type, "Type mismatch for #{inspect(term)}"
      end
    end

    test "inline numeric literals preserve values", %{db: db, manager: manager} do
      # Inline numerics don't use str2id/id2str, so test them differently
      triples = [
        {RDF.iri("http://example.org/n1"), RDF.iri("http://example.org/value"),
         RDF.XSD.Integer.new!(12_345)},
        {RDF.iri("http://example.org/n2"), RDF.iri("http://example.org/value"),
         RDF.XSD.Integer.new!(-9999)},
        {RDF.iri("http://example.org/n3"), RDF.iri("http://example.org/value"),
         RDF.XSD.Integer.new!(0)}
      ]

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph)

      # Query back and verify values
      for {s, p, o} <- triples do
        {:ok, s_id} = Manager.lookup_id(manager, s)
        {:ok, p_id} = Manager.lookup_id(manager, p)
        {:ok, o_id} = Manager.get_or_create_id(manager, o)

        # Verify the triple exists
        {:ok, true} = Index.triple_exists?(db, {s_id, p_id, o_id})
      end
    end
  end

  # ===========================================================================
  # 1.6.2.3: All Three Indices Contain Same Triples
  # ===========================================================================

  describe "1.6.2.3 index consistency - all three indices match" do
    test "SPO, POS, and OSP indices contain identical triples", %{db: db, manager: manager} do
      triple_count = 2000
      triples = generate_triples_with_reuse(triple_count)
      graph = RDF.Graph.new(triples)

      {:ok, count} = Loader.load_graph(db, manager, graph, bulk_mode: true, batch_size: 500)
      assert count == triple_count

      # Get all triples from each index
      {:ok, spo_triples} = scan_index(db, :spo)
      {:ok, pos_triples} = scan_index(db, :pos)
      {:ok, osp_triples} = scan_index(db, :osp)

      # Convert to canonical form for comparison
      spo_set = MapSet.new(spo_triples)
      pos_set = MapSet.new(pos_triples)
      osp_set = MapSet.new(osp_triples)

      assert MapSet.equal?(spo_set, pos_set),
             "SPO and POS indices differ! SPO: #{MapSet.size(spo_set)}, POS: #{MapSet.size(pos_set)}"

      assert MapSet.equal?(spo_set, osp_set),
             "SPO and OSP indices differ! SPO: #{MapSet.size(spo_set)}, OSP: #{MapSet.size(osp_set)}"

      assert MapSet.size(spo_set) == triple_count,
             "Index count #{MapSet.size(spo_set)} != expected #{triple_count}"
    end

    test "parallel bulk load maintains index consistency", %{db: db, manager: manager} do
      triple_count = 5000
      triples = generate_triples_with_reuse(triple_count)
      graph = RDF.Graph.new(triples)

      # Load with parallel stages
      {:ok, count} =
        Loader.load_graph(db, manager, graph,
          bulk_mode: true,
          stages: System.schedulers_online(),
          max_demand: 5,
          batch_size: 1000
        )

      assert count == triple_count

      # Verify index consistency
      {:ok, spo_triples} = scan_index(db, :spo)
      {:ok, pos_triples} = scan_index(db, :pos)
      {:ok, osp_triples} = scan_index(db, :osp)

      spo_set = MapSet.new(spo_triples)
      pos_set = MapSet.new(pos_triples)
      osp_set = MapSet.new(osp_triples)

      assert MapSet.equal?(spo_set, pos_set)
      assert MapSet.equal?(spo_set, osp_set)
      assert MapSet.size(spo_set) == triple_count
    end

    test "each triple is findable via all indices", %{db: db, manager: manager} do
      # Use a smaller set for detailed verification
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"),
         RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p1"),
         RDF.literal("o2")},
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p2"),
         RDF.literal("o3")}
      ]

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph)

      for {s, p, o} <- triples do
        {:ok, s_id} = Manager.lookup_id(manager, s)
        {:ok, p_id} = Manager.lookup_id(manager, p)
        {:ok, o_id} = Manager.get_or_create_id(manager, o)

        # Check SPO
        spo_key = Index.spo_key(s_id, p_id, o_id)
        {:ok, true} = NIF.exists(db, :spo, spo_key)

        # Check POS
        pos_key = Index.pos_key(p_id, o_id, s_id)
        {:ok, true} = NIF.exists(db, :pos, pos_key)

        # Check OSP
        osp_key = Index.osp_key(o_id, s_id, p_id)
        {:ok, true} = NIF.exists(db, :osp, osp_key)
      end
    end
  end

  # ===========================================================================
  # 1.6.2.4: Queries Return Correct Results After Bulk Load
  # ===========================================================================

  describe "1.6.2.4 query correctness after bulk load" do
    test "subject-based lookups return correct triples", %{db: db, manager: manager} do
      # Create 100 subjects, each with 5 triples
      triples =
        for subject_num <- 1..100, triple_num <- 1..5 do
          {
            RDF.iri("http://example.org/subject/#{subject_num}"),
            RDF.iri("http://example.org/predicate/#{triple_num}"),
            RDF.literal("Value #{subject_num}-#{triple_num}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph, bulk_mode: true)

      # Query for specific subject
      subject = RDF.iri("http://example.org/subject/42")
      {:ok, s_id} = Manager.lookup_id(manager, subject)

      {:ok, results} = Index.lookup_all(db, {{:bound, s_id}, :var, :var})

      # Should find exactly 5 triples
      assert length(results) == 5

      # All should have correct subject
      for {result_s, _p, _o} <- results do
        assert result_s == s_id
      end
    end

    test "predicate-based lookups return correct triples", %{db: db, manager: manager} do
      # Use 10 predicates across 500 triples
      triples =
        for i <- 1..500 do
          {
            RDF.iri("http://example.org/s/#{i}"),
            RDF.iri("http://example.org/p/#{rem(i, 10)}"),
            RDF.literal("value#{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph, bulk_mode: true)

      # Query for predicate p/5 - should match 50 triples
      predicate = RDF.iri("http://example.org/p/5")
      {:ok, p_id} = Manager.lookup_id(manager, predicate)

      {:ok, results} = Index.lookup_all(db, {:var, {:bound, p_id}, :var})

      # 500/10 = 50 triples per predicate
      assert length(results) == 50

      # All should have correct predicate
      for {_s, result_p, _o} <- results do
        assert result_p == p_id
      end
    end

    test "object-based lookups return correct triples", %{db: db, manager: manager} do
      # Create triples where some objects are shared
      shared_object = RDF.iri("http://example.org/shared-target")

      triples =
        for i <- 1..100 do
          obj = if rem(i, 5) == 0, do: shared_object, else: RDF.literal("unique-#{i}")

          {
            RDF.iri("http://example.org/subj/#{i}"),
            RDF.iri("http://example.org/pred"),
            obj
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db, manager, graph)

      # Query for shared object - should match 20 triples (100/5)
      {:ok, o_id} = Manager.lookup_id(manager, shared_object)

      {:ok, results} = Index.lookup_all(db, {:var, :var, {:bound, o_id}})

      assert length(results) == 20

      # All should have correct object
      for {_s, _p, result_o} <- results do
        assert result_o == o_id
      end
    end

    test "full scan returns all loaded triples", %{db: db, manager: manager} do
      triple_count = 1000
      triples = generate_triples_with_reuse(triple_count)
      graph = RDF.Graph.new(triples)

      {:ok, count} = Loader.load_graph(db, manager, graph, bulk_mode: true)
      assert count == triple_count

      # Full scan
      {:ok, all_triples} = Index.lookup_all(db, {:var, :var, :var})

      assert length(all_triples) == triple_count
    end
  end

  # ===========================================================================
  # 1.6.2.5: Persistence Survives Restart
  # ===========================================================================

  describe "1.6.2.5 persistence survives restart" do
    test "data persists after close and reopen", %{db: db, manager: manager, path: path} do
      # Close setup db first
      Manager.stop(manager)
      NIF.close(db)

      # Use a fresh path for this test to avoid conflicts
      test_path = "#{path}_persist_test"

      # First session: load data
      {:ok, db1} = NIF.open(test_path)
      {:ok, manager1} = Manager.start_link(db: db1)

      triples =
        for i <- 1..500 do
          {
            RDF.iri("http://example.org/persist/s/#{i}"),
            RDF.iri("http://example.org/persist/p"),
            RDF.literal("persistent value #{i}")
          }
        end

      graph = RDF.Graph.new(triples)
      {:ok, count1} = Loader.load_graph(db1, manager1, graph, bulk_mode: true)
      assert count1 == 500

      # Close first session
      Manager.stop(manager1)
      NIF.close(db1)

      # Ensure db is fully released before reopening
      :erlang.garbage_collect()
      Process.sleep(200)

      # Second session: verify data
      {:ok, db2} = NIF.open(test_path)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Query should return all triples
      {:ok, all_triples} = Index.lookup_all(db2, {:var, :var, :var})
      assert length(all_triples) == 500

      # Verify specific term IDs are retrievable
      sample_term = RDF.iri("http://example.org/persist/s/250")
      {:ok, id} = Manager.lookup_id(manager2, sample_term)
      assert is_integer(id)

      Manager.stop(manager2)
      NIF.close(db2)
      File.rm_rf(test_path)
    end

    test "dictionary state persists correctly", %{db: db, manager: manager, path: path} do
      # Close setup db first
      Manager.stop(manager)
      NIF.close(db)

      # Use a fresh path for this test
      test_path = "#{path}_dict_persist"

      # First session: create terms
      {:ok, db1} = NIF.open(test_path)
      {:ok, manager1} = Manager.start_link(db: db1)

      terms = [
        RDF.iri("http://example.org/persist/uri"),
        RDF.bnode("persist_bnode"),
        RDF.literal("persistent literal")
      ]

      ids1 =
        for term <- terms do
          {:ok, id} = Manager.get_or_create_id(manager1, term)
          id
        end

      Manager.stop(manager1)
      NIF.close(db1)

      # Ensure db is fully released before reopening
      :erlang.garbage_collect()
      Process.sleep(200)

      # Second session: verify same IDs
      {:ok, db2} = NIF.open(test_path)
      {:ok, manager2} = Manager.start_link(db: db2)

      ids2 =
        for term <- terms do
          {:ok, id} = Manager.get_or_create_id(manager2, term)
          id
        end

      assert ids1 == ids2, "IDs changed after restart!"

      Manager.stop(manager2)
      NIF.close(db2)
      File.rm_rf(test_path)
    end

    test "index consistency maintained after restart", %{db: db, manager: manager, path: path} do
      # Close setup db first
      Manager.stop(manager)
      NIF.close(db)

      # Use a fresh path for this test
      test_path = "#{path}_index_persist"

      # First session: load data
      {:ok, db1} = NIF.open(test_path)
      {:ok, manager1} = Manager.start_link(db: db1)

      triples = generate_triples_with_reuse(1000)
      graph = RDF.Graph.new(triples)
      {:ok, _} = Loader.load_graph(db1, manager1, graph, bulk_mode: true)

      Manager.stop(manager1)
      NIF.close(db1)

      # Ensure db is fully released before reopening
      :erlang.garbage_collect()
      Process.sleep(200)

      # Second session: verify index consistency
      {:ok, db2} = NIF.open(test_path)

      {:ok, spo_triples} = scan_index(db2, :spo)
      {:ok, pos_triples} = scan_index(db2, :pos)
      {:ok, osp_triples} = scan_index(db2, :osp)

      spo_set = MapSet.new(spo_triples)
      pos_set = MapSet.new(pos_triples)
      osp_set = MapSet.new(osp_triples)

      assert MapSet.equal?(spo_set, pos_set)
      assert MapSet.equal?(spo_set, osp_set)
      assert MapSet.size(spo_set) == 1000

      NIF.close(db2)
      File.rm_rf(test_path)
    end

    test "sequence counter resumes correctly after restart", %{db: db, manager: manager, path: path} do
      # Close setup db first
      Manager.stop(manager)
      NIF.close(db)

      # Use a fresh path for this test
      test_path = "#{path}_seq_persist"

      # First session: create terms
      {:ok, db1} = NIF.open(test_path)
      {:ok, manager1} = Manager.start_link(db: db1)

      terms1 = for i <- 1..100, do: RDF.iri("http://example.org/seq/#{i}")

      ids1 =
        for term <- terms1 do
          {:ok, id} = Manager.get_or_create_id(manager1, term)
          id
        end

      max_seq1 =
        ids1
        |> Enum.map(fn id ->
          {_type, seq} = Dictionary.decode_id(id)
          seq
        end)
        |> Enum.max()

      Manager.stop(manager1)
      NIF.close(db1)

      # Ensure db is fully released before reopening
      :erlang.garbage_collect()
      Process.sleep(200)

      # Second session: create more terms
      {:ok, db2} = NIF.open(test_path)
      {:ok, manager2} = Manager.start_link(db: db2)

      terms2 = for i <- 101..200, do: RDF.iri("http://example.org/seq/#{i}")

      ids2 =
        for term <- terms2 do
          {:ok, id} = Manager.get_or_create_id(manager2, term)
          id
        end

      min_seq2 =
        ids2
        |> Enum.map(fn id ->
          {_type, seq} = Dictionary.decode_id(id)
          seq
        end)
        |> Enum.min()

      # New sequence numbers should be greater than old ones
      assert min_seq2 > max_seq1,
             "Sequence counter did not resume correctly: min_seq2=#{min_seq2}, max_seq1=#{max_seq1}"

      Manager.stop(manager2)
      NIF.close(db2)
      File.rm_rf(test_path)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Generate triples with some shared predicates (typical RDF pattern)
  defp generate_triples_with_reuse(count) do
    predicates = [
      RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
      RDF.iri("http://www.w3.org/2000/01/rdf-schema#label"),
      RDF.iri("http://example.org/prop/name"),
      RDF.iri("http://example.org/prop/value"),
      RDF.iri("http://example.org/prop/related")
    ]

    for i <- 1..count do
      predicate = Enum.at(predicates, rem(i, length(predicates)))

      {
        RDF.iri("http://example.org/subject/#{i}"),
        predicate,
        RDF.literal("Value #{i}")
      }
    end
  end

  # Lookup ID for a term directly from RocksDB
  defp lookup_id(db, term) do
    {:ok, key} = StringToId.encode_term(term)

    case NIF.get(db, :str2id, key) do
      {:ok, <<id::64-big>>} -> id
      {:error, :not_found} -> nil
    end
  end

  # Get term type for verification
  defp term_type(%RDF.IRI{}), do: :uri
  defp term_type(%RDF.BlankNode{}), do: :bnode
  defp term_type(%RDF.Literal{}), do: :literal

  # Scan an index and convert keys to canonical {s, p, o} triples
  defp scan_index(db, index) do
    # Use prefix_stream with empty prefix to get all entries
    case NIF.prefix_stream(db, index, <<>>) do
      {:ok, stream} ->
        triples =
          stream
          |> Enum.map(fn {key, _value} -> Index.key_to_triple(index, key) end)

        {:ok, triples}

      {:error, _} = error ->
        error
    end
  end
end
