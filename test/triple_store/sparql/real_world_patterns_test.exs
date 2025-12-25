defmodule TripleStore.SPARQL.RealWorldPatternsTest do
  @moduledoc """
  Tests for real-world SPARQL query patterns commonly seen in production.

  These tests cover query patterns from:
  - Wikidata-style queries
  - DBpedia-style queries
  - Common enterprise patterns
  - Plan cache effectiveness with Zipfian distribution
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.{Query, PlanCache}
  alias TripleStore.Update

  import TripleStore.Test.IntegrationHelpers, only: [extract_count: 1, get_iri: 1]

  @moduletag :real_world_patterns

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @ex "http://example.org/"
  @schema "http://schema.org/"

  setup do
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "real_world_#{test_id}")

    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      File.rm_rf!(db_path)
    end)

    %{ctx: ctx, db_path: db_path}
  end

  # ===========================================================================
  # Wikidata-style Patterns
  # ===========================================================================

  describe "Wikidata-style query patterns" do
    test "find items by type with labels", %{ctx: ctx} do
      # Pattern: Find all items of type Person with their labels
      # Similar to Wikidata's wdt:P31/wd:Q5 pattern
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Person"},
        {"#{@ex}alice", "#{@rdfs}label", "Alice Smith"},
        {"#{@ex}bob", "#{@rdf}type", "#{@ex}Person"},
        {"#{@ex}bob", "#{@rdfs}label", "Bob Jones"},
        {"#{@ex}acme", "#{@rdf}type", "#{@ex}Organization"}
      ])

      sparql = """
      SELECT ?item ?label WHERE {
        ?item <#{@rdf}type> <#{@ex}Person> .
        ?item <#{@rdfs}label> ?label
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert length(results) == 2
    end

    test "find related items via property chains", %{ctx: ctx} do
      # Pattern: Find author's publications' subjects
      # Similar to property path queries in Wikidata
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@ex}authored", "#{@ex}paper1"},
        {"#{@ex}alice", "#{@ex}authored", "#{@ex}paper2"},
        {"#{@ex}paper1", "#{@ex}subject", "#{@ex}AI"},
        {"#{@ex}paper2", "#{@ex}subject", "#{@ex}ML"},
        {"#{@ex}paper2", "#{@ex}subject", "#{@ex}AI"}
      ])

      sparql = """
      SELECT DISTINCT ?subject WHERE {
        <#{@ex}alice> <#{@ex}authored>/<#{@ex}subject> ?subject
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      subjects = Enum.map(results, &get_iri(&1["subject"])) |> MapSet.new()
      assert MapSet.member?(subjects, "#{@ex}AI")
      assert MapSet.member?(subjects, "#{@ex}ML")
    end
  end

  # ===========================================================================
  # DBpedia-style Patterns
  # ===========================================================================

  describe "DBpedia-style query patterns" do
    test "category hierarchy traversal", %{ctx: ctx} do
      # Pattern: Find all parent categories via skos:broader*
      insert_triples(ctx, [
        {"#{@ex}Category:ML", "#{@ex}broader", "#{@ex}Category:AI"},
        {"#{@ex}Category:AI", "#{@ex}broader", "#{@ex}Category:CS"},
        {"#{@ex}Category:CS", "#{@ex}broader", "#{@ex}Category:Science"},
        {"#{@ex}Category:NLP", "#{@ex}broader", "#{@ex}Category:AI"}
      ])

      sparql = """
      SELECT ?parent WHERE {
        <#{@ex}Category:ML> <#{@ex}broader>+ ?parent
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      parents = Enum.map(results, &get_iri(&1["parent"])) |> MapSet.new()
      assert MapSet.member?(parents, "#{@ex}Category:AI")
      assert MapSet.member?(parents, "#{@ex}Category:CS")
      assert MapSet.member?(parents, "#{@ex}Category:Science")
    end

    test "find entities with FILTER and OPTIONAL", %{ctx: ctx} do
      # Pattern: Find people with optional birth year, filter by name
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Person"},
        {"#{@ex}alice", "#{@ex}name", "Alice"},
        {"#{@ex}alice", "#{@ex}birthYear", "1990"},
        {"#{@ex}bob", "#{@rdf}type", "#{@ex}Person"},
        {"#{@ex}bob", "#{@ex}name", "Bob"}
        # Note: Bob has no birthYear - testing OPTIONAL
      ])

      sparql = """
      SELECT ?person ?name ?year WHERE {
        ?person <#{@rdf}type> <#{@ex}Person> .
        ?person <#{@ex}name> ?name
        OPTIONAL { ?person <#{@ex}birthYear> ?year }
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert length(results) == 2

      # Alice should have a year, Bob should not
      alice_result = Enum.find(results, &(get_literal(&1["name"]) == "Alice"))
      bob_result = Enum.find(results, &(get_literal(&1["name"]) == "Bob"))

      assert alice_result["year"] != nil
      assert bob_result["year"] == nil
    end
  end

  # ===========================================================================
  # Enterprise Patterns
  # ===========================================================================

  describe "enterprise query patterns" do
    test "organizational hierarchy", %{ctx: ctx} do
      # Pattern: Find all reports (direct and indirect) for a manager
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@ex}reportsTo", "#{@ex}bob"},
        {"#{@ex}bob", "#{@ex}reportsTo", "#{@ex}carol"},
        {"#{@ex}dave", "#{@ex}reportsTo", "#{@ex}bob"},
        {"#{@ex}eve", "#{@ex}reportsTo", "#{@ex}carol"}
      ])

      # Find all people who report to Carol (directly or indirectly)
      sparql = """
      SELECT ?person WHERE {
        ?person <#{@ex}reportsTo>+ <#{@ex}carol>
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = Enum.map(results, &get_iri(&1["person"])) |> MapSet.new()
      assert MapSet.member?(people, "#{@ex}alice")
      assert MapSet.member?(people, "#{@ex}bob")
      assert MapSet.member?(people, "#{@ex}dave")
      assert MapSet.member?(people, "#{@ex}eve")
    end

    test "multi-hop relationship query", %{ctx: ctx} do
      # Pattern: Find products in same category as purchased items
      insert_triples(ctx, [
        {"#{@ex}user1", "#{@ex}purchased", "#{@ex}product1"},
        {"#{@ex}product1", "#{@ex}inCategory", "#{@ex}electronics"},
        {"#{@ex}product2", "#{@ex}inCategory", "#{@ex}electronics"},
        {"#{@ex}product3", "#{@ex}inCategory", "#{@ex}clothing"}
      ])

      sparql = """
      SELECT DISTINCT ?recommended WHERE {
        <#{@ex}user1> <#{@ex}purchased>/<#{@ex}inCategory>/^<#{@ex}inCategory> ?recommended
        FILTER(?recommended != <#{@ex}product1>)
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      recommended = Enum.map(results, &get_iri(&1["recommended"])) |> MapSet.new()
      assert MapSet.member?(recommended, "#{@ex}product2")
      refute MapSet.member?(recommended, "#{@ex}product3")
    end
  end

  # ===========================================================================
  # Plan Cache with Zipfian Distribution
  # ===========================================================================

  describe "plan cache with realistic workload" do
    test "achieves high hit rate with Zipfian query distribution" do
      # Create a standalone plan cache for testing
      cache_name = :"TestCache_#{:erlang.unique_integer([:positive])}"
      {:ok, cache_pid} = PlanCache.start_link(name: cache_name, max_size: 100)

      # Define a set of query keys (simulating real workload)
      # In production, these would be query hashes
      query_keys = ["q1", "q2", "q3", "q4", "q5"]

      # Zipfian distribution: query 0 is most common, query 4 is least common
      # Frequency proportional to 1/rank
      query_sequence = [
        0, 0, 0, 0, 0,  # Query 0: 5x
        1, 1, 1, 1,     # Query 1: 4x
        2, 2, 2,        # Query 2: 3x
        3, 3,           # Query 3: 2x
        4               # Query 4: 1x
      ] |> Enum.shuffle()

      # Simulate cache access pattern
      for idx <- query_sequence do
        key = Enum.at(query_keys, idx)
        case PlanCache.get(key, name: cache_name) do
          :miss ->
            # Cache miss - store a dummy plan
            PlanCache.put(key, {:dummy_plan, idx}, name: cache_name)
          {:ok, _plan} ->
            # Cache hit - nothing to do
            :ok
        end
      end

      # Check cache stats
      stats = PlanCache.stats(name: cache_name)
      total_lookups = stats.hits + stats.misses
      hit_rate = if total_lookups > 0, do: stats.hits / total_lookups * 100, else: 0

      # With Zipfian distribution, we expect high hit rate after warmup
      # First 5 unique queries will be misses, rest should be hits
      # Expected: 15 total, 5 misses, 10 hits = 66%+
      assert hit_rate >= 60, "Expected hit rate >= 60% with Zipfian distribution, got #{hit_rate}%"

      GenServer.stop(cache_pid)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp insert_triples(ctx, triples) do
    rdf_triples = Enum.map(triples, fn {s, p, o} ->
      s_term = if String.starts_with?(s, "http://"), do: RDF.iri(s), else: RDF.literal(s)
      p_term = RDF.iri(p)
      o_term = if String.starts_with?(to_string(o), "http://"), do: RDF.iri(o), else: RDF.literal(o)
      {s_term, p_term, o_term}
    end)
    {:ok, _} = Update.insert(ctx, rdf_triples)
  end

  defp get_literal({:literal, :simple, value}), do: value
  defp get_literal({:literal, :typed, value, _}), do: value
  defp get_literal({:literal, :lang, value, _}), do: value
  defp get_literal(%RDF.Literal{} = lit), do: to_string(RDF.Literal.value(lit))
  defp get_literal(other), do: other
end
