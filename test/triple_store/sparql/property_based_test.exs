defmodule TripleStore.SPARQL.PropertyBasedTest do
  @moduledoc """
  Property-based tests for SPARQL query engine using StreamData.

  These tests verify invariants that should hold for any valid input,
  such as:
  - Path traversal correctness (transitivity, reflexivity)
  - Query result consistency
  - Optimizer plan equivalence
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.Query
  alias TripleStore.Update

  @moduletag :property_based

  @ex "http://example.org/"

  # ===========================================================================
  # Test Setup
  # ===========================================================================

  setup do
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "property_based_#{test_id}")

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
  # Generators
  # ===========================================================================

  # Generate a valid node name
  defp node_name do
    StreamData.string(:alphanumeric, min_length: 1, max_length: 10)
    |> StreamData.map(&"node_#{&1}")
  end

  # Generate a list of chain edges (node1 -> node2 -> node3 -> ...)
  defp chain_graph(min_length, max_length) do
    StreamData.list_of(node_name(), min_length: min_length, max_length: max_length)
    |> StreamData.map(fn nodes ->
      nodes = Enum.uniq(nodes)

      for {from, to} <- Enum.zip(nodes, Enum.drop(nodes, 1)) do
        {"#{@ex}#{from}", "#{@ex}next", "#{@ex}#{to}"}
      end
    end)
    |> StreamData.filter(&(&1 != []))
  end

  # ===========================================================================
  # Property: Transitive Closure Completeness
  # ===========================================================================

  property "transitive closure contains all reachable nodes", %{ctx: ctx} do
    check all(chain <- chain_graph(3, 15), max_runs: 25) do
      # Insert chain triples
      insert_triples(ctx, chain)

      # Get first and last nodes from chain
      [{first_s, _, _} | _] = chain
      {_, _, last_o} = List.last(chain)

      # Query transitive closure
      sparql = """
      SELECT ?node WHERE {
        <#{first_s}> <#{@ex}next>+ ?node
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      found_nodes = Enum.map(results, &get_iri(&1["node"])) |> MapSet.new()

      # All nodes after first should be reachable
      expected_nodes =
        chain
        |> Enum.map(fn {_, _, o} -> o end)
        |> MapSet.new()

      assert MapSet.subset?(expected_nodes, found_nodes),
             "Expected all chain nodes to be reachable via transitive closure"

      # Last node should be reachable
      assert MapSet.member?(found_nodes, last_o),
             "Last node should be reachable"

      # Clean up for next iteration
      delete_all_triples(ctx)
    end
  end

  # ===========================================================================
  # Property: Zero-or-more Includes Start Node
  # ===========================================================================

  property "zero-or-more path includes start node", %{ctx: ctx} do
    check all(chain <- chain_graph(2, 10), max_runs: 25) do
      insert_triples(ctx, chain)

      [{first_s, _, _} | _] = chain

      # Query with zero-or-more (should include start)
      sparql = """
      SELECT ?node WHERE {
        <#{first_s}> <#{@ex}next>* ?node
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      found_nodes = Enum.map(results, &get_iri(&1["node"])) |> MapSet.new()

      # Start node should be included (zero steps)
      assert MapSet.member?(found_nodes, first_s),
             "Zero-or-more should include the start node itself"

      delete_all_triples(ctx)
    end
  end

  # ===========================================================================
  # Property: One-or-more Excludes Start Node
  # ===========================================================================

  property "one-or-more path excludes start node (unless cycle)", %{ctx: ctx} do
    check all(chain <- chain_graph(2, 10), max_runs: 25) do
      insert_triples(ctx, chain)

      [{first_s, _, _} | _] = chain

      # Query with one-or-more
      sparql = """
      SELECT ?node WHERE {
        <#{first_s}> <#{@ex}next>+ ?node
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      found_nodes = Enum.map(results, &get_iri(&1["node"])) |> MapSet.new()

      # For a linear chain (no cycles), start should NOT be included
      # (we only generate linear chains without cycles in chain_graph)
      refute MapSet.member?(found_nodes, first_s),
             "One-or-more on linear chain should not include start node"

      delete_all_triples(ctx)
    end
  end

  # ===========================================================================
  # Property: Result Count Consistency
  # ===========================================================================

  property "COUNT matches actual result length", %{ctx: ctx} do
    check all(chain <- chain_graph(3, 15), max_runs: 25) do
      insert_triples(ctx, chain)

      [{first_s, _, _} | _] = chain

      # Query all reachable nodes
      select_sparql = """
      SELECT ?node WHERE {
        <#{first_s}> <#{@ex}next>* ?node
      }
      """

      count_sparql = """
      SELECT (COUNT(?node) AS ?count) WHERE {
        <#{first_s}> <#{@ex}next>* ?node
      }
      """

      {:ok, select_results} = Query.query(ctx, select_sparql)
      {:ok, count_results} = Query.query(ctx, count_sparql)

      count = extract_count(hd(count_results)["count"])

      assert count == length(select_results),
             "COUNT should match actual result length"

      delete_all_triples(ctx)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp insert_triples(ctx, triples) do
    rdf_triples =
      Enum.map(triples, fn {s, p, o} ->
        {RDF.iri(s), RDF.iri(p), RDF.iri(o)}
      end)

    {:ok, _} = Update.insert(ctx, rdf_triples)
  end

  defp delete_all_triples(ctx) do
    # Delete all triples by querying and deleting each
    {:ok, results} = Query.query(ctx, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")

    if results != [] do
      rdf_triples =
        Enum.map(results, fn r ->
          {to_rdf_term(r["s"]), to_rdf_term(r["p"]), to_rdf_term(r["o"])}
        end)

      {:ok, _} = Update.delete(ctx, rdf_triples)
    end
  end

  defp to_rdf_term({:named_node, iri}), do: RDF.iri(iri)
  defp to_rdf_term({:literal, :simple, val}), do: RDF.literal(val)
  defp to_rdf_term({:literal, :typed, val, dt}), do: RDF.literal(val, datatype: dt)
  defp to_rdf_term(other), do: other

  defp get_iri({:named_node, iri}), do: iri
  defp get_iri(%RDF.IRI{value: iri}), do: iri
  defp get_iri(other), do: other

  defp extract_count(result) do
    case result do
      %RDF.Literal{} = lit -> RDF.Literal.value(lit) |> to_string() |> String.to_integer()
      {:literal, :typed, value, _} -> String.to_integer(value)
      {:literal, :simple, value} -> String.to_integer(value)
      value when is_integer(value) -> value
      value when is_binary(value) -> String.to_integer(value)
    end
  end
end
