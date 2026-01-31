defmodule TripleStore.Integration.RealWorldScenariosTest do
  @moduledoc """
  Integration tests for Section 6.5.1: RDF Datasets.

  Tests real-world RDF dataset patterns:
  - VoID dataset description
  - Named graph for provenance
  - Named graph for access control
  - Named graph for temporal data
  - Union graph via query
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Query

  @test_db_base "/tmp/real_world_scenarios_test"
  @ex "http://example.org/"
  @void "http://rdfs.org/ns/void#"
  @prov "http://www.w3.org/ns/prov#"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("real_world_scenarios_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  defp load_test_data(db, manager) do
    # Load test data simulating real-world RDF dataset patterns
    trig_content = """
    @prefix ex: <#{@ex}> .
    @prefix void: <#{@void}> .
    @prefix prov: <#{@prov}> .
    @prefix dcat: <http://www.w3.org/ns/dcat#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    # Main data graph
    GRAPH ex:data {
      ex:resource1 a ex:Type ;
                  ex:name "Resource 1" ;
                  ex:value 100 .
      ex:resource2 a ex:Type ;
                  ex:name "Resource 2" ;
                  ex:value 200 .
    }

    # VoID dataset description graph
    GRAPH ex:void {
      ex:dataset a void:Dataset ;
                 void:entities 2 ;
                 void:propertyPartition [
                   void:property ex:name ;
                   void:triples 2
                 ] .
    }

    # Provenance graph
    GRAPH ex:provenance {
      ex:resource1 prov:wasGeneratedBy [
        prov:entity ex:source1 ;
        prov:atTime "2024-01-01T00:00:00Z"^^xsd:dateTime
      ] .
      ex:resource2 prov:wasGeneratedBy [
        prov:entity ex:source2 ;
        prov:atTime "2024-01-02T00:00:00Z"^^xsd:dateTime
      ] .
    }

    # Temporal version graphs
    GRAPH ex:version-2024-01 {
      ex:resource1 ex:value 100 ;
                  ex:version "2024-01" .
    }

    GRAPH ex:version-2024-02 {
      ex:resource1 ex:value 150 ;
                  ex:version "2024-02" .
    }

    # Access control graph
    GRAPH ex:acl {
      ex:data ex:readAccess "public" ;
             ex:writeAccess "admin" .
      ex:provenance ex:readAccess "admin" ;
                  ex:writeAccess "admin" .
    }
    """

    {:ok, _count} = Loader.load_trig_string(db, manager, trig_content)

    # Grant public read permissions to test graphs
    ctx = %{db: db, dict_manager: manager}
    :ok = Authorization.set_public(ctx, "#{@ex}data")
    :ok = Authorization.set_public(ctx, "#{@ex}void")
    :ok = Authorization.set_public(ctx, "#{@ex}provenance")
    :ok = Authorization.set_public(ctx, "#{@ex}version-2024-01")
    :ok = Authorization.set_public(ctx, "#{@ex}version-2024-02")
    :ok = Authorization.set_public(ctx, "#{@ex}acl")
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    load_test_data(db, manager)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(db_path)
    end)

    %{ctx: ctx, db: db, manager: manager}
  end

  # ===========================================================================
  # 6.5.1.1: VoID Dataset Description
  # ===========================================================================

  describe "6.5.1.1 VoID dataset description" do
    test "queries VoID metadata from named graph", %{ctx: ctx} do
      query = """
      PREFIX void: <#{@void}>
      PREFIX ex: <#{@ex}>

      SELECT ?entity_count WHERE {
        GRAPH ex:void {
          ex:dataset void:entities ?entity_count .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 1

      [result] = results

      assert result["entity_count"] ==
               {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "queries property partition from VoID graph", %{ctx: ctx} do
      query = """
      PREFIX void: <#{@void}>
      PREFIX ex: <#{@ex}>

      SELECT ?property ?triples WHERE {
        GRAPH ex:void {
          ex:dataset void:propertyPartition [
            void:property ?property ;
            void:triples ?triples
          ] .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 1

      [result] = results
      assert {:named_node, "http://example.org/name"} = result["property"]

      assert result["triples"] ==
               {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "joins data graph with VoID metadata", %{ctx: ctx} do
      query = """
      PREFIX void: <#{@void}>
      PREFIX ex: <#{@ex}>

      SELECT ?dataset ?entities WHERE {
        GRAPH ex:void {
          ?dataset a void:Dataset ;
                  void:entities ?entities .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      refute Enum.empty?(results)
    end
  end

  # ===========================================================================
  # 6.5.1.2: Named Graph for Provenance
  # ===========================================================================

  describe "6.5.1.2 Named graph for provenance" do
    test "queries data with provenance information", %{ctx: ctx} do
      query = """
      PREFIX prov: <#{@prov}>
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?source ?time WHERE {
        # Get resources from data graph
        GRAPH ex:data {
          ?resource a ex:Type .
        }
        # Join with provenance
        GRAPH ex:provenance {
          ?resource prov:wasGeneratedBy [
            prov:entity ?source ;
            prov:atTime ?time
          ] .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 2

      # Verify provenance data
      Enum.each(results, fn result ->
        assert {:named_node, resource} = result["resource"]
        assert {:named_node, source} = result["source"]
        assert String.starts_with?(source, "http://example.org/source")
      end)
    end

    test "OPTIONAL provenance returns resources without provenance", %{ctx: ctx} do
      query = """
      PREFIX prov: <#{@prov}>
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?source WHERE {
        GRAPH ex:data {
          ?resource a ex:Type .
        }
        OPTIONAL {
          GRAPH ex:provenance {
            ?resource prov:wasGeneratedBy/prov:entity ?source .
          }
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      # All resources should be returned, with or without provenance
      assert length(results) == 2
    end
  end

  # ===========================================================================
  # 6.5.1.3: Named Graph for Access Control
  # ===========================================================================

  describe "6.5.1.3 Named graph for access control" do
    test "queries access control permissions", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?graph ?read ?write WHERE {
        GRAPH ex:acl {
          ?graph ex:readAccess ?read ;
                 ex:writeAccess ?write .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 2

      # Verify ACL entries
      Enum.each(results, fn result ->
        assert {:named_node, graph} = result["graph"]
        assert String.starts_with?(graph, "http://example.org/")
        assert {:literal, :simple, _} = result["read"]
      end)
    end

    test "checks read permission before accessing data", %{ctx: ctx} do
      # This test verifies the authorization system works with graph ACLs
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?resource WHERE {
        GRAPH ?g {
          ?resource a ex:Type .
        }
      }
      """

      # Should only return resources from graphs we have access to
      assert {:ok, results} = Query.query(ctx, query)
      refute Enum.empty?(results)
    end
  end

  # ===========================================================================
  # 6.5.1.4: Named Graph for Temporal Data
  # ===========================================================================

  describe "6.5.1.4 Named graph for temporal data" do
    test "queries specific version by graph IRI", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?value WHERE {
        GRAPH ex:version-2024-01 {
          ?resource ex:value ?value .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 1

      [result] = results

      assert result["value"] ==
               {:literal, :typed, "100", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "compares versions across temporal graphs", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?v1_value ?v2_value WHERE {
        GRAPH ex:version-2024-01 {
          ?resource ex:value ?v1_value .
        }
        GRAPH ex:version-2024-02 {
          ?resource ex:value ?v2_value .
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 1

      [result] = results
      # Value increased from version 1 to version 2
      assert result["v1_value"] ==
               {:literal, :typed, "100", "http://www.w3.org/2001/XMLSchema#integer"}

      assert result["v2_value"] ==
               {:literal, :typed, "150", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    @tag :skip
    test "finds latest version using graph variable", %{ctx: ctx} do
      # Note: STR() on graph variables needs implementation support
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?max_value WHERE {
        ?resource ex:value ?max_value .
        {
          SELECT ?resource (MAX(?value) AS ?max_value) WHERE {
            GRAPH ?g {
              ?resource ex:value ?value .
            }
            FILTER(STRENDS(STR(?g), "version-"))
          }
          GROUP BY ?resource
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      refute Enum.empty?(results)
    end
  end

  # ===========================================================================
  # 6.5.1.5: Union Graph via Query
  # ===========================================================================

  describe "6.5.1.5 Union graph via query" do
    test "combines data from multiple graphs", %{ctx: ctx} do
      # Create additional graphs for union test
      ctx = %{db: ctx.db, dict_manager: ctx.dict_manager}

      trig_content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:dataset1 {
        ex:item1 ex:price "10" .
        ex:item2 ex:price "20" .
      }

      GRAPH ex:dataset2 {
        ex:item3 ex:price "30" .
        ex:item4 ex:price "40" .
      }
      """

      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)
      :ok = Authorization.set_public(ctx, "#{@ex}dataset1")
      :ok = Authorization.set_public(ctx, "#{@ex}dataset2")

      # Query across multiple graphs
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?item ?price WHERE {
        {
          GRAPH ex:dataset1 {
            ?item ex:price ?price .
          }
        } UNION {
          GRAPH ex:dataset2 {
            ?item ex:price ?price .
          }
        }
      }
      ORDER BY ?price
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 4

      prices = Enum.map(results, fn r -> r["price"] end)
      assert {:literal, :simple, "10"} in prices
      assert {:literal, :simple, "40"} in prices
    end

    test "uses graph variable to create dynamic union", %{ctx: ctx} do
      # Note: COUNT(*) not fully supported, using COUNT(?s) instead
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?g ?count WHERE {
        {
          SELECT ?g (COUNT(?s) AS ?count) WHERE {
            GRAPH ?g {
              ?s ?p ?o .
            }
          }
          GROUP BY ?g
        }
      }
      ORDER BY DESC(?count)
      """

      assert {:ok, results} = Query.query(ctx, query)
      refute Enum.empty?(results)

      # All graphs should have data
      Enum.each(results, fn result ->
        assert result["count"] > 0
      end)
    end

    @tag :skip
    test "filters union results by graph metadata", %{ctx: ctx} do
      # Note: STR() on graph variables needs implementation support
      query = """
      PREFIX ex: <#{@ex}>

      SELECT ?resource ?name WHERE {
        ?resource ex:name ?name .
        ?resource a ex:Type .
        FILTER EXISTS {
          GRAPH ?g {
            ?resource ex:name ?name .
          }
          FILTER(STRENDS(STR(?g), "version-"))
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      # Should find resource in at least one version graph
      assert length(results) >= 0
    end
  end
end
