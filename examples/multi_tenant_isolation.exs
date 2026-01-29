#!/usr/bin/env elixir

# Multi-Tenant Data Isolation Example
#
# This example demonstrates how to use named graphs for multi-tenant
# data isolation in the quad store. Each tenant gets their own
# isolated graph while sharing the same database.

defmodule MultiTenantIsolation do
  @moduledoc """
  Example: Multi-tenant data isolation using named graphs.

  Each tenant gets their own named graph, ensuring complete data
  isolation while allowing for efficient cross-tenant queries
  when needed (e.g., for admin analytics).
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.{Query, Update, Authorization}
  alias TripleStore.Loader

  @db_path "/tmp/multi_tenant_example"
  @base_iri "http://example.org/tenant"

  # ===========================================================================
  # Setup and Teardown
  # ===========================================================================

  def setup do
    File.rm_rf(@db_path)
    {:ok, db} = NIF.open(@db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    %{db: db, dict_manager: manager}
  end

  def teardown(%{db: db, dict_manager: manager}) do
    Manager.stop(manager)
    NIF.close(db)
    File.rm_rf(@db_path)
  end

  # ===========================================================================
  # Tenant Management
  # ===========================================================================

  @doc """
  Creates a new tenant with an isolated named graph.
  """
  def create_tenant(ctx, tenant_id) do
    graph_iri = tenant_graph(tenant_id)
    query = "CREATE GRAPH <#{graph_iri}>"

    case Update.execute(ctx, query) do
      {:ok, _} -> {:ok, tenant_id}
      error -> error
    end
  end

  @doc """
  Grants public read access to a tenant's graph.
  """
  def grant_public_access(ctx, tenant_id) do
    graph_iri = tenant_graph(tenant_id)
    Authorization.set_public(ctx, graph_iri)
  end

  @doc """
  Deletes a tenant and all their data.
  """
  def delete_tenant(ctx, tenant_id) do
    graph_iri = tenant_graph(tenant_id)
    query = "DROP GRAPH <#{graph_iri}>"
    Update.execute(ctx, query)
  end

  # ===========================================================================
  # Data Operations
  # ===========================================================================

  @doc """
  Inserts data into a tenant's isolated graph.
  """
  def insert_tenant_data(ctx, tenant_id, data) do
    graph_iri = tenant_graph(tenant_id)

    trig_data = """
      @prefix ex: <http://example.org/>.

      GRAPH <#{graph_iri}> {
        #{data}
      }
    """

    Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_data)
  end

  @doc """
  Queries data from a specific tenant's graph only.
  """
  def query_tenant(ctx, tenant_id, query_template) do
    graph_iri = tenant_graph(tenant_id)

    query = """
      PREFIX ex: <http://example.org/>

      #{query_template}
      GRAPH <#{graph_iri}> {
        ?s ?p ?o .
      }
    """

    Query.query(ctx, query)
  end

  @doc """
  Admin query across all tenant graphs for analytics.
  """
  def query_all_tenants(ctx, query_template) do
    query = """
      PREFIX ex: <http://example.org/>

      #{query_template}
      GRAPH ?g {
        ?s ?p ?o .
      }
      FILTER(STRENDS(STR(?g), "/tenant/"))
    """

    Query.query(ctx, query)
  end

  # ===========================================================================
  # Example Usage
  # ===========================================================================

  def run_example do
    ctx = setup()

    try do
      IO.puts("=== Multi-Tenant Isolation Example ===\n")

      # Create tenants
      IO.puts("1. Creating tenants...")
      {:ok, _} = create_tenant(ctx, "acme-corp")
      {:ok, _} = create_tenant(ctx, "globex-inc")
      grant_public_access(ctx, "acme-corp")
      grant_public_access(ctx, "globex-inc")
      IO.puts("   ✓ Created 2 tenants\n")

      # Insert data for each tenant
      IO.puts("2. Inserting tenant data...")
      insert_tenant_data(ctx, "acme-corp", """
        ex:company-name "Acme Corporation" .
        ex:industry "Manufacturing" .
        ex:employee-count 1500 .
        ex:founded "1950" .
      """)

      insert_tenant_data(ctx, "globex-inc", """
        ex:company-name "Globex Inc" .
        ex:industry "Technology" .
        ex:employee-count 500 .
        ex:founded "2010" .
      """)
      IO.puts("   ✓ Inserted data for both tenants\n")

      # Query individual tenant
      IO.puts("3. Querying Acme Corp data...")
      {:ok, acme_results} = query_tenant(ctx, "acme-corp", """
        SELECT ?s ?p ?o WHERE {
      """)

      print_results("Acme Corp", acme_results)

      IO.puts("\n4. Querying Globex Inc data...")
      {:ok, globex_results} = query_tenant(ctx, "globex-inc", """
        SELECT ?s ?p ?o WHERE {
      """)

      print_results("Globex Inc", globex_results)

      # Admin analytics query
      IO.puts("\n5. Admin analytics: Count employees across all tenants...")
      {:ok, all_results} = query_all_tenants(ctx, """
        SELECT ?g (SUM(?count) AS ?total) WHERE {
          GRAPH ?g {
            ?s ex:employee-count ?count .
          }
        }
        GROUP BY ?g
      """)

      print_results("All Tenants", all_results)

      IO.puts("\n6. Data isolation verification...")
      verify_isolation(ctx)

      IO.puts("\n=== Example Complete ===")
    after
      teardown(ctx)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp tenant_graph(tenant_id) do
    "#{@base_iri}/#{tenant_id}"
  end

  defp print_results(title, results) do
    IO.puts("   #{title}:")
    Enum.each(results, fn result ->
      IO.puts("     #{inspect(result)}")
    end)
  end

  defp verify_isolation(ctx) do
    # Verify that querying without GRAPH only returns default graph (empty)
    query = """
      PREFIX ex: <http://example.org/>

      SELECT ?s ?p ?o WHERE {
        ?s ?p ?o .
      }
    """

    case Query.query(ctx, query) do
      {:ok, []} ->
        IO.puts("   ✓ Data isolation verified: no data in default graph")

      {:ok, results} ->
        IO.puts("   ✗ Warning: Found #{length(results)} quads in default graph")

      _error ->
        IO.puts("   ✗ Query error during isolation verification")
    end
  end
end

# Run the example if executed directly
if System.argv() |> List.first() == __ENV__.file |> Path.basename() do
  MultiTenantIsolation.run_example()
end
