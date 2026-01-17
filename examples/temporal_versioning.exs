#!/usr/bin/env elixir

# Temporal Versioning Example
#
# This example demonstrates how to use named graphs for temporal
# versioning of RDF data, allowing you to track changes over time
# and query historical states.

defmodule TemporalVersioning do
  @moduledoc """
  Example: Temporal versioning using named graphs.

  Each time period (e.g., month) gets its own named graph,
  allowing you to query historical data and track changes over time.
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.{Query, Update, Authorization}
  alias TripleStore.Loader

  @db_path "/tmp/temporal_versioning_example"
  @base_iri "http://example.org/data/version"

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
  # Version Management
  # ===========================================================================

  @doc """
  Creates a new version graph for the specified date.
  """
  def create_version(ctx, date) do
    graph_iri = version_graph(date)
    query = "CREATE GRAPH <#{graph_iri}>"
    Update.execute(ctx, query)
  end

  @doc """
  Inserts data into a specific version's graph.
  """
  def insert_version_data(ctx, date, data) do
    graph_iri = version_graph(date)

    trig_data = """
      @prefix ex: <http://example.org/>

      GRAPH <#{graph_iri}> {
        #{data}
      }
    """

    Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_data)
  end

  @doc """
  Queries data as of a specific date.
  """
  def query_as_of(ctx, date, query_template) do
    graph_iri = version_graph(date)

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
  Compares data between two versions, returning changes.
  """
  def compare_versions(ctx, date1, date2) do
    query = """
      PREFIX ex: <http://example.org/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      SELECT ?resource ?old_value ?new_value WHERE {
        GRAPH <#{version_graph(date1)}> {
          ?resource ex:value ?old_value .
        }
        GRAPH <#{version_graph(date2)}> {
          ?resource ex:value ?new_value .
        }
        FILTER(?old_value != ?new_value)
      }
    """

    Query.query(ctx, query)
  end

  @doc """
  Queries the latest version across all time periods.
  """
  def query_latest(ctx, resource) do
    query = """
      PREFIX ex: <http://example.org/>

      SELECT ?max_value ?version WHERE {
        {
          SELECT ?resource (MAX(?value) AS ?max_value) WHERE {
            GRAPH ?g {
              ?resource ex:value ?value .
            }
            FILTER(STRENDS(STR(?g), "/version/"))
          }
          GROUP BY ?resource
        }
        GRAPH ?version {
          ?resource ex:value ?max_value .
        }
      }
    """

    Query.query(ctx, query)
  end

  @doc """
  Gets version history for a specific resource.
  """
  def get_history(ctx, resource_iri) do
    query = """
      PREFIX ex: <http://example.org/>

      SELECT ?g ?value WHERE {
        GRAPH ?g {
          <#{resource_iri}> ex:value ?value .
        }
        FILTER(STRENDS(STR(?g), "/version/"))
      }
      ORDER BY ASC(?g)
    """

    Query.query(ctx, query)
  end

  # ===========================================================================
  # Example Usage
  # ===========================================================================

  def run_example do
    ctx = setup()

    try do
      IO.puts("=== Temporal Versioning Example ===\n")

      # Create version graphs for three months
      dates = ["2024-01", "2024-02", "2024-03"]

      IO.puts("1. Creating version graphs...")
      Enum.each(dates, fn date ->
        create_version(ctx, date)
        Authorization.set_public(ctx, version_graph(date))
      end)
      IO.puts("   ✓ Created 3 version graphs\n")

      # Insert data for each version
      IO.puts("2. Inserting versioned data...")

      # January: Initial state
      insert_version_data(ctx, "2024-01", """
        ex:product-123 ex:name "Widget Pro" .
        ex:product-123 ex:price "99.99"^^xsd:decimal .
        ex:product-123 ex:stock "100"^^xsd:integer .
        ex:product-123 ex:status "available" .
      """)

      # February: Price increase
      insert_version_data(ctx, "2024-02", """
        ex:product-123 ex:name "Widget Pro" .
        ex:product-123 ex:price "119.99"^^xsd:decimal .
        ex:product-123 ex:stock "85"^^xsd:integer .
        ex:product-123 ex:status "available" .
      """)

      # March: Price decrease, low stock
      insert_version_data(ctx, "2024-03", """
        ex:product-123 ex:name "Widget Pro" .
        ex:product-123 ex:price "109.99"^^xsd:decimal .
        ex:product-123 ex:stock "15"^^xsd:integer .
        ex:product-123 ex:status "low-stock" .
      """)
      IO.puts("   ✓ Inserted data for all versions\n")

      # Query specific version
      IO.puts("3. Querying January 2024 data...")
      {:ok, jan_results} = query_as_of(ctx, "2024-01", """
        SELECT ?s ?p ?o WHERE {
      """)

      print_version_results("January 2024", jan_results)

      IO.puts("\n4. Comparing January vs February...")
      {:ok, changes} = compare_versions(ctx, "2024-01", "2024-02")

      print_comparison_results(changes)

      IO.puts("\n5. Comparing February vs March...")
      {:ok, changes2} = compare_versions(ctx, "2024-02", "2024-03")

      print_comparison_results(changes2)

      IO.puts("\n6. Getting full history...")
      {:ok, history} = get_history(ctx, "http://example.org/product-123")

      print_history_results(history)

      IO.puts("\n7. Finding latest price...")
      {:ok, latest} = query_latest(ctx, "http://example.org/product-123")

      print_latest_results(latest)

      IO.puts("\n=== Example Complete ===")
    after
      teardown(ctx)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp version_graph(date) do
    "#{@base_iri}/#{date}"
  end

  defp print_version_results(title, results) do
    IO.puts("   #{title}:")
    Enum.each(results, fn result ->
      p = format_iri(result["p"])
      o = format_term(result["o"])
      IO.puts("     #{p} = #{o}")
    end)
  end

  defp print_comparison_results(results) do
    IO.puts("   Changes:")
    Enum.each(results, fn result ->
      old = format_term(result["old_value"])
      new_val = format_term(result["new_value"])
      IO.puts("     Price: #{old} → #{new_val}")
    end)
  end

  defp print_history_results(results) do
    IO.puts("   History:")
    Enum.each(results, fn result ->
      g = result["g"]
      version = g |> String.split("/") |> List.last()
      value = format_term(result["value"])
      IO.puts("     #{version}: #{value}")
    end)
  end

  defp print_latest_results([result | _]) do
    value = format_term(result["max_value"])
    version = result["version"] |> String.split("/") |> List.last()
    IO.puts("   Latest price: #{value} (from #{version})")
  end

  defp print_latest_results([]) do
    IO.puts("   No results found")
  end

  defp format_iri({:named_node, iri}), do: format_iri_string(iri)
  defp format_iri(other), do: inspect(other)

  defp format_iri_string(iri) do
    case String.split(iri, "/") |> List.last() do
      "" -> iri
      last -> last
    end
  end

  defp format_term({:literal, :simple, value}), do: "\"#{value}\""
  defp format_term({:literal, :typed, value, _type}), do: value
  defp format_term({:literal, :lang, value, _lang}), do: "\"#{value}\""
  defp format_term({:named_node, iri}), do: format_iri_string(iri)
  defp format_term(other), do: inspect(other)
end

# Run the example if executed directly
if System.argv() |> List.first() == __ENV__.file |> Path.basename() do
  TemporalVersioning.run_example()
end
