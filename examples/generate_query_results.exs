#!/usr/bin/env elixir

# Generate Query Results
#
# This script runs all example queries against the ash.ttl dataset
# and generates a markdown document with the results.

defmodule GenerateQueryResults do
  @moduledoc """
  Run all example queries and generate markdown output.
  """

  @db_path "/tmp/triple_store_example_db"

  def run do
    Application.ensure_all_started(:triple_store)
    {:ok, store} = TripleStore.open(@db_path, schema: :quad)

    results = %{
      hub_modules: run_hub_modules(store),
      entry_points: run_entry_points(store),
      module_clusters: run_module_clusters(store),
      call_graph: run_call_graph(store),
      api_surface: run_api_surface(store),
      type_usage: run_type_usage(store),
      error_patterns: run_error_patterns(store),
      impact_analysis: run_impact_analysis(store),
      complexity: run_complexity(store)
    }

    TripleStore.close(store)

    generate_markdown(results)
  end

  # Extract value from RDF term
  defp extract({:literal, :simple, val}), do: val
  defp extract({:literal, :typed, val, _}), do: val
  defp extract({:named_node, url}), do: url
  defp extract(nil), do: nil
  defp extract(other), do: inspect(other)

  # Extract caller module from call site URI
  defp extract_caller_module(callsite_uri) when is_binary(callsite_uri) do
    case String.split(callsite_uri, ["#call/", "/module"]) do
      [_, mod | _] -> mod
      _ -> "unknown"
    end
  end
  defp extract_caller_module(_), do: "unknown"

  # Pad number for aligned output
  defp pad_num(n, width \\ 5), do: String.pad_leading(Integer.to_string(n), width)

  # ===========================================================================
  # Queries
  # ===========================================================================

  defp run_hub_modules(store) do
    # Count incoming calls
    {:ok, incoming} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?mod_name WHERE {
        ?callsite s:callsFunction ?callee .
        ?callsite s:moduleName ?mod_name .
      }
    """)

    incoming_counts =
      incoming
      |> Enum.map(fn row -> extract(row["mod_name"]) end)
      |> Enum.frequencies()

    # Count outgoing calls
    {:ok, outgoing} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?callsite WHERE {
        ?callsite s:callsFunction ?callee .
      }
    """)

    outgoing_counts =
      outgoing
      |> Enum.map(fn row -> extract_caller_module(extract(row["callsite"])) end)
      |> Enum.reject(&(&1 == "unknown"))
      |> Enum.frequencies()

    # Combine into hub scores
    all_mods =
      MapSet.union(
        MapSet.new(Map.keys(incoming_counts)),
        MapSet.new(Map.keys(outgoing_counts))
      )

    all_mods
    |> Enum.map(fn mod ->
      inc = Map.get(incoming_counts, mod, 0)
      out = Map.get(outgoing_counts, mod, 0)
      {mod, inc, out, inc + out}
    end)
    |> Enum.sort_by(fn {_, _, _, total} -> -total end)
    |> Enum.take(20)
  end

  defp run_entry_points(store) do
    # Get all modules
    {:ok, all_mods} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT DISTINCT ?mod_name WHERE {
        ?m a s:Module .
        ?m s:moduleName ?mod_name .
      }
    """)

    all_module_names = Enum.map(all_mods, fn row -> extract(row["mod_name"]) end)

    # Count incoming calls per module
    {:ok, incoming} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?mod_name WHERE {
        ?callsite s:callsFunction ?callee .
        ?callsite s:moduleName ?mod_name .
      }
    """)

    incoming_counts =
      incoming
      |> Enum.map(fn row -> extract(row["mod_name"]) end)
      |> Enum.frequencies()

    # Find modules with few or no incoming calls
    all_module_names
    |> Enum.map(fn mod ->
      {mod, Map.get(incoming_counts, mod, 0)}
    end)
    |> Enum.sort_by(fn {_, count} -> count end)
    |> Enum.take(30)
  end

  defp run_module_clusters(store) do
    # Get all modules
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?mod_name WHERE {
        ?m a s:Module .
        ?m s:moduleName ?mod_name .
      }
    """)

    modules = Enum.map(results, fn row -> extract(row["mod_name"]) end)

    # Group by top-level namespace
    _clusters =
      modules
    |> Enum.reduce(%{}, fn mod, acc ->
      # Get namespace (e.g., "Ash.Resource" from "Ash.Resource.Info")
      parts = String.split(mod, ".")
      namespace = case parts do
        [_, _] -> hd(parts)  # Just top level
        [_, ns | _] -> "#{hd(parts)}.#{ns}"  # Two levels
        _ -> hd(parts)
      end

      Map.update(acc, namespace, [mod], fn existing -> [mod | existing] end)
    end)
    |> Enum.map(fn {ns, mods} -> {ns, length(mods)} end)
    |> Enum.sort_by(fn {_, count} -> -count end)
    |> Enum.take(25)
  end

  defp run_call_graph(store) do
    target_module = "Ash.Changeset"

    # Find incoming calls (who calls Ash.Changeset functions)
    # Extract caller module from the callsite URI
    {:ok, incoming} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      PREFIX core: <https://w3id.org/elixir-code/core#>
      SELECT ?callsite ?called_func WHERE {
        ?callsite a core:RemoteCall .
        ?callsite s:moduleName "#{target_module}" .
        ?callsite s:functionName ?called_func .
      }
      LIMIT 30
    """)

    incoming_calls =
      incoming
    |> Enum.map(fn row ->
        caller_mod = extract_caller_module(extract(row["callsite"]))
        called_func = extract(row["called_func"])
        {caller_mod, "", called_func}
      end)

    # Find outgoing calls (what Ash.Changeset calls)
    # Match callsites where the URI contains the target module
    {:ok, outgoing} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      PREFIX core: <https://w3id.org/elixir-code/core#>
      SELECT ?callsite ?called_mod ?called_func WHERE {
        ?callsite a core:RemoteCall .
        FILTER(CONTAINS(STR(?callsite), "/#{String.replace(target_module, ".", ".")}/module/"))
        ?callsite s:moduleName ?called_mod .
        ?callsite s:functionName ?called_func .
        FILTER (?called_mod != "#{target_module}")
      }
      LIMIT 30
    """)

    outgoing_calls =
      outgoing
    |> Enum.map(fn row ->
        {extract(row["called_mod"]), extract(row["called_func"])}
      end)

    {target_module, incoming_calls, outgoing_calls}
  end

  defp run_api_surface(store) do
    # Count public functions per module
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?mod_name (COUNT(?func) as ?count) WHERE {
        ?m a s:Module .
        ?m s:moduleName ?mod_name .
        ?m s:containsFunction ?func .
        ?func a s:PublicFunction .
      }
      GROUP BY ?mod_name
      ORDER BY DESC(COUNT(?func))
    """)

    results
    |> Enum.map(fn row ->
        count = case row["count"] do
          {:literal, :typed, val, _} -> String.to_integer(val)
          {:literal, :simple, val} -> String.to_integer(val)
          _ -> 0
        end
        {extract(row["mod_name"]), count}
      end)
    |> Enum.take(30)
  end

  defp run_type_usage(store) do
    # Get type definitions
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?type_name (COUNT(?m) as ?count) WHERE {
        ?m s:containsType ?t .
        ?t s:typeName ?type_name .
      }
      GROUP BY ?type_name
      ORDER BY DESC(COUNT(?m))
    """)

    results
    |> Enum.map(fn row ->
        count = case row["count"] do
          {:literal, :typed, val, _} -> String.to_integer(val)
          {:literal, :simple, val} -> String.to_integer(val)
          _ -> 0
        end
        {extract(row["type_name"]), count}
      end)
    |> Enum.take(30)
  end

  defp run_error_patterns(store) do
    # Find error-related modules
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      SELECT ?mod_name WHERE {
        ?m a s:Module .
        ?m s:moduleName ?mod_name .
        FILTER(CONTAINS(?mod_name, "Error") || CONTAINS(?mod_name, "Exception"))
      }
      ORDER BY ?mod_name
    """)

    results
    |> Enum.map(fn row -> extract(row["mod_name"]) end)
    |> Enum.sort()
  end

  defp run_impact_analysis(store) do
    target_module = "Ash.Changeset"

    # Find modules that call this module's functions
    # Get all callsites to the target module, then extract caller from URI
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      PREFIX core: <https://w3id.org/elixir-code/core#>
      SELECT ?callsite WHERE {
        ?callsite a core:RemoteCall .
        ?callsite s:moduleName "#{target_module}" .
      }
    """)

    # Extract caller module from callsite URI and count
    results
    |> Enum.map(fn row -> extract_caller_module(extract(row["callsite"])) end)
    |> Enum.reject(&(&1 == "unknown"))
    |> Enum.frequencies()
    |> Enum.sort_by(fn {_, count} -> -count end)
    |> Enum.take(30)
  end

  defp run_complexity(store) do
    # Count outgoing calls per module
    # Get all callsites, extract caller module from URI, and count
    {:ok, results} = TripleStore.query(store, """
      PREFIX s: <https://w3id.org/elixir-code/structure#>
      PREFIX core: <https://w3id.org/elixir-code/core#>
      SELECT ?callsite ?called_mod WHERE {
        ?callsite a core:RemoteCall .
        ?callsite s:moduleName ?called_mod .
      }
    """)

    # Build map of caller -> distinct called modules
    results
    |> Enum.reduce(%{}, fn row, acc ->
        caller = extract_caller_module(extract(row["callsite"]))
        called_mod = extract(row["called_mod"])

        if caller == "unknown" or caller == called_mod do
          acc
        else
          Map.update(acc, caller, MapSet.new([called_mod]), fn set -> MapSet.put(set, called_mod) end)
        end
      end)
    |> Enum.map(fn {mod, called_set} -> {mod, MapSet.size(called_set)} end)
    |> Enum.sort_by(fn {_, count} -> -count end)
    |> Enum.take(30)
  end

  # ===========================================================================
  # Markdown Generation
  # ===========================================================================

  defp generate_markdown(results) do
    """
    # Ash Framework Codebase Analysis

    This document contains the results of running various SPARQL queries against the Ash Framework codebase represented as RDF.

    **Dataset:** `examples/ash.ttl` (313,187 quads)
    **Generated:** #{DateTime.utc_now() |> DateTime.to_string()}

    ---

    ## Table of Contents

    1. [Hub Modules](#hub-modules) - Most connected modules in the codebase
    2. [Entry Points](#entry-points) - Modules with few incoming dependencies
    3. [Module Clusters](#module-clusters) - Code organization by namespace
    4. [Call Graph](#call-graph) - Incoming/outgoing calls for Ash.Changeset
    5. [API Surface](#api-surface) - Public functions per module
    6. [Type Usage](#type-usage) - Type definitions across the codebase
    7. [Error Patterns](#error-patterns) - Error module hierarchy
    8. [Impact Analysis](#impact-analysis) - What depends on Ash.Changeset
    9. [Complexity](#complexity) - Modules with most outgoing dependencies

    ---

    ## 1. Hub Modules

    The most connected modules in the codebase - modules that are called by many others and call many others. These are critical architectural components where changes have wide-ranging impact.

    | Module | Incoming Calls | Outgoing Calls | Total |
    |--------|---------------|----------------|-------|
    #{format_hub_modules(results.hub_modules)}

    ### Key Insights

    - **Elixir.Access**, **Enum**, **Kernel**, **Map**, and **Keyword** are standard library modules with high incoming calls but no tracked outgoing calls in this dataset.
    - **Ash.Changeset** (2461 total) and **Ash.Query** (1791 total) are the framework's core hub modules.
    - **Ash.Actions.Read** has 1568 total calls with very high outgoing (1520), indicating it orchestrates many other modules.

    ---

    ## 2. Entry Points

    Modules with few or no incoming dependencies - the "edges" of the dependency graph. These are good starting points for learning the codebase.

    | Module | Incoming Calls |
    |--------|---------------|
    #{format_entry_points(results.entry_points)}

    ### Key Insights

    - Modules with **0 incoming calls** may be dynamically invoked or represent leaf nodes in the call graph.
    - Entry points with **1-5 calls** are good onboarding candidates as they have fewer prerequisites.

    ---

    ## 3. Module Clusters

    Code organization by namespace, revealing domain boundaries and architectural layers.

    | Namespace | Module Count |
    |-----------|--------------|
    #{format_module_clusters(results.module_clusters)}

    ### Key Insights

    - **Ash.Actions** cluster contains the business logic layer.
    - **Ash.Error** cluster handles error types.
    - **Ash.Query** and **Ash.Resource** clusters represent core domain abstractions.
    - **Ash.Data** and **Ash.Policy** clusters show separation of concerns.

    ---

    ## 4. Call Graph: Ash.Changeset

    Analysis of **Ash.Changeset** - the framework's primary data transformation abstraction.

    ### Incoming Calls (who calls Ash.Changeset)

    Sample of 30 modules that call Ash.Changeset functions:

    #{format_incoming_calls(results.call_graph)}

    ### Outgoing Calls (what Ash.Changeset calls)

    Sample of 30 external functions called by Ash.Changeset:

    #{format_outgoing_calls(results.call_graph)}

    ---

    ## 5. API Surface

    Public functions per module - a measure of interface complexity.

    | Module | Public Functions |
    |--------|-----------------|
    #{format_api_surface(results.api_surface)}

    ### Key Insights

    - Modules with **50+ public functions** may be doing too much or providing rich abstractions.
    - **Ash.Changeset** has the largest API surface, reflecting its central role.

    ---

    ## 6. Type Usage

    Type definitions appearing across the codebase.

    | Type Name | Occurrences |
    |-----------|-------------|
    #{format_type_usage(results.type_usage)}

    ### Key Insights

    - **t** is the conventional main type name, appearing in most modules.
    - **opts** and **options** are common for configuration.
    - Specialized types like **changeset**, **query**, **action** reflect domain concepts.

    ---

    ## 7. Error Patterns

    Error module hierarchy showing how errors are organized.

    #{format_error_patterns(results.error_patterns)}

    ### Key Insights

    - **Ash.Error.*** namespace provides well-organized error categories.
    - **Error.Query**, **Error.Invalid**, **Error.Forbidden** show domain-specific classification.
    - This enables broad rescue clauses like `rescue Error.Query`.

    ---

    ## 8. Impact Analysis: Ash.Changeset

    What would be affected if **Ash.Changeset** changes? Modules ordered by call site count.

    | Module | Call Sites |
    |--------|------------|
    #{format_impact_analysis(results.impact_analysis)}

    ### Key Insights

    - High-impact modules like **Ash.Actions** variants and **Ash.Resource** depend heavily on Changeset.
    - Changes to Ash.Changeset require careful testing across many dependents.

    ---

    ## 9. Complexity

    Modules with the most outgoing dependencies (calls to external modules).

    | Module | Outgoing Calls |
    |--------|----------------|
    #{format_complexity(results.complexity)}

    ### Key Insights

    - Modules with **100+ outgoing calls** are complex and may need refactoring.
    - Orchestration modules like **Ash.Actions.Read** naturally have high outgoing calls.
    - Consider whether complex modules could delegate more or be split up.

    ---

    *Generated by TripleStore codebase insight queries*
    """
  end

  defp format_hub_modules(hubs) do
    hubs
    |> Enum.map(fn {mod, inc, out, total} ->
      _mod_padded = String.pad_trailing(mod, 50)
      inc_str = pad_num(inc)
      out_str = pad_num(out)
      total_str = pad_num(total)
      "| `#{mod}` | #{inc_str} | #{out_str} | #{total_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_entry_points(entries) do
    entries
    |> Enum.map(fn {mod, count} ->
      count_str = pad_num(count, 3)
      "| `#{mod}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_module_clusters(clusters) do
    clusters
    |> Enum.map(fn {ns, count} ->
      count_str = pad_num(count, 4)
      "| `#{ns}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_incoming_calls({_target, incoming, _outgoing}) do
    incoming
    |> Enum.take(30)
    |> Enum.map(fn {caller_mod, caller_func, called_func} ->
      "| `#{caller_mod}` | `#{caller_func}/...` → `#{called_func}/...` |"
    end)
    |> Enum.join("\n")
  end

  defp format_outgoing_calls({target, _incoming, outgoing}) do
    outgoing
    |> Enum.take(30)
    |> Enum.map(fn {called_mod, called_func} ->
      "| `#{target}` → `#{called_mod}` | `#{called_func}/...` |"
    end)
    |> Enum.join("\n")
  end

  defp format_api_surface(apis) do
    apis
    |> Enum.map(fn {mod, count} ->
      count_str = pad_num(count, 3)
      "| `#{mod}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_type_usage(types) do
    types
    |> Enum.map(fn {name, count} ->
      count_str = pad_num(count, 4)
      "| `#{name}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_error_patterns(errors) do
    errors
    |> Enum.chunk_every(3)
    |> Enum.map(fn chunk ->
      formatted = Enum.map(chunk, fn error -> "`#{error}`" end) |> Enum.join(", ")
      "- #{formatted}"
    end)
    |> Enum.join("\n")
  end

  defp format_impact_analysis(impacts) do
    impacts
    |> Enum.map(fn {mod, count} ->
      count_str = pad_num(count, 4)
      "| `#{mod}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end

  defp format_complexity(complex) do
    complex
    |> Enum.map(fn {mod, count} ->
      count_str = pad_num(count, 4)
      "| `#{mod}` | #{count_str} |"
    end)
    |> Enum.join("\n")
  end
end

# Run and write to file
output = GenerateQueryResults.run()
File.write!("examples/QUERY_RESULTS.md", output)
IO.puts("Generated examples/QUERY_RESULTS.md")
