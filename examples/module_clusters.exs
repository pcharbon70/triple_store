# Module Clusters Query
#
# Run with: mix run examples/module_clusters.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# Module clusters show how code is organized by namespace. Elixir module
# names like `Ash.Resource.Info` create natural groupings that reveal
# the domain architecture.
#
# WHY THIS MATTERS:
#
# 1. DOMAIN BOUNDARIES
#    Clusters reveal bounded contexts in your domain. `Ash.Resource.*`,
#    `Ash.Query.*`, `Ash.Policy.*` are distinct domains with their own
#    responsibilities.
#
# 2. CODEBASE NAVIGATION
#    Understanding clusters helps you know where to find things:
#    - Resource definitions? → Ash.Resource.*
#    - Query building? → Ash.Query.*
#    - Authorization? → Ash.Policy.*
#
# 3. TEAM OWNERSHIP
#    Large clusters might warrant dedicated team ownership. A cluster
#    with 50+ modules is essentially a subsystem.
#
# 4. REFACTORING TARGETS
#    Very large clusters might need splitting. Very small ones might
#    be candidates for consolidation.
#
# 5. ARCHITECTURAL LAYERS
#    Namespace patterns often indicate architectural layers:
#    - Ash.Resource.* - Data layer
#    - Ash.Actions.* - Business logic
#    - Ash.Error.* - Error handling
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule ModuleClustersQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("📦 MODULE CLUSTERS", "Code organization by namespace")

      # Get all modules
      {:ok, results} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT ?mod_name WHERE {
          ?mod a s:Module .
          ?mod s:moduleName ?mod_name .
        }
      """)

      modules = Enum.map(results, fn row -> extract(row["mod_name"]) end)

      # Group by top-level namespace (first two parts)
      by_namespace =
        modules
        |> Enum.group_by(fn name ->
          case String.split(name, ".") do
            [first, second | _] -> "#{first}.#{second}"
            [first] -> first
            _ -> "Other"
          end
        end)
        |> Enum.map(fn {ns, mods} -> {ns, length(mods), mods} end)
        |> Enum.sort_by(fn {_, count, _} -> -count end)

      # Visual output
      max_count = by_namespace |> Enum.map(fn {_, c, _} -> c end) |> Enum.max()
      scale = max(1, div(max_count, 40))

      IO.puts("  Namespace                         Modules")
      separator()

      Enum.each(by_namespace, fn {namespace, count, _mods} ->
        bar_width = div(count, scale)
        visual = String.duplicate("█", bar_width)
        IO.puts("  #{String.pad_trailing(namespace, 32)} #{pad_num(count)} #{visual}")
      end)

      # Detailed breakdown for top clusters
      IO.puts("")
      IO.puts("  📋 TOP CLUSTER DETAILS:")
      separator()

      by_namespace
      |> Enum.take(5)
      |> Enum.each(fn {namespace, count, mods} ->
        IO.puts("")
        IO.puts("  #{namespace} (#{count} modules)")

        # Show sub-namespaces
        sub_ns =
          mods
          |> Enum.group_by(fn name ->
            parts = String.split(name, ".")
            if length(parts) >= 3 do
              Enum.take(parts, 3) |> Enum.join(".")
            else
              name
            end
          end)
          |> Enum.map(fn {ns, ms} -> {ns, length(ms)} end)
          |> Enum.sort_by(fn {_, c} -> -c end)
          |> Enum.take(5)

        Enum.each(sub_ns, fn {sub, sub_count} ->
          short = String.replace(sub, namespace <> ".", "")
          IO.puts("    └─ #{String.pad_trailing(short, 28)} #{sub_count}")
        end)
      end)

      # Stats
      IO.puts("")
      IO.puts("  📊 CLUSTER STATISTICS:")
      separator()
      IO.puts("  Total namespaces: #{length(by_namespace)}")
      IO.puts("  Total modules: #{length(modules)}")

      large = Enum.count(by_namespace, fn {_, c, _} -> c >= 20 end)
      medium = Enum.count(by_namespace, fn {_, c, _} -> c >= 5 and c < 20 end)
      small = Enum.count(by_namespace, fn {_, c, _} -> c < 5 end)

      IO.puts("")
      IO.puts("  Cluster sizes:")
      IO.puts("    Large (20+):  #{large}")
      IO.puts("    Medium (5-19): #{medium}")
      IO.puts("    Small (<5):   #{small}")
    end)
  end
end

ModuleClustersQuery.run()
