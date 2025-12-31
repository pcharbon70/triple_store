# API Surface Query
#
# Run with: mix run examples/api_surface.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# The API surface shows how many public functions each module exposes.
# This is a measure of module complexity and interface size.
#
# WHY THIS MATTERS:
#
# 1. INTERFACE COMPLEXITY
#    Large API surfaces (many public functions) can indicate:
#    - A module doing too much (violating single responsibility)
#    - A rich, feature-complete abstraction (like Ash.Changeset)
#    - Potential candidates for splitting into sub-modules
#
# 2. LEARNING CURVE
#    Modules with 50+ public functions take longer to learn. Consider
#    whether all those functions need to be public.
#
# 3. MAINTENANCE BURDEN
#    Every public function is a contract. More public functions = more
#    API surface to maintain, document, and keep backwards-compatible.
#
# 4. DOCUMENTATION PRIORITY
#    Large API surfaces need better documentation. Focus docs effort on
#    modules with many public functions.
#
# HEALTHY RANGES:
#   - 1-10 functions: Focused, single-purpose module
#   - 10-30 functions: Feature-rich but manageable
#   - 30-50 functions: Consider if all need to be public
#   - 50+ functions: May need refactoring or sub-modules
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule ApiSurfaceQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("📚 API SURFACE", "Public functions per module")

      # Get public functions per module
      {:ok, results} =
        TripleStore.query(store, """
          PREFIX s: <https://w3id.org/elixir-code/structure#>
          SELECT ?mod_name ?func_name WHERE {
            ?mod a s:Module .
            ?mod s:moduleName ?mod_name .
            ?mod s:containsFunction ?func .
            ?func a s:PublicFunction .
            ?func s:functionName ?func_name .
          }
        """)

      by_module =
        results
        |> Enum.map(fn row -> {extract(row["mod_name"]), extract(row["func_name"])} end)
        |> Enum.group_by(fn {mod, _} -> mod end, fn {_, func} -> func end)
        |> Enum.map(fn {mod, funcs} -> {mod, Enum.uniq(funcs)} end)
        |> Enum.sort_by(fn {_, funcs} -> -length(funcs) end)

      # Top 25 by size
      top_25 = Enum.take(by_module, 25)

      IO.puts("  Module                                      Public Funcs")
      separator()

      Enum.each(top_25, fn {mod, funcs} ->
        count = length(funcs)
        visual = bar(count, 2)
        IO.puts("  #{String.pad_trailing(mod, 44)} #{pad_num(count)} #{visual}")
      end)

      # Distribution stats
      counts = Enum.map(by_module, fn {_, funcs} -> length(funcs) end)
      total_funcs = Enum.sum(counts)
      total_mods = length(by_module)

      case top_25 do
        [{largest_mod, largest_funcs} | _] ->
          avg = Float.round(total_funcs / total_mods, 1)

          small = Enum.count(counts, &(&1 <= 10))
          medium = Enum.count(counts, &(&1 > 10 and &1 <= 30))
          large = Enum.count(counts, &(&1 > 30 and &1 <= 50))
          xlarge = Enum.count(counts, &(&1 > 50))

          IO.puts("")
          IO.puts("  📊 DISTRIBUTION:")
          separator()
          IO.puts("  Total modules: #{total_mods}")
          IO.puts("  Total public functions: #{total_funcs}")
          IO.puts("  Average per module: #{avg}")
          IO.puts("")
          IO.puts("  Size breakdown:")
          IO.puts("    Small (1-10):     #{small} modules")
          IO.puts("    Medium (11-30):   #{medium} modules")
          IO.puts("    Large (31-50):    #{large} modules")
          IO.puts("    X-Large (50+):    #{xlarge} modules")

          # Show functions for the largest module
          IO.puts("")
          IO.puts("  📋 Largest module: #{largest_mod} (#{length(largest_funcs)} functions)")
          IO.puts("     Sample functions:")

          largest_funcs
          |> Enum.take(10)
          |> Enum.each(fn f -> IO.puts("       - #{f}") end)

          if length(largest_funcs) > 10 do
            IO.puts("       ... and #{length(largest_funcs) - 10} more")
          end

        [] ->
          IO.puts("")
          IO.puts("  ⚠️  No module/function data found in the store.")
          IO.puts("     Make sure you have loaded code analysis RDF data first.")
      end
    end)
  end
end

ApiSurfaceQuery.run()
