# Entry Points Query
#
# Run with: mix run examples/entry_points.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# Entry points are modules with few or no incoming dependencies. They sit at
# the "edges" of your dependency graph - nothing (or very little) calls them.
#
# WHY THIS MATTERS:
#
# 1. ONBOARDING STARTING POINTS
#    When learning a new codebase, start with entry point modules. They have
#    fewer dependencies to understand first, making them easier to grok.
#
# 2. APPLICATION BOUNDARIES
#    Entry points often represent your public API, CLI commands, web
#    controllers, or background job handlers - the edges where external
#    systems interact with your code.
#
# 3. DEAD CODE DETECTION
#    Modules with zero incoming calls might be unused (dead code), or they
#    might be dynamically invoked (behaviours, callbacks, macros).
#
# 4. ARCHITECTURAL LAYERS
#    In well-layered architectures, entry points sit at the top layer.
#    If core modules appear here, it might indicate architectural issues.
#
# INTERPRETATION:
#   - 0 calls: Possibly dead code OR dynamically invoked (check behaviours)
#   - 1-5 calls: Specialized modules, good onboarding candidates
#   - High calls: Not an entry point - it's a dependency
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule EntryPointsQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("🚪 ENTRY POINTS", "Modules with few incoming dependencies")

      # Get all modules
      {:ok, all_mods} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT DISTINCT ?mod_name WHERE {
          ?mod a s:Module .
          ?mod s:moduleName ?mod_name .
        }
      """)

      # Count incoming calls per module
      {:ok, called} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT ?mod_name WHERE {
          ?callsite s:callsFunction ?callee .
          ?callsite s:moduleName ?mod_name .
        }
      """)

      call_counts =
        called
        |> Enum.map(fn row -> extract(row["mod_name"]) end)
        |> Enum.frequencies()

      # Find modules with low incoming call counts
      entry_points =
        all_mods
        |> Enum.map(fn row -> extract(row["mod_name"]) end)
        |> Enum.map(fn mod -> {mod, Map.get(call_counts, mod, 0)} end)
        |> Enum.sort_by(fn {_, count} -> count end)

      # Separate into categories
      zero_calls = Enum.filter(entry_points, fn {_, c} -> c == 0 end)
      low_calls = Enum.filter(entry_points, fn {_, c} -> c > 0 and c <= 5 end)

      # Filter to main modules (not error/dsl internals)
      filter_main = fn list ->
        Enum.filter(list, fn {mod, _} ->
          String.starts_with?(mod, "Ash.") and
          not String.contains?(mod, ".Error.") and
          not String.contains?(mod, ".Dsl.") and
          not String.contains?(mod, "Transformer") and
          not String.contains?(mod, "Verifier")
        end)
      end

      IO.puts("  ZERO INCOMING CALLS (potential entry points or dead code):")
      separator()
      filter_main.(zero_calls)
      |> Enum.take(15)
      |> Enum.each(fn {mod, _} ->
        IO.puts("  #{mod}")
      end)

      IO.puts("\n  LOW INCOMING CALLS (1-5 calls, specialized modules):")
      separator()
      filter_main.(low_calls)
      |> Enum.take(15)
      |> Enum.each(fn {mod, count} ->
        IO.puts("  #{String.pad_trailing(mod, 50)} #{count} calls")
      end)

      # Summary
      IO.puts("")
      IO.puts("  📊 Total modules: #{length(entry_points)}")
      IO.puts("     Zero incoming calls: #{length(zero_calls)}")
      IO.puts("     Low incoming calls (1-5): #{length(low_calls)}")
      IO.puts("")
      IO.puts("  💡 Tip: Zero-call modules may use behaviours/callbacks.")
      IO.puts("     Check if they implement Ash.Resource, Ash.Type, etc.")
    end)
  end
end

EntryPointsQuery.run()
