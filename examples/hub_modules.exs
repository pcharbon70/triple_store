# Hub Modules Query
#
# Run with: mix run examples/hub_modules.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# Hub modules are the "central nodes" in your codebase's dependency graph.
# They have high connectivity - many modules call them, and they call many
# modules. These are critical architectural components.
#
# WHY THIS MATTERS:
#
# 1. ARCHITECTURAL SIGNIFICANCE
#    Hub modules are often your core abstractions (e.g., Ash.Changeset,
#    Ash.Query). Changes to these ripple through the entire codebase.
#
# 2. TESTING PRIORITY
#    Bugs in hub modules affect many consumers. These deserve the most
#    thorough testing and code review attention.
#
# 3. REFACTORING RISK
#    High-connectivity modules are risky to refactor. Plan carefully and
#    consider backwards compatibility.
#
# 4. ONBOARDING FOCUS
#    New developers should understand hub modules first - they're central
#    to how the system works.
#
# METRICS:
#   - Incoming: How many times this module's functions are called
#   - Outgoing: How many external function calls this module makes
#   - Total: Combined connectivity score
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule HubModulesQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("🔗 HUB MODULES", "Most connected modules in the codebase")

      # Count incoming calls (how many call sites target each module)
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

      # Count outgoing calls (how many calls originate from each module)
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

      hub_scores =
        all_mods
        |> Enum.map(fn mod ->
          inc = Map.get(incoming_counts, mod, 0)
          out = Map.get(outgoing_counts, mod, 0)
          {mod, inc, out, inc + out}
        end)
        |> Enum.sort_by(fn {_, _, _, total} -> -total end)
        |> Enum.take(20)

      # Print results
      IO.puts("  Module                                      In    Out   Total")
      separator()

      Enum.each(hub_scores, fn {mod, inc, out, total} ->
        mod_padded = String.pad_trailing(mod, 42)
        IO.puts("  #{mod_padded} #{pad_num(inc)} #{pad_num(out)} #{pad_num(total)}")
      end)

      # Summary insights
      IO.puts("")
      {top_mod, top_in, top_out, _top_total} = hd(hub_scores)
      IO.puts("  📊 Top hub: #{top_mod}")
      IO.puts("     #{top_in} incoming calls, #{top_out} outgoing calls")
      IO.puts("")
      IO.puts("  💡 Tip: Use impact_analysis.exs to see what depends on a specific module")
    end)
  end
end

HubModulesQuery.run()
