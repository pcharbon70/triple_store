# Impact Analysis Query
#
# Run with: mix run examples/impact_analysis.exs [ModuleName]
# Example:  mix run examples/impact_analysis.exs Ash.Changeset
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# Impact analysis shows what would be affected if you change a specific
# module. It answers: "If I modify X, what else might break?"
#
# WHY THIS MATTERS:
#
# 1. CHANGE RISK ASSESSMENT
#    Before refactoring a module, know your blast radius. A module with
#    50 dependents is riskier to change than one with 5.
#
# 2. TESTING STRATEGY
#    Impact analysis tells you what to test. If you change Ash.Changeset,
#    you should test all the modules that depend on it.
#
# 3. DEPRECATION PLANNING
#    Want to deprecate a function? See exactly who uses it and how,
#    so you can plan migration paths.
#
# 4. CODE REVIEW FOCUS
#    Reviewers should scrutinize changes to high-impact modules more
#    carefully than changes to isolated modules.
#
# 5. BACKWARDS COMPATIBILITY
#    High-impact modules need stable APIs. Consider semantic versioning
#    and deprecation warnings before breaking changes.
#
# METRICS:
#   - Dependent modules: Count of unique modules that call this one
#   - Function usage: Which specific functions are called
#   - Coupling depth: How tightly coupled are the dependents?
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule ImpactAnalysisQuery do
  import QueryHelpers

  def run(target_module) do
    with_store(fn store ->
      header("💥 IMPACT ANALYSIS", "What breaks if #{target_module} changes?")

      # Find all call sites that call functions in the target module
      {:ok, results} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT ?callsite ?called_func WHERE {
          ?callsite s:callsFunction ?callee .
          ?callsite s:moduleName "#{target_module}" .
          ?callsite s:functionName ?called_func .
        }
      """)

      # Group by calling module
      dependents =
        results
        |> Enum.map(fn row ->
          callsite = extract(row["callsite"])
          func = extract(row["called_func"])
          caller = extract_caller_module(callsite)
          {caller, func}
        end)
        |> Enum.reject(fn {caller, _} -> caller == target_module end)
        |> Enum.group_by(fn {caller, _} -> caller end, fn {_, func} -> func end)
        |> Enum.map(fn {caller, funcs} -> {caller, Enum.uniq(funcs)} end)
        |> Enum.sort_by(fn {_, funcs} -> -length(funcs) end)

      if Enum.empty?(dependents) do
        IO.puts("  No external dependents found for #{target_module}")
        IO.puts("  This module may be an entry point or leaf node.")
      else
        IO.puts("  #{length(dependents)} modules depend on #{target_module}")
        IO.puts("")

        # Categorize by coupling level
        high_coupling = Enum.filter(dependents, fn {_, funcs} -> length(funcs) >= 5 end)
        medium_coupling = Enum.filter(dependents, fn {_, funcs} -> length(funcs) >= 2 and length(funcs) < 5 end)
        low_coupling = Enum.filter(dependents, fn {_, funcs} -> length(funcs) == 1 end)

        if length(high_coupling) > 0 do
          IO.puts("  🔴 HIGH COUPLING (5+ functions used):")
          separator()
          Enum.take(high_coupling, 10)
          |> Enum.each(fn {caller, funcs} ->
            IO.puts("  #{caller}")
            IO.puts("    uses #{length(funcs)} functions: #{Enum.take(funcs, 5) |> Enum.join(", ")}#{if length(funcs) > 5, do: "...", else: ""}")
          end)
          IO.puts("")
        end

        if length(medium_coupling) > 0 do
          IO.puts("  🟡 MEDIUM COUPLING (2-4 functions used):")
          separator()
          Enum.take(medium_coupling, 10)
          |> Enum.each(fn {caller, funcs} ->
            IO.puts("  #{String.pad_trailing(caller, 45)} #{Enum.join(funcs, ", ")}")
          end)
          IO.puts("")
        end

        if length(low_coupling) > 0 do
          IO.puts("  🟢 LOW COUPLING (1 function used):")
          separator()
          Enum.take(low_coupling, 15)
          |> Enum.each(fn {caller, [func]} ->
            IO.puts("  #{String.pad_trailing(caller, 45)} #{func}")
          end)
          if length(low_coupling) > 15 do
            IO.puts("  ... and #{length(low_coupling) - 15} more")
          end
        end

        # Function usage frequency
        IO.puts("")
        IO.puts("  📊 MOST USED FUNCTIONS:")
        separator()

        all_func_usage =
          dependents
          |> Enum.flat_map(fn {_, funcs} -> funcs end)
          |> Enum.frequencies()
          |> Enum.sort_by(fn {_, count} -> -count end)
          |> Enum.take(10)

        Enum.each(all_func_usage, fn {func, count} ->
          IO.puts("  #{String.pad_trailing(func, 30)} used by #{count} modules")
        end)

        IO.puts("")
        IO.puts("  💡 Changes to frequently-used functions have the highest impact.")
      end
    end)
  end
end

target = System.argv() |> List.first() || "Ash.Changeset"
ImpactAnalysisQuery.run(target)
