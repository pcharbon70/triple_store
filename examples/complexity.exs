# Complexity Query
#
# Run with: mix run examples/complexity.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# This query identifies modules with high outgoing call counts - they call
# many functions in other modules. This is one measure of complexity.
#
# WHY THIS MATTERS:
#
# 1. COGNITIVE LOAD
#    Modules with many outgoing dependencies are harder to understand.
#    You need to know about all those other modules to fully grasp what
#    this module does.
#
# 2. TESTING DIFFICULTY
#    High outgoing calls often mean more mocking/stubbing in tests.
#    These modules may need integration tests rather than unit tests.
#
# 3. COUPLING SMELL
#    Very high outgoing call counts can indicate:
#    - God modules doing too much
#    - Insufficient abstraction
#    - Cross-cutting concerns needing extraction
#
# 4. REFACTORING CANDIDATES
#    Consider breaking up modules with 100+ outgoing calls. They might
#    be orchestrators that should delegate more.
#
# 5. DEPENDENCY INJECTION NEEDS
#    High-complexity modules benefit from dependency injection to
#    improve testability and reduce coupling.
#
# HEALTHY RANGES:
#   - <20 calls: Well-focused module
#   - 20-50 calls: Normal for feature modules
#   - 50-100 calls: Getting complex
#   - 100+ calls: Consider refactoring
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule ComplexityQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("🔀 COMPLEXITY ANALYSIS", "Modules with most outgoing dependencies")

      # Count outgoing calls per module
      {:ok, results} =
        TripleStore.query(store, """
          PREFIX s: <https://w3id.org/elixir-code/structure#>
          SELECT ?callsite ?called_mod WHERE {
            ?callsite s:callsFunction ?callee .
            ?callsite s:moduleName ?called_mod .
          }
        """)

      by_caller =
        results
        |> Enum.map(fn row ->
          callsite = extract(row["callsite"])
          extract_caller_module(callsite)
        end)
        |> Enum.reject(&(&1 == "unknown"))
        |> Enum.frequencies()
        |> Enum.sort_by(fn {_, count} -> -count end)

      # Visual output
      max_count = by_caller |> Enum.take(1) |> Enum.map(fn {_, c} -> c end) |> List.first() || 1
      scale = max(1, div(max_count, 35))

      IO.puts("  Module                                      Outgoing Calls")
      separator()

      Enum.take(by_caller, 25)
      |> Enum.each(fn {mod, count} ->
        visual = bar(count, scale)
        IO.puts("  #{String.pad_trailing(mod, 44)} #{pad_num(count)} #{visual}")
      end)

      # Complexity categories
      counts = Enum.map(by_caller, fn {_, c} -> c end)

      very_complex = Enum.count(counts, &(&1 >= 100))
      complex = Enum.count(counts, &(&1 >= 50 and &1 < 100))
      moderate = Enum.count(counts, &(&1 >= 20 and &1 < 50))
      simple = Enum.count(counts, &(&1 < 20))

      IO.puts("")
      IO.puts("  📊 COMPLEXITY DISTRIBUTION:")
      separator()
      IO.puts("  Very complex (100+ calls): #{very_complex} modules")
      IO.puts("  Complex (50-99 calls):     #{complex} modules")
      IO.puts("  Moderate (20-49 calls):    #{moderate} modules")
      IO.puts("  Simple (<20 calls):        #{simple} modules")

      # Show what the most complex module calls
      case by_caller do
        [{most_complex, _} | _] ->
          IO.puts("")
          IO.puts("  🔍 MOST COMPLEX: #{most_complex}")
          separator()

          {:ok, deps} =
            TripleStore.query(store, """
              PREFIX s: <https://w3id.org/elixir-code/structure#>
              SELECT ?called_mod WHERE {
                ?callsite s:callsFunction ?callee .
                ?callsite s:moduleName ?called_mod .
                FILTER(CONTAINS(STR(?callsite), "#{most_complex}/"))
              }
            """)

          dep_modules =
            deps
            |> Enum.map(fn row -> extract(row["called_mod"]) end)
            |> Enum.frequencies()
            |> Enum.sort_by(fn {_, c} -> -c end)
            |> Enum.take(10)

          IO.puts("  Top dependencies:")

          Enum.each(dep_modules, fn {mod, count} ->
            IO.puts("    #{String.pad_trailing(mod, 35)} #{count} calls")
          end)

          IO.puts("")
          IO.puts("  💡 Tip: High complexity often indicates orchestration logic.")
          IO.puts("     Consider extracting smaller, focused functions.")

        [] ->
          IO.puts("")
          IO.puts("  ⚠️  No call data found in the store.")
          IO.puts("     Make sure you have loaded code analysis RDF data first.")
      end
    end)
  end
end

ComplexityQuery.run()
