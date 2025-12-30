# Error Patterns Query
#
# Run with: mix run examples/error_patterns.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# This query maps out the error module hierarchy, showing how errors are
# organized and categorized in the codebase.
#
# WHY THIS MATTERS:
#
# 1. ERROR HANDLING STRATEGY
#    A well-organized error hierarchy indicates thoughtful error handling.
#    Categories like `Error.Query`, `Error.Forbidden`, `Error.Invalid` show
#    domain-specific error classification.
#
# 2. DEBUGGING AID
#    When you encounter an error, knowing the hierarchy helps you find:
#    - Related errors in the same category
#    - The module responsible for that error type
#    - Common handling patterns for that category
#
# 3. API ERROR DESIGN
#    Error hierarchies often mirror the API structure. Understanding errors
#    helps you understand what can go wrong at each layer.
#
# 4. EXCEPTION PATTERNS
#    Elixir uses `defexception` for custom errors. This query reveals what
#    exceptions exist and how they're organized.
#
# COMMON PATTERNS:
#   - Error.Query.*: Query/filter parsing and execution errors
#   - Error.Invalid.*: Validation and constraint violations
#   - Error.Forbidden.*: Authorization/permission errors
#   - Error.Framework.*: Internal framework errors
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule ErrorPatternsQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("⚠️  ERROR PATTERNS", "Error module hierarchy and organization")

      # Find all error-related modules
      {:ok, results} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT ?mod_name WHERE {
          ?mod a s:Module .
          ?mod s:moduleName ?mod_name .
          FILTER(CONTAINS(?mod_name, "Error"))
        }
      """)

      errors =
        results
        |> Enum.map(fn row -> extract(row["mod_name"]) end)
        |> Enum.sort()

      # Group by error category
      by_category =
        errors
        |> Enum.group_by(fn name ->
          parts = String.split(name, ".")
          case Enum.find_index(parts, &(&1 == "Error")) do
            nil -> "Other"
            idx ->
              # Take up to 2 parts after "Error" for category
              Enum.slice(parts, 0, min(idx + 2, length(parts)))
              |> Enum.join(".")
          end
        end)
        |> Enum.sort_by(fn {_, mods} -> -length(mods) end)

      IO.puts("  ERROR HIERARCHY:")
      separator()

      Enum.each(by_category, fn {category, mods} ->
        IO.puts("")
        IO.puts("  📁 #{category} (#{length(mods)} errors)")

        # Show child errors
        mods
        |> Enum.reject(&(&1 == category))
        |> Enum.sort()
        |> Enum.take(8)
        |> Enum.each(fn mod ->
          # Remove the category prefix to show just the error name
          short = String.replace(mod, category <> ".", "")
          if short != mod do
            IO.puts("     └─ #{short}")
          end
        end)

        remaining = length(mods) - 9
        if remaining > 0 do
          IO.puts("     └─ ... and #{remaining} more")
        end
      end)

      # Statistics
      IO.puts("")
      IO.puts("  📊 ERROR STATISTICS:")
      separator()
      IO.puts("  Total error modules: #{length(errors)}")
      IO.puts("  Error categories: #{length(by_category)}")

      # Top categories
      IO.puts("")
      IO.puts("  Top categories by count:")
      by_category
      |> Enum.take(5)
      |> Enum.each(fn {cat, mods} ->
        IO.puts("    #{String.pad_trailing(cat, 30)} #{length(mods)} errors")
      end)

      IO.puts("")
      IO.puts("  💡 Tip: Consistent error hierarchies make rescue clauses cleaner.")
      IO.puts("     You can rescue Error.Query to catch all query-related errors.")
    end)
  end
end

ErrorPatternsQuery.run()
