# Type Usage Query
#
# Run with: mix run examples/type_usage.exs
#
# ============================================================================
# WHAT THIS QUERY REVEALS
# ============================================================================
#
# This query shows which type definitions appear most frequently across
# modules. Types like `t` (the conventional main type for a module) appear
# in many modules, while specialized types are more localized.
#
# WHY THIS MATTERS:
#
# 1. TYPE CONVENTIONS
#    In Elixir/Erlang, `t` is the conventional name for a module's main
#    type. Seeing `t` appear in most modules confirms the codebase follows
#    this convention.
#
# 2. DOMAIN MODELING
#    Specialized types reveal domain concepts. Types like `phase`,
#    `action_result`, or `error_info` show how the domain is modeled.
#
# 3. TYPE REUSE
#    Types that appear in many modules might be candidates for extraction
#    into shared type modules to reduce duplication.
#
# 4. API CONTRACTS
#    Public types form the API contract. Understanding the type landscape
#    helps you understand what data shapes flow through the system.
#
# COMMON PATTERNS:
#   - `t`: The module's main struct/type
#   - `opts`, `options`: Configuration options
#   - `result`, `response`: Return value types
#   - Callback types: `on_success`, `handler`, etc.
#
# ============================================================================

Code.require_file("query_helpers.ex", __DIR__)

defmodule TypeUsageQuery do
  import QueryHelpers

  def run do
    with_store(fn store ->
      header("🏷️  TYPE USAGE", "Type definitions across the codebase")

      # Get all type definitions
      {:ok, results} = TripleStore.query(store, """
        PREFIX s: <https://w3id.org/elixir-code/structure#>
        SELECT ?type_name ?mod_name WHERE {
          ?mod a s:Module .
          ?mod s:moduleName ?mod_name .
          ?mod s:containsType ?type .
          ?type s:typeName ?type_name .
        }
      """)

      # Group by type name
      by_type =
        results
        |> Enum.map(fn row -> {extract(row["type_name"]), extract(row["mod_name"])} end)
        |> Enum.group_by(fn {type, _} -> type end, fn {_, mod} -> mod end)
        |> Enum.map(fn {type, mods} -> {type, Enum.uniq(mods)} end)
        |> Enum.sort_by(fn {_, mods} -> -length(mods) end)

      # Print top types
      IO.puts("  Type Name              Modules  Example Modules")
      separator()

      Enum.take(by_type, 25)
      |> Enum.each(fn {type, mods} ->
        type_padded = String.pad_trailing(type, 22)
        examples = mods |> Enum.take(3) |> Enum.map(&short_name/1) |> Enum.join(", ")
        IO.puts("  #{type_padded} #{pad_num(length(mods))}    #{examples}")
      end)

      # Stats
      total_types = length(by_type)
      unique_in_one = Enum.count(by_type, fn {_, mods} -> length(mods) == 1 end)
      widespread = Enum.count(by_type, fn {_, mods} -> length(mods) >= 5 end)

      IO.puts("")
      IO.puts("  📊 TYPE STATISTICS:")
      separator()
      IO.puts("  Unique type names: #{total_types}")
      IO.puts("  Types in 1 module only: #{unique_in_one} (specialized)")
      IO.puts("  Types in 5+ modules: #{widespread} (widespread)")

      # Show specialized types (appear in exactly one module)
      IO.puts("")
      IO.puts("  🎯 SPECIALIZED TYPES (unique to one module):")
      separator()

      by_type
      |> Enum.filter(fn {_, mods} -> length(mods) == 1 end)
      |> Enum.take(15)
      |> Enum.each(fn {type, [mod]} ->
        IO.puts("  @type #{type} in #{short_name(mod)}")
      end)

      IO.puts("")
      IO.puts("  💡 Tip: The `t` type convention helps dialyzer and documentation.")
    end)
  end
end

TypeUsageQuery.run()
