# Call Graph Query Example
#
# Run with: mix run examples/call_graph_query.exs
#
# This script demonstrates querying the Ash framework call graph
# to find which modules call functions in a target module.

defmodule CallGraphQuery do
  @moduledoc """
  Query the call graph from the Ash RDF dataset.
  """

  @doc """
  Extract a readable value from RDF term tuples.
  """
  def extract({:literal, :simple, val}), do: val
  def extract({:literal, :typed, val, _}), do: val
  def extract({:named_node, url}), do: url
  def extract(other), do: inspect(other)

  @doc """
  Extract the caller module from a call site URI.
  Example: "https://example.org/code#call/Ash.Resource.Change/module/0/141"
           -> "Ash.Resource.Change"
  """
  def extract_caller_module(callsite_uri) do
    callsite_uri
    |> String.split(["#call/", "/module"])
    |> Enum.at(1, "unknown")
  end

  @doc """
  Find all modules that call functions in the target module.
  """
  def incoming_calls(store, target_module) do
    query = """
    PREFIX s: <https://w3id.org/elixir-code/structure#>
    SELECT ?callsite ?called_func WHERE {
      ?callsite s:callsFunction ?callee .
      ?callsite s:moduleName "#{target_module}" .
      ?callsite s:functionName ?called_func .
      FILTER(!CONTAINS(STR(?callsite), "#{target_module}/"))
    }
    """

    case TripleStore.query(store, query) do
      {:ok, results} ->
        results
        |> Enum.map(fn row ->
          callsite = extract(row["callsite"])
          caller_mod = extract_caller_module(callsite)
          called_func = extract(row["called_func"])
          {caller_mod, called_func}
        end)
        |> Enum.group_by(fn {mod, _} -> mod end, fn {_, func} -> func end)
        |> Enum.map(fn {mod, funcs} -> {mod, Enum.uniq(funcs)} end)
        |> Enum.sort_by(fn {_, funcs} -> -length(funcs) end)

      {:error, reason} ->
        IO.puts("Query failed: #{inspect(reason)}")
        []
    end
  end

  @doc """
  Find all outgoing calls from a module (what does it call?).
  """
  def outgoing_calls(store, source_module) do
    query = """
    PREFIX s: <https://w3id.org/elixir-code/structure#>
    SELECT ?called_mod ?called_func WHERE {
      ?callsite s:callsFunction ?callee .
      ?callsite s:moduleName ?called_mod .
      ?callsite s:functionName ?called_func .
      FILTER(CONTAINS(STR(?callsite), "#{source_module}/"))
    }
    """

    case TripleStore.query(store, query) do
      {:ok, results} ->
        results
        |> Enum.map(fn row ->
          {extract(row["called_mod"]), extract(row["called_func"])}
        end)
        |> Enum.uniq()
        |> Enum.group_by(fn {mod, _} -> mod end, fn {_, func} -> func end)
        |> Enum.sort_by(fn {mod, _} -> mod end)

      {:error, reason} ->
        IO.puts("Query failed: #{inspect(reason)}")
        []
    end
  end

  @doc """
  Print a formatted call graph report.
  """
  def print_report(store, module_name) do
    separator = String.duplicate("=", 70)
    sub_separator = String.duplicate("-", 60)

    IO.puts(separator)
    IO.puts("📊 CALL GRAPH FOR #{module_name}")
    IO.puts(separator)

    # Incoming calls
    IO.puts("\n🟢 INCOMING CALLS (who calls #{module_name}?):")
    IO.puts(sub_separator)

    incoming = incoming_calls(store, module_name)

    if Enum.empty?(incoming) do
      IO.puts("  (no incoming calls found)")
    else
      Enum.each(incoming, fn {caller_mod, funcs} ->
        func_list = Enum.take(funcs, 5) |> Enum.join(", ")
        more = if length(funcs) > 5, do: " (+#{length(funcs) - 5} more)", else: ""
        IO.puts("  #{caller_mod}")
        IO.puts("    calls: #{func_list}#{more}")
      end)
    end

    # Outgoing calls
    IO.puts("\n🔵 OUTGOING CALLS (what does #{module_name} call?):")
    IO.puts(sub_separator)

    outgoing = outgoing_calls(store, module_name)

    if Enum.empty?(outgoing) do
      IO.puts("  (no outgoing calls found in dataset)")
    else
      Enum.each(outgoing, fn {called_mod, funcs} ->
        func_list = Enum.join(funcs, ", ")
        IO.puts("  → #{called_mod}: #{func_list}")
      end)
    end

    IO.puts("")
  end
end

# Main script execution
target_module = System.argv() |> List.first() || "Ash.Changeset"
data_path = "./tmp/ash_data"

IO.puts("Opening store at #{data_path}...")

case TripleStore.open(data_path) do
  {:ok, store} ->
    CallGraphQuery.print_report(store, target_module)
    TripleStore.close(store)

  {:error, reason} ->
    IO.puts("Failed to open store: #{inspect(reason)}")
    IO.puts("Make sure you've loaded the data first:")
    IO.puts("  mix run -e '{:ok, s} = TripleStore.open(\"./tmp/ash_data\"); TripleStore.load(s, \"examples/ash.ttl\"); TripleStore.close(s)'")
    System.halt(1)
end
