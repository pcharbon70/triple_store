defmodule TripleStore.SPARQL.ContextTypesTest do
  use ExUnit.Case, async: true

  test "query and property-path contexts share public database and dictionary handle types" do
    for module <- [TripleStore.SPARQL.Query, TripleStore.SPARQL.PropertyPath] do
      {:ok, types} = Code.Typespec.fetch_types(module)
      {:type, context} = Enum.find(types, fn {_, {name, _, _}} -> name == :context end)
      quoted = context |> Code.Typespec.type_to_quoted() |> Macro.to_string()

      assert quoted =~ "TripleStore.db_ref()"
      assert quoted =~ "TripleStore.manager()"
      refute quoted =~ "reference()"
    end
  end
end
