defmodule TripleStore.Benchmark.LDBC.OperationModelTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.{Codec, Operation, OperationRegistry, Result}

  test "loads linked, versioned definitions for all three suites" do
    assert {:ok, operations} = OperationRegistry.load()
    assert Enum.map(operations, & &1.suite) == [:spb, :snb_bi, :snb_interactive]
    assert String.length(OperationRegistry.checksum(operations)) == 64
    assert Enum.all?(operations, &(Operation.validate(&1) == :ok))
  end

  test "rejects unknown and missing parameters before query execution" do
    schema = [%{name: "personId", type: :id}, %{name: "name", type: :string}]

    assert {:error, {:unknown_parameters, ["injected"]}} =
             Codec.decode_parameters(schema, %{
               "personId" => 1,
               "name" => "Ada",
               "injected" => "x"
             })

    assert {:error, {:missing_parameter, "name"}} =
             Codec.decode_parameters(schema, %{"personId" => 1})
  end

  test "detects lossy and invalid boundary values" do
    assert {:error, {:integer_overflow, 8}} = Codec.decode({:integer, 8}, 128)
    assert {:error, :precision_loss} = Codec.decode(:float32, 0.1)
    assert {:error, :timezone_drift} = Codec.decode(:timestamp, "2025-01-01T00:00:00+01:00")
    assert {:error, :invalid_utf8} = Codec.decode(:string, <<255>>)
    assert {:ok, :null} = Codec.decode({:optional, :string}, :null)
    assert {:ok, :unbound} = Codec.decode({:optional, :string}, :unbound)
  end

  test "preserves column order and duplicates and sorts only unordered results" do
    operation = operation(%{mode: :unordered})
    rows = [%{"id" => 2, "name" => "B"}, %{"id" => 1, "name" => "A"}, %{"id" => 1, "name" => "A"}]

    assert {:ok, result} = Result.from_maps(operation, rows)
    assert result.columns == ["id", "name"]
    assert length(result.rows) == 3
    assert {:ok, canonical} = Result.canonicalize(result)
    assert Enum.frequencies(canonical.rows)[[1, "A"]] == 2

    assert {:error, :sorting_not_permitted} =
             operation(%{mode: :ordered, keys: [{"id", :asc}]})
             |> Result.from_maps(rows)
             |> then(fn {:ok, ordered} -> Result.canonicalize(ordered) end)
  end

  test "rejects missing result columns and unbound/null mismatches" do
    assert {:error, {:invalid_result_row, 0, {:missing_columns, ["name"]}}} =
             Result.from_maps(operation(%{mode: :unordered}), [%{"id" => 1}])

    optional = %{
      operation(%{mode: :unordered})
      | result_schema: [%{name: "value", type: {:optional, :string}}]
    }

    assert {:ok, null_result} = Result.from_maps(optional, [%{"value" => :null}])
    assert {:ok, unbound_result} = Result.from_maps(optional, [%{"value" => :unbound}])
    refute null_result.rows == unbound_result.rows
  end

  defp operation(ordering) do
    %Operation{
      id: "test/read@v1",
      suite: :spb,
      profile_id: "test",
      catalog_id: "test/read@v1",
      upstream_id: "test",
      kind: :read,
      parameter_schema: [],
      result_schema: [%{name: "id", type: :id}, %{name: "name", type: :string}],
      ordering: ordering,
      limit: nil,
      timeout_class: :short,
      tags: [],
      strategy: {:sparql, "SELECT * WHERE { ?s ?p ?o }"},
      source: %{source_id: "test", path: "query.sparql", checksum: "abc"},
      transformation_version: "v1"
    }
  end
end
