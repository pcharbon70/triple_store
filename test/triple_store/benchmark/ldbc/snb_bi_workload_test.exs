defmodule TripleStore.Benchmark.LDBC.SNBBIWorkloadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.{Parameters, Workload}

  test "packages every canonical read variant and preserves exact contracts" do
    assert {:ok, definitions} = Workload.load()
    assert length(definitions) == 31

    assert Workload.variant_labels(definitions) == [
             "1",
             "2a",
             "2b",
             "2m",
             "3",
             "4",
             "5",
             "6",
             "7",
             "8a",
             "8b",
             "8m",
             "9",
             "10a",
             "10b",
             "11",
             "12",
             "13",
             "14a",
             "14b",
             "15a",
             "15b",
             "16a",
             "16b",
             "17",
             "18",
             "19a",
             "19b",
             "20a",
             "20b",
             "20m"
           ]

    assert {:ok, read_1} = Workload.fetch(definitions, 1)

    assert Enum.map(read_1.result, & &1.name) == [
             "year",
             "isComment",
             "lengthCategory",
             "messageCount",
             "averageMessageLength",
             "sumMessageLength",
             "percentageOfMessages"
           ]

    assert read_1.duplicate_semantics == :bag
    assert {:ok, read_19} = Workload.fetch(definitions, 19, "a")

    assert {:extension, "ldbc.snb.bi-weighted-interaction-path.v1", _module} =
             read_19.strategy
  end

  test "comparable parameters require pinned official provenance and exact coverage" do
    assert {:ok, definitions} = Workload.load()

    manifest = %{
      scale_factor: "1",
      transformation: %{output_checksum: "sha256:dataset"}
    }

    sequences =
      Map.new(definitions, fn definition ->
        label =
          Integer.to_string(definition.number) <>
            if(definition.variant == "default", do: "", else: definition.variant)

        row = Map.new(definition.parameters, &{&1.name, value_for(&1.type)})
        {label, [row]}
      end)

    assert {:ok, bundle} = Parameters.new(manifest, sequences)
    assert :ok = Parameters.validate(bundle, manifest, definitions, comparable: true)

    assert {:error, [:official_provenance]} =
             bundle
             |> Map.put(:provenance, :smoke)
             |> Parameters.validate(manifest, definitions, comparable: true)

    assert {:error, errors} =
             bundle
             |> put_in([:sequences, "1"], [%{"datetime" => "not-a-date"}])
             |> Parameters.validate(manifest, definitions)

    assert {"1", 0, "datetime", :invalid_datetime} in errors
  end

  defp value_for("ID"), do: 1
  defp value_for("32-bit Integer"), do: 1
  defp value_for("64-bit Integer"), do: 1
  defp value_for("32-bit Float"), do: 1.0
  defp value_for("Boolean"), do: true
  defp value_for("Date"), do: "2012-11-29"
  defp value_for("DateTime"), do: "2012-11-29T00:00:00Z"
  defp value_for(type) when type in ["String", "Long String"], do: "value"
  defp value_for("\\{String\\}"), do: ["value"]
end
