defmodule TripleStore.Benchmark.LDBC.SNBBIAnalyticsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.SNB.BI.{Analytics, HandlerRegistry, Workload}
  alias TripleStore.Dictionary.Manager

  test "registry covers every standard variant exactly once" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, registry} = HandlerRegistry.build(definitions)

    assert registry.version == HandlerRegistry.version()
    assert registry.handlers |> Map.keys() |> Enum.sort() == Enum.sort(HandlerRegistry.keys())
    assert map_size(registry.handlers) == 22

    [first | rest] =
      Enum.filter(definitions, &({&1.number, &1.variant} in HandlerRegistry.keys()))

    first_key = {first.number, first.variant}

    assert {:error, {:missing_bi_handlers, [missing | _]}} = HandlerRegistry.build(rest)
    assert missing == first_key

    assert {:error, {:duplicate_bi_handlers, [^first_key]}} =
             HandlerRegistry.build([first, first | rest])
  end

  test "default handler executes prepared list bindings and enforces the result contract", %{
    test: test
  } do
    {context, cleanup} = query_context(test)
    on_exit(cleanup)

    definition = list_definition()

    assert {:ok, result} =
             Analytics.execute(definition, context, %{"languages" => ["fr", "en"]})

    assert result.columns == ["language"]
    assert result.rows == [["en"], ["fr"]]
  end

  test "explicit expert handler takes precedence over the default registry" do
    definition = list_definition()

    handler = fn _context, _parameters, _opts ->
      {:ok, [%{"language" => "override"}]}
    end

    context = %{operation_handlers: %{{12, "default"} => handler}}

    assert {:ok, result} = Analytics.execute(definition, context, %{})
    assert result.rows == [["override"]]
  end

  defp list_definition do
    %{
      number: 12,
      variant: "default",
      parameters: [%{name: "languages", type: "\\{String\\}"}],
      result: [%{name: "language", type: "String"}],
      ordering: [%{name: "language", direction: "asc"}],
      limit: nil,
      translation: %{
        checksum: "sha256:test",
        sparql: "SELECT ?language WHERE { VALUES ?language { {{languages}} } }"
      }
    }
  end

  defp query_context(test) do
    path =
      Path.join(
        System.tmp_dir!(),
        "snb_bi_analytics_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    cleanup = fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end

    {%{db: db, dict_manager: manager}, cleanup}
  end
end
