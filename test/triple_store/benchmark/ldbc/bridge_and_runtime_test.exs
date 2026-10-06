defmodule TripleStore.Benchmark.LDBC.BridgeAndRuntimeTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{Bridge, Runtime}
  alias TripleStore.Benchmark.LDBC.Bridge.Protocol

  @handshake %{
    "profile_id" => "spb-smoke-v2.0.2",
    "dataset_manifest" => "dataset-sha256",
    "operation_catalog" => "catalog-sha256"
  }

  test "requires an exact handshake and correlates concurrent requests" do
    handler = fn
      :handshake, _frame -> {:ok, %{"accepted" => true}}
      :execute, frame -> {:ok, Map.fetch!(frame, "parameters")}
    end

    assert {:ok, bridge} =
             Bridge.start_link(benchmark_mode: true, handshake: @handshake, handler: handler)

    assert {:error, %{class: :bridge, reason: :handshake_required}} = execute(bridge, "before")
    assert {:ok, %{"accepted" => true}} = Bridge.request(bridge, handshake_frame())

    tasks =
      for number <- 1..4 do
        Task.async(fn -> execute(bridge, Integer.to_string(number), %{"number" => number}) end)
      end

    assert Enum.map(tasks, &Task.await/1) ==
             Enum.map(1..4, &{:ok, %{"number" => &1}})
  end

  test "enforces bounded parameters, concurrency, and cancellation" do
    parent = self()

    handler = fn
      :handshake, _frame ->
        {:ok, :ready}

      :execute, frame ->
        send(parent, {:started, frame["id"]})
        Process.sleep(5_000)
        {:ok, :late}
    end

    assert {:ok, bridge} =
             Bridge.start_link(
               benchmark_mode: true,
               handshake: @handshake,
               handler: handler,
               limits: [parameter_bytes: 64, concurrency: 1]
             )

    assert {:ok, :ready} = Bridge.request(bridge, handshake_frame())
    caller = Task.async(fn -> execute(bridge, "slow") end)
    assert_receive {:started, "slow"}
    assert {:error, %{reason: :concurrency_limit}} = execute(bridge, "second")
    assert :ok = Bridge.cancel(bridge, "slow")
    assert {:error, %{class: :cancellation, reason: :cancelled}} = Task.await(caller)

    assert {:error, %{reason: :parameters_too_large}} =
             execute(bridge, "large", %{"value" => String.duplicate("x", 100)})
  end

  test "round trips framed JSON and rejects oversized or malformed frames" do
    frame = %{"id" => "1", "action" => "health"}
    assert {:ok, encoded} = Protocol.encode(frame, 1_024)
    assert {:ok, ^frame} = Protocol.decode(encoded, 1_024)
    assert {:error, :frame_too_large} = Protocol.encode(frame, 1)
    assert {:error, :malformed_frame} = Protocol.decode(<<1, 2>>, 1_024)
  end

  test "runtime owns optional services and reports deterministic teardown" do
    path = Path.join(System.tmp_dir!(), "ldbc-runtime-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(path) end)

    service =
      {:probe, fn _store -> Agent.start_link(fn -> :running end) end,
       fn agent ->
         Agent.stop(agent)
         :ok
       end}

    assert {:ok, runtime} = Runtime.start_link(path: path, schema: :quad, services: [service])
    assert {:ok, %{schema: :quad}} = Runtime.store(runtime)
    assert {:ok, :quad} = Runtime.run(runtime, fn store -> {:ok, store.schema} end)
    assert {:ok, accounting} = Runtime.shutdown(runtime)
    assert accounting.services_started == 1
    assert accounting.services_stopped == 1
    assert accounting.outstanding_resources == []
    assert accounting.store_close == :ok
  end

  test "bridge cannot start without explicit benchmark mode" do
    assert {:error, :benchmark_mode_required} =
             Bridge.start_link(handshake: @handshake, handler: fn _, _ -> {:ok, nil} end)
  end

  defp handshake_frame do
    Map.merge(@handshake, %{
      "id" => "handshake",
      "action" => "handshake",
      "protocol_version" => Protocol.version()
    })
  end

  defp execute(bridge, id, parameters \\ %{}) do
    Bridge.request(bridge, %{
      "id" => id,
      "action" => "execute",
      "operation_id" => "operation",
      "parameters" => parameters
    })
  end
end
