defmodule TripleStore.Benchmark.LDBC.Phase3CommonRunnerIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter

  alias TripleStore.Benchmark.LDBC.{
    Artifacts,
    Bridge,
    Correctness,
    Measurement,
    OperationRegistry,
    Result,
    Runtime
  }

  alias TripleStore.Benchmark.LDBC.Bridge.{PortOwner, Protocol}
  alias TripleStore.QuadOperations

  @moduletag :integration
  @handshake %{
    "profile_id" => "phase-3-smoke",
    "dataset_manifest" => "dataset-sha256",
    "operation_catalog" => "catalog-sha256"
  }

  test "external framed driver executes all suites, a write, reset, health, and shutdown" do
    root = temp_root("external")
    on_exit(fn -> File.rm_rf(root) end)
    assert {:ok, runtime} = Runtime.start_link(path: Path.join(root, "store"), schema: :quad)
    assert :ok = seed_store(runtime)

    operation_ids = [
      "ldbc/spb/aggregation-01@v2.0.2",
      "ldbc/snb-bi/read-01@v1.0.3",
      "ldbc/snb-interactive/complex-read-01@v1.2.0"
    ]

    handler = handler(runtime, operation_ids)

    assert {:ok, bridge} =
             Bridge.start_link(benchmark_mode: true, handshake: @handshake, handler: handler)

    script = driver_script(operation_ids)

    assert {:ok, owner} =
             PortOwner.start_link(
               bridge: bridge,
               executable: System.find_executable("python3"),
               arguments: ["-u", "-c", script],
               owner: self()
             )

    monitor = Process.monitor(owner)

    assert_receive {:ldbc_driver_frame, %{"id" => "handshake", "status" => "success"}}, 5_000

    for id <- operation_ids do
      assert_receive {:ldbc_driver_frame, %{"id" => ^id, "status" => "success"}}, 5_000
    end

    assert_receive {:ldbc_driver_frame, %{"id" => "write", "status" => "success"}}, 5_000
    assert_receive {:ldbc_driver_frame, %{"id" => "reset", "status" => "success"}}, 5_000
    assert_receive {:ldbc_driver_frame, %{"id" => "health", "status" => "success"}}, 5_000
    assert_receive {:ldbc_driver_frame, %{"id" => "shutdown", "status" => "success"}}, 5_000
    assert_receive {:ldbc_driver_exit, 0}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, 5_000

    assert {:ok, true} = Runtime.run(runtime, &{:ok, store_has_data?(&1)})
    assert {:ok, %{store_close: :ok, outstanding_resources: []}} = Runtime.shutdown(runtime)
  end

  test "malformed and crashed drivers leave the embedded runtime recoverable" do
    root = temp_root("failure")
    on_exit(fn -> File.rm_rf(root) end)
    assert {:ok, runtime} = Runtime.start_link(path: Path.join(root, "store"), schema: :quad)
    assert :ok = seed_store(runtime)

    assert {:ok, bridge} =
             Bridge.start_link(
               benchmark_mode: true,
               handshake: @handshake,
               handler: handler(runtime, [])
             )

    malformed_script = """
    import sys, struct
    payload = b'{bad json'
    sys.stdout.buffer.write(struct.pack('>I', len(payload)) + payload)
    sys.stdout.buffer.flush()
    header = sys.stdin.buffer.read(4)
    size = struct.unpack('>I', header)[0]
    sys.stdin.buffer.read(size)
    """

    assert {:ok, malformed_owner} =
             PortOwner.start_link(
               bridge: bridge,
               executable: System.find_executable("python3"),
               arguments: ["-u", "-c", malformed_script],
               owner: self()
             )

    malformed_monitor = Process.monitor(malformed_owner)
    assert_receive {:ldbc_driver_frame, %{"status" => "error"}}, 5_000
    assert_receive {:ldbc_driver_exit, 0}, 5_000
    assert_receive {:DOWN, ^malformed_monitor, :process, ^malformed_owner, :normal}, 5_000

    Process.flag(:trap_exit, true)

    assert {:ok, crash_owner} =
             PortOwner.start_link(
               bridge: bridge,
               executable: System.find_executable("python3"),
               arguments: ["-u", "-c", "import os; os._exit(7)"],
               owner: self()
             )

    assert_receive {:ldbc_driver_exit, 7}, 5_000
    assert_receive {:EXIT, ^crash_owner, {:driver_exit, 7}}, 5_000
    assert {:ok, true} = Runtime.run(runtime, &{:ok, store_has_data?(&1)})
    assert {:ok, _accounting} = Runtime.shutdown(runtime)
  end

  test "correctness, measurements, artifacts, and explicit baseline form one fail-closed gate" do
    root = temp_root("artifacts")
    on_exit(fn -> File.rm_rf(root) end)

    expected = %Result{
      columns: ["id"],
      types: [:id],
      rows: [[1], [2]],
      ordering: %{mode: :ordered}
    }

    wrong = %{expected | rows: [[2], [1]]}
    correctness = Correctness.compare(expected, wrong)

    records = [
      record("good", :success, 10, true),
      record("wrong", :error, nil, false)
    ]

    summary = Measurement.summarize(records)
    refute summary.score_eligible?

    assert {:ok, artifact} =
             Artifacts.write(root, %{
               manifest: %{run_id: "integration"},
               environment: %{test: true},
               catalog: [],
               raw_samples: records,
               errors: [%{operation_id: "wrong", reason: :incorrect}],
               correctness: [correctness],
               resources: %{},
               summary: summary,
               gates: %{
                 profile: true,
                 correctness: false,
                 scheduling: true,
                 duration: true,
                 completeness: true
               },
               official_score: 99
             })

    json = artifact.paths.summary |> File.read!() |> Jason.decode!()
    refute Map.has_key?(json, "official_score")
    assert File.read!(artifact.paths.samples_csv) =~ "good,measured,success"
    assert File.read!(artifact.paths.report_markdown) =~ "Invalid samples: 1"
  end

  test "registry checksum and protocol output are stable across repeated lifecycles" do
    assert {:ok, first} = OperationRegistry.load()
    assert {:ok, second} = OperationRegistry.load()
    assert OperationRegistry.checksum(first) == OperationRegistry.checksum(second)

    frame = %{"id" => "health", "action" => "health"}
    assert {:ok, encoded} = Protocol.encode(frame, 1_024)
    assert {:ok, ^frame} = Protocol.decode(encoded, 1_024)

    refute Enum.any?(first, fn operation ->
             match?({:native, TripleStore.Benchmark.Runner, _function}, operation.strategy)
           end)
  end

  defp handler(runtime, operation_ids) do
    fn
      :handshake, _frame ->
        {:ok, %{protocol_version: Protocol.version()}}

      :execute, %{"operation_id" => operation_id} ->
        if operation_id in operation_ids do
          Runtime.run(runtime, &{:ok, store_has_data?(&1)})
        else
          {:error, {:validation, :unknown_operation}}
        end

      :batch, _frame ->
        {:ok, %{written: 1}}

      :reset, _frame ->
        Runtime.run(runtime, &{:ok, store_has_data?(&1)})

      :health, _frame ->
        {:ok, %{status: :healthy}}

      :shutdown, _frame ->
        {:ok, %{status: :stopping}}

      _action, _frame ->
        {:error, {:bridge, :unsupported}}
    end
  end

  defp seed_store(runtime) do
    Runtime.run(runtime, fn store ->
      quad =
        {RDF.iri("urn:subject"), RDF.iri("urn:predicate"), RDF.literal("object"),
         RDF.iri("urn:graph")}

      with {:ok, encoded} <- Adapter.from_rdf_quads(store.dict_manager, [quad]) do
        QuadOperations.insert_quads(store.db, encoded, sync: true)
      end
    end)
  end

  defp store_has_data?(store) do
    ErlangAdapter.fold_keys(store.db, :gspo, <<>>, false, fn _key, _acc -> true end,
      fill_cache: false
    )
  end

  defp driver_script(operation_ids) do
    requests =
      [
        Map.merge(@handshake, %{
          "id" => "handshake",
          "action" => "handshake",
          "protocol_version" => Protocol.version()
        })
      ] ++
        Enum.map(
          operation_ids,
          &%{"id" => &1, "action" => "execute", "operation_id" => &1, "parameters" => %{}}
        ) ++
        [
          %{"id" => "write", "action" => "batch", "operations" => [%{"kind" => "insert"}]},
          %{"id" => "reset", "action" => "reset"},
          %{"id" => "health", "action" => "health"},
          %{"id" => "shutdown", "action" => "shutdown"}
        ]

    encoded = Jason.encode!(requests)

    """
    import json, struct, sys
    requests = json.loads(#{inspect(encoded)})
    for request in requests:
        payload = json.dumps(request, separators=(',', ':')).encode('utf-8')
        sys.stdout.buffer.write(struct.pack('>I', len(payload)) + payload)
        sys.stdout.buffer.flush()
        header = sys.stdin.buffer.read(4)
        if len(header) != 4:
            raise SystemExit(2)
        size = struct.unpack('>I', header)[0]
        response = json.loads(sys.stdin.buffer.read(size))
        if response.get('id') != request['id'] or response.get('status') != 'success':
            raise SystemExit(3)
    """
  end

  defp record(id, status, sample_us, eligible?) do
    %{
      operation_id: id,
      mode: :measured,
      status: status,
      result: if(eligible?, do: {:ok, :correct}, else: {:error, :incorrect}),
      total_us: sample_us || 20,
      sample_us: sample_us,
      score_eligible?: eligible?
    }
  end

  defp temp_root(name),
    do: Path.join(System.tmp_dir!(), "ldbc-phase3-#{name}-#{System.unique_integer([:positive])}")
end
