defmodule TripleStore.Benchmark.LDBC.ExecutionAndMeasurementTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.{Environment, Execution, Measurement, Operation}

  test "records every exposed phase and admits only fully validated measured samples" do
    operation = operation()

    phases = %{
      setup: fn _ -> {:ok, :input} end,
      parse: fn :input -> {:ok, :parsed} end,
      plan: fn :parsed -> {:ok, :planned} end,
      execute: fn :planned -> {:ok, Stream.map(1..3, &(&1 * 2))} end,
      validation: fn [2, 4, 6] -> {:ok, :correct} end
    }

    record = Execution.run(operation, phases: phases, teardown: fn _ -> :ok end)
    assert record.status == :success
    assert record.result == {:ok, :correct}
    assert is_integer(record.sample_us)
    assert record.score_eligible?

    assert Enum.sort(Map.keys(record.timings_us)) ==
             Enum.sort([:setup, :parse, :plan, :execute, :materialize, :validation, :teardown])
  end

  test "times out lazy materialization and always tears down" do
    parent = self()

    slow_stream =
      Stream.map(1..2, fn number ->
        Process.sleep(50)
        number
      end)

    record =
      Execution.run(operation(),
        phases: %{
          execute: fn _ -> {:ok, slow_stream} end,
          validation: fn _ -> :ok end
        },
        phase_timeouts: %{materialize: 10},
        teardown: fn _ ->
          send(parent, :torn_down)
          :ok
        end
      )

    assert record.status == :error
    assert {:error, %{class: :timeout, phase: :materialize}} = record.result
    assert record.sample_us == nil
    refute record.score_eligible?
    assert_receive :torn_down
  end

  test "separates warmup and invalid records and gates the score" do
    successful = record("read-1", :measured, true, 100)
    warmup = record("read-1", :warmup, true, nil)
    failed = record("read-2", :measured, false, nil)

    summary = Measurement.summarize([successful, warmup, failed])
    assert summary.measured_count == 2
    assert summary.warmup_count == 1
    assert summary.valid_sample_count == 1
    assert summary.invalid_sample_count == 1
    refute summary.score_eligible?
    assert summary.operations["read-1"].p95_us == 100
  end

  test "captures reproducibility, engine, driver, and resource state" do
    environment =
      Environment.capture(
        engine: [schema: :quad, cache: :cold, reasoning: :disabled],
        driver: [concurrency: 4, warmup: 2, seed: 42, scale_factor: 0.1]
      )

    assert is_binary(environment.captured_at)
    assert is_binary(environment.source.git_sha)
    assert environment.runtime.elixir == System.version()
    assert environment.engine.schema == :quad
    assert environment.driver.concurrency == 4
    assert is_integer(environment.resources.beam_memory_bytes)
  end

  defp operation do
    %Operation{
      id: "test/read@v1",
      suite: :spb,
      profile_id: "test",
      catalog_id: "test/read@v1",
      upstream_id: "read",
      kind: :read,
      parameter_schema: [],
      result_schema: [],
      ordering: %{mode: :unordered},
      limit: nil,
      timeout_class: :short,
      tags: [],
      strategy: {:native, __MODULE__, :execute},
      source: %{source_id: "test", path: "test", checksum: "test"},
      transformation_version: "v1"
    }
  end

  defp record(id, mode, eligible, sample) do
    %{
      operation_id: id,
      mode: mode,
      status: if(eligible, do: :success, else: :error),
      result: if(eligible, do: {:ok, :correct}, else: {:error, :wrong}),
      score_eligible?: eligible and mode == :measured,
      sample_us: sample
    }
  end
end
