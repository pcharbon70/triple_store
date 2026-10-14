defmodule TripleStore.Benchmark.LDBC.Phase5BReadCorrectnessIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.Iterator
  alias TripleStore.Benchmark.LDBC.{SNB.Converter, StoreFixture}

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Parameters,
    Qualification,
    Workload
  }

  @moduletag :integration

  test "all BI variants execute with stable typed digests across reopen", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()

    assert {:ok, parameters} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    assert {:ok, first} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               timeout: 5_000
             )

    assert length(first.entries) == 31
    assert first.execution == %{pass: 31}
    assert first.comparisons == %{blocked: 31}
    refute first.qualified?
    assert Enum.all?(first.operations, & &1.execution_complete?)
    refute Enum.any?(first.operations, & &1.qualified?)

    assert Enum.count(first.entries, fn entry ->
             entry.comparison.reason == {:ineligible_reference_purpose, :contract_fixture}
           end) == 1

    assert Enum.count(first.entries, &(&1.comparison.reason == :accepted_answer_missing)) == 30

    assert Enum.all?(first.entries, fn entry ->
             entry.execution.status == :pass and
               String.starts_with?(entry.execution.digest, "sha256:") and
               is_list(entry.execution.columns) and
               is_list(entry.execution.types)
           end)

    assert run_digest(first) ==
             "sha256:f762b3cd7ad6593f0dc56b3537173703572100f651e77890be5dfcc5efa4b4a1"

    assert {:ok, closed} = StoreFixture.close(fixture)
    assert {:ok, reopened} = StoreFixture.reopen(closed)

    assert {:ok, second} =
             Qualification.run(definitions, reopened, parameters,
               reference_profile: :snb_bi_smoke,
               timeout: 5_000
             )

    assert execution_digests(second) == execution_digests(first)
    assert :ok = StoreFixture.teardown(reopened, delete: true)
  end

  test "execution and result-contract failures remain per-variant qualification evidence", %{
    test: test
  } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()

    assert {:ok, parameters} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    iterator_count = length(iterator_pids())

    timeout_context =
      fixture.store
      |> Map.put(:permit_all, true)
      |> Map.put(:operation_handlers, %{{1, "default"} => fn _, _, _ -> {:error, :timeout} end})

    assert {:ok, timeout_run} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               context: timeout_context
             )

    timeout_entry = find_entry(timeout_run, 1, "default")
    assert timeout_entry.execution == %{status: :fail, reason: :timeout}
    assert timeout_entry.comparison == %{status: :blocked, reason: :execution_failed}

    malformed_context =
      fixture.store
      |> Map.put(:permit_all, true)
      |> Map.put(:operation_handlers, %{{1, "default"} => fn _, _, _ -> {:ok, [%{}]} end})

    assert {:ok, malformed_run} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               context: malformed_context
             )

    malformed_entry = find_entry(malformed_run, 1, "default")
    assert malformed_entry.execution.status == :fail
    assert match?({:invalid_bi_result, 0, _reason}, malformed_entry.execution.reason)
    refute malformed_entry.qualified?

    assert {:ok, cancelled_run} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               cancelled?: fn -> true end
             )

    cancelled_entries =
      Enum.filter(cancelled_run.entries, &(Map.get(&1.execution, :reason) == :cancelled))

    assert cancelled_entries != []
    assert Enum.all?(cancelled_entries, &match?({:extension, _, _}, &1.strategy))
    assert length(iterator_pids()) == iterator_count

    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  defp execution_digests(run) do
    Map.new(run.entries, &{{&1.operation_id, &1.variant}, &1.execution.digest})
  end

  defp run_digest(run) do
    run
    |> execution_digests()
    |> Enum.sort()
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> then(&("sha256:" <> Base.encode16(&1, case: :lower)))
  end

  defp find_entry(run, number, variant) do
    Enum.find(run.entries, &(&1.number == number and &1.variant == variant))
  end

  defp iterator_pids do
    Enum.filter(Process.list(), &iterator_process?/1)
  end

  defp iterator_process?(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} ->
        match?({Iterator, _function, _arity}, Keyword.get(dictionary, :"$initial_call"))

      nil ->
        false
    end
  end

  defp parameter_sequences(definitions) do
    Map.new(definitions, fn definition ->
      row = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})
      {variant_label(definition), [row]}
    end)
  end

  defp parameter_value("ID"), do: 1
  defp parameter_value("32-bit Integer"), do: 1
  defp parameter_value("64-bit Integer"), do: 1
  defp parameter_value("32-bit Float"), do: 1.0
  defp parameter_value("Boolean"), do: true
  defp parameter_value("Date"), do: "2012-11-29"
  defp parameter_value("DateTime"), do: "2012-11-29T00:00:00Z"
  defp parameter_value(type) when type in ["String", "Long String"], do: "value"
  defp parameter_value("\\{String\\}"), do: ["value"]

  defp variant_label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_dir(test) do
    Path.join(
      System.tmp_dir!(),
      "phase_5b_read_correctness_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
    )
  end
end
