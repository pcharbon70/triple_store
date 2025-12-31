defmodule TripleStore.Backend.RocksDB.CrashHarnessTest do
  @moduledoc """
  Subprocess crash-harness tests for NIF lifetime safety.

  These run critical sequences in a separate BEAM so a NIF crash
  won't bring down the main test runner. Uses `mix run --no-compile`
  to avoid triggering recompilation in the subprocess.
  """

  use ExUnit.Case, async: false

  defp run_script!(script) do
    # Use mix run with --no-compile to avoid triggering recompilation
    # This is safe since we're running in a subprocess
    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-compile", "-e", script],
        stderr_to_stdout: true,
        cd: File.cwd!(),
        env: [{"MIX_ENV", "test"}]
      )

    {output, status}
  end

  @tag :slow
  test "iterator remains usable after db close (subprocess)" do
    script = """
    alias TripleStore.Backend.RocksDB.NIF
    path = System.tmp_dir!() <> "/ts_crash_iter_" <> Integer.to_string(System.unique_integer([:positive]))
    {:ok, db} = NIF.open(path)
    :ok = NIF.put(db, :spo, "key1", "value1")
    :ok = NIF.put(db, :spo, "key2", "value2")
    {:ok, iter} = NIF.prefix_iterator(db, :spo, "key")
    :ok = NIF.close(db)
    IO.inspect(NIF.iterator_next(iter), label: "next")
    IO.inspect(NIF.iterator_next(iter), label: "next")
    IO.inspect(NIF.iterator_next(iter), label: "next")
    :ok = NIF.iterator_close(iter)
    File.rm_rf(path)
    """

    {output, status} = run_script!(script)
    assert status == 0, "Script failed with: #{output}"
    assert output =~ "next: {:ok, \"key1\", \"value1\"}"
  end

  @tag :slow
  test "snapshot remains usable after db close (subprocess)" do
    script = """
    alias TripleStore.Backend.RocksDB.NIF
    path = System.tmp_dir!() <> "/ts_crash_snap_" <> Integer.to_string(System.unique_integer([:positive]))
    {:ok, db} = NIF.open(path)
    :ok = NIF.put(db, :spo, "key1", "value1")
    {:ok, snap} = NIF.snapshot(db)
    :ok = NIF.put(db, :spo, "key1", "value2")
    :ok = NIF.close(db)
    IO.inspect(NIF.snapshot_get(snap, :spo, "key1"), label: "snap")
    :ok = NIF.release_snapshot(snap)
    File.rm_rf(path)
    """

    {output, status} = run_script!(script)
    assert status == 0, "Script failed with: #{output}"
    assert output =~ "snap: {:ok, \"value1\"}"
  end
end
