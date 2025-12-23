defmodule TripleStore.TestHelpers do
  @moduledoc "Shared test utilities with portable tmpdir detection."

  def test_tmpdir do
    case System.get_env("TRIPLE_STORE_TEST_TMPDIR") do
      nil -> if File.dir?("/dev/shm"), do: "/dev/shm", else: System.tmp_dir!()
      dir -> dir
    end
  end

  def test_db_path(name) do
    Path.join(test_tmpdir(), "triple_store_#{name}_#{:erlang.unique_integer([:positive])}")
  end

  def cleanup_test_db(path), do: File.rm_rf(path)
end
