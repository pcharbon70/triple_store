defmodule TripleStore.Test.Fixtures do
  @moduledoc """
  Test fixtures and helpers for TripleStore tests.

  Provides common test data and utility functions for testing
  the triple store components.
  """

  @doc """
  Returns a temporary directory path for test databases.
  The directory is cleaned up after the test.
  """
  def tmp_db_path do
    path =
      Path.join(System.tmp_dir!(), "triple_store_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(path)
    path
  end

  @doc """
  Cleans up a test database directory.
  """
  def cleanup_db(path) do
    File.rm_rf!(path)
  end
end
