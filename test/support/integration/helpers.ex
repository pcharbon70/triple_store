defmodule TripleStore.Integration.Helpers do
  @moduledoc """
  Shared helper functions for integration tests.

  This module provides common utilities used across multiple integration test files
  to reduce code duplication and ensure consistency.

  ## Features

  - Path generation for unique temporary test databases
  - Path cleanup utilities
  - Graph permission helpers
  - Automated cleanup of orphaned test databases
  - Test case setup helpers

  ## Usage

  In your test file, add wrapper functions that delegate to this module:

      defp unique_path, do: TripleStore.Integration.Helpers.unique_path("my_test")
      defp cleanup_path(path), do: TripleStore.Integration.Helpers.cleanup_path(path)
  """

  use Agent

  @doc """
  Starts the path tracker for cleanup automation.

  This should be called in a test_helper.exs or setup block.
  """
  def start_link(_opts \\ []) do
    Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)
  end

  @doc """
  Registers a path for automatic cleanup on test suite exit.
  """
  def register_path(path) do
    if Process.whereis(__MODULE__) do
      Agent.update(__MODULE__, fn paths -> MapSet.put(paths, path) end)
    end

    :ok
  end

  @doc """
  Cleans up all registered test database paths.

  This is called automatically by the test helper if cleanup automation is enabled.
  """
  def cleanup_all_registered do
    if Process.whereis(__MODULE__) do
      paths = Agent.get(__MODULE__, fn paths -> paths end)
      Enum.each(paths, &cleanup_path/1)
      Agent.update(__MODULE__, fn _ -> MapSet.new() end)
    end

    :ok
  end

  @doc """
  Cleans up orphaned test databases from previous test runs.

  Finds and removes directories matching the test database prefix pattern
  that are older than 24 hours.

  ## Examples

      iex> TripleStore.Integration.Helpers.cleanup_orphaned_databases()
      {:ok, 3}  # Cleaned up 3 orphaned databases
  """
  def cleanup_orphaned_databases(max_age_hours \\ 24) do
    cutoff_time = System.system_time(:second) - max_age_hours * 3600

    "/tmp"
    |> File.ls()
    |> case do
      {:ok, files} -> files
      _ -> []
    end
    |> Enum.filter(&test_database_path?/1)
    |> Enum.filter(fn path ->
      full_path = Path.join(["/tmp", path])

      case File.stat(full_path) do
        {:ok, %{mtime: mtime}} -> mtime < cutoff_time
        _ -> false
      end
    end)
    |> Enum.reduce({:ok, 0}, fn path, {:ok, count} ->
      full_path = Path.join(["/tmp", path])

      case File.rm_rf(full_path) do
        {:ok, _} -> {:ok, count + 1}
        _ -> {:ok, count}
      end
    end)
  end

  @doc """
  Generates a unique path for temporary test databases.

  Uses system time and random components to ensure uniqueness across test runs.
  Automatically registers the path for cleanup if the tracker is running.

  ## Examples

      iex> TripleStore.Integration.Helpers.unique_path()
      "/tmp/test_db_1234567890_123456"

      iex> TripleStore.Integration.Helpers.unique_path("my_test")
      "/tmp/my_test_1234567890_123456"
  """
  def unique_path(prefix \\ "test_db") do
    time_component = System.system_time(:microsecond)
    rand_component = :rand.uniform(1_000_000)
    path = "/tmp/#{prefix}_#{time_component}_#{rand_component}"
    register_path(path)
    path
  end

  @doc """
  Cleans up a temporary test database path.

  Removes the directory and all its contents.

  ## Examples

      iex> TripleStore.Integration.Helpers.cleanup_path("/tmp/test_db_123")
      :ok
  """
  def cleanup_path(path) do
    File.rm_rf(path)
  end

  @doc """
  Grants public read permissions to a list of graphs for testing.

  ## Examples

      ctx = %{db: db, dict_manager: manager}
      :ok = TripleStore.Integration.Helpers.grant_public_permissions(ctx, [
        "http://example.org/graph1",
        "http://example.org/graph2"
      ])
  """
  def grant_public_permissions(ctx, graph_iris) when is_list(graph_iris) do
    alias TripleStore.SPARQL.Authorization

    Enum.each(graph_iris, fn graph_iri ->
      Authorization.set_public(ctx, graph_iri)
    end)
  end

  @doc """
  Safely stops a GenServer process, ignoring exit errors.

  ## Examples

      iex> TripleStore.Integration.Helpers.safe_stop(manager)
      :ok
  """
  def safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 1000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  def safe_stop(_), do: :ok

  @doc """
  Safely closes a database, ignoring exit errors.

  ## Examples

      iex> TripleStore.Integration.Helpers.safe_close_db(db)
      :ok
  """
  def safe_close_db(db) do
    try do
      TripleStore.Backend.RocksDB.ErlangAdapter.close(db)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  # Private helpers

  defp test_database_path?(filename) do
    String.starts_with?(filename, "test_db_") or
      String.starts_with?(filename, "trig_loading_test_") or
      String.starts_with?(filename, "nquads_loading_test_") or
      String.starts_with?(filename, "sparql_graph_test_") or
      String.starts_with?(filename, "quad_delete_test_") or
      String.starts_with?(filename, "quad_insert_lookup_test_") or
      String.starts_with?(filename, "database_lifecycle_test_") or
      String.starts_with?(filename, "quad_storage_lifecycle_test_") or
      String.starts_with?(filename, "graph_clause_query_test_") or
      String.starts_with?(filename, "format_conversion_test_") or
      String.starts_with?(filename, "roundtrip_test_") or
      String.starts_with?(filename, "real_world_scenarios_test_") or
      String.starts_with?(filename, "update_operations_test_") or
      String.starts_with?(filename, "quad_benchmark_test_") or
      String.starts_with?(filename, "migration_test_") or
      String.starts_with?(filename, "concurrency_test_") or
      String.starts_with?(filename, "error_handling_test_")
  end
end
