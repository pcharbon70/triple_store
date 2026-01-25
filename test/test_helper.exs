ExUnit.start(exclude: [:benchmark, :large_dataset, :slow, :lifetime_safety])

# Start the pool (skip if NIF is not yet implemented)
# Spawn without link to avoid crashes propagating
spawn(fn ->
  case TripleStore.Test.DbPool.start_link() do
    {:ok, _} ->
      :ok

    {:error, _} ->
      IO.warn("DbPool not started: NIF not yet implemented")
      IO.warn("Some tests may be skipped. This is expected during migration phases.")
  end
end)

# Give the pool a moment to start (or fail)
Process.sleep(100)

# Start the integration test helpers for cleanup automation
spawn(fn ->
  TripleStore.Integration.Helpers.start_link()

  # Clean up orphaned test databases from previous runs (older than 24 hours)
  case TripleStore.Integration.Helpers.cleanup_orphaned_databases(24) do
    {:ok, 0} ->
      :ok

    {:ok, count} ->
      IO.puts("Cleaned up #{count} orphaned test database(s) from previous runs")

    {:error, reason} ->
      IO.warn("Failed to cleanup orphaned databases: #{inspect(reason)}")
  end
end)
