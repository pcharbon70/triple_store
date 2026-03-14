ExUnit.start(exclude: [:benchmark, :large_dataset, :slow, :lifetime_safety])

case TripleStore.Test.DbPool.start_link() do
  {:ok, _pid} ->
    :ok

  {:error, {:already_started, _pid}} ->
    :ok

  {:error, _reason} ->
    IO.warn("DbPool not started: ErlangAdapter not yet implemented")
    IO.warn("Some tests may be skipped. This is expected during migration phases.")
end

case TripleStore.Integration.Helpers.start_link() do
  {:ok, _pid} ->
    :ok

  {:error, {:already_started, _pid}} ->
    :ok

  {:error, reason} ->
    IO.warn("Failed to start integration helpers: #{inspect(reason)}")
end

# Clean up orphaned test databases from previous runs (older than 24 hours)
case TripleStore.Integration.Helpers.cleanup_orphaned_databases(24) do
  {:ok, 0} ->
    :ok

  {:ok, count} ->
    IO.puts("Cleaned up #{count} orphaned test database(s) from previous runs")

  {:error, reason} ->
    IO.warn("Failed to cleanup orphaned databases: #{inspect(reason)}")
end
