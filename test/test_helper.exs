ExUnit.start(exclude: [:benchmark, :large_dataset, :slow])

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
