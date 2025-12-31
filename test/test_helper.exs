ExUnit.start(exclude: [:benchmark, :large_dataset, :slow])

# Start the pool
{:ok, _} = TripleStore.Test.DbPool.start_link()
