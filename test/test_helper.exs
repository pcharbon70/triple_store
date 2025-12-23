ExUnit.start(exclude: [:benchmark, :large_dataset])

# Start the pool
{:ok, _} = TripleStore.Test.DbPool.start_link()
