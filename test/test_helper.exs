ExUnit.start()

# Start the pool
{:ok, _} = TripleStore.Test.DbPool.start_link()
