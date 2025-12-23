defmodule TripleStore.PooledDbCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias TripleStore.Test.DbPool

  using do
    quote do
      use ExUnit.Case, async: true
      alias TripleStore.Backend.RocksDB.NIF
    end
  end

  setup do
    db_info = DbPool.checkout()
    on_exit(fn -> DbPool.checkin(db_info) end)
    {:ok, db: db_info.db, db_path: db_info.path}
  end
end
