defmodule TripleStore.Index.TestHelper do
  @moduledoc """
  Test helper functions for Triple Index Layer tests.

  Provides common setup, teardown, and assertion utilities to reduce
  boilerplate across index test files.
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Test.DbPool

  @doc """
  Checks out a pooled test database.

  ## Arguments

  - `base_name` - Base name for identification (unused with pooled databases)

  ## Returns

  A map containing the database reference, filesystem path, and pool id.

  ## Example

      db_info = IndexTestHelper.setup_test_db("my_test")
  """
  @spec setup_test_db(String.t()) :: %{db: NIF.db_ref(), path: String.t(), id: integer()}
  def setup_test_db(_base_name) do
    DbPool.checkout()
  end

  @doc """
  Returns a pooled test database to the pool.

  ## Arguments

  - `db_info` - Database info map from `setup_test_db/1`

  ## Example

      IndexTestHelper.cleanup_test_db(db_info)
  """
  @spec cleanup_test_db(%{db: NIF.db_ref(), path: String.t(), id: integer()}) :: :ok
  def cleanup_test_db(db_info) do
    DbPool.checkin(db_info)
    :ok
  end

  @doc """
  Asserts that a triple exists in all three indices (SPO, POS, OSP).

  This verifies atomicity of insert operations by checking that the
  triple is present in all indices.

  ## Arguments

  - `db` - Database reference
  - `triple` - The triple `{s, p, o}` to check

  ## Example

      assert_triple_in_all_indices(db, {1, 2, 3})
  """
  @spec assert_triple_in_all_indices(NIF.db_ref(), Index.triple()) :: :ok
  def assert_triple_in_all_indices(db, {s, p, o}) do
    import ExUnit.Assertions

    spo_key = Index.spo_key(s, p, o)
    pos_key = Index.pos_key(p, o, s)
    osp_key = Index.osp_key(o, s, p)

    assert {:ok, <<>>} = NIF.get(db, :spo, spo_key),
           "Triple {#{s}, #{p}, #{o}} not found in SPO index"

    assert {:ok, <<>>} = NIF.get(db, :pos, pos_key),
           "Triple {#{s}, #{p}, #{o}} not found in POS index"

    assert {:ok, <<>>} = NIF.get(db, :osp, osp_key),
           "Triple {#{s}, #{p}, #{o}} not found in OSP index"

    :ok
  end

  @doc """
  Asserts that a triple does NOT exist in any index.

  This verifies atomicity of delete operations by checking that the
  triple is absent from all indices.

  ## Arguments

  - `db` - Database reference
  - `triple` - The triple `{s, p, o}` to check

  ## Example

      assert_triple_not_in_any_index(db, {1, 2, 3})
  """
  @spec assert_triple_not_in_any_index(NIF.db_ref(), Index.triple()) :: :ok
  def assert_triple_not_in_any_index(db, {s, p, o}) do
    import ExUnit.Assertions

    spo_key = Index.spo_key(s, p, o)
    pos_key = Index.pos_key(p, o, s)
    osp_key = Index.osp_key(o, s, p)

    assert {:ok, nil} = NIF.get(db, :spo, spo_key),
           "Triple {#{s}, #{p}, #{o}} unexpectedly found in SPO index"

    assert {:ok, nil} = NIF.get(db, :pos, pos_key),
           "Triple {#{s}, #{p}, #{o}} unexpectedly found in POS index"

    assert {:ok, nil} = NIF.get(db, :osp, osp_key),
           "Triple {#{s}, #{p}, #{o}} unexpectedly found in OSP index"

    :ok
  end

  @doc """
  Generates a list of sample triples for testing.

  ## Arguments

  - `count` - Number of triples to generate
  - `opts` - Options
    - `:subject_offset` - Starting subject ID (default: 1)
    - `:predicate_offset` - Starting predicate ID (default: 1000)
    - `:object_offset` - Starting object ID (default: 2000)

  ## Returns

  List of `{subject, predicate, object}` tuples.

  ## Example

      triples = generate_triples(10)
      # => [{1, 1000, 2000}, {2, 1001, 2001}, ...]
  """
  @spec generate_triples(non_neg_integer(), keyword()) :: [Index.triple()]
  def generate_triples(count, opts \\ []) do
    subject_offset = Keyword.get(opts, :subject_offset, 1)
    predicate_offset = Keyword.get(opts, :predicate_offset, 1000)
    object_offset = Keyword.get(opts, :object_offset, 2000)

    for i <- 0..(count - 1) do
      {subject_offset + i, predicate_offset + i, object_offset + i}
    end
  end

  @doc """
  Asserts that triples are in lexicographic order based on index type.

  ## Arguments

  - `triples` - List of triples to check
  - `index_type` - The index type (`:spo`, `:pos`, or `:osp`)

  ## Example

      assert_lexicographic_order([{1, 2, 3}, {1, 2, 4}, {1, 3, 5}], :spo)
  """
  @spec assert_lexicographic_order([Index.triple()], :spo | :pos | :osp) :: :ok
  def assert_lexicographic_order(triples, index_type) do
    import ExUnit.Assertions

    keys =
      Enum.map(triples, fn {s, p, o} ->
        case index_type do
          :spo -> Index.spo_key(s, p, o)
          :pos -> Index.pos_key(p, o, s)
          :osp -> Index.osp_key(o, s, p)
        end
      end)

    sorted_keys = Enum.sort(keys)
    assert keys == sorted_keys, "Triples are not in lexicographic order for #{index_type} index"
    :ok
  end

  @doc """
  Creates an ExUnit setup block for index tests.

  This macro creates the common setup pattern used across index tests,
  including database creation and cleanup.

  ## Usage

      use TripleStore.Index.TestHelper

      # This creates a setup block that provides %{db: db, path: path}
  """
  defmacro __using__(_opts) do
    quote do
      alias TripleStore.Backend.RocksDB.NIF
      alias TripleStore.Index
      alias TripleStore.Index.TestHelper, as: IndexTestHelper

      @test_db_base "/tmp/triple_store_index_test"

      setup do
        db_info = IndexTestHelper.setup_test_db("test")

        on_exit(fn ->
          IndexTestHelper.cleanup_test_db(db_info)
        end)

        {:ok, db: db_info.db, path: db_info.path}
      end
    end
  end
end
