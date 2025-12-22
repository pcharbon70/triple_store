defmodule TripleStore.RdfIntegrationTestHelper do
  @moduledoc """
  Shared test setup and helpers for RDF.ex integration tests.

  Provides common setup for loader and exporter tests, including
  database initialization, manager setup, and cleanup.

  ## Usage

      use TripleStore.RdfIntegrationTestHelper

  This will import the helper functions and set up a common setup
  block that provides `db`, `manager`, and `path` in the test context.
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader

  @doc """
  Macro to set up RDF integration tests with common database setup.

  Provides:
  - `:db` - RocksDB database reference
  - `:manager` - Dictionary manager process
  - `:path` - Temporary database path
  """
  defmacro __using__(_opts) do
    quote do
      import TripleStore.RdfIntegrationTestHelper

      setup do
        TripleStore.RdfIntegrationTestHelper.setup_test_db()
      end
    end
  end

  @doc """
  Sets up a temporary test database with manager.

  Returns a map with `:db`, `:manager`, and `:path` keys.
  Registers cleanup to run on test exit.
  """
  @spec setup_test_db() :: {:ok, map()}
  def setup_test_db do
    test_path = "/tmp/triple_store_rdf_test_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    ExUnit.Callbacks.on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  @doc """
  Loads test triples into the database.

  ## Arguments

  - `db` - Database reference
  - `manager` - Dictionary manager
  - `triples` - List of RDF triples to load

  ## Returns

  `:ok` on success
  """
  @spec load_test_triples(reference(), pid(), [RDF.Triple.t()]) :: :ok
  def load_test_triples(db, manager, triples) do
    graph = RDF.Graph.new(triples)
    {:ok, _} = Loader.load_graph(db, manager, graph)
    :ok
  end

  @doc """
  Creates a sample RDF graph with the specified number of triples.

  ## Arguments

  - `count` - Number of triples to create

  ## Returns

  An `RDF.Graph` with the specified number of triples
  """
  @spec sample_graph(pos_integer()) :: RDF.Graph.t()
  def sample_graph(count) do
    triples =
      for i <- 1..count do
        {
          RDF.iri("http://example.org/s#{i}"),
          RDF.iri("http://example.org/p"),
          RDF.literal("value#{i}")
        }
      end

    RDF.Graph.new(triples)
  end

  @doc """
  Creates a temporary file with the given content.

  ## Arguments

  - `content` - File content
  - `extension` - File extension (e.g., ".ttl")

  ## Returns

  Path to the temporary file. File is automatically cleaned up on test exit.
  """
  @spec create_temp_file(String.t(), String.t()) :: String.t()
  def create_temp_file(content, extension) do
    path = "/tmp/triple_store_test_#{:erlang.unique_integer([:positive])}#{extension}"
    File.write!(path, content)

    ExUnit.Callbacks.on_exit(fn ->
      File.rm(path)
    end)

    path
  end
end
