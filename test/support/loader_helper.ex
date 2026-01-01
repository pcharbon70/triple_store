defmodule TripleStore.Test.LoaderHelper do
  @moduledoc """
  Shared test helpers for Loader tests.

  Provides common setup/cleanup functions and test data generators
  to reduce code duplication across loader test files.
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager

  @doc """
  Sets up a test database with a dictionary manager.

  Returns `{db, manager, test_path}` tuple.

  ## Arguments

  - `test_base` - Base path for test database (e.g., "/tmp/my_test")
  - `suffix` - Unique suffix for this test

  ## Example

      {db, manager, path} = LoaderHelper.setup_test_db("/tmp/loader_test", "parallel")
  """
  @spec setup_test_db(String.t(), String.t()) :: {reference(), pid(), String.t()}
  def setup_test_db(test_base, suffix) do
    test_path = "#{test_base}_#{suffix}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager, test_path}
  end

  @doc """
  Cleans up a test database.

  Stops the manager, closes the database, and removes the test directory.

  ## Arguments

  - `manager` - Dictionary manager process
  - `db` - Database reference
  - `test_path` - Path to test database directory
  """
  @spec cleanup_test_db(pid(), reference(), String.t()) :: :ok
  def cleanup_test_db(manager, db, test_path) do
    if Process.alive?(manager), do: Manager.stop(manager)
    NIF.close(db)
    File.rm_rf(test_path)
    :ok
  end

  @doc """
  Creates a test RDF graph with the specified number of triples.

  Each triple has the form:
  - Subject: `<http://example.org/subject/{i}>`
  - Predicate: `<http://example.org/predicate>`
  - Object: `"value_{i}"`

  ## Arguments

  - `count` - Number of triples to create

  ## Example

      graph = LoaderHelper.create_test_graph(100)
      assert RDF.Graph.triple_count(graph) == 100
  """
  @spec create_test_graph(non_neg_integer()) :: RDF.Graph.t()
  def create_test_graph(count) do
    1..count
    |> Enum.map(fn i ->
      {RDF.iri("http://example.org/subject/#{i}"),
       RDF.iri("http://example.org/predicate"),
       RDF.literal("value_#{i}")}
    end)
    |> RDF.Graph.new()
  end

  @doc """
  Creates a list of test RDF triples.

  Similar to `create_test_graph/1` but returns a list instead of an RDF.Graph.

  ## Arguments

  - `count` - Number of triples to create
  """
  @spec create_test_triples(non_neg_integer()) :: [RDF.Triple.t()]
  def create_test_triples(count) do
    1..count
    |> Enum.map(fn i ->
      {RDF.iri("http://example.org/s/#{i}"),
       RDF.iri("http://example.org/p"),
       RDF.literal("v#{i}")}
    end)
  end

  @doc """
  Sets up a telemetry handler that sends events to the test process.

  Returns the handler ID for later cleanup.

  ## Arguments

  - `event` - Telemetry event path (e.g., `[:triple_store, :loader, :batch]`)
  - `handler_id` - Unique ID for the handler

  ## Example

      handler_id = "test-batch-handler"
      LoaderHelper.setup_telemetry_handler([:triple_store, :loader, :batch], handler_id)
      # ... run test ...
      :telemetry.detach(handler_id)
  """
  @spec setup_telemetry_handler([atom()], String.t()) :: :ok
  def setup_telemetry_handler(event, handler_id) do
    test_pid = self()

    handler = fn event_name, measurements, metadata, _config ->
      send(test_pid, {:telemetry, event_name, measurements, metadata})
    end

    :telemetry.attach(handler_id, event, handler, nil)
  end

  @doc """
  Sets up telemetry handlers for multiple events.

  Returns a list of handler IDs for cleanup.

  ## Arguments

  - `events` - List of telemetry event paths
  - `handler_prefix` - Prefix for handler IDs

  ## Example

      events = [
        [:triple_store, :loader, :start],
        [:triple_store, :loader, :batch],
        [:triple_store, :loader, :stop]
      ]
      handlers = LoaderHelper.setup_telemetry_handlers(events, "test")
      # ... run test ...
      Enum.each(handlers, &:telemetry.detach/1)
  """
  @spec setup_telemetry_handlers([[atom()]], String.t()) :: [String.t()]
  def setup_telemetry_handlers(events, handler_prefix) do
    events
    |> Enum.with_index()
    |> Enum.map(fn {event, idx} ->
      handler_id = "#{handler_prefix}-#{idx}"
      setup_telemetry_handler(event, handler_id)
      handler_id
    end)
  end

  @doc """
  Detaches multiple telemetry handlers.

  ## Arguments

  - `handler_ids` - List of handler IDs to detach
  """
  @spec cleanup_telemetry_handlers([String.t()]) :: :ok
  def cleanup_telemetry_handlers(handler_ids) do
    Enum.each(handler_ids, &:telemetry.detach/1)
    :ok
  end
end
